import { describe, expect, it } from "vitest";
import { createSqlContext } from "../../src/store/sql/db.js";
import { createRunStore } from "../../src/store/sql/runs.js";
import { useTestNamespace } from "../helpers/namespace.js";
import { useRunFixtures } from "../helpers/run-fixtures.js";

const ns = useTestNamespace();
const f = useRunFixtures(ns);

const DAY_MS = 24 * 60 * 60 * 1000;

async function runCount(taskId: string): Promise<number> {
	const rows = await f.sql<{ n: number }>(
		`SELECT count(*)::int AS n FROM ${f.t("runs")} WHERE task_id = $1`,
		[taskId]
	);
	return rows[0]?.n ?? -1;
}

describe("IdempotencyStore / RunStore.create with idempotency (decision 9)", () => {
	it("new key: creates the run and records the key", async () => {
		const input = f.newRun({ idempotency: { key: "k-new", ttlMs: DAY_MS } });
		const result = await f.runs.create(input);
		expect(result.kind).toBe("created");
		if (result.kind !== "created") return;
		expect(result.run.id).toBe(input.id);
		expect(result.enqueue).toEqual({
			runId: input.id,
			taskId: "task-a",
			queue: "default",
			seq: 1,
			delayMs: 0,
		});
		const found = await f.idempotency.find("task-a", "k-new");
		expect(found?.runId).toBe(input.id);
	});

	it("live key: returns the existing run and rolls the losing insert back", async () => {
		const first = f.newRun({ idempotency: { key: "k-live", ttlMs: DAY_MS } });
		await f.runs.create(first);
		const second = f.newRun({ idempotency: { key: "k-live", ttlMs: DAY_MS } });
		const result = await f.runs.create(second);
		expect(result.kind).toBe("existing");
		expect(result.run.id).toBe(first.id);
		expect(await f.runs.get(second.id)).toBeNull();
		expect(await runCount("task-a")).toBe(1);
	});

	it("same key under another task is independent", async () => {
		await f.runs.create(f.newRun({ idempotency: { key: "shared", ttlMs: DAY_MS } }));
		const other = await f.runs.create(
			f.newRun({ taskId: "task-b", idempotency: { key: "shared", ttlMs: DAY_MS } })
		);
		expect(other.kind).toBe("created");
	});

	it("expired key is taken over by another run", async () => {
		const first = f.newRun({ idempotency: { key: "k-exp", ttlMs: DAY_MS } });
		await f.runs.create(first);
		await f.sql(
			`UPDATE ${f.t("idempotency_keys")} SET expires_at = now() - interval '1 second' WHERE key = $1`,
			["k-exp"]
		);
		const second = f.newRun({ idempotency: { key: "k-exp", ttlMs: DAY_MS } });
		const result = await f.runs.create(second);
		expect(result.kind).toBe("created");
		expect(result.run.id).toBe(second.id);
		expect((await f.idempotency.find("task-a", "k-exp"))?.runId).toBe(second.id);
		// The previous run is untouched.
		expect((await f.runs.get(first.id))?.status).toBe("queued");
	});

	it("concurrent creates with the same key: exactly one run, both get its id", async () => {
		const inputs = Array.from({ length: 4 }, () =>
			f.newRun({ taskId: "task-race", idempotency: { key: "k-race", ttlMs: DAY_MS } })
		);
		const results = await Promise.all(inputs.map(input => f.runs.create(input)));
		const ids = new Set(results.map(r => r.run.id));
		expect(ids.size).toBe(1);
		expect(results.filter(r => r.kind === "created")).toHaveLength(1);
		expect(await runCount("task-race")).toBe(1);
		const [winner] = [...ids];
		expect((await f.idempotency.find("task-race", "k-race"))?.runId).toBe(winner);
	});

	it("internal key with ttlMs null never expires", async () => {
		const first = f.newRun({ idempotency: { key: "__child:x:y", ttlMs: null } });
		await f.runs.create(first);
		const found = await f.idempotency.find("task-a", "__child:x:y");
		expect(found).toEqual({ runId: first.id, expiresAt: null });
		const again = await f.runs.create(
			f.newRun({ idempotency: { key: "__child:x:y", ttlMs: null } })
		);
		expect(again.kind).toBe("existing");
		expect(again.run.id).toBe(first.id);
	});

	it("the TTL passed by the caller lands in expires_at (DB clock)", async () => {
		// TODO(phase 3): the engine applies idempotencyKeyTtl (default 7 days) to user keys.
		const ttlMs = 7 * DAY_MS;
		await f.runs.create(f.newRun({ idempotency: { key: "k-ttl", ttlMs } }));
		const rows = await f.sql<{ ms: number }>(
			`SELECT (extract(epoch from (expires_at - now())) * 1000)::float8 AS ms
			 FROM ${f.t("idempotency_keys")} WHERE key = $1`,
			["k-ttl"]
		);
		const ms = rows[0]?.ms ?? 0;
		expect(ms).toBeGreaterThan(ttlMs - 60_000);
		expect(ms).toBeLessThanOrEqual(ttlMs);
	});

	it("inside a StoreTx, a lost claim rolls back only to the savepoint", async () => {
		const first = f.newRun({ idempotency: { key: "k-tx", ttlMs: DAY_MS } });
		await f.runs.create(first);
		const client = await ns.pool.connect();
		try {
			await client.query("BEGIN");
			const txRuns = createRunStore({ kind: "tx", client }, createSqlContext(ns.schema));
			const other = f.newRun({ taskId: "task-tx" });
			await txRuns.create(other);
			const loser = f.newRun({ idempotency: { key: "k-tx", ttlMs: DAY_MS } });
			const result = await txRuns.create(loser);
			expect(result.kind).toBe("existing");
			expect(result.run.id).toBe(first.id);
			await client.query("COMMIT");
			expect(await f.runs.get(other.id)).not.toBeNull();
			expect(await f.runs.get(loser.id)).toBeNull();
		} finally {
			client.release();
		}
	});

	it("claim() and find() directly", async () => {
		const run = f.newRun();
		await f.runs.create(run);
		expect(
			await f.idempotency.claim({ taskId: "task-a", key: "direct", runId: run.id, ttlMs: 1000 })
		).toEqual({ claimed: true });
		const other = f.newRun();
		await f.runs.create(other);
		expect(
			await f.idempotency.claim({ taskId: "task-a", key: "direct", runId: other.id, ttlMs: 1000 })
		).toEqual({ claimed: false, existingRunId: run.id });
		expect(await f.idempotency.find("task-a", "missing")).toBeNull();
	});
});
