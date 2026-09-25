/**
 * Decision 26: `trigger(task, payload, { tx })` inside the caller's transaction, dispatched after
 * the commit by a started engine (LISTEN/NOTIFY, poll and catch-up sweeps), reconciler fallback.
 */
import { randomUUID } from "node:crypto";
import { Client, escapeIdentifier, type PoolClient } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { ConfigError } from "../../src/errors.js";
import { migrate } from "../../src/migrations/migrate.js";
import { createOutboxListener, outboxChannel } from "../../src/outbox.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { TEST_DATABASE_URL } from "../helpers/env.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const other = useTestNamespace();

const shortId = () => randomUUID().slice(0, 8);
const warns: string[] = [];
const logger: EngineLogger = {
	debug: () => {},
	info: () => {},
	warn: (_fields, message) => warns.push(message),
	error: (_fields, message) => warns.push(message),
};

const engines: Engine[] = [];
function makeEngine(
	tasks: AnyTask[],
	overrides: Partial<EngineConfig> = {},
	namespace = ns
): Engine {
	const engine = createEngine({
		postgres: namespace.databaseUrl,
		redis: namespace.redisUrl,
		schema: namespace.schema,
		prefix: namespace.prefix,
		tasks,
		pool: { max: 6 },
		shutdownTimeoutMs: 1_000,
		reconciler: { intervalMs: 200, graceMs: 200, batchSize: 50 },
		logger,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

const t = (table: string, schema = ns.schema) => `${escapeIdentifier(schema)}.${table}`;

/** A task recording each execution of a payload `n`. */
function recorder() {
	const executions: number[] = [];
	const task = defineTask({
		id: `rec-${shortId()}`,
		queue: { name: `q${shortId()}` },
		run: async (payload: { n: number }) => {
			executions.push(payload.n);
			return payload.n;
		},
	});
	return { task, executions };
}

async function runStatus(id: string, schema = ns.schema) {
	const { rows } = await ns.pool.query<{ status: string; outbox_pending: boolean }>(
		`SELECT status, outbox_pending FROM ${t("runs", schema)} WHERE id = $1`,
		[id]
	);
	return rows[0];
}

/** This backend's subtransaction cache state (PG16+: `pg_stat_get_backend_subxact`). */
async function subxactState(client: PoolClient) {
	const { rows } = await client.query<{ count: number; overflowed: boolean }>(
		`SELECT s.subxact_count AS count, s.subxact_overflowed AS overflowed
		 FROM pg_stat_get_backend_idset() b, LATERAL pg_stat_get_backend_subxact(b) s
		 WHERE pg_stat_get_backend_pid(b) = pg_backend_pid()`
	);
	return rows[0];
}

async function withTx<T>(fn: (client: PoolClient) => Promise<T>, commit = true): Promise<T> {
	const client = await ns.pool.connect();
	try {
		await client.query("BEGIN");
		const result = await fn(client);
		await client.query(commit ? "COMMIT" : "ROLLBACK");
		return result;
	} catch (error) {
		await client.query("ROLLBACK").catch(() => {});
		throw error;
	} finally {
		client.release();
	}
}

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	await migrate(other.pool, other.schema);
	await ns.pool.query(`CREATE TABLE ${t("orders")} (id text PRIMARY KEY)`);
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

describe("trigger(..., { tx })", () => {
	it("commit: the run and the business row commit together and the run executes once", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();

		const handle = await withTx(async client => {
			await client.query(`INSERT INTO ${t("orders")} (id) VALUES ($1)`, ["o-commit"]);
			const h = await engine.trigger(task, { n: 1 }, { tx: client });
			// Not visible (nor enqueued) before the commit.
			expect(await runStatus(h.id)).toBeUndefined();
			return h;
		});

		const committedAt = Date.now();
		await waitFor(async () => (await runStatus(handle.id))?.status === "completed", {
			timeoutMs: 5_000,
			intervalMs: 10,
		});
		// Well under the 2 s poll: dispatched by the NOTIFY wake-up.
		expect(Date.now() - committedAt).toBeLessThan(1_500);
		expect(executions).toEqual([1]);
		expect((await runStatus(handle.id))?.outbox_pending).toBe(false);
		const { rowCount } = await ns.pool.query(`SELECT 1 FROM ${t("orders")} WHERE id = 'o-commit'`);
		expect(rowCount).toBe(1);
	});

	it("rollback: no run row and nothing executes", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();

		const handle = await withTx(async client => {
			await client.query(`INSERT INTO ${t("orders")} (id) VALUES ($1)`, ["o-rollback"]);
			return engine.trigger(task, { n: 2 }, { tx: client });
		}, false);

		await new Promise(resolve => setTimeout(resolve, 600));
		expect(await runStatus(handle.id)).toBeUndefined();
		expect(executions).toEqual([]);
		const { rowCount } = await ns.pool.query(
			`SELECT 1 FROM ${t("orders")} WHERE id = 'o-rollback'`
		);
		expect(rowCount).toBe(0);
	});

	it("many triggers in one transaction are all dispatched exactly once after the commit", async () => {
		const { task, executions } = recorder();
		const engineA = makeEngine([task]);
		const engineB = makeEngine([task]);
		await engineA.start();
		await engineB.start();
		const queued: string[] = [];
		for (const e of [engineA, engineB]) e.events.on("run:queued", ev => void queued.push(ev.runId));

		const handles = await withTx(async client => {
			const out = [];
			for (let n = 0; n < 50; n++) out.push(await engineA.trigger(task, { n }, { tx: client }));
			return out;
		});

		await waitFor(async () => executions.length === 50, { timeoutMs: 10_000 });
		await new Promise(resolve => setTimeout(resolve, 300));
		expect([...executions].sort((a, b) => a - b)).toEqual(Array.from({ length: 50 }, (_, i) => i));
		// Two listening engines, one claim per run: each `run:queued` exactly once.
		expect([...queued].sort()).toEqual(handles.map(h => h.id).sort());
	});

	it("a run committed while no engine listens is dispatched when one starts", async () => {
		const { task, executions } = recorder();
		const triggerOnly = makeEngine([task]);
		const handle = await withTx(client => triggerOnly.trigger(task, { n: 3 }, { tx: client }));
		await new Promise(resolve => setTimeout(resolve, 300));
		expect(await runStatus(handle.id)).toEqual({ status: "queued", outbox_pending: true });

		const worker = makeEngine([task]);
		await worker.start();
		await waitFor(async () => (await runStatus(handle.id))?.status === "completed", {
			timeoutMs: 5_000,
		});
		expect(executions).toEqual([3]);
	});

	it("a lost listener connection reconnects; runs committed meanwhile are still dispatched", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();
		const channel = outboxChannel(ns.schema);
		const listeners = async () =>
			(
				await ns.pool.query<{ pid: number }>(
					"SELECT pid FROM pg_stat_activity WHERE query = $1 AND datname = current_database()",
					[`LISTEN ${escapeIdentifier(channel)}`]
				)
			).rows.map(r => r.pid);
		await waitFor(async () => (await listeners()).length === 1, { timeoutMs: 5_000 });
		const [pid] = await listeners();
		await ns.pool.query("SELECT pg_terminate_backend($1)", [pid]);

		const handle = await withTx(client => engine.trigger(task, { n: 4 }, { tx: client }));
		await waitFor(async () => (await runStatus(handle.id))?.status === "completed", {
			timeoutMs: 8_000,
		});
		expect(executions).toEqual([4]);
		await waitFor(async () => (await listeners()).some(p => p !== pid), { timeoutMs: 8_000 });
	});

	it("an enqueue lost after the claim is recovered by the reconciler (decision 1)", async () => {
		const { task, executions } = recorder();
		const triggerOnly = makeEngine([task]);
		const handle = await withTx(client => triggerOnly.trigger(task, { n: 5 }, { tx: client }));
		// Simulates a claim whose enqueue was lost: flag cleared, no job in Redis.
		await ns.pool.query(`UPDATE ${t("runs")} SET outbox_pending = false WHERE id = $1`, [
			handle.id,
		]);
		const worker = makeEngine([task]);
		await worker.start();
		await waitFor(async () => (await runStatus(handle.id))?.status === "completed", {
			timeoutMs: 8_000,
		});
		expect(executions).toEqual([5]);
	});

	it("idempotency key: a live key returns the existing run and the caller's transaction stays usable", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();
		const key = `k-${shortId()}`;
		const existing = await engine.trigger(task, { n: 6 }, { idempotencyKey: key });

		const [again, fresh, freshAgain] = await withTx(async client => {
			const a = await engine.trigger(task, { n: 60 }, { tx: client, idempotencyKey: key });
			// Still usable: the live key's claim inserted nothing and raised nothing.
			await client.query(`INSERT INTO ${t("orders")} (id) VALUES ($1)`, ["o-idem"]);
			const freshKey = `k-${shortId()}`;
			const f = await engine.trigger(task, { n: 61 }, { tx: client, idempotencyKey: freshKey });
			const f2 = await engine.trigger(task, { n: 62 }, { tx: client, idempotencyKey: freshKey });
			return [a, f, f2];
		});

		expect(again.id).toBe(existing.id);
		expect(freshAgain.id).toBe(fresh.id);
		await waitFor(async () => (await runStatus(fresh.id))?.status === "completed", {
			timeoutMs: 5_000,
		});
		await waitFor(async () => executions.length === 2, { timeoutMs: 5_000 });
		expect([...executions].sort()).toEqual([6, 61]);
		const { rowCount } = await ns.pool.query(`SELECT 1 FROM ${t("orders")} WHERE id = 'o-idem'`);
		expect(rowCount).toBe(1);
	});

	it("200 keyed triggers in one transaction open no subtransaction (no subxid overflow)", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();
		const prefix = `bulk-${shortId()}`;
		const handles = await withTx(async client => {
			const out = [];
			for (let n = 0; n < 200; n++) {
				out.push(
					await engine.trigger(task, { n }, { tx: client, idempotencyKey: `${prefix}-${n}` })
				);
			}
			// Same keys again: every one resolves to the run created above.
			const again = await engine.trigger(
				task,
				{ n: 999 },
				{
					tx: client,
					idempotencyKey: `${prefix}-7`,
				}
			);
			expect(again.id).toBe(out[7]?.id);
			expect(await subxactState(client)).toEqual({ count: 0, overflowed: false });
			// Still usable.
			await client.query(`INSERT INTO ${t("orders")} (id) VALUES ($1)`, [`o-${prefix}`]);
			return out;
		});
		expect(new Set(handles.map(h => h.id)).size).toBe(200);
		await waitFor(async () => executions.length === 200, { timeoutMs: 15_000 });
		expect([...executions].sort((a, b) => a - b)).toEqual(Array.from({ length: 200 }, (_, i) => i));
	});

	it("an autocommit client (no BEGIN) with an idempotency key creates and dispatches the run", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();
		const key = `k-${shortId()}`;
		const client = await ns.pool.connect();
		let first: { id: string };
		let second: { id: string };
		try {
			first = await engine.trigger(task, { n: 70 }, { tx: client, idempotencyKey: key });
			second = await engine.trigger(task, { n: 71 }, { tx: client, idempotencyKey: key });
		} finally {
			client.release();
		}
		expect(second.id).toBe(first.id);
		await waitFor(async () => (await runStatus(first.id))?.status === "completed", {
			timeoutMs: 5_000,
		});
		expect(executions).toEqual([70]);
	});

	it("delay counts from the transaction's start, not from the post-commit claim", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await withTx(async client => {
			const h = await engine.trigger(task, { n: 80 }, { tx: client, delay: 2_000 });
			// Hold the transaction open for most of the delay.
			await new Promise(resolve => setTimeout(resolve, 1_500));
			return h;
		});
		const committedAt = Date.now();
		await waitFor(async () => (await runStatus(handle.id))?.status === "completed", {
			timeoutMs: 5_000,
			intervalMs: 10,
		});
		// Dispatched with the remaining ~500 ms, not a fresh 2 s after the claim.
		expect(Date.now() - committedAt).toBeLessThan(1_500);
		expect(executions).toEqual([80]);
		const { rows } = await ns.pool.query<{ ok: boolean }>(
			`SELECT started_at >= created_at + interval '2 seconds' AS ok FROM ${t("runs")} WHERE id = $1`,
			[handle.id]
		);
		// Never started before the delay elapsed from the trigger (tx) time.
		expect(rows[0]?.ok).toBe(true);
	});

	it("concurrencyKey is stored and enforced for runs triggered in a transaction", async () => {
		let running = 0;
		let maxRunning = 0;
		const done: number[] = [];
		const task = defineTask({
			id: `ck-${shortId()}`,
			queue: { name: `q${shortId()}`, concurrency: 5, concurrencyKey: { limit: 1 } },
			run: async (payload: { n: number }) => {
				running++;
				maxRunning = Math.max(maxRunning, running);
				await new Promise(resolve => setTimeout(resolve, 150));
				running--;
				done.push(payload.n);
				return payload.n;
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handles = await withTx(async client => {
			const out = [];
			for (let n = 0; n < 3; n++) {
				out.push(await engine.trigger(task, { n }, { tx: client, concurrencyKey: "tenant-1" }));
			}
			return out;
		});
		await waitFor(() => done.length === 3, { timeoutMs: 15_000 });
		expect(maxRunning).toBe(1);
		for (const h of handles) {
			expect((await engine.runs.get(h.id))?.concurrencyKey).toBe("tenant-1");
		}
	});

	it("leaves the caller's session untouched (no LISTEN, SET or dangling savepoint)", async () => {
		const { task } = recorder();
		const engine = makeEngine([task]);
		const client = await ns.pool.connect();
		try {
			const before = await client.query(
				"SELECT current_setting('search_path') AS sp, current_setting('statement_timeout') AS st"
			);
			await client.query("BEGIN");
			await engine.trigger(task, { n: 7 }, { tx: client, idempotencyKey: `k-${shortId()}` });
			const listening = await client.query("SELECT pg_listening_channels() AS c");
			expect(listening.rows).toEqual([]);
			// No subtransaction was opened (the claim is a single statement).
			expect(await subxactState(client)).toEqual({ count: 0, overflowed: false });
			await client.query("ROLLBACK");
			const after = await client.query(
				"SELECT current_setting('search_path') AS sp, current_setting('statement_timeout') AS st"
			);
			expect(after.rows).toEqual(before.rows);
		} finally {
			client.release();
		}
	});

	it("channels are schema-scoped: another schema's engine never dispatches the run", async () => {
		const { task, executions } = recorder();
		const triggerOnly = makeEngine([task]);
		const otherWorker = makeEngine([task], {}, other);
		await otherWorker.start();
		expect(outboxChannel(ns.schema)).not.toBe(outboxChannel(other.schema));

		const handle = await withTx(client => triggerOnly.trigger(task, { n: 8 }, { tx: client }));
		await new Promise(resolve => setTimeout(resolve, 800));
		expect(await runStatus(handle.id)).toEqual({ status: "queued", outbox_pending: true });
		expect(executions).toEqual([]);
	});

	it("rejects a pool and debounce + tx with a ConfigError", async () => {
		const { task } = recorder();
		const engine = makeEngine([task]);
		await expect(engine.trigger(task, { n: 9 }, { tx: ns.pool })).rejects.toThrow(/pg\.Pool/);
		await withTx(async client => {
			await expect(
				engine.trigger(task, { n: 9 }, { tx: client, debounce: { key: "d", delay: "1s" } })
			).rejects.toBeInstanceOf(ConfigError);
		});
		await expect(
			engine.trigger(task, { n: 9 }, { tx: {} as unknown as PoolClient })
		).rejects.toBeInstanceOf(ConfigError);
	});
});

describe("createOutboxListener", () => {
	it("sweeps on NOTIFY (not only on the poll) and coalesces a burst", async () => {
		const channel = outboxChannel(ns.schema);
		let sweeps = 0;
		const listener = createOutboxListener({
			clientConfig: { connectionString: TEST_DATABASE_URL },
			channel,
			logger,
			pollIntervalMs: 60_000,
			dispatchBatch: async () => {
				sweeps++;
				await new Promise(resolve => setTimeout(resolve, 20));
				return false;
			},
		});
		try {
			await listener.start();
			expect(listener.isListening()).toBe(true);
			await waitFor(() => sweeps === 1, { timeoutMs: 2_000 }); // catch-up after LISTEN
			const notifier = new Client({ connectionString: TEST_DATABASE_URL });
			await notifier.connect();
			try {
				for (let i = 0; i < 20; i++)
					await notifier.query("SELECT pg_notify($1, $2)", [channel, `${i}`]);
			} finally {
				await notifier.end();
			}
			await waitFor(() => sweeps >= 2, { timeoutMs: 2_000 });
			await new Promise(resolve => setTimeout(resolve, 200));
			// 20 notifications, far fewer sweeps (one running + at most one coalesced rerun per burst).
			expect(sweeps).toBeLessThan(10);
		} finally {
			await listener.close();
		}
		expect(listener.isListening()).toBe(false);
	});
});
