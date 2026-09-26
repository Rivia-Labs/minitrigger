/**
 * Phase 6 acceptance through the engine: every reconciler case (forced DB states picked up by a
 * started engine within a short `reconciler.intervalMs`), stall by heartbeat, leadership loss and
 * reacquisition, `migrate()` while another process is leader, orphan waiting run with a warn and
 * retention in batches through the `-system` job.
 */
import { randomUUID } from "node:crypto";
import { escapeIdentifier } from "pg";
import { afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import type { BullmqTransportConfig, CommonConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import type { Store } from "../../src/store/types.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

const shortId = () => randomUUID().slice(0, 8);

type LogRecord = {
	engine: string;
	level: string;
	message: string;
	fields: Record<string, unknown>;
};
const logs: LogRecord[] = [];
function loggerFor(engine: string): EngineLogger {
	const push = (level: string) => (fields: Record<string, unknown>, message: string) => {
		logs.push({ engine, level, message, fields });
	};
	return { debug: push("debug"), info: push("info"), warn: push("warn"), error: push("error") };
}

const engines: Engine[] = [];
let store: Store;

function makeEngine(
	tasks: AnyTask[],
	overrides: Partial<CommonConfig> & Partial<BullmqTransportConfig> = {},
	name = "a"
): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 5 },
		heartbeatIntervalMs: 100,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 1_000,
		shutdownTimeoutMs: 1_000,
		reconciler: { intervalMs: 200, graceMs: 200, batchSize: 50 },
		logger: loggerFor(name),
		...overrides,
	});
	engines.push(engine);
	return engine;
}

const t = (table: string) => `${escapeIdentifier(ns.schema)}.${escapeIdentifier(table)}`;

async function runRow(id: string) {
	const { rows } = await ns.pool.query<{
		status: string;
		attempt: number;
		enqueue_seq: number;
		execution_id: string | null;
		output: unknown;
	}>(`SELECT * FROM ${t("runs")} WHERE id = $1`, [id]);
	const row = rows[0];
	if (!row) throw new Error(`run ${id} not found`);
	return row;
}

async function waitForStatus(id: string, status: string, timeoutMs = 8_000) {
	await waitFor(async () => (await runRow(id)).status === status, { timeoutMs, intervalMs: 25 });
	return runRow(id);
}

/** A plain task on its own queue that returns its payload doubled. */
function doubler() {
	const queue = `q${shortId()}`;
	const task = defineTask({
		id: `double-${shortId()}`,
		queue: { name: queue },
		run: async (payload: { n: number }) => ({ doubled: payload.n * 2 }),
	});
	return { task, queue };
}

/** Inserts a `queued` run straight into Postgres, WITHOUT enqueuing it (a lost enqueue). */
async function insertRun(taskId: string, queue: string, payload: unknown = { n: 21 }) {
	const created = await store.runs.create({
		id: randomUUID(),
		taskId,
		queue,
		payload: payload as never,
		maxAttempts: 3,
		delayMs: 0,
	});
	if (created.kind !== "created") throw new Error("expected a new run");
	return created.run;
}

/** Backend pid holding the leader lock `(hashtext(schema), 2)`. */
async function leaderPid(): Promise<number | undefined> {
	const { rows } = await ns.pool.query<{ pid: number }>(
		`SELECT pid FROM pg_locks
		 WHERE locktype = 'advisory' AND classid = hashtext($1) AND objid = 2 AND objsubid = 2
		   AND granted = true`,
		[ns.schema]
	);
	return rows[0]?.pid;
}

const becameLeader = (engine: string) =>
	logs.some(l => l.engine === engine && l.message.includes("is now the leader"));

/** `scheduleRetention`'s dedup window (`src/engine.ts`): `floor(now_ms / intervalMs)`, decision 17. */
const RETENTION_WINDOW_INTERVAL_MS = 60_000;

/**
 * `scheduleRetention` derives its "one job per window" dedup from the real DB clock -- there is no
 * injectable clock for the real engine (only the test engine has one, Phase 9). A test asserting
 * "no second sweep happens" over some observation period is only deterministic if that whole
 * period fits inside a single window: otherwise the window can tick over mid-test by wall-clock
 * luck alone, and a perfectly correct engine enqueues a second (empty) sweep -- a real event, not
 * a bug, but exactly the flake this guards against (it hit once in ~a hundred runs, matching the
 * ~1% chance of the old fixed 600ms check straddling a 60s boundary). Polls the DB clock (never a
 * blind sleep) until at least `marginMs` remains before the next boundary, so everything that runs
 * after this resolves -- bounded by this test's own timeouts below -- is guaranteed to land inside
 * the same window.
 */
async function waitUntilSafeRetentionWindow(marginMs: number): Promise<void> {
	await waitFor(
		async () => {
			const { rows } = await ns.pool.query<{ now_ms: string }>(
				"SELECT floor(extract(epoch FROM now()) * 1000)::bigint AS now_ms"
			);
			const nowMs = Number(rows[0]?.now_ms);
			const msIntoWindow = nowMs % RETENTION_WINDOW_INTERVAL_MS;
			return msIntoWindow >= marginMs && msIntoWindow <= RETENTION_WINDOW_INTERVAL_MS - marginMs;
		},
		{ timeoutMs: RETENTION_WINDOW_INTERVAL_MS, intervalMs: 25 }
	);
}

/**
 * Polls `check` every `intervalMs` across `durationMs`, failing as soon as it turns false -- unlike
 * a single check after a fixed sleep, a regression is caught close to when it actually happened
 * instead of only at the very end.
 */
async function assertStableFor(
	check: () => boolean,
	durationMs: number,
	intervalMs = 50
): Promise<void> {
	const deadline = Date.now() + durationMs;
	for (;;) {
		expect(check()).toBe(true);
		const remaining = deadline - Date.now();
		if (remaining <= 0) return;
		await new Promise(resolve => setTimeout(resolve, Math.min(intervalMs, remaining)));
	}
}

beforeAll(async () => {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks: [],
	});
	await engine.migrate();
	await engine.stop();
	store = createSqlStore(ns.pool, ns.schema);
});

beforeEach(async () => {
	// The reconciler is schema-wide: every test starts from empty tables.
	await ns.pool.query(
		`TRUNCATE ${t("runs")}, ${t("idempotency_keys")}, ${t("steps")}, ${t("waitpoints")}, ${t("run_logs")} CASCADE`
	);
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
	logs.length = 0;
});

describe("reconciler cases through the engine", () => {
	it("case 1: a queued run whose enqueue was lost is re-enqueued with its current seq", async () => {
		const { task, queue } = doubler();
		const run = await insertRun(task.id, queue);
		await makeEngine([task]).start();

		const row = await waitForStatus(run.id, "completed");
		expect(row.output).toEqual({ doubled: 42 });
		expect(row.attempt).toBe(1);
		expect(row.enqueue_seq).toBe(1);
	});

	it("case 2: a waiting run past run_after with nothing pending is re-enqueued with the SAME seq", async () => {
		const { task, queue } = doubler();
		const run = await insertRun(task.id, queue);
		await ns.pool.query(
			`UPDATE ${t("runs")} SET status = 'waiting', run_after = now() - interval '5 seconds'
			 WHERE id = $1`,
			[run.id]
		);
		await makeEngine([task]).start();

		const row = await waitForStatus(run.id, "completed");
		expect(row.enqueue_seq).toBe(1);
		expect(row.attempt).toBe(1);
	});

	it("case 3: an orphan waiting run (run_after NULL, no waitpoint) is resumed with a warn naming it", async () => {
		const { task, queue } = doubler();
		const run = await insertRun(task.id, queue);
		await ns.pool.query(
			`UPDATE ${t("runs")} SET status = 'waiting', run_after = NULL WHERE id = $1`,
			[run.id]
		);
		await makeEngine([task]).start();

		const row = await waitForStatus(run.id, "completed");
		expect(row.enqueue_seq).toBe(2);
		expect(row.attempt).toBe(1);
		const warn = logs.find(l => l.level === "warn" && l.fields["runId"] === run.id);
		expect(warn?.message).toMatch(/run_after=NULL/);
	});

	it("case 4 (stall by heartbeat): a zombie that never heartbeats is recovered with attempt + 1, then completes", async () => {
		const { task, queue } = doubler();
		const run = await insertRun(task.id, queue);
		// A "zombie" execution: acquired with an execution id nobody heartbeats for.
		const zombie = await store.runs.acquire(run.id, randomUUID(), run.enqueueSeq);
		expect(zombie?.status).toBe("running");

		await makeEngine([task]).start();

		const row = await waitForStatus(run.id, "completed");
		expect(row.attempt).toBe(2);
		expect(row.execution_id).toBeNull();
		expect(row.output).toEqual({ doubled: 42 });
	});

	it("case 4: a stalled run with no attempts left fails definitively", async () => {
		const { task, queue } = doubler();
		const created = await store.runs.create({
			id: randomUUID(),
			taskId: task.id,
			queue,
			payload: { n: 1 },
			maxAttempts: 1,
			delayMs: 0,
		});
		if (created.kind !== "created") throw new Error("expected a new run");
		await store.runs.acquire(created.run.id, randomUUID(), 1);

		await makeEngine([task]).start();

		const run = await waitForStatus(created.run.id, "failed");
		expect(run.attempt).toBe(1);
		const full = await store.runs.get(created.run.id);
		expect(full?.error?.message).toMatch(/stalled/);
	});

	it("case 5: an overdue waitpoint whose expire job never fired is expired and the run resumed", async () => {
		const queue = `q${shortId()}`;
		const task = defineTask({
			id: `approval-${shortId()}`,
			queue: { name: queue },
			run: async (_payload: undefined, ctx) => {
				const token = await ctx.step.createToken("approval", { timeout: "1h" });
				const result = await ctx.step.waitForToken(token);
				return { ok: result.ok };
			},
		});
		// Long grace: only case 5 may act on this run.
		const engine = makeEngine([task], {
			reconciler: { intervalMs: 200, graceMs: 60_000, batchSize: 50 },
		});
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(handle.id, "waiting");

		// The expire job is 1h away: make the waitpoint overdue in Postgres only.
		await ns.pool.query(
			`UPDATE ${t("waitpoints")} SET expires_at = now() - interval '1 second' WHERE run_id = $1`,
			[handle.id]
		);

		const row = await waitForStatus(handle.id, "completed");
		expect(row.output).toEqual({ ok: false });
		const { rows } = await ns.pool.query<{ status: string }>(
			`SELECT status FROM ${t("waitpoints")} WHERE run_id = $1`,
			[handle.id]
		);
		expect(rows.map(r => r.status)).toEqual(["timed_out"]);
	});

	it("does not run the reconciler on a non-leader engine", async () => {
		const { task, queue } = doubler();
		const leader = makeEngine([task], {}, "leader");
		await leader.start();
		await waitFor(() => becameLeader("leader"));
		const follower = makeEngine([task], {}, "follower");
		await follower.start();

		const run = await insertRun(task.id, queue);
		await waitForStatus(run.id, "completed");
		expect(becameLeader("follower")).toBe(false);
		expect(logs.some(l => l.engine === "follower" && l.message === "reconciler tick")).toBe(false);
		expect(logs.some(l => l.engine === "leader" && l.message === "reconciler tick")).toBe(true);
	});
});

describe("leadership", () => {
	it("after the leader's backend is terminated, the other engine becomes leader within a few ticks", async () => {
		const { task, queue } = doubler();
		const a = makeEngine([task], {}, "a");
		await a.start();
		await waitFor(() => becameLeader("a"));
		const b = makeEngine([task], {}, "b");
		await b.start();

		const pid = await leaderPid();
		expect(pid).toBeDefined();
		await ns.pool.query("SELECT pg_terminate_backend($1)", [pid]);

		await waitFor(() => becameLeader("b"), { timeoutMs: 3_000 });
		expect(
			logs.some(
				l => l.engine === "a" && l.level === "warn" && l.message.includes("leadership lost")
			)
		).toBe(true);
		const newPid = await leaderPid();
		expect(newPid).toBeDefined();
		expect(newPid).not.toBe(pid);

		// The new leader reconciles: a lost enqueue is still recovered.
		const run = await insertRun(task.id, queue);
		await waitForStatus(run.id, "completed");
	});

	it("stop() releases leadership so another engine takes over", async () => {
		const a = makeEngine([], {}, "a");
		await a.start();
		await waitFor(() => becameLeader("a"));
		const b = makeEngine([], {}, "b");
		await b.start();

		await a.stop();
		await waitFor(() => becameLeader("b"), { timeoutMs: 3_000 });
	});

	it("migrate() does not block while another process is leader", async () => {
		const a = makeEngine([], {}, "a");
		await a.start();
		await waitFor(() => becameLeader("a"));
		expect(await leaderPid()).toBeDefined();

		const b = makeEngine([], {}, "b");
		const started = Date.now();
		const result = await Promise.race([
			b.migrate().then(() => "migrated" as const),
			new Promise<"blocked">(resolve => setTimeout(() => resolve("blocked"), 5_000)),
		]);
		expect(result).toBe("migrated");
		expect(Date.now() - started).toBeLessThan(5_000);
		// Still the leader: migrations use a different advisory key.
		expect(await leaderPid()).toBeDefined();
	});
});

describe("retention", () => {
	it("deletes old finished runs in batches through the -system job, keeping recent ones", async () => {
		// Comfortably covers the waitFor + assertStableFor budgets below (up to ~5.6s) plus engine
		// startup, so the window can't tick over mid-test by wall-clock luck (see
		// waitUntilSafeRetentionWindow).
		await waitUntilSafeRetentionWindow(10_000);

		const { task, queue } = doubler();
		const old: string[] = [];
		for (let i = 0; i < 7; i++) {
			const run = await insertRun(task.id, queue);
			old.push(run.id);
		}
		const recent = await insertRun(task.id, queue);
		const active = await insertRun(task.id, queue);
		await ns.pool.query(
			`UPDATE ${t("runs")} SET status = 'completed', run_after = NULL,
			   finished_at = now() - interval '3 days' WHERE id = ANY($1::uuid[])`,
			[old]
		);
		await ns.pool.query(
			`UPDATE ${t("runs")} SET status = 'completed', run_after = NULL, finished_at = now()
			 WHERE id = $1`,
			[recent.id]
		);
		// A queued run older than retention is never deleted (not finished); keep it off the
		// reconciler's radar with a far run_after.
		await ns.pool.query(
			`UPDATE ${t("runs")} SET run_after = now() + interval '1 day' WHERE id = $1`,
			[active.id]
		);

		await makeEngine([task], {
			idempotencyKeyTtl: "1d",
			retention: { days: 1, batchSize: 3, intervalMs: RETENTION_WINDOW_INTERVAL_MS },
		}).start();

		await waitFor(
			() => logs.some(l => l.message.startsWith("retention: deleted") && l.fields["deleted"] === 7),
			{ timeoutMs: 5_000 }
		);
		const { rows } = await ns.pool.query<{ id: string }>(`SELECT id FROM ${t("runs")}`);
		expect(rows.map(r => r.id).sort()).toEqual([recent.id, active.id].sort());
		// One retention job per window: later reconciler ticks (every 200ms here) don't enqueue
		// another sweep. `waitUntilSafeRetentionWindow` above guarantees this whole observation
		// fits inside the same window, so a second log here would be a real regression, not luck.
		await assertStableFor(
			() => logs.filter(l => l.message.startsWith("retention: deleted")).length === 1,
			600
		);
	});
});
