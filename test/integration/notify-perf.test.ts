/**
 * Phase 7 perf fix: `RunNotifier.status` no longer needs a `runs.get` per transition (the store
 * results already carry `status`/`updatedAt`, and taskId/attempt/error come from the caller's own
 * `ExecutionState`/`RunRecord`/reconciler result). These tests prove:
 * 1. No extra `SELECT ... FROM runs WHERE id = $1` (the shape of `RunStore.get`) is issued while a
 *    run goes through queued/started/retrying/waiting/completed/canceled/stall-recovery.
 * 2. `engine.events` still reports the right `taskId`/`attempt`/`updatedAt`/`error` for every
 *    transition, and `runs.subscribe` still reports the right `updatedAt` (database clock,
 *    decision 17, never `new Date()`).
 */
import { randomUUID } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine, type RunEvent } from "../../src/engine.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import { RUN_COLUMNS } from "../../src/store/sql/resolve.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);
const testLogger: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };

const engines: Engine[] = [];
const pools: Pool[] = [];

function makeEngine(tasks: AnyTask[], overrides: Partial<EngineConfig> = {}): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 5 },
		heartbeatIntervalMs: 1_000,
		stallTimeoutMs: 10_000,
		shutdownTimeoutMs: 2_000,
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

/**
 * A pool that counts statements shaped like `RunStore.get`'s `SELECT ${RUN_COLUMNS} FROM runs
 * WHERE id = $1` (the exact query the old `RunNotifier` issued once per transition). Any other
 * `runs` read/write (acquire, complete, fail, retry, suspend, cancel, recoverStalled, ...) uses a
 * different column list, `RETURNING` clause or `WHERE`, so this only catches the query we removed.
 */
function countingPool(): { pool: Pool; getRunCount: () => number } {
	const pool = new Pool({ connectionString: ns.databaseUrl, max: 8 });
	pool.on("error", () => {});
	pools.push(pool);
	let count = 0;
	// Exactly `getRun`'s `SELECT ${RUN_COLUMNS} FROM runs WHERE id = $1` (`RunStore.get`): a plain
	// `SELECT ... FROM` with nothing after the `WHERE id = $1`. Other statements that also touch
	// `id = $1` (e.g. `acquire`'s `UPDATE ... WHERE id = $1 AND status IN (...) ... RETURNING
	// ${RUN_COLUMNS}`) are `UPDATE`s, not `SELECT ... FROM`, and/or have more after `id = $1`.
	const isGetRun = (text: unknown) =>
		typeof text === "string" &&
		/^\s*SELECT/i.test(text) &&
		text.includes(`SELECT ${RUN_COLUMNS} FROM`) &&
		/WHERE id = \$1\s*$/.test(text.trim());
	const sqlOf = (arg: unknown) =>
		typeof arg === "string" ? arg : (arg as { text?: unknown } | null)?.text;
	const wrapped = new WeakSet<PoolClient>();
	pool.on("connect", client => {
		if (wrapped.has(client)) return;
		wrapped.add(client);
		const clientQuery = client.query.bind(client) as (...args: unknown[]) => unknown;
		(client as unknown as { query: (...args: unknown[]) => unknown }).query = (...args) => {
			if (isGetRun(sqlOf(args[0]))) count++;
			return clientQuery(...args);
		};
	});
	return { pool, getRunCount: () => count };
}

async function rawStatus(runId: string): Promise<string | undefined> {
	const { rows } = await ns.pool.query<{ status: string }>(
		`SELECT status FROM "${ns.schema}".runs WHERE id = $1`,
		[runId]
	);
	return rows[0]?.status;
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
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
	await Promise.all(pools.splice(0).map(p => p.end()));
});

describe("RunNotifier: no extra runs.get per transition", () => {
	it("queued -> started -> retrying -> started -> completed", async () => {
		const task = defineTask({
			id: `retry-${shortId()}`,
			queue: { name: `q${shortId()}` },
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 10, jitter: false } },
			run: async (_payload: undefined, ctx) => {
				if (ctx.run.attempt === 1) throw new Error("first attempt fails");
				return "ok";
			},
		});
		const { pool, getRunCount } = countingPool();
		const engine = makeEngine([task], { postgres: { pool } });
		const seen: RunEvent[] = [];
		for (const name of ["run:queued", "run:started", "run:retrying", "run:completed"] as const) {
			engine.events.on(name, e => {
				seen.push(e);
			});
		}
		await engine.start();
		const handle = await engine.trigger(task);
		await waitFor(async () => (await rawStatus(handle.id)) === "completed", { timeoutMs: 10_000 });

		expect(seen.map(e => e.name)).toEqual([
			"run:queued",
			"run:started",
			"run:retrying",
			"run:started",
			"run:completed",
		]);
		expect(seen.map(e => e.taskId)).toEqual(Array(5).fill(task.id));
		expect(seen.map(e => e.attempt)).toEqual([1, 1, 2, 2, 2]);
		expect(seen[2]?.error?.message).toBe("first attempt fails");
		for (const e of seen) expect(e.updatedAt).toBeInstanceOf(Date);

		expect(getRunCount()).toBe(0);
	});

	it("queued -> started -> waiting (sleep) -> started -> completed", async () => {
		const task = defineTask({
			id: `sleep-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_payload: undefined, ctx) => {
				await ctx.step.sleep("nap", "50ms");
				return "done";
			},
		});
		const { pool, getRunCount } = countingPool();
		const engine = makeEngine([task], { postgres: { pool } });
		const seen: RunEvent[] = [];
		for (const name of ["run:queued", "run:started", "run:waiting", "run:completed"] as const) {
			engine.events.on(name, e => {
				seen.push(e);
			});
		}
		await engine.start();
		const handle = await engine.trigger(task);
		await waitFor(async () => (await rawStatus(handle.id)) === "completed", { timeoutMs: 10_000 });

		expect(seen.map(e => e.name)).toEqual([
			"run:queued",
			"run:started",
			"run:waiting",
			"run:started",
			"run:completed",
		]);
		expect(seen.every(e => e.taskId === task.id)).toBe(true);
		expect(seen.every(e => e.attempt === 1)).toBe(true);
		expect(seen.every(e => e.error === null)).toBe(true);

		expect(getRunCount()).toBe(0);
	});

	it("waiting -> canceled via engine.runs.cancel()", async () => {
		const task = defineTask({
			id: `wait-cancel-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_payload: undefined, ctx) => {
				await ctx.step.waitForToken("approval");
			},
		});
		const { pool, getRunCount } = countingPool();
		const engine = makeEngine([task], { postgres: { pool } });
		const seen: RunEvent[] = [];
		for (const name of ["run:waiting", "run:canceled"] as const) {
			engine.events.on(name, e => {
				seen.push(e);
			});
		}
		await engine.start();
		const handle = await engine.trigger(task);
		await waitFor(async () => (await rawStatus(handle.id)) === "waiting", { timeoutMs: 10_000 });

		const before = getRunCount();
		expect(await engine.runs.cancel(handle.id)).toBe("canceled");
		await waitFor(() => seen.some(e => e.name === "run:canceled"), { timeoutMs: 5_000 });

		expect(seen.map(e => e.name)).toEqual(["run:waiting", "run:canceled"]);
		const canceled = seen[1];
		expect(canceled).toMatchObject({ runId: handle.id, taskId: task.id, attempt: 1 });
		expect(canceled?.error?.name).toBe("Canceled");
		expect(canceled?.updatedAt).toBeInstanceOf(Date);

		expect(getRunCount()).toBe(before);
		expect(getRunCount()).toBe(0);
	});

	it("stall recovery (case 4) through the leader loop: retrying then completed, no extra runs.get", async () => {
		const task = defineTask({
			id: `stall-${shortId()}`,
			queue: { name: `q${shortId()}` },
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 10, jitter: false } },
			run: async () => "recovered",
		});
		const { pool, getRunCount } = countingPool();
		const rawStore = createSqlStore(pool, ns.schema);
		const engine = makeEngine([task], {
			postgres: { pool },
			heartbeatIntervalMs: 100,
			stallTimeoutMs: 500,
			reconciler: { intervalMs: 100, graceMs: 100, batchSize: 50 },
		});
		const seen: RunEvent[] = [];
		for (const name of ["run:retrying", "run:completed"] as const) {
			engine.events.on(name, e => {
				seen.push(e);
			});
		}
		// A "zombie" execution (as `resilience.test.ts`'s case-4 test does): a run created and
		// acquired directly, WITHOUT enqueuing it or ever heartbeating for it, so the reconciler is
		// the only thing that ever moves it forward (attempt 1 never actually runs).
		const created = await rawStore.runs.create({
			id: randomUUID(),
			taskId: task.id,
			queue: task.options.queue?.name ?? "default",
			payload: null,
			maxAttempts: 2,
			delayMs: 0,
		});
		if (created.kind !== "created") throw new Error("expected a new run");
		const acquired = await rawStore.runs.acquire(created.run.id, randomUUID(), 1);
		expect(acquired?.status).toBe("running");

		await engine.start();
		await waitFor(async () => (await rawStatus(created.run.id)) === "completed", {
			timeoutMs: 15_000,
		});

		expect(seen.map(e => e.name)).toEqual(["run:retrying", "run:completed"]);
		const retrying = seen[0];
		expect(retrying).toMatchObject({ runId: created.run.id, taskId: task.id, attempt: 2 });
		expect(retrying?.error?.message).toMatch(/stalled/);
		expect(retrying?.updatedAt).toBeInstanceOf(Date);

		expect(getRunCount()).toBe(0);
	});
});

describe("runs.subscribe: updatedAt still tracks the database clock", () => {
	it("emits monotonically increasing updatedAt across queued/started/retrying/completed", async () => {
		const task = defineTask({
			id: `subscribe-${shortId()}`,
			queue: { name: `q${shortId()}` },
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 10, jitter: false } },
			run: async (_payload: undefined, ctx) => {
				if (ctx.run.attempt === 1) throw new Error("boom");
				return "ok";
			},
		});
		// Trigger-only, not started yet: the run cannot move past `queued`, so subscribing now is
		// guaranteed to observe every later transition (pub/sub subscribes before the Postgres read
		// that yields the starting point -- mandatory order, see `engine.ts`'s `subscribeRun`).
		const engine = makeEngine([task]);
		const handle = await engine.trigger(task);
		const iterator = engine.runs.subscribe(handle.id)[Symbol.asyncIterator]();
		const first = await iterator.next();
		expect(first.done).toBe(false);
		expect(first.value?.status).toBe("queued");

		await engine.start();

		const statuses: { status: string; updatedAt: Date }[] = [
			{ status: first.value?.status ?? "", updatedAt: first.value?.updatedAt ?? new Date(0) },
		];
		for (;;) {
			const next = await iterator.next();
			if (next.done) break;
			statuses.push({ status: next.value.status, updatedAt: next.value.updatedAt });
			if (next.value.status === "completed" || next.value.status === "failed") break;
		}

		expect(statuses.map(s => s.status)).toEqual([
			"queued",
			"running",
			"queued",
			"running",
			"completed",
		]);
		for (let i = 1; i < statuses.length; i++) {
			const prev = statuses[i - 1];
			const cur = statuses[i];
			expect(prev).toBeDefined();
			expect(cur).toBeDefined();
			if (prev && cur)
				expect(cur.updatedAt.getTime()).toBeGreaterThanOrEqual(prev.updatedAt.getTime());
		}
	});
});
