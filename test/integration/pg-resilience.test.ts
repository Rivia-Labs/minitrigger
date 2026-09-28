/**
 * Resilience and cross-process concurrency of the Postgres-only transport (decision 28; Task 12):
 * two engines claiming the same globally-limited queue (Review Focus 5), a freed slot waking the
 * other process through NOTIFY, the dedicated LISTEN connection reconnecting after its backend is
 * terminated, runs recovered by the stall reconciler after the executing engine lost every
 * Postgres connection, and forged NOTIFY payloads (Review Focus 1 and 6).
 *
 * Backends are only ever terminated when they are attributable to an engine of this file: the
 * listener by `application_name` + its last statement (`LISTEN` on this file's schema's cancel
 * channel), the pool/leader of the victim engine by a unique `application_name` on an injected
 * `pg.Pool`. Other test files (running in parallel) and this file's own pool are never touched.
 */
import { randomUUID } from "node:crypto";
import { Client, type ClientConfig, escapeIdentifier, Pool } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { CommonConfig, EngineLogger, PostgresTransportConfig } from "../../src/config.js";
import { createEngine, type Engine, type RunEvent } from "../../src/engine.js";
import { channelName } from "../../src/pg/listener.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const shortId = () => randomUUID().slice(0, 8);
const uniqueQueue = () => `q${shortId()}`;
const uniqueTaskId = (label: string) => `${label}-${shortId()}`;
const t = (table: string) => `${escapeIdentifier(ns.schema)}.${escapeIdentifier(table)}`;

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
const extraPools: Pool[] = [];

function makeEngine(
	name: string,
	tasks: AnyTask[],
	overrides: Partial<CommonConfig> & Partial<Omit<PostgresTransportConfig, "transport">> = {}
): Engine {
	const engine = createEngine({
		transport: "postgres",
		postgres: ns.databaseUrl,
		schema: ns.schema,
		tasks,
		pool: { max: 20 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 5_000,
		shutdownTimeoutMs: 1_000,
		logger: loggerFor(name),
		...overrides,
	});
	engines.push(engine);
	return engine;
}

/** Resolves with the local time the first matching event fires on `engine`. */
function eventAt(
	engine: Engine,
	name: RunEvent["name"],
	runId: () => string | undefined
): Promise<number> {
	return new Promise(resolve => {
		const off = engine.events.on(name, event => {
			if (event.runId === runId()) {
				off();
				resolve(Date.now());
			}
		});
	});
}

async function runRow(id: string) {
	const { rows } = await ns.pool.query<{ status: string; attempt: number; output: unknown }>(
		`SELECT status, attempt, output FROM ${t("runs")} WHERE id = $1`,
		[id]
	);
	const row = rows[0];
	if (!row) throw new Error(`run ${id} not found`);
	return row;
}

async function waitForStatus(id: string, status: string, timeoutMs = 10_000) {
	await waitFor(async () => (await runRow(id)).status === status, { timeoutMs, intervalMs: 20 });
	return runRow(id);
}

/**
 * The last statement of this schema's LISTEN connection (wake, status, then cancel): other files'
 * listeners LISTEN on other schemas' channels (a long schema is hashed into the channel name).
 */
const listenStatement = () => `LISTEN ${escapeIdentifier(channelName("cancel", ns.schema))}`;

/** Pids of this schema's LISTEN connection(s). */
async function listenerPids(): Promise<number[]> {
	const { rows } = await ns.pool.query<{ pid: number }>(
		`SELECT pid FROM pg_stat_activity
		 WHERE application_name = 'minitrigger-listener' AND query = $1 AND pid <> pg_backend_pid()`,
		[listenStatement()]
	);
	return rows.map(r => r.pid);
}

beforeAll(async () => {
	const engine = createEngine({
		transport: "postgres",
		postgres: ns.databaseUrl,
		schema: ns.schema,
		tasks: [],
	});
	await engine.migrate();
	await engine.stop();
	await ns.pool.query(
		`CREATE TABLE ${t("test_executions")} (run_id uuid NOT NULL, execution_id uuid NOT NULL)`
	);
});

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(engines.splice(0).map(e => e.stop()));
	await Promise.all(extraPools.splice(0).map(p => p.end()));
	logs.length = 0;
});

afterAll(async () => {
	await Promise.all(extraPools.splice(0).map(p => p.end()));
});

describe("postgres transport: resilience and cross-process concurrency", () => {
	it("two engines on the same queue never execute the same run twice and never exceed globalConcurrency 3", async () => {
		const queue = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("shared"),
			queue: { name: queue, concurrency: 5, globalConcurrency: 3 },
			run: async (payload: { i: number }, ctx) => {
				// The real execution id of this attempt, as the claim wrote it.
				await ns.pool.query(
					`INSERT INTO ${t("test_executions")} (run_id, execution_id)
					 SELECT id, execution_id FROM ${t("runs")} WHERE id = $1`,
					[ctx.run.id]
				);
				await sleep(20);
				return payload.i;
			},
		});
		const a = makeEngine("a", [task]);
		const b = makeEngine("b", [task]);
		await Promise.all([a.start(), b.start()]);

		// Polled on a pool of its own: the peak must not be hidden behind the task's own inserts.
		const probe = new Pool({ connectionString: ns.databaseUrl, max: 1 });
		probe.on("error", () => {});
		extraPools.push(probe);
		let peak = 0;
		let polling = true;
		const poller = (async () => {
			while (polling) {
				const { rows } = await probe.query<{ n: number }>(
					`SELECT count(*)::int AS n FROM ${t("runs")} WHERE queue = $1 AND status = 'running'`,
					[queue]
				);
				peak = Math.max(peak, rows[0]?.n ?? 0);
				await sleep(5);
			}
		})();

		const ids: string[] = [];
		for (let i = 0; i < 200; i++) {
			// Alternate the triggering engine: both processes get wake-ups for the same queue.
			ids.push((await (i % 2 === 0 ? a : b).trigger(task, { i })).id);
		}
		try {
			await waitFor(
				async () => {
					const { rows } = await ns.pool.query<{ n: number }>(
						`SELECT count(*)::int AS n FROM ${t("runs")} WHERE queue = $1 AND status = 'completed'`,
						[queue]
					);
					return rows[0]?.n === 200;
				},
				{ timeoutMs: 60_000, intervalMs: 100 }
			);
		} finally {
			polling = false;
			await poller;
		}

		const { rows: runs } = await ns.pool.query<{ status: string; attempt: number }>(
			`SELECT status, attempt FROM ${t("runs")} WHERE queue = $1`,
			[queue]
		);
		expect(runs).toHaveLength(200);
		expect(runs.every(r => r.status === "completed" && r.attempt === 1)).toBe(true);

		const { rows: execs } = await ns.pool.query<{
			runs: number;
			executions: number;
			dupes: number;
		}>(
			`SELECT count(DISTINCT run_id)::int AS runs, count(*)::int AS executions,
			        (SELECT count(*)::int FROM (
			           SELECT run_id FROM ${t("test_executions")} WHERE run_id = ANY($1::uuid[])
			           GROUP BY run_id HAVING count(DISTINCT execution_id) > 1 OR count(*) > 1) d) AS dupes
			 FROM ${t("test_executions")} WHERE run_id = ANY($1::uuid[])`,
			[ids]
		);
		expect(execs[0]).toEqual({ runs: 200, executions: 200, dupes: 0 });
		// Review Focus 5: the advisory lock per queue makes the limit exact.
		expect(peak).toBeGreaterThan(0);
		expect(peak).toBeLessThanOrEqual(3);
		expect(logs.filter(l => l.level === "error")).toEqual([]);
	}, 90_000);

	it("a slot freed in one engine wakes the other without waiting for the poll", async () => {
		const queue = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("slot"),
			queue: { name: queue, concurrency: 1, globalConcurrency: 1 },
			run: async (payload: { ms: number }) => {
				await sleep(payload.ms);
				return payload.ms;
			},
		});
		// A poll far above the bound below: only the freed-slot NOTIFY can wake B in time.
		const polling = { intervalMs: 20_000 };
		const a = makeEngine("a", [task], { polling, shutdownTimeoutMs: 5_000 });
		const b = makeEngine("b", [task], { polling });
		await a.start();

		let r1: string | undefined;
		const r1Started = eventAt(a, "run:started", () => r1);
		// 1 s (the brief's 200 ms, widened): A must still be busy with r1 when `a.stop()` is called
		// below, even on a loaded CI machine, or A itself could take r2 once r1 completes.
		r1 = (await a.trigger(task, { ms: 1_000 })).id;
		await r1Started;
		// B starts only now: r1 is already running in A, so B can never take it.
		await b.start();

		let r2: string | undefined;
		const r2StartedOnB = eventAt(b, "run:started", () => r2);
		r2 = (await b.trigger(task, { ms: 1 })).id;
		// B's claim is blocked by the global limit; A stops claiming (graceful stop drains r1), so
		// the only engine that can take r2 is B, and only once A's completion frees the slot.
		await sleep(50);
		expect((await runRow(r2)).status).toBe("queued");
		const stopping = a.stop();
		const r1Row = await waitForStatus(r1, "completed");
		expect(r1Row.attempt).toBe(1);
		const { rows } = await ns.pool.query<{ ms: number }>(
			`SELECT (extract(epoch FROM clock_timestamp()) * 1000
			         - extract(epoch FROM updated_at) * 1000)::float8 AS ms
			 FROM ${t("runs")} WHERE id = $1`,
			[r1]
		);
		const completedAt = Date.now() - (rows[0]?.ms ?? 0);
		const startedOnB = await r2StartedOnB;
		await stopping;

		expect(startedOnB - completedAt).toBeLessThan(3_000);
		await waitForStatus(r2, "completed");
	}, 30_000);

	it("listener backend terminated: reconnects and NOTIFY works again", async () => {
		const task = defineTask({
			id: uniqueTaskId("listen"),
			queue: { name: uniqueQueue() },
			run: async (payload: { v: number }) => payload.v,
		});
		// A 20 s poll: a pickup within the bound below can only come from the NOTIFY.
		const engine = makeEngine("listener", [task], { polling: { intervalMs: 20_000 } });
		await engine.start();

		const before = await listenerPids();
		expect(before).toHaveLength(1);
		const { rows } = await ns.pool.query<{ ok: boolean }>("SELECT pg_terminate_backend($1) AS ok", [
			before[0],
		]);
		expect(rows[0]?.ok).toBe(true);

		// Reconnected (a new backend): polled, not a fixed sleep.
		const after = await waitFor(
			async () => {
				const pids = await listenerPids();
				return pids.length === 1 && pids[0] !== before[0] ? pids : undefined;
			},
			{ timeoutMs: 10_000, intervalMs: 50 }
		);
		if (!after) throw new Error("unreachable");
		expect(after).toHaveLength(1);
		expect(after[0]).not.toBe(before[0]);

		let runId: string | undefined;
		const started = eventAt(engine, "run:started", () => runId);
		const triggeredAt = Date.now();
		runId = (await engine.trigger(task, { v: 7 })).id;
		const startedAt = await started;
		expect(startedAt - triggeredAt).toBeLessThan(3_000);
		expect((await waitForStatus(runId, "completed")).output).toBe(7);
		// The drop itself is a (throttled) warn; nothing escalated to an error.
		expect(logs.filter(l => l.level === "error")).toEqual([]);
	}, 20_000);

	it("Postgres connections dropped during execution: run goes lost and is recovered by the stall reconciler", async () => {
		const appName = `mt_victim_${shortId()}`;
		// While `blocked`, every new connection of the victim's pool fails (Postgres "down" for that
		// process): terminating its backends alone is not enough, the pool reconnects and the
		// heartbeat succeeds within milliseconds.
		let blocked = false;
		class BlockableClient extends Client {
			constructor(config?: string | ClientConfig) {
				super(config);
				// pg-pool connects with a callback; an instance property shadows the typed overloads.
				const connect = this.connect.bind(this) as (callback: (err?: Error) => void) => void;
				Object.assign(this, {
					connect: (callback: (err?: Error) => void) => {
						if (!blocked) return connect(callback);
						process.nextTick(() =>
							callback(new Error("test: Postgres unreachable for the victim"))
						);
					},
				});
			}
		}
		const victimPool = new Pool({
			connectionString: ns.databaseUrl,
			max: 20,
			application_name: appName,
			connectionTimeoutMillis: 2_000,
			Client: BlockableClient,
		});
		// Terminated idle clients emit "error" on the pool; an injected pool is ours to listen on.
		victimPool.on("error", () => {});
		extraPools.push(victimPool);

		const queue = uniqueQueue();
		const executions: number[] = [];
		const task = defineTask({
			id: uniqueTaskId("lost"),
			queue: { name: queue },
			retry: { maxAttempts: 3, backoff: { type: "fixed", baseMs: 10, jitter: false } },
			run: async (_payload: undefined, ctx) => {
				executions.push(ctx.run.attempt);
				if (ctx.run.attempt === 1) await sleep(3_000);
				return ctx.run.attempt;
			},
		});
		const common = {
			stallTimeoutMs: 2_000,
			reconciler: { intervalMs: 200, graceMs: 200, batchSize: 50 },
		};
		// The victim executes attempt 1; it is the only engine running when the run starts.
		const victim = makeEngine("victim", [task], {
			...common,
			postgres: { pool: victimPool },
		});
		await victim.start();

		let runId: string | undefined;
		const started = eventAt(victim, "run:started", () => runId);
		runId = (await victim.trigger(task)).id;
		await started;
		// The rescuer (its own pool) becomes leader once the victim's leader connection dies.
		const rescuer = makeEngine("rescuer", [task], common);
		const rescuerRetrying: string[] = [];
		rescuer.events.on("run:retrying", event => {
			rescuerRetrying.push(event.runId);
		});
		await rescuer.start();

		// Drop every connection of the victim: its pool can no longer reconnect, and its dedicated
		// clients (leader, LISTEN: plain `pg.Client`s built from the pool's options) are terminated
		// again whenever they reconnect, until the stall reconciler (the rescuer, leader once the
		// victim's leader client died) moved the run to its next attempt.
		const id = runId;
		blocked = true;
		const deadline = Date.now() + 15_000;
		let terminated = 0;
		try {
			while (Date.now() < deadline) {
				const { rows } = await ns.pool.query<{ n: number }>(
					`SELECT count(pg_terminate_backend(pid))::int AS n FROM pg_stat_activity
					 WHERE pid <> pg_backend_pid()
					   AND (application_name = $1
					        OR (application_name = 'minitrigger-listener' AND query = $2))`,
					[appName, listenStatement()]
				);
				terminated += rows[0]?.n ?? 0;
				if ((await runRow(id)).attempt >= 2) break;
				await sleep(50);
			}
		} finally {
			blocked = false;
		}
		expect(terminated).toBeGreaterThan(0);
		expect((await runRow(id)).attempt).toBe(2);
		// Recovered by the rescuer's reconciler (stall case), the only engine that could reach Postgres.
		expect(rescuerRetrying).toContain(id);

		const row = await waitForStatus(id, "completed", 15_000);
		expect(row.attempt).toBe(2);
		expect(row.output).toBe(2);
		// Attempt 1's late result (after its 3 s sleep) is dropped by the ownership check.
		await sleep(3_500);
		expect(await runRow(id)).toMatchObject({ status: "completed", attempt: 2, output: 2 });
		expect(executions).toEqual([1, 2]);
	}, 45_000);

	it("forged NOTIFY payloads on wake/status/cancel channels are harmless", async () => {
		const task = defineTask({
			id: uniqueTaskId("forged"),
			queue: { name: uniqueQueue() },
			run: async (payload: { v: number }) => payload.v,
		});
		const engine = makeEngine("forged", [task], { polling: { intervalMs: 5_000 } });
		await engine.start();

		for (const [kind, payload] of [
			["wake", "nao-existe"],
			["wake", ""],
			["status", "zzz"],
			["status", ""],
			["cancel", randomUUID()],
			["cancel", "not-a-uuid"],
			["cancel", ""],
		] as const) {
			await ns.pool.query("SELECT pg_notify($1::text, $2::text)", [
				channelName(kind, ns.schema),
				payload,
			]);
		}
		await sleep(200);

		let runId: string | undefined;
		const started = eventAt(engine, "run:started", () => runId);
		const triggeredAt = Date.now();
		runId = (await engine.trigger(task, { v: 1 })).id;
		expect(await started).toBeLessThan(triggeredAt + 1_000);
		expect((await waitForStatus(runId, "completed")).output).toBe(1);
		// Review Focus 1: no warn/error for any forged payload.
		expect(logs.filter(l => l.level === "error" || l.level === "warn")).toEqual([]);
	}, 20_000);

	it("a forged cancel NOTIFY with the id of a running run does not abort it", async () => {
		const aborted: string[] = [];
		const task = defineTask({
			id: uniqueTaskId("forged-cancel"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 1 },
			run: async (payload: { ms: number }, ctx) => {
				let done = false;
				// Only an abort while the task is still running counts ("finished" fires at the end).
				ctx.signal.addEventListener("abort", () => {
					if (!done) aborted.push(ctx.run.id);
				});
				await sleep(payload.ms);
				done = true;
				return "done";
			},
		});
		const engine = makeEngine("runner", [task]);
		await engine.start();
		const cancelChannel = channelName("cancel", ns.schema);

		// Counts the engine's cancel re-read (`store.runs.get`, a pool query by id) of one run: the
		// proof that the forged NOTIFY was received and processed, not merely sent.
		let rereads = 0;
		let watchedId: string | undefined;
		const originalQuery = Pool.prototype.query;
		vi.spyOn(Pool.prototype, "query").mockImplementation(function (this: Pool, ...args: unknown[]) {
			const [text, values] = args;
			if (
				this !== ns.pool &&
				watchedId !== undefined &&
				typeof text === "string" &&
				/FROM\s+\S*runs\S*\s+WHERE id = \$1\s*$/.test(text) &&
				Array.isArray(values) &&
				values[0] === watchedId
			) {
				rereads++;
			}
			return (originalQuery as (...a: unknown[]) => unknown).apply(this, args);
		} as typeof Pool.prototype.query);

		// Forged: a raw NOTIFY with the id of a run really executing here.
		let forgedId: string | undefined;
		const forgedStarted = eventAt(engine, "run:started", () => forgedId);
		forgedId = (await engine.trigger(task, { ms: 3_000 })).id;
		await forgedStarted;
		watchedId = forgedId;
		await ns.pool.query("SELECT pg_notify($1::text, $2::text)", [cancelChannel, forgedId]);
		// The re-read happened while the run was still executing, and it did not abort it.
		await waitFor(() => rereads > 0, { timeoutMs: 2_000, intervalMs: 10 });
		expect((await runRow(forgedId)).status).toBe("running");
		expect(aborted).toEqual([]);
		const forged = await waitForStatus(forgedId, "completed");
		expect(forged).toMatchObject({ attempt: 1, output: "done" });
		expect(aborted).toEqual([]);

		// Control: a real cancel from another (trigger-only) engine aborts the execution.
		const other = makeEngine("other", [task]);
		let realId: string | undefined;
		const realStarted = eventAt(engine, "run:started", () => realId);
		realId = (await engine.trigger(task, { ms: 5_000 })).id;
		await realStarted;
		await expect(other.runs.cancel(realId)).resolves.toBe("canceled");
		const id = realId;
		await waitFor(() => aborted.includes(id), { timeoutMs: 3_000, intervalMs: 20 });
		expect((await runRow(id)).status).toBe("canceled");
		expect(aborted).toEqual([id]);
		expect(logs.filter(l => l.level === "error")).toEqual([]);
	}, 20_000);
});
