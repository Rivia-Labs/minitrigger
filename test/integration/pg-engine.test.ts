/**
 * The Postgres-only transport end to end (`transport: "postgres"`, decision 28; Task 10b): the
 * backend wired into `createEngine`, with no Redis at all. Latency cases use a long
 * `polling.intervalMs` wherever a pass must prove the NOTIFY path (a poll would be too late).
 */
import { randomUUID } from "node:crypto";
import { Client, escapeIdentifier, Pool } from "pg";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { CommonConfig, EngineLogger, PostgresTransportConfig } from "../../src/config.js";
import { createEngine, type Engine, type RunEvent } from "../../src/engine.js";
import { ConfigError, MinitriggerError } from "../../src/errors.js";
import { channelName } from "../../src/pg/listener.js";
import { createNotifyBatcher } from "../../src/pg/notify-batcher.js";
import { createPgTransport } from "../../src/pg/transport.js";
import { createReconciler } from "../../src/reconciler.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { type TestNamespace, useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
/** A second schema, for the mixed-mode case in the other direction (BullMQ first). */
const ns2 = useTestNamespace();

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const shortId = () => randomUUID().slice(0, 8);
const uniqueQueue = () => `q${shortId()}`;
const uniqueTaskId = (label: string) => `${label}-${shortId()}`;
const t = (table: string, namespace: TestNamespace = ns) =>
	`${escapeIdentifier(namespace.schema)}.${escapeIdentifier(table)}`;

type LogRecord = { level: string; message: string; fields: Record<string, unknown> };
const logs: LogRecord[] = [];
const testLogger: EngineLogger = {
	debug: (fields, message) => logs.push({ level: "debug", message, fields }),
	info: (fields, message) => logs.push({ level: "info", message, fields }),
	warn: (fields, message) => logs.push({ level: "warn", message, fields }),
	error: (fields, message) => logs.push({ level: "error", message, fields }),
};

const engines: Engine[] = [];

function makeEngine(
	tasks: AnyTask[],
	overrides: Partial<CommonConfig> & Partial<Omit<PostgresTransportConfig, "transport">> = {},
	namespace: TestNamespace = ns
): Engine {
	const engine = createEngine({
		transport: "postgres",
		postgres: namespace.databaseUrl,
		schema: namespace.schema,
		tasks,
		pool: { max: 20 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 5_000,
		shutdownTimeoutMs: 1_000,
		logger: testLogger,
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

async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 8_000) {
	await waitFor(async () => (await engine.runs.get(id))?.status === status, {
		timeoutMs,
		intervalMs: 20,
	});
	const run = await engine.runs.get(id);
	if (!run) throw new Error("unreachable");
	return run;
}

async function countRuns(taskId: string): Promise<number> {
	const { rows } = await ns.pool.query<{ n: string }>(
		`SELECT count(*) AS n FROM ${t("runs")} WHERE task_id = $1`,
		[taskId]
	);
	return Number(rows[0]?.n);
}

async function tickRow(scheduleId: string) {
	const { rows } = await ns.pool.query<{
		run_after: Date;
		attempts: number;
		locked_by: string | null;
		data: { prevMillis: number };
		future: boolean;
	}>(
		`SELECT run_after, attempts, locked_by, data, run_after > now() AS future
		 FROM ${t("system_jobs")} WHERE job_id = $1`,
		[`schedule-tick:${scheduleId}`]
	);
	return rows[0];
}

beforeAll(async () => {
	for (const namespace of [ns, ns2]) {
		const engine = createEngine({
			transport: "postgres",
			postgres: namespace.databaseUrl,
			schema: namespace.schema,
			tasks: [],
		});
		await engine.migrate();
		await engine.stop();
	}
});

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(engines.splice(0).map(e => e.stop()));
	// No schedule tick may outlive its test (a later engine would claim it without the task).
	await ns.pool.query(`DELETE FROM ${t("system_jobs")}`);
	await ns.pool.query(`DELETE FROM ${t("schedules")}`);
	logs.length = 0;
});

describe("engine with transport postgres", () => {
	it("runs a task end to end without Redis (trigger, steps, sleep, token, child)", async () => {
		const queue = uniqueQueue();
		const tokens = new Map<string, string>();
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: queue },
			run: async (payload: { n: number }) => payload.n + 1,
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (payload: { n: number }, ctx) => {
				const doubled = await ctx.step.run("double", () => payload.n * 2);
				await ctx.step.sleep("nap", "200ms");
				const token = await ctx.step.createToken("approval");
				tokens.set(ctx.run.id, token.id);
				const approval = await ctx.step.waitForToken<{ ok: boolean }>(token);
				const result = await ctx.step.triggerAndWait("child", child, { n: doubled });
				return {
					doubled,
					approval: approval.ok ? approval.output : null,
					child: result.ok ? result.output : null,
				};
			},
		});
		const engine = makeEngine([parent, child]);
		await engine.start();

		const handle = await engine.trigger(parent, { n: 5 });
		await waitForStatus(engine, handle.id, "waiting");
		const tokenId = (await waitFor(() => tokens.get(handle.id))) as string;
		await expect(engine.tokens.complete(tokenId, { ok: true })).resolves.toEqual({
			completed: true,
		});
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ doubled: 10, approval: { ok: true }, child: 11 });
		// The NOTIFY self-test passed: no warn about notifications at all.
		expect(logs.filter(l => l.level === "warn" || l.level === "error")).toEqual([]);
	});

	it("trigger(..., { tx }) is claimable right after commit (outbox_pending false, wake NOTIFY in the insert)", async () => {
		const task = defineTask({
			id: uniqueTaskId("tx"),
			queue: { name: uniqueQueue() },
			run: async (payload: { v: number }) => payload.v,
		});
		// A poll interval far above the bound below: only the insert's NOTIFY can wake the worker.
		const engine = makeEngine([task], { polling: { intervalMs: 5_000 } });
		await engine.start();

		let runId: string | undefined;
		const started = eventAt(engine, "run:started", () => runId);
		const client = await ns.pool.connect();
		let committedAt: number;
		try {
			await client.query("BEGIN");
			runId = (await engine.trigger(task, { v: 3 }, { tx: client })).id;
			const before = await ns.pool.query(`SELECT 1 FROM ${t("runs")} WHERE id = $1`, [runId]);
			expect(before.rowCount).toBe(0);
			await client.query("COMMIT");
			committedAt = Date.now();
		} finally {
			client.release();
		}
		const startedAt = await started;
		expect(startedAt - committedAt).toBeLessThan(1_000);
		if (runId === undefined) throw new Error("unreachable");
		const { rows } = await ns.pool.query<{ outbox_pending: boolean }>(
			`SELECT outbox_pending FROM ${t("runs")} WHERE id = $1`,
			[runId]
		);
		expect(rows[0]?.outbox_pending).toBe(false);
		await waitForStatus(engine, runId, "completed");
	});

	it("wake latency: a trigger is picked up quickly with LISTEN, and within polling.intervalMs + jitter when NOTIFY is broken", async () => {
		const queue = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("wake"),
			queue: { name: queue },
			run: async () => "ok",
		});

		// 1) With LISTEN (and a 5 s poll, so only the NOTIFY can explain a fast pickup).
		const fast = makeEngine([task], { polling: { intervalMs: 5_000 } });
		await fast.start();
		let first: string | undefined;
		const firstStarted = eventAt(fast, "run:started", () => first);
		const t0 = Date.now();
		first = (await fast.trigger(task)).id;
		expect((await firstStarted) - t0).toBeLessThan(1_000);
		await fast.stop();

		// 2) Listener killed: the trigger's NOTIFY is lost; the 1 s poll (or the reconnect's
		// wake-all) still picks the run up.
		const engine = makeEngine([task], { polling: { intervalMs: 1_000 } });
		await engine.start();
		const killed = await ns.pool.query<{ ok: boolean }>(
			`SELECT pg_terminate_backend(pid) AS ok FROM pg_stat_activity
			 WHERE application_name = 'minitrigger-listener' AND query LIKE $1`,
			// Its last statement LISTENed on this schema's cancel channel (the last one it LISTENs).
			[`%${channelName("cancel", ns.schema)}%`]
		);
		expect(killed.rows.length).toBe(1);
		let second: string | undefined;
		const secondStarted = eventAt(engine, "run:started", () => second);
		const t1 = Date.now();
		second = (await engine.trigger(task)).id;
		expect((await secondStarted) - t1).toBeLessThan(1_500);
	});

	it("a delayed run scheduled by another engine is picked up on time, not a poll interval later", async () => {
		const task = defineTask({
			id: uniqueTaskId("delayed"),
			queue: { name: uniqueQueue() },
			run: async () => "ok",
		});
		const worker = makeEngine([task], { polling: { intervalMs: 5_000 } });
		await worker.start();
		// Let the worker's first (empty) claim settle into its 5 s sleep.
		await sleep(100);
		const producer = makeEngine([task]); // Never started: trigger only.

		let runId: string | undefined;
		const started = eventAt(worker, "run:started", () => runId);
		const t0 = Date.now();
		runId = (await producer.trigger(task, undefined, { delay: 300 })).id;
		const elapsed = (await started) - t0;
		expect(elapsed).toBeGreaterThanOrEqual(250);
		expect(elapsed).toBeLessThan(1_500);
	});

	it("stop() hands back a running run with a NOTIFY and a second engine picks it up within 1 s", async () => {
		let executions = 0;
		const task = defineTask({
			id: uniqueTaskId("handback"),
			queue: { name: uniqueQueue() },
			run: async (_payload: undefined, ctx) => {
				executions++;
				if (executions === 1) {
					// Ignores nothing: ends only when stop() aborts it (hand-back).
					await new Promise<void>(resolve =>
						ctx.signal.addEventListener("abort", () => resolve(), { once: true })
					);
					return "first";
				}
				return "second";
			},
		});
		const a = makeEngine([task], { shutdownTimeoutMs: 300 });
		await a.start();
		const handle = await a.trigger(task);
		await waitForStatus(a, handle.id, "running");

		// B only polls every 5 s: a pickup within 1 s can only come from the hand-back's NOTIFY.
		const b = makeEngine([task], { polling: { intervalMs: 5_000 } });
		await b.start();
		await sleep(100);

		const handedBack = eventAt(a, "run:queued", () => handle.id);
		const startedOnB = eventAt(b, "run:started", () => handle.id);
		await a.stop();
		const gap = (await startedOnB) - (await handedBack);
		expect(gap).toBeLessThan(1_000);
		const run = await waitForStatus(b, handle.id, "completed");
		expect(run.output).toBe("second");
		expect(run.attempt).toBe(1);
	});

	it("refuses to start in bullmq mode on a schema already used in postgres mode (and vice versa)", async () => {
		const pgEngine = makeEngine([]);
		await pgEngine.start();
		const bullmqOnPg = createEngine({
			postgres: ns.databaseUrl,
			redis: ns.redisUrl,
			schema: ns.schema,
			prefix: ns.prefix,
			tasks: [],
			logger: testLogger,
		});
		engines.push(bullmqOnPg);
		const error = await bullmqOnPg.start().catch((e: unknown) => e);
		expect(error).toBeInstanceOf(ConfigError);
		expect((error as Error).message).toMatch(/used by engines with transport "postgres"/);

		// The other direction, on a fresh schema: BullMQ first.
		const bullmqEngine = createEngine({
			postgres: ns2.databaseUrl,
			redis: ns2.redisUrl,
			schema: ns2.schema,
			prefix: ns2.prefix,
			tasks: [],
			logger: testLogger,
		});
		engines.push(bullmqEngine);
		await bullmqEngine.start();
		const pgOnBullmq = makeEngine([], {}, ns2);
		const error2 = await pgOnBullmq.start().catch((e: unknown) => e);
		expect(error2).toBeInstanceOf(ConfigError);
		expect((error2 as Error).message).toMatch(/used by engines with transport "bullmq"/);
	});

	it("LISTEN connection timeout at start() is a MinitriggerError, not a ConfigError, and the engine can start again", async () => {
		const realConnect = Client.prototype.connect;
		// Only the LISTEN client's connect hangs (an unreachable host for that connection alone).
		const hangingForListener = function (this: Client, ...args: unknown[]): unknown {
			const params = (this as unknown as { connectionParameters?: { application_name?: string } })
				.connectionParameters;
			if (params?.application_name === "minitrigger-listener") return new Promise(() => {});
			return (realConnect as unknown as (...a: unknown[]) => unknown).apply(this, args);
		};
		const spy = vi
			.spyOn(Client.prototype, "connect")
			.mockImplementation(hangingForListener as unknown as typeof Client.prototype.connect);
		const task = defineTask({
			id: uniqueTaskId("listen-timeout"),
			queue: { name: uniqueQueue() },
			run: async () => "ok",
		});
		const engine = makeEngine([task]);
		const t0 = Date.now();
		const error = await engine.start().catch((e: unknown) => e);
		expect(Date.now() - t0).toBeLessThan(8_000);
		expect(error).toBeInstanceOf(MinitriggerError);
		expect(error).not.toBeInstanceOf(ConfigError);
		expect((error as Error).message).toMatch(/LISTEN/);

		spy.mockRestore();
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "completed");
	}, 25_000);

	it("a schedule ticks every second and survives a leader restart", async () => {
		const task = defineTask({
			id: uniqueTaskId("every-second"),
			queue: { name: uniqueQueue() },
			schedule: { cron: "* * * * * *" },
			run: async () => "tick",
		});
		const a = makeEngine([task]);
		await a.start();
		await waitFor(async () => (await countRuns(task.id)) >= 2, { timeoutMs: 12_000 });
		await a.stop();
		const afterA = await countRuns(task.id);

		const b = makeEngine([task]);
		await b.start();
		await waitFor(async () => (await countRuns(task.id)) >= afterA + 2, { timeoutMs: 12_000 });
		await b.schedules.delete(task.id);

		// One run per tick timestamp: every run has its own `__schedule:id:ts` key.
		const { rows } = await ns.pool.query<{ key: string }>(
			`SELECT key FROM ${t("idempotency_keys")} WHERE task_id = $1 AND key LIKE $2`,
			[task.id, `__schedule:${task.id}:%`]
		);
		const total = await countRuns(task.id);
		expect(rows.length).toBe(total);
		expect(new Set(rows.map(r => r.key)).size).toBe(total);
		for (const { key } of rows) {
			expect(Number(key.split(":").at(-1)) % 1_000).toBe(0);
		}
	}, 30_000);

	it("restarting the engine does not skip a due schedule tick", async () => {
		const task = defineTask({
			id: uniqueTaskId("due-tick"),
			queue: { name: uniqueQueue() },
			schedule: { cron: "0 0 3 * * *" }, // Daily: never fires on its own during the test.
			run: async () => "tick",
		});
		const a = makeEngine([task]);
		await a.start();
		await a.stop();
		// A tick that came due while no engine was running.
		const dueMs = (Math.floor(Date.now() / 1_000) - 5) * 1_000;
		await ns.pool.query(
			`UPDATE ${t("system_jobs")}
			 SET run_after = now() - interval '2 seconds',
			     data = jsonb_set(data, '{prevMillis}', to_jsonb($2::bigint))
			 WHERE job_id = $1`,
			[`schedule-tick:${task.id}`, dueMs]
		);

		const b = makeEngine([task]);
		await b.start(); // Its declarative sync must not move the due tick forward.
		await waitFor(
			async () => {
				const { rows } = await ns.pool.query(
					`SELECT 1 FROM ${t("idempotency_keys")} WHERE task_id = $1 AND key = $2`,
					[task.id, `__schedule:${task.id}:${dueMs}`]
				);
				return rows.length === 1;
			},
			{ timeoutMs: 15_000 }
		);
		expect(await countRuns(task.id)).toBe(1);
	});

	it("schedule-tick claimed by a process that dies is reprocessed once the lock expires, without a duplicate run", async () => {
		const task = defineTask({
			id: uniqueTaskId("dead-claim"),
			queue: { name: uniqueQueue() },
			schedule: { cron: "0 0 3 * * *" },
			run: async () => "tick",
		});
		const engine = makeEngine([task], { polling: { intervalMs: 200 } });
		await engine.start();
		await waitFor(() => tickRow(task.id), { timeoutMs: 15_000 });
		const tickMs = (Math.floor(Date.now() / 1_000) - 5) * 1_000;

		// The dead process: claimed the tick (lock for 300 ms) and created its run, then died
		// before rewriting the row.
		const store = createSqlStore(ns.pool, ns.schema);
		await store.runs.create({
			id: randomUUID(),
			taskId: task.id,
			queue: task.options.queue?.name ?? "",
			payload: null,
			maxAttempts: 3,
			delayMs: 0,
			idempotency: { key: `__schedule:${task.id}:${tickMs}`, ttlMs: null },
		});
		const deadToken = randomUUID();
		await ns.pool.query(
			`UPDATE ${t("system_jobs")}
			 SET run_after = now() - interval '1 second',
			     locked_until = now() + interval '300 milliseconds', locked_by = $2::uuid,
			     data = jsonb_set(data, '{prevMillis}', to_jsonb($3::bigint))
			 WHERE job_id = $1`,
			[`schedule-tick:${task.id}`, deadToken, tickMs]
		);

		// After the lock expires, a worker reprocesses it and moves it to the next occurrence.
		await waitFor(
			async () => {
				const row = await tickRow(task.id);
				return row?.future === true && row.locked_by === null;
			},
			{ timeoutMs: 15_000 }
		);
		const row = await tickRow(task.id);
		expect(row?.attempts).toBe(0);
		expect(row?.data.prevMillis).not.toBe(tickMs);
		expect(await countRuns(task.id)).toBe(1);
	});

	it("a schedule-tick that exhausts its attempts moves to the next occurrence instead of disappearing", async () => {
		const scheduled = defineTask({
			id: uniqueTaskId("unregistered"),
			queue: { name: uniqueQueue() },
			run: async () => "never",
		});
		const other = defineTask({
			id: uniqueTaskId("other"),
			queue: { name: uniqueQueue() },
			run: async () => "x",
		});
		// A trigger-only engine that knows the task writes the schedule...
		const writer = makeEngine([scheduled]);
		const scheduleId = `sched-${shortId()}`;
		await writer.schedules.upsert({ id: scheduleId, task: scheduled, cron: "0 0 3 * * *" });
		// ...and a started engine that does not know it processes its ticks.
		const engine = makeEngine([other], { polling: { intervalMs: 100 } });
		await engine.start();
		await ns.pool.query(`UPDATE ${t("system_jobs")} SET run_after = now() WHERE job_id = $1`, [
			`schedule-tick:${scheduleId}`,
		]);

		// The backoff is shortened by hand: every retried attempt is made due again at once.
		const permanent = () =>
			logs.some(l => l.message.startsWith("schedule tick failed permanently"));
		await waitFor(
			async () => {
				if (permanent()) return true;
				await ns.pool.query(
					`UPDATE ${t("system_jobs")} SET run_after = now()
					 WHERE job_id = $1 AND attempts > 0 AND locked_by IS NULL`,
					[`schedule-tick:${scheduleId}`]
				);
				return false;
			},
			{ timeoutMs: 15_000 }
		);
		// The "failed permanently" line is logged before the row is rewritten: wait for the write.
		await waitFor(
			async () => {
				const r = await tickRow(scheduleId);
				return r?.future === true && r.locked_by === null;
			},
			{ timeoutMs: 15_000 }
		);
		const row = await tickRow(scheduleId);
		expect(row).toBeDefined();
		expect(row?.future).toBe(true);
		expect(row?.attempts).toBe(0);
		expect(row?.locked_by).toBeNull();
		expect(await countRuns(scheduled.id)).toBe(0);
	}, 30_000);

	it("waitpoint expiry fires from system_jobs and cancels the child", async () => {
		const queue = uniqueQueue();
		const child = defineTask({
			id: uniqueTaskId("slow-child"),
			queue: { name: queue },
			run: async (_payload: undefined, ctx) => {
				await new Promise<void>(resolve => {
					const timer = setTimeout(resolve, 5_000);
					ctx.signal.addEventListener("abort", () => {
						clearTimeout(timer);
						resolve();
					});
				});
				return "late";
			},
		});
		const parent = defineTask({
			id: uniqueTaskId("impatient"),
			queue: { name: queue },
			run: async (_payload: undefined, ctx) => {
				const result = await ctx.step.triggerAndWait("child", child, undefined, {
					timeout: "300ms",
				});
				return result.ok ? "ok" : result.error.name;
			},
		});
		const engine = makeEngine([parent, child]);
		await engine.start();
		const t0 = Date.now();
		const handle = await engine.trigger(parent);
		const run = await waitForStatus(engine, handle.id, "completed", 4_000);
		expect(run.output).toBe("Timeout");
		expect(Date.now() - t0).toBeLessThan(3_000);
		const { items } = await engine.runs.list({ taskId: child.id });
		expect(items.map(r => r.status)).toEqual(["canceled"]);
	});

	it("retention runs from system_jobs", async () => {
		const task = defineTask({
			id: uniqueTaskId("retained"),
			queue: { name: uniqueQueue() },
			run: async () => "done",
		});
		const producer = makeEngine([task]);
		const old = await producer.trigger(task);
		await ns.pool.query(
			`UPDATE ${t("runs")} SET status = 'completed', run_after = NULL,
			   finished_at = now() - interval '3 days' WHERE id = $1`,
			[old.id]
		);
		const engine = makeEngine([task], {
			idempotencyKeyTtl: "1d",
			retention: { days: 1, intervalMs: 500 },
			reconciler: { intervalMs: 200 },
		});
		await engine.start();
		await waitFor(async () => (await engine.runs.get(old.id)) === null, { timeoutMs: 5_000 });
		const { rows } = await ns.pool.query(
			`SELECT 1 FROM ${t("system_jobs")} WHERE name = 'retention' AND locked_by IS NOT NULL`
		);
		expect(rows.length).toBe(0);
	});

	it("ctx.stream, runs.readStream and queue.rateLimit are ConfigError", async () => {
		expect(() =>
			makeEngine([
				defineTask({
					id: uniqueTaskId("limited"),
					queue: { name: uniqueQueue(), rateLimit: { max: 1, durationMs: 1_000 } },
					run: async () => "x",
				}),
			])
		).toThrow(ConfigError);
		engines.pop();

		const task = defineTask({
			id: uniqueTaskId("streamer"),
			queue: { name: uniqueQueue() },
			run: async (_payload: undefined, ctx) => {
				async function* source() {
					yield "a";
				}
				try {
					await ctx.stream("out", source());
					return "streamed";
				} catch (error) {
					return error instanceof ConfigError ? "ConfigError" : String(error);
				}
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		// Review 5 item B5: fatal (like an invalid step argument), even though the task caught it.
		const run = await waitForStatus(engine, handle.id, "failed");
		expect(run.error?.name).toBe("ConfigError");
		expect(run.attempt).toBe(1);
		expect(() => engine.runs.readStream(handle.id, "out")).toThrow(ConfigError);
	});

	it("reconciler skips cases 1/2 (backlog is not a lost enqueue)", async () => {
		const queue = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("backlog"),
			queue: { name: queue },
			run: async () => "x",
		});
		const producer = makeEngine([task]);
		const ids: string[] = [];
		for (let i = 0; i < 5; i++) ids.push((await producer.trigger(task)).id);
		await ns.pool.query(
			`UPDATE ${t("runs")} SET run_after = now() - interval '1 hour' WHERE id = ANY($1::uuid[])`,
			[ids]
		);

		// The engine's own leader tick, with no worker for this queue.
		const engine = makeEngine([task], { reconciler: { intervalMs: 200, graceMs: 1_000 } });
		await engine.start({ queues: [] });
		await waitFor(() => logs.some(l => l.message.startsWith("this engine is now the leader")));
		await sleep(600); // A few more leader ticks.
		expect(logs.filter(l => l.level === "warn" || l.level === "error")).toEqual([]);
		expect(logs.some(l => l.message === "reconciler tick")).toBe(false);

		// The same reconciler configuration, called directly for its report.
		const store = createSqlStore(ns.pool, ns.schema);
		const transport = createPgTransport({
			store,
			batcher: createNotifyBatcher({ db: ns.pool, logger: testLogger }),
			schema: ns.schema,
			queues: [],
			systemConcurrency: 1,
			stallTimeoutMs: 5_000,
			polling: { intervalMs: 1_000, jitterMs: 0 },
			wake: { subscribe: () => () => {} },
			logger: testLogger,
		});
		const report = await createReconciler({
			store,
			transport,
			logger: testLogger,
			graceMs: 1_000,
			stallTimeoutMs: 5_000,
			batchSize: 100,
			recoverLostEnqueues: false,
		}).runOnce();
		expect(report.staleQueued).toBe(0);
		expect(report.staleWaiting).toBe(0);
		expect(report.skippedLostEnqueues).toBe(true);
		for (const id of ids) expect((await engine.runs.get(id))?.status).toBe("queued");
	});

	it("a claim stuck on a saturated pool when stop() gives up never claims afterwards", async () => {
		const pool = new Pool({ connectionString: ns.databaseUrl, max: 1 });
		try {
			const task = defineTask({
				id: uniqueTaskId("saturated"),
				queue: { name: uniqueQueue() },
				run: async () => "done",
			});
			// Long poll and reconciler intervals: after start(), only the wake of the trigger below
			// makes anything ask this pool for a connection, so the queue's claim is first in line.
			const engine = makeEngine([task], {
				postgres: { pool },
				polling: { intervalMs: 60_000 },
				reconciler: { intervalMs: 60_000 },
			});
			await engine.start();
			await sleep(300); // the startup passes (first claims, first leader tick) are over
			const held = await pool.connect();
			const trigger = makeEngine([task]); // its own pool: the insert and wake are not blocked
			const handle = await trigger.trigger(task, undefined);
			await sleep(500); // the wake reached the listener; the claim waits for a checkout

			await engine.stop(); // gives up on the claim after its drain bound
			// The engine's pool is being ended (as for a pool the engine owns): the claim gets the
			// connection back, and the hand-back of anything it claimed would find the pool gone.
			held.release();
			const ending = pool.end();
			await sleep(1_000);
			const { rows } = await ns.pool.query<{ status: string }>(
				`SELECT status FROM ${t("runs")} WHERE id = $1`,
				[handle.id]
			);
			expect(rows[0]?.status).toBe("queued");
			await ending;
		} finally {
			if (!pool.ended) await pool.end().catch(() => {});
		}
	}, 30_000);

	it("a claimed run whose concurrency re-check throws is released (no attempt spent) and then completes", async () => {
		const task = defineTask({
			id: uniqueTaskId("recheck-throws"),
			queue: { name: uniqueQueue(), concurrencyKey: { limit: 1 } },
			retry: { maxAttempts: 1 },
			run: async () => "done",
		});
		let injected = 0;
		const originalQuery = Client.prototype.query;
		vi.spyOn(Client.prototype, "query").mockImplementation(function (
			this: Client,
			...args: unknown[]
		) {
			const [text] = args;
			if (
				injected === 0 &&
				typeof text === "string" &&
				text.includes("SELECT queue, concurrency_key, concurrency_waits FROM")
			) {
				injected++;
				return Promise.reject(new Error("boom: injected re-check failure"));
			}
			return (originalQuery as (...a: unknown[]) => unknown).apply(this, args);
		} as typeof Client.prototype.query);
		const engine = makeEngine([task], { polling: { intervalMs: 200 } });
		const events: RunEvent[] = [];
		for (const name of ["run:queued", "run:started", "run:completed", "run:failed"] as const) {
			engine.events.on(name, event => {
				events.push(event);
			});
		}
		await engine.start();

		const handle = await engine.trigger(task, undefined, { concurrencyKey: "k" });
		// Well under stallTimeoutMs (5 s): without the release the run would stay `running` until
		// stall recovery, which fails it (maxAttempts 1). The release backs off ~1 s.
		const run = await waitForStatus(engine, handle.id, "completed", 4_000);
		expect(injected).toBe(1);
		expect(run.attempt).toBe(1);
		expect(run.output).toBe("done");
		expect(logs.some(l => l.message.startsWith("claimed run released with backoff"))).toBe(true);
		const mine = events.filter(e => e.runId === handle.id).map(e => [e.name, e.attempt]);
		// The release publishes nothing (like a `limited` re-check: the run never started).
		expect(mine).toEqual([
			["run:queued", 1],
			["run:started", 1],
			["run:completed", 1],
		]);
	});

	it("a concurrency re-check that always throws backs off (growing delay), never a hot claim loop", async () => {
		const task = defineTask({
			id: uniqueTaskId("recheck-always-throws"),
			queue: { name: uniqueQueue(), concurrencyKey: { limit: 1 } },
			retry: { maxAttempts: 1 },
			run: async () => "done",
		});
		const failures: number[] = [];
		let failing = true;
		const originalQuery = Client.prototype.query;
		vi.spyOn(Client.prototype, "query").mockImplementation(function (
			this: Client,
			...args: unknown[]
		) {
			const [text] = args;
			if (
				failing &&
				typeof text === "string" &&
				text.includes("SELECT queue, concurrency_key, concurrency_waits FROM")
			) {
				failures.push(performance.now());
				return Promise.reject(new Error("boom: injected re-check failure"));
			}
			return (originalQuery as (...a: unknown[]) => unknown).apply(this, args);
		} as typeof Client.prototype.query);
		const engine = makeEngine([task], { polling: { intervalMs: 200 } });
		await engine.start();
		const handle = await engine.trigger(task, undefined, { concurrencyKey: "k" });

		await sleep(3_600);
		// Backoff 1 s, then 2 s (+ jitter < 250 ms): claims at ~0, ~1 s and ~3 s. Without it the
		// run went claim -> error -> release (run_after = now(), immediate wake) in a tight loop.
		expect(failures.length).toBeGreaterThanOrEqual(2);
		expect(failures.length).toBeLessThanOrEqual(3);
		const [a, b, c] = failures;
		if (a === undefined || b === undefined) throw new Error("unreachable");
		expect(b - a).toBeGreaterThanOrEqual(900);
		if (c !== undefined) expect(c - b).toBeGreaterThan(b - a + 500);
		const { rows } = await ns.pool.query<{ status: string; waits: number; attempt: number }>(
			`SELECT status, concurrency_waits AS waits, attempt FROM ${t("runs")} WHERE id = $1`,
			[handle.id]
		);
		expect(rows[0]).toEqual({ status: "queued", waits: failures.length, attempt: 1 });
		// Logged as an error, throttled: one line for all these failures.
		const errors = logs.filter(
			l => l.level === "error" && l.fields.runId === handle.id && /re-check/.test(l.message)
		);
		expect(errors).toHaveLength(1);

		failing = false;
		const run = await waitForStatus(engine, handle.id, "completed", 12_000);
		expect(run.attempt).toBe(1);
	}, 30_000);
});
