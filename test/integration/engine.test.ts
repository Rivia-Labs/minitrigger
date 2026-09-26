import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import {
	type BullmqTransportConfig,
	type CommonConfig,
	type EngineLogger,
} from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import {
	AbortTaskRunError,
	ConfigError,
	SerializationError,
	TaskNotRegisteredError,
} from "../../src/errors.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { type TestNamespace, useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const ns2 = useTestNamespace();

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const shortId = () => randomUUID().slice(0, 8);
const uniqueQueue = () => `q${shortId()}`;
const uniqueTaskId = (label: string) => `${label}-${shortId()}`;

type LogRecord = { level: string; message: string; fields: Record<string, unknown> };
const logs: LogRecord[] = [];
const testLogger: EngineLogger = {
	debug: (fields, message) => logs.push({ level: "debug", message, fields }),
	info: (fields, message) => logs.push({ level: "info", message, fields }),
	warn: (fields, message) => logs.push({ level: "warn", message, fields }),
	error: (fields, message) => logs.push({ level: "error", message, fields }),
};

const engines: Engine[] = [];
const cleanups: Array<() => Promise<void>> = [];

function makeEngine(
	tasks: AnyTask[],
	overrides: Partial<CommonConfig> & Partial<BullmqTransportConfig> = {},
	namespace: TestNamespace = ns
): Engine {
	const engine = createEngine({
		postgres: namespace.databaseUrl,
		redis: namespace.redisUrl,
		schema: namespace.schema,
		prefix: namespace.prefix,
		tasks,
		pool: { max: 5 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 5_000,
		shutdownTimeoutMs: 2_000,
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

/** A raw BullMQ queue on the test prefix, for inspecting/injecting jobs. */
function rawQueue(name: string, namespace: TestNamespace = ns): Queue {
	const connection = new Redis(namespace.redisUrl, { maxRetriesPerRequest: null });
	const queue = new Queue(name, { connection, prefix: namespace.prefix });
	cleanups.push(async () => {
		await queue.close().catch(() => {});
		connection.disconnect();
	});
	return queue;
}

async function runRow(id: string, namespace: TestNamespace = ns) {
	const { rows } = await namespace.pool.query<{
		status: string;
		attempt: number;
		enqueue_seq: number;
		execution_id: string | null;
		run_after: Date | null;
		heartbeat_at: Date | null;
		output: unknown;
	}>(`SELECT * FROM ${escapeIdentifier(namespace.schema)}.runs WHERE id = $1`, [id]);
	const row = rows[0];
	if (!row) throw new Error(`run ${id} not found`);
	return row;
}

async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 8_000) {
	await waitFor(async () => (await engine.runs.get(id))?.status === status, {
		timeoutMs,
		intervalMs: 25,
	});
	const run = await engine.runs.get(id);
	if (!run) throw new Error("unreachable");
	return run;
}

beforeAll(async () => {
	for (const namespace of [ns, ns2]) {
		const engine = createEngine({
			postgres: namespace.databaseUrl,
			redis: namespace.redisUrl,
			schema: namespace.schema,
			prefix: namespace.prefix,
			tasks: [],
		});
		await engine.migrate();
		await engine.stop();
	}
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
	for (const cleanup of cleanups.splice(0)) await cleanup();
	logs.length = 0;
});

describe("engine: execution", () => {
	it("runs a task to completion and records the output (success)", async () => {
		let seenCtx: { id: string; taskId: string; attempt: number; createdAt: Date } | undefined;
		const task = defineTask({
			id: uniqueTaskId("double"),
			queue: { name: uniqueQueue() },
			run: async (payload: { n: number }, ctx) => {
				seenCtx = ctx.run;
				ctx.logger.info("doubling", { n: payload.n });
				return { doubled: payload.n * 2 };
			},
		});
		const engine = makeEngine([task]);
		await engine.start();

		const handle = await engine.trigger(task, { n: 21 });
		expect(handle.taskId).toBe(task.id);
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ doubled: 42 });
		expect(run.attempt).toBe(1);
		expect(run.error).toBeNull();
		expect(run.finishedAt).toBeInstanceOf(Date);
		expect((await runRow(handle.id)).execution_id).toBeNull();
		expect(seenCtx).toMatchObject({ id: handle.id, taskId: task.id, attempt: 1 });
		expect(seenCtx?.createdAt).toBeInstanceOf(Date);
		expect(logs.some(l => l.message === "doubling" && l.fields["runId"] === handle.id)).toBe(true);
	});

	it("a task returning nothing stores null output", async () => {
		const task = defineTask({
			id: uniqueTaskId("void"),
			queue: { name: uniqueQueue() },
			run: async (_payload: { a: number }) => {},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task, { a: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toBeNull();
	});

	it("a task without a payload parameter is triggered without one (stored as null)", async () => {
		const task = defineTask({
			id: uniqueTaskId("no-payload"),
			queue: { name: uniqueQueue() },
			run: async payload => ({ received: payload === null }),
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.payload).toBeNull();
		expect(run.output).toEqual({ received: true });
	});

	it("retries a failing task until maxAttempts, then fails", async () => {
		let calls = 0;
		const attempts: number[] = [];
		const task = defineTask({
			id: uniqueTaskId("flaky"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 3, backoff: { type: "fixed", baseMs: 50, jitter: false } },
			run: async (_p: { x: number }, ctx) => {
				calls++;
				attempts.push(ctx.run.attempt);
				throw new Error(`boom ${calls}`);
			},
		});
		const engine = makeEngine([task]);
		await engine.start();

		const handle = await engine.trigger(task, { x: 1 });
		const run = await waitForStatus(engine, handle.id, "failed");
		expect(calls).toBe(3);
		expect(attempts).toEqual([1, 2, 3]);
		expect(run.attempt).toBe(3);
		expect(run.error).toMatchObject({ name: "Error", message: "boom 3", retryable: true });
		const row = await runRow(handle.id);
		expect(row.execution_id).toBeNull();
		expect(row.enqueue_seq).toBe(3);
	});

	it("succeeds on a later attempt after a retry", async () => {
		let calls = 0;
		const task = defineTask({
			id: uniqueTaskId("second-time"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 3, backoff: { type: "fixed", baseMs: 20, jitter: false } },
			run: async (_p: { x: number }) => {
				calls++;
				if (calls === 1) throw new Error("first fails");
				return "ok";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task, { x: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toBe("ok");
		expect(run.attempt).toBe(2);
		// Completion clears the error kept from the failed attempt.
		expect(run.error).toBeNull();
	});

	it("AbortTaskRunError fails the run without retrying", async () => {
		let calls = 0;
		const task = defineTask({
			id: uniqueTaskId("abort"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 5, backoff: { type: "fixed", baseMs: 10 } },
			run: async (_p: { x: number }) => {
				calls++;
				throw new AbortTaskRunError("do not retry");
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task, { x: 1 });
		const run = await waitForStatus(engine, handle.id, "failed");
		// `failed` is terminal and no retry was ever enqueued (enqueue_seq never moved).
		expect((await runRow(handle.id)).enqueue_seq).toBe(1);
		expect(calls).toBe(1);
		expect(run.attempt).toBe(1);
		expect(run.error).toMatchObject({ name: "AbortTaskRunError", retryable: false });
	});

	it("maxDurationMs aborts ctx.signal and fails the attempt with a retryable TimeoutError", async () => {
		const reasons: string[] = [];
		const task = defineTask({
			id: uniqueTaskId("slow"),
			queue: { name: uniqueQueue() },
			maxDurationMs: 200,
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 20, jitter: false } },
			run: async (_p: { x: number }, ctx) => {
				await new Promise<void>(resolve => {
					ctx.signal.addEventListener("abort", () => {
						reasons.push((ctx.signal.reason as Error).name);
						resolve();
					});
				});
				// Ignores the abort and "succeeds": the executor already stopped waiting.
				return "late";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task, { x: 1 });
		const run = await waitForStatus(engine, handle.id, "failed");
		expect(run.attempt).toBe(2);
		expect(run.output).toBeNull();
		expect(run.error).toMatchObject({ name: "TimeoutError", retryable: true });
		expect(run.error?.message).toContain("maxDurationMs");
		expect(reasons).toEqual(["TimeoutError", "TimeoutError"]);
	});

	it("rejects a non-serializable task output as a definitive failure", async () => {
		const task = defineTask({
			id: uniqueTaskId("bad-output"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 3 },
			run: async (_p: { x: number }) => ({ big: "x".repeat(2_000) }),
		});
		const engine = makeEngine([task], { limits: { maxPayloadBytes: 1_000 } });
		await engine.start();
		const handle = await engine.trigger(task, { x: 1 });
		const run = await waitForStatus(engine, handle.id, "failed");
		expect(run.attempt).toBe(1);
		expect(run.error).toMatchObject({ name: "SerializationError", retryable: false });
	});

	it("heartbeats while running, and aborts the signal when ownership is lost", async () => {
		let aborted: string | undefined;
		let release: (() => void) | undefined;
		const task = defineTask({
			id: uniqueTaskId("hb"),
			queue: { name: uniqueQueue() },
			run: async (_p: { x: number }, ctx) => {
				ctx.signal.addEventListener("abort", () => {
					aborted = (ctx.signal.reason as Error).name;
				});
				await new Promise<void>(resolve => {
					release = resolve;
				});
				return "done";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task, { x: 1 });
		await waitFor(() => release !== undefined);
		const first = (await runRow(handle.id)).heartbeat_at;
		await waitFor(async () => {
			const hb = (await runRow(handle.id)).heartbeat_at;
			return hb !== null && first !== null && hb.getTime() > first.getTime();
		});

		// Steal ownership (as a stall recovery/cancel in another process would).
		await ns.pool.query(
			`UPDATE ${escapeIdentifier(ns.schema)}.runs SET execution_id = gen_random_uuid() WHERE id = $1`,
			[handle.id]
		);
		await waitFor(() => aborted !== undefined, { timeoutMs: 3_000 });
		expect(aborted).toBe("ExecutionLostError");
		// The executor stopped waiting for the zombie as soon as ownership was lost.
		await waitFor(() =>
			logs.some(
				l =>
					l.message === "execution lost ownership of the run; stopping without writing" &&
					l.fields["runId"] === handle.id
			)
		);
		release?.();
		// The zombie's result was never written.
		expect((await engine.runs.get(handle.id))?.status).toBe("running");
	});
});

describe("engine: trigger", () => {
	it("idempotency: the same key returns the same run and enqueues it once", async () => {
		let calls = 0;
		const queueName = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("idem"),
			queue: { name: queueName },
			run: async (p: { v: number }) => {
				calls++;
				return p.v;
			},
		});
		const engine = makeEngine([task]);
		const a = await engine.trigger(task, { v: 1 }, { idempotencyKey: "key-1" });
		const b = await engine.trigger(task, { v: 2 }, { idempotencyKey: "key-1" });
		expect(b.id).toBe(a.id);

		const counts = await rawQueue(queueName).getJobCounts("waiting", "delayed", "prioritized");
		expect(counts["waiting"] ?? 0).toBe(1);

		await engine.start();
		const run = await waitForStatus(engine, a.id, "completed");
		expect(run.output).toBe(1);
		// Only one job was ever enqueued (asserted above) and it is gone now.
		await waitFor(async () => {
			const c = await rawQueue(queueName).getJobCounts("waiting", "active", "delayed");
			return Object.values(c).every(n => n === 0);
		});
		expect(calls).toBe(1);

		const c = await engine.trigger(task, { v: 3 }, { idempotencyKey: "key-2" });
		expect(c.id).not.toBe(a.id);
	});

	it("user keys without a TTL get the default idempotencyKeyTtl (7 days)", async () => {
		const task = defineTask({
			id: uniqueTaskId("ttl"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }) => {},
		});
		const engine = makeEngine([task]);
		const a = await engine.trigger(task, { v: 1 }, { idempotencyKey: "default-ttl" });
		const b = await engine.trigger(
			task,
			{ v: 1 },
			{ idempotencyKey: "explicit-ttl", idempotencyKeyTtl: "1h" }
		);
		const { rows } = await ns.pool.query<{ run_id: string; seconds: number }>(
			`SELECT run_id, extract(epoch FROM expires_at - now())::float8 AS seconds
			 FROM ${escapeIdentifier(ns.schema)}.idempotency_keys WHERE task_id = $1`,
			[task.id]
		);
		const byRun = new Map(rows.map(r => [r.run_id, r.seconds]));
		expect(byRun.get(a.id)).toBeGreaterThan(7 * 86_400 - 60);
		expect(byRun.get(a.id)).toBeLessThanOrEqual(7 * 86_400);
		expect(byRun.get(b.id)).toBeGreaterThan(3_600 - 60);
		expect(byRun.get(b.id)).toBeLessThanOrEqual(3_600);
	});

	it("delay: the run executes only after run_after", async () => {
		const task = defineTask({
			id: uniqueTaskId("delayed"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }) => "done",
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task, { v: 1 }, { delay: "1s" });

		const queued = await engine.runs.get(handle.id);
		expect(queued?.status).toBe("queued");
		const delayMs = (queued?.runAfter?.getTime() ?? 0) - (queued?.createdAt.getTime() ?? 0);
		expect(delayMs).toBeGreaterThanOrEqual(990);
		expect(delayMs).toBeLessThan(1_500);

		await sleep(500);
		expect((await engine.runs.get(handle.id))?.status).toBe("queued");

		const run = await waitForStatus(engine, handle.id, "completed");
		expect((run.startedAt?.getTime() ?? 0) - run.createdAt.getTime()).toBeGreaterThanOrEqual(900);
	});

	it("validates the task, the idempotency key and the payload", async () => {
		const task = defineTask({
			id: uniqueTaskId("valid"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: unknown }) => {},
		});
		const other = defineTask({ id: uniqueTaskId("other"), run: async (_p: { v: number }) => {} });
		const engine = makeEngine([task], { limits: { maxPayloadBytes: 100 } });

		await expect(engine.trigger(other, { v: 1 })).rejects.toBeInstanceOf(TaskNotRegisteredError);
		await expect(engine.trigger(task, { v: 1 }, { idempotencyKey: "__x" })).rejects.toBeInstanceOf(
			ConfigError
		);
		await expect(engine.trigger(task, { v: new Date() })).rejects.toBeInstanceOf(
			SerializationError
		);
		await expect(engine.trigger(task, { v: "x".repeat(200) })).rejects.toBeInstanceOf(
			SerializationError
		);
		await expect(
			engine.trigger(task, { v: 1 }, { idempotencyKey: "k", idempotencyKeyTtl: "31d" })
		).rejects.toThrow(/retention\.days/);
	});
});

describe("engine: queues and workers", () => {
	it("a stale job (old enqueue_seq) is a no-op", async () => {
		let calls = 0;
		const queueName = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("stale"),
			queue: { name: queueName },
			run: async (_p: { v: number }) => {
				calls++;
				return calls;
			},
		});
		const engine = makeEngine([task]);
		// Enqueued as `${id}-1` but not started yet; then the run moves on to seq 2.
		const handle = await engine.trigger(task, { v: 1 });
		await ns.pool.query(
			`UPDATE ${escapeIdentifier(ns.schema)}.runs SET enqueue_seq = 2 WHERE id = $1`,
			[handle.id]
		);
		await engine.start();

		const queue = rawQueue(queueName);
		// The job is removed only once its processor returned: the stale delivery is fully done.
		await waitFor(async () => (await queue.getJob(`${handle.id}-1`)) === undefined);
		expect(calls).toBe(0);
		expect((await engine.runs.get(handle.id))?.status).toBe("queued");

		await queue.add("run", { runId: handle.id, seq: 2 }, { jobId: `${handle.id}-2` });
		await waitForStatus(engine, handle.id, "completed");
		expect(calls).toBe(1);
	});

	it("two workers (two engines) competing for the same run: only one executes it", async () => {
		let calls = 0;
		const queueName = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("compete"),
			queue: { name: queueName, concurrency: 2 },
			run: async (_p: { v: number }) => {
				calls++;
				await sleep(300);
				return "done";
			},
		});
		const a = makeEngine([task]);
		const b = makeEngine([task]);
		await a.start();
		await b.start();

		const handle = await a.trigger(task, { v: 1 });
		// A duplicate delivery of the same (runId, seq) under another jobId.
		const queue = rawQueue(queueName);
		await queue.add("run", { runId: handle.id, seq: 1 }, { jobId: `dup-${handle.id}` });
		await queue.add("run", { runId: handle.id, seq: 1 }, { jobId: `dup2-${handle.id}` });

		const run = await waitForStatus(a, handle.id, "completed");
		await waitFor(async () => {
			const c = await queue.getJobCounts("active", "waiting");
			return c["active"] === 0 && c["waiting"] === 0;
		});
		expect(calls).toBe(1);
		expect(run.attempt).toBe(1);
	});

	it("start({ queues: ['llm'] }) starts only that task queue plus the -system worker", async () => {
		let llmCalls = 0;
		let otherCalls = 0;
		const llm = defineTask({
			id: uniqueTaskId("llm"),
			queue: { name: "llm", concurrency: 2 },
			run: async (_p: { v: number }) => {
				llmCalls++;
			},
		});
		const other = defineTask({
			id: uniqueTaskId("other"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }) => {
				otherCalls++;
			},
		});
		const engine = makeEngine([llm, other]);
		await engine.start({ queues: ["llm"] });

		const a = await engine.trigger(llm, { v: 1 });
		const b = await engine.trigger(other, { v: 1 });
		await waitForStatus(engine, a.id, "completed");

		const system = rawQueue(`${ns.prefix}-system`);
		await system.add("reconcile", {}, { jobId: `probe-${shortId()}`, removeOnComplete: true });
		await waitFor(async () => {
			const counts = await system.getJobCounts("waiting", "active", "completed", "failed");
			return Object.values(counts).every(n => n === 0);
		});

		// `other`'s queue has no worker at all in this process: nothing can pick its run up.
		expect(llmCalls).toBe(1);
		expect(otherCalls).toBe(0);
		expect((await engine.runs.get(b.id))?.status).toBe("queued");
		await expect(makeEngine([llm]).start({ queues: ["nope"] })).rejects.toBeInstanceOf(ConfigError);
	});

	it("two tasks on the same queue with different concurrency: createEngine throws", () => {
		const q = uniqueQueue();
		const a = defineTask({
			id: uniqueTaskId("a"),
			queue: { name: q, concurrency: 1 },
			run: async () => {},
		});
		const b = defineTask({
			id: uniqueTaskId("b"),
			queue: { name: q, concurrency: 2 },
			run: async () => {},
		});
		expect(() => makeEngine([a, b])).toThrow(ConfigError);
		expect(() => makeEngine([a, b])).toThrow(/concurrency/);
	});

	it("rejects a task queue colliding with the -system queue", () => {
		const a = defineTask({
			id: uniqueTaskId("a"),
			queue: { name: `${ns.prefix}-system` },
			run: async () => {},
		});
		expect(() => makeEngine([a])).toThrow(ConfigError);
	});

	it("two tasks on the same queue, one declaring and one referencing, both run", async () => {
		const q = uniqueQueue();
		const declaring = defineTask({
			id: uniqueTaskId("declaring"),
			queue: { name: q, concurrency: 3 },
			run: async (_p: { v: number }) => "a",
		});
		const referencing = defineTask({
			id: uniqueTaskId("referencing"),
			queue: { name: q },
			run: async (_p: { v: number }) => "b",
		});
		const engine = makeEngine([declaring, referencing]);
		await engine.start();
		const a = await engine.trigger(declaring, { v: 1 });
		const b = await engine.trigger(referencing, { v: 1 });
		expect((await waitForStatus(engine, a.id, "completed")).output).toBe("a");
		expect((await waitForStatus(engine, b.id, "completed")).output).toBe("b");
		expect((await engine.runs.get(b.id))?.queue).toBe(q);
	});
});

describe("engine: lifecycle", () => {
	it("stop() hands a slow run back to queued without incrementing attempt", async () => {
		let calls = 0;
		let started = false;
		let abortReason: string | undefined;
		const queueName = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("slow-stop"),
			queue: { name: queueName },
			run: async (_p: { v: number }, ctx) => {
				calls++;
				if (calls === 1) {
					started = true;
					ctx.signal.addEventListener("abort", () => {
						abortReason = (ctx.signal.reason as Error).name;
					});
					await sleep(3_000); // ignores the signal
				}
				return `call ${calls}`;
			},
		});
		const engine = makeEngine([task], { shutdownTimeoutMs: 300 });
		await engine.start();
		const handle = await engine.trigger(task, { v: 1 });
		await waitFor(() => started);

		await engine.stop();
		await engine.stop(); // idempotent

		const row = await runRow(handle.id);
		expect(row.status).toBe("queued");
		expect(row.attempt).toBe(1);
		expect(row.execution_id).toBeNull();
		expect(row.enqueue_seq).toBe(2);
		expect(row.run_after).not.toBeNull();
		expect(abortReason).toBe("AbortError");
		const job = await rawQueue(queueName).getJob(`${handle.id}-2`);
		expect(job?.data).toEqual({ runId: handle.id, seq: 2 });

		// Another engine picks the handed-back run up; the attempt was not consumed.
		const next = makeEngine([task]);
		await next.start();
		const run = await waitForStatus(next, handle.id, "completed");
		expect(run.attempt).toBe(1);
		expect(run.output).toBe("call 2");
		await expect(engine.trigger(task, { v: 1 })).rejects.toThrow(/stopped/);
	});

	it("stop() waits for a run that finishes within shutdownTimeoutMs (graceful path)", async () => {
		let started = false;
		const task = defineTask({
			id: uniqueTaskId("graceful"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }) => {
				started = true;
				await sleep(200);
				return "finished";
			},
		});
		const engine = makeEngine([task], { shutdownTimeoutMs: 3_000 });
		await engine.start();
		const handle = await engine.trigger(task, { v: 1 });
		await waitFor(() => started);

		await engine.stop();

		const row = await runRow(handle.id);
		expect(row.status).toBe("completed");
		expect(row.attempt).toBe(1);
		expect(row.enqueue_seq).toBe(1);
		expect(row.execution_id).toBeNull();
		expect(row.output).toBe("finished");
		expect(logs.some(l => l.message === "run handed back to the queue on shutdown")).toBe(false);
	});

	it("stop() on a never-started engine resolves and blocks further use", async () => {
		const task = defineTask({
			id: uniqueTaskId("never-started"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }) => {},
		});
		const engine = makeEngine([task]);
		await engine.stop();
		await engine.stop();
		await expect(engine.trigger(task, { v: 1 })).rejects.toThrow(/stopped/);
		await expect(engine.start()).rejects.toThrow(/stopped/);
	});

	// Item 7: `queues.pause`/`resume`/`scale` and `runs.cancelMany` used to skip `assertNotStopped()`
	// (unlike `runs.replay`/`runs.retryFailed`/`trigger`), so they'd keep "working" (queuing store
	// writes and BullMQ calls) against an engine the caller already tore down.
	it("queues.pause/resume/scale and runs.cancelMany reject the same way as other ops after stop()", async () => {
		const queue = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("stopped-ops"),
			queue: { name: queue },
			run: async (_p: { v: number }) => {},
		});
		const engine = makeEngine([task]);
		await engine.stop();

		await expect(engine.queues.pause(queue)).rejects.toThrow(/stopped/);
		await expect(engine.queues.resume(queue)).rejects.toThrow(/stopped/);
		await expect(engine.queues.scale(queue, 1)).rejects.toThrow(/stopped/);
		await expect(engine.runs.cancelMany({ limit: 10 })).rejects.toThrow(/stopped/);
	});

	it("a failed start() cleans up and can be retried", async () => {
		const task = defineTask({
			id: uniqueTaskId("restart"),
			queue: { name: uniqueQueue() },
			run: async (p: { v: number }) => p.v,
		});
		const engine = makeEngine([task]);
		await expect(engine.start({ queues: ["nope"] })).rejects.toBeInstanceOf(ConfigError);
		await engine.start();
		const handle = await engine.trigger(task, { v: 7 });
		expect((await waitForStatus(engine, handle.id, "completed")).output).toBe(7);
	});

	it("a run acquired while stop() is in progress is handed back before the queues close", async () => {
		// Test-only dependency injection through the public `postgres: { pool }` option: the
		// acquire UPDATE is held until the test opens the gate, i.e. after `stop()` already took
		// its snapshot of the active executions.
		let gateHit = false;
		let openGate: () => void = () => {};
		const gate = new Promise<void>(resolve => {
			openGate = resolve;
		});
		const basePool = ns.pool;
		const gatedPool = new Proxy(basePool, {
			get(target, prop) {
				if (prop === "query") {
					return async (text: unknown, ...rest: unknown[]) => {
						if (typeof text === "string" && text.includes("SET status = 'running'")) {
							gateHit = true;
							await gate;
						}
						return (target.query as (...args: unknown[]) => unknown).call(target, text, ...rest);
					};
				}
				const value: unknown = Reflect.get(target, prop, target);
				return typeof value === "function" ? value.bind(target) : value;
			},
		});

		let calls = 0;
		const queueName = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("late-acquire"),
			queue: { name: queueName },
			run: async (_p: { v: number }) => {
				calls++;
				return "ran";
			},
		});
		const engine = makeEngine([task], { postgres: { pool: gatedPool }, shutdownTimeoutMs: 200 });
		await engine.start();
		const handle = await engine.trigger(task, { v: 1 });
		await waitFor(() => gateHit);

		const stopping = engine.stop();
		// Past the shutdown wait (200 ms): the hand-back snapshot is already taken.
		await sleep(400);
		openGate();
		await stopping;

		const row = await runRow(handle.id);
		expect(row.status).toBe("queued");
		expect(row.attempt).toBe(1);
		expect(row.execution_id).toBeNull();
		expect(row.enqueue_seq).toBe(2);
		expect(row.run_after).not.toBeNull();
		expect(calls).toBe(0);
		const job = await rawQueue(queueName).getJob(`${handle.id}-2`);
		expect(job?.data).toEqual({ runId: handle.id, seq: 2 });

		const next = makeEngine([task]);
		await next.start();
		const run = await waitForStatus(next, handle.id, "completed");
		expect(run.attempt).toBe(1);
		expect(calls).toBe(1);
	});

	it("does not close an injected pg pool or ioredis connection", async () => {
		const task = defineTask({
			id: uniqueTaskId("injected"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }) => "ok",
		});
		const engine = makeEngine([task], {
			postgres: { pool: ns.pool },
			redis: { connection: ns.redis },
		});
		await engine.start();
		const handle = await engine.trigger(task, { v: 1 });
		await waitForStatus(engine, handle.id, "completed");
		await engine.stop();
		await expect(ns.pool.query("SELECT 1")).resolves.toBeTruthy();
		await expect(ns.redis.ping()).resolves.toBe("PONG");
	});

	it("two engines with different schemas/prefixes in one process are isolated", async () => {
		const task = defineTask({
			id: uniqueTaskId("iso"),
			queue: { name: "shared" },
			run: async (p: { v: number }) => p.v,
		});
		const a = makeEngine([task], {}, ns);
		const b = makeEngine([task], {}, ns2);
		await a.start();
		await b.start();
		const ra = await a.trigger(task, { v: 1 });
		const rb = await b.trigger(task, { v: 2 });
		expect((await waitForStatus(a, ra.id, "completed")).output).toBe(1);
		expect((await waitForStatus(b, rb.id, "completed")).output).toBe(2);
		expect(await a.runs.get(rb.id)).toBeNull();
		expect(await b.runs.get(ra.id)).toBeNull();
		expect((await runRow(ra.id, ns)).status).toBe("completed");
		expect((await runRow(rb.id, ns2)).status).toBe("completed");
	});
});

describe("engine: failure paths", () => {
	it("a worker that doesn't know the task fails the attempt (retryable); one that knows it completes it", async () => {
		const queueName = uniqueQueue();
		const known = defineTask({
			id: uniqueTaskId("known"),
			queue: { name: queueName },
			retry: { maxAttempts: 5, backoff: { type: "fixed", baseMs: 1_000, jitter: false } },
			run: async (p: { v: number }) => p.v + 1,
		});
		const unrelated = defineTask({
			id: uniqueTaskId("unrelated"),
			queue: { name: queueName },
			run: async (_p: { v: number }) => {},
		});
		const producer = makeEngine([known]);
		const ignorant = makeEngine([unrelated]);
		await ignorant.start();

		const handle = await producer.trigger(known, { v: 1 });
		await waitFor(async () => {
			const row = await runRow(handle.id);
			return row.status === "queued" && row.attempt === 2;
		});
		const failed = await producer.runs.get(handle.id);
		expect(failed?.error).toMatchObject({ name: "TaskNotRegisteredError", retryable: true });
		await ignorant.stop();

		await producer.start();
		const run = await waitForStatus(producer, handle.id, "completed");
		expect(run.output).toBe(2);
		expect(run.attempt).toBeGreaterThanOrEqual(2);
		expect(run.error).toBeNull();
	});

	it("an enqueue failure after commit leaves the run queued with run_after set (decision 1)", async () => {
		const queueName = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("enqueue-fails"),
			queue: { name: queueName },
			run: async (_p: { v: number }) => {},
		});
		const redis = new Redis(ns.redisUrl, { maxRetriesPerRequest: null });
		cleanups.push(async () => {
			redis.disconnect();
		});
		await redis.ping();
		const engine = makeEngine([task], { redis: { connection: redis } });
		redis.disconnect();

		const handle = await engine.trigger(task, { v: 1 });

		const row = await runRow(handle.id);
		expect(row.status).toBe("queued");
		expect(row.enqueue_seq).toBe(1);
		expect(row.run_after).not.toBeNull();
		expect(
			logs.some(
				l =>
					l.level === "warn" &&
					l.message.startsWith("enqueue failed") &&
					l.fields["runId"] === handle.id
			)
		).toBe(true);
		expect(await rawQueue(queueName).getJob(`${handle.id}-1`)).toBeUndefined();
	});

	it("rejects timer-fed durations above 2^31-1 ms at createEngine", () => {
		const task = defineTask({
			id: uniqueTaskId("huge"),
			queue: { name: uniqueQueue() },
			maxDurationMs: 2_147_483_648,
			run: async () => {},
		});
		expect(() => makeEngine([task])).toThrow(ConfigError);
		expect(() => makeEngine([task])).toThrow(/maxDurationMs.*2147483647/);
		expect(() => makeEngine([], { shutdownTimeoutMs: 2_147_483_648 })).toThrow(ConfigError);
	});
});

describe("engine: runs API", () => {
	it("cancel aborts a running execution locally and its result is discarded", async () => {
		let aborted: string | undefined;
		let started = false;
		const task = defineTask({
			id: uniqueTaskId("cancel"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				started = true;
				await new Promise<void>(resolve => {
					ctx.signal.addEventListener("abort", () => {
						aborted = (ctx.signal.reason as Error).name;
						resolve();
					});
				});
				return "should be discarded";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task, { v: 1 });
		await waitFor(() => started);

		expect(await engine.runs.cancel(handle)).toBe("canceled");
		await waitFor(() => aborted !== undefined);
		expect(aborted).toBe("AbortError");
		await waitFor(() =>
			logs.some(
				l => l.message === "run canceled while executing" && l.fields["runId"] === handle.id
			)
		);
		const run = await engine.runs.get(handle.id);
		expect(run?.status).toBe("canceled");
		expect(run?.output).toBeNull();
		expect(run?.error).toMatchObject({ name: "Canceled" });
		expect(await engine.runs.cancel(handle.id)).toBe("already_finished");
		expect(await engine.runs.cancel(randomUUID())).toBe("not_found");
	});

	it("cancel of a queued run removes its job", async () => {
		const queueName = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("cancel-queued"),
			queue: { name: queueName },
			run: async (_p: { v: number }) => {},
		});
		const engine = makeEngine([task]);
		const handle = await engine.trigger(task, { v: 1 }, { delay: "10s" });
		const queue = rawQueue(queueName);
		expect(await queue.getJob(`${handle.id}-1`)).toBeDefined();
		expect(await engine.runs.cancel(handle.id)).toBe("canceled");
		expect(await queue.getJob(`${handle.id}-1`)).toBeUndefined();
	});

	it("replay creates a new run with the same payload and no idempotency key", async () => {
		const task = defineTask({
			id: uniqueTaskId("replay"),
			queue: { name: uniqueQueue() },
			run: async (p: { v: number }) => p.v * 10,
		});
		const engine = makeEngine([task]);
		await engine.start();
		const first = await engine.trigger(task, { v: 4 }, { idempotencyKey: "replay-key" });
		await waitForStatus(engine, first.id, "completed");

		const second = await engine.runs.replay(first);
		expect(second.id).not.toBe(first.id);
		const run = await waitForStatus(engine, second.id, "completed");
		expect(run.payload).toEqual({ v: 4 });
		expect(run.output).toBe(40);
		// The original key still points at the first run.
		expect((await engine.trigger(task, { v: 9 }, { idempotencyKey: "replay-key" })).id).toBe(
			first.id
		);
	});

	it("list filters by task and status with a cursor", async () => {
		const task = defineTask({
			id: uniqueTaskId("list"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }) => {},
		});
		const engine = makeEngine([task]);
		const ids: string[] = [];
		for (let i = 0; i < 3; i++) ids.push((await engine.trigger(task, { v: i })).id);

		const page1 = await engine.runs.list({ taskId: task.id, status: "queued", limit: 2 });
		expect(page1.items).toHaveLength(2);
		expect(page1.nextCursor).not.toBeNull();
		const page2 = await engine.runs.list({
			taskId: task.id,
			limit: 2,
			...(page1.nextCursor ? { cursor: page1.nextCursor } : {}),
		});
		expect(page2.items).toHaveLength(1);
		expect(new Set([...page1.items, ...page2.items].map(r => r.id))).toEqual(new Set(ids));
	});
});
