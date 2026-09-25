/**
 * Audit fixes on the step/executor boundary: `:` reserved in step names (collision with the
 * internal `__child:*` keys, decisions 9/11), steps in flight when the task function throws,
 * child tasks not registered in this process, invalid child payloads (fatal) and `ctx.signal`
 * aborted once the attempt ended.
 */
import { randomUUID } from "node:crypto";
import { escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const shortId = () => randomUUID().slice(0, 8);
const uniqueQueue = () => `q${shortId()}`;
const uniqueTaskId = (label: string) => `${label}-${shortId()}`;
const fastRetry = (maxAttempts: number) => ({
	maxAttempts,
	backoff: { type: "fixed" as const, baseMs: 10, jitter: false },
});

const noop = () => {};
const testLogger: EngineLogger = { debug: noop, info: noop, warn: noop, error: noop };

const engines: Engine[] = [];

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
	await Promise.all(engines.splice(0).map(engine => engine.stop()));
});

async function startedEngine(tasks: AnyTask[], overrides: Partial<EngineConfig> = {}) {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 10 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 5_000,
		shutdownTimeoutMs: 2_000,
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	await engine.start();
	return engine;
}

async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 15_000) {
	await waitFor(async () => (await engine.runs.get(id))?.status === status, {
		timeoutMs,
		intervalMs: 20,
	});
	const run = await engine.runs.get(id);
	if (!run) throw new Error("unreachable");
	return run;
}

async function childCount(parentId: string): Promise<number> {
	const { rows } = await ns.pool.query<{ n: number }>(
		`SELECT count(*)::int AS n FROM ${escapeIdentifier(ns.schema)}.runs WHERE parent_run_id = $1`,
		[parentId]
	);
	return rows[0]?.n ?? 0;
}

describe("':' in step names (decision 11)", () => {
	it("a name that could collide with a batch child key fails the run without retry", async () => {
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: uniqueQueue() },
			run: async (p: number) => p,
		});
		let error: unknown;
		let goodCalled = false;
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: uniqueQueue() },
			retry: fastRetry(3),
			run: async (_p: { v: number }, ctx) => {
				// `batchTriggerAndWait("x")` keys its first child `__child:${runId}:x:0`, exactly the
				// key of `triggerAndWait("x:0")`.
				await ctx.step.batchTriggerAndWait("x", child, [1]);
				try {
					await ctx.step.triggerAndWait("x:0", child, 2);
				} catch (e) {
					error = e;
				}
				// A code bug: later steps rethrow it instead of running.
				return ctx.step.run("good", () => {
					goodCalled = true;
					return 1;
				});
			},
		});
		const engine = await startedEngine([parent, child]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "failed");
		expect(run.attempt).toBe(1);
		expect(run.error).toMatchObject({ name: "ConfigError", retryable: false });
		expect(run.error?.message).toMatch(/":" is reserved/);
		expect((error as Error).message).toMatch(/":" is reserved/);
		expect(goodCalled).toBe(false);
		expect(await childCount(handle.id)).toBe(1);
	});
});

describe("task function throws while other steps are in flight", () => {
	it("waits for them: a completed in-flight step is recorded once and the retry reuses it", async () => {
		const abortedDuringB: Array<{ attempt: number; aborted: boolean }> = [];
		const bStarts: number[] = [];
		const task = defineTask({
			id: uniqueTaskId("drain"),
			queue: { name: uniqueQueue() },
			retry: fastRetry(2),
			run: async (_p: { v: number }, ctx) => {
				const attempt = ctx.run.attempt;
				await Promise.all([
					ctx.step.run("a", async () => {
						await sleep(50);
						if (attempt === 1) throw new Error("a fails on attempt 1");
						return "a";
					}),
					ctx.step.run("b", async () => {
						bStarts.push(attempt);
						// Long side effect, still running when "a" rejects the Promise.all.
						await sleep(500);
						abortedDuringB.push({ attempt, aborted: ctx.signal.aborted });
						return "b";
					}),
				]);
				return "done";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.attempt).toBe(2);
		expect(bStarts).toEqual([1]);
		expect(abortedDuringB).toEqual([{ attempt: 1, aborted: false }]);
		const steps = await engine.runs.steps(handle.id);
		expect(steps.map(s => [s.name, s.status, s.output])).toEqual([
			["a", "completed", "a"],
			["b", "completed", "b"],
		]);
	});

	it("maxDurationMs bounds the wait; the attempt fails with the original error", async () => {
		let bAborted: boolean | undefined;
		// "b" only stops when this is released (a stand-in for a step that ignores ctx.signal), so
		// the test controls exactly how long it runs instead of a real multi-second timer that would
		// keep firing in the background well past this test's own assertions (golden rule 14).
		let releaseB: () => void = () => {};
		const bReleased = new Promise<void>(resolve => {
			releaseB = resolve;
		});
		const task = defineTask({
			id: uniqueTaskId("drain-timeout"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 1 },
			maxDurationMs: 400,
			run: async (_p: { v: number }, ctx) => {
				await Promise.all([
					ctx.step.run("a", async () => {
						throw new Error("a fails");
					}),
					ctx.step.run("b", async () => {
						await bReleased; // ignores ctx.signal
						bAborted = ctx.signal.aborted;
						return "b";
					}),
				]);
				return "done";
			},
		});
		try {
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const started = Date.now();
			const run = await waitForStatus(engine, handle.id, "failed", 4_000);
			expect(Date.now() - started).toBeLessThan(4_000);
			expect(run.attempt).toBe(1);
			expect(run.error).toMatchObject({ name: "Error", message: "a fails", retryable: true });
			expect(bAborted).toBeUndefined();
			const steps = await engine.runs.steps(handle.id);
			expect(steps).toEqual([]);
		} finally {
			releaseB();
		}
	});

	it("logs a warn naming the in-flight steps before draining for a failure (diagnosability)", async () => {
		const warnLogs: { fields: unknown; message: string }[] = [];
		const capturingLogger: EngineLogger = {
			debug: noop,
			info: noop,
			warn: (fields, message) => warnLogs.push({ fields, message }),
			error: noop,
		};
		const task = defineTask({
			id: uniqueTaskId("drain-fail-warn"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 1 },
			run: async (_p: { v: number }, ctx) => {
				await Promise.all([
					ctx.step.run("a", () => {
						throw new Error("a fails");
					}),
					ctx.step.run("slow-b", async () => {
						await sleep(50);
						return "b";
					}),
				]);
				return "done";
			},
		});
		const engine = await startedEngine([task], { logger: capturingLogger });
		const handle = await engine.trigger(task, { v: 1 });
		await waitForStatus(engine, handle.id, "failed");
		const warned = warnLogs.find(l => l.message.includes("in flight"));
		expect(warned?.message).toContain('"slow-b"');
		expect(warned?.fields).toMatchObject({ runId: handle.id, steps: ["slow-b"] });
	});

	it("an in-flight step that suspends during the wait does not prevent the failure", async () => {
		const task = defineTask({
			id: uniqueTaskId("drain-suspend"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 1 },
			run: async (_p: { v: number }, ctx) => {
				await Promise.all([
					ctx.step.run("a", () => {
						throw new Error("a fails");
					}),
					ctx.step.sleep("nap", "1h"),
				]);
				return "done";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "failed");
		expect(run.attempt).toBe(1);
		expect(run.error).toMatchObject({ message: "a fails", retryable: true });
		// The sleep was recorded while the executor waited for it (instead of racing the fail).
		const steps = await engine.runs.steps(handle.id);
		expect(steps.map(s => [s.name, s.kind])).toEqual([["nap", "sleep"]]);
	});

	it("a sleep recorded during the drain replays correctly on retry and the run completes", async () => {
		const task = defineTask({
			id: uniqueTaskId("drain-sleep-retry"),
			queue: { name: uniqueQueue() },
			retry: fastRetry(2),
			run: async (_p: { v: number }, ctx) => {
				const attempt = ctx.run.attempt;
				const [a] = await Promise.all([
					ctx.step.run("a", async () => {
						if (attempt === 1) throw new Error("a fails on attempt 1");
						return "a";
					}),
					ctx.step.sleep("nap", "50ms"),
				]);
				return a;
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.attempt).toBe(2);
		expect(run.output).toBe("a");
		// "nap" was recorded `waiting` during attempt 1's drain and replayed to `completed` on the
		// retry, instead of being re-run from scratch or causing a NonDeterminismError.
		const steps = await engine.runs.steps(handle.id);
		expect(steps.map(s => [s.name, s.kind, s.status])).toEqual([
			["a", "run", "completed"],
			["nap", "sleep", "completed"],
		]);
	});

	it("a cancel during the drain writes nothing from the executor; the run ends canceled", async () => {
		let bStarted = false;
		let bFinished = false;
		const task = defineTask({
			id: uniqueTaskId("drain-cancel"),
			queue: { name: uniqueQueue() },
			retry: { maxAttempts: 1 },
			run: async (_p: { v: number }, ctx) => {
				await Promise.all([
					ctx.step.run("a", () => {
						throw new Error("a fails");
					}),
					ctx.step.run("b", async () => {
						bStarted = true;
						await sleep(300); // real timer, ignores ctx.signal, short and self-terminating
						bFinished = true;
						return "b";
					}),
				]);
				return "done";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		await waitFor(async () => bStarted, { timeoutMs: 5_000, intervalMs: 10 });
		expect(await engine.runs.cancel(handle.id)).toBe("canceled");
		const run = await waitForStatus(engine, handle.id, "canceled");
		expect(run.error).toMatchObject({ name: "Canceled" });
		// "b" keeps running for real (it never checked ctx.signal) and eventually finishes, but its
		// write lands after the cancel already cleared ownership (decision 5): discarded, not a
		// second terminal write racing the cancel.
		await waitFor(async () => bFinished, { timeoutMs: 2_000, intervalMs: 10 });
		expect((await engine.runs.get(handle.id))?.status).toBe("canceled");
	});
});

describe("a fatal step error fails the run promptly (does not wait for in-flight steps)", () => {
	it("a fatal NonDeterminismError (duplicate name) with a long in-flight step fails without waiting for it", async () => {
		const task = defineTask({
			id: uniqueTaskId("fatal-no-wait"),
			queue: { name: uniqueQueue() },
			retry: fastRetry(3),
			run: async (_p: { v: number }, ctx) => {
				// Started first, still in flight (never settles) when the second call below fails
				// synchronously with a fatal, code-bug error.
				void ctx.step.run("slow", () => new Promise(() => {}));
				await ctx.step.run("slow", () => "dup");
				return "done";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const started = Date.now();
		const run = await waitForStatus(engine, handle.id, "failed", 3_000);
		expect(Date.now() - started).toBeLessThan(3_000);
		expect(run.attempt).toBe(1);
		expect(run.error).toMatchObject({ name: "DuplicateStepError", retryable: false });
		const steps = await engine.runs.steps(handle.id);
		expect(steps).toEqual([]);
	}, 5_000);
});

describe("child tasks not registered in this process, invalid child payloads", () => {
	it("a child registered only in another engine runs there and the parent gets its output", async () => {
		const child = defineTask({
			id: uniqueTaskId("remote-child"),
			queue: { name: uniqueQueue() },
			run: async (p: { n: number }) => ({ doubled: p.n * 2 }),
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: uniqueQueue() },
			retry: fastRetry(3),
			run: async (_p: { v: number }, ctx) => {
				const result = await ctx.step.triggerAndWait("child", child, { n: 21 });
				return result.ok ? result.output : null;
			},
		});
		const parentEngine = await startedEngine([parent]); // does not know `child`
		await startedEngine([child]);
		const handle = await parentEngine.trigger(parent, { v: 1 });
		const run = await waitForStatus(parentEngine, handle.id, "completed");
		expect(run.attempt).toBe(1);
		expect(run.output).toEqual({ doubled: 42 });
	});

	for (const [label, payload] of [
		["a Date", { at: new Date(0) }],
		["a value over maxPayloadBytes", { blob: "x".repeat(4_096) }],
	] as const) {
		it(`a child payload with ${label} fails the parent on attempt 1 without retry`, async () => {
			const child = defineTask({
				id: uniqueTaskId("child"),
				queue: { name: uniqueQueue() },
				run: async (_p: unknown) => null,
			});
			const parent = defineTask({
				id: uniqueTaskId("parent"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					await ctx.step.triggerAndWait("child", child, payload as never);
					return "unreachable";
				},
			});
			const engine = await startedEngine([parent, child], { limits: { maxPayloadBytes: 1_024 } });
			const handle = await engine.trigger(parent, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect(run.attempt).toBe(1);
			expect(run.error).toMatchObject({ name: "SerializationError", retryable: false });
			expect(await childCount(handle.id)).toBe(0);
		});
	}
});

describe("ctx.signal after the attempt ended", () => {
	it("aborts the signal once the run completed, with a 'finished' reason", async () => {
		let signal: AbortSignal | undefined;
		const task = defineTask({
			id: uniqueTaskId("done"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				signal = ctx.signal;
				return "ok";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		await waitForStatus(engine, handle.id, "completed");
		await waitFor(async () => signal?.aborted === true, { timeoutMs: 5_000, intervalMs: 20 });
		expect(String((signal?.reason as Error | undefined)?.message)).toMatch(/finished/);
	});
});

describe("ctx.stream when the attempt ends mid-stream", () => {
	it("an aborted run ends its stream with an error: readers don't block, the key expires", async () => {
		const task = defineTask({
			id: uniqueTaskId("stream"),
			queue: { name: uniqueQueue() },
			maxDurationMs: 300,
			retry: { maxAttempts: 1 },
			run: async (_p: { v: number }, ctx) => {
				async function* hangs(): AsyncGenerator<string> {
					yield "partial";
					await new Promise(() => {}); // a source that ignores ctx.signal
				}
				await ctx.stream("out", hangs());
				return "unreachable";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const received: unknown[] = [];
		await expect(
			(async () => {
				for await (const chunk of engine.runs.readStream(handle.id, "out")) received.push(chunk);
			})()
		).rejects.toThrow(/ended with an error/);
		expect(received).toEqual(["partial"]);
		await waitForStatus(engine, handle.id, "failed");
		expect(await ns.redis.ttl(`${ns.prefix}:stream:${handle.id}:out`)).toBeGreaterThan(0);
	});
});
