/**
 * Audit fixes on the step/executor boundary: `:` reserved in step names (collision with the
 * internal `__child:*` keys, decisions 9/11) and `ctx.signal` aborted once the attempt ended.
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

describe("ctx.signal after the attempt ended", () => {
	it("aborts a still-running step of a failed attempt before the retry runs it again", async () => {
		const abortedDuringB: Array<{ attempt: number; aborted: boolean }> = [];
		const bStarts: number[] = [];
		const task = defineTask({
			id: uniqueTaskId("finished"),
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
						if (attempt === 1) {
							// Long side effect: by the time it would continue, the attempt is over.
							await sleep(1_000);
							abortedDuringB.push({ attempt, aborted: ctx.signal.aborted });
						}
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
		await waitFor(async () => abortedDuringB.length === 1, { timeoutMs: 5_000, intervalMs: 20 });
		expect(bStarts).toEqual([1, 2]);
		expect(abortedDuringB).toEqual([{ attempt: 1, aborted: true }]);
	});

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
