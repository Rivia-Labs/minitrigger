import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { ExecutionLostError } from "../../src/errors.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import {
	describeTransports,
	type EngineOverrides,
	engineConfigFor,
} from "../helpers/engine-factory.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

describeTransports("steps", transport => {
	const ns = useTestNamespace();

	const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
	const shortId = () => randomUUID().slice(0, 8);
	const uniqueQueue = () => `q${shortId()}`;
	const uniqueTaskId = (label: string) => `${label}-${shortId()}`;
	/** Retry right away (tests force replays by failing an attempt). */
	const fastRetry = (maxAttempts: number) => ({
		maxAttempts,
		backoff: { type: "fixed" as const, baseMs: 10, jitter: false },
	});

	type LogRecord = { level: string; message: string; fields: Record<string, unknown> };
	const logs: LogRecord[] = [];
	const testLogger: EngineLogger = {
		debug: (fields, message) => logs.push({ level: "debug", message, fields }),
		info: (fields, message) => logs.push({ level: "info", message, fields }),
		warn: (fields, message) => logs.push({ level: "warn", message, fields }),
		error: (fields, message) => logs.push({ level: "error", message, fields }),
	};

	const engines: Engine[] = [];

	function makeEngine(tasks: AnyTask[], overrides: EngineOverrides = {}): Engine {
		const engine = createEngine(
			engineConfigFor(transport, ns, {
				tasks,
				pool: { max: 5 },
				heartbeatIntervalMs: 200,
				heartbeatTimeoutMs: 1_000,
				stallTimeoutMs: 5_000,
				shutdownTimeoutMs: 2_000,
				logger: testLogger,
				...overrides,
			})
		);
		engines.push(engine);
		return engine;
	}

	async function startedEngine(tasks: AnyTask[], overrides: EngineOverrides = {}) {
		const engine = makeEngine(tasks, overrides);
		await engine.start();
		return engine;
	}

	interface StepRow {
		name: string;
		seq_index: number;
		status: string;
		output: unknown;
		wake_at: Date | null;
	}

	async function stepRows(runId: string): Promise<StepRow[]> {
		const { rows } = await ns.pool.query<StepRow>(
			`SELECT name, seq_index, status, output, wake_at FROM ${escapeIdentifier(ns.schema)}.steps
			 WHERE run_id = $1 ORDER BY seq_index`,
			[runId]
		);
		return rows;
	}

	async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 10_000) {
		await waitFor(async () => (await engine.runs.get(id))?.status === status, {
			timeoutMs,
			intervalMs: 25,
		});
		const run = await engine.runs.get(id);
		if (!run) throw new Error("unreachable");
		return run;
	}

	/** Waits until the sleep step `name` is recorded (`waiting`): the execution is suspending. */
	async function waitForSleepRecorded(runId: string, name: string) {
		await waitFor(
			async () => (await stepRows(runId)).some(s => s.name === name && s.status === "waiting"),
			{ timeoutMs: 10_000, intervalMs: 10 }
		);
	}

	function deferred<T = void>() {
		let resolve!: (value: T) => void;
		const promise = new Promise<T>(r => {
			resolve = r;
		});
		return { promise, resolve };
	}

	beforeAll(async () => {
		const engine = createEngine(
			engineConfigFor(transport, ns, {
				tasks: [],
			})
		);
		await engine.migrate();
		await engine.stop();
	});

	afterEach(async () => {
		await Promise.all(engines.splice(0).map(e => e.stop()));
		logs.length = 0;
	});

	describe("steps: memoized replay", () => {
		it("a crash after 2 of 4 steps resumes without re-running steps 1-2", async () => {
			const calls = { a: 0, b: 0, c: 0, d: 0 };
			let blockC = true;
			const task = defineTask({
				id: uniqueTaskId("crash"),
				queue: { name: uniqueQueue() },
				run: async (_p: { v: number }, ctx) => {
					const a = await ctx.step.run("a", () => {
						calls.a++;
						return 1;
					});
					const b = await ctx.step.run("b", async () => {
						calls.b++;
						return 2;
					});
					const c = await ctx.step.run("c", async () => {
						calls.c++;
						if (blockC) {
							// "Crash": the process goes away while step 3 runs.
							await new Promise(resolve => ctx.signal.addEventListener("abort", resolve));
							throw new Error("process died");
						}
						return 3;
					});
					const d = await ctx.step.run("d", () => {
						calls.d++;
						return 4;
					});
					return a + b + c + d;
				},
			});
			const first = await startedEngine([task], { shutdownTimeoutMs: 100 });
			const handle = await first.trigger(task, { v: 1 });
			await waitFor(() => calls.c === 1);
			expect((await stepRows(handle.id)).map(s => s.name)).toEqual(["a", "b"]);
			blockC = false;
			await first.stop(); // hands the run back (decision 20)

			const second = await startedEngine([task]);
			const run = await waitForStatus(second, handle.id, "completed");
			expect(run.output).toBe(10);
			expect(run.attempt).toBe(1);
			expect(calls).toEqual({ a: 1, b: 1, c: 2, d: 1 });
			expect((await stepRows(handle.id)).map(s => [s.name, s.seq_index])).toEqual([
				["a", 0],
				["b", 1],
				["c", 2],
				["d", 3],
			]);
		});

		it("a failed attempt in step 3 retries without re-running steps 1-2", async () => {
			const calls = { a: 0, b: 0, c: 0, d: 0 };
			const task = defineTask({
				id: uniqueTaskId("retry-steps"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					await ctx.step.run("a", () => ++calls.a);
					await ctx.step.run("b", () => ++calls.b);
					await ctx.step.run("c", () => {
						calls.c++;
						if (calls.c === 1) throw new Error("transient");
						return calls.c;
					});
					await ctx.step.run("d", () => ++calls.d);
					return "done";
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.attempt).toBe(2);
			expect(calls).toEqual({ a: 1, b: 1, c: 2, d: 1 });
		});

		it("Promise.all of 3 step.run is deterministic on replay", async () => {
			const calls = { a: 0, b: 0, c: 0 };
			const seen: string[][] = [];
			const task = defineTask({
				id: uniqueTaskId("parallel"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(2),
				run: async (_p: { v: number }, ctx) => {
					// Completion order (c, b, a) differs from call order (a, b, c).
					const results = await Promise.all([
						ctx.step.run("a", async () => {
							calls.a++;
							await sleep(80);
							return "A";
						}),
						ctx.step.run("b", async () => {
							calls.b++;
							await sleep(40);
							return "B";
						}),
						ctx.step.run("c", async () => {
							calls.c++;
							return "C";
						}),
					]);
					seen.push(results);
					if (ctx.run.attempt === 1) throw new Error("force a replay");
					return results;
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.attempt).toBe(2);
			expect(run.output).toEqual(["A", "B", "C"]);
			expect(seen).toEqual([
				["A", "B", "C"],
				["A", "B", "C"],
			]);
			expect(calls).toEqual({ a: 1, b: 1, c: 1 });
			expect((await stepRows(handle.id)).map(s => [s.name, s.seq_index])).toEqual([
				["a", 0],
				["b", 1],
				["c", 2],
			]);
		});

		it("string, array, void, null and falsy step outputs round-trip", async () => {
			const seen: unknown[][] = [];
			const task = defineTask({
				id: uniqueTaskId("roundtrip"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(2),
				run: async (_p: { v: number }, ctx) => {
					const values: unknown[] = [];
					values.push(await ctx.step.run("string", () => "hello"));
					values.push(await ctx.step.run("array", () => [1, "two", { three: 3 }, [null]]));
					values.push(await ctx.step.run("void", async () => {}));
					values.push(await ctx.step.run("null", () => null));
					values.push(await ctx.step.run("zero", () => 0));
					values.push(await ctx.step.run("false", () => false));
					values.push(await ctx.step.run("empty-string", () => ""));
					values.push(await ctx.step.run("object", () => ({ nested: { list: [1, 2] } })));
					seen.push(values);
					if (ctx.run.attempt === 1) throw new Error("force a replay");
					return "ok";
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			await waitForStatus(engine, handle.id, "completed");
			const expected = [
				"hello",
				[1, "two", { three: 3 }, [null]],
				null, // void is stored (and returned) as null
				null,
				0,
				false,
				"",
				{ nested: { list: [1, 2] } },
			];
			expect(seen).toEqual([expected, expected]);
			expect((await stepRows(handle.id)).map(s => s.output)).toEqual(expected);
		});

		it("replays correctly when outputs are deferred (tiny maxInlineStepOutputBytes)", async () => {
			const STEPS = 120; // spans several deferred-output batches
			const calls = new Array<number>(STEPS).fill(0);
			let replayed: number[] = [];
			const task = defineTask({
				id: uniqueTaskId("deferred"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(2),
				run: async (_p: { v: number }, ctx) => {
					const outputs: number[] = [];
					// Half sequential, half concurrent (both paths hit the lazy loader).
					for (let i = 0; i < STEPS / 2; i++) {
						const out = await ctx.step.run(`s${i}`, () => {
							calls[i] = (calls[i] ?? 0) + 1;
							return { i, pad: "x".repeat(20) };
						});
						outputs.push(out.i);
					}
					const rest = await Promise.all(
						Array.from({ length: STEPS / 2 }, (_, k) => {
							const i = STEPS / 2 + k;
							return ctx.step.run(`s${i}`, async () => {
								calls[i] = (calls[i] ?? 0) + 1;
								return { i, pad: "y".repeat(20) };
							});
						})
					);
					outputs.push(...rest.map(r => r.i));
					if (ctx.run.attempt === 1) throw new Error("force a replay");
					replayed = outputs;
					return outputs.length;
				},
			});
			const engine = await startedEngine([task], { limits: { maxInlineStepOutputBytes: 10 } });
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.output).toBe(STEPS);
			expect(replayed).toEqual(Array.from({ length: STEPS }, (_, i) => i));
			expect(calls.every(c => c === 1)).toBe(true);
		});
	});

	describe("steps: determinism", () => {
		it("a changed step order between attempts fails the run with NonDeterminismError, without retry", async () => {
			let swapOrder = false;
			const task = defineTask({
				id: uniqueTaskId("nondet"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(5),
				run: async (_p: { v: number }, ctx) => {
					const order = swapOrder ? ["b", "a"] : ["a", "b"];
					for (const name of order) await ctx.step.run(name, () => name);
					swapOrder = true; // the "next deploy" changes the code
					throw new Error("force a replay");
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect(run.attempt).toBe(2);
			expect(run.error).toMatchObject({ name: "NonDeterminismError", retryable: false });
			expect(run.error?.message).toMatch(/step #0 is "b" now, but was "a"/);
		});

		it("a saved step name at a different seq_index is NonDeterminismError even if caught", async () => {
			let insertExtra = false;
			const task = defineTask({
				id: uniqueTaskId("nondet-shift"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(5),
				run: async (_p: { v: number }, ctx) => {
					try {
						// Attempt 2 inserts a new step at index 0: "a" moves to index 1.
						if (insertExtra) await ctx.step.run("new", () => 0);
						await ctx.step.run("a", () => 1);
					} catch {
						return "swallowed"; // catching it does not rescue the run
					}
					insertExtra = true;
					throw new Error("force a replay");
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect(run.attempt).toBe(2);
			expect(run.error?.name).toBe("NonDeterminismError");
			expect(run.output).toBeNull();
		});

		it("a repeated step name fails the run with DuplicateStepError, without retry", async () => {
			let secondCalled = false;
			const task = defineTask({
				id: uniqueTaskId("dup"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					await ctx.step.run("same", () => 1);
					await ctx.step.run("same", () => {
						secondCalled = true;
						return 2;
					});
					return "unreachable";
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect(run.attempt).toBe(1);
			expect(run.error).toMatchObject({ name: "DuplicateStepError", retryable: false });
			expect(secondCalled).toBe(false);
		});

		it("an invalid step name ('#') fails the run without retry and consumes no seq_index", async () => {
			let error: unknown;
			let goodCalled = false;
			const task = defineTask({
				id: uniqueTaskId("hash"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					try {
						await ctx.step.run("bad#name", () => 1);
					} catch (e) {
						error = e;
					}
					// A code bug: later steps rethrow it instead of running.
					return ctx.step.run("good", () => {
						goodCalled = true;
						return 2;
					});
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect((error as Error).message).toMatch(/"#" is reserved/);
			expect(run.attempt).toBe(1);
			expect(run.error).toMatchObject({ name: "ConfigError", retryable: false });
			expect(goodCalled).toBe(false);
			expect(await stepRows(handle.id)).toEqual([]);
		});

		it("ctx.step.run with a non-function fn fails the run without retry (fatal, like an invalid name)", async () => {
			let error: unknown;
			let goodCalled = false;
			const task = defineTask({
				id: uniqueTaskId("badfn"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					try {
						// @ts-expect-error -- exercising the runtime check for non-TS callers.
						await ctx.step.run("bad-fn", "not a function");
					} catch (e) {
						error = e;
					}
					return ctx.step.run("good", () => {
						goodCalled = true;
						return 2;
					});
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect((error as Error).message).toMatch(/fn must be a function/);
			expect(run.attempt).toBe(1);
			expect(run.error).toMatchObject({ name: "ConfigError", retryable: false });
			expect(goodCalled).toBe(false);
			expect(await stepRows(handle.id)).toEqual([]);
		});

		it("ctx.step.sleep with an invalid duration fails the run without retry (fatal, like an invalid name)", async () => {
			let error: unknown;
			let goodCalled = false;
			const task = defineTask({
				id: uniqueTaskId("badduration"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					try {
						// @ts-expect-error -- exercising the runtime check for non-TS callers.
						await ctx.step.sleep("nap", "not-a-duration");
					} catch (e) {
						error = e;
					}
					return ctx.step.run("good", () => {
						goodCalled = true;
						return 2;
					});
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect((error as Error).message).toMatch(/invalid duration/);
			expect(run.attempt).toBe(1);
			expect(run.error).toMatchObject({ name: "ConfigError", retryable: false });
			expect(goodCalled).toBe(false);
			expect(await stepRows(handle.id)).toEqual([]);
		});

		it("ctx.step.batchTriggerAndWait with non-array payloads fails the run without retry (fatal)", async () => {
			let error: unknown;
			let goodCalled = false;
			const child = defineTask({
				id: uniqueTaskId("badbatch-child"),
				queue: { name: uniqueQueue() },
				run: async () => "child",
			});
			const task = defineTask({
				id: uniqueTaskId("badbatch"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					try {
						// @ts-expect-error -- exercising the runtime check for non-TS callers.
						await ctx.step.batchTriggerAndWait("kids", child, "not-an-array");
					} catch (e) {
						error = e;
					}
					return ctx.step.run("good", () => {
						goodCalled = true;
						return 2;
					});
				},
			});
			const engine = await startedEngine([child, task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect((error as Error).message).toMatch(/payloads must be an array/);
			expect(run.attempt).toBe(1);
			expect(run.error).toMatchObject({ name: "ConfigError", retryable: false });
			expect(goodCalled).toBe(false);
			expect(await stepRows(handle.id)).toEqual([]);
		});

		it("a step called inside a step.run fn fails the run with NestedStepError, without retry", async () => {
			const calls = { before: 0, outer: 0, innerFn: 0, sideEffect: 0 };
			const task = defineTask({
				id: uniqueTaskId("nested"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					await ctx.step.run("before", () => ++calls.before);
					await ctx.step.run("outer", async () => {
						calls.outer++;
						calls.sideEffect++; // a side effect of the outer fn before the nested call
						try {
							await ctx.step.run("inner", () => ++calls.innerFn);
						} catch {
							// swallowing the nested-step error does not rescue the run
						}
						return "outer";
					});
					return "unreachable";
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "failed");
			expect(run.attempt).toBe(1);
			expect(run.error).toMatchObject({ name: "NestedStepError", retryable: false });
			expect(run.error?.message).toContain(
				'ctx.step.run("inner") called inside step.run("outer"): nested steps are not supported'
			);
			expect(calls).toEqual({ before: 1, outer: 1, innerFn: 0, sideEffect: 1 });
			// Neither the inner step nor the outer one was recorded: no replay can be corrupted.
			expect((await stepRows(handle.id)).map(s => [s.name, s.seq_index])).toEqual([["before", 0]]);
		});
	});

	describe("steps: ownership", () => {
		it("a step finishing after the run was canceled writes nothing and throws ExecutionLostError", async () => {
			const started = deferred();
			const gate = deferred();
			let captured: unknown;
			let signalAborted: boolean | undefined;
			let afterCalled = false;
			const task = defineTask({
				id: uniqueTaskId("lost"),
				queue: { name: uniqueQueue() },
				run: async (_p: { v: number }, ctx) => {
					try {
						await ctx.step.run("slow", async () => {
							started.resolve();
							await gate.promise; // ignores ctx.signal
							return "late";
						});
					} catch (error) {
						captured = error;
						signalAborted = ctx.signal.aborted;
					}
					// The execution is dead: further steps are refused at the boundary.
					await ctx.step.run("after", () => {
						afterCalled = true;
						return 1;
					});
					return "unreachable";
				},
			});
			const worker = await startedEngine([task], {
				heartbeatIntervalMs: 30_000,
				stallTimeoutMs: 60_000,
			});
			// A different engine cancels, so no local abort reaches the execution: only the guarded
			// insert can notice the lost ownership (decision 5).
			const client = makeEngine([task]);
			const handle = await worker.trigger(task, { v: 1 });
			await started.promise;
			expect(await client.runs.cancel(handle.id)).toBe("canceled");
			gate.resolve();

			await waitFor(() => captured !== undefined);
			expect(captured).toBeInstanceOf(ExecutionLostError);
			expect(signalAborted).toBe(true);
			await sleep(100);
			expect(afterCalled).toBe(false);
			expect(await stepRows(handle.id)).toEqual([]);
			const run = await client.runs.get(handle.id);
			expect(run?.status).toBe("canceled");
			expect(run?.output).toBeNull();
		});
	});

	describe("steps: sleep and suspension", () => {
		it("a ~1 s sleep resumes the run without holding the worker slot", async () => {
			const queue = { name: uniqueQueue(), concurrency: 1 };
			let beforeSleep = 0;
			const sleeper = defineTask({
				id: uniqueTaskId("sleeper"),
				queue,
				run: async (_p: { v: number }, ctx) => {
					await ctx.step.run("before", () => ++beforeSleep);
					await ctx.step.sleep("nap", "1s");
					return "woke";
				},
			});
			const quick = defineTask({
				id: uniqueTaskId("quick"),
				queue: { name: queue.name },
				run: async (_p: { v: number }) => "quick",
			});
			const engine = await startedEngine([sleeper, quick]);
			const a = await engine.trigger(sleeper, { v: 1 });
			const waiting = await waitForStatus(engine, a.id, "waiting");
			expect(waiting.runAfter).toBeInstanceOf(Date);
			const b = await engine.trigger(quick, { v: 1 });
			const quickRun = await waitForStatus(engine, b.id, "completed");
			expect((await engine.runs.get(a.id))?.status).toBe("waiting");

			const done = await waitForStatus(engine, a.id, "completed");
			expect(done.output).toBe("woke");
			expect(done.attempt).toBe(1);
			expect(beforeSleep).toBe(1);
			expect(quickRun.finishedAt?.getTime()).toBeLessThan(done.finishedAt?.getTime() ?? 0);
			const [before, nap] = await stepRows(a.id);
			expect(before?.name).toBe("before");
			expect(nap).toMatchObject({ name: "nap", seq_index: 1, status: "completed" });
			// Resumed no earlier than wake_at (DB clock).
			expect(done.finishedAt?.getTime()).toBeGreaterThanOrEqual(nap?.wake_at?.getTime() ?? 0);
			expect((nap?.wake_at?.getTime() ?? 0) - (done.startedAt?.getTime() ?? 0)).toBeGreaterThan(
				900
			);
		});

		it("Promise.all([step.run, step.sleep]) does not re-run the step on replay", async () => {
			let runCalls = 0;
			const gate = deferred();
			const task = defineTask({
				id: uniqueTaskId("run-and-sleep"),
				queue: { name: uniqueQueue() },
				run: async (_p: { v: number }, ctx) => {
					const [value] = await Promise.all([
						ctx.step.run("work", async () => {
							runCalls++;
							await gate.promise; // still running when the sleep suspends
							return "R";
						}),
						ctx.step.sleep("nap", 100),
					]);
					return value;
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			await waitForSleepRecorded(handle.id, "nap");
			// The suspension is waiting for the in-flight step (decision 2).
			expect((await engine.runs.get(handle.id))?.status).toBe("running");
			gate.resolve();
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.output).toBe("R");
			expect(run.attempt).toBe(1);
			expect(runCalls).toBe(1);
			expect((await stepRows(handle.id)).map(s => [s.name, s.seq_index, s.status])).toEqual([
				["work", 0, "completed"],
				["nap", 1, "completed"],
			]);
		});

		it("try/catch around step.sleep neither breaks the replay nor causes a retry", async () => {
			let executions = 0;
			let caught = 0;
			let afterCalls = 0;
			const task = defineTask({
				id: uniqueTaskId("catch-sleep"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					executions++;
					try {
						await ctx.step.sleep("nap", 300);
					} catch {
						caught++; // misuse: swallows the suspension signal
					}
					// Rethrows the pending suspension: the function cannot move past the sleep.
					await ctx.step.run("after", () => ++afterCalls);
					return "done";
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.output).toBe("done");
			expect(run.attempt).toBe(1);
			expect(run.error).toBeNull();
			expect(executions).toBe(2);
			expect(caught).toBe(1);
			expect(afterCalls).toBe(1);
		});

		it("a function that swallows the suspension and returns stays suspended (warns)", async () => {
			let executions = 0;
			const task = defineTask({
				id: uniqueTaskId("swallow"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					executions++;
					try {
						await ctx.step.sleep("nap", 200);
					} catch {
						return `swallowed ${executions}`;
					}
					return `woke ${executions}`;
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.output).toBe("woke 2");
			expect(run.attempt).toBe(1);
			expect(
				logs.some(
					l => l.level === "warn" && l.message.includes("did not end with the suspension signal")
				)
			).toBe(true);
		});

		it("an in-flight step failing during the suspension settle re-runs on replay without counting an attempt", async () => {
			let flakyCalls = 0;
			const gate = deferred();
			const task = defineTask({
				id: uniqueTaskId("flaky-inflight"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				run: async (_p: { v: number }, ctx) => {
					const [value] = await Promise.all([
						ctx.step.run("flaky", async () => {
							flakyCalls++;
							if (flakyCalls === 1) {
								await gate.promise;
								throw new Error("boom during settle");
							}
							return "ok";
						}),
						ctx.step.sleep("nap", 100),
					]);
					return value;
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			await waitForSleepRecorded(handle.id, "nap");
			expect((await engine.runs.get(handle.id))?.status).toBe("running");
			gate.resolve();
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.output).toBe("ok");
			expect(run.attempt).toBe(1);
			expect(run.error).toBeNull();
			expect(flakyCalls).toBe(2);
		});

		it("a sleep replayed before it is due suspends again (DB clock decides)", async () => {
			let executions = 0;
			const task = defineTask({
				id: uniqueTaskId("early"),
				queue: { name: uniqueQueue() },
				run: async (_p: { v: number }, ctx) => {
					executions++;
					await ctx.step.sleep("nap", 3_000);
					return executions;
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			await waitForStatus(engine, handle.id, "waiting");
			if (transport === "postgres") {
				// Wake it early: the claim picks up a `waiting` run whose run_after is due (decision 1,
				// "No modo Postgres"), so moving run_after to now() is the Postgres-mode equivalent.
				await ns.pool.query(
					`UPDATE ${escapeIdentifier(ns.schema)}.runs SET run_after = now() WHERE id = $1`,
					[handle.id]
				);
				await waitFor(() => executions === 2);
				const early = await waitForStatus(engine, handle.id, "waiting");
				expect(early.runAfter).toBeInstanceOf(Date);
				const run = await waitForStatus(engine, handle.id, "completed");
				expect(run.output).toBe(3);
				expect(run.attempt).toBe(1);
				return;
			}
			// Wake it early (as a reconciler/stale path could): bump the seq and enqueue now.
			const { rows } = await ns.pool.query<{ enqueue_seq: number; queue: string }>(
				`UPDATE ${escapeIdentifier(ns.schema)}.runs SET enqueue_seq = enqueue_seq + 1
				 WHERE id = $1 RETURNING enqueue_seq, queue`,
				[handle.id]
			);
			const row = rows[0];
			if (!row) throw new Error("unreachable");
			const connection = new Redis(ns.redisUrl, { maxRetriesPerRequest: null });
			const q = new Queue(row.queue, { connection, prefix: ns.prefix });
			try {
				await q.add(
					"run",
					{ runId: handle.id, seq: row.enqueue_seq },
					{ jobId: `${handle.id}-${row.enqueue_seq}` }
				);
				await waitFor(() => executions === 2);
				const early = await waitForStatus(engine, handle.id, "waiting");
				expect(early.runAfter).toBeInstanceOf(Date);
				const run = await waitForStatus(engine, handle.id, "completed");
				expect(run.output).toBe(3);
				expect(run.attempt).toBe(1);
			} finally {
				await q.close();
				connection.disconnect();
			}
		});

		it("Promise.all of 2 sleeps suspends until the later wake_at and resumes exactly once", async () => {
			let executions = 0;
			const task = defineTask({
				id: uniqueTaskId("two-sleeps"),
				queue: { name: uniqueQueue() },
				run: async (_p: { v: number }, ctx) => {
					executions++;
					await Promise.all([ctx.step.sleep("short", 200), ctx.step.sleep("long", 1_200)]);
					return executions;
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const waiting = await waitForStatus(engine, handle.id, "waiting");
			const rows = await stepRows(handle.id);
			expect(rows.map(r => [r.name, r.status])).toEqual([
				["short", "waiting"],
				["long", "waiting"],
			]);
			const maxWake = Math.max(...rows.map(r => r.wake_at?.getTime() ?? 0));
			expect(waiting.runAfter?.getTime()).toBe(maxWake);

			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.output).toBe(2);
			expect(run.attempt).toBe(1);
			expect(executions).toBe(2);
			expect(run.finishedAt?.getTime()).toBeGreaterThanOrEqual(maxWake);
		});

		it("a cancel during the pre-suspension settle writes nothing more", async () => {
			const gate = deferred();
			let executions = 0;
			let workDone = false;
			const task = defineTask({
				id: uniqueTaskId("cancel-settle"),
				queue: { name: uniqueQueue() },
				run: async (_p: { v: number }, ctx) => {
					executions++;
					await Promise.all([
						ctx.step.run("work", async () => {
							await gate.promise; // ignores ctx.signal
							workDone = true;
							return "late";
						}),
						ctx.step.sleep("nap", 100),
					]);
					return "unreachable";
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			await waitForSleepRecorded(handle.id, "nap");
			expect(await engine.runs.cancel(handle.id)).toBe("canceled");
			const canceled = await engine.runs.get(handle.id);
			gate.resolve();
			await waitFor(() => workDone);
			await sleep(300);

			const run = await engine.runs.get(handle.id);
			expect(run?.status).toBe("canceled");
			expect(run?.updatedAt.getTime()).toBe(canceled?.updatedAt.getTime());
			expect(run?.runAfter).toBeNull();
			expect(executions).toBe(1);
			// The late step was rejected by the ownership guard; the suspension never ran.
			expect((await stepRows(handle.id)).map(s => [s.name, s.status])).toEqual([
				["nap", "waiting"],
			]);
		});

		it("a maxDurationMs timeout during the pre-suspension settle suspends (no attempt counted)", async () => {
			let slowCalls = 0;
			const task = defineTask({
				id: uniqueTaskId("timeout-settle"),
				queue: { name: uniqueQueue() },
				retry: fastRetry(3),
				maxDurationMs: 400,
				run: async (_p: { v: number }, ctx) => {
					const [value] = await Promise.all([
						ctx.step.run("slow", async () => {
							slowCalls++;
							if (slowCalls === 1) {
								// Outlives maxDurationMs while the sleep suspends; stops on ctx.signal.
								await new Promise((_, reject) =>
									ctx.signal.addEventListener("abort", () => reject(ctx.signal.reason))
								);
							}
							return "S";
						}),
						ctx.step.sleep("nap", 100),
					]);
					return value;
				},
			});
			const engine = await startedEngine([task]);
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.output).toBe("S");
			expect(run.attempt).toBe(1);
			expect(run.error).toBeNull();
			expect(slowCalls).toBe(2);
			expect((await stepRows(handle.id)).map(s => [s.name, s.status])).toEqual([
				["slow", "completed"],
				["nap", "completed"],
			]);
		});

		it("replays deferred outputs across a batch boundary after a sleep", async () => {
			const BEFORE = 55; // crosses the 50-step deferred-output batch boundary
			const calls = new Array<number>(BEFORE + 5).fill(0);
			let executions = 0;
			let final: number[] = [];
			const task = defineTask({
				id: uniqueTaskId("deferred-sleep"),
				queue: { name: uniqueQueue() },
				run: async (_p: { v: number }, ctx) => {
					executions++;
					const seen: number[] = [];
					for (let i = 0; i < BEFORE; i++) {
						const out = await ctx.step.run(`s${i}`, () => {
							calls[i] = (calls[i] ?? 0) + 1;
							return { i, pad: "x".repeat(20) };
						});
						seen.push(out.i);
					}
					await ctx.step.sleep("nap", 100);
					for (let i = BEFORE; i < BEFORE + 5; i++) {
						const out = await ctx.step.run(`s${i}`, () => {
							calls[i] = (calls[i] ?? 0) + 1;
							return { i, pad: "y".repeat(20) };
						});
						seen.push(out.i);
					}
					final = seen;
					return seen.length;
				},
			});
			const engine = await startedEngine([task], { limits: { maxInlineStepOutputBytes: 10 } });
			const handle = await engine.trigger(task, { v: 1 });
			const run = await waitForStatus(engine, handle.id, "completed");
			expect(run.output).toBe(BEFORE + 5);
			expect(run.attempt).toBe(1);
			expect(executions).toBe(2);
			expect(final).toEqual(Array.from({ length: BEFORE + 5 }, (_, i) => i));
			expect(calls.every(c => c === 1)).toBe(true);
		});
	});
});
