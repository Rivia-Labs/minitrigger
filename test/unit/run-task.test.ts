/**
 * `runTask` (testing subpath): runs a task's `run` function directly, no engine/store/queue. Fast
 * unit tests of a single task's logic -- see `src/testing/run-task.ts` for what is (and isn't)
 * exercised compared to `createTestEngine`.
 */
import { describe, expect, it } from "vitest";
import { AbortTaskRunError, defineTask } from "../../src/index.js";
import { runTask } from "../../src/testing/index.js";

describe("runTask: step.run", () => {
	it("runs fn immediately and records the call", async () => {
		const calls: string[] = [];
		const task = defineTask({
			id: "charge",
			run: async (payload: { orderId: string }, ctx) => {
				const charge = await ctx.step.run("charge", async () => {
					calls.push(payload.orderId);
					return { chargeId: `ch_${payload.orderId}` };
				});
				return charge.chargeId;
			},
		});

		const result = await runTask(task, { orderId: "1" });
		expect(result).toMatchObject({ output: "ch_1", error: null });
		expect(calls).toEqual(["1"]);
		expect(result.steps).toEqual([{ name: "charge", kind: "run", output: { chargeId: "ch_1" } }]);
	});

	it("propagates fn's own error, without retry classification (fatal-free path)", async () => {
		const task = defineTask({
			id: "flaky",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("boom", () => {
					throw new Error("nope");
				});
				return "unreachable";
			},
		});
		const result = await runTask(task);
		expect(result.output).toBeNull();
		expect(result.error).toMatchObject({ name: "Error", message: "nope", retryable: true });
	});

	it("AbortTaskRunError marks the result non-retryable", async () => {
		const task = defineTask({
			id: "abort-me",
			run: async () => {
				throw new AbortTaskRunError("give up");
			},
		});
		const result = await runTask(task);
		expect(result.error).toMatchObject({ name: "AbortTaskRunError", retryable: false });
	});

	// Item 4(b): fixed -- this used to assert `retryable: false`, but that doesn't match
	// production (`src/executor/step-context.ts`'s `run()`): an oversized/non-serializable step
	// output is an ordinary `SerializationError` handed back to the task's own code, not a fatal
	// condition. Uncaught here, it becomes a normal retryable failure, same as any other error a
	// step's `fn` might throw -- not fatal, and it doesn't block further `ctx.step.*` calls.
	it("rejects a non-serializable step output as an ordinary (retryable) error, not fatal", async () => {
		const task = defineTask({
			id: "bad-output",
			run: async (_payload: undefined, ctx) => {
				// @ts-expect-error -- exercising the runtime check for non-TS callers.
				await ctx.step.run("bad", () => new Date());
				return "unreachable";
			},
		});
		const result = await runTask(task);
		expect(result.output).toBeNull();
		expect(result.error?.retryable).toBe(true);
		expect(result.error?.message).toMatch(/not serializable/);
	});

	it("a caught oversized/invalid step output does not block later steps (not fatal)", async () => {
		const task = defineTask({
			id: "bad-output-caught",
			run: async (_payload: undefined, ctx) => {
				let caught: string | undefined;
				try {
					// @ts-expect-error -- exercising the runtime check for non-TS callers.
					await ctx.step.run("bad", () => new Date());
				} catch (error) {
					caught = (error as Error).name;
				}
				const after = await ctx.step.run("after", () => "ok");
				return { caught, after };
			},
		});
		const result = await runTask(task);
		expect(result.error).toBeNull();
		expect(result.output).toEqual({ caught: "SerializationError", after: "ok" });
	});

	it("rejects duplicate step names within one run", async () => {
		const task = defineTask({
			id: "dup",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("x", () => 1);
				await ctx.step.run("x", () => 2);
				return "unreachable";
			},
		});
		const result = await runTask(task);
		expect(result.error).toMatchObject({ name: "DuplicateStepError", retryable: false });
	});

	it("rejects an invalid step name (validateStepName reused)", async () => {
		const task = defineTask({
			id: "bad-name",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("has#hash", () => 1);
				return "unreachable";
			},
		});
		const result = await runTask(task);
		expect(result.error).toMatchObject({ name: "ConfigError", retryable: false });
	});
});

describe("runTask: payload/output validation", () => {
	it("rejects a non-serializable payload before running (like trigger())", async () => {
		const task = defineTask({
			id: "typed",
			run: async (_payload: { when: unknown }) => "ok",
		});
		await expect(runTask(task, { when: new Date() })).rejects.toThrow(/not serializable/);
	});

	it("rejects a non-serializable output, without throwing (mirrors production: run fails)", async () => {
		// @ts-expect-error task output is a Date, which is not JSON-serializable
		const task = defineTask({ id: "bad-task-output", run: async () => new Date() });
		const result = await runTask(task);
		expect(result.output).toBeNull();
		expect(result.error?.retryable).toBe(false);
	});

	it("normalizes a void output to null", async () => {
		const task = defineTask({ id: "void-out", run: async () => {} });
		const result = await runTask(task);
		expect(result.output).toBeNull();
		expect(result.error).toBeNull();
	});
});

describe("runTask: sleep", () => {
	it("resolves instantly and records the call", async () => {
		const task = defineTask({
			id: "with-sleep",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.sleep("cool-off", "1d");
				return "done";
			},
		});
		const start = Date.now();
		const result = await runTask(task);
		expect(Date.now() - start).toBeLessThan(100);
		expect(result.output).toBe("done");
		expect(result.steps).toEqual([{ name: "cool-off", kind: "sleep", output: null }]);
	});
});

describe("runTask: tokens", () => {
	it("waitForToken resolves with the given resolver value", async () => {
		const task = defineTask({
			id: "approval",
			run: async (_payload: undefined, ctx) => {
				const result = await ctx.step.waitForToken<{ approved: boolean }>("approval");
				return result.ok ? result.output.approved : "denied";
			},
		});
		const result = await runTask(task, undefined, { tokens: { approval: { approved: true } } });
		expect(result.output).toBe(true);
	});

	it("a resolver may be a function, called lazily", async () => {
		let calls = 0;
		const task = defineTask({
			id: "approval-fn",
			run: async (_payload: undefined, ctx) => {
				const result = await ctx.step.waitForToken("approval");
				return result;
			},
		});
		await runTask(task, undefined, {
			tokens: {
				approval: () => {
					calls++;
					return 42;
				},
			},
		});
		expect(calls).toBe(1);
	});

	it("waitForToken without a resolver throws an actionable error, not a result.error", async () => {
		const task = defineTask({
			id: "unresolved",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.waitForToken("approval");
				return "unreachable";
			},
		});
		await expect(runTask(task)).rejects.toThrow(/no resolver was given/);
		await expect(runTask(task)).rejects.toThrow(/tokens: \{ "approval"/);
	});

	it("createToken alone (never waited) needs no resolver", async () => {
		const task = defineTask({
			id: "create-only",
			run: async (_payload: undefined, ctx) => {
				const handle = await ctx.step.createToken("approval");
				return handle.name;
			},
		});
		const result = await runTask(task);
		expect(result.output).toBe("approval");
		expect(result.error).toBeNull();
	});

	// Item 4(c): the step recorded for `createToken` used to carry `{ id: name }` (the step's own
	// NAME, not the token's id) as its output -- mismatched from the id the caller actually gets
	// back from `ctx.step.createToken`.
	it("records the SAME id in the step output as the handle createToken returns", async () => {
		const task = defineTask({
			id: "create-id-match",
			run: async (_payload: undefined, ctx) => {
				const handle = await ctx.step.createToken("approval");
				return handle.id;
			},
		});
		const result = await runTask(task);
		const step = result.steps.find(s => s.name === "approval");
		expect(step?.kind).toBe("token");
		expect(step?.output).toEqual({ id: result.output });
	});
});

const child = defineTask({
	id: "child-task",
	run: async (payload: { n: number }) => payload.n * 2,
});

describe("runTask: nested steps (item 4a)", () => {
	it("a ctx.step.* call inside a step.run fn is a fatal NestedStepError", async () => {
		const task = defineTask({
			id: "nested",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("outer", async () => {
					await ctx.step.run("inner", () => 1);
					return "unreachable";
				});
				return "unreachable";
			},
		});
		const result = await runTask(task);
		expect(result.output).toBeNull();
		expect(result.error).toMatchObject({ name: "NestedStepError", retryable: false });
	});

	it("is fatal even when the outer step.run's fn catches the nested error", async () => {
		const task = defineTask({
			id: "nested-caught",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("outer", async () => {
					try {
						await ctx.step.run("inner", () => 1);
					} catch {
						// Swallowed -- must not save the caller from the fatal error.
					}
					return "reached-anyway";
				});
				return "unreachable-too";
			},
		});
		const result = await runTask(task);
		expect(result.output).toBeNull();
		expect(result.error).toMatchObject({ name: "NestedStepError", retryable: false });
	});

	it("nesting applies to every step kind (sleep, createToken, trigger)", async () => {
		const nestedSleep = defineTask({
			id: "nested-sleep",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("outer", async () => {
					await ctx.step.sleep("inner-sleep", "1s");
				});
				return "unreachable";
			},
		});
		expect((await runTask(nestedSleep)).error).toMatchObject({ name: "NestedStepError" });

		const nestedToken = defineTask({
			id: "nested-token",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("outer", async () => {
					await ctx.step.createToken("inner-token");
				});
				return "unreachable";
			},
		});
		expect((await runTask(nestedToken)).error).toMatchObject({ name: "NestedStepError" });

		const nestedTrigger = defineTask({
			id: "nested-trigger",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("outer", async () => {
					await ctx.step.trigger("inner-trigger", child, { n: 1 });
				});
				return "unreachable";
			},
		});
		const result = await runTask(nestedTrigger, undefined, {
			children: { "child-task": p => (p as { n: number }).n * 2 },
		});
		expect(result.error).toMatchObject({ name: "NestedStepError" });
	});

	it("does not throw for concurrent (non-nested) steps started before either awaits", async () => {
		const task = defineTask({
			id: "concurrent",
			run: async (_payload: undefined, ctx) => {
				const [a, b] = await Promise.all([ctx.step.run("a", () => 1), ctx.step.run("b", () => 2)]);
				return (a as number) + (b as number);
			},
		});
		const result = await runTask(task);
		expect(result.output).toBe(3);
		expect(result.error).toBeNull();
	});
});

describe("runTask: trigger/triggerAndWait/batchTriggerAndWait", () => {
	it("trigger uses the provided stub and records it in `triggered`", async () => {
		const task = defineTask({
			id: "fire-and-forget",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.trigger("kick-off", child, { n: 3 });
				return "done";
			},
		});
		const result = await runTask(task, undefined, {
			children: { "child-task": payload => (payload as { n: number }).n * 2 },
		});
		expect(result.output).toBe("done");
		expect(result.triggered).toEqual([
			{ name: "kick-off", taskId: "child-task", mode: "trigger", payload: { n: 3 }, output: 6 },
		]);
	});

	it("triggerAndWait returns the stub's output as a WaitResult", async () => {
		const task = defineTask({
			id: "wait-for-child",
			run: async (_payload: undefined, ctx) => {
				const result = await ctx.step.triggerAndWait("double", child, { n: 5 });
				return result.ok ? result.output : "failed";
			},
		});
		const result = await runTask(task, undefined, {
			children: { "child-task": payload => (payload as { n: number }).n * 2 },
		});
		expect(result.output).toBe(10);
	});

	it("triggerAndWait turns a throwing stub into { ok: false } instead of failing the run", async () => {
		const task = defineTask({
			id: "wait-for-flaky-child",
			run: async (_payload: undefined, ctx) => {
				const result = await ctx.step.triggerAndWait("double", child, { n: 5 });
				return result.ok ? "ok" : result.error.message;
			},
		});
		const result = await runTask(task, undefined, {
			children: {
				"child-task": () => {
					throw new Error("child blew up");
				},
			},
		});
		expect(result.output).toBe("child blew up");
	});

	it("batchTriggerAndWait runs the stub per payload, in order", async () => {
		const task = defineTask({
			id: "batch",
			run: async (_payload: undefined, ctx) => {
				const results = await ctx.step.batchTriggerAndWait("batch", child, [
					{ n: 1 },
					{ n: 2 },
					{ n: 3 },
				]);
				return results.map(r => (r.ok ? r.output : null));
			},
		});
		const result = await runTask(task, undefined, {
			children: { "child-task": payload => (payload as { n: number }).n * 10 },
		});
		expect(result.output).toEqual([10, 20, 30]);
	});

	it("batchTriggerAndWait with [] needs no stub and returns []", async () => {
		const task = defineTask({
			id: "empty-batch",
			run: async (_payload: undefined, ctx) => {
				return ctx.step.batchTriggerAndWait("batch", child, []);
			},
		});
		const result = await runTask(task);
		expect(result.output).toEqual([]);
	});

	it("trigger without a stub throws an actionable error", async () => {
		const task = defineTask({
			id: "missing-stub",
			run: async (_payload: undefined, ctx) => {
				await ctx.step.trigger("kick-off", child, { n: 1 });
				return "unreachable";
			},
		});
		await expect(runTask(task)).rejects.toThrow(/no stub was given/);
		await expect(runTask(task)).rejects.toThrow(/children: \{ "child-task"/);
	});
});

describe("runTask: logger/progress/stream/signal", () => {
	it("ctx.logger collects entries", async () => {
		const task = defineTask({
			id: "logs",
			run: async (_payload: undefined, ctx) => {
				ctx.logger.info("hello", { a: 1 });
				ctx.logger.warn("careful");
				return "ok";
			},
		});
		const result = await runTask(task);
		expect(result.logs).toEqual([
			{ level: "info", message: "hello", data: { a: 1 } },
			{ level: "warn", message: "careful", data: null },
		]);
	});

	it("ctx.progress.set records every value in order", async () => {
		const task = defineTask({
			id: "progress",
			run: async (_payload: undefined, ctx) => {
				await ctx.progress.set({ pct: 10 });
				await ctx.progress.set({ pct: 90 });
				return "ok";
			},
		});
		const result = await runTask(task);
		expect(result.progress).toEqual([{ pct: 10 }, { pct: 90 }]);
	});

	it("ctx.stream collects items in order, per stream name", async () => {
		const task = defineTask({
			id: "streaming",
			run: async (_payload: undefined, ctx) => {
				async function* gen() {
					yield "a";
					yield "b";
				}
				await ctx.stream("tokens", gen());
				return "ok";
			},
		});
		const result = await runTask(task);
		expect(result.streams).toEqual({ tokens: ["a", "b"] });
	});

	it("ctx.signal is never aborted unless options.signal is given", async () => {
		const task = defineTask({
			id: "signal-default",
			run: async (_payload: undefined, ctx) => ctx.signal.aborted,
		});
		expect((await runTask(task)).output).toBe(false);

		const controller = new AbortController();
		controller.abort();
		const aborted = defineTask({
			id: "signal-given",
			run: async (_payload: undefined, ctx) => ctx.signal.aborted,
		});
		expect((await runTask(aborted, undefined, { signal: controller.signal })).output).toBe(true);
	});

	it("ctx.run.attempt defaults to 1 and is overridable", async () => {
		const task = defineTask({
			id: "attempt",
			run: async (_payload: undefined, ctx) => ctx.run.attempt,
		});
		expect((await runTask(task)).output).toBe(1);
		expect((await runTask(task, undefined, { attempt: 5 })).output).toBe(5);
	});
});

describe("runTask: task with no payload", () => {
	it("can be called without a payload argument", async () => {
		const task = defineTask({ id: "no-payload", run: async () => "ok" });
		const result = await runTask(task);
		expect(result.output).toBe("ok");
	});
});
