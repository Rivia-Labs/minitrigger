import { describe, expectTypeOf, it } from "vitest";
import type { Engine } from "../../src/engine.js";
import type { StepApi, TaskOutput, TaskPayload, WaitResult } from "../../src/task.js";
import { defineTask } from "../../src/task.js";

/**
 * Compile-time only assertions for `defineTask`/`StepApi` (Phase 3 task 3.1).
 * As with `serializable-types.test.ts`, the interesting checks are the type
 * errors `pnpm typecheck` would raise on a regression, and the
 * `// @ts-expect-error` cases, which fail typecheck if the constraint stops
 * rejecting what it should. Nothing here needs to run: functions that take a
 * `StepApi`/`Task` value are declared but never invoked.
 */

describe("defineTask: P/O inference and Serializable checks", () => {
	it("infers P and O from run's parameter and return type", () => {
		const task = defineTask({
			id: "infer",
			run: async (payload: { url: string }) => ({ status: "ok" as const, url: payload.url }),
		});
		expectTypeOf<TaskPayload<typeof task>>().toEqualTypeOf<{ url: string }>();
		expectTypeOf<TaskOutput<typeof task>>().toEqualTypeOf<{ status: "ok"; url: string }>();
	});

	it("rejects a { d: Date } payload", () => {
		// @ts-expect-error payload field `d` is a Date, which is not JSON-serializable
		defineTask({
			id: "bad-payload",
			run: async (payload: { d: Date }) => payload.d.toISOString(),
		});
	});

	it("rejects a Date output", () => {
		// @ts-expect-error task output is a Date, which is not JSON-serializable
		defineTask({ id: "bad-output", run: async () => new Date() });
	});

	it("allows a void output (stored as null by the engine)", () => {
		const task = defineTask({ id: "void-ok", run: async () => {} });
		expectTypeOf<TaskOutput<typeof task>>().toEqualTypeOf<void>();
	});

	it("allows an interface payload", () => {
		interface Payload {
			id: string;
			count: number;
			tags: string[];
		}
		const task = defineTask({ id: "iface", run: async (payload: Payload) => payload.id });
		expectTypeOf<TaskPayload<typeof task>>().toEqualTypeOf<Payload>();
		expectTypeOf<TaskOutput<typeof task>>().toEqualTypeOf<string>();
	});
});

describe("StepApi: payload constrained to the given task", () => {
	const child = defineTask({
		id: "child",
		run: async (payload: { amount: number }) => ({ ok: true as const, amount: payload.amount }),
	});

	it("triggerAndWait rejects a mismatched payload and returns a typed WaitResult", () => {
		function useStep(step: StepApi) {
			// @ts-expect-error `text` doesn't exist on the child task's payload
			step.triggerAndWait("child-step", child, { text: "nope" });

			const result = step.triggerAndWait("child-step", child, { amount: 1 });
			expectTypeOf(result).toEqualTypeOf<Promise<WaitResult<{ ok: true; amount: number }>>>();
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});

	it("trigger rejects a mismatched payload and returns a handle typed with the task's output", () => {
		function useStep(step: StepApi) {
			// @ts-expect-error wrong payload shape for `child`
			step.trigger("child-step", child, { text: "nope" });

			const handle = step.trigger("child-step", child, { amount: 1 });
			expectTypeOf(handle).resolves.toHaveProperty("id").toEqualTypeOf<string>();
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});

	it("batchTriggerAndWait rejects a mismatched payload and returns typed per-item results", () => {
		function useStep(step: StepApi) {
			// @ts-expect-error wrong payload shape for `child`
			step.batchTriggerAndWait("child-batch", child, [{ text: "nope" }]);

			const batch = step.batchTriggerAndWait("child-batch", child, [{ amount: 1 }, { amount: 2 }]);
			expectTypeOf(batch).toEqualTypeOf<Promise<WaitResult<{ ok: true; amount: number }>[]>>();
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});
});

describe("tasks without a payload parameter", () => {
	const noPayload = defineTask({ id: "no-payload", run: async () => "done" });
	const withPayload = defineTask({
		id: "with-payload",
		run: async (payload: { amount: number }) => payload.amount,
	});

	it("infers the payload as void (not unknown)", () => {
		expectTypeOf<TaskPayload<typeof noPayload>>().toEqualTypeOf<void>();
	});

	it("engine.trigger: the payload is optional and anything else is rejected", () => {
		function use(engine: Engine) {
			engine.trigger(noPayload);
			engine.trigger(noPayload, undefined, { delay: "1s" });
			// @ts-expect-error a task without a payload parameter accepts no payload
			engine.trigger(noPayload, { amount: 1 });
			// @ts-expect-error a task without a payload parameter accepts no payload
			engine.trigger(noPayload, "x");

			engine.trigger(withPayload, { amount: 1 });
			// @ts-expect-error the payload of a task that declares one is required
			engine.trigger(withPayload);
			// @ts-expect-error wrong payload shape
			engine.trigger(withPayload, { text: "nope" });
		}
		expectTypeOf(use).returns.toBeVoid();
	});

	it("StepApi.trigger/triggerAndWait: the payload is optional; batch keeps an array", () => {
		function useStep(step: StepApi) {
			step.trigger("a", noPayload);
			step.trigger("b", noPayload, undefined, { delay: "1s" });
			// @ts-expect-error a task without a payload parameter accepts no payload
			step.trigger("c", noPayload, { amount: 1 });

			const result = step.triggerAndWait("d", noPayload);
			expectTypeOf(result).toEqualTypeOf<Promise<WaitResult<string>>>();
			step.triggerAndWait("e", noPayload, undefined, { timeout: "1m" });
			// @ts-expect-error a task without a payload parameter accepts no payload
			step.triggerAndWait("f", noPayload, { amount: 1 });

			// @ts-expect-error the payload of a task that declares one is required
			step.trigger("g", withPayload);
			// @ts-expect-error the payload of a task that declares one is required
			step.triggerAndWait("h", withPayload);

			const batch = step.batchTriggerAndWait("i", noPayload, [undefined, undefined]);
			expectTypeOf(batch).toEqualTypeOf<Promise<WaitResult<string>[]>>();
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});
});

describe("TaskScheduleOptions: schedule.payload required/Serializable when the task requires one (fix 5)", () => {
	it("rejects defineTask({ schedule }) omitting payload when run requires one", () => {
		defineTask({
			id: "sched-missing-payload",
			// @ts-expect-error schedule.payload is required: run's payload isn't optional
			schedule: { cron: "0 9 * * *" },
			run: async (payload: { url: string }) => payload.url,
		});
	});

	it("rejects a schedule payload that doesn't match the task's own payload type", () => {
		defineTask({
			id: "sched-mismatched-payload",
			// @ts-expect-error `url` must be a string
			schedule: { cron: "0 9 * * *", payload: { url: 123 } },
			run: async (payload: { url: string }) => payload.url,
		});
	});

	it("accepts a matching schedule payload for a task that requires one", () => {
		const task = defineTask({
			id: "sched-ok-payload",
			schedule: { cron: "0 9 * * *", payload: { url: "https://example.com" } },
			run: async (payload: { url: string }) => payload.url,
		});
		expectTypeOf<TaskPayload<typeof task>>().toEqualTypeOf<{ url: string }>();
	});

	it("allows an omitted schedule.payload for a task that takes none", () => {
		defineTask({ id: "sched-no-payload", schedule: { cron: "0 9 * * *" }, run: async () => {} });
	});
});

describe("StepApi.run: T inference from fn's return type", () => {
	type ToolName = "search" | "calculate";
	type AgentDecision =
		| { type: "tool_call"; tool: ToolName; args: string }
		| { type: "final"; answer: string };

	it("infers a union return type without an explicit generic (regression: used to collapse to one branch)", () => {
		function useStep(step: StepApi) {
			return step.run("decide", (): AgentDecision => ({ type: "final", answer: "x" }));
		}
		expectTypeOf(useStep).returns.toEqualTypeOf<Promise<AgentDecision>>();
	});

	it("infers a union return type from an async fn", () => {
		function useStep(step: StepApi) {
			return step.run(
				"decide",
				async (): Promise<AgentDecision> => ({ type: "final", answer: "x" })
			);
		}
		expectTypeOf(useStep).returns.toEqualTypeOf<Promise<AgentDecision>>();
	});

	it("still accepts an explicit generic (equivalent, no longer necessary)", () => {
		function useStep(step: StepApi) {
			return step.run<AgentDecision>("decide", () => ({ type: "final", answer: "x" }));
		}
		expectTypeOf(useStep).returns.toEqualTypeOf<Promise<AgentDecision>>();
	});

	it("infers a plain (non-union) sync return type", () => {
		function useStep(step: StepApi) {
			return step.run("sync", () => 42);
		}
		expectTypeOf(useStep).returns.toEqualTypeOf<Promise<number>>();
	});

	it("infers a plain (non-union) async return type", () => {
		function useStep(step: StepApi) {
			return step.run("async", async () => "hi");
		}
		expectTypeOf(useStep).returns.toEqualTypeOf<Promise<string>>();
	});

	it("allows a void-returning fn (stored as null by the engine)", () => {
		function useStep(step: StepApi) {
			return step.run("void", async () => {});
		}
		expectTypeOf(useStep).returns.toEqualTypeOf<Promise<void>>();
	});

	it("rejects a Date return", () => {
		function useStep(step: StepApi) {
			// @ts-expect-error Date is not JSON-serializable
			step.run("bad", () => new Date());
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});

	it("rejects a function return", () => {
		function useStep(step: StepApi) {
			// @ts-expect-error functions are not JSON-serializable
			step.run("bad", () => () => {});
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});

	it("rejects a class instance return", () => {
		class Point {
			constructor(
				public x: number,
				public y: number
			) {}
			distanceToOrigin(): number {
				return Math.hypot(this.x, this.y);
			}
		}
		function useStep(step: StepApi) {
			// @ts-expect-error class instance is not JSON-serializable
			step.run("bad", () => new Point(1, 2));
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});

	it("rejects a Map return", () => {
		function useStep(step: StepApi) {
			// @ts-expect-error Map is not JSON-serializable
			step.run("bad", () => new Map([["a", 1]]));
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});

	it("rejects a union return with one non-serializable branch", () => {
		function useStep(step: StepApi) {
			// @ts-expect-error one branch of the union is a Date
			step.run("bad", (): { ok: true } | Date => ({ ok: true }));
		}
		expectTypeOf(useStep).returns.toBeVoid();
	});
});

describe("engine.schedules.upsert: payload required/Serializable when the task requires one (fix 5)", () => {
	const withPayload = defineTask({
		id: "sched-upsert-with-payload",
		run: async (payload: { url: string }) => payload,
	});
	const withoutPayload = defineTask({ id: "sched-upsert-no-payload", run: async () => {} });

	it("requires payload when the task needs one; accepts a valid one", () => {
		function use(engine: Engine) {
			// @ts-expect-error payload is required
			engine.schedules.upsert({ id: "s1", task: withPayload, cron: "0 9 * * *" });
			engine.schedules.upsert({
				id: "s1",
				task: withPayload,
				cron: "0 9 * * *",
				payload: { url: "x" },
			});
			engine.schedules.upsert({
				id: "s1",
				task: withPayload,
				cron: "0 9 * * *",
				// @ts-expect-error wrong payload shape
				payload: { text: "nope" },
			});
		}
		expectTypeOf(use).returns.toBeVoid();
	});

	it("allows omitting payload for a task without one", () => {
		function use(engine: Engine) {
			engine.schedules.upsert({ id: "s2", task: withoutPayload, cron: "0 9 * * *" });
		}
		expectTypeOf(use).returns.toBeVoid();
	});
});
