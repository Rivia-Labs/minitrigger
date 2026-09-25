import { describe, expect, it } from "vitest";
import { ConfigError, TaskNotRegisteredError } from "../../src/errors.js";
import { buildRegistry, getTask } from "../../src/registry.js";
import type { TaskQueueOptions } from "../../src/task.js";
import { defineTask } from "../../src/task.js";

function task(id: string, queue?: TaskQueueOptions) {
	return queue === undefined
		? defineTask({ id, run: async () => {} })
		: defineTask({ id, queue, run: async () => {} });
}

describe("buildRegistry: tasks", () => {
	it("indexes every task by id", () => {
		const a = task("a");
		const b = task("b");
		const { tasks } = buildRegistry([a, b]);
		expect(tasks.get("a")).toBe(a);
		expect(tasks.get("b")).toBe(b);
		expect(tasks.size).toBe(2);
	});

	it("throws ConfigError on a duplicate task id, naming both", () => {
		const a = task("dup", { name: "queue-a" });
		const b = task("dup", { name: "queue-b" });
		expect(() => buildRegistry([a, b])).toThrow(ConfigError);
		try {
			buildRegistry([a, b]);
			expect.unreachable();
		} catch (err) {
			expect(err).toBeInstanceOf(ConfigError);
			expect((err as Error).message).toContain("dup");
			expect((err as Error).message).toContain("queue-a");
			expect((err as Error).message).toContain("queue-b");
		}
	});
});

describe("buildRegistry: implicit default queue", () => {
	it('assigns tasks without a queue to "default" with concurrency 10', () => {
		const a = task("a");
		const { queues } = buildRegistry([a]);
		expect(queues.get("default")).toEqual({
			name: "default",
			concurrency: 10,
			taskIds: ["a"],
		});
	});

	it("groups several queue-less tasks into the same default queue", () => {
		const a = task("a");
		const b = task("b");
		const { queues } = buildRegistry([a, b]);
		expect(queues.size).toBe(1);
		expect(queues.get("default")?.taskIds).toEqual(["a", "b"]);
	});
});

describe("buildRegistry: queue name validation", () => {
	it("rejects an invalid queue name", () => {
		expect(() => buildRegistry([task("a", { name: "Not Valid!" })])).toThrow(ConfigError);
	});

	it('rejects the reserved "system" queue name', () => {
		expect(() => buildRegistry([task("a", { name: "system" })])).toThrow(ConfigError);
	});
});

describe("buildRegistry: shared queue settings (Phase 3 acceptance)", () => {
	it("errors when two tasks declare different concurrency for the same queue", () => {
		const a = task("a", { name: "llm", concurrency: 2 });
		const b = task("b", { name: "llm", concurrency: 5 });
		expect(() => buildRegistry([a, b])).toThrow(ConfigError);
		try {
			buildRegistry([a, b]);
			expect.unreachable();
		} catch (err) {
			const message = (err as Error).message;
			expect(message).toContain("llm");
			expect(message).toContain("concurrency");
			expect(message).toContain('"a"');
			expect(message).toContain('"b"');
			expect(message).toContain("2");
			expect(message).toContain("5");
			expect(message).toContain("declare the same");
		}
	});

	it("lets one task declare settings and another just reference the queue name", () => {
		const declaring = task("declaring", {
			name: "llm",
			concurrency: 2,
			rateLimit: { max: 10, durationMs: 60_000 },
		});
		const referencing = task("referencing", { name: "llm" });

		const { queues } = buildRegistry([declaring, referencing]);
		expect(queues.get("llm")).toEqual({
			name: "llm",
			concurrency: 2,
			rateLimit: { max: 10, durationMs: 60_000 },
			taskIds: ["declaring", "referencing"],
		});
	});

	it("agreeing declarations from multiple tasks resolve without error", () => {
		const a = task("a", { name: "llm", concurrency: 3 });
		const b = task("b", { name: "llm", concurrency: 3 });
		const { queues } = buildRegistry([a, b]);
		expect(queues.get("llm")?.concurrency).toBe(3);
	});

	it("defaults concurrency to 10 when nobody declares it, even with other fields set", () => {
		const a = task("a", { name: "llm", rateLimit: { max: 1, durationMs: 1_000 } });
		const { queues } = buildRegistry([a]);
		expect(queues.get("llm")?.concurrency).toBe(10);
	});

	it("errors on conflicting globalConcurrency", () => {
		const a = task("a", { name: "llm", globalConcurrency: 1 });
		const b = task("b", { name: "llm", globalConcurrency: 2 });
		expect(() => buildRegistry([a, b])).toThrow(ConfigError);
	});

	it("errors on conflicting rateLimit", () => {
		const a = task("a", { name: "llm", rateLimit: { max: 10, durationMs: 1_000 } });
		const b = task("b", { name: "llm", rateLimit: { max: 20, durationMs: 1_000 } });
		expect(() => buildRegistry([a, b])).toThrow(ConfigError);
	});

	it("keeps unrelated queues independent", () => {
		const a = task("a", { name: "llm", concurrency: 2 });
		const b = task("b", { name: "emails", concurrency: 5 });
		const { queues } = buildRegistry([a, b]);
		expect(queues.get("llm")?.concurrency).toBe(2);
		expect(queues.get("emails")?.concurrency).toBe(5);
		expect(queues.size).toBe(2);
	});
});

describe("buildRegistry: numeric queue settings", () => {
	it.each([
		["concurrency", { name: "llm", concurrency: 0 }],
		["concurrency", { name: "llm", concurrency: 1.5 }],
		["concurrency", { name: "llm", concurrency: Number.NaN }],
		["globalConcurrency", { name: "llm", globalConcurrency: 0 }],
		["globalConcurrency", { name: "llm", globalConcurrency: -2 }],
		["rateLimit.max", { name: "llm", rateLimit: { max: 0, durationMs: 1_000 } }],
		["rateLimit.max", { name: "llm", rateLimit: { max: 2.5, durationMs: 1_000 } }],
		["rateLimit.durationMs", { name: "llm", rateLimit: { max: 1, durationMs: 0 } }],
		["rateLimit.durationMs", { name: "llm", rateLimit: { max: 1, durationMs: Infinity } }],
	] as Array<[string, TaskQueueOptions]>)(
		"rejects a non-integer or < 1 %s, naming the queue and the task",
		(field, queue) => {
			const a = task("bad-task", queue);
			expect(() => buildRegistry([a])).toThrow(ConfigError);
			expect(() => buildRegistry([a])).toThrow(
				new RegExp(`queue "llm".*task "bad-task".*${field.replace(".", "\\.")}`)
			);
		}
	);

	it("accepts integer settings >= 1", () => {
		const a = task("a", {
			name: "llm",
			concurrency: 1,
			globalConcurrency: 1,
			rateLimit: { max: 1, durationMs: 1 },
		});
		expect(() => buildRegistry([a])).not.toThrow();
	});
});

describe("getTask", () => {
	it("returns the task by id", () => {
		const a = task("a");
		const registry = buildRegistry([a]);
		expect(getTask(registry, "a")).toBe(a);
	});

	it("returns the task when passed a Task value", () => {
		const a = task("a");
		const registry = buildRegistry([a]);
		expect(getTask(registry, a)).toBe(a);
	});

	it("throws TaskNotRegisteredError with an actionable message for an unknown id", () => {
		const registry = buildRegistry([task("a")]);
		expect(() => getTask(registry, "missing")).toThrow(TaskNotRegisteredError);
		expect(() => getTask(registry, "missing")).toThrow(
			'task "missing" is not registered in this engine'
		);
		expect(() => getTask(registry, "missing")).toThrow(/createEngine\(\{ tasks: \[\.\.\.\] \}\)/);
	});
});
