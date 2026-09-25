import { describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import { defineTask } from "../../src/task.js";

describe("defineTask", () => {
	it("returns a Task carrying id and options", () => {
		const task = defineTask({
			id: "greet",
			run: async (payload: { name: string }) => `hello ${payload.name}`,
		});

		expect(task.id).toBe("greet");
		expect(task.options.id).toBe("greet");
		expect(typeof task.options.run).toBe("function");
	});

	it('leaves queue undefined when omitted (implicit "default" queue, resolved by the registry)', () => {
		const task = defineTask({ id: "no-queue", run: async () => {} });
		expect(task.options.queue).toBeUndefined();
	});

	it("passes queue, retry, maxDurationMs and schedule through unchanged", () => {
		const task = defineTask({
			id: "configured",
			queue: { name: "llm", concurrency: 2, rateLimit: { max: 10, durationMs: 60_000 } },
			retry: { maxAttempts: 3, backoff: { type: "exponential", baseMs: 1_000 } },
			maxDurationMs: 5 * 60_000,
			schedule: { cron: "0 9 * * *", timezone: "America/Belem", payload: { name: "x" } },
			run: async (payload: { name: string }) => payload.name,
		});

		expect(task.options.queue).toEqual({
			name: "llm",
			concurrency: 2,
			rateLimit: { max: 10, durationMs: 60_000 },
		});
		expect(task.options.retry).toEqual({
			maxAttempts: 3,
			backoff: { type: "exponential", baseMs: 1_000 },
		});
		expect(task.options.maxDurationMs).toBe(300_000);
		expect(task.options.schedule).toEqual({
			cron: "0 9 * * *",
			timezone: "America/Belem",
			payload: { name: "x" },
		});
	});

	it("validates the task id and throws ConfigError for an invalid one", () => {
		expect(() => defineTask({ id: "has a space", run: async () => {} })).toThrow(ConfigError);
		expect(() => defineTask({ id: "has:colon", run: async () => {} })).toThrow(ConfigError);
		expect(() => defineTask({ id: "", run: async () => {} })).toThrow(ConfigError);
	});

	it("accepts task ids made of letters, digits, dot, underscore and dash", () => {
		const task = defineTask({ id: "task.name_v2-final", run: async () => {} });
		expect(task.id).toBe("task.name_v2-final");
	});

	it("stores a void-returning run as-is (normalization to null happens in the executor, not here)", () => {
		const task = defineTask({ id: "void-output", run: async () => {} });
		expect(typeof task.options.run).toBe("function");
	});
});
