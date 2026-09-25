import { describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import {
	validateIdempotencyKey,
	validateQueueName,
	validateSqlIdentifier,
	validateStepName,
	validateTaskId,
} from "../../src/utils/identifiers.js";

describe("validateSqlIdentifier", () => {
	it("accepts valid schema/prefix names", () => {
		expect(validateSqlIdentifier("schema", "minitrigger")).toBe("minitrigger");
		expect(validateSqlIdentifier("prefix", "_private_1")).toBe("_private_1");
	});

	it("rejects invalid names with a clear, actionable message", () => {
		expect(() => validateSqlIdentifier("schema", "Minha-Lib")).toThrow(ConfigError);
		expect(() => validateSqlIdentifier("schema", "Minha-Lib")).toThrow(
			/schema "Minha-Lib" is invalid: must be 1-63 characters, start with a lowercase letter/
		);
		expect(() => validateSqlIdentifier("prefix", "1starts-with-digit")).toThrow(ConfigError);
		expect(() => validateSqlIdentifier("schema", "")).toThrow(ConfigError);
		expect(() => validateSqlIdentifier("schema", "a".repeat(64))).toThrow(ConfigError);
	});

	it("truncates a very long offending value instead of echoing it in full", () => {
		const huge = "A".repeat(5_000);
		expect(() => validateSqlIdentifier("schema", huge)).toThrow(ConfigError);
		try {
			validateSqlIdentifier("schema", huge);
			expect.unreachable();
		} catch (err) {
			const message = (err as Error).message;
			expect(message.length).toBeLessThan(500);
			expect(message).toContain("…");
		}
	});
});

describe("validateQueueName", () => {
	it("accepts valid queue names", () => {
		expect(validateQueueName("llm")).toBe("llm");
		expect(validateQueueName("my-queue_1")).toBe("my-queue_1");
	});

	it("rejects invalid patterns", () => {
		expect(() => validateQueueName("LLM")).toThrow(ConfigError);
		expect(() => validateQueueName("")).toThrow(ConfigError);
		expect(() => validateQueueName("a".repeat(65))).toThrow(ConfigError);
	});

	it("rejects the reserved 'system' queue name, suggesting a fix", () => {
		expect(() => validateQueueName("system")).toThrow(ConfigError);
		expect(() => validateQueueName("system")).toThrow(/reserved/);
		expect(() => validateQueueName("system")).toThrow(/choose a different name/);
	});
});

describe("validateStepName", () => {
	it("accepts valid step names", () => {
		expect(validateStepName("download")).toBe("download");
	});

	it("rejects empty, too-long, or '#'-containing names", () => {
		expect(() => validateStepName("")).toThrow(ConfigError);
		expect(() => validateStepName("a".repeat(257))).toThrow(ConfigError);
		expect(() => validateStepName("step#wait")).toThrow(ConfigError);
	});

	it("names the offending step and tells the caller how to fix a '#' in the name", () => {
		expect(() => validateStepName("step#wait")).toThrow(
			/step name "step#wait" is invalid: "#" is reserved.*remove it/
		);
	});

	it("reports the actual and max length for a too-long step name", () => {
		expect(() => validateStepName("a".repeat(300))).toThrow(/at most 256 characters, got 300/);
	});

	it("accepts a step name at the length boundary", () => {
		expect(validateStepName("a".repeat(256))).toHaveLength(256);
	});
});

describe("validateIdempotencyKey", () => {
	it("accepts valid user keys", () => {
		expect(validateIdempotencyKey("order-123")).toBe("order-123");
	});

	it("rejects empty, too-long, or internally-namespaced keys", () => {
		expect(() => validateIdempotencyKey("")).toThrow(ConfigError);
		expect(() => validateIdempotencyKey("a".repeat(513))).toThrow(ConfigError);
		expect(() => validateIdempotencyKey("__child:run:step")).toThrow(ConfigError);
	});

	it("names the offending key and suggests a fix for the reserved '__' prefix", () => {
		expect(() => validateIdempotencyKey("__child:run:step")).toThrow(
			/idempotency key "__child:run:step" is invalid: keys starting with "__" are reserved.*doesn't start with "__"/
		);
	});
});

describe("validateTaskId", () => {
	it("accepts valid task ids", () => {
		expect(validateTaskId("resumir-vaga")).toBe("resumir-vaga");
		expect(validateTaskId("task.v2_final-1")).toBe("task.v2_final-1");
	});

	it("rejects ids with ':' or invalid characters", () => {
		expect(() => validateTaskId("task:1")).toThrow(ConfigError);
		expect(() => validateTaskId("")).toThrow(ConfigError);
		expect(() => validateTaskId("a".repeat(129))).toThrow(ConfigError);
		expect(() => validateTaskId("has space")).toThrow(ConfigError);
	});
});
