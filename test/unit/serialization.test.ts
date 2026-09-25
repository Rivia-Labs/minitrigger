import { describe, expect, it } from "vitest";
import { SerializationError } from "../../src/errors.js";
import {
	assertSerializable,
	normalizeOutput,
	truncateLogData,
} from "../../src/utils/serialization.js";

describe("assertSerializable", () => {
	it("accepts plain JSON-shaped values and returns their byte size", () => {
		const value = { a: 1, b: "two", c: [1, 2, 3], d: null, e: { nested: true } };
		const size = assertSerializable(value, { label: "payload", maxBytes: 1_000 });
		expect(size).toBe(Buffer.byteLength(JSON.stringify(value)));
	});

	it("allows undefined object property values (dropped by JSON)", () => {
		expect(() =>
			assertSerializable({ a: 1, b: undefined }, { label: "payload", maxBytes: 1_000 })
		).not.toThrow();
	});

	it("allows Object.create(null) objects", () => {
		const value: Record<string, unknown> = Object.create(null);
		value.a = 1;
		expect(() => assertSerializable(value, { label: "payload", maxBytes: 1_000 })).not.toThrow();
	});

	it("rejects top-level undefined", () => {
		expect(() => assertSerializable(undefined, { label: "output", maxBytes: 1_000 })).toThrow(
			SerializationError
		);
	});

	it("rejects undefined inside arrays", () => {
		expect(() =>
			assertSerializable([1, undefined, 3], { label: "output", maxBytes: 1_000 })
		).toThrow(SerializationError);
	});

	it.each([
		["Date", new Date()],
		["Map", new Map()],
		["Set", new Set()],
		["RegExp", /x/],
		["function", () => 1],
		["symbol", Symbol("s")],
		["bigint", 10n],
	])("rejects a bare %s", (_label, value) => {
		expect(() => assertSerializable(value, { label: "output", maxBytes: 1_000 })).toThrow(
			SerializationError
		);
	});

	it("rejects NaN and Infinity", () => {
		expect(() => assertSerializable(Number.NaN, { label: "output", maxBytes: 1_000 })).toThrow(
			SerializationError
		);
		expect(() =>
			assertSerializable(Number.POSITIVE_INFINITY, { label: "output", maxBytes: 1_000 })
		).toThrow(SerializationError);
	});

	it("rejects class instances (non-plain prototype)", () => {
		class Point {
			constructor(
				public x: number,
				public y: number
			) {}
		}
		expect(() => assertSerializable(new Point(1, 2), { label: "output", maxBytes: 1_000 })).toThrow(
			SerializationError
		);
	});

	it("reports a precise nested path for the offending field", () => {
		expect(() =>
			assertSerializable(
				{ data: { date: new Date() } },
				{ label: 'output of step "llm"', maxBytes: 1_000 }
			)
		).toThrow(/output of step "llm" is not serializable: field `data\.date` is a Date/);
	});

	it("detects circular references", () => {
		const obj: Record<string, unknown> = { a: 1 };
		obj.self = obj;
		expect(() => assertSerializable(obj, { label: "output", maxBytes: 1_000 })).toThrow(
			/circular reference/
		);
	});

	it("does not flag repeated (non-circular) references to the same object", () => {
		const shared = { x: 1 };
		expect(() =>
			assertSerializable({ a: shared, b: shared }, { label: "output", maxBytes: 1_000 })
		).not.toThrow();
	});

	it("throws when the payload exceeds maxBytes", () => {
		const big = { data: "x".repeat(2_000) };
		expect(() => assertSerializable(big, { label: 'payload of task "x"', maxBytes: 100 })).toThrow(
			/payload of task "x" is .+ exceeding the configured size limit/
		);
	});

	it("rejects a string containing a NUL byte, naming the path", () => {
		expect(() =>
			assertSerializable({ note: "a\u0000b" }, { label: "output", maxBytes: 1_000 })
		).toThrow(/output is not serializable: field `note` contains a NUL byte/);
	});

	it("rejects a string containing an unpaired (lone) surrogate", () => {
		expect(() =>
			assertSerializable({ note: "a\uD800b" }, { label: "output", maxBytes: 1_000 })
		).toThrow(/output is not serializable: field `note` is not well-formed Unicode/);
	});

	it("accepts a well-formed string with a full surrogate pair (astral character)", () => {
		expect(() =>
			assertSerializable({ note: "😀" }, { label: "output", maxBytes: 1_000 })
		).not.toThrow();
	});

	it("rejects an object key containing a NUL byte, naming the key", () => {
		expect(() =>
			assertSerializable({ "bad\u0000key": 1 }, { label: "output", maxBytes: 1_000 })
		).toThrow("output is not serializable: object key `bad\u0000key` contains a NUL byte (U+0000)");
	});

	it("rejects an object key containing an unpaired surrogate", () => {
		expect(() =>
			assertSerializable({ "bad\uD800key": 1 }, { label: "output", maxBytes: 1_000 })
		).toThrow(/is not well-formed Unicode/);
	});
});

describe("normalizeOutput", () => {
	it("maps a top-level undefined (void run) to null", () => {
		expect(normalizeOutput(undefined)).toBeNull();
	});

	it("leaves null as null", () => {
		expect(normalizeOutput(null)).toBeNull();
	});

	it("passes through other values unchanged", () => {
		expect(normalizeOutput(0)).toBe(0);
		expect(normalizeOutput("x")).toBe("x");
		const obj = { a: 1 };
		expect(normalizeOutput(obj)).toBe(obj);
	});

	it("composes with assertSerializable so a void task/step output is accepted", () => {
		expect(() =>
			assertSerializable(normalizeOutput(undefined), { label: "output", maxBytes: 1_000 })
		).not.toThrow();
	});
});

describe("truncateLogData", () => {
	it("returns small data unchanged", () => {
		const data = { a: 1, b: "two" };
		expect(truncateLogData(data, 1_000)).toBe(data);
	});

	it("returns undefined as-is", () => {
		expect(truncateLogData(undefined, 1_000)).toBeUndefined();
	});

	it("truncates oversized data into a marker that fits maxBytes", () => {
		const data = { big: "x".repeat(10_000) };
		const result = truncateLogData(data, 200) as {
			__truncated: true;
			preview: string;
			originalBytes: number;
		};
		expect(result.__truncated).toBe(true);
		expect(result.originalBytes).toBeGreaterThan(200);
		expect(Buffer.byteLength(JSON.stringify(result), "utf8")).toBeLessThanOrEqual(200);
	});

	it("handles a very small maxBytes budget without throwing", () => {
		const data = { big: "x".repeat(1_000) };
		const result = truncateLogData(data, 10);
		expect(result).toBeDefined();
	});
});
