import { describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import { parseDuration } from "../../src/utils/duration.js";

describe("parseDuration", () => {
	it("treats a number as milliseconds", () => {
		expect(parseDuration(500)).toBe(500);
		expect(parseDuration(0)).toBe(0);
	});

	it("rounds fractional millisecond numbers", () => {
		expect(parseDuration(1.6)).toBe(2);
	});

	it("rejects negative or non-finite numbers", () => {
		expect(() => parseDuration(-1)).toThrow(ConfigError);
		expect(() => parseDuration(Number.POSITIVE_INFINITY)).toThrow(ConfigError);
		expect(() => parseDuration(Number.NaN)).toThrow(ConfigError);
	});

	it("parses unit-suffixed strings", () => {
		expect(parseDuration("500ms")).toBe(500);
		expect(parseDuration("5s")).toBe(5_000);
		expect(parseDuration("5m")).toBe(300_000);
		expect(parseDuration("1.5h")).toBe(5_400_000);
		expect(parseDuration("2d")).toBe(172_800_000);
		expect(parseDuration("1w")).toBe(604_800_000);
	});

	it("returns an integer number of ms", () => {
		expect(Number.isInteger(parseDuration("1.5h"))).toBe(true);
	});

	it("throws ConfigError with a clear message on invalid strings", () => {
		expect(() => parseDuration("5x" as never)).toThrow(ConfigError);
		expect(() => parseDuration("5x" as never)).toThrow(/invalid duration/);
		expect(() => parseDuration("" as never)).toThrow(ConfigError);
		expect(() => parseDuration("five minutes" as never)).toThrow(ConfigError);
	});

	it("rejects a duration string whose numeric part overflows to a non-finite value", () => {
		const huge400Digits = `${"9".repeat(400)}w`;
		expect(() => parseDuration(huge400Digits as never)).toThrow(ConfigError);
		expect(() => parseDuration(huge400Digits as never)).toThrow(/invalid duration/);
	});
});
