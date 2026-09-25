import { describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import {
	computeBackoffMs,
	DEFAULT_MAX_BACKOFF_MS,
	DEFAULT_RETRY,
} from "../../src/utils/backoff.js";

describe("computeBackoffMs", () => {
	it("computes exponential backoff without jitter", () => {
		const opts = { type: "exponential" as const, baseMs: 1_000, jitter: false };
		expect(computeBackoffMs(opts, 1)).toBe(1_000);
		expect(computeBackoffMs(opts, 2)).toBe(2_000);
		expect(computeBackoffMs(opts, 3)).toBe(4_000);
		expect(computeBackoffMs(opts, 4)).toBe(8_000);
	});

	it("caps exponential backoff at maxMs", () => {
		const opts = { type: "exponential" as const, baseMs: 1_000, maxMs: 5_000, jitter: false };
		expect(computeBackoffMs(opts, 10)).toBe(5_000);
	});

	it("computes fixed backoff", () => {
		const opts = { type: "fixed" as const, baseMs: 2_000, jitter: false };
		expect(computeBackoffMs(opts, 1)).toBe(2_000);
		expect(computeBackoffMs(opts, 5)).toBe(2_000);
	});

	it("caps fixed backoff at maxMs", () => {
		const opts = { type: "fixed" as const, baseMs: 10_000, maxMs: 3_000, jitter: false };
		expect(computeBackoffMs(opts, 1)).toBe(3_000);
	});

	it("applies equal jitter by default, never reaching 0", () => {
		const opts = { type: "fixed" as const, baseMs: 1_000 };
		const withMinRandom = computeBackoffMs(opts, 1, () => 0);
		const withMaxRandom = computeBackoffMs(opts, 1, () => 1);
		expect(withMinRandom).toBe(500); // half + 0 * half
		expect(withMaxRandom).toBe(1_000); // half + 1 * half
		expect(withMinRandom).toBeGreaterThan(0);
	});

	it("uses a deterministic random function when provided", () => {
		const opts = { type: "exponential" as const, baseMs: 1_000, jitter: true };
		expect(computeBackoffMs(opts, 1, () => 0.5)).toBe(750); // half(500) + 0.5*half(500)
	});

	it("rejects a non-positive-integer attempt", () => {
		const opts = { type: "fixed" as const, baseMs: 1_000 };
		expect(() => computeBackoffMs(opts, 0)).toThrow(ConfigError);
		expect(() => computeBackoffMs(opts, 1.5)).toThrow(ConfigError);
		expect(() => computeBackoffMs(opts, -1)).toThrow(ConfigError);
	});

	it("rejects invalid baseMs/maxMs", () => {
		expect(() => computeBackoffMs({ type: "fixed", baseMs: -1 }, 1)).toThrow(ConfigError);
		expect(() => computeBackoffMs({ type: "fixed", baseMs: 1, maxMs: -1 }, 1)).toThrow(ConfigError);
	});

	it("exposes sensible defaults", () => {
		expect(DEFAULT_RETRY.maxAttempts).toBe(3);
		expect(DEFAULT_RETRY.backoff).toEqual({ type: "exponential", baseMs: 1_000, maxMs: 60_000 });
	});

	it("never returns NaN when baseMs is 0 and the attempt is huge (0 * Infinity)", () => {
		const opts = { type: "exponential" as const, baseMs: 0, jitter: false };
		const result = computeBackoffMs(opts, 2_000);
		expect(Number.isNaN(result)).toBe(false);
		expect(Number.isFinite(result)).toBe(true);
		expect(result).toBe(DEFAULT_MAX_BACKOFF_MS);
	});

	it("caps at a default maxMs (1 hour) when maxMs is not specified", () => {
		const opts = { type: "exponential" as const, baseMs: 1_000, jitter: false };
		expect(computeBackoffMs(opts, 1_000)).toBe(DEFAULT_MAX_BACKOFF_MS);
		expect(DEFAULT_MAX_BACKOFF_MS).toBe(3_600_000);
	});

	it("caps an Infinity-producing exponential growth at maxMs instead of returning Infinity", () => {
		const opts = { type: "exponential" as const, baseMs: 1_000, maxMs: 10_000, jitter: false };
		const result = computeBackoffMs(opts, 100_000);
		expect(Number.isFinite(result)).toBe(true);
		expect(result).toBe(10_000);
	});

	it("always returns an integer >= 0", () => {
		const opts = { type: "exponential" as const, baseMs: 0, jitter: true };
		const result = computeBackoffMs(opts, 5_000, () => 0.37);
		expect(Number.isInteger(result)).toBe(true);
		expect(result).toBeGreaterThanOrEqual(0);
	});
});
