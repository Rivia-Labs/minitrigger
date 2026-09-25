import { describe, expect, it } from "vitest";
import { DAY_MS, type EngineConfig, resolveConfig } from "../../src/config.js";
import { ConfigError } from "../../src/errors.js";

const base: EngineConfig = {
	postgres: "postgres://localhost/db",
	redis: "redis://localhost:6379",
	tasks: [],
};

describe("resolveConfig", () => {
	it("applies every default", () => {
		const r = resolveConfig(base);
		expect(r.schema).toBe("minitrigger");
		expect(r.prefix).toBe("minitrigger");
		expect(r.poolMax).toBe(10);
		expect(r.heartbeatIntervalMs).toBe(10_000);
		expect(r.heartbeatTimeoutMs).toBe(2_000);
		expect(r.stallTimeoutMs).toBe(60_000);
		expect(r.shutdownTimeoutMs).toBe(30_000);
		expect(r.idempotencyKeyTtlMs).toBe(7 * DAY_MS);
		expect(r.retentionDays).toBe(30);
		expect(r.systemConcurrency).toBe(5);
		expect(r.limits).toEqual({
			maxPayloadBytes: 1024 * 1024,
			maxStepOutputBytes: 256 * 1024,
			maxLogEntryBytes: 16 * 1024,
			maxInlineStepOutputBytes: 16 * 1024 * 1024,
		});
		expect(typeof r.logger.warn).toBe("function");
	});

	it("keeps explicit values and parses idempotencyKeyTtl durations", () => {
		const logger = { debug() {}, info() {}, warn() {}, error() {} };
		const r = resolveConfig({
			...base,
			schema: "app_jobs",
			prefix: "app",
			pool: { max: 3 },
			heartbeatIntervalMs: 100,
			stallTimeoutMs: 1_000,
			idempotencyKeyTtl: "2d",
			retention: { days: 3 },
			limits: { maxPayloadBytes: 10 },
			systemConcurrency: 2,
			logger,
		});
		expect(r.schema).toBe("app_jobs");
		expect(r.prefix).toBe("app");
		expect(r.poolMax).toBe(3);
		expect(r.idempotencyKeyTtlMs).toBe(2 * DAY_MS);
		expect(r.retentionDays).toBe(3);
		expect(r.limits.maxPayloadBytes).toBe(10);
		expect(r.limits.maxStepOutputBytes).toBe(256 * 1024);
		expect(r.logger).toBe(logger);
	});

	it("accepts console as a logger", () => {
		expect(() => resolveConfig({ ...base, logger: console })).not.toThrow();
	});

	it.each(["Bad", "1abc", "with-dash", ""])(
		"rejects invalid schema/prefix %j (decision 15)",
		value => {
			expect(() => resolveConfig({ ...base, schema: value })).toThrow(ConfigError);
			expect(() => resolveConfig({ ...base, prefix: value })).toThrow(ConfigError);
		}
	);

	it("rejects an idempotencyKeyTtl larger than retention.days (decision 9)", () => {
		expect(() =>
			resolveConfig({ ...base, idempotencyKeyTtl: "8d", retention: { days: 7 } })
		).toThrow(/retention\.days/);
		expect(() =>
			resolveConfig({ ...base, idempotencyKeyTtl: "7d", retention: { days: 7 } })
		).not.toThrow();
		// Default TTL (7d) against a shorter retention.
		expect(() => resolveConfig({ ...base, retention: { days: 1 } })).toThrow(ConfigError);
	});

	it("rejects a heartbeat interval that is not smaller than the stall timeout", () => {
		expect(() => resolveConfig({ ...base, heartbeatIntervalMs: 60_000 })).toThrow(ConfigError);
	});

	it.each([
		{ pool: { max: 0 } },
		{ heartbeatIntervalMs: -1 },
		{ heartbeatTimeoutMs: 1.5 },
		{ shutdownTimeoutMs: -5 },
		{ systemConcurrency: 0 },
		{ retention: { days: 0 } },
		{ idempotencyKeyTtl: 0 },
		{ limits: { maxStepOutputBytes: 0 } },
	] as Partial<EngineConfig>[])("rejects invalid numeric option %j", override => {
		expect(() => resolveConfig({ ...base, ...override })).toThrow(ConfigError);
	});

	it.each([
		{ heartbeatIntervalMs: 2_147_483_648, stallTimeoutMs: 2_147_483_649 },
		{ heartbeatTimeoutMs: 2_147_483_648 },
		{ stallTimeoutMs: 2_147_483_648 },
		{ shutdownTimeoutMs: 2_147_483_648 },
	] as Partial<EngineConfig>[])(
		"rejects a timer-fed value above 2^31-1 ms (setTimeout would overflow and fire at once) %j",
		override => {
			expect(() => resolveConfig({ ...base, ...override })).toThrow(ConfigError);
			expect(() => resolveConfig({ ...base, ...override })).toThrow(/2147483647/);
		}
	);

	it("accepts timer-fed values of exactly 2^31-1 ms", () => {
		expect(() =>
			resolveConfig({
				...base,
				heartbeatTimeoutMs: 2_147_483_647,
				stallTimeoutMs: 2_147_483_647,
				shutdownTimeoutMs: 2_147_483_647,
			})
		).not.toThrow();
	});

	it("rejects missing connections, tasks and malformed loggers", () => {
		expect(() => resolveConfig({ ...base, postgres: "" })).toThrow(ConfigError);
		expect(() => resolveConfig({ ...base, redis: {} as unknown as EngineConfig["redis"] })).toThrow(
			ConfigError
		);
		expect(() =>
			resolveConfig({ ...base, tasks: undefined as unknown as EngineConfig["tasks"] })
		).toThrow(ConfigError);
		expect(() =>
			resolveConfig({
				...base,
				logger: { debug() {}, info() {}, warn() {} } as unknown as EngineConfig["logger"] & object,
			})
		).toThrow(/logger\.error/);
	});
});
