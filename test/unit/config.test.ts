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
		expect(r.retentionIntervalMs).toBe(3_600_000);
		expect(r.retentionBatchSize).toBe(1_000);
		expect(r.reconciler).toEqual({ intervalMs: 15_000, graceMs: 30_000, batchSize: 100 });
		expect(r.systemConcurrency).toBe(5);
		expect(r.limits).toEqual({
			maxPayloadBytes: 1024 * 1024,
			maxStepOutputBytes: 256 * 1024,
			maxLogEntryBytes: 16 * 1024,
			maxInlineStepOutputBytes: 16 * 1024 * 1024,
		});
		expect(r.logging).toEqual({ minLevel: "info", mirror: true });
		expect(typeof r.logger.warn).toBe("function");
	});

	it("accepts logging options and rejects invalid ones", () => {
		expect(
			resolveConfig({ ...base, logging: { minLevel: "debug", mirror: false } }).logging
		).toEqual({ minLevel: "debug", mirror: false });
		expect(resolveConfig({ ...base, logging: { minLevel: "warn" } }).logging).toEqual({
			minLevel: "warn",
			mirror: true,
		});
		expect(() => resolveConfig({ ...base, logging: { minLevel: "verbose" as never } })).toThrow(
			ConfigError
		);
		expect(() => resolveConfig({ ...base, logging: { mirror: "yes" as never } })).toThrow(
			ConfigError
		);
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

	it("keeps explicit reconciler and retention options (Phase 6)", () => {
		const r = resolveConfig({
			...base,
			reconciler: { intervalMs: 500, graceMs: 0, batchSize: 7 },
			retention: { days: 10, intervalMs: 60_000, batchSize: 50 },
		});
		expect(r.reconciler).toEqual({ intervalMs: 500, graceMs: 0, batchSize: 7 });
		expect(r.retentionDays).toBe(10);
		expect(r.retentionIntervalMs).toBe(60_000);
		expect(r.retentionBatchSize).toBe(50);
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
		{ reconciler: { intervalMs: 0 } },
		{ reconciler: { graceMs: -1 } },
		{ reconciler: { batchSize: 1.5 } },
		{ retention: { intervalMs: 0 } },
		{ retention: { batchSize: 0 } },
	] as Partial<EngineConfig>[])("rejects invalid numeric option %j", override => {
		expect(() => resolveConfig({ ...base, ...override })).toThrow(ConfigError);
	});

	it.each([
		{ heartbeatIntervalMs: 2_147_483_648, stallTimeoutMs: 2_147_483_649 },
		{ heartbeatTimeoutMs: 2_147_483_648 },
		{ stallTimeoutMs: 2_147_483_648 },
		{ shutdownTimeoutMs: 2_147_483_648 },
		{ reconciler: { intervalMs: 2_147_483_648 } },
		{ retention: { intervalMs: 2_147_483_648 } },
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

	it("names the missing field and how to fix an empty connection string", () => {
		expect(() => resolveConfig({ ...base, postgres: "" })).toThrow(
			/config\.postgres must not be an empty string; pass a non-empty connection string, or \{ pool: <existing pg\.Pool instance> \}/
		);
	});

	it("reports the type/shape (never the value) of a malformed connection option", () => {
		// A bare number isn't a secret, so the resolved value itself is fine to show.
		expect(() =>
			resolveConfig({ ...base, postgres: 42 as unknown as EngineConfig["postgres"] })
		).toThrow(/config\.postgres must be a non-empty connection string or \{ pool: .* got 42/);
		// An object shape is described structurally, never `String(value)` ("[object Object]").
		expect(() => resolveConfig({ ...base, redis: {} as unknown as EngineConfig["redis"] })).toThrow(
			/config\.redis must be a non-empty connection string or \{ connection: .* got an object/
		);
	});

	it("never echoes the actual connection string value in the error (only its type)", () => {
		const secret = "postgres://user:super-secret-password@host:5432/db";
		try {
			resolveConfig({ ...base, postgres: 12345 as unknown as EngineConfig["postgres"] });
			expect.unreachable();
		} catch (err) {
			expect((err as Error).message).not.toContain("super-secret-password");
		}
		// Sanity: a valid string with a password is still accepted (never inspected/echoed).
		expect(() => resolveConfig({ ...base, postgres: secret })).not.toThrow();
	});

	it("detects a raw pg.Pool/ioredis instance passed directly and suggests wrapping it", () => {
		const fakePool = { query: () => {} };
		expect(() =>
			resolveConfig({ ...base, postgres: fakePool as unknown as EngineConfig["postgres"] })
		).toThrow(
			/config\.postgres looks like a raw pg\.Pool instance \(has \.query\(\)\).*wrap.*\{ pool: <that instance> \}/
		);

		const fakeRedis = { duplicate: () => {} };
		expect(() =>
			resolveConfig({ ...base, redis: fakeRedis as unknown as EngineConfig["redis"] })
		).toThrow(
			/config\.redis looks like a raw ioredis Redis instance \(has \.duplicate\(\)\).*wrap.*\{ connection: <that instance> \}/
		);
	});

	it("reports the wrapped field's type when { pool } / { connection } is provided but empty", () => {
		expect(() =>
			resolveConfig({ ...base, postgres: { pool: null } as unknown as EngineConfig["postgres"] })
		).toThrow(/config\.postgres\.pool must be an existing pg\.Pool instance, got null/);
	});

	it("includes the resolved value when idempotencyKeyTtl resolves to 0 (was previously silent)", () => {
		expect(() => resolveConfig({ ...base, idempotencyKeyTtl: 0 })).toThrow(
			/idempotencyKeyTtl must resolve to more than 0 ms, got 0 ms \(parsed from 0\)/
		);
		expect(() => resolveConfig({ ...base, idempotencyKeyTtl: "0ms" })).toThrow(
			/idempotencyKeyTtl must resolve to more than 0 ms, got 0 ms \(parsed from "0ms"\)/
		);
	});
});
