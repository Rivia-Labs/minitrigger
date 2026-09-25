import { describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import {
	checkEnvironment,
	checkPendingMigrations,
	checkPostgresVersion,
	MIN_POSTGRES_VERSION_NUM,
	type PgQueryClient,
} from "../../src/env-check.js";
import { ConfigError } from "../../src/errors.js";
import { MIGRATIONS } from "../../src/migrations/index.js";
import type { RedisConfigClient } from "../../src/queue/redis-check.js";

/** Every migration already applied, so `checkPendingMigrations` sees nothing pending. */
const ALL_MIGRATIONS_APPLIED = MIGRATIONS.map(migration => ({ version: migration.version }));

function fakePool(rowsByQuery: Record<string, unknown[]>): PgQueryClient {
	return {
		query: async <T extends Record<string, unknown>>(text: string) => {
			for (const [key, rows] of Object.entries(rowsByQuery)) {
				if (text.includes(key)) return { rows: rows as T[] };
			}
			throw new Error(`fakePool: no fixture for query ${JSON.stringify(text)}`);
		},
	};
}

function fakeRedis(reply: unknown): RedisConfigClient {
	return { config: async () => reply };
}

function fakeLogger(): EngineLogger & {
	calls: Array<{ level: string; fields: unknown; message: string }>;
} {
	const calls: Array<{ level: string; fields: unknown; message: string }> = [];
	return {
		calls,
		debug: (fields, message) => calls.push({ level: "debug", fields, message }),
		info: (fields, message) => calls.push({ level: "info", fields, message }),
		warn: (fields, message) => calls.push({ level: "warn", fields, message }),
		error: (fields, message) => calls.push({ level: "error", fields, message }),
	};
}

describe("checkPostgresVersion", () => {
	it("resolves without a warning when the version meets the minimum", async () => {
		const pool = fakePool({ server_version_num: [{ server_version_num: "160001" }] });
		await expect(checkPostgresVersion(pool)).resolves.toBeUndefined();
	});

	it("throws ConfigError with an actionable message when the version is too old", async () => {
		const pool = fakePool({ server_version_num: [{ server_version_num: "120005" }] });
		await expect(checkPostgresVersion(pool)).rejects.toThrow(ConfigError);
		await expect(checkPostgresVersion(pool)).rejects.toThrow(/12\.5/);
		await expect(checkPostgresVersion(pool)).rejects.toThrow(/13\.0/);
	});

	it("respects a custom minVersionNum", async () => {
		const pool = fakePool({ server_version_num: [{ server_version_num: "160001" }] });
		await expect(checkPostgresVersion(pool, 170000)).rejects.toThrow(ConfigError);
	});

	it("returns a warning (does not throw) when the command fails outright", async () => {
		const pool: PgQueryClient = {
			query: async () => {
				throw new Error("permission denied for function pg_show_all_settings");
			},
		};
		const warning = await checkPostgresVersion(pool);
		expect(warning).toBeDefined();
		expect(warning).toMatch(/could not read Postgres/);
		expect(warning).toMatch(/permission denied/);
	});

	it("returns a warning when the reply shape is unexpected", async () => {
		const pool = fakePool({ server_version_num: [{}] });
		const warning = await checkPostgresVersion(pool);
		expect(warning).toMatch(/unexpected value/);
	});

	it("MIN_POSTGRES_VERSION_NUM is exported and used as the default floor", async () => {
		const pool = fakePool({
			server_version_num: [{ server_version_num: String(MIN_POSTGRES_VERSION_NUM - 1) }],
		});
		await expect(checkPostgresVersion(pool)).rejects.toThrow(ConfigError);
	});
});

describe("checkPendingMigrations", () => {
	it("resolves without a warning when nothing is pending", async () => {
		const pool = fakePool({
			to_regclass: [{ exists: true }],
			schema_migrations: ALL_MIGRATIONS_APPLIED,
		});
		await expect(checkPendingMigrations(pool, "mt_test")).resolves.toBeUndefined();
	});

	it("throws ConfigError listing the pending versions when autoMigrate is not set", async () => {
		const pool = fakePool({
			to_regclass: [{ exists: true }],
			schema_migrations: [{ version: 1 }],
		});
		await expect(checkPendingMigrations(pool, "mt_test")).rejects.toThrow(ConfigError);
		await expect(checkPendingMigrations(pool, "mt_test")).rejects.toThrow(/mt_test/);
	});

	it("does not throw when autoMigrate is true", async () => {
		const pool = fakePool({
			to_regclass: [{ exists: true }],
			schema_migrations: [{ version: 1 }],
		});
		await expect(
			checkPendingMigrations(pool, "mt_test", { autoMigrate: true })
		).resolves.toBeUndefined();
	});

	it("treats a schema that doesn't exist yet as every migration pending", async () => {
		const pool = fakePool({ to_regclass: [{ exists: false }] });
		await expect(checkPendingMigrations(pool, "mt_test")).rejects.toThrow(ConfigError);
	});

	it("returns a warning (does not throw) when the check itself fails", async () => {
		const pool: PgQueryClient = {
			query: async () => {
				throw new Error("connection terminated");
			},
		};
		const warning = await checkPendingMigrations(pool, "mt_test");
		expect(warning).toMatch(/could not check for pending migrations/);
		expect(warning).toMatch(/connection terminated/);
	});
});

describe("checkEnvironment", () => {
	function allGoodPool(): PgQueryClient {
		return fakePool({
			server_version_num: [{ server_version_num: "160001" }],
			to_regclass: [{ exists: true }],
			schema_migrations: ALL_MIGRATIONS_APPLIED,
		});
	}

	it("returns no warnings and does not throw when every check passes", async () => {
		const report = await checkEnvironment({
			pool: allGoodPool(),
			redis: fakeRedis(["maxmemory-policy", "noeviction"]),
			schema: "mt_test",
		});
		expect(report.warnings).toEqual([]);
	});

	it("collects a warning for CONFIG blocked on managed Redis without throwing", async () => {
		const logger = fakeLogger();
		const redis: RedisConfigClient = {
			config: async () => {
				throw new Error("ERR unknown command 'CONFIG'");
			},
		};
		const report = await checkEnvironment({
			pool: allGoodPool(),
			redis,
			schema: "mt_test",
			logger,
		});
		expect(report.warnings).toHaveLength(1);
		expect(report.warnings[0]?.check).toBe("redis-eviction-policy");
		expect(report.warnings[0]?.message).toMatch(/CONFIG/);
		expect(logger.calls).toHaveLength(1);
		expect(logger.calls[0]?.level).toBe("warn");
	});

	it("throws a single ConfigError combining every definitive failure", async () => {
		const badPool = fakePool({
			server_version_num: [{ server_version_num: "120000" }],
			to_regclass: [{ exists: true }],
			schema_migrations: [{ version: 1 }],
		});
		const badRedis = fakeRedis(["maxmemory-policy", "allkeys-lru"]);

		let caught: unknown;
		try {
			await checkEnvironment({ pool: badPool, redis: badRedis, schema: "mt_test" });
		} catch (error) {
			caught = error;
		}
		expect(caught).toBeInstanceOf(ConfigError);
		const message = (caught as ConfigError).message;
		expect(message).toMatch(/Postgres server version/);
		expect(message).toMatch(/maxmemory-policy/);
		expect(message).toMatch(/pending migration/);
	});

	it("does not throw for pending migrations when autoMigrate is true", async () => {
		const pool = fakePool({
			server_version_num: [{ server_version_num: "160001" }],
			to_regclass: [{ exists: true }],
			schema_migrations: [{ version: 1 }],
		});
		const report = await checkEnvironment({
			pool,
			redis: fakeRedis(["maxmemory-policy", "noeviction"]),
			schema: "mt_test",
			autoMigrate: true,
		});
		expect(report.warnings).toEqual([]);
	});

	it("wraps a generic (non-ConfigError) failure from the Redis check as a warning, per decision 22", async () => {
		const redis: RedisConfigClient = {
			config: vi.fn(async () => {
				throw new TypeError("boom");
			}),
		};
		const report = await checkEnvironment({
			pool: allGoodPool(),
			redis,
			schema: "mt_test",
		});
		expect(report.warnings).toHaveLength(1);
		expect(report.warnings[0]?.message).toMatch(/boom/);
	});
});
