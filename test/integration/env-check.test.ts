import { describe, expect, it } from "vitest";
import {
	checkEnvironment,
	checkPendingMigrations,
	checkPostgresVersion,
} from "../../src/env-check.js";
import { ConfigError } from "../../src/errors.js";
import { migrate } from "../../src/migrations/migrate.js";
import { useTestNamespace } from "../helpers/namespace.js";

// One Postgres schema/Redis prefix for the whole file (`useTestNamespace`): the "before
// migrate()" tests below must run (and are ordered, top to bottom, by vitest) before anything
// calls `migrate(ns.pool, ns.schema)`, and the "after migrate()" tests after it -- vitest doesn't
// isolate schema state between `it`s the way it isolates files.
const ns = useTestNamespace();

describe("checkPostgresVersion (real Postgres)", () => {
	it("resolves without a warning against the docker-compose Postgres (16)", async () => {
		await expect(checkPostgresVersion(ns.pool)).resolves.toBeUndefined();
	});

	it("throws ConfigError when the required minimum is above the real server's version", async () => {
		// The real server is Postgres 16 (docker-compose.yml); nothing satisfies "version >= 99".
		await expect(checkPostgresVersion(ns.pool, 990000)).rejects.toThrow(ConfigError);
		await expect(checkPostgresVersion(ns.pool, 990000)).rejects.toThrow(/99\.0/);
	});
});

describe("before migrate() has run", () => {
	it("checkPendingMigrations throws ConfigError against a fresh schema", async () => {
		await expect(checkPendingMigrations(ns.pool, ns.schema)).rejects.toThrow(ConfigError);
		await expect(checkPendingMigrations(ns.pool, ns.schema)).rejects.toThrow(new RegExp(ns.schema));
	});

	it("checkPendingMigrations does not throw when autoMigrate is true", async () => {
		await expect(
			checkPendingMigrations(ns.pool, ns.schema, { autoMigrate: true })
		).resolves.toBeUndefined();
	});

	it("checkEnvironment throws a ConfigError naming the pending migrations", async () => {
		let caught: unknown;
		try {
			await checkEnvironment({ pool: ns.pool, redis: ns.redis, schema: ns.schema });
		} catch (error) {
			caught = error;
		}
		expect(caught).toBeInstanceOf(ConfigError);
		expect((caught as ConfigError).message).toMatch(/pending migration/);
	});

	it("checkEnvironment does not throw for pending migrations when autoMigrate is true", async () => {
		const report = await checkEnvironment({
			pool: ns.pool,
			redis: ns.redis,
			schema: ns.schema,
			autoMigrate: true,
		});
		expect(report.warnings).toEqual([]);
	});
});

describe("after migrate() has run", () => {
	it("applies every migration to the test namespace's schema", async () => {
		await migrate(ns.pool, ns.schema);
	});

	it("checkPendingMigrations resolves without a warning", async () => {
		await expect(checkPendingMigrations(ns.pool, ns.schema)).resolves.toBeUndefined();
	});

	it("checkEnvironment returns no warnings (docker-compose Postgres 16 / Redis noeviction)", async () => {
		const report = await checkEnvironment({ pool: ns.pool, redis: ns.redis, schema: ns.schema });
		expect(report.warnings).toEqual([]);
	});

	it("surfaces the CONFIG-blocked warning instead of failing, when Redis rejects CONFIG GET", async () => {
		const warnLogs: Array<{ check: string; message: string }> = [];
		const report = await checkEnvironment({
			pool: ns.pool,
			// A fake standing in for a managed Redis provider that blocks CONFIG (decision 22): the
			// real `ns.redis` client is only used elsewhere in this file, to exercise the real
			// success path against the docker-compose Redis.
			redis: {
				config: async () => {
					throw new Error("ERR unknown command 'CONFIG', with args beginning with: 'GET'");
				},
			},
			schema: ns.schema,
			logger: {
				debug: () => {},
				info: () => {},
				warn: (fields, message) => {
					warnLogs.push({ check: String((fields as { check?: string }).check), message });
				},
				error: () => {},
			},
		});
		expect(report.warnings).toHaveLength(1);
		expect(report.warnings[0]?.check).toBe("redis-eviction-policy");
		expect(warnLogs).toHaveLength(1);
		expect(warnLogs[0]?.message).toMatch(/CONFIG/);
	});
});
