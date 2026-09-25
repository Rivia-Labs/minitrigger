import { escapeIdentifier } from "pg";
import { describe, expect, it } from "vitest";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

// Paired with namespace-isolation.test.ts: both files deliberately use the same table name
// ("items") and the same redis key suffix ("marker"), so if the per-file schema/prefix ever
// collided, one file would see the other's row count / value.

const ns = useTestNamespace();

describe("useTestNamespace", () => {
	it("generates a schema and prefix that satisfy the identifier pattern", () => {
		expect(ns.schema).toMatch(/^[a-z_][a-z0-9_]{0,62}$/);
		expect(ns.prefix).toMatch(/^[a-z_][a-z0-9_]{0,62}$/);
		expect(ns.schema.length).toBeLessThanOrEqual(63);
	});

	it("exposes the configured connection strings", () => {
		expect(ns.databaseUrl).toContain("postgres://");
		expect(ns.redisUrl).toContain("redis://");
	});

	it("creates its own postgres schema and only sees its own rows", async () => {
		const table = `${escapeIdentifier(ns.schema)}.items`;
		await ns.pool.query(`CREATE TABLE ${table} (id int primary key, label text)`);
		await ns.pool.query(`INSERT INTO ${table} (id, label) VALUES (1, 'namespace-file-a')`);

		const { rows } = await ns.pool.query<{ id: number; label: string }>(
			`SELECT id, label FROM ${table} ORDER BY id`
		);

		expect(rows).toEqual([{ id: 1, label: "namespace-file-a" }]);
	});

	it("scopes redis keys under its own prefix", async () => {
		await ns.redis.set(`${ns.prefix}:marker`, "namespace-file-a");

		await expect(waitFor(() => ns.redis.get(`${ns.prefix}:marker`))).resolves.toBe(
			"namespace-file-a"
		);

		const keys = await ns.redis.keys(`${ns.prefix}*`);
		expect(keys).toEqual([`${ns.prefix}:marker`]);
	});
});
