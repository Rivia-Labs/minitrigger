import { escapeIdentifier } from "pg";
import { describe, expect, it } from "vitest";
import { useTestNamespace } from "../helpers/namespace.js";

// Paired with namespace.test.ts: both files deliberately use the same table name ("items") and
// the same redis key suffix ("marker") in their own namespace. If schemas or prefixes ever
// collided across files, one of the two would see 2 rows / the other file's value instead of 1
// row / its own value.

const ns = useTestNamespace();

describe("useTestNamespace isolation across files", () => {
	it("has a namespace distinct from other test files", () => {
		expect(ns.schema).toMatch(/^mt_test_[0-9a-f_]+$/);
		expect(ns.prefix).toBe(ns.schema);
	});

	it("does not see rows inserted by the other test file's namespace", async () => {
		const table = `${escapeIdentifier(ns.schema)}.items`;
		await ns.pool.query(`CREATE TABLE ${table} (id int primary key, label text)`);
		await ns.pool.query(`INSERT INTO ${table} (id, label) VALUES (1, 'namespace-file-b')`);

		const { rows } = await ns.pool.query<{ id: number; label: string }>(
			`SELECT id, label FROM ${table} ORDER BY id`
		);

		// Exactly this file's row: no cross-contamination from namespace.test.ts, which inserts
		// the same id into a same-named table in its own (different) schema.
		expect(rows).toEqual([{ id: 1, label: "namespace-file-b" }]);
	});

	it("does not see redis keys set by the other test file's namespace", async () => {
		await ns.redis.set(`${ns.prefix}:marker`, "namespace-file-b");

		const keys = await ns.redis.keys(`${ns.prefix}*`);
		expect(keys).toEqual([`${ns.prefix}:marker`]);

		const value = await ns.redis.get(`${ns.prefix}:marker`);
		expect(value).toBe("namespace-file-b");
	});

	it("cannot query the other schema by guessing its name", async () => {
		const bogusSchema = ns.schema.slice(0, -1) + (ns.schema.endsWith("0") ? "1" : "0");
		await expect(
			ns.pool.query(`SELECT 1 FROM ${escapeIdentifier(bogusSchema)}.items`)
		).rejects.toThrow();
	});
});
