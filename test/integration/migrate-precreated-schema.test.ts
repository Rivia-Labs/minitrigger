/**
 * Audit low item (a): a DBA pre-creates the schema and grants it to the application role, which
 * has no CREATE privilege on the database. `CREATE SCHEMA IF NOT EXISTS` still checks that
 * privilege, so `migrate()` must only issue it when the schema is missing.
 */
import { randomUUID } from "node:crypto";
import { escapeIdentifier, Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MIGRATIONS } from "../../src/migrations/index.js";
import { migrate } from "../../src/migrations/migrate.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
const role = `mt_limited_${randomUUID().replaceAll("-", "").slice(0, 16)}`;
const password = randomUUID();
let limitedPool: Pool | undefined;

function limitedUrl(): string {
	const url = new URL(ns.databaseUrl);
	url.username = role;
	url.password = password;
	return url.toString();
}

beforeAll(async () => {
	const r = escapeIdentifier(role);
	const db = (await ns.pool.query<{ db: string }>("SELECT current_database() AS db")).rows[0]?.db;
	if (!db) throw new Error("no current_database()");
	await ns.pool.query(`CREATE ROLE ${r} LOGIN PASSWORD '${password}'`);
	// Only CONNECT on the database (PUBLIC's default); explicitly no CREATE.
	await ns.pool.query(`REVOKE CREATE ON DATABASE ${escapeIdentifier(db)} FROM ${r}`);
	await ns.pool.query(`GRANT USAGE, CREATE ON SCHEMA ${escapeIdentifier(ns.schema)} TO ${r}`);
	limitedPool = new Pool({ connectionString: limitedUrl(), max: 2 });
});

afterAll(async () => {
	await limitedPool?.end();
	const r = escapeIdentifier(role);
	await ns.pool.query(`DROP OWNED BY ${r} CASCADE`);
	await ns.pool.query(`DROP ROLE IF EXISTS ${r}`);
});

describe("migrate() on a schema pre-created by a DBA", () => {
	it("applies every migration without CREATE on the database", async () => {
		const pool = limitedPool;
		if (!pool) throw new Error("no pool");
		// Precondition: the role really lacks CREATE on the database.
		const { rows } = await pool.query<{ can: boolean }>(
			"SELECT has_database_privilege(current_database(), 'CREATE') AS can"
		);
		expect(rows[0]?.can).toBe(false);

		const result = await migrate(pool, ns.schema);
		expect(result.applied).toEqual(MIGRATIONS.map(m => m.version));
		expect((await migrate(pool, ns.schema)).applied).toEqual([]);
	});
});
