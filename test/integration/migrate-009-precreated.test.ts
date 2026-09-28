/**
 * Review 5, item C2: the README's "Upgrading to 0.4.0" tells operators with a large `runs` table
 * to pre-build migration 009's two indexes `CONCURRENTLY` under the same names before
 * `engine.migrate()`. This follows those steps literally on a schema at version 8 (the 0.3.x
 * schema) and checks that `migrate()` applies 009 and keeps the pre-built indexes (same OIDs, still
 * valid), instead of failing or building them again.
 */
import { Client, escapeIdentifier } from "pg";
import { describe, expect, it } from "vitest";
import { MIGRATIONS } from "../../src/migrations/index.js";
import { migrate } from "../../src/migrations/migrate.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();

/** Applies migrations up to `lastVersion` the way `migrate()` does (a 0.3.x schema). */
async function migrateUpTo(lastVersion: number): Promise<void> {
	const client = await ns.pool.connect();
	try {
		await client.query("BEGIN");
		await client.query(`SET LOCAL search_path TO ${escapeIdentifier(ns.schema)}`);
		await client.query(`
			CREATE TABLE IF NOT EXISTS schema_migrations (
				version int PRIMARY KEY,
				name text NOT NULL,
				applied_at timestamptz NOT NULL DEFAULT now()
			)
		`);
		for (const migration of MIGRATIONS) {
			if (migration.version > lastVersion) continue;
			await client.query(migration.sql);
			await client.query("INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", [
				migration.version,
				migration.name,
			]);
		}
		await client.query("COMMIT");
	} catch (error) {
		await client.query("ROLLBACK").catch(() => {});
		throw error;
	} finally {
		client.release();
	}
}

async function indexState(): Promise<Map<string, { oid: number; valid: boolean; def: string }>> {
	const { rows } = await ns.pool.query<{
		name: string;
		oid: number;
		valid: boolean;
		def: string;
	}>(
		`SELECT c.relname AS name, c.oid::int AS oid, i.indisvalid AS valid,
		        pg_get_indexdef(c.oid) AS def
		 FROM pg_index i
		 JOIN pg_class c ON c.oid = i.indexrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname IN ('idx_runs_claim', 'idx_runs_running_queue')`,
		[ns.schema]
	);
	return new Map(rows.map(row => [row.name, { oid: row.oid, valid: row.valid, def: row.def }]));
}

describe("migration 009 with its indexes pre-built CONCURRENTLY (README, Upgrading to 0.4.0)", () => {
	it("applies 009 and keeps the pre-built indexes", async () => {
		const versions = MIGRATIONS.map(m => m.version);
		expect(versions).toContain(9);
		await migrateUpTo(8);

		// A non-empty runs table, like the case the README is about.
		await ns.pool.query(
			`INSERT INTO ${escapeIdentifier(ns.schema)}.runs
			   (id, task_id, queue, payload, status, max_attempts, run_after)
			 SELECT gen_random_uuid(), 't', 'q' || (g % 5), 'null',
			        (ARRAY['queued','running','waiting','completed'])[1 + g % 4], 1, now()
			 FROM generate_series(1, 2000) g`
		);

		// The README's SQL, verbatim, on a session outside any transaction.
		const client = new Client({ connectionString: ns.databaseUrl });
		await client.connect();
		try {
			await client.query(`SET search_path TO ${escapeIdentifier(ns.schema)}`);
			await client.query(
				`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_claim ON runs (queue, run_after)
				   WHERE status IN ('queued', 'waiting')`
			);
			await client.query(
				`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_running_queue ON runs (queue)
				   WHERE status = 'running'`
			);
			const quoted = escapeIdentifier(ns.schema);
			const { rows } = await client.query<{ index_name: string; indisvalid: boolean }>(
				`SELECT indexrelid::regclass::text AS index_name, indisvalid FROM pg_index
				 WHERE indexrelid IN (
				   '${quoted}.idx_runs_claim'::regclass,
				   '${quoted}.idx_runs_running_queue'::regclass
				 )`
			);
			expect(rows).toHaveLength(2);
			expect(rows.every(row => row.indisvalid)).toBe(true);
		} finally {
			await client.end();
		}
		const before = await indexState();
		expect(before.size).toBe(2);

		const result = await migrate(ns.pool, ns.schema);
		expect(result.applied).toEqual([9]);

		const after = await indexState();
		for (const name of ["idx_runs_claim", "idx_runs_running_queue"]) {
			const was = before.get(name);
			const now = after.get(name);
			expect(now, name).toBeDefined();
			// Same index (not dropped and rebuilt), still valid, same definition.
			expect(now?.oid, name).toBe(was?.oid);
			expect(now?.valid, name).toBe(true);
			expect(now?.def, name).toBe(was?.def);
		}

		// The rest of 009 went in (the new tables), and a second migrate() is a no-op.
		const { rows: tables } = await ns.pool.query<{ name: string }>(
			`SELECT table_name AS name FROM information_schema.tables
			 WHERE table_schema = $1 AND table_name IN ('system_jobs', 'schema_meta')
			 ORDER BY table_name`,
			[ns.schema]
		);
		expect(tables.map(row => row.name)).toEqual(["schema_meta", "system_jobs"]);
		expect((await migrate(ns.pool, ns.schema)).applied).toEqual([]);
	});

	it("without pre-built indexes, 009 builds the same definitions", async () => {
		// Sanity check for the test above: the pre-built definitions equal what 009 itself creates.
		const before = await indexState();
		const { rows } = await ns.pool.query<{ def: string }>(
			`SELECT pg_get_indexdef(c.oid) AS def FROM pg_class c
			 JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname = $1 AND c.relname IN ('idx_runs_claim', 'idx_runs_running_queue')
			 ORDER BY c.relname`,
			[ns.schema]
		);
		expect(rows.map(row => row.def)).toEqual(
			["idx_runs_claim", "idx_runs_running_queue"].map(name => before.get(name)?.def)
		);
		// Rebuild with 009's own statements in a scratch schema and compare modulo the schema name.
		const scratch = `${ns.schema.slice(0, 50)}_c2`;
		try {
			await migrate(ns.pool, scratch);
			const { rows: fresh } = await ns.pool.query<{ def: string }>(
				`SELECT pg_get_indexdef(c.oid) AS def FROM pg_class c
				 JOIN pg_namespace n ON n.oid = c.relnamespace
				 WHERE n.nspname = $1 AND c.relname IN ('idx_runs_claim', 'idx_runs_running_queue')
				 ORDER BY c.relname`,
				[scratch]
			);
			expect(fresh.map(row => row.def.replaceAll(scratch, ns.schema))).toEqual(
				rows.map(row => row.def)
			);
		} finally {
			await ns.pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(scratch)} CASCADE`);
		}
	});
});
