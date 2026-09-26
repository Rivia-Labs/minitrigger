import { Client, escapeIdentifier } from "pg";
import { describe, expect, it } from "vitest";
import { MIGRATIONS } from "../../src/migrations/index.js";
import { migrate, pendingMigrations } from "../../src/migrations/migrate.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();

const ALL_VERSIONS = MIGRATIONS.map(migration => migration.version);

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

describe("migrate()", () => {
	it("applies pending migrations exactly once under 5 parallel calls", async () => {
		const results = await Promise.all(Array.from({ length: 5 }, () => migrate(ns.pool, ns.schema)));

		const totalApplied = results.reduce((sum, result) => sum + result.applied.length, 0);
		expect(totalApplied).toBe(ALL_VERSIONS.length);

		const { rows } = await ns.pool.query<{ version: number }>(
			`SELECT version FROM ${escapeIdentifier(ns.schema)}.schema_migrations`
		);
		expect(rows).toHaveLength(ALL_VERSIONS.length);
		expect(rows.map(row => row.version).sort()).toEqual([...ALL_VERSIONS].sort());
	});

	it("is idempotent: a re-run after everything is applied applies nothing", async () => {
		const result = await migrate(ns.pool, ns.schema);
		expect(result.applied).toEqual([]);

		const { rows } = await ns.pool.query<{ version: number }>(
			`SELECT version FROM ${escapeIdentifier(ns.schema)}.schema_migrations`
		);
		expect(rows).toHaveLength(ALL_VERSIONS.length);
	});

	it("creates exactly the expected tables", async () => {
		const { rows } = await ns.pool.query<{ tablename: string }>(
			"SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY tablename",
			[ns.schema]
		);

		expect(rows.map(row => row.tablename)).toEqual(
			[
				"idempotency_keys",
				"queue_state",
				"run_logs",
				"runs",
				"schedules",
				"schema_migrations",
				"steps",
				"waitpoints",
			].sort()
		);
	});

	it("creates the specified partial indexes, and no index touches heartbeat_at or updated_at", async () => {
		const { rows } = await ns.pool.query<{ indexname: string; indexdef: string }>(
			"SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1",
			[ns.schema]
		);
		expect(rows.length).toBeGreaterThan(0);

		for (const { indexdef } of rows) {
			const lower = indexdef.toLowerCase();
			expect(lower).not.toContain("heartbeat_at");
			expect(lower).not.toContain("updated_at");
		}

		const byName = new Map(rows.map(row => [row.indexname, row.indexdef]));

		expect(byName.get("idx_runs_running")).toContain("WHERE (status = 'running'::text)");
		expect(byName.get("idx_runs_finished_at")).toMatch(
			/WHERE \(status = ANY \(ARRAY\['completed'::text, 'failed'::text, 'canceled'::text\]\)\)/
		);
		expect(byName.get("idx_waitpoints_pending")).toContain("WHERE (status = 'pending'::text)");
		expect(byName.get("idx_waitpoints_expires_pending")).toContain(
			"WHERE (status = 'pending'::text)"
		);
		expect(byName.get("idx_runs_created")).toContain("(created_at DESC, id DESC)");
		expect(byName.get("idx_runs_active_created")).toMatch(
			/\(created_at, id\) WHERE \(status = ANY \(ARRAY\['queued'::text, 'running'::text, 'waiting'::text\]\)\)/
		);
	});

	it("creates runs with fillfactor=80", async () => {
		const { rows } = await ns.pool.query<{ reloptions: string[] | null }>(
			`SELECT reloptions
			 FROM pg_class c
			 JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname = $1 AND c.relname = 'runs'`,
			[ns.schema]
		);

		expect(rows[0]?.reloptions).toContain("fillfactor=80");
	});

	it("gives every foreign key the specified ON DELETE action", async () => {
		const { rows } = await ns.pool.query<{ conname: string; confdeltype: string }>(
			`SELECT conname, confdeltype
			 FROM pg_constraint
			 WHERE contype = 'f'
			   AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)`,
			[ns.schema]
		);
		const byName = new Map(rows.map(row => [row.conname, row.confdeltype]));

		// c = cascade, n = set null (pg_constraint.confdeltype)
		expect(byName.get("idempotency_keys_run_id_fkey")).toBe("c");
		expect(byName.get("steps_run_id_fkey")).toBe("c");
		expect(byName.get("waitpoints_run_id_fkey")).toBe("c");
		expect(byName.get("run_logs_run_id_fkey")).toBe("c");
		expect(byName.get("runs_parent_run_id_fkey")).toBe("n");
		expect(byName.get("runs_parent_waitpoint_id_fkey")).toBe("n");
	});

	it("migrates two different schemas independently", async () => {
		const otherSchema = `${ns.schema}_other`;
		await ns.pool.query(`CREATE SCHEMA ${escapeIdentifier(otherSchema)}`);
		try {
			const result = await migrate(ns.pool, otherSchema);
			expect(result.applied).toEqual(ALL_VERSIONS);

			const { rows: otherRows } = await ns.pool.query<{ version: number }>(
				`SELECT version FROM ${escapeIdentifier(otherSchema)}.schema_migrations`
			);
			expect(otherRows).toHaveLength(ALL_VERSIONS.length);

			// The original namespace's schema_migrations is untouched by migrating the other schema.
			const { rows: nsRows } = await ns.pool.query<{ version: number }>(
				`SELECT version FROM ${escapeIdentifier(ns.schema)}.schema_migrations`
			);
			expect(nsRows).toHaveLength(ALL_VERSIONS.length);
		} finally {
			await ns.pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(otherSchema)} CASCADE`);
		}
	});

	it("is not blocked while another session holds the leader advisory lock (hashtext(schema), 2)", async () => {
		const leaderSchema = `${ns.schema}_leader`;
		await ns.pool.query(`CREATE SCHEMA ${escapeIdentifier(leaderSchema)}`);

		const leader = new Client({ connectionString: ns.databaseUrl });
		await leader.connect();
		try {
			await leader.query("SELECT pg_advisory_lock(hashtext($1), 2)", [leaderSchema]);

			const raced = await Promise.race([
				migrate(ns.pool, leaderSchema).then(result => ({ timedOut: false as const, result })),
				delay(5_000).then(() => ({ timedOut: true as const })),
			]);

			expect(raced.timedOut).toBe(false);
			if (!raced.timedOut) {
				expect(raced.result.applied).toEqual(ALL_VERSIONS);
			}
		} finally {
			await leader.query("SELECT pg_advisory_unlock(hashtext($1), 2)", [leaderSchema]);
			await leader.end();
			await ns.pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(leaderSchema)} CASCADE`);
		}
	});
});

describe("pendingMigrations()", () => {
	it("returns every migration as pending when the schema doesn't exist yet", async () => {
		const missingSchema = `${ns.schema}_missing`;
		const pending = await pendingMigrations(ns.pool, missingSchema);
		expect(pending).toEqual(ALL_VERSIONS);
	});

	it("returns nothing pending once migrate() has applied everything", async () => {
		const pending = await pendingMigrations(ns.pool, ns.schema);
		expect(pending).toEqual([]);
	});
});
