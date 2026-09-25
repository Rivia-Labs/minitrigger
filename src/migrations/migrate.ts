import { escapeIdentifier, type Pool } from "pg";
import { validateSqlIdentifier } from "../utils/identifiers.js";
import { MIGRATIONS } from "./index.js";

export interface MigrateResult {
	/** Versions applied by *this* call, in order. A call that finds nothing pending returns `[]`. */
	readonly applied: number[];
}

/**
 * Applies every pending migration in {@link MIGRATIONS} to `schema`, inside a single
 * transaction (a failure rolls back everything, so a schema never ends up half-migrated).
 *
 * Concurrency: guarded by `pg_advisory_xact_lock(hashtext(schema), 1)` (decision 16 -- the "1"
 * lock is reserved for migrations, "2" for the reconciler/retention leader). Several concurrent
 * `migrate()` calls for the same schema serialize on this lock: the first to acquire it applies
 * whatever is pending and commits (releasing the lock), then every other call acquires the lock
 * in turn, finds nothing pending, and returns `{ applied: [] }`. `hashtext()` returns `int4`,
 * matching the two-int overload of `pg_advisory_xact_lock` used here.
 */
export async function migrate(pool: Pool, schema: string): Promise<MigrateResult> {
	validateSqlIdentifier("schema", schema);
	const quotedSchema = escapeIdentifier(schema);

	const client = await pool.connect();
	let queryError: Error | undefined;
	try {
		await client.query("BEGIN");
		await client.query("SELECT pg_advisory_xact_lock(hashtext($1), 1)", [schema]);

		// CREATE SCHEMA is not affected by search_path (it names the schema directly), so it runs
		// before search_path is set. Only when the schema is missing: `CREATE SCHEMA IF NOT EXISTS`
		// still requires CREATE on the database, which a role given a DBA-created schema usually
		// lacks. Concurrent migrate() calls are serialized by the advisory lock above, so the
		// check-then-create can't race another migrate() of the same schema.
		const { rows: schemaRows } = await client.query<{ exists: boolean }>(
			"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1) AS exists",
			[schema]
		);
		if (schemaRows[0]?.exists !== true) {
			await client.query(`CREATE SCHEMA IF NOT EXISTS ${quotedSchema}`);
		}

		// LOCAL: scoped to this transaction only. Never a session-wide SET, which would leak the
		// search_path to whatever else later borrows this connection from the pool -- including
		// user code, if the pool was injected (decision 6's spirit).
		await client.query(`SET LOCAL search_path TO ${quotedSchema}`);

		await client.query(`
			CREATE TABLE IF NOT EXISTS schema_migrations (
				version int PRIMARY KEY,
				name text NOT NULL,
				applied_at timestamptz NOT NULL DEFAULT now()
			)
		`);

		const { rows: appliedRows } = await client.query<{ version: number }>(
			"SELECT version FROM schema_migrations"
		);
		const alreadyApplied = new Set(appliedRows.map(row => row.version));

		const applied: number[] = [];
		for (const migration of MIGRATIONS) {
			if (alreadyApplied.has(migration.version)) continue;

			// Unqualified names in the embedded SQL resolve against `schema` via the search_path
			// set above.
			await client.query(migration.sql);
			await client.query("INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", [
				migration.version,
				migration.name,
			]);
			applied.push(migration.version);
		}

		await client.query("COMMIT");
		return { applied };
	} catch (error) {
		queryError = error instanceof Error ? error : new Error(String(error));
		await client.query("ROLLBACK").catch(() => {
			// Best-effort: the connection may already be unusable, which is why we also pass
			// `queryError` to release() below instead of returning it to the pool either way.
		});
		throw error;
	} finally {
		client.release(queryError);
	}
}

/**
 * Returns the versions from {@link MIGRATIONS} not yet applied to `schema`, without creating the
 * schema, taking any lock, or writing anything. Used for startup checks (decision 22 / Phase 9):
 * must not fail if `schema` (or `schema.schema_migrations`) doesn't exist yet -- that just means
 * every migration is pending.
 */
export async function pendingMigrations(pool: Pool, schema: string): Promise<number[]> {
	validateSqlIdentifier("schema", schema);
	const quotedSchema = escapeIdentifier(schema);
	const allVersions = MIGRATIONS.map(migration => migration.version);

	const { rows: existsRows } = await pool.query<{ exists: boolean }>(
		"SELECT to_regclass($1) IS NOT NULL AS exists",
		[`${schema}.schema_migrations`]
	);
	if (existsRows[0]?.exists !== true) {
		return allVersions;
	}

	const { rows } = await pool.query<{ version: number }>(
		`SELECT version FROM ${quotedSchema}.schema_migrations`
	);
	const alreadyApplied = new Set(rows.map(row => row.version));
	return allVersions.filter(version => !alreadyApplied.has(version));
}
