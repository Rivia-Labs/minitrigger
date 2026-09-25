import { escapeIdentifier, type Pool, type PoolClient, type QueryResultRow } from "pg";

/**
 * Where a repository sends its SQL. Bound to the pool, each store method opens its own
 * transaction; bound to a transaction, it runs inside the caller's (see docs/store-contract.md).
 */
export type SqlExecutor = { kind: "pool"; pool: Pool } | { kind: "tx"; client: PoolClient };

export interface SqlContext {
	/** Escaped schema name, e.g. `"minitrigger"`. */
	readonly schema: string;
	/** Fully qualified, escaped table name: `t("runs")` → `"minitrigger".runs`. */
	t(table: TableName): string;
}

export type TableName =
	| "runs"
	| "steps"
	| "waitpoints"
	| "idempotency_keys"
	| "run_logs"
	| "schema_migrations"
	| "schedules";

export function createSqlContext(schema: string): SqlContext {
	const escaped = escapeIdentifier(schema);
	return { schema: escaped, t: table => `${escaped}.${table}` };
}

export interface Db {
	query<R extends QueryResultRow = QueryResultRow>(
		text: string,
		values?: unknown[]
	): Promise<{ rows: R[]; rowCount: number }>;
}

function wrap(client: PoolClient): Db {
	return {
		async query<R extends QueryResultRow>(text: string, values?: unknown[]) {
			const res = await client.query<R>(text, values);
			return { rows: res.rows, rowCount: res.rowCount ?? 0 };
		},
	};
}

/**
 * Runs `fn` atomically. On a pool executor it opens BEGIN/COMMIT on a dedicated client and
 * destroys the client on failure; on a tx executor it runs inside the caller's transaction.
 * Never hold one of these open while user code runs (SPEC §6).
 */
export async function atomic<T>(exec: SqlExecutor, fn: (db: Db) => Promise<T>): Promise<T> {
	if (exec.kind === "tx") return fn(wrap(exec.client));
	const client = await exec.pool.connect();
	try {
		await client.query("BEGIN");
		const result = await fn(wrap(client));
		await client.query("COMMIT");
		client.release();
		return result;
	} catch (err) {
		await client.query("ROLLBACK").catch(() => {});
		client.release(err instanceof Error ? err : new Error(String(err)));
		throw err;
	}
}

/** Single statement, no explicit transaction when bound to the pool. */
export function direct(exec: SqlExecutor): Db {
	if (exec.kind === "tx") return wrap(exec.client);
	return {
		async query<R extends QueryResultRow>(text: string, values?: unknown[]) {
			const res = await exec.pool.query<R>(text, values);
			return { rows: res.rows, rowCount: res.rowCount ?? 0 };
		},
	};
}
