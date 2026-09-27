import { escapeIdentifier, type Pool, type PoolClient, type QueryResultRow } from "pg";

/**
 * The minimal client a transaction-bound repository needs: node-postgres `PoolClient`/`Client`
 * satisfy it, and so can a thin adapter over another driver/ORM transaction (decision 26: the seam
 * for `trigger(..., { tx })`). Parameters use `$1..$n` placeholders; `rowCount` may be omitted.
 */
export interface SqlQueryable {
	query(text: string, values?: unknown[]): Promise<{ rows: unknown[]; rowCount?: number | null }>;
}

/**
 * Where a repository sends its SQL. Bound to the pool, each store method opens its own
 * transaction; bound to a transaction, it runs inside the caller's (see docs/store-contract.md).
 */
export type SqlExecutor = { kind: "pool"; pool: Pool } | { kind: "tx"; client: SqlQueryable };

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
	| "schedules"
	| "queue_state"
	| "system_jobs"
	| "schema_meta";

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

/** A {@link Db} over one client (a transaction's, or a checked-out pool client). */
export function wrap(client: SqlQueryable): Db {
	return {
		async query<R extends QueryResultRow>(text: string, values?: unknown[]) {
			const res = await client.query(text, values);
			return { rows: res.rows as R[], rowCount: res.rowCount ?? 0 };
		},
	};
}

/**
 * A client checked out of `pool` with a no-op `error` listener attached until `release`. pg-pool
 * removes its own listener from a checked-out client, and `pg` emits an `error` event when the
 * socket ends unexpectedly (a terminated backend, a failover) -- even after the in-flight query
 * already rejected, or while the client sits idle in a transaction. Without a listener that event
 * is an uncaught exception that crashes the host process. The failure itself still surfaces as the
 * rejection of the next (or current) query. `release(err)` detaches the listener first (pg-pool
 * re-adds its own), so a reused client never accumulates listeners.
 */
export async function checkOut(
	pool: Pool
): Promise<{ client: PoolClient; release(err?: Error): void }> {
	const client = await pool.connect();
	const onError = () => {};
	client.on("error", onError);
	let released = false;
	return {
		client,
		release(err) {
			if (released) return;
			released = true;
			client.removeListener("error", onError);
			client.release(err);
		},
	};
}

/**
 * An error the server reported (a `pg` `DatabaseError`: SQLSTATE `code` + `severity`), as opposed
 * to a socket/protocol failure. Checked structurally, so a `pg` copy other than ours (an injected
 * pool) still counts.
 */
function isServerError(err: unknown): boolean {
	if (typeof err !== "object" || err === null) return false;
	const { code, severity } = err as { code?: unknown; severity?: unknown };
	return typeof code === "string" && typeof severity === "string" && severity !== "FATAL";
}

/**
 * After a failed transaction: a client whose ROLLBACK succeeded after a server-reported statement
 * error (a statement timeout, a constraint, a division by zero...) is in sync and goes back to the
 * pool; destroying it only churned connections under contention (review 5, A7). A failed ROLLBACK,
 * a FATAL or a non-server error destroys it.
 */
export function releaseAfterRollback(
	release: (err?: Error) => void,
	err: unknown,
	rolledBack: boolean
): void {
	if (rolledBack && isServerError(err)) release();
	else release(err instanceof Error ? err : new Error(String(err)));
}

/**
 * Runs `fn` atomically. On a pool executor it opens BEGIN/COMMIT on a dedicated client (released
 * per {@link releaseAfterRollback} on failure); on a tx executor it runs inside the caller's
 * transaction. Never hold one of these open while user code runs (SPEC §6).
 */
export async function atomic<T>(exec: SqlExecutor, fn: (db: Db) => Promise<T>): Promise<T> {
	if (exec.kind === "tx") return fn(wrap(exec.client));
	const { client, release } = await checkOut(exec.pool);
	let begun = false;
	try {
		await client.query("BEGIN");
		begun = true;
		const result = await fn(wrap(client));
		await client.query("COMMIT");
		release();
		return result;
	} catch (err) {
		const rolledBack = begun
			? await client.query("ROLLBACK").then(
					() => true,
					() => false
				)
			: false;
		releaseAfterRollback(release, err, rolledBack);
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
