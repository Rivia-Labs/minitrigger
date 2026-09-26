/**
 * `@rivia-labs/minitrigger/drizzle`: `trigger(task, payload, { tx: drizzleTx(tx, sql) })` inside a
 * Drizzle transaction (decision 26). Structural types only: nothing here imports drizzle-orm.
 */
import { ConfigError } from "../errors.js";
import { splitPlaceholders, valueAt } from "./placeholders.js";
import { type TransactionClient, toQueryResult, wrongHandle } from "./shared.js";

export type { TransactionClient } from "./shared.js";

/** The parts of a Drizzle PostgreSQL transaction (node-postgres or postgres.js driver) used. */
export interface DrizzleTransactionLike {
	execute(query: any): PromiseLike<unknown>;
	rollback(): unknown;
}

/** Drizzle's `sql` tag (`import { sql } from "drizzle-orm"`): the tag itself plus `sql.param`. */
export interface DrizzleSqlTag {
	(strings: TemplateStringsArray, ...params: any[]): any;
	param(value: any): any;
}

const ADAPTER = "drizzleTx";

/** `pg_current_xact_id()` (unique per transaction, even across a reused connection) + backend pid. */
const XACT_CHECK_SQL = "SELECT pg_current_xact_id()::text AS xid, pg_backend_pid() AS pid";

interface XactIdentity {
	readonly xid: string;
	readonly pid: number;
}

function toTemplateStrings(parts: readonly string[]): TemplateStringsArray {
	return Object.assign([...parts], { raw: [...parts] }) as unknown as TemplateStringsArray;
}

/** `undefined` when the result does not look like {@link XACT_CHECK_SQL}'s single row. */
function readXactIdentity(result: { rows: readonly unknown[] }): XactIdentity | undefined {
	const row = result.rows[0] as { xid?: unknown; pid?: unknown } | undefined;
	if (typeof row?.xid !== "string" || typeof row?.pid !== "number") return undefined;
	return { xid: row.xid, pid: row.pid };
}

/**
 * Wraps the `tx` of `db.transaction(async tx => ...)` (drizzle-orm, `node-postgres` or
 * `postgres-js` driver), so the run commits or rolls back with it. Pass drizzle's own `sql` tag
 * too (`import { sql } from "drizzle-orm"`): the adapter imports nothing from drizzle-orm, and
 * builds each statement with it through its public API, `$n` placeholders becoming
 * `sql.param(value)` (bind parameters, never inlined text).
 *
 * Throws `ConfigError` for the database object itself (not a transaction: each statement would
 * autocommit) or a missing `sql` tag.
 *
 * **Used after its transaction ended:** drizzle-orm 0.45.x exposes no stable "this transaction has
 * committed/rolled back" flag on either driver's transaction object (checked against the installed
 * `drizzle-orm/node-postgres` and `drizzle-orm/postgres-js` internals), and both drivers keep the
 * `tx` object fully queryable afterwards -- node-postgres simply releases the pooled client back to
 * the pool, so a stray query through the stale `tx` can silently run inside a *different*
 * transaction that later checks out the same connection. Since there is no reliable flag, this
 * instead compares Postgres's own transaction identity: `pg_current_xact_id()` (unique per
 * transaction) plus `pg_backend_pid()`, read once right here at wrap time -- while the transaction
 * must still be valid, per the documented contract of calling `drizzleTx(tx, sql)` synchronously
 * inside the `db.transaction(async tx => ...)` callback -- and again on every `query()` call. A
 * mismatch throws `ConfigError` instead of running silently. This adds one extra round trip per
 * `query()` call (plus the one at wrap time); a `trigger(..., { tx })` is one or two of them.
 */
export function drizzleTx(tx: DrizzleTransactionLike, sql: DrizzleSqlTag): TransactionClient {
	const expected =
		"the tx of db.transaction(async tx => ...) (a drizzle-orm PostgreSQL transaction)";
	if (
		tx === null ||
		typeof tx !== "object" ||
		typeof (tx as { execute?: unknown }).execute !== "function"
	) {
		throw wrongHandle(ADAPTER, expected, tx);
	}
	if (typeof (tx as { rollback?: unknown }).rollback !== "function") {
		throw new ConfigError(
			`${ADAPTER}(): got the drizzle database, not a transaction, so the run would not commit ` +
				"or roll back with your writes; pass the tx of db.transaction(async tx => ...)"
		);
	}
	if (typeof sql !== "function" || typeof (sql as { param?: unknown }).param !== "function") {
		throw new ConfigError(
			`${ADAPTER}(tx, sql): pass drizzle's sql tag as the second argument ` +
				'(import { sql } from "drizzle-orm"), got ' +
				(sql === undefined ? "nothing" : typeof sql)
		);
	}
	const xactCheck = () => sql(toTemplateStrings([XACT_CHECK_SQL]));
	// Kicked off immediately (not awaited here): by the time the first `query()` call reaches
	// `assertSameTransaction`, this has usually already resolved, and it was issued while the
	// transaction was presumably still valid (right at wrap time).
	const baseline: Promise<XactIdentity | undefined> = Promise.resolve(tx.execute(xactCheck())).then(
		result => readXactIdentity(toQueryResult(ADAPTER, result))
	);
	// Never queried (e.g. the caller only wraps but never uses `tx` inside the callback): avoid an
	// unhandled-rejection warning. `assertSameTransaction` still surfaces the real error on `await`.
	baseline.catch(() => {});
	async function assertSameTransaction(): Promise<void> {
		const base = await baseline;
		if (!base) return; // unexpected result shape; let the real query surface whatever is wrong
		const current = readXactIdentity(toQueryResult(ADAPTER, await tx.execute(xactCheck())));
		if (current && (current.xid !== base.xid || current.pid !== base.pid)) {
			throw new ConfigError(
				`${ADAPTER}(): this wrapper was used after its transaction ended (drizzle-orm keeps ` +
					"the tx object queryable, and node-postgres/postgres.js recycle the connection, so " +
					"this could be running inside an unrelated transaction); await every use of it -- " +
					"including trigger(..., { tx }) -- inside the db.transaction(async tx => ...) callback"
			);
		}
	}
	return {
		async query(text, values = []) {
			await assertSameTransaction();
			const { segments, indexes } = splitPlaceholders(text);
			const strings = toTemplateStrings(segments);
			const params = indexes.map(index => sql.param(valueAt(values, index)));
			return toQueryResult(ADAPTER, await tx.execute(sql(strings, ...params)));
		},
	};
}
