import { ConfigError } from "../errors.js";
import type { SqlQueryable } from "../store/sql/db.js";

/**
 * What every ORM adapter returns: the `TransactionClient` shape `trigger(..., { tx })` takes
 * (decision 26), i.e. `{ query(text, values) }` with `$1..$n` placeholders resolving to
 * `{ rows }`. Identical to the main entry's `TransactionClient`.
 */
export type TransactionClient = SqlQueryable;

type QueryResult = Awaited<ReturnType<SqlQueryable["query"]>>;

/** Short, never-throwing description of a value for error messages (no contents dumped). */
export function describeHandle(value: unknown): string {
	if (value === null) return "null";
	if (typeof value === "function") return "a function";
	if (typeof value !== "object") return typeof value;
	const name = (value as { constructor?: { name?: unknown } }).constructor?.name;
	return typeof name === "string" && name !== "Object" ? `a ${name}` : "an object";
}

/** `ConfigError` for a handle that is not what the adapter wraps. */
export function wrongHandle(adapter: string, expected: string, value: unknown): ConfigError {
	return new ConfigError(`${adapter}() expects ${expected}, got ${describeHandle(value)}`);
}

/**
 * Normalizes what an ORM's raw query resolved to: a node-postgres `QueryResult` (`{ rows }`),
 * or a plain row array (Prisma, postgres.js `RowList`, TypeORM `records`).
 */
export function toQueryResult(adapter: string, result: unknown): QueryResult {
	if (Array.isArray(result)) return { rows: result };
	if (result !== null && typeof result === "object") {
		const { rows, rowCount } = result as { rows?: unknown; rowCount?: unknown };
		if (Array.isArray(rows)) {
			return typeof rowCount === "number" ? { rows, rowCount } : { rows };
		}
	}
	throw new ConfigError(
		`${adapter}: the raw query resolved to ${describeHandle(result)} instead of rows; ` +
			"is the handle bound to a PostgreSQL connection?"
	);
}
