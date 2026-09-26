/**
 * `@rivia-labs/minitrigger/knex`: `trigger(task, payload, { tx: knexTx(trx) })` inside a knex
 * transaction (decision 26). Structural types only: nothing here imports knex.
 */
import { ConfigError } from "../errors.js";
import { toKnexRaw } from "./placeholders.js";
import { type TransactionClient, toQueryResult, wrongHandle } from "./shared.js";

export type { TransactionClient } from "./shared.js";

/** The parts of a knex (`pg` client) `Transaction` the adapter uses. */
export interface KnexTransactionLike {
	raw(sql: string, bindings: any[]): PromiseLike<unknown>;
	readonly isTransaction?: boolean | undefined;
	isCompleted?(): boolean;
}

const ADAPTER = "knexTx";

/**
 * Wraps a knex transaction (`await knex.transaction()` or the `trx` of
 * `knex.transaction(async trx => ...)`), so the run commits or rolls back with it. The engine's
 * `$n` SQL is sent through `trx.raw(sql, bindings)`: each `$n` becomes a `?` binding (one per
 * occurrence) and every literal `?` (jsonb operators, string literals) is escaped as `\?`, so
 * values always travel as bind parameters.
 *
 * Throws `ConfigError` for the knex instance itself (not a transaction: each statement would
 * autocommit) or a transaction that already committed/rolled back.
 */
export function knexTx(trx: KnexTransactionLike): TransactionClient {
	const expected = "the trx of knex.transaction() (a knex transaction)";
	if (
		(typeof trx !== "function" && (trx === null || typeof trx !== "object")) ||
		typeof (trx as { raw?: unknown }).raw !== "function"
	) {
		throw wrongHandle(ADAPTER, expected, trx);
	}
	const dialect = (trx as { client?: { dialect?: unknown } }).client?.dialect;
	if (typeof dialect === "string" && dialect !== "postgresql") {
		throw new ConfigError(
			`${ADAPTER}(): the knex client is "${dialect}"; minitrigger needs "pg" (postgresql)`
		);
	}
	if (trx.isTransaction !== true) {
		throw new ConfigError(
			`${ADAPTER}(): got the knex instance, not a transaction, so the run would not commit or ` +
				"roll back with your writes; pass the trx of knex.transaction(async trx => ...)"
		);
	}
	const assertOpen = () => {
		if (trx.isCompleted?.() === true) {
			throw new ConfigError(`${ADAPTER}(): the knex transaction already committed or rolled back`);
		}
	};
	assertOpen();
	return {
		async query(text, values) {
			assertOpen();
			const { sql, bindings } = toKnexRaw(text, values);
			return toQueryResult(ADAPTER, await trx.raw(sql, bindings));
		},
	};
}
