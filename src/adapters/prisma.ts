/**
 * `@rivia-labs/minitrigger/prisma`: `trigger(task, payload, { tx: prismaTx(tx) })` inside a Prisma
 * interactive transaction (decision 26). Structural types only: nothing here imports Prisma.
 */
import { ConfigError } from "../errors.js";
import { type TransactionClient, toQueryResult, wrongHandle } from "./shared.js";

export type { TransactionClient } from "./shared.js";

/** The part of a Prisma interactive-transaction client (`$transaction(async tx => ...)`) used. */
export interface PrismaTransactionLike {
	$queryRawUnsafe(query: string, ...values: any[]): PromiseLike<unknown>;
}

const ADAPTER = "prismaTx";

/**
 * Wraps the `tx` of `prisma.$transaction(async tx => ...)` (interactive transaction, PostgreSQL),
 * so the run commits or rolls back with it. Statements go through `tx.$queryRawUnsafe(sql,
 * ...values)`: PostgreSQL `$n` placeholders are passed through as-is and the values as bind
 * parameters. Prisma's `$queryRawUnsafe` sends every value with a declared type of `text`, unlike
 * node-postgres (which leaves parameters untyped for Postgres to infer from context) -- Postgres
 * has no implicit/assignment cast from `text` to e.g. `uuid`, so this only works because every `$n`
 * in the SQL the engine runs through `options.tx` (`trigger(..., { tx })`'s create statement, the
 * only path an adapter's `query()` ever sees) is cast explicitly (`$1::uuid`, `$4::jsonb`, ...);
 * that guarantee does not extend to the rest of the engine's SQL, which never goes through an ORM
 * adapter.
 *
 * Throws `ConfigError` for the `PrismaClient` itself (not a transaction: each statement would
 * autocommit on its own). Keep the interactive transaction's `timeout` in mind: a trigger is one
 * or two short statements.
 */
export function prismaTx(tx: PrismaTransactionLike): TransactionClient {
	if (
		tx === null ||
		typeof tx !== "object" ||
		typeof (tx as { $queryRawUnsafe?: unknown }).$queryRawUnsafe !== "function"
	) {
		throw wrongHandle(ADAPTER, "the tx of prisma.$transaction(async tx => ...)", tx);
	}
	// The interactive-transaction client hides $connect/$disconnect (Prisma 5-7); Prisma 7 keeps
	// $transaction on it for nested transactions, so that one cannot tell them apart.
	if (typeof (tx as { $connect?: unknown }).$connect === "function") {
		throw new ConfigError(
			`${ADAPTER}(): got the PrismaClient, not an interactive transaction, so the run would not ` +
				"commit or roll back with your writes; pass the tx of prisma.$transaction(async tx => ...)"
		);
	}
	return {
		async query(text, values = []) {
			const params = values.map(value => (value === undefined ? null : value));
			return toQueryResult(ADAPTER, await tx.$queryRawUnsafe(text, ...params));
		},
	};
}
