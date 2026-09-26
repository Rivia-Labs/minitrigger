/**
 * `@rivia-labs/minitrigger/typeorm`: `trigger(task, payload, { tx: typeormTx(manager) })` inside
 * a TypeORM transaction (decision 26). Structural types only: nothing here imports TypeORM.
 */
import { ConfigError } from "../errors.js";
import { type TransactionClient, toQueryResult, wrongHandle } from "./shared.js";

export type { TransactionClient } from "./shared.js";

/** The parts of a TypeORM (0.3.x / 1.x, `postgres` driver) `QueryRunner` the adapter uses. */
export interface TypeormQueryRunnerLike {
	readonly isTransactionActive: boolean;
	readonly isReleased: boolean;
	query(
		query: string,
		parameters: any[] | undefined,
		useStructuredResult: true
	): Promise<{ records?: unknown }>;
}

/** The parts of a TypeORM `EntityManager` the adapter uses (the transactional one has a runner). */
export interface TypeormEntityManagerLike {
	readonly queryRunner?: TypeormQueryRunnerLike | undefined;
}

const ADAPTER = "typeormTx";
const EXPECTED =
	"the EntityManager passed to dataSource.transaction(async manager => ...) or a QueryRunner " +
	"after queryRunner.startTransaction()";

function driverType(handle: object): unknown {
	const source =
		(handle as { dataSource?: unknown }).dataSource ??
		(handle as { connection?: unknown }).connection;
	return (source as { options?: { type?: unknown } } | undefined)?.options?.type;
}

/**
 * Wraps a TypeORM transactional `EntityManager` (the one `dataSource.transaction(async manager
 * => ...)` hands you) or a `QueryRunner` inside `startTransaction()`, so the run commits or rolls
 * back with it. Every statement runs on the transaction's own connection
 * (`queryRunner.query(sql, params, true)`, bind parameters), never on the pool.
 *
 * Throws `ConfigError` for a manager that is not bound to a transaction (`dataSource.manager`):
 * each trigger would then autocommit on its own, which is almost certainly a bug. Checked again
 * on every query, so a wrapper used after the transaction ended fails loudly too.
 */
export function typeormTx(
	handle: TypeormEntityManagerLike | TypeormQueryRunnerLike
): TransactionClient {
	if (handle === null || typeof handle !== "object") throw wrongHandle(ADAPTER, EXPECTED, handle);
	const isRunner =
		typeof (handle as { startTransaction?: unknown }).startTransaction === "function" &&
		typeof (handle as { query?: unknown }).query === "function";
	const runner = isRunner
		? (handle as TypeormQueryRunnerLike)
		: (handle as TypeormEntityManagerLike).queryRunner;
	if (
		!isRunner &&
		!("queryRunner" in handle) &&
		typeof (handle as { transaction?: unknown }).transaction !== "function"
	) {
		throw wrongHandle(ADAPTER, EXPECTED, handle);
	}
	const type = driverType(runner ?? handle);
	if (typeof type === "string" && type !== "postgres") {
		throw new ConfigError(
			`${ADAPTER}(): the DataSource uses the "${type}" driver; minitrigger needs "postgres"`
		);
	}
	const assertActive = (): TypeormQueryRunnerLike => {
		if (runner === undefined || runner === null) {
			throw new ConfigError(
				`${ADAPTER}(): this EntityManager is not bound to a transaction (e.g. dataSource.manager), ` +
					"so the run would not commit or roll back with your writes; pass the manager of " +
					"dataSource.transaction(async manager => ...) instead"
			);
		}
		if (runner.isReleased || !runner.isTransactionActive) {
			throw new ConfigError(
				`${ADAPTER}(): the QueryRunner has no active transaction (not started, already ` +
					"committed/rolled back, or released); trigger inside the transaction"
			);
		}
		return runner;
	};
	assertActive();
	return {
		async query(text, values) {
			const result = await assertActive().query(text, values ?? [], true);
			return toQueryResult(ADAPTER, result.records ?? []);
		},
	};
}
