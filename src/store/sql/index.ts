import type { Pool } from "pg";
import { validateSqlIdentifier } from "../../utils/identifiers.js";
import type { Store, StoreTx } from "../types.js";
import { createSqlContext, type SqlExecutor } from "./db.js";
import { createIdempotencyStore } from "./idempotency.js";
import { createLogStore } from "./logs.js";
import { createRunStore } from "./runs.js";
import { createScheduleStore } from "./schedules.js";
import { createStepStore } from "./steps.js";
import { createWaitpointStore } from "./waitpoints.js";

/**
 * Builds the Postgres-backed {@link Store} for `schema` (decision 15; validated and escaped via
 * {@link SqlContext}). Repositories bound to the pool each open their own transaction per
 * multi-statement method (see docs/store-contract.md); {@link Store.withTransaction} opens one
 * transaction on a dedicated client and hands every repository, bound to that client, to the
 * caller.
 */
export function createSqlStore(pool: Pool, schema: string): Store {
	validateSqlIdentifier("schema", schema);
	const ctx = createSqlContext(schema);

	const poolExec: SqlExecutor = { kind: "pool", pool };

	return {
		runs: createRunStore(poolExec, ctx),
		steps: createStepStore(poolExec, ctx),
		waitpoints: createWaitpointStore(poolExec, ctx),
		idempotency: createIdempotencyStore(poolExec, ctx),
		logs: createLogStore(poolExec, ctx),
		schedules: createScheduleStore(poolExec, ctx),

		async withTransaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T> {
			const client = await pool.connect();
			try {
				await client.query("BEGIN");

				const txExec: SqlExecutor = { kind: "tx", client };
				const tx: StoreTx = {
					runs: createRunStore(txExec, ctx),
					steps: createStepStore(txExec, ctx),
					waitpoints: createWaitpointStore(txExec, ctx),
					idempotency: createIdempotencyStore(txExec, ctx),
					logs: createLogStore(txExec, ctx),
					schedules: createScheduleStore(txExec, ctx),
				};

				const result = await fn(tx);
				await client.query("COMMIT");
				client.release();
				return result;
			} catch (err) {
				await client.query("ROLLBACK").catch(() => {
					// Best-effort: the connection may already be unusable, which is why we also
					// destroy the client below regardless of whether ROLLBACK itself succeeded.
				});
				client.release(err instanceof Error ? err : new Error(String(err)));
				throw err;
			}
		},
	};
}
