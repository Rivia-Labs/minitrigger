import type { ClaimResult, IdempotencyStore } from "../types.js";
import { atomic, type Db, direct, type SqlContext, type SqlExecutor } from "./db.js";
import { nowPlusMs } from "./resolve.js";

export interface ClaimInput {
	taskId: string;
	key: string;
	runId: string;
	ttlMs: number | null;
}

/**
 * Decision 9, step 2, verbatim: a fresh key or an expired one is taken by `runId`; a live key
 * (including `expires_at NULL`, since `NULL < now()` is not true) returns no row and the owner is
 * read in the same transaction. Never inspects `xmax`. Returns null when the live key vanished
 * between the two statements (its run was deleted); callers retry.
 */
export async function claimKey(
	db: Db,
	ctx: SqlContext,
	input: ClaimInput
): Promise<ClaimResult | null> {
	const { rows } = await db.query<{ run_id: string }>(
		`INSERT INTO ${ctx.t("idempotency_keys")} AS k (task_id, key, run_id, expires_at)
		 VALUES ($1, $2, $3, CASE WHEN $4::float8 IS NULL THEN NULL ELSE ${nowPlusMs("$4")} END)
		 ON CONFLICT (task_id, key) DO UPDATE
		   SET run_id = EXCLUDED.run_id, expires_at = EXCLUDED.expires_at
		   WHERE k.expires_at < now()
		 RETURNING run_id`,
		[input.taskId, input.key, input.runId, input.ttlMs]
	);
	if (rows[0]) return { claimed: true };
	const existing = await db.query<{ run_id: string }>(
		`SELECT run_id FROM ${ctx.t("idempotency_keys")} WHERE task_id = $1 AND key = $2`,
		[input.taskId, input.key]
	);
	const row = existing.rows[0];
	// No row: the live key vanished between the two statements (retention deleted its run).
	return row ? { claimed: false, existingRunId: row.run_id } : null;
}

export function createIdempotencyStore(exec: SqlExecutor, ctx: SqlContext): IdempotencyStore {
	return {
		async claim(input) {
			for (let i = 0; i < 2; i++) {
				const result = await atomic(exec, db => claimKey(db, ctx, input));
				if (result) return result;
			}
			throw new Error(`idempotency key (${input.taskId}, ${input.key}) kept vanishing`);
		},

		async find(taskId, key) {
			const { rows } = await direct(exec).query<{ run_id: string; expires_at: Date | null }>(
				`SELECT run_id, expires_at FROM ${ctx.t("idempotency_keys")}
				 WHERE task_id = $1 AND key = $2`,
				[taskId, key]
			);
			const row = rows[0];
			return row ? { runId: row.run_id, expiresAt: row.expires_at } : null;
		},
	};
}
