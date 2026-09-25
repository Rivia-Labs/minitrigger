import type { ClaimResult, IdempotencyStore } from "../types.js";
import type { Exec } from "./resolve.js";
import { fkViolation, keyOf, type MemTx, pgText, toDate, uuid } from "./state.js";

export interface ClaimInput {
	taskId: string;
	key: string;
	runId: string;
	ttlMs: number | null;
}

/**
 * Decision 9, step 2: a fresh key or an expired one (`expires_at < now()`; NULL never expires)
 * is taken by `runId`; a live key returns its owner. The `run_id` FK is checked on write.
 * Never returns null here (single-threaded: the owner cannot vanish between two statements), but
 * keeps the SQL signature.
 */
export function claimKey(tx: MemTx, input: ClaimInput): ClaimResult | null {
	const taskId = pgText(input.taskId);
	const key = pgText(input.key);
	const k = keyOf(taskId, key);
	const existing = tx.db.keys.get(k);
	if (existing && !(existing.expiresAt !== null && existing.expiresAt < tx.now)) {
		return { claimed: false, existingRunId: existing.runId };
	}
	const runId = uuid(input.runId);
	if (!tx.db.runs.has(runId)) {
		throw fkViolation("idempotency_keys", "idempotency_keys_run_id_fkey");
	}
	tx.set(tx.db.keys, k, {
		taskId,
		key,
		runId,
		expiresAt: input.ttlMs === null ? null : tx.now + input.ttlMs,
	});
	return { claimed: true };
}

export function createIdempotencyStore(exec: Exec): IdempotencyStore {
	return {
		async claim(input) {
			for (let i = 0; i < 2; i++) {
				const result = await exec.run(tx => claimKey(tx, input));
				if (result) return result;
			}
			throw new Error(`idempotency key (${input.taskId}, ${input.key}) kept vanishing`);
		},

		find(taskId, key) {
			return exec.run(tx => {
				const row = tx.db.keys.get(keyOf(pgText(taskId), pgText(key)));
				return row ? { runId: row.runId, expiresAt: toDate(row.expiresAt) } : null;
			});
		},
	};
}
