/**
 * Run claim of the Postgres transport (decision 28, docs/store-contract.md "Postgres transport").
 * One statement picks due `queued`/`waiting` rows with `FOR UPDATE OF r SKIP LOCKED` and moves
 * them to `running`: it never waits on a row lock, so it cannot join a cycle with the
 * parent -> run -> descendants lock order. With `globalConcurrency`, a transaction-scoped
 * advisory lock per queue serializes the claims so the `running` count is exact.
 */
import type { ClaimRunsInput, RunClaimStore, RunRecord } from "../types.js";
import { atomic, type Db, direct, type SqlContext, type SqlExecutor } from "./db.js";
import { mapRun, qualifiedRunColumns, type RunRow } from "./resolve.js";

function time(date: Date | null): number {
	return date === null ? Number.NEGATIVE_INFINITY : date.getTime();
}

/** Same order as the claim's `ORDER BY r.run_after, r.created_at, r.id`. */
function byRunAfterCreatedId(a: RunRecord, b: RunRecord): number {
	return (
		time(a.runAfter) - time(b.runAfter) ||
		a.createdAt.getTime() - b.createdAt.getTime() ||
		(a.id < b.id ? -1 : a.id > b.id ? 1 : 0)
	);
}

export function createRunClaimStore(exec: SqlExecutor, ctx: SqlContext): RunClaimStore {
	const runs = ctx.t("runs");
	const queueState = ctx.t("queue_state");
	const returning = qualifiedRunColumns("r");

	async function claim(db: Db, queue: string, limit: number): Promise<RunRecord[]> {
		const { rows } = await db.query<RunRow>(
			`WITH picked AS (
			   SELECT r.id FROM ${runs} r
			   WHERE r.queue = $1
			     AND r.status IN ('queued', 'waiting')
			     AND r.run_after <= now()
			     AND NOT r.outbox_pending
			     AND NOT EXISTS (SELECT 1 FROM ${queueState} q WHERE q.queue = r.queue AND q.paused)
			   ORDER BY r.run_after, r.created_at, r.id
			   LIMIT $2::int
			   FOR UPDATE OF r SKIP LOCKED
			 )
			 UPDATE ${runs} r
			 SET status = 'running', execution_id = gen_random_uuid(), heartbeat_at = now(),
			     started_at = CASE WHEN r.concurrency_key IS NULL
			                       THEN coalesce(r.started_at, now()) ELSE r.started_at END,
			     debounce_key = NULL, updated_at = now()
			 FROM picked WHERE r.id = picked.id
			 RETURNING ${returning}`,
			[queue, limit]
		);
		// UPDATE ... RETURNING does not guarantee order: re-sort like the SELECT.
		return rows.map(mapRun).sort(byRunAfterCreatedId);
	}

	return {
		async claimRuns({ queue, limit, globalConcurrency }: ClaimRunsInput): Promise<RunRecord[]> {
			if (limit <= 0) return [];
			if (globalConcurrency === undefined) return claim(direct(exec), queue, limit);
			return atomic(exec, async db => {
				// Decision 28(e): serializes the queue's claims; the count below sees every committed
				// `running` (READ COMMITTED: fresh snapshot per statement). One bigint key, outside
				// `(hashtext(schema), 1|2)` (decision 16), like the decision 25 lock.
				await db.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
					`${ctx.schema}:${queue}:global`,
				]);
				const { rows } = await db.query<{ n: number }>(
					`SELECT count(*)::int AS n FROM ${runs} WHERE queue = $1 AND status = 'running'`,
					[queue]
				);
				const free = Math.min(limit, globalConcurrency - (rows[0]?.n ?? 0));
				return free > 0 ? claim(db, queue, free) : [];
			});
		},

		async nextDueAt(queue: string): Promise<Date | null> {
			const { rows } = await direct(exec).query<{ next: Date | null }>(
				`SELECT min(run_after) AS next FROM ${runs}
				 WHERE queue = $1 AND status IN ('queued', 'waiting') AND run_after > now()`,
				[queue]
			);
			return rows[0]?.next ?? null;
		},
	};
}
