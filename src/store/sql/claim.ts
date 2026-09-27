/**
 * Run claim of the Postgres transport (decision 28, docs/store-contract.md "Postgres transport").
 * One statement picks due `queued`/`waiting` rows with `FOR UPDATE OF r SKIP LOCKED` and moves
 * them to `running`: it never waits on a row lock, so it cannot join a cycle with the
 * parent -> run -> descendants lock order. With `globalConcurrency`, a transaction-scoped
 * advisory lock per queue serializes the claims so the `running` count is exact.
 */
import type { ClaimRunsInput, RunClaimStore, RunRecord } from "../types.js";
import { atomic, type Db, direct, type SqlContext, type SqlExecutor } from "./db.js";
import { DELAY_UNTIL_MIN_RUN_AFTER, mapRun, qualifiedRunColumns, type RunRow } from "./resolve.js";

/**
 * Server-side bound of each statement of a claim (`SET LOCAL statement_timeout`). The worker's
 * `drain()` waits `CLAIM_DRAIN_TIMEOUT_MS` (5 s, src/pg/workers.ts) for the claim in flight; a
 * claim must not commit after that (the engine may have ended the pool, leaving the claimed runs
 * `running` with nobody to run or hand them back). The globalConcurrency path runs three bounded
 * statements (lock, count, claim), so its worst case is 3 x this, still under the drain bound. A
 * timeout is a failed claim: rolled back, and the worker backs off.
 */
export const CLAIM_STATEMENT_TIMEOUT_MS = 1_500;

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
		// Due-ness uses `statement_timestamp()`, not `now()`: the claim runs in a transaction
		// (statement_timeout, advisory lock), and `now()` is frozen at its BEGIN, so a run that came
		// due between the BEGIN and this statement would be skipped. Written times keep `now()`.
		const { rows } = await db.query<RunRow>(
			`WITH picked AS (
			   SELECT r.id FROM ${runs} r
			   WHERE r.queue = $1
			     AND r.status IN ('queued', 'waiting')
			     AND r.run_after <= statement_timestamp()
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
			if (exec.kind === "tx") {
				throw new Error("claimRuns must not run inside a StoreTx (SET LOCAL would leak)");
			}
			return atomic(exec, async db => {
				// SET cannot take bind parameters; the value is a constant integer. Transaction-scoped
				// only (never a session SET on a pooled client).
				await db.query(`SET LOCAL statement_timeout = ${CLAIM_STATEMENT_TIMEOUT_MS}`);
				if (globalConcurrency === undefined) return claim(db, queue, limit);
				// Decision 28(e): serializes the queue's claims; the count below sees every committed
				// `running` (READ COMMITTED: fresh snapshot per statement). One bigint key, outside
				// `(hashtext(schema), 1|2)` (decision 16). Seed 1, not 0: the decision 25 lock hashes
				// `schema:queue:<user key>` with seed 0, so a user `concurrencyKey` of "global" would
				// otherwise produce this very key; with its own seed no user key can (short of a
				// 64-bit hash collision).
				await db.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 1))", [
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

		async nextDueInMs(queue: string): Promise<number | null> {
			// The delay is computed in SQL (decision 17): the app clock may drift from the DB's.
			const { rows } = await direct(exec).query<{ ms: number | null }>(
				`SELECT ${DELAY_UNTIL_MIN_RUN_AFTER} AS ms FROM ${runs}
				 WHERE queue = $1 AND status IN ('queued', 'waiting') AND run_after > now()`,
				[queue]
			);
			return rows[0]?.ms ?? null;
		},
	};
}
