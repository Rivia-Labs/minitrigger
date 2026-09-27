/**
 * Run claim of the Postgres transport (decision 28, docs/store-contract.md "Postgres transport").
 * One statement picks due `queued`/`waiting` rows with `FOR UPDATE OF r SKIP LOCKED` and moves
 * them to `running`: it never waits on a row lock, so it cannot join a cycle with the
 * parent -> run -> descendants lock order. With `globalConcurrency`, a transaction-scoped
 * advisory lock per queue serializes the claims so the `running` count is exact.
 */

import { MinitriggerError } from "../../errors.js";
import type { ClaimRunsInput, RunClaimStore, RunRecord } from "../types.js";
import { checkOut, type Db, direct, type SqlContext, type SqlExecutor, wrap } from "./db.js";
import { DELAY_UNTIL_MIN_RUN_AFTER, mapRun, qualifiedRunColumns, type RunRow } from "./resolve.js";

/**
 * Server-side bound of each statement of a claim (`SET LOCAL statement_timeout`). The worker's
 * `drain()` waits `CLAIM_DRAIN_TIMEOUT_MS` (5 s, src/pg/workers.ts) for the claim in flight; a
 * claim must not commit after that (the engine may have ended the pool, leaving the claimed runs
 * `running` with nobody to run or hand them back). The globalConcurrency path runs three bounded
 * statements (lock, count, claim), so its worst case is 3 x this, still under the drain bound. A
 * timeout is a failed claim: rolled back, and the worker backs off. Once `drain()` set the claim's
 * deadline (`ClaimRunsInput.remainingMs`), the connection checkout, BEGIN and COMMIT are bounded
 * too: past the deadline nothing is claimed (no BEGIN after a slow checkout, ROLLBACK instead of
 * COMMIT), and each statement's timeout is capped by the time left.
 */
export const CLAIM_STATEMENT_TIMEOUT_MS = 1_500;
/**
 * Shortest statement timeout a claim runs with once `stop()` set its deadline: with less time left
 * than this, the claim gives up (claims nothing) instead of racing the drain bound.
 */
export const MIN_CLAIM_STATEMENT_TIMEOUT_MS = 50;

/** Internal: the claim's deadline passed; rolled back, nothing claimed. */
class ClaimDeadlinePassed extends Error {}

/**
 * The statement timeout for the next claim statement given the time left (`undefined` = no
 * deadline), or `undefined` when the deadline is (about to be) over. Always a safe integer.
 */
function statementTimeoutMs(remaining: number | undefined): number | undefined {
	if (remaining === undefined) return CLAIM_STATEMENT_TIMEOUT_MS;
	if (!Number.isFinite(remaining) || remaining < MIN_CLAIM_STATEMENT_TIMEOUT_MS) return undefined;
	return Math.min(CLAIM_STATEMENT_TIMEOUT_MS, Math.floor(remaining));
}

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
		// `statement_timestamp()`, not `now()`, for due-ness AND the written times: the claim runs in
		// a transaction (statement_timeout, advisory lock), and `now()` is frozen at its BEGIN, so a
		// run that came due between the BEGIN and this statement would be skipped, and one inserted
		// or re-queued in that window would get an `updated_at` older than the one it already had.
		// The pause check compares with `$1`, never `r.queue`: uncorrelated, it is a one-time
		// InitPlan filter and the scan walks `idx_runs_claim` in order up to the LIMIT; correlated,
		// the planner picks an anti join and sorts the whole due backlog first (review 5, A1).
		const { rows } = await db.query<RunRow>(
			`WITH picked AS (
			   SELECT r.id FROM ${runs} r
			   WHERE r.queue = $1
			     AND r.status IN ('queued', 'waiting')
			     AND r.run_after <= statement_timestamp()
			     AND NOT r.outbox_pending
			     AND NOT EXISTS (SELECT 1 FROM ${queueState} q WHERE q.queue = $1 AND q.paused)
			   ORDER BY r.run_after, r.created_at, r.id
			   LIMIT $2::int
			   FOR UPDATE OF r SKIP LOCKED
			 )
			 UPDATE ${runs} r
			 SET status = 'running', execution_id = gen_random_uuid(),
			     heartbeat_at = statement_timestamp(),
			     started_at = CASE WHEN r.concurrency_key IS NULL
			                       THEN coalesce(r.started_at, statement_timestamp())
			                       ELSE r.started_at END,
			     debounce_key = NULL, updated_at = statement_timestamp()
			 FROM picked WHERE r.id = picked.id
			 RETURNING ${returning}`,
			[queue, limit]
		);
		// UPDATE ... RETURNING does not guarantee order: re-sort like the SELECT.
		return rows.map(mapRun).sort(byRunAfterCreatedId);
	}

	return {
		async claimRuns({
			queue,
			limit,
			globalConcurrency,
			remainingMs,
			onGlobalLimit,
		}: ClaimRunsInput): Promise<RunRecord[]> {
			if (limit <= 0) return [];
			if (exec.kind === "tx") {
				throw new MinitriggerError(
					"claimRuns must not run inside a StoreTx (SET LOCAL would leak)"
				);
			}
			const { client, release } = await checkOut(exec.pool);
			const db = wrap(client);
			/** The statement timeout in force in this transaction (none before the first SET). */
			let applied: number | undefined;
			/**
			 * Before each statement: past the deadline, nothing is claimed; else the statement is
			 * bounded by `min(CLAIM_STATEMENT_TIMEOUT_MS, time left)`. Without a deadline this is the
			 * single `SET LOCAL` of the transaction (no extra round trip in normal operation).
			 */
			const bound = async (): Promise<void> => {
				const ms = statementTimeoutMs(remainingMs?.());
				if (ms === undefined) throw new ClaimDeadlinePassed();
				if (applied !== undefined && applied <= ms) return;
				// SET cannot take bind parameters; `ms` is a validated integer. Transaction-scoped
				// only (never a session SET on a pooled client).
				await db.query(`SET LOCAL statement_timeout = ${ms}`);
				applied = ms;
			};
			let begun = false;
			try {
				// A slow checkout (a saturated pool) may have outlived the deadline: no BEGIN at all.
				if (statementTimeoutMs(remainingMs?.()) === undefined) throw new ClaimDeadlinePassed();
				await client.query("BEGIN");
				begun = true;
				let claimed: RunRecord[];
				let atGlobalCap = false;
				await bound();
				if (globalConcurrency === undefined) {
					claimed = await claim(db, queue, limit);
				} else {
					// Decision 28(e): serializes the queue's claims; the count below sees every
					// committed `running` (READ COMMITTED: fresh snapshot per statement). One bigint key,
					// outside `(hashtext(schema), 1|2)` (decision 16). Seed 1, not 0: the decision 25
					// lock hashes `schema:queue:<user key>` with seed 0, so a user `concurrencyKey` of
					// "global" would otherwise produce this very key; with its own seed no user key can
					// (short of a 64-bit hash collision).
					await db.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 1))", [
						`${ctx.schema}:${queue}:global`,
					]);
					await bound();
					const { rows } = await db.query<{ n: number }>(
						`SELECT count(*)::int AS n FROM ${runs} WHERE queue = $1 AND status = 'running'`,
						[queue]
					);
					const globalFree = globalConcurrency - (rows[0]?.n ?? 0);
					const free = Math.min(limit, globalFree);
					if (free > 0) await bound();
					claimed = free > 0 ? await claim(db, queue, free) : [];
					// The global cap, not the due backlog, bounded this claim: at the cap after COMMIT.
					atGlobalCap = globalFree < limit && claimed.length >= Math.max(0, globalFree);
				}
				// Never COMMIT past the deadline: `stop()` stopped waiting, nobody would run or hand
				// back what this brings.
				if (claimed.length > 0 && statementTimeoutMs(remainingMs?.()) === undefined) {
					throw new ClaimDeadlinePassed();
				}
				await client.query("COMMIT");
				release();
				if (atGlobalCap) onGlobalLimit?.();
				return claimed;
			} catch (err) {
				const rolledBack = begun
					? await client.query("ROLLBACK").then(
							() => true,
							() => false
						)
					: true;
				if (err instanceof ClaimDeadlinePassed) {
					if (rolledBack) release();
					else release(new Error("ROLLBACK failed"));
					return [];
				}
				release(err instanceof Error ? err : new Error(String(err)));
				throw err;
			}
		},

		async nextDueInMs(queue: string): Promise<number | null> {
			// The delay is computed in SQL (decision 17): the app clock may drift from the DB's.
			const { rows } = await direct(exec).query<{ ms: number | null }>(
				// A paused queue has nothing to wake up for (resume notifies); uncorrelated like the
				// claim's pause check (review 5, A1/A2).
				`SELECT ${DELAY_UNTIL_MIN_RUN_AFTER} AS ms FROM ${runs}
				 WHERE queue = $1 AND status IN ('queued', 'waiting') AND run_after > now()
				   AND NOT EXISTS (SELECT 1 FROM ${queueState} q WHERE q.queue = $1 AND q.paused)`,
				[queue]
			);
			return rows[0]?.ms ?? null;
		},
	};
}
