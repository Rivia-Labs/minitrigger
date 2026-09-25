/**
 * Shared internal SQL used by runs.ts and waitpoints.ts while the relevant run rows are already
 * locked `FOR UPDATE` (decisions 3 and 10; lock order in docs/store-contract.md):
 * parent run -> run -> descendants (level by level, ascending id) -> waitpoint rows.
 * Nothing here opens a transaction; callers run it inside `atomic()`.
 */
import type {
	CanceledRun,
	EnqueueRequest,
	Json,
	RunRecord,
	RunStatus,
	SerializedError,
	WaitpointResolution,
} from "../types.js";
import { CANCELED_ERROR_NAME } from "../types.js";
import type { Db, SqlContext } from "./db.js";

/** `delayMs` for BullMQ, computed by the database from `run_after` (decision 17). */
export const DELAY_MS_SQL =
	"greatest(0, round(extract(epoch from (run_after - now())) * 1000))::float8";

/** SQL expression `now() + $n ms`; `$n` must be bound as a number. */
export function nowPlusMs(param: string): string {
	return `now() + ${param}::float8 * interval '1 millisecond'`;
}

/** jsonb parameters are always sent as JSON text (pg would turn JS arrays into PG arrays). */
export function jsonParam(value: Json | SerializedError | null): string {
	return JSON.stringify(value ?? null);
}

export const TERMINAL_SQL = "('completed','failed','canceled')";

export function isTerminal(status: RunStatus): boolean {
	return status === "completed" || status === "failed" || status === "canceled";
}

export function canceledError(reason: string): SerializedError {
	return { name: CANCELED_ERROR_NAME, message: reason, retryable: false };
}

// ---------------------------------------------------------------------------
// Run rows
// ---------------------------------------------------------------------------

export const RUN_COLUMNS =
	"id, task_id, queue, payload, status, output, error, attempt, max_attempts, execution_id, " +
	"enqueue_seq, heartbeat_at, run_after, parent_run_id, parent_waitpoint_id, parent_step_name, " +
	"batch_index, started_at, finished_at, created_at, updated_at";

export interface RunRow {
	id: string;
	task_id: string;
	queue: string;
	payload: Json;
	status: RunStatus;
	output: Json | null;
	error: SerializedError | null;
	attempt: number;
	max_attempts: number;
	execution_id: string | null;
	enqueue_seq: number;
	heartbeat_at: Date | null;
	run_after: Date | null;
	parent_run_id: string | null;
	parent_waitpoint_id: string | null;
	parent_step_name: string | null;
	batch_index: number | null;
	started_at: Date | null;
	finished_at: Date | null;
	created_at: Date;
	updated_at: Date;
}

export function mapRun(row: RunRow): RunRecord {
	return {
		id: row.id,
		taskId: row.task_id,
		queue: row.queue,
		payload: row.payload,
		status: row.status,
		output: row.output,
		error: row.error,
		attempt: row.attempt,
		maxAttempts: row.max_attempts,
		executionId: row.execution_id,
		enqueueSeq: row.enqueue_seq,
		heartbeatAt: row.heartbeat_at,
		runAfter: row.run_after,
		parentRunId: row.parent_run_id,
		parentWaitpointId: row.parent_waitpoint_id,
		parentStepName: row.parent_step_name,
		batchIndex: row.batch_index,
		startedAt: row.started_at,
		finishedAt: row.finished_at,
		createdAt: row.created_at,
		updatedAt: row.updated_at,
	};
}

export interface EnqueueRow {
	id: string;
	task_id: string;
	queue: string;
	enqueue_seq: number;
	delay_ms: number;
	status: RunStatus;
	updated_at: Date;
}

/** RETURNING list producing an {@link EnqueueRow}. */
export const ENQUEUE_RETURNING = `id, task_id, queue, enqueue_seq, status, updated_at, ${DELAY_MS_SQL} AS delay_ms`;

export function mapEnqueue(row: EnqueueRow): EnqueueRequest {
	return {
		runId: row.id,
		taskId: row.task_id,
		queue: row.queue,
		seq: row.enqueue_seq,
		delayMs: row.delay_ms,
		status: row.status,
		updatedAt: row.updated_at,
	};
}

// ---------------------------------------------------------------------------
// Locking helpers
// ---------------------------------------------------------------------------

/**
 * First step of the lock order for a run that may resolve its parent's waitpoint: read
 * `parent_run_id`/`parent_waitpoint_id` WITHOUT a lock and, when the run waits-resolves a parent,
 * lock that parent `FOR UPDATE`. Returns null when the run does not exist. Both columns only ever
 * change to NULL (ON DELETE SET NULL), so the caller's later re-read under the run lock sees a
 * subset of what was locked here.
 */
export async function lockParentOf(
	db: Db,
	ctx: SqlContext,
	runId: string
): Promise<{ parentRunId: string | null; parentWaitpointId: string | null } | null> {
	const { rows } = await db.query<{
		parent_run_id: string | null;
		parent_waitpoint_id: string | null;
	}>(`SELECT parent_run_id, parent_waitpoint_id FROM ${ctx.t("runs")} WHERE id = $1`, [runId]);
	const row = rows[0];
	if (!row) return null;
	if (row.parent_waitpoint_id !== null && row.parent_run_id !== null) {
		await db.query(`SELECT 1 FROM ${ctx.t("runs")} WHERE id = $1 FOR UPDATE`, [row.parent_run_id]);
	}
	return { parentRunId: row.parent_run_id, parentWaitpointId: row.parent_waitpoint_id };
}

// ---------------------------------------------------------------------------
// Resume rule (decision 3)
// ---------------------------------------------------------------------------

/**
 * SQL predicate: the run `runIdExpr` has a `pending` waitpoint that BLOCKS its resume. Owner's
 * interpretation of decision 3: only waitpoints the execution is actually awaiting block. A
 * `token` counts only once its `${step_name}#wait` step (kind `wait`) exists, i.e. after
 * `waitForToken` ran for it; a token created and not (yet) awaited never keeps a sleeping run
 * from waking up. `child_run`/`batch` waitpoints always count (created with their waiting step).
 * Used by `runs.suspend`, {@link applyResumeRule} and the reconciler's cases 2 and 3.
 */
export function blockingWaitpointExistsSql(ctx: SqlContext, runIdExpr: string): string {
	return `EXISTS (SELECT 1 FROM ${ctx.t("waitpoints")} w
	         WHERE w.run_id = ${runIdExpr} AND w.status = 'pending'
	           AND (w.kind <> 'token' OR EXISTS (
	             SELECT 1 FROM ${ctx.t("steps")} ws
	             WHERE ws.run_id = w.run_id AND ws.name = w.step_name || '#wait'
	               AND ws.kind = 'wait')))`;
}

/**
 * With the run already locked `FOR UPDATE`: if it is `waiting` and has no blocking pending
 * waitpoint ({@link blockingWaitpointExistsSql}),
 * bump `enqueue_seq`, set `run_after = greatest(max(wake_at of waiting sleeps), now())` and return
 * the enqueue request. Otherwise null (a `running` run is never enqueued here).
 */
export async function applyResumeRule(
	db: Db,
	ctx: SqlContext,
	runId: string
): Promise<EnqueueRequest | null> {
	const { rows } = await db.query<EnqueueRow>(
		`UPDATE ${ctx.t("runs")} r
		 SET enqueue_seq = r.enqueue_seq + 1,
		     run_after = greatest(
		       (SELECT max(s.wake_at) FROM ${ctx.t("steps")} s
		         WHERE s.run_id = r.id AND s.status = 'waiting' AND s.wake_at IS NOT NULL),
		       now()),
		     updated_at = now()
		 WHERE r.id = $1 AND r.status = 'waiting'
		   AND NOT ${blockingWaitpointExistsSql(ctx, "r.id")}
		 RETURNING ${ENQUEUE_RETURNING}`,
		[runId]
	);
	const row = rows[0];
	return row ? mapEnqueue(row) : null;
}

// ---------------------------------------------------------------------------
// Child resolution (decision 10)
// ---------------------------------------------------------------------------

/**
 * A child with `parent_waitpoint_id` reached a terminal state; the parent run is already locked.
 * `child_run` -> completed; `batch` -> decrement, completed at 0. Already `timed_out`/`completed`
 * -> null (no-op). Child outputs are never copied: the parent reads them from `runs`.
 */
export async function resolveChildWaitpoint(
	db: Db,
	ctx: SqlContext,
	waitpointId: string
): Promise<WaitpointResolution | null> {
	const { rows } = await db.query<{ run_id: string; status: string; has_expiry: boolean }>(
		`UPDATE ${ctx.t("waitpoints")}
		 SET pending_count = CASE WHEN kind = 'batch' THEN pending_count - 1 ELSE pending_count END,
		     status = CASE WHEN kind = 'batch' AND pending_count - 1 > 0 THEN 'pending' ELSE 'completed' END,
		     resolved_at = CASE WHEN kind = 'batch' AND pending_count - 1 > 0 THEN NULL ELSE now() END
		 WHERE id = $1 AND status = 'pending' AND kind IN ('child_run', 'batch')
		 RETURNING run_id, status, expires_at IS NOT NULL AS has_expiry`,
		[waitpointId]
	);
	const row = rows[0];
	if (!row) return null;
	if (row.status !== "completed") {
		return { waitpointId, hasExpireJob: false, resume: null };
	}
	const resume = await applyResumeRule(db, ctx, row.run_id);
	return { waitpointId, hasExpireJob: row.has_expiry, resume };
}

// ---------------------------------------------------------------------------
// Recursive cancel walk
// ---------------------------------------------------------------------------

interface LockedChildRow {
	id: string;
	task_id: string;
	status: RunStatus;
	queue: string;
	enqueue_seq: number;
	attempt: number;
}

/**
 * Sets the given (already locked, non-terminal) runs to `canceled`. Returns each canceled run's
 * new `updated_at` (Phase 7 perf: so callers can fill {@link CanceledRun.updatedAt} without a
 * separate `runs.get`).
 */
export async function markCanceled(
	db: Db,
	ctx: SqlContext,
	ids: readonly string[],
	reason: string
): Promise<Map<string, Date>> {
	if (ids.length === 0) return new Map();
	const { rows } = await db.query<{ id: string; updated_at: Date }>(
		`UPDATE ${ctx.t("runs")}
		 SET status = 'canceled', execution_id = NULL, run_after = NULL, finished_at = now(),
		     error = $2::jsonb, updated_at = now()
		 WHERE id = ANY($1::uuid[]) AND status NOT IN ${TERMINAL_SQL}
		 RETURNING id, updated_at`,
		[ids, jsonParam(canceledError(reason))]
	);
	return new Map(rows.map(r => [r.id, r.updated_at]));
}

/**
 * Locks and cancels descendants level by level (ORDER BY id FOR UPDATE per level), walking
 * through terminal nodes too. The first level is either the children of `rootIds`
 * (`parent_run_id`) or the children attached to a waitpoint (`parent_waitpoint_id`). Parent
 * waitpoint resolution is NOT done for these runs: their parents are being canceled/expired.
 */
export async function cancelDescendants(
	db: Db,
	ctx: SqlContext,
	start: { parentRunIds: readonly string[] } | { waitpointId: string },
	reason: string
): Promise<CanceledRun[]> {
	const canceled: CanceledRun[] = [];
	const error = canceledError(reason);
	const visited = new Set<string>();
	let level: LockedChildRow[];
	if ("waitpointId" in start) {
		level = (
			await db.query<LockedChildRow>(
				`SELECT id, task_id, status, queue, enqueue_seq, attempt FROM ${ctx.t("runs")}
				 WHERE parent_waitpoint_id = $1 ORDER BY id FOR UPDATE`,
				[start.waitpointId]
			)
		).rows;
	} else {
		start.parentRunIds.forEach(id => visited.add(id));
		level = await lockChildLevel(db, ctx, start.parentRunIds);
	}
	while (level.length > 0) {
		const fresh = level.filter(row => !visited.has(row.id));
		for (const row of fresh) visited.add(row.id);
		const toCancel = fresh.filter(row => !isTerminal(row.status));
		const updatedAt = await markCanceled(
			db,
			ctx,
			toCancel.map(row => row.id),
			reason
		);
		for (const row of toCancel) {
			const at = updatedAt.get(row.id);
			if (!at) throw new Error(`cancelDescendants: run ${row.id} was not updated`);
			canceled.push({
				runId: row.id,
				taskId: row.task_id,
				queue: row.queue,
				enqueueSeq: row.enqueue_seq,
				previousStatus: row.status as CanceledRun["previousStatus"],
				attempt: row.attempt,
				error,
				updatedAt: at,
			});
		}
		level = await lockChildLevel(
			db,
			ctx,
			fresh.map(row => row.id)
		);
	}
	return canceled;
}

/**
 * Times out every `pending` waitpoint owned by the given (already locked, just canceled) runs,
 * with `resolved_at = now()`. Runs last in the lock order (waitpoint rows); nobody else can touch
 * these rows meanwhile because every resolver locks the owning run first. Returns the ids of the
 * ones that had `expires_at`, i.e. whose `wp-${id}-expire` job the caller removes after commit.
 * No resume rule: the owning runs are terminal.
 */
export async function timeOutWaitpointsOf(
	db: Db,
	ctx: SqlContext,
	runIds: readonly string[]
): Promise<string[]> {
	if (runIds.length === 0) return [];
	const { rows } = await db.query<{ id: string; has_expiry: boolean }>(
		`UPDATE ${ctx.t("waitpoints")}
		 SET status = 'timed_out', resolved_at = now()
		 WHERE run_id = ANY($1::uuid[]) AND status = 'pending'
		 RETURNING id, expires_at IS NOT NULL AS has_expiry`,
		[runIds]
	);
	return rows
		.filter(r => r.has_expiry)
		.map(r => r.id)
		.sort();
}

async function lockChildLevel(
	db: Db,
	ctx: SqlContext,
	parentIds: readonly string[]
): Promise<LockedChildRow[]> {
	if (parentIds.length === 0) return [];
	const { rows } = await db.query<LockedChildRow>(
		`SELECT id, task_id, status, queue, enqueue_seq, attempt FROM ${ctx.t("runs")}
		 WHERE parent_run_id = ANY($1::uuid[]) ORDER BY id FOR UPDATE`,
		[parentIds]
	);
	return rows;
}
