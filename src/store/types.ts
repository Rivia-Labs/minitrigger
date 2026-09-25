/**
 * Persistence contract (Phase 2, task 2.0). The executor, engine, reconciler,
 * retention and the Phase 9 in-memory store depend ONLY on these interfaces.
 *
 * Global rules every implementation MUST honour (see docs/store-contract.md):
 *
 * - Every method is atomic. On a pool-bound {@link Store} a multi-statement
 *   method opens its own transaction (one pg client, BEGIN/COMMIT/ROLLBACK);
 *   on a {@link StoreTx} it runs inside the caller's transaction, using a
 *   SAVEPOINT only where partial rollback is needed ({@link RunStore.create}).
 * - Isolation is READ COMMITTED; correctness comes from row locks.
 * - Lock order (deadlock avoidance): parent run -> run -> descendants (level by
 *   level, ascending id) -> waitpoint rows. A method that needs a run's parent
 *   reads `parent_run_id` without a lock, locks the parent `FOR UPDATE`, then
 *   locks/updates the run and re-validates.
 * - Every timestamp written or compared (`now()`, `wake_at`, `run_after`,
 *   `expires_at`, `heartbeat_at`, `finished_at`) comes from the database
 *   clock, and every `delayMs` returned is computed by the database
 *   (`greatest(0, run_after - now())`) (decision 17). The in-memory store uses
 *   a single injected clock for the same purpose.
 * - "Ownership guard" = the run row has `execution_id = executionId AND
 *   status = 'running'`, checked with `FOR SHARE` (or by the UPDATE itself on
 *   the run row) (decision 5). A failed guard is reported as `{ kind: "lost" }`,
 *   never as an exception.
 * - Every transition out of `running` sets `execution_id = NULL` (decision 5)
 *   and every write to `runs` other than {@link RunStore.heartbeat} sets
 *   `updated_at = now()`.
 * - Resuming a `waiting` run ("resume rule", decision 3): with the run locked
 *   `FOR UPDATE`, only if `status = 'waiting'` and it has no `pending`
 *   waitpoint: `enqueue_seq + 1`, `run_after = greatest(max(wake_at of its
 *   waiting sleep steps), now())`, status stays `waiting`; return an
 *   {@link EnqueueRequest}. Otherwise return `null`.
 */

import type { SerializedError } from "../utils/error-format.js";
import type { Json } from "../utils/serialization.js";

export type { SerializedError } from "../utils/error-format.js";
export type { Json } from "../utils/serialization.js";

// ---------------------------------------------------------------------------
// Constants and enums
// ---------------------------------------------------------------------------

export type RunStatus = "queued" | "running" | "waiting" | "completed" | "failed" | "canceled";
export type TerminalRunStatus = "completed" | "failed" | "canceled";
export const TERMINAL_RUN_STATUSES: readonly TerminalRunStatus[] = [
	"completed",
	"failed",
	"canceled",
];

export type StepStatus = "completed" | "waiting";
export type WaitpointKind = "token" | "child_run" | "batch";
export type WaitpointStatus = "pending" | "completed" | "timed_out";
export type LogLevel = "debug" | "info" | "warn" | "error";

/** Hard cap for {@link RunStore.list} (Phase 2). */
export const MAX_LIST_LIMIT = 100;
/** Default `$2` of the Phase 4 step window query. */
export const DEFAULT_MAX_INLINE_STEP_OUTPUT_BYTES = 16 * 1024 * 1024;
/** `runs.error.name` written by cancellation (decision 10). */
export const CANCELED_ERROR_NAME = "Canceled";

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/** One row of `runs`. */
export interface RunRecord {
	id: string;
	taskId: string;
	/** User queue name (unprefixed). */
	queue: string;
	payload: Json;
	status: RunStatus;
	/** Set only when `completed`. */
	output: Json | null;
	/** Last attempt's error (kept while retrying), or the final error / cancel reason. */
	error: SerializedError | null;
	/** 1-based; inserted as 1. Incremented only on failed attempt or recovered stall (decision 4). */
	attempt: number;
	maxAttempts: number;
	executionId: string | null;
	/** Inserted as 1; the seq of the first enqueue. jobId = `${id}-${enqueueSeq}`. */
	enqueueSeq: number;
	heartbeatAt: Date | null;
	/** Next moment the run should execute; NULL while waiting on waitpoints or when terminal. */
	runAfter: Date | null;
	parentRunId: string | null;
	parentWaitpointId: string | null;
	parentStepName: string | null;
	batchIndex: number | null;
	startedAt: Date | null;
	finishedAt: Date | null;
	createdAt: Date;
	updatedAt: Date;
}

/** One row of `steps` (PK `(run_id, name)`). */
export interface StepRecord {
	runId: string;
	name: string;
	seqIndex: number;
	status: StepStatus;
	/**
	 * `step.run`: the memoized output. `createToken`: `{ id }`. `step.trigger`:
	 * `{ id: childRunId }`. Waitpoint-backed steps (`triggerAndWait`,
	 * `batchTriggerAndWait`, `${name}#wait`): `{ waitpointId }` only (decision 10).
	 * Sleeps: null.
	 */
	output: Json | null;
	/** Only for sleep steps. */
	wakeAt: Date | null;
	createdAt: Date;
}

/** One row of `waitpoints` (unique `(run_id, step_name)`). `runId` is the WAITING run. */
export interface WaitpointRecord {
	id: string;
	runId: string;
	/** Token: the `createToken` step name (not `#wait`). Child/batch: the step name. */
	stepName: string;
	kind: WaitpointKind;
	status: WaitpointStatus;
	/** `batch` only: children not yet terminal. NULL for `token` and `child_run`. */
	pendingCount: number | null;
	/** `token` only: the completion value. Child results live in `runs` (decision 10). */
	output: Json | null;
	expiresAt: Date | null;
	createdAt: Date;
}

/** One row of `run_logs`. */
export interface LogEntry {
	/** bigserial; pg returns int8 as string. Monotonic per insert order. */
	id: string;
	runId: string;
	level: LogLevel;
	message: string;
	data: Json | null;
	ts: Date;
}

// ---------------------------------------------------------------------------
// Shared result types
// ---------------------------------------------------------------------------

/** Ownership lost: canceled, timed out, stall recovered, released (decision 5). */
export interface Lost {
	kind: "lost";
}

/** Identifies the execution performing a guarded write. */
export interface Ownership {
	runId: string;
	executionId: string;
}

/** What the caller must enqueue after commit: jobId `${runId}-${seq}` on `queue`. */
export interface EnqueueRequest {
	runId: string;
	taskId: string;
	queue: string;
	seq: number;
	/** Computed by the DB from `run_after - now()`, clamped to >= 0. */
	delayMs: number;
}

/**
 * Outcome of resolving (completing, decrementing or expiring) a waitpoint.
 * After commit the caller enqueues `resume` and, if `hasExpireJob`, removes
 * the `-system` job `wp-${waitpointId}-expire` best-effort (decision 10).
 */
export interface WaitpointResolution {
	waitpointId: string;
	/** True when this call moved the waitpoint out of `pending` and it had `expires_at`. */
	hasExpireJob: boolean;
	resume: EnqueueRequest | null;
}

/** A run moved to `canceled`; caller removes job `${runId}-${enqueueSeq}` and publishes an abort. */
export interface CanceledRun {
	runId: string;
	queue: string;
	enqueueSeq: number;
	previousStatus: "queued" | "running" | "waiting";
}

// ---------------------------------------------------------------------------
// RunStore
// ---------------------------------------------------------------------------

export interface NewRun {
	/** Generated by the application (decision 9). */
	id: string;
	taskId: string;
	queue: string;
	payload: Json;
	maxAttempts: number;
	/** `run_after = now() + delayMs`; 0 for immediate. */
	delayMs: number;
	/** `ttlMs: null` => `expires_at NULL` (internal keys). User keys get the engine default TTL. */
	idempotency?: { key: string; ttlMs: number | null };
}

export type CreateRunResult =
	| { kind: "created"; run: RunRecord; enqueue: EnqueueRequest }
	| { kind: "existing"; run: RunRecord };

export interface NewChildRun {
	id: string;
	taskId: string;
	queue: string;
	payload: Json;
	maxAttempts: number;
	/** `__child:${parentRunId}:${stepName}` or `...:${batchIndex}`; stored with `expires_at NULL`. */
	idempotencyKey: string;
	/** Required for `mode: "batch"`, null otherwise. */
	batchIndex: number | null;
}

export interface TriggerChildrenInput {
	parent: Ownership;
	stepName: string;
	seqIndex: number;
	/**
	 * - `trigger`: exactly one child, no waitpoint; step `completed` with `{ id }`.
	 * - `wait`: exactly one child + waitpoint `child_run`; step `waiting` with `{ waitpointId }`.
	 * - `batch`: >= 1 children + waitpoint `batch` with `pending_count = children.length`.
	 */
	mode: "trigger" | "wait" | "batch";
	/** Generated by the application; ignored for `mode: "trigger"`. */
	waitpointId: string;
	/** `expires_at = now() + timeoutMs`; null = no timeout. Ignored for `trigger`. */
	timeoutMs: number | null;
	children: readonly NewChildRun[];
}

export type TriggerChildrenResult =
	| {
			kind: "created";
			/** One per child, in input order (child runs are `queued`, `run_after = now()`). */
			enqueue: EnqueueRequest[];
			/** Null for `mode: "trigger"`. `expireDelayMs` is null when there is no timeout. */
			waitpoint: { id: string; expiresAt: Date | null; expireDelayMs: number | null } | null;
	  }
	| Lost;

export type SuspendResult =
	| { kind: "waiting_on_waitpoints" }
	| { kind: "sleeping"; enqueue: EnqueueRequest }
	| { kind: "requeued"; enqueue: EnqueueRequest }
	| Lost;

export type CompleteRunResult = { kind: "completed"; parent: WaitpointResolution | null } | Lost;
export type FailRunResult = { kind: "failed"; parent: WaitpointResolution | null } | Lost;
export type RetryRunResult = { kind: "retrying"; attempt: number; enqueue: EnqueueRequest } | Lost;
export type ReleaseRunResult = { kind: "released"; enqueue: EnqueueRequest } | Lost;

export type CancelRunResult =
	| { kind: "not_found" }
	| { kind: "already_finished"; status: TerminalRunStatus }
	| {
			kind: "canceled";
			/** The root first, then descendants. */
			canceled: CanceledRun[];
			/** Resolution of the ROOT's parent waitpoint, if any. */
			parent: WaitpointResolution | null;
			/**
			 * Ids of the canceled runs' own waitpoints that were `pending` with `expires_at` and
			 * are now `timed_out`: the caller removes `wp-${id}-expire` best-effort after commit.
			 */
			expireJobsToRemove: string[];
	  };

export type RecoverStalledResult =
	| { kind: "not_stalled" }
	| { kind: "requeued"; attempt: number; enqueue: EnqueueRequest }
	| { kind: "failed"; parent: WaitpointResolution | null };

export interface ChildResult {
	runId: string;
	batchIndex: number | null;
	status: RunStatus;
	output: Json | null;
	error: SerializedError | null;
}

export interface RunCursor {
	createdAt: Date;
	id: string;
}

export interface ListRunsFilter {
	taskId?: string;
	status?: RunStatus | readonly RunStatus[];
	/** Inclusive lower bound on `created_at`. */
	createdFrom?: Date;
	/** Exclusive upper bound on `created_at`. */
	createdTo?: Date;
	/** Exclusive: rows strictly after this cursor in `(created_at desc, id desc)` order. */
	cursor?: RunCursor;
	/** 1..{@link MAX_LIST_LIMIT}; larger values are clamped. */
	limit: number;
}

export interface ListRunsResult {
	items: RunRecord[];
	/** Cursor of the last item when `items.length === limit`, else null. */
	nextCursor: RunCursor | null;
}

export interface RunStore {
	/**
	 * Trigger (decision 9; also used by `runs.replay()` without `idempotency`,
	 * and by schedule ticks). Inserts the run (`queued`, attempt 1, enqueue_seq 1,
	 * `run_after = now() + delayMs`, no parent), then claims the key via
	 * {@link IdempotencyStore.claim}. Not claimed => roll back the insert
	 * (ROLLBACK, or ROLLBACK TO SAVEPOINT inside a StoreTx) and return the run that
	 * owns the key, enqueuing nothing. If that run vanished meanwhile (retention),
	 * retry the whole operation once.
	 */
	create(input: NewRun): Promise<CreateRunResult>;

	/**
	 * `step.trigger` / `triggerAndWait` / `batchTriggerAndWait` in ONE transaction
	 * (decision 10), before any enqueue: ownership guard on the parent (FOR SHARE);
	 * insert waitpoint (wait/batch); insert each child (`queued`, `run_after = now()`,
	 * `parent_run_id`, `parent_waitpoint_id`, `parent_step_name`, `batch_index`) with
	 * its internal key (`expires_at NULL`); insert the step. A key or step conflict is
	 * an invariant violation: throw and roll back everything.
	 */
	triggerChildren(input: TriggerChildrenInput): Promise<TriggerChildrenResult>;

	/**
	 * Acquisition, exactly decision 5's statement:
	 * `UPDATE runs SET status='running', execution_id=$2, heartbeat_at=now(),
	 * started_at=coalesce(started_at, now()) WHERE id=$1 AND status IN
	 * ('queued','waiting') AND enqueue_seq=$3 RETURNING *`. Null => stale job, no-op.
	 */
	acquire(runId: string, executionId: string, seq: number): Promise<RunRecord | null>;

	/**
	 * Decision 6. Own short transaction: `SET LOCAL statement_timeout = timeoutMs`,
	 * then `UPDATE runs SET heartbeat_at = now() WHERE id AND execution_id AND
	 * status='running'` touching ONLY `heartbeat_at` (HOT; no `updated_at`).
	 * Never on a StoreTx (not exposed there). DB errors/timeouts are thrown.
	 */
	heartbeat(runId: string, executionId: string, timeoutMs: number): Promise<"ok" | "lost">;

	/**
	 * Final success. Guarded update to `completed`, `output`, `finished_at = now()`,
	 * `run_after = NULL`, `execution_id = NULL`. If the run has `parent_waitpoint_id`,
	 * resolves it in the same transaction (see "Child resolution" in the doc) —
	 * lock parent first.
	 */
	complete(owner: Ownership, output: Json): Promise<CompleteRunResult>;

	/**
	 * Definitive failure (attempts exhausted, AbortTaskRunError, non-retryable). As {@link complete}.
	 * Like completion, it does NOT cancel descendants: children started with `step.trigger` may
	 * outlive their parent (only {@link cancel} and waitpoint expiry cancel children).
	 */
	fail(owner: Ownership, error: SerializedError): Promise<FailRunResult>;

	/**
	 * Failed attempt with retries left (decision 4). Guarded update: `queued`,
	 * `attempt + 1`, `error`, `execution_id = NULL`, `run_after = now() + backoffMs`,
	 * `enqueue_seq + 1`. `backoffMs` comes from `computeBackoffMs(opts, run.attempt)`.
	 */
	retry(owner: Ownership, error: SerializedError, backoffMs: number): Promise<RetryRunResult>;

	/**
	 * Graceful-stop hand-back (decision 20). Guarded update: `queued`,
	 * `execution_id = NULL`, `run_after = now()`, `enqueue_seq + 1`, attempt unchanged.
	 */
	release(owner: Ownership): Promise<ReleaseRunResult>;

	/**
	 * Suspension transaction (decision 3), after `Promise.allSettled` of in-flight
	 * steps. `SELECT ... FOR UPDATE` on the run with ownership, then re-check:
	 * - a `pending` waitpoint exists => `waiting`, `run_after = NULL`, `enqueue_seq + 1`
	 *   (so a stale redelivery of the current job is rejected by {@link acquire}), no enqueue;
	 * - else a waiting sleep step with `wake_at > now()` => `waiting`,
	 *   `enqueue_seq + 1`, `run_after = max(wake_at)`, enqueue with that delay;
	 * - else => `queued`, `enqueue_seq + 1`, `run_after = now()`, delay 0.
	 * Always `execution_id = NULL`; attempt unchanged.
	 */
	suspend(owner: Ownership): Promise<SuspendResult>;

	/**
	 * Recursive cancel (decisions 5, 10; Phase 6). Lock the root's parent (if any),
	 * then the root, then non-terminal descendants level by level (`parent_run_id =
	 * ANY(...)` ORDER BY id FOR UPDATE). Each non-terminal one becomes `canceled`,
	 * `execution_id = NULL`, `run_after = NULL`, `finished_at = now()`,
	 * `error = { name: "Canceled", message, retryable: false }`. Only the root's
	 * parent waitpoint is resolved. Descendants of terminal runs are still visited.
	 * Then (waitpoint rows last in the lock order) every `pending` waitpoint owned by a canceled
	 * run becomes `timed_out`, `resolved_at = now()`; those with `expires_at` are reported in
	 * `expireJobsToRemove`. A later {@link WaitpointStore.completeToken} on them is `not_pending`.
	 */
	cancel(runId: string, reason: string): Promise<CancelRunResult>;

	get(runId: string): Promise<RunRecord | null>;

	/** Filters + composite cursor `(created_at, id)` desc, index `runs(task_id, created_at desc, id desc)`. */
	list(filter: ListRunsFilter): Promise<ListRunsResult>;

	/**
	 * Replay of `triggerAndWait`/`batchTriggerAndWait`: one query
	 * `WHERE parent_waitpoint_id = $1 ORDER BY batch_index` (decision 10).
	 */
	listChildResults(waitpointId: string): Promise<ChildResult[]>;

	// --- Reconciler (Phase 6). `graceMs`/timeouts are compared against DB now(). ---

	/** Case 1: `queued` AND `run_after < now() - grace` => re-enqueue with the CURRENT seq (read-only). */
	listStaleQueued(graceMs: number, limit: number): Promise<EnqueueRequest[]>;

	/** Case 2: `waiting`, no pending waitpoint, `run_after < now() - grace` => SAME seq (read-only). */
	listStaleWaiting(graceMs: number, limit: number): Promise<EnqueueRequest[]>;

	/**
	 * Case 3: `waiting`, `run_after IS NULL`, no pending waitpoint. Locks candidates
	 * `FOR UPDATE SKIP LOCKED`, re-checks, applies the resume rule. Caller warns.
	 */
	repairOrphanWaiting(limit: number): Promise<EnqueueRequest[]>;

	/** Case 4, step 1: ids with `status='running' AND heartbeat_at < now() - stallTimeoutMs`. */
	listStalled(stallTimeoutMs: number, limit: number): Promise<string[]>;

	/**
	 * Case 4, step 2, one run per transaction: lock parent (if any) then the run,
	 * re-check stalled. `attempt < max_attempts` => `queued`, `attempt + 1`, `error`,
	 * `run_after = now()`, `enqueue_seq + 1`; else final `failed` + child resolution.
	 * Always `execution_id = NULL`. A final failure does NOT cancel descendants (as {@link fail}).
	 */
	recoverStalled(
		runId: string,
		stallTimeoutMs: number,
		error: SerializedError
	): Promise<RecoverStalledResult>;

	/**
	 * Retention (Phase 6): deletes up to `limit` runs with terminal status and
	 * `finished_at < now() - olderThanMs` whose parent is NULL or terminal
	 * (`FOR UPDATE SKIP LOCKED`). Cascades delete steps/waitpoints/logs/keys; children
	 * get `parent_*` SET NULL. Returns the number deleted (0 => done).
	 */
	deleteFinishedBatch(olderThanMs: number, limit: number): Promise<number>;
}

/** {@link RunStore} as exposed inside a transaction (no heartbeat: its SET LOCAL would leak). */
export type TxRunStore = Omit<RunStore, "heartbeat">;

// ---------------------------------------------------------------------------
// StepStore
// ---------------------------------------------------------------------------

export interface LoadedStep {
	name: string;
	seqIndex: number;
	status: StepStatus;
	wakeAt: Date | null;
	/** Always null when `ExecutionSnapshot.outputsDeferred` is true. */
	output: Json | null;
}

export interface WaitpointSummary {
	id: string;
	stepName: string;
	kind: WaitpointKind;
	status: WaitpointStatus;
	pendingCount: number | null;
	expiresAt: Date | null;
}

/** Everything the executor needs at the start of an execution. */
export interface ExecutionSnapshot {
	/** DB `now()` at load time: compare `wakeAt` against this, never `Date.now()`. */
	now: Date;
	/** Ordered by `seqIndex`. */
	steps: LoadedStep[];
	/** Phase 4 window query: total `pg_column_size(output)` > maxInlineBytes. */
	outputsDeferred: boolean;
	/** All waitpoints of the run (without outputs). */
	waitpoints: WaitpointSummary[];
}

export type InsertSleepResult = { kind: "inserted"; wakeAt: Date; delayMs: number } | Lost;

export interface StepStore {
	/**
	 * Phase 4: the step window query (one statement) plus the run's waitpoints and
	 * DB `now()` (at most one more statement). Read-only, no locks.
	 */
	loadForExecution(runId: string, maxInlineBytes: number): Promise<ExecutionSnapshot>;

	/** Deferred outputs for `seq_index BETWEEN fromSeq AND toSeq`, ordered. */
	loadOutputs(
		runId: string,
		fromSeq: number,
		toSeq: number
	): Promise<{ name: string; seqIndex: number; output: Json | null }[]>;

	/**
	 * `step.run` miss (the `createToken` step is written by {@link WaitpointStore.createToken}):
	 * `INSERT ... status='completed' ... ON CONFLICT (run_id, name) DO NOTHING` conditioned on
	 * the ownership guard (FOR SHARE) (Phase 4). Guard failed => `lost`; step already exists
	 * under that name => `ok` (idempotent replay, output not overwritten). A `(run_id,
	 * seq_index)` collision under a different name throws (unique violation). `output` is
	 * bound as JSON text, so any {@link Json} value (string, array, ...) round-trips.
	 */
	insertCompleted(
		owner: Ownership,
		step: { name: string; seqIndex: number; output: Json | null }
	): Promise<"ok" | "lost">;

	/** `step.sleep` first call: guarded insert `waiting`, `wake_at = now() + durationMs`. */
	insertSleep(
		owner: Ownership,
		step: { name: string; seqIndex: number; durationMs: number }
	): Promise<InsertSleepResult>;

	/**
	 * `step.sleep` replay when due: guarded `UPDATE ... SET status='completed' WHERE
	 * status='waiting' AND wake_at <= now()`. `not_due` if the step is still in the future.
	 */
	completeSleep(owner: Ownership, name: string): Promise<"ok" | "not_due" | "lost">;

	/**
	 * `${name}#wait` step of `waitForToken`: guarded insert `waiting`, output
	 * `{ waitpointId }`, `wake_at NULL`, `ON CONFLICT (run_id, name) DO NOTHING`. Same outcomes
	 * as {@link insertCompleted}: guard failed => `lost`; already exists => `ok`; seq_index
	 * collision under another name throws.
	 */
	insertWaitRef(
		owner: Ownership,
		step: { name: string; seqIndex: number; waitpointId: string }
	): Promise<"ok" | "lost">;

	/** Public `engine.runs.steps(runId)`: all steps with outputs, ordered by `seq_index`. */
	list(runId: string): Promise<StepRecord[]>;
}

// ---------------------------------------------------------------------------
// WaitpointStore
// ---------------------------------------------------------------------------

export type CreateTokenResult =
	| { kind: "created"; waitpointId: string; expiresAt: Date | null; expireDelayMs: number | null }
	| Lost;

export type CompleteTokenResult =
	| { kind: "not_found" }
	| { kind: "not_pending"; status: WaitpointStatus }
	| { kind: "completed"; resolution: WaitpointResolution };

export type ExpireWaitpointResult =
	/** Unknown id, not `pending`, or no `expires_at`. */
	| { kind: "noop" }
	/** Still `pending` but `expires_at > now()`: re-delay the expire job by `delayMs` (DB clock). */
	| { kind: "not_due"; delayMs: number }
	| {
			kind: "expired";
			resolution: WaitpointResolution;
			canceledChildren: CanceledRun[];
			/** As {@link CancelRunResult}: timed-out waitpoints of `canceledChildren` with `expires_at`. */
			expireJobsToRemove: string[];
	  };

export interface WaitpointStore {
	/**
	 * `createToken(name)` in one transaction: ownership guard; insert waitpoint
	 * `token`, `pending`, `expires_at = now() + timeoutMs` (null = none); insert step
	 * `name` `completed` with output `{ id }` (decision 11). Caller schedules
	 * `wp-${id}-expire` with `expireDelayMs` after commit.
	 */
	createToken(
		owner: Ownership,
		token: { waitpointId: string; name: string; seqIndex: number; timeoutMs: number | null }
	): Promise<CreateTokenResult>;

	/**
	 * `engine.tokens.complete` (decision 3). Read `run_id` unlocked, lock that run
	 * FOR UPDATE, then `UPDATE waitpoints SET status='completed', output WHERE id AND
	 * kind='token' AND status='pending'`, then the resume rule (no enqueue if the
	 * run is `running` or has other pending waitpoints).
	 */
	completeToken(waitpointId: string, output: Json): Promise<CompleteTokenResult>;

	/**
	 * `-system` expiry handler and reconciler case 5. Lock the owning run FOR UPDATE;
	 * act only if `status='pending' AND expires_at <= now()`: set `timed_out`; for
	 * `child_run`/`batch` cancel non-terminal children recursively (as
	 * {@link RunStore.cancel}, without resolving this waitpoint again, and timing out the
	 * canceled children's own pending waitpoints); apply the resume rule. Pending with
	 * `expires_at > now()` => `not_due` with the DB-computed remaining delay.
	 */
	expire(waitpointId: string): Promise<ExpireWaitpointResult>;

	/** Reconciler case 5: ids of `pending` waitpoints with `expires_at <= now()`. */
	listExpired(limit: number): Promise<string[]>;

	/** Token replay (decision 10): status + output read from `waitpoints`. */
	getToken(
		waitpointId: string
	): Promise<{ status: WaitpointStatus; output: Json | null; runId: string } | null>;
}

// ---------------------------------------------------------------------------
// IdempotencyStore
// ---------------------------------------------------------------------------

export type ClaimResult = { claimed: true } | { claimed: false; existingRunId: string };

export interface IdempotencyStore {
	/**
	 * Decision 9 step 2, run by {@link RunStore.create} in the SAME transaction,
	 * right after the run insert: `INSERT ... ON CONFLICT (task_id, key) DO UPDATE
	 * SET run_id, expires_at WHERE idempotency_keys.expires_at < now() RETURNING
	 * run_id`. No row => `SELECT run_id` for the live key. Never uses `xmax`.
	 * `expires_at = now() + ttlMs`, or NULL when `ttlMs` is null.
	 */
	claim(input: {
		taskId: string;
		key: string;
		runId: string;
		ttlMs: number | null;
	}): Promise<ClaimResult>;

	/** Lookup by `(task_id, key)`, live or expired. */
	find(taskId: string, key: string): Promise<{ runId: string; expiresAt: Date | null } | null>;
}

// ---------------------------------------------------------------------------
// LogStore
// ---------------------------------------------------------------------------

export interface NewLogEntry {
	runId: string;
	level: LogLevel;
	message: string;
	/** Already truncated to `maxLogEntryBytes`. */
	data: Json | null;
	/** Emission time (display only, never compared); DB `now()` if omitted. */
	ts?: Date;
}

export interface LogStore {
	/** One statement: `INSERT ... SELECT FROM unnest($1::uuid[], $2::text[], ...)`. No guard. */
	insertBatch(entries: readonly NewLogEntry[]): Promise<void>;

	/** Ordered by `id` asc; `afterId` exclusive; uses index `run_logs(run_id, id)`. */
	list(runId: string, opts: { afterId?: string; limit: number }): Promise<LogEntry[]>;
}

// ---------------------------------------------------------------------------
// Schedules (Phase 8)
// ---------------------------------------------------------------------------

// TODO(phase 8): `ScheduleStore` (upsert/delete/get/list over a `schedules`
// table added by a new migration). Ticks create runs through RunStore.create
// with key `__schedule:${id}:${ts}` and `ttlMs: null`.

// ---------------------------------------------------------------------------
// Store and transactions
// ---------------------------------------------------------------------------

/** Repositories bound to one open transaction (one pg client between BEGIN and COMMIT). */
export interface StoreTx {
	runs: TxRunStore;
	steps: StepStore;
	waitpoints: WaitpointStore;
	idempotency: IdempotencyStore;
	logs: LogStore;
}

/** Repositories bound to the pool, plus the transaction entry point. */
export interface Store {
	runs: RunStore;
	steps: StepStore;
	waitpoints: WaitpointStore;
	idempotency: IdempotencyStore;
	logs: LogStore;
	/**
	 * Runs `fn` in one transaction: COMMIT if it resolves, ROLLBACK and rethrow if it
	 * rejects; the client is always released. `tx` must not be used after `fn`
	 * settles, and transactions do not nest. The in-memory store implements this
	 * with a mutex plus copy-on-write state, so it is all-or-nothing and serialized.
	 */
	withTransaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T>;
}
