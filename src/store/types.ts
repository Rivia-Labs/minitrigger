/**
 * Persistence contract (Phase 2, task 2.0). The executor, engine, reconciler,
 * retention and the Phase 9 in-memory store depend ONLY on these interfaces.
 *
 * Global rules every implementation MUST honour (see docs/store-contract.md):
 *
 * - Every method is atomic. On a pool-bound {@link Store} a multi-statement
 *   method opens its own transaction (one pg client, BEGIN/COMMIT/ROLLBACK);
 *   on a {@link StoreTx} it runs inside the caller's transaction, using a
 *   SAVEPOINT only where partial rollback is needed (debounced
 *   {@link RunStore.create}).
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

/**
 * `steps.kind`: which `ctx.step` method recorded the row (decision 11; replay compares it
 * exactly). `run` = `step.run`, `sleep` = `step.sleep`, `token` = `createToken`, `wait` =
 * `${name}#wait` of `waitForToken`, `trigger` = `step.trigger`, `trigger_wait` =
 * `triggerAndWait`, `batch_wait` = `batchTriggerAndWait`.
 */
export type StepKind =
	| "run"
	| "sleep"
	| "token"
	| "wait"
	| "trigger"
	| "trigger_wait"
	| "batch_wait";
export type WaitpointKind = "token" | "child_run" | "batch";
export type WaitpointStatus = "pending" | "completed" | "timed_out";
export type LogLevel = "debug" | "info" | "warn" | "error";

/** Hard cap for {@link RunStore.list} (Phase 2). */
export const MAX_LIST_LIMIT = 100;
/** Default `$2` of the Phase 4 step window query. */
export const DEFAULT_MAX_INLINE_STEP_OUTPUT_BYTES = 16 * 1024 * 1024;
/** `runs.error.name` written by cancellation (decision 10). */
export const CANCELED_ERROR_NAME = "Canceled";

/**
 * `error.message` of the children canceled because their parent's `child_run`/`batch` waitpoint
 * timed out (`waitpoints.expire`). The executor maps exactly these to a `Timeout` result; a child
 * that failed or was canceled on its own before the timeout keeps its real outcome.
 */
export const WAITPOINT_TIMEOUT_REASON = "Parent waitpoint timed out";

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
	/**
	 * `ctx.progress.set` (decision 23): a single mutable value overwritten by its owning execution,
	 * unrelated to steps (not memoized, no `seq_index`). `null` until the first `set`. Survives
	 * retry and resume (never reset by the engine).
	 */
	progress: Json | null;
	/**
	 * Decision 24: set by a debounced trigger while the run is `queued` and not yet started;
	 * cleared (`NULL`) by {@link RunStore.acquire}, so a run collapses triggers only before its first
	 * execution.
	 */
	debounceKey: string | null;
	/** Decision 25: counted among `running` runs of the same queue at acquisition. */
	concurrencyKey: string | null;
}

/** One row of `steps` (PK `(run_id, name)`). */
export interface StepRecord {
	runId: string;
	name: string;
	seqIndex: number;
	kind: StepKind;
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
	/**
	 * The run's status at the moment this was computed (Phase 7 perf: `queued` for a fresh
	 * enqueue, `waiting` for a resume/sleep). Lets the caller publish it without a `runs.get`.
	 */
	status: RunStatus;
	/** The run row's `updated_at` at the moment this was computed (same DB write, no `runs.get`). */
	updatedAt: Date;
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
	taskId: string;
	queue: string;
	enqueueSeq: number;
	previousStatus: "queued" | "running" | "waiting";
	/** Unchanged by cancel; carried so notifiers don't need a `runs.get` (Phase 7 perf). */
	attempt: number;
	/** Always `{ name: "Canceled", message: reason, retryable: false }` for this cancel call. */
	error: SerializedError;
	/** The run row's `updated_at` after this cancel (status is always `canceled`; no `runs.get` needed). */
	updatedAt: Date;
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
	/** Decision 24. Mutually exclusive with `idempotency` (the store throws). */
	debounce?: NewRunDebounce;
	/** Decision 25: stored in `runs.concurrency_key`; enforced by {@link RunStore.acquireWithConcurrencyLimit}. */
	concurrencyKey?: string;
	/**
	 * Decision 26 (transactional trigger): the run is inserted with `outbox_pending = true` and the
	 * same statement runs `pg_notify(channel, '')`, so the caller must NOT enqueue `created.enqueue`
	 * (the transaction is not committed yet); a started engine claims it with
	 * {@link RunStore.claimOutbox} after the commit. Not combinable with `debounce` (the store
	 * throws). Only the SQL store supports it (the memory store throws).
	 */
	outbox?: { channel: string };
}

/**
 * Decision 24: collapse triggers of the same `(task_id, key)` into the single `queued` run that
 * has not started yet. When set, {@link NewRun.delayMs} is ignored: a new run gets
 * `run_after = now() + least(delayMs, maxDelayMs)` and `debounce_deadline = now() + maxDelayMs`.
 */
export interface NewRunDebounce {
	key: string;
	/**
	 * The debounce window (> 0). Always trailing: every collapsed trigger pushes `run_after` to
	 * `least(now() + delayMs, debounce_deadline)`.
	 */
	delayMs: number;
	/** `last`: a collapsed trigger replaces the pending run's payload. `first`: keeps it. */
	payload: "last" | "first";
	/** Cap on how far the run can be postponed from its first trigger; null = no cap. */
	maxDelayMs: number | null;
}

export type CreateRunResult =
	| { kind: "created"; run: RunRecord; enqueue: EnqueueRequest }
	| { kind: "existing"; run: RunRecord }
	| DebouncedCreateRunResult;

/**
 * Decision 24: collapsed into the pending run of the same debounce key. `run_after` is
 * recomputed as `least(now() + delayMs, debounce_deadline)`; `enqueue_seq` is bumped ONLY when
 * that recomputation actually moves `run_after` (audit item 1: a collapse that lands on the same
 * `run_after` as before -- e.g. every collapse after the deadline is pinned -- has nothing to
 * reschedule, so it must not fence the job that's already correctly scheduled for it). The
 * payload (`debounce.payload === "last"`) is still applied either way.
 */
export type DebouncedCreateRunResult =
	| {
			kind: "debounced";
			run: RunRecord;
			/** `run_after` changed: the caller enqueues `enqueue` and removes `${runId}-${staleSeq}`. */
			enqueue: EnqueueRequest;
			staleSeq: number;
	  }
	| {
			kind: "debounced";
			run: RunRecord;
			/** `run_after` did not change: nothing to enqueue or remove. */
			enqueue: null;
	  };

/** Decision 25: options of {@link RunStore.acquireWithConcurrencyLimit}. */
export interface ConcurrencyLimitOptions {
	/** Max `running` runs of the same `(queue, concurrency_key)` (integer >= 1). */
	limit: number;
}

/** Decision 25: base of the growing backoff of a `limited` acquisition attempt. */
export const CONCURRENCY_LIMITED_BASE_MS = 1_000;
/** Decision 25: cap of the growing backoff. */
export const CONCURRENCY_LIMITED_MAX_MS = 30_000;
/** Decision 25: upper bound of the random jitter added on top of the (capped) backoff. */
export const CONCURRENCY_LIMITED_JITTER_MS = 250;

/**
 * Decision 25: the retry delay of a `limited` acquisition attempt, given the run's
 * `concurrency_waits` BEFORE this attempt increments it (so the first `limited` result uses
 * `waits = 0` => `CONCURRENCY_LIMITED_BASE_MS`, the second `waits = 1` => twice that, and so on,
 * capped at {@link CONCURRENCY_LIMITED_MAX_MS}), plus jitter. Time itself always comes from the
 * database (decision 17); this only computes the number of milliseconds added to `now()`.
 */
export function concurrencyLimitedDelayMs(waitsBeforeThisAttempt: number): number {
	const base = Math.min(
		CONCURRENCY_LIMITED_BASE_MS * 2 ** waitsBeforeThisAttempt,
		CONCURRENCY_LIMITED_MAX_MS
	);
	return base + Math.floor(Math.random() * Math.min(CONCURRENCY_LIMITED_JITTER_MS, base));
}

export type AcquireWithLimitResult =
	| { kind: "acquired"; run: RunRecord }
	/** Stale or duplicate job (wrong seq, not `queued`/`waiting`): no-op, like `acquire` → null. */
	| { kind: "stale" }
	/**
	 * The run's concurrency key already has `limit` running runs: not acquired, attempt
	 * unchanged, status unchanged, `concurrency_waits + 1`, `enqueue_seq + 1`, `run_after = now() +
	 * `{@link concurrencyLimitedDelayMs}` of the PRE-increment `concurrency_waits` (a growing
	 * backoff per consecutive `limited` result of this run, reset to 0 once it acquires). The
	 * caller enqueues `enqueue`.
	 */
	| { kind: "limited"; enqueue: EnqueueRequest };

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
	/** Children's `run_after = now() + delayMs` (`step.trigger({ delay })`); default 0. */
	delayMs?: number;
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
	// `kind` isn't a `RunStatus` here (unlike `sleeping`/`requeued`, whose status lives on
	// `enqueue`), so the actual status ("waiting") is spelled out for the caller.
	| { kind: "waiting_on_waitpoints"; status: "waiting"; updatedAt: Date }
	| { kind: "sleeping"; enqueue: EnqueueRequest }
	| { kind: "requeued"; enqueue: EnqueueRequest }
	| Lost;

/**
 * `expireJobsToRemove`: the finished run's own waitpoints that were still `pending` with
 * `expires_at` and are now `timed_out` (as {@link CancelRunResult}); the caller removes
 * `wp-${id}-expire` best-effort after commit. `updatedAt` is the run row's `updated_at` from the
 * same write (`kind` is already its new status; Phase 7 perf: no `runs.get` needed).
 */
export type CompleteRunResult =
	| {
			kind: "completed";
			updatedAt: Date;
			parent: WaitpointResolution | null;
			expireJobsToRemove: string[];
	  }
	| Lost;
export type FailRunResult =
	| {
			kind: "failed";
			updatedAt: Date;
			parent: WaitpointResolution | null;
			expireJobsToRemove: string[];
	  }
	| Lost;
export type RetryRunResult = { kind: "retrying"; attempt: number; enqueue: EnqueueRequest } | Lost;
export type ReleaseRunResult = { kind: "released"; enqueue: EnqueueRequest } | Lost;
export type SetProgressResult = { kind: "ok"; updatedAt: Date } | Lost;

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
	| {
			kind: "failed";
			taskId: string;
			/** Unchanged (attempts were exhausted); carried so notifiers don't need a `runs.get`. */
			attempt: number;
			updatedAt: Date;
			parent: WaitpointResolution | null;
			expireJobsToRemove: string[];
	  };

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
	 * `run_after = now() + delayMs`, no parent) and claims the key with
	 * {@link IdempotencyStore.claim}'s semantics, atomically (SQL: one statement,
	 * the run inserted only if the key was claimed; no savepoint). Not claimed =>
	 * no run is inserted; return the run that owns the key, enqueuing nothing. If
	 * that run vanished meanwhile (retention), retry the whole operation once.
	 *
	 * With `debounce` (decision 24), in one transaction: `SELECT ... FOR UPDATE` the `queued` run
	 * with the same `(task_id, debounce_key)`. Found => collapse into it: `run_after` recomputed
	 * to `least(now() + delayMs, debounce_deadline)` (trailing), payload per `payload`;
	 * `enqueue_seq + 1` (and `enqueue` set) ONLY when that recomputed `run_after` differs from the
	 * locked row's current one (audit item 1 -- `acquire` fences on the seq, not `run_after`, so
	 * bumping it when nothing actually moved would needlessly fence the job already scheduled for
	 * the unchanged `run_after`, e.g. every collapse once `run_after` is pinned at
	 * `debounce_deadline`); otherwise `enqueue: null`, nothing to reschedule. Returns `debounced`
	 * either way. Not found => insert with `debounce_key`/`debounce_deadline`; a unique violation
	 * of the partial index
	 * (a concurrent trigger inserted first) rolls back to a savepoint and loops, now finding and
	 * collapsing into that run. Never `xmax`.
	 */
	create(input: NewRun): Promise<CreateRunResult>;

	/**
	 * Decision 26: claims up to `limit` committed runs with `outbox_pending` (oldest `created_at`
	 * first, `FOR UPDATE SKIP LOCKED`, so concurrent sweeps of several engines split the rows and
	 * never dispatch one twice) and clears the flag in the same statement. `claimed` counts every
	 * cleared row (the caller sweeps again while `claimed === limit`); `enqueue` holds only those
	 * still `queued` (a run already acquired through the reconciler's re-enqueue, or canceled, is
	 * just cleared). `updated_at` is not touched.
	 */
	claimOutbox(limit: number): Promise<OutboxClaim>;

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
	 * Also sets `debounce_key = NULL` (decision 24: the key is released once the run leaves
	 * `queued`).
	 */
	acquire(runId: string, executionId: string, seq: number): Promise<RunRecord | null>;

	/**
	 * Decision 25: {@link acquire} for a queue that declares `concurrencyKey`. One transaction:
	 * `SELECT ... FOR UPDATE` the run (same predicate as `acquire`; none → `stale`). Without a
	 * `concurrency_key` it is acquired exactly like `acquire`. With one:
	 * `pg_advisory_xact_lock(hashtextextended(schema || ':' || queue || ':' || key, 0))` (a
	 * single-bigint lock, a key space separate from decision 16's two-int locks) serializes the
	 * acquirers of that key, then `count(*)` of `running` runs with the same `(queue,
	 * concurrency_key)`: `>= limit` → `limited` (see {@link AcquireWithLimitResult}; the run's
	 * `concurrency_waits` grows the retry delay); else the decision-5 UPDATE, which also resets
	 * `concurrency_waits` to 0. Only `running` counts: a `waiting` run of the key does not hold a
	 * slot.
	 */
	acquireWithConcurrencyLimit(
		runId: string,
		executionId: string,
		seq: number,
		options: ConcurrencyLimitOptions
	): Promise<AcquireWithLimitResult>;

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
	 * lock parent first. The run's own `pending` waitpoints become `timed_out` in the same
	 * transaction (nothing can resume a finished run); see {@link CompleteRunResult}.
	 */
	complete(owner: Ownership, output: Json): Promise<CompleteRunResult>;

	/**
	 * Definitive failure (attempts exhausted, AbortTaskRunError, non-retryable). As {@link complete}.
	 * Like completion, it does NOT cancel descendants: children started with `step.trigger` may
	 * outlive their parent (only {@link cancel} and waitpoint expiry cancel children). Its own
	 * `pending` waitpoints become `timed_out` (as `complete`/`cancel`).
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
	 * `ctx.progress.set` (decision 23). Guarded single-statement update, like {@link heartbeat}
	 * (own short transaction, `SET LOCAL statement_timeout = timeoutMs`, never on a StoreTx) but
	 * touching `progress` and (unlike `heartbeat`) `updated_at`, so `runs.subscribe` sees it as a
	 * change: `UPDATE runs SET progress = $3::jsonb, updated_at = now() WHERE <ownership guard>
	 * RETURNING updated_at`. Guard failed (ownership lost) => `lost`. Overwrites the whole value
	 * (not a merge); survives retry/resume because nothing else ever touches this column. Bounded
	 * by `timeoutMs` so a write blocked behind another transaction's lock on the run row can't hang
	 * indefinitely (e.g. `close()` awaiting it during shutdown, after the heartbeat already
	 * stopped).
	 */
	setProgress(
		runId: string,
		executionId: string,
		value: Json,
		timeoutMs: number
	): Promise<SetProgressResult>;

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

	/**
	 * Case 1: `queued` AND `run_after < now() - grace` => re-enqueue with the CURRENT seq
	 * (read-only). Keyset pagination `ORDER BY run_after, id` after `after` (exclusive): runs whose
	 * jobs legitimately exist stay stale, so the reconciler walks past them instead of re-reading
	 * the first page every tick (head-of-line blocking). `nextCursor` is null on a short page.
	 */
	listStaleQueued(graceMs: number, limit: number, after?: StaleCursor | null): Promise<StalePage>;

	/**
	 * Case 2: `waiting`, no pending waitpoint, `run_after < now() - grace` => SAME seq (read-only).
	 * Paginated like {@link listStaleQueued}.
	 */
	listStaleWaiting(graceMs: number, limit: number, after?: StaleCursor | null): Promise<StalePage>;

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
	 * Always `execution_id = NULL`. A final failure does NOT cancel descendants (as {@link fail})
	 * but times out the run's own pending waitpoints (`expireJobsToRemove`).
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

/** Result of {@link RunStore.claimOutbox}. */
export interface OutboxClaim {
	claimed: number;
	enqueue: Array<EnqueueRequest & { attempt: number; error: SerializedError | null }>;
}

/**
 * Opaque keyset cursor of {@link RunStore.listStaleQueued}/{@link RunStore.listStaleWaiting}: the
 * last row's `(run_after, id)`. `runAfter` keeps the store's full precision (Postgres
 * microseconds, as UTC ISO text), which a JS `Date` would truncate. Only pass it back to the store
 * that produced it.
 */
export interface StaleCursor {
	readonly runAfter: string;
	readonly id: string;
}

/** One page of stale runs; `nextCursor` is null when the page was short (the end was reached). */
export interface StalePage {
	items: EnqueueRequest[];
	nextCursor: StaleCursor | null;
}

/** {@link RunStore} as exposed inside a transaction (no heartbeat: its SET LOCAL would leak). */
export type TxRunStore = Omit<RunStore, "heartbeat">;

// ---------------------------------------------------------------------------
// StepStore
// ---------------------------------------------------------------------------

export interface LoadedStep {
	name: string;
	seqIndex: number;
	kind: StepKind;
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
	 * bound as JSON text, so any {@link Json} value (string, array, ...) round-trips. `kind` is
	 * written to `steps.kind` (`run` for `step.run`).
	 */
	insertCompleted(
		owner: Ownership,
		step: { kind: StepKind; name: string; seqIndex: number; output: Json | null }
	): Promise<"ok" | "lost">;

	/**
	 * `step.sleep` first call: guarded insert `waiting`, `kind = 'sleep'`,
	 * `wake_at = now() + durationMs`.
	 */
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
	 * collision under another name throws. `kind` is written to `steps.kind` (`wait` for
	 * `${name}#wait`).
	 */
	insertWaitRef(
		owner: Ownership,
		step: { kind: StepKind; name: string; seqIndex: number; waitpointId: string }
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
	/**
	 * The token is `timed_out` because its own run's terminal exit is what timed it out, in the
	 * same transaction (`waitpoints.resolved_at = runs.finished_at` exactly, checked under the
	 * run's `FOR UPDATE` lock). A token that expired on its own first, in an earlier transaction,
	 * whose run only finished later, is `not_pending` with `status: "timed_out"` instead.
	 */
	| { kind: "run_finished" }
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
	 * `wp-${id}-expire` with `expireDelayMs` after commit. Idempotent by `(run_id, step_name)`:
	 * an existing token waitpoint of that step is returned (its id; `expireDelayMs` only while
	 * still `pending`), both inserts use `ON CONFLICT DO NOTHING`; a non-token waitpoint under the
	 * name throws.
	 */
	createToken(
		owner: Ownership,
		token: { waitpointId: string; name: string; seqIndex: number; timeoutMs: number | null }
	): Promise<CreateTokenResult>;

	/**
	 * `engine.tokens.complete` (decision 3). Read `run_id` unlocked, lock that run
	 * FOR UPDATE, then `UPDATE waitpoints SET status='completed', output WHERE id AND
	 * kind='token' AND status='pending'`, then the resume rule (no enqueue if the
	 * run is `running` or has other blocking waitpoints). Not pending: `run_finished` only when
	 * the token is `timed_out` AND its run is terminal AND `resolved_at = finished_at` exactly
	 * (same transaction: the run's own terminal exit is what timed it out); otherwise
	 * `not_pending` with the token's status (a token that expired on its own, in an earlier
	 * transaction, whose run later finished is `not_pending` with `status: "timed_out"`).
	 */
	completeToken(waitpointId: string, output: Json): Promise<CompleteTokenResult>;

	/**
	 * `-system` expiry handler and reconciler case 5. Lock the owning run FOR UPDATE;
	 * act only if `status='pending' AND expires_at <= now()`: set `timed_out`; for
	 * `child_run`/`batch` cancel non-terminal children recursively (as
	 * {@link RunStore.cancel}, without resolving this waitpoint again, and timing out the
	 * canceled children's own pending waitpoints; a child that already finished is left alone
	 * together with its descendants); apply the resume rule. Pending with
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

/** Distinguishes schedules declared via `engine.schedules.upsert()` from ones synced from `defineTask({ schedule })`. */
export type ScheduleSource = "api" | "declarative";

/** One row of `schedules`. Postgres is authoritative (decision 8): the tick handler always reads
 * `cron`/`timezone`/`payload` from here, never trusts the BullMQ job scheduler's own copy. */
export interface ScheduleRecord {
	id: string;
	taskId: string;
	cron: string;
	timezone: string | null;
	payload: Json | null;
	source: ScheduleSource;
	createdAt: Date;
	updatedAt: Date;
}

export interface NewSchedule {
	/** Chosen by the caller; validated as `^[a-zA-Z0-9_.-]{1,128}$` (no `:`; decision 7's spirit --
	 * it is embedded in the idempotency key `__schedule:${id}:${ts}` and used as the BullMQ job
	 * scheduler id). */
	id: string;
	taskId: string;
	cron: string;
	timezone: string | null;
	payload: Json | null;
	source: ScheduleSource;
}

export interface UpsertScheduleResult {
	/**
	 * The row as it is after this call: the freshly written row normally, or -- only when
	 * {@link UpsertScheduleOptions.requireSourceMatch} blocked the write -- the pre-existing row,
	 * untouched (see {@link conflict}).
	 */
	schedule: ScheduleRecord;
	/** True when this call inserted a new row (no previous `cron`/`timezone` to compare against). */
	created: boolean;
	/** True when `cron` differs from what was previously stored; always true when `created`. */
	cronChanged: boolean;
	/** True when `timezone` differs from what was previously stored; always true when `created`. */
	timezoneChanged: boolean;
	/**
	 * Set (instead of writing anything) when `requireSourceMatch` was given and a row already
	 * existed under a different `source` -- e.g. an `engine.schedules.upsert()` (`source: "api"`)
	 * call racing/targeting an id already owned by `defineTask({ schedule })` (`source:
	 * "declarative"`), or vice versa. The caller decides what to do with the conflict (fix 4,
	 * `src/schedules.ts`): the declarative sync logs and skips; the public API throws.
	 */
	conflict?: { existingSource: ScheduleSource };
}

export interface UpsertScheduleOptions {
	/**
	 * When true, the write only proceeds if there is no existing row with this `id` or its `source`
	 * already matches `input.source`; otherwise nothing is written and {@link UpsertScheduleResult.conflict}
	 * is set instead. Default false (unconditioned overwrite, incl. `source`), which is what the
	 * plain store contract tests exercise directly.
	 */
	requireSourceMatch?: boolean;
}

export interface ListSchedulesFilter {
	source?: ScheduleSource;
}

/**
 * Schedules (Phase 8, decisions 8 and 21). Ticks create runs through {@link RunStore.create} with
 * idempotency key `__schedule:${id}:${ts}` and `ttlMs: null`; that flow lives in the engine, not
 * here. `id` is validated by every method that can introduce a new one ({@link upsert}); `get`,
 * `delete` and `list` accept any string (a non-matching id simply won't be found).
 */
export interface ScheduleStore {
	/**
	 * Insert-or-replace by `id` (single statement `ON CONFLICT (id) DO UPDATE`, inside one
	 * transaction that first reads the previous `cron`/`timezone` to compute the `*Changed` flags).
	 * The caller (engine) uses those flags to decide whether the BullMQ job scheduler needs a
	 * matching `upsertJobScheduler` call.
	 */
	upsert(input: NewSchedule, options?: UpsertScheduleOptions): Promise<UpsertScheduleResult>;

	/** True if a row with `id` existed and was deleted. */
	delete(id: string): Promise<boolean>;

	get(id: string): Promise<ScheduleRecord | null>;

	/** All schedules, optionally filtered by `source`, ordered by `id`. */
	list(filter?: ListSchedulesFilter): Promise<ScheduleRecord[]>;

	/** Shorthand for `list({ source: "declarative" })`. */
	listDeclarative(): Promise<ScheduleRecord[]>;

	/**
	 * `pruneDeclarative` (decision 21): deletes every `source = 'declarative'` schedule whose `id`
	 * is not in `ids` (the ones `defineTask({ schedule })` currently declares across the engine's
	 * registered tasks). An empty `ids` deletes every declarative schedule. Never touches `api`
	 * schedules. Returns the number of rows deleted.
	 */
	deleteDeclarativeExcept(ids: readonly string[]): Promise<number>;
}

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
	schedules: ScheduleStore;
}

/** Repositories bound to the pool, plus the transaction entry point. */
export interface Store {
	runs: RunStore;
	steps: StepStore;
	waitpoints: WaitpointStore;
	idempotency: IdempotencyStore;
	logs: LogStore;
	schedules: ScheduleStore;
	/**
	 * Runs `fn` in one transaction: COMMIT if it resolves, ROLLBACK and rethrow if it
	 * rejects; the client is always released. `tx` must not be used after `fn`
	 * settles, and transactions do not nest. The in-memory store implements this
	 * with a mutex plus copy-on-write state, so it is all-or-nothing and serialized.
	 */
	withTransaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T>;
}
