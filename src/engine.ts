/**
 * `createEngine(config)` and the engine lifecycle (SPEC.md §3, Phase 3).
 *
 * No global state (decision 13): everything (pool, Redis connections, BullMQ queues/workers,
 * active executions) hangs off the object returned here, so several engines with different
 * schema/prefix can live in one process.
 */

import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { createLazyBackend } from "./backends/lazy.js";
import { createPostgresBackend } from "./backends/postgres.js";
import { withTimeout } from "./backends/shared.js";
import type { BackendContext, BackendFactory, BackendRuntime } from "./backends/types.js";
import {
	assertTimerMs,
	assertTtlWithinRetention,
	derivePoolMax,
	type EngineConfig,
	type EngineLogger,
	MAX_QUEUE_SCALE_CONCURRENCY,
	type ResolvedConfig,
	resolveConfig,
} from "./config.js";
import { ConfigError, MinitriggerError, SerializationError } from "./errors.js";
import {
	ActiveExecutions,
	afterResolution,
	cancelLocally,
	childTaskSpec,
	createSystemDispatcher,
	type ExecutorDeps,
	enqueue,
	executeRun,
	type RunNotifier,
	type RunStatusInfo,
	type RunTransition,
	taskQueueName,
	transitionOfStatus,
} from "./executor/index.js";
import { createLeaderElector, type LeaderElector } from "./leader.js";
import { type MigrateResult, migrate } from "./migrations/migrate.js";
import type { PubSub, PubSubPublisher, RunStatusMessage } from "./pubsub.js";
import { SCHEDULE_TICK_JOB_NAME, systemQueueName } from "./queue/ids.js";
import type { Transport } from "./queue/transport.js";
import { createReconciler } from "./reconciler.js";
import { buildRegistry, getTask, type Registry } from "./registry.js";
import { createRetention } from "./retention.js";
import {
	createReconcileSchedulesHook,
	createSchedulesApi,
	type EngineSchedules,
	foreignScheduleState,
	foreignScheduleTickSchema,
	removeRetiredForeignScheduler,
	ScheduleTickSchemaMismatchError,
	syncDeclarativeSchedules,
} from "./schedules.js";
import type { SqlQueryable } from "./store/sql/db.js";
import { createSqlStore } from "./store/sql/index.js";
import {
	type BatchCandidatesFilter,
	type CreateRunResult,
	type Json,
	type LogLevel,
	MAX_BATCH_LIMIT,
	type NewRun,
	type NewRunDebounce,
	type RunCursor,
	type RunRecord,
	type RunStatus,
	retryIdempotencyKey,
	type SerializedError,
	type StepKind,
	type StepStatus,
	type Store,
	TERMINAL_RUN_STATUSES,
	type WaitpointStatus,
} from "./store/types.js";
import type { Streams } from "./streams.js";
import type { AnyTask, PayloadArgs, RunHandle, TaskOutput, TaskPayload } from "./task.js";
import { computeBackoffMs } from "./utils/backoff.js";
import { type Duration, parseDuration } from "./utils/duration.js";
import { describeOffendingValue } from "./utils/error-format.js";
import {
	validateIdempotencyKey,
	validateQueueName,
	validateTriggerKey,
} from "./utils/identifiers.js";
import { assertSerializable } from "./utils/serialization.js";

const DEFAULT_LIST_LIMIT = 20;
/** Item 6: `outbox.notify: false` above this `pollIntervalMs` gets a one-time startup warning. */
const OUTBOX_NOTIFY_FALSE_WARN_THRESHOLD_MS = 10_000;
/**
 * How long `stop()` waits, after handing back the still-active runs, for the in-flight
 * `executeRun` calls (a late acquire being handed back, a retry or parent-resume enqueue) so their
 * enqueues land before the queues close. Normally near-instant: an aborted execution stops waiting
 * for the task function right away.
 */
const IN_FLIGHT_DRAIN_MS = 5_000;
/** How long `stop()` waits for the re-enqueue of each run it hands back (Redis may be unreachable). */
const HAND_BACK_ENQUEUE_TIMEOUT_MS = 3_000;
/** Added to `2 * heartbeatTimeoutMs` for the hand-back flush of each still-active run on `stop()`. */
const HAND_BACK_FLUSH_MARGIN_MS = 500;
/** How long `stop()` waits for a running leader tick before releasing leadership. */
const LEADER_TICK_STOP_TIMEOUT_MS = 5_000;
/** stop() during start(): wait this long before dropping the subscriber to unblock start(). */
const START_SETTLE_GRACE_MS = 1_000;
/** ...then at most this long more before stopping anyway. */
const START_SETTLE_TIMEOUT_MS = 10_000;
/** How long `stop()` waits for pending best-effort status/cancel publishes before closing Redis. */
const NOTIFY_DRAIN_MS = 2_000;
/**
 * How long `stop()` waits for `pool.end()` of the pool the engine created (review item b):
 * `pg` only resolves it once every checked-out client is released, so a client held by work that
 * survived the bounded waits above would otherwise keep `stop()` pending forever.
 */
export const POOL_END_TIMEOUT_MS = 5_000;
const DEFAULT_LOGS_LIMIT = 100;
const MAX_LOGS_LIMIT = 1_000;
/**
 * `runs.subscribe` safety net: after this long without a pub/sub message, the subscription
 * re-reads the run from Postgres (pub/sub is at-most-once; a message lost on a reconnect must not
 * leave a subscriber waiting forever for a terminal status).
 */
const SUBSCRIBE_POLL_MS = 15_000;

export interface TriggerOptions {
	/** Decision 9: same `(task, key)` while the key is alive returns the existing run. Must not start with `__`. */
	idempotencyKey?: string;
	/** Overrides `config.idempotencyKeyTtl` (default 7 days) for this key; must not exceed `retention.days`. */
	idempotencyKeyTtl?: Duration;
	/** Delays the first execution (`run_after = now() + delay`, DB clock). Not with `debounce`. */
	delay?: Duration;
	/**
	 * Decision 24: collapse triggers with the same `(task, key)` into one pending run. While a run
	 * of the key is `queued` and has not started yet, a new trigger returns that run instead of
	 * creating another, pushing the execution `delay` further out (trailing: the run executes once
	 * the key has been quiet for `delay`, capped by `maxDelay`). Once the run starts, the next
	 * trigger creates a new pending run (so there is at most one pending run per key; add
	 * `concurrencyKey` to also keep two from running at once). Not combinable with `idempotencyKey`
	 * or `delay`.
	 */
	debounce?: DebounceOptions;
	/**
	 * Decision 25: at most `queue.concurrencyKey.limit` runs of this queue with the same key
	 * execute at once. The task's queue must declare `concurrencyKey: { limit }`.
	 */
	concurrencyKey?: string;
	/**
	 * Decision 26 (transactional trigger / outbox): inserts the run inside YOUR open transaction,
	 * so it commits or rolls back together with your own writes. Pass the node-postgres client on
	 * which you ran `BEGIN` (`await pool.connect()` + `BEGIN`, or a `pg.Client`), connected to the
	 * engine's database; never the `Pool` itself. Nothing is enqueued before your `COMMIT`: after it,
	 * a started engine (any process) dispatches the run within milliseconds (LISTEN/NOTIFY), or
	 * within ~2 s by its poll if the notification is lost. `delay` counts from the start of your
	 * transaction (Postgres `now()`). No status/event is emitted by `trigger` itself: `run:queued`
	 * is emitted by the engine that dispatches the run. Not combinable with `debounce`.
	 * The engine only runs single parameterized statements on it (no `SAVEPOINT`, so any number
	 * of triggers, with or without `idempotencyKey`, opens no subtransaction), never session-level
	 * state. A client without `BEGIN` also works: each trigger then autocommits on its own.
	 */
	tx?: TransactionClient;
}

/**
 * What `trigger(..., { tx })` accepts: a node-postgres `PoolClient`/`Client` inside `BEGIN`, or
 * any adapter with the same `query(text, values)` shape (`$1..$n` placeholders) bound to an open
 * transaction on the engine's database (decision 26).
 */
export type TransactionClient = SqlQueryable;

/** Decision 24: `trigger(task, payload, { debounce })`. */
export interface DebounceOptions {
	/** Scoped by task; non-empty, at most 512 characters, must not start with `__`. */
	key: string;
	/**
	 * The debounce window (> 0). Always trailing: each trigger of the key pushes the execution to
	 * `delay` after it, so the run executes once the key has been quiet for `delay` (capped by
	 * `maxDelay`).
	 */
	delay: Duration;
	/** `last` (default): a collapsed trigger replaces the pending run's payload. `first`: keeps it. */
	payload?: "last" | "first";
	/**
	 * Upper bound on how long a run can be postponed, from its first trigger (`trailing`).
	 * Must be >= `delay`. Default: no bound.
	 */
	maxDelay?: Duration;
}

export interface StartOptions {
	/** Only start workers for these task queues. The `-system` worker always starts (decision 8). */
	queues?: string[];
}

/** Public view of a run. */
export interface Run<O = unknown> {
	id: string;
	taskId: string;
	queue: string;
	status: RunStatus;
	payload: Json;
	/** Set only when `completed`. */
	output: O | null;
	/** Last attempt's error (kept while retrying), the final error, or the cancel reason. */
	error: SerializedError | null;
	attempt: number;
	maxAttempts: number;
	parentRunId: string | null;
	batchIndex: number | null;
	/** Next time the run is due; null while waiting on waitpoints or once finished. */
	runAfter: Date | null;
	startedAt: Date | null;
	finishedAt: Date | null;
	createdAt: Date;
	updatedAt: Date;
	/** Last value written by `ctx.progress.set` (decision 23); `null` until the first `set`. */
	progress: Json | null;
	/** `trigger(..., { concurrencyKey })` (decision 25), or null. */
	concurrencyKey: string | null;
}

export interface ListRunsOptions {
	taskId?: string;
	status?: RunStatus | readonly RunStatus[];
	createdFrom?: Date;
	createdTo?: Date;
	/** `nextCursor` of a previous page. */
	cursor?: RunCursor;
	/** Default 20, max 100. */
	limit?: number;
}

export interface RunList {
	items: Run[];
	nextCursor: RunCursor | null;
}

export type CancelRunOutcome = "canceled" | "already_finished" | "not_found";

/**
 * Which runs a batch operation (`runs.cancelMany`/`runs.retryFailed`) acts on, walked oldest
 * first (`createdAt`, then id). Pass the previous result's `nextCursor` as `cursor` to continue.
 */
export interface BatchRunsFilter {
	taskId?: string;
	queue?: string;
	/** Inclusive. */
	createdFrom?: Date;
	/** Exclusive. */
	createdTo?: Date;
	/** `nextCursor` of a previous call with the same filter. */
	cursor?: RunCursor;
	/** Required: how many matching runs this call handles, 1 to 1 000. */
	limit: number;
}

/** Item 2: one matched run's unexpected failure in `runs.cancelMany`/`runs.retryFailed`. */
export interface BatchRunError {
	name: string;
	message: string;
}

export interface CancelManyResult {
	/**
	 * Every run this call moved to `canceled`: the matched runs and, recursively, their pending
	 * descendants (so it can hold more than `limit` ids). A matched run that finished (or was
	 * canceled by someone else) meanwhile is simply not listed.
	 */
	canceled: string[];
	/**
	 * One entry per matched run whose cancel raised something other than the retried deadlock (a
	 * store/Redis hiccup): the batch keeps going, canceling every other matched run, rather than
	 * losing `canceled` entirely to the first error. Call again (same filter/cursor) to retry these.
	 */
	failed: Array<{ runId: string; error: BatchRunError }>;
	/** More runs matched than `limit`: call again with `cursor: nextCursor`. */
	hasMore: boolean;
	nextCursor: RunCursor | null;
}

/**
 * Why `runs.retryFailed` left a matched failed run alone. `invalid_payload` (item 2): the run's
 * stored payload no longer validates (e.g. `config.limits.maxPayloadBytes` was lowered since it
 * was created) -- `createRunDetailed`'s `assertSerializable` throws a `SerializationError`/
 * `ConfigError` for it every time, so retrying it would fail the same way forever; skip it and let
 * the rest of the page's candidates retry normally.
 */
export type RetrySkipReason = "task_not_registered" | "already_retried" | "invalid_payload";

export interface RetryFailedResult {
	/** One entry per failed run retried by this call: the failed run and its new run. */
	retried: Array<{ runId: string; newRunId: string }>;
	/**
	 * `task_not_registered`: this process does not register the run's task (e.g. mid rolling
	 * deploy; retry from a process that does). `already_retried`: a concurrent call retried it first.
	 */
	skipped: Array<{ runId: string; reason: RetrySkipReason }>;
	/** More runs matched than `limit`: call again with `cursor: nextCursor`. */
	hasMore: boolean;
	nextCursor: RunCursor | null;
}

/** Decision 27: the persisted pause state of a queue. */
export interface QueueState {
	name: string;
	paused: boolean;
	/** Last `pause`/`resume` of this queue (DB clock); null if it never had one. */
	updatedAt: Date | null;
}

/** One queue of `engine.queues.list()`. */
export interface QueueInfo extends QueueState {
	/** Some task registered in THIS engine uses the queue. */
	declared: boolean;
	/** `concurrency` resolved from this engine's tasks (null when not declared here). */
	declaredConcurrency: number | null;
	/**
	 * This process's worker concurrency (the `scale()`d value, else the declared one); null when
	 * this process runs no worker for the queue (not started, or filtered out by
	 * `start({ queues })`).
	 */
	concurrency: number | null;
	/**
	 * Executions of this queue's runs in progress in THIS process right now: from acquisition until
	 * the execution settles (a run being handed back on `stop()` included). Other processes'
	 * executions are not counted.
	 */
	activeLocal: number;
}

/**
 * Queue control (decision 27). Works without `start()` (an admin process can pause/resume).
 * Pausing never aborts running executions: they finish (or suspend) normally; only new jobs of
 * the queue stop being fetched, by every process. Retries, resumes and sleeps of the queue's runs
 * wait while it is paused.
 */
export interface EngineQueues {
	/**
	 * Persists `paused = true` in Postgres, then sets BullMQ's queue-level pause (Redis), so every
	 * worker of every process stops fetching the queue's jobs within one fetch. Idempotent. The
	 * queue must be used by a task of this engine or already have a persisted state.
	 */
	pause(name: string): Promise<QueueState>;
	/** Persists `paused = false`, then lifts BullMQ's pause. Idempotent. */
	resume(name: string): Promise<QueueState>;
	/**
	 * The queues of this engine's tasks plus every queue with a persisted state, sorted by name.
	 * `paused` is read from Postgres (the source of truth).
	 */
	list(): Promise<QueueInfo[]>;
	/**
	 * Changes THIS process's worker concurrency for the queue at runtime (integer >= 1), until the
	 * process restarts: not persisted, other processes are not affected. A decrease stops new
	 * fetches beyond the new limit at once (running jobs finish); an increase applies at the
	 * worker's next fetch. Requires a started worker for the queue (`ConfigError` otherwise).
	 * Keep `pool.max` above the total concurrency + 5 (a warning is logged when it is not).
	 */
	scale(name: string, concurrency: number): Promise<void>;
}

/** One `ctx.logger` entry of a run (`run_logs`). */
export interface RunLogEntry {
	/** Monotonic per insert order (bigserial, as a string); use it as `afterId` to page/tail. */
	id: string;
	level: LogLevel;
	message: string;
	/** Truncated to `maxLogEntryBytes` (with a marker) when it was larger. */
	data: Json | null;
	/** When the entry was logged. */
	ts: Date;
}

export interface RunLogsOptions {
	/** Exclusive: only entries with a larger id (pass the previous page's `nextAfterId`; null = from the start). */
	afterId?: string | null | undefined;
	/** Default 100, max 1 000. */
	limit?: number;
}

export interface RunLogPage {
	items: RunLogEntry[];
	/** `id` of the last item when the page is full (more may follow), else null. */
	nextAfterId: string | null;
}

/** One step of a run, in `seq` order. */
export interface RunStep {
	name: string;
	/** Call order within the run (decision 3). */
	seq: number;
	/** Which `ctx.step` method recorded it (`run`, `sleep`, `token`, `wait`, `trigger`, `trigger_wait`, `batch_wait`). */
	kind: StepKind;
	status: StepStatus;
	/**
	 * `run`: the memoized output. `token`: `{ id }`. `trigger`: `{ id }` of the child run. Waits
	 * (`wait`, `trigger_wait`, `batch_wait`): only the reference `{ waitpointId }`; results live in
	 * the waitpoint / child runs (decision 10). `sleep`: null.
	 */
	output: Json | null;
	/** Only for `sleep` steps. */
	wakeAt: Date | null;
	createdAt: Date;
}

/** One status change delivered by `runs.subscribe`. */
export interface RunStatusUpdate {
	runId: string;
	status: RunStatus;
	/** `runs.updated_at` of that change (database clock, decision 17). */
	updatedAt: Date;
	/**
	 * Latest known progress (decision 23): the initial emission's is read from Postgres; a later
	 * one keeps the previous value when the message that triggered it didn't carry a fresher one
	 * (an ordinary status transition, cheaply omitting it) and updates it on a `ctx.progress.set`
	 * write or a transition published with the value at hand.
	 */
	progress: Json | null;
}

export interface SubscribeOptions {
	/** Ends the iteration (without an error) when it fires. */
	signal?: AbortSignal;
}

export interface ReadStreamOptions {
	/** Ends the iteration (without an error) when it fires. */
	signal?: AbortSignal;
}

/** Local run event names (`engine.events.on`). */
export type RunEventName = `run:${RunTransition}`;

/**
 * A transition made by THIS process (trigger, acquire, suspend, complete, fail, retry, cancel,
 * stall recovery when it is the leader). Other processes' transitions are not delivered here; use
 * `runs.subscribe` for a cross-process view of one run.
 */
export interface RunEvent {
	name: RunEventName;
	runId: string;
	taskId: string;
	/** The status the transition moved the run to. */
	status: RunStatus;
	attempt: number;
	/** Last error (e.g. of the failed attempt for `run:retrying`/`run:failed`). */
	error: SerializedError | null;
	/** `runs.updated_at` read right after the transition. */
	updatedAt: Date;
	/**
	 * Wall-clock start of the CURRENT EXECUTION this transition ends/suspends -- since its most
	 * recent acquisition or resume, not the whole attempt's history across earlier suspensions (DB
	 * clock, decision 17). Present for `run:completed`/`run:failed`/`run:retrying`/`run:waiting`;
	 * absent otherwise -- e.g. `run:queued` (including a suspend that requeues right away because
	 * everything it waited for was already resolved by the time the suspension transaction ran:
	 * that's a `queued` transition, not a `waiting` execution with something pending), `run:started`,
	 * `run:canceled`, a stall recovery -- where there is no execution to time from this process's
	 * point of view.
	 */
	startedAt?: Date;
	/**
	 * How long the CURRENT EXECUTION ran before this write (monotonic clock, `performance.now()`;
	 * never persisted). Same presence rule and "current execution, not the whole attempt" caveat as
	 * {@link RunEvent.startedAt}.
	 */
	durationMs?: number;
}

export type RunEventHandler = (event: RunEvent) => void | Promise<void>;

export interface EngineEvents {
	/**
	 * Registers a handler for a local run event; returns the unsubscribe function. Handlers run
	 * after the transition was committed, off the run's critical path; a throwing/rejecting
	 * handler is logged (`logger.error`) and never affects the run.
	 */
	on(event: RunEventName, handler: RunEventHandler): () => void;
}

export interface EngineRuns {
	get<O = unknown>(run: string | RunHandle<O>): Promise<Run<O> | null>;
	list(options?: ListRunsOptions): Promise<RunList>;
	/** Cancels the run and, recursively, its pending descendants. */
	cancel(run: string | RunHandle<unknown>): Promise<CancelRunOutcome>;
	/** New run of the same task with the same payload (no idempotency key). */
	replay<O = unknown>(run: string | RunHandle<O>): Promise<RunHandle<O>>;
	/**
	 * Cancels up to `limit` `queued`/`running`/`waiting` runs matching `filter`, oldest first, each
	 * exactly like `cancel()` (descendants too, local aborts, pub/sub, events), in its own short
	 * transaction (no transaction spans the batch). Safe to run concurrently with anything
	 * (per-run row locks; a run finished meanwhile is skipped).
	 */
	cancelMany(filter: BatchRunsFilter): Promise<CancelManyResult>;
	/**
	 * Retries up to `limit` `failed` top-level runs matching `filter`, oldest first, like
	 * `replay()`: a NEW run of the same task with the same payload (the failed run stays as it is,
	 * `attempt` starts over at 1). Each failed run is retried at most once, even by concurrent
	 * calls (internal idempotency key `__retry:<runId>`): a run already retried no longer matches.
	 * Children (runs with a parent) are left out: their failure was already delivered to the parent.
	 */
	retryFailed(filter: BatchRunsFilter): Promise<RetryFailedResult>;
	/** The run's `ctx.logger` entries, oldest first (page with `afterId`). */
	logs(run: string | RunHandle<unknown>, options?: RunLogsOptions): Promise<RunLogPage>;
	/** The run's recorded steps, in `seq` order. */
	steps(run: string | RunHandle<unknown>): Promise<RunStep[]>;
	/**
	 * Status updates of one run (works without `start()`). Emits the current status read from
	 * Postgres first, then every later change received via Redis pub/sub (only if newer than the
	 * last one emitted), and ends right after a terminal status (immediately if the run is already
	 * finished). Ends without emitting anything if the run does not exist. `break`, `return()`,
	 * `options.signal` and `engine.stop()` end it too.
	 */
	subscribe(
		run: string | RunHandle<unknown>,
		options?: SubscribeOptions
	): AsyncIterable<RunStatusUpdate>;
	/**
	 * Reads the Redis Stream written by `ctx.stream(name, ...)` from its start: chunks already
	 * written come at once, then live ones, until the writer ends it. Each reader uses its own
	 * Redis connection, closed when the iteration ends. A stream expires 1 day after its end.
	 */
	readStream<T = Json | string>(
		run: string | RunHandle<unknown>,
		name: string,
		options?: ReadStreamOptions
	): AsyncIterable<T>;
}

/**
 * Result of `engine.tokens.complete`. A completion that does not apply is a no-op
 * `{ completed: false, reason }`:
 * - `not_found`: unknown id (or not a token);
 * - `completed`: the token was already completed;
 * - `timed_out`: the token's own timeout fired first, in an earlier transaction than any run
 *   terminal exit — including a token that timed out on its own and whose run only finished
 *   later;
 * - `run_finished`: the token's run already completed, failed or was canceled, and that very
 *   terminal transition is what timed out this token (same transaction, so nothing will ever
 *   read this output).
 */
export type CompleteTokenOutcome =
	| { completed: true }
	| {
			completed: false;
			reason: "not_found" | Exclude<WaitpointStatus, "pending"> | "run_finished";
	  };

export interface EngineTokens {
	/**
	 * Completes a token created by `ctx.step.createToken` (decisions 3, 12): `output` is validated
	 * like a payload (`maxPayloadBytes`; `undefined` is stored as `null`). Resumes the waiting run
	 * only if it is `waiting` with nothing else blocking. When the token's own terminal exit is
	 * what timed it out (completed/failed/canceled run, same transaction as the timeout) the
	 * result is `{ completed: false, reason: "run_finished" }`; a token that timed out on its own
	 * first, in an earlier transaction, reports `"timed_out"` even if its run later finished.
	 */
	complete(tokenId: string, output?: unknown): Promise<CompleteTokenOutcome>;
}

export interface Engine {
	/** Applies pending migrations to `config.schema` (safe to call concurrently). */
	migrate(): Promise<MigrateResult>;
	/** Starts workers (decision 19: an engine that never calls this only triggers and queries). */
	start(options?: StartOptions): Promise<void>;
	/** Graceful shutdown (decision 20). Idempotent. */
	stop(): Promise<void>;
	/** The payload is optional (and must be omitted or `undefined`) for a task that takes none. */
	trigger<T extends AnyTask>(
		task: T,
		...args: PayloadArgs<TaskPayload<T>, [options?: TriggerOptions]>
	): Promise<RunHandle<TaskOutput<T>>>;
	runs: EngineRuns;
	tokens: EngineTokens;
	schedules: EngineSchedules;
	/** Queue control (decision 27): pause/resume across processes, list, local scale. */
	queues: EngineQueues;
	/** Local run events of this process. */
	events: EngineEvents;
	/** The logger this engine was configured with (`config.logger`; default: warn/error to `console`). */
	readonly logger: EngineLogger;
}

/**
 * The surface `Engine` and `TestEngine` (`src/testing/test-engine.ts`, Phase 9.2 L4) share: enough
 * to trigger tasks and query/react to runs from code that does not care whether it is talking to
 * the real engine or a test one (e.g. a helper shared by production code and tests). Deliberately
 * excludes `migrate`/`start`/`stop`, which only `Engine` has in this shape (`TestEngine.stop()`
 * differs -- it ends open subscriptions/stream reads instead of a graceful worker shutdown).
 */
export type EngineClient = Pick<
	Engine,
	"trigger" | "runs" | "tokens" | "schedules" | "events" | "queues"
>;

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * What differs between the real engine (Postgres + BullMQ + Redis) and the test engine
 * (`createTestEngine`, everything in memory). Everything else -- `trigger`, `runs.*`, `tokens`,
 * `events`, `schedules` and the executor deps -- is built once by {@link createEngineCore}, so both
 * engines run the same code paths (the test engine must not fork execution semantics).
 * @internal
 */
export interface EngineBackend {
	store: Store;
	transport: Transport;
	/** Cross-process status/cancel publishing (best-effort). */
	publisher: PubSubPublisher;
	/** Builds the subscriber side; called at most once, lazily. */
	createPubSub(): PubSub;
	/** Behind `ctx.stream`. */
	streams: Pick<Streams, "pipe">;
	/** Behind `runs.readStream`. */
	readStream(
		runId: string,
		name: string,
		signal: AbortSignal | undefined
	): AsyncIterable<string | Json>;
	/**
	 * Decision 26: `runs.create` inside the caller's transaction, flagged for the outbox. Absent
	 * (test engine): `trigger(..., { tx })` is a `ConfigError`.
	 */
	createRunInTransaction?(tx: TransactionClient, input: NewRun): Promise<CreateRunResult>;
	/** Called after `queues.scale()` changed a worker's concurrency (pool sizing warning). */
	onScaled?(): void;
	/**
	 * A backend loaded on demand (`transport: "bullmq"`, decision 14 amendment): loads it, so that
	 * `createPubSub()` (synchronous) can be called. Awaited by `runs.subscribe` before it creates the
	 * subscriber; `start()` loads it in its first step. Absent: always loaded.
	 */
	load?(): Promise<void>;
}

/** Lifecycle state owned by the engine wrapping the core. @internal */
export interface EngineCoreLifecycle {
	/** True once `stop()` began (see `ExecutorDeps.isShuttingDown`). */
	isShuttingDown(): boolean;
	/** Throws once the engine was stopped. */
	assertNotStopped(): void;
	/**
	 * Throws only once the engine is fully stopped (its resources closed), not while `stop()` is
	 * still draining: reads, `runs.cancel` and `tokens.complete` keep working during a graceful
	 * shutdown (e.g. a webhook completing a token). Omitted: never throws.
	 */
	assertNotClosed?(): void;
}

/** The shared part of an engine (see {@link EngineBackend}). @internal */
export interface EngineCore {
	deps: ExecutorDeps;
	active: ActiveExecutions;
	notifier: RunNotifier;
	/** Publishes the status and emits the local event (best-effort). */
	notifyStatus(info: RunStatusInfo): void;
	/** Aborts local executions of runs canceled elsewhere. */
	abortLocal(runIds: readonly string[]): void;
	trigger: Engine["trigger"];
	runs: EngineRuns;
	tokens: EngineTokens;
	schedules: EngineSchedules;
	events: EngineEvents;
	queues: EngineQueues;
	/**
	 * Decision 27: re-applies every persisted queue pause state to the transport; returns how many
	 * queues had to change. Throws on a store/transport failure.
	 */
	syncQueueStates(): Promise<number>;
	/** The subscriber side, created on first use. */
	getPubSub(): PubSub;
	/** The subscriber side if it was already created. */
	currentPubSub(): PubSub | undefined;
	/** Forgets the subscriber side (a failed start() closes it); returns it, if any. */
	discardPubSub(): PubSub | undefined;
	/**
	 * Ends every open `runs.subscribe` iteration: quietly (`stop()`), or, given `error`, by making
	 * each iteration throw it (its subscriber was discarded, so it ended without a terminal status).
	 */
	endSubscriptions(error?: Error): void;
	/** Bounded wait for the best-effort publishes still in flight. */
	drainNotifications(timeoutMs: number): Promise<void>;
}

/**
 * Builds the shared part of an engine on top of `backend`. `resolved` must come from
 * `resolveConfig` and `registry` from `buildRegistry` + {@link validateTasks}. @internal
 */
export function createEngineCore(
	resolved: ResolvedConfig,
	registry: Registry,
	backend: EngineBackend,
	lifecycle: EngineCoreLifecycle
): EngineCore {
	const { logger } = resolved;
	const { store, transport, publisher } = backend;
	const { assertNotStopped } = lifecycle;
	const assertNotClosed = lifecycle.assertNotClosed ?? (() => {});
	const active = new ActiveExecutions();

	// --- Pub/sub (Phase 6 cancel broadcast, Phase 7 status seam). Publishing uses the shared
	// connection, so a trigger-only engine (decision 19) still publishes what it causes; only a
	// started engine subscribes (dedicated connection, decision 14). ---
	let pubsub: PubSub | undefined;
	const pendingNotifications = new Set<Promise<void>>();
	function trackNotification(promise: Promise<void>, what: string): void {
		const tracked = promise.then(
			() => undefined,
			(error: unknown) => logger.debug({ err: error }, `best-effort ${what} publish failed`)
		);
		pendingNotifications.add(tracked);
		void tracked.finally(() => pendingNotifications.delete(tracked));
	}
	// --- Local events (Phase 7): emitted where this process made the transition. ---
	const eventHandlers = new Map<RunEventName, Set<RunEventHandler>>();
	function emitEvent(transition: RunTransition, info: RunStatusInfo): void {
		const name: RunEventName = `run:${transition}`;
		const handlers = eventHandlers.get(name);
		if (!handlers || handlers.size === 0) return;
		const event: RunEvent = {
			name,
			runId: info.runId,
			taskId: info.taskId,
			status: statusOfTransition(transition),
			attempt: info.attempt,
			error: info.error,
			updatedAt: info.updatedAt,
			...(info.startedAt !== undefined ? { startedAt: info.startedAt } : {}),
			...(info.durationMs !== undefined ? { durationMs: info.durationMs } : {}),
		};
		const onError = (error: unknown) =>
			logger.error({ err: error, event: name, runId: info.runId }, "engine.events handler failed");
		for (const handler of Array.from(handlers)) {
			try {
				const result = handler(event);
				if (result instanceof Promise) result.catch(onError);
			} catch (error) {
				onError(error);
			}
		}
	}
	const events: EngineEvents = {
		on(event, handler) {
			if (!RUN_EVENT_NAMES.has(event)) {
				throw new ConfigError(
					`unknown event "${String(event)}"; expected one of ${[...RUN_EVENT_NAMES].join(", ")}`
				);
			}
			let handlers = eventHandlers.get(event);
			if (!handlers) {
				handlers = new Set();
				eventHandlers.set(event, handlers);
			}
			// A wrapper per registration: the same function registered twice gets two calls and
			// two independent unsubscribes.
			const registered: RunEventHandler = e => handler(e);
			handlers.add(registered);
			return () => {
				handlers.delete(registered);
			};
		},
	};

	/**
	 * Publishes the status for `runs.subscribe` (other processes included) and emits the local
	 * event, entirely from `info` (Phase 7 perf: no `runs.get` per transition; `updatedAt` comes
	 * from the store result, decision 17).
	 */
	function notifyStatus(info: RunStatusInfo): void {
		trackNotification(
			(async () => {
				emitEvent(info.transition ?? transitionOfStatus(info.status), info);
				await publisher.publishStatus({
					runId: info.runId,
					status: info.status,
					updatedAt: info.updatedAt.toISOString(),
				});
			})(),
			"run status"
		);
	}
	/**
	 * `ctx.progress.set` (decision 23): publishes the new value for `runs.subscribe`, as a
	 * `running`-status message (the write only ever happens while the run is `running`, decision
	 * 5). No local event: progress isn't a transition (`RunTransition`/`engine.events` stay as-is).
	 */
	function notifyProgress(runId: string, updatedAt: Date, value: Json | null): void {
		trackNotification(
			publisher.publishStatus({
				runId,
				status: "running",
				updatedAt: updatedAt.toISOString(),
				progress: value,
			}),
			"run progress"
		);
	}
	const notifier: RunNotifier = {
		status(info) {
			notifyStatus(info);
		},
		progress(runId, updatedAt, value) {
			notifyProgress(runId, updatedAt, value);
		},
		canceled(runIds) {
			if (runIds.length === 0) return;
			trackNotification(publisher.publishCancel(runIds), "cancel");
		},
	};
	/** Aborts local executions of runs canceled elsewhere (pub/sub) or by the reconciler. */
	function abortLocal(runIds: readonly string[]): void {
		for (const runId of runIds) active.get(runId)?.abort("canceled");
	}

	// --- Schedules (Phase 8, decisions 8, 9, 17, 21). `engine.schedules.*` works without start()
	// (the `-system` `Queue` already exists once the transport is built); only the tick HANDLER
	// (which needs the `-system` worker) and the declarative sync require it. ---
	const schedules: EngineSchedules = createSchedulesApi({
		store,
		transport,
		registry,
		logger,
		maxPayloadBytes: resolved.limits.maxPayloadBytes,
		assertNotStopped,
		assertNotClosed,
	});

	const deps: ExecutorDeps = {
		store,
		transport,
		registry,
		config: resolved,
		active,
		isShuttingDown: lifecycle.isShuttingDown,
		notify: notifier,
		streams: backend.streams,
	};

	/**
	 * The status/cancel subscriber, created by `start()` or by the first `runs.subscribe` (a
	 * trigger-only engine can subscribe too). Closed by `stop()`.
	 */
	function getPubSub(): PubSub {
		pubsub ??= backend.createPubSub();
		return pubsub;
	}
	/**
	 * Ends every open `runs.subscribe` iteration: quietly on `stop()`, or with the given error
	 * (thrown by the iteration) when its subscriber was discarded by a failed start().
	 */
	const openSubscriptions = new Set<(error?: Error) => void>();

	type CreateRunOptions = {
		delayMs: number;
		idempotency?: { key: string; ttlMs: number | null } | undefined;
		debounce?: NewRunDebounce | undefined;
		concurrencyKey?: string | undefined;
		tx?: TransactionClient | undefined;
	};

	async function createRun(
		task: AnyTask,
		payload: unknown,
		options: CreateRunOptions
	): Promise<RunHandle<never>> {
		const { id, taskId } = await createRunDetailed(task, payload, options);
		return { id, taskId };
	}

	/** {@link createRun}, also telling whether the run was created (`runs.retryFailed`). */
	async function createRunDetailed(
		task: AnyTask,
		payload: unknown,
		options: CreateRunOptions
	): Promise<{ id: string; taskId: string; kind: CreateRunResult["kind"] }> {
		assertSerializable(payload, {
			label: `payload of task "${task.id}"`,
			maxBytes: resolved.limits.maxPayloadBytes,
		});
		const input: NewRun = {
			id: randomUUID(),
			taskId: task.id,
			queue: taskQueueName(task),
			payload: payload as Json,
			maxAttempts: childTaskSpec(task).maxAttempts,
			delayMs: options.delayMs,
			...(options.idempotency ? { idempotency: options.idempotency } : {}),
			...(options.debounce ? { debounce: options.debounce } : {}),
			...(options.concurrencyKey !== undefined ? { concurrencyKey: options.concurrencyKey } : {}),
		};
		if (options.tx !== undefined) {
			// Decision 26: nothing is enqueued or published here (the caller's transaction is not
			// committed); the engine that claims the run after the commit does both.
			const result = await createRunInCallerTransaction(options.tx, input);
			// Only `id` (plus the result `kind`) is read from the tx-path result here, and that is
			// intentional, not incidental: an ORM adapter (decision 26) can normalize a raw query's
			// rows in ways that lose fidelity outside that one field -- e.g. knex's
			// `postProcessResponse` camelCasing the rows (dropping `task_id`), `rowCount` (dropped by
			// adapters whose ORM returns a plain row array, `toQueryResult` in
			// src/adapters/shared.ts) and timestamp columns (some adapters' drivers return them as
			// strings, not `Date`). `taskId` comes from the task itself. Do not start reading other
			// fields (e.g. `updatedAt`) off a tx-path `result.run` without checking every adapter
			// handles that column consistently.
			return { id: result.run.id, taskId: task.id, kind: result.kind };
		}
		const result = await store.runs.create(input);
		// Decision 9: only a newly created run is enqueued; an existing one is returned as-is.
		if (result.kind === "created") {
			notifier.status({
				runId: result.run.id,
				taskId: result.run.taskId,
				status: result.run.status,
				attempt: result.run.attempt,
				error: result.run.error,
				updatedAt: result.run.updatedAt,
			});
			await enqueue(deps, result.enqueue);
		} else if (result.kind === "debounced" && result.enqueue) {
			// Decision 24: the pending run stays `queued` (nothing is published); its seq was
			// bumped, so enqueue the new job first, then drop the stale one (best-effort: a
			// redelivery of it is rejected by `acquire`'s seq check anyway). Audit item 1: when
			// `run_after` didn't actually change (e.g. already pinned at the debounce deadline),
			// `enqueue` is null and there is nothing to (re)schedule or remove -- the job already
			// enqueued for the unchanged `run_after`/seq is still exactly right.
			await enqueue(deps, result.enqueue);
			await transport.removeRunJob(result.run.queue, result.run.id, result.staleSeq);
		}
		return { id: result.run.id, taskId: result.run.taskId, kind: result.kind };
	}

	async function createRunInCallerTransaction(
		tx: TransactionClient,
		input: NewRun
	): Promise<CreateRunResult> {
		const create = backend.createRunInTransaction;
		if (!create) {
			throw new ConfigError(
				"trigger(..., { tx }) is not supported by this engine (createTestEngine has no Postgres " +
					"transaction to join): trigger without `tx` in tests, e.g. behind a function that " +
					"takes the transaction and is called only in production"
			);
		}
		return create(tx, input);
	}

	async function trigger<T extends AnyTask>(
		taskArg: T,
		...args: PayloadArgs<TaskPayload<T>, [options?: TriggerOptions]>
	): Promise<RunHandle<TaskOutput<T>>> {
		assertNotStopped();
		const [rawPayload, options = {}] = args as [unknown, TriggerOptions | undefined];
		// A task that takes no payload is triggered without one; store it as JSON `null`.
		const payload = rawPayload === undefined ? null : rawPayload;
		const task = getTask(registry, taskArg);

		let idempotency: { key: string; ttlMs: number } | undefined;
		if (options.idempotencyKey !== undefined) {
			const key = validateIdempotencyKey(options.idempotencyKey);
			const ttlMs =
				options.idempotencyKeyTtl === undefined
					? resolved.idempotencyKeyTtlMs
					: parseDuration(options.idempotencyKeyTtl);
			if (ttlMs <= 0) {
				throw new ConfigError(
					`trigger options.idempotencyKeyTtl must resolve to more than 0 ms, got ${ttlMs} ms (parsed from ${describeOffendingValue(options.idempotencyKeyTtl)})`
				);
			}
			assertTtlWithinRetention(ttlMs, resolved.retentionDays, "idempotencyKeyTtl");
			idempotency = { key, ttlMs };
		} else if (options.idempotencyKeyTtl !== undefined) {
			throw new ConfigError("idempotencyKeyTtl was given without an idempotencyKey");
		}
		const delayMs = options.delay === undefined ? 0 : parseDuration(options.delay);
		const debounce =
			options.debounce === undefined ? undefined : resolveDebounce(options, idempotency);
		const concurrencyKey =
			options.concurrencyKey === undefined
				? undefined
				: resolveConcurrencyKey(registry, task, options.concurrencyKey);

		const tx = options.tx === undefined ? undefined : validateTransactionClient(options.tx);
		if (tx !== undefined && debounce !== undefined) {
			throw new ConfigError(
				"trigger options.tx cannot be combined with options.debounce: collapsing into the " +
					"pending run would lock that shared run (and block its workers and every other " +
					"trigger of the key) until your transaction ends; trigger with debounce after COMMIT"
			);
		}

		return createRun(task, payload, { delayMs, idempotency, debounce, concurrencyKey, tx });
	}

	/** Bounded wait for the best-effort publishes still in flight. */
	async function drainNotifications(timeoutMs: number): Promise<void> {
		if (pendingNotifications.size === 0) return;
		let timer: ReturnType<typeof setTimeout> | undefined;
		await Promise.race([
			Promise.allSettled(Array.from(pendingNotifications)),
			new Promise<void>(resolve => {
				timer = setTimeout(resolve, timeoutMs);
			}),
		]);
		clearTimeout(timer);
	}

	/**
	 * `runs.cancel()` of one run, for `cancelMany` (decision 5/10 semantics unchanged: store cancel,
	 * local aborts + pub/sub, parent resume, expire-job cleanup). A deadlock (only possible when
	 * something outside the documented lock order touches the same runs) is retried once.
	 *
	 * Item 2: never throws -- an unexpected (non-deadlock) error canceling one run must not discard
	 * `mapBounded`'s results for the others in the same page, so it's caught here and reported back
	 * as a `"failed"` outcome instead of propagating.
	 */
	async function cancelOneOfBatch(
		runId: string
	): Promise<{ kind: "canceled"; ids: string[] } | { kind: "failed"; error: BatchRunError }> {
		const reason = "canceled via engine.runs.cancelMany()";
		try {
			const result = await retryOnDeadlock(() => store.runs.cancel(runId, reason));
			if (result.kind !== "canceled") return { kind: "canceled", ids: [] };
			await cancelLocally({ transport, active, notify: notifier }, result.canceled);
			await afterResolution(deps, result.parent, result.expireJobsToRemove);
			return { kind: "canceled", ids: result.canceled.map(c => c.runId) };
		} catch (error) {
			return { kind: "failed", error: batchRunError(error) };
		}
	}

	/** Decision 25: a replay/retry keeps the original's concurrency key while the queue declares one. */
	function replayedConcurrencyKey(task: AnyTask, record: RunRecord): string | undefined {
		return record.concurrencyKey !== null &&
			registry.queues.get(taskQueueName(task))?.concurrencyKey !== undefined
			? record.concurrencyKey
			: undefined;
	}

	async function retryOneOfBatch(
		record: RunRecord
	): Promise<{ newRunId: string } | { reason: RetrySkipReason }> {
		const task = registry.tasks.get(record.taskId);
		if (!task) return { reason: "task_not_registered" };
		let created: { id: string; taskId: string; kind: CreateRunResult["kind"] };
		try {
			created = await createRunDetailed(task, record.payload, {
				delayMs: 0,
				idempotency: { key: retryIdempotencyKey(record.id), ttlMs: null },
				concurrencyKey: replayedConcurrencyKey(task, record),
			});
		} catch (error) {
			// Item 2: a bad stored payload (or other per-run ConfigError, e.g. a queue that no
			// longer declares the concurrency key it was created with) would throw the same way on
			// every future call too -- skip it instead of failing the whole batch.
			if (error instanceof SerializationError || error instanceof ConfigError) {
				return { reason: "invalid_payload" };
			}
			throw error;
		}
		return created.kind === "created" ? { newRunId: created.id } : { reason: "already_retried" };
	}

	// --- Queue control (decision 27). ---
	const systemQueue = systemQueueName(resolved.prefix);

	async function applyQueuePause(queue: string, paused: boolean): Promise<void> {
		if (paused) await transport.pauseQueue(queue);
		else await transport.resumeQueue(queue);
	}

	async function setQueuePaused(name: unknown, paused: boolean): Promise<QueueState> {
		assertNotStopped();
		const method = paused ? "queues.pause" : "queues.resume";
		if (typeof name !== "string") {
			throw new ConfigError(
				`${method}: the queue name must be a string, got ${describeOffendingValue(name)}`
			);
		}
		const queue = validateQueueName(name);
		if (queue === systemQueue) {
			throw new ConfigError(`${method}: the internal system queue "${queue}" cannot be paused`);
		}
		if (!registry.queues.has(queue) && (await store.queues.get(queue)) === null) {
			throw new ConfigError(
				`${method}: unknown queue "${queue}"; this engine's tasks use ` +
					`${[...registry.queues.keys()].map(q => `"${q}"`).join(", ") || "no queue"}`
			);
		}
		// Postgres first (the source of truth, decision 1), then the Redis flag. A failed Redis call
		// only warns: the leader's reconciler tick and every start() re-apply the persisted state.
		const { record } = await store.queues.setPaused(queue, paused);
		try {
			await applyQueuePause(queue, paused);
			// A concurrent pause/resume (maybe from another process) may have committed after our
			// write and applied its flag before ours: converge on what Postgres holds now.
			const current = await store.queues.get(queue);
			if (current && current.paused !== paused) await applyQueuePause(queue, current.paused);
		} catch (error) {
			logger.warn(
				{ err: error, queue, paused },
				`${method}: the state is saved in Postgres, but applying it to BullMQ failed; the ` +
					"leader's next reconciler tick (or the next start()) applies it"
			);
		}
		return { name: queue, paused: record.paused, updatedAt: record.updatedAt };
	}

	async function syncQueueStates(): Promise<number> {
		let fixed = 0;
		for (const row of await store.queues.list()) {
			if (row.queue === systemQueue) continue;
			if ((await transport.isQueuePaused(row.queue)) === row.paused) continue;
			// Review 3 item 4: `row` may be stale -- a queues.pause()/resume() may have committed
			// (and set its Redis flag) since the list above. Re-read this queue's row right before
			// applying, so a concurrent pause is never undone.
			const current = await store.queues.get(row.queue);
			if (current === null) continue;
			if ((await transport.isQueuePaused(row.queue)) === current.paused) continue;
			await applyQueuePause(row.queue, current.paused);
			// Like setQueuePaused: a write that committed after our re-read may have applied its
			// flag before ours; converge on what Postgres holds now.
			const after = await store.queues.get(row.queue);
			if (after && after.paused !== current.paused) await applyQueuePause(row.queue, after.paused);
			fixed++;
			logger.info(
				{ queue: row.queue, paused: after?.paused ?? current.paused },
				"queue pause state re-applied to BullMQ from Postgres"
			);
		}
		return fixed;
	}

	const queues: EngineQueues = {
		pause: name => setQueuePaused(name, true),
		resume: name => setQueuePaused(name, false),
		async list(): Promise<QueueInfo[]> {
			assertNotClosed();
			const states = new Map((await store.queues.list()).map(r => [r.queue, r]));
			const activeByQueue = new Map<string, number>();
			for (const execution of active.values()) {
				const queue = execution.run.queue;
				activeByQueue.set(queue, (activeByQueue.get(queue) ?? 0) + 1);
			}
			const names = [...new Set([...registry.queues.keys(), ...states.keys()])]
				.filter(name => name !== systemQueue)
				.sort();
			return names.map((name): QueueInfo => {
				const declared = registry.queues.get(name);
				const state = states.get(name);
				return {
					name,
					paused: state?.paused ?? false,
					updatedAt: state?.updatedAt ?? null,
					declared: declared !== undefined,
					declaredConcurrency: declared?.concurrency ?? null,
					concurrency: transport.workerConcurrency(name) ?? null,
					activeLocal: activeByQueue.get(name) ?? 0,
				};
			});
		},
		async scale(name, concurrency) {
			assertNotStopped();
			if (
				!Number.isInteger(concurrency) ||
				concurrency < 1 ||
				concurrency > MAX_QUEUE_SCALE_CONCURRENCY
			) {
				throw new ConfigError(
					`queues.scale: concurrency must be an integer between 1 and ${MAX_QUEUE_SCALE_CONCURRENCY}, got ${describeOffendingValue(concurrency)}`
				);
			}
			if (typeof name !== "string" || !transport.setWorkerConcurrency(name, concurrency)) {
				throw new ConfigError(
					`queues.scale: this process runs no worker for queue ${describeOffendingValue(name)}; ` +
						"scale applies to this process's own workers: call it after start() on an engine " +
						"that started that queue"
				);
			}
			logger.info({ queue: name, concurrency }, "queue worker concurrency changed in this process");
			backend.onScaled?.();
		},
	};

	const runs: EngineRuns = {
		async get<O>(run: string | RunHandle<O>): Promise<Run<O> | null> {
			assertNotClosed();
			const runId = runIdOf(run);
			// Not a UUID: no such run (the SQL cast would throw instead), like logs/steps/tokens.
			if (!isUuid(runId)) return null;
			const record = await store.runs.get(runId);
			return record ? toPublicRun<O>(record) : null;
		},

		async list(options: ListRunsOptions = {}): Promise<RunList> {
			assertNotClosed();
			const result = await store.runs.list({
				limit: validLimit("runs.list", options.limit, DEFAULT_LIST_LIMIT),
				...(options.taskId !== undefined
					? { taskId: validFilterText("runs.list options.taskId", options.taskId) }
					: {}),
				...(options.status !== undefined
					? { status: validStatusFilter("runs.list options.status", options.status) }
					: {}),
				...(options.createdFrom !== undefined ? { createdFrom: options.createdFrom } : {}),
				...(options.createdTo !== undefined ? { createdTo: options.createdTo } : {}),
				...(options.cursor !== undefined ? { cursor: options.cursor } : {}),
			});
			return { items: result.items.map(r => toPublicRun(r)), nextCursor: result.nextCursor };
		},

		async cancel(run: string | RunHandle<unknown>): Promise<CancelRunOutcome> {
			assertNotClosed();
			const runId = runIdOf(run);
			if (!isUuid(runId)) return "not_found";
			const result = await store.runs.cancel(runId, "canceled via engine.runs.cancel()");
			if (result.kind !== "canceled") return result.kind;

			// Aborts local executions and publishes the cancel for the other processes' (pub/sub).
			// Either way ownership is already gone, so a zombie can't write anything.
			await cancelLocally({ transport, active, notify: notifier }, result.canceled);
			await afterResolution(deps, result.parent, result.expireJobsToRemove);
			return "canceled";
		},

		async replay<O>(run: string | RunHandle<O>): Promise<RunHandle<O>> {
			assertNotStopped();
			const runId = runIdOf(run);
			const record = isUuid(runId) ? await store.runs.get(runId) : null;
			if (!record) throw new MinitriggerError(`run ${runId} not found`);
			const task = getTask(registry, record.taskId);
			return createRun(task, record.payload, {
				delayMs: 0,
				// Decision 25: the replay keeps the original's concurrency key while the queue still
				// declares one (never the debounce key: a replay is an explicit extra run).
				concurrencyKey: replayedConcurrencyKey(task, record),
			});
		},

		async cancelMany(filter: BatchRunsFilter): Promise<CancelManyResult> {
			assertNotStopped();
			const page = await store.runs.listBatchCandidates({
				purpose: "cancel",
				...validBatchFilter("runs.cancelMany", filter),
			});
			const perRun = await mapBounded(page.items, BATCH_CONCURRENCY, r => cancelOneOfBatch(r.id));
			const canceled: string[] = [];
			const failed: CancelManyResult["failed"] = [];
			page.items.forEach((record, i) => {
				const outcome = perRun[i];
				if (!outcome) return;
				if (outcome.kind === "canceled") canceled.push(...outcome.ids);
				else failed.push({ runId: record.id, error: outcome.error });
			});
			return {
				canceled,
				failed,
				hasMore: page.nextCursor !== null,
				nextCursor: page.nextCursor,
			};
		},

		async retryFailed(filter: BatchRunsFilter): Promise<RetryFailedResult> {
			assertNotStopped();
			const page = await store.runs.listBatchCandidates({
				purpose: "retry",
				...validBatchFilter("runs.retryFailed", filter),
			});
			const outcomes = await mapBounded(page.items, BATCH_CONCURRENCY, retryOneOfBatch);
			const result: RetryFailedResult = {
				retried: [],
				skipped: [],
				hasMore: page.nextCursor !== null,
				nextCursor: page.nextCursor,
			};
			page.items.forEach((record, i) => {
				const outcome = outcomes[i];
				if (!outcome) return;
				if ("newRunId" in outcome) {
					result.retried.push({ runId: record.id, newRunId: outcome.newRunId });
				} else {
					result.skipped.push({ runId: record.id, reason: outcome.reason });
				}
			});
			return result;
		},

		async logs(run, options = {}): Promise<RunLogPage> {
			assertNotClosed();
			const limit = clampLimit(
				validLimit("runs.logs", options.limit, DEFAULT_LOGS_LIMIT),
				MAX_LOGS_LIMIT
			);
			const afterId = options.afterId;
			if (afterId != null && (typeof afterId !== "string" || !/^\d{1,19}$/.test(afterId))) {
				throw new ConfigError(
					`runs.logs options.afterId must be the \`id\` of a previous log entry (a string of digits), got ${describeOffendingValue(afterId)}`
				);
			}
			const runId = runIdOf(run);
			if (!isUuid(runId)) return { items: [], nextAfterId: null };
			const entries = await store.logs.list(runId, {
				limit,
				...(afterId != null ? { afterId } : {}),
			});
			const items = entries.map(
				(e): RunLogEntry => ({
					id: e.id,
					level: e.level,
					message: e.message,
					data: e.data,
					ts: e.ts,
				})
			);
			const last = items[items.length - 1];
			return { items, nextAfterId: items.length === limit && last ? last.id : null };
		},

		async steps(run): Promise<RunStep[]> {
			assertNotClosed();
			const runId = runIdOf(run);
			if (!UUID_PATTERN.test(runId)) return [];
			const records = await store.steps.list(runId);
			return records.map(r => ({
				name: r.name,
				seq: r.seqIndex,
				kind: r.kind,
				status: r.status,
				output: r.output,
				wakeAt: r.wakeAt,
				createdAt: r.createdAt,
			}));
		},

		subscribe(run, options = {}): AsyncIterable<RunStatusUpdate> {
			assertNotStopped();
			return subscribeRun(runIdOf(run), options.signal);
		},

		readStream<T>(
			run: string | RunHandle<unknown>,
			name: string,
			options: ReadStreamOptions = {}
		): AsyncIterable<T> {
			assertNotStopped();
			return backend.readStream(runIdOf(run), name, options.signal) as AsyncIterable<T>;
		},
	};

	/**
	 * `runs.subscribe` (Phase 7), in the mandatory order: (1) register on the pub/sub channel and
	 * buffer messages, (2) read the status from Postgres and emit it, (3) emit buffered/live
	 * messages only when newer than the last emitted `updatedAt`, (4) end after a terminal status.
	 */
	async function* subscribeRun(
		runId: string,
		signal: AbortSignal | undefined
	): AsyncGenerator<RunStatusUpdate, void, undefined> {
		if (!UUID_PATTERN.test(runId) || signal?.aborted) return;
		if (backend.load) {
			// A backend loaded on demand must be loaded before `getPubSub()` (synchronous). A stop()
			// begun meanwhile ends the iteration quietly, as it would have ended it right after.
			await backend.load();
			if (signal?.aborted || lifecycle.isShuttingDown()) return;
		}
		const pending: RunStatusMessage[] = [];
		let wake: (() => void) | undefined;
		let ended = false;
		/** Review 4 item 5: why the iteration was ended from outside, when not quietly. */
		let endError: Error | undefined;
		const notify = () => {
			const resolve = wake;
			wake = undefined;
			resolve?.();
		};
		const endWith = (error?: Error) => {
			if (!ended) endError = error;
			ended = true;
			notify();
		};
		// The abort listener receives the Event: never pass it through as an error.
		const end = () => endWith();
		/** An ended iteration returns quietly, or throws the error it was ended with. */
		const finish = (): void => {
			if (endError) throw endError;
		};
		const ps = getPubSub();
		// (1) Before the read, so nothing published after it can be missed.
		const unsubscribe = ps.subscribeStatus(runId, message => {
			pending.push(message);
			notify();
		});
		signal?.addEventListener("abort", end, { once: true });
		openSubscriptions.add(endWith);
		try {
			await ps.ready;
			if (ended) return finish();
			// (2) Postgres is the source of truth for the starting point.
			const row = await store.runs.get(runId);
			if (ended) return finish();
			if (!row) return;
			let last: RunStatusUpdate = {
				runId,
				status: row.status,
				updatedAt: row.updatedAt,
				progress: row.progress,
			};
			yield last;
			if (isTerminal(last.status)) return;

			// (3) Only strictly newer changes; see `isNewerRunUpdate` for the same-millisecond rules
			// (status differs, or -- decision 23 -- only the progress value differs).
			while (!ended) {
				const message = pending.shift();
				if (message) {
					const updatedAt = new Date(message.updatedAt);
					if (
						Number.isNaN(updatedAt.getTime()) ||
						!isNewerRunUpdate(last, {
							status: message.status,
							updatedAt,
							progress: message.progress,
						})
					) {
						continue;
					}
					last = {
						runId,
						status: message.status,
						updatedAt,
						progress: message.progress !== undefined ? message.progress : last.progress,
					};
					yield last;
					// (4)
					if (isTerminal(last.status)) return;
					continue;
				}
				const timedOut = await new Promise<boolean>(resolve => {
					const timer = setTimeout(() => resolve(true), SUBSCRIBE_POLL_MS);
					wake = () => {
						clearTimeout(timer);
						resolve(false);
					};
				});
				if (ended) break;
				if (!timedOut) continue;
				wake = undefined;
				// Safety net: pub/sub may drop messages (e.g. on a reconnect).
				const current = await store.runs.get(runId);
				if (!current) return;
				if (
					isNewerRunUpdate(last, {
						status: current.status,
						updatedAt: current.updatedAt,
						progress: current.progress,
					})
				) {
					pending.push({
						runId,
						status: current.status,
						updatedAt: current.updatedAt.toISOString(),
						progress: current.progress,
					});
				}
			}
			finish();
		} finally {
			unsubscribe();
			signal?.removeEventListener("abort", end);
			openSubscriptions.delete(endWith);
		}
	}

	const tokens: EngineTokens = {
		async complete(tokenId: string, output?: unknown): Promise<CompleteTokenOutcome> {
			assertNotClosed();
			const value = output === undefined ? null : output;
			assertSerializable(value, {
				label: "output of tokens.complete()",
				maxBytes: resolved.limits.maxPayloadBytes,
			});
			if (typeof tokenId !== "string" || !UUID_PATTERN.test(tokenId)) {
				return { completed: false, reason: "not_found" };
			}
			const result = await store.waitpoints.completeToken(tokenId, value as Json);
			switch (result.kind) {
				case "not_found":
					return { completed: false, reason: "not_found" };
				case "run_finished":
					return { completed: false, reason: "run_finished" };
				case "not_pending":
					return {
						completed: false,
						reason: result.status === "pending" ? "completed" : result.status,
					};
				case "completed":
					await afterResolution(deps, result.resolution, []);
					return { completed: true };
			}
		},
	};

	return {
		deps,
		active,
		notifier,
		notifyStatus,
		abortLocal,
		trigger,
		runs,
		tokens,
		schedules,
		events,
		queues,
		syncQueueStates,
		getPubSub,
		currentPubSub: () => pubsub,
		discardPubSub() {
			const current = pubsub;
			pubsub = undefined;
			return current;
		},
		endSubscriptions(error?: Error) {
			for (const end of Array.from(openSubscriptions)) end(error);
		},
		drainNotifications,
	};
}

/** `import()`/`require()` codes for a module that could not be resolved. */
const MODULE_NOT_FOUND_CODES: ReadonlySet<unknown> = new Set([
	"ERR_MODULE_NOT_FOUND",
	"MODULE_NOT_FOUND",
]);
/** `Cannot find package 'ioredis' imported from ...` / `Cannot find module 'bullmq'`. */
const MISSING_PEER_PATTERN = /['"](?:bullmq|ioredis)(?:\/[^'"]*)?['"]/;

/**
 * Whether `error` (or an error in its `cause` chain) says `bullmq` or `ioredis` itself could not
 * be resolved -- not a file inside them, not another package.
 */
function isMissingOptionalPeer(error: unknown): boolean {
	let current: unknown = error;
	for (let depth = 0; depth < 5 && current instanceof Error; depth++) {
		const code = (current as { code?: unknown }).code;
		if (MODULE_NOT_FOUND_CODES.has(code) && MISSING_PEER_PATTERN.test(current.message)) {
			return true;
		}
		current = current.cause;
	}
	return false;
}

/**
 * Loads `src/backends/bullmq.ts` (its own chunk: the dynamic `import()` keeps it, and with it
 * `bullmq`/`ioredis`, out of the static graph of every entry). A missing optional peer is a
 * `ConfigError` with the install hint; any other load error is rethrown as is.
 */
async function loadBullmqBackendFactory(): Promise<BackendFactory> {
	try {
		const module = await import("./backends/bullmq.js");
		return module.createBullmqBackend;
	} catch (error) {
		if (!isMissingOptionalPeer(error)) throw error;
		throw new ConfigError(
			'transport "bullmq" needs the optional peers bullmq and ioredis: pnpm add bullmq ioredis ' +
				'(or use transport: "postgres")',
			{ cause: error }
		);
	}
}

export function createEngine(config: EngineConfig): Engine {
	const resolved = resolveConfig(config);
	const { logger } = resolved;
	const registry = buildRegistry(resolved.tasks);
	validateTasks(registry, resolved);

	const ownsPool = typeof resolved.postgres === "string";
	const allQueues = Array.from(registry.queues.values());
	const pool =
		typeof resolved.postgres === "string"
			? new Pool({
					connectionString: resolved.postgres,
					// Low (b): sized by the README guide unless given, and a bounded wait for a free
					// connection, so a heartbeat on a saturated pool fails with a warn (decision 6)
					// instead of waiting forever.
					max:
						resolved.poolMax ??
						derivePoolMax(allQueues, resolved.systemConcurrency, resolved.transport),
					connectionTimeoutMillis: resolved.poolConnectionTimeoutMs,
				})
			: resolved.postgres.pool;
	if (ownsPool) {
		// An idle client error would otherwise crash the process (unhandled "error" event).
		pool.on("error", error => logger.error({ err: error }, "postgres pool error"));
	}

	const store = createSqlStore(pool, resolved.schema);

	/** Set synchronously by `stop()`; `executeRun` hands back any run it acquires after that. */
	let shuttingDown = false;
	let lifecycle: "idle" | "starting" | "started" | "stopping" = "idle";
	let startPromise: Promise<void> | undefined;
	let stopPromise: Promise<void> | undefined;

	function assertNotStopped(): void {
		if (lifecycle === "stopping") {
			throw new MinitriggerError("this engine was stopped; create a new one");
		}
	}
	/** Set once stop() finished closing everything (see `EngineCoreLifecycle.assertNotClosed`). */
	let closed = false;
	function assertNotClosed(): void {
		if (closed) throw new MinitriggerError("this engine was stopped; create a new one");
	}

	/**
	 * The core, bound right after the backend (the backend context's `isActiveLocally` and
	 * `notifyStatus` need it; the core needs the backend's `EngineBackend` first).
	 */
	let boundCore: EngineCore | undefined;
	const requireCore = (): EngineCore => {
		if (!boundCore) throw new MinitriggerError("internal: engine core used before it was built");
		return boundCore;
	};
	const backendContext: BackendContext = {
		resolved,
		registry,
		pool,
		ownsPool,
		store,
		logger,
		isShuttingDown: () => shuttingDown,
		// Review focus 6: the Postgres cancel re-read only looks at runs executing here.
		isActiveLocally: runId => requireCore().active.get(runId) !== undefined,
		notifyStatus: info => requireCore().notifyStatus(info),
	};
	// Decision 14 amendment: the BullMQ backend (the only module reaching `bullmq`/`ioredis`) is
	// loaded on first use, so an engine on `transport: "postgres"` -- and the main entry and
	// `/testing` themselves -- never load those optional peers.
	const runtime: BackendRuntime =
		resolved.transport === "postgres"
			? createPostgresBackend(backendContext)
			: createLazyBackend(backendContext, loadBullmqBackendFactory);
	const { transport } = runtime;

	const core = createEngineCore(resolved, registry, runtime.backend, {
		isShuttingDown: () => shuttingDown,
		assertNotStopped,
		assertNotClosed,
	});
	boundCore = core;
	const { deps, active, notifier, notifyStatus, abortLocal } = core;

	/** Review item (c): does another schema of this database still own that schedule? */
	const stateOfForeignSchedule = (foreignSchema: string, scheduleId: string) =>
		foreignScheduleState(pool, foreignSchema, scheduleId);
	/** Review item 3: see {@link BackendRuntime.isPrefixOwner}. */
	const isPrefixOwner = () => runtime.isPrefixOwner();

	// --- Leader loop (Phase 6, decision 16): reconciler + retention scheduling, leader only. ---
	const reconciler = createReconciler({
		store,
		transport,
		logger,
		graceMs: resolved.reconciler.graceMs,
		stallTimeoutMs: resolved.stallTimeoutMs,
		batchSize: resolved.reconciler.batchSize,
		// Decision 1 amendment: with transport "postgres" a due run is backlog, not a lost enqueue.
		recoverLostEnqueues: resolved.transport === "bullmq",
		reconcileQueues: core.syncQueueStates,
		reconcileSchedules: createReconcileSchedulesHook({
			store,
			transport,
			logger,
			schema: resolved.schema,
			foreignScheduleState: stateOfForeignSchedule,
			isPrefixOwner,
		}),
		onCanceled: canceled => {
			const runIds = canceled.map(c => c.runId);
			abortLocal(runIds);
			for (const c of canceled) {
				notifyStatus({
					runId: c.runId,
					taskId: c.taskId,
					status: "canceled",
					attempt: c.attempt,
					error: c.error,
					updatedAt: c.updatedAt,
					transition: "canceled",
				});
			}
			notifier.canceled(runIds);
		},
		// Stall recovery: back to `queued` means a retry (attempt + 1), otherwise it failed.
		onStatusChange: info =>
			notifyStatus({
				runId: info.runId,
				taskId: info.taskId,
				status: info.status,
				attempt: info.attempt,
				error: info.error,
				updatedAt: info.updatedAt,
				transition: info.status === "queued" ? "retrying" : "failed",
			}),
	});
	const retention = createRetention({
		store,
		logger,
		days: resolved.retentionDays,
		batchSize: resolved.retentionBatchSize,
	});
	let leader: LeaderElector | undefined;
	let leaderTimer: ReturnType<typeof setInterval> | undefined;
	let leaderTick: Promise<void> | undefined;
	let leaderLoopStopped = false;
	/** Set when leadership is lost: the next tick doesn't contend, giving healthier processes a turn. */
	let skipNextAcquire = false;
	/** Last retention window this process enqueued (the jobs are removed on completion). */
	let lastRetentionWindow: number | undefined;
	/** Every `executeRun` call still in progress (from the acquire to its last enqueue). */
	const inFlight = new Set<Promise<void>>();
	/** Aborted by `stop()`: ends a retention sweep between batches (low item c). */
	const shutdownController = new AbortController();
	/** Undoes the `onCancel` registration of a `start()` that failed after it. */
	let offCancel: (() => void) | undefined;

	const dispatchSystem = createSystemDispatcher({
		config: resolved,
		store,
		transport,
		active,
		notify: notifier,
		runRetention: () => retention.runOnce(shutdownController.signal),
		registry,
	});
	/**
	 * Audit item 8: a `schedule-tick` tagged with another schema (two schemas sharing this prefix)
	 * is refused before the tick handler could create a run here or remove the other schema's
	 * scheduler; the throw lets BullMQ retry it, possibly on the owning engine's worker.
	 * Review item (c): unless that schema was retired (schema, `schedules` table or row gone from
	 * this database): then its scheduler is removed once (`warn`) and the tick ends quietly, instead
	 * of failing 5 times per occurrence forever.
	 */
	const processSystem: typeof dispatchSystem = async (name, data, job) => {
		if (name === SCHEDULE_TICK_JOB_NAME) {
			const foreign = foreignScheduleTickSchema(data, resolved.schema);
			if (foreign !== undefined) {
				const dataId = (data as { scheduleId?: unknown }).scheduleId;
				const resolvedId =
					typeof job.repeatJobKey === "string"
						? job.repeatJobKey
						: typeof dataId === "string"
							? dataId
							: undefined;
				const scheduleId = resolvedId ?? String(job.id ?? "");
				if (resolvedId !== undefined) {
					try {
						const outcome = await removeRetiredForeignScheduler(
							{ transport, logger, foreignScheduleState: stateOfForeignSchedule, isPrefixOwner },
							resolvedId,
							foreign
						);
						if (outcome !== "live") return;
					} catch (error) {
						logger.warn(
							{ err: error, scheduleId, schema: foreign },
							"could not check whether another schema still owns this schedule; tick refused"
						);
					}
				}
				const error = new ScheduleTickSchemaMismatchError(scheduleId, foreign, resolved.schema);
				logger.error(
					{ err: error, scheduleId, schema: resolved.schema },
					"schedule tick of another schema sharing this prefix refused (nothing created or removed)"
				);
				throw error;
			}
		}
		return dispatchSystem(name, data, job);
	};

	function start(options: StartOptions = {}): Promise<void> {
		try {
			assertNotStopped();
		} catch (error) {
			return Promise.reject(error);
		}
		if (lifecycle !== "idle") {
			return Promise.reject(new MinitriggerError("engine.start() was already called"));
		}
		lifecycle = "starting";
		const attempt = (async () => {
			// Reachability, environment checks (decision 22) and namespace ownership, each followed
			// by `assertStartNotAbandoned()` (review 4 item 4).
			await runtime.beforeStart({ assertNotAbandoned: assertStartNotAbandoned });
			runtime.warnPoolSizing(options.queues);
			warnOutboxConfig();

			// Item 11: everything that can fail runs BEFORE the workers start, so a failed start()
			// leaves no worker behind and can be retried. The declarative sync only needs the
			// `-system` Queue (ticks produced meanwhile wait for the worker).
			// Phase 8, decision 21: create/update every `defineTask({ schedule })` in Postgres + BullMQ.
			await syncDeclarativeSchedules({
				store,
				transport,
				registry,
				logger,
				maxPayloadBytes: resolved.limits.maxPayloadBytes,
				pruneDeclarative: resolved.schedules.pruneDeclarative,
			});
			assertStartNotAbandoned();

			// Decision 27: a queue paused in Postgres is paused in BullMQ BEFORE any worker fetches
			// (Redis lost the flag on a FLUSHALL, or a pause's Redis call failed). A failure here
			// fails start() (retryable): starting workers could run jobs of a paused queue.
			await core.syncQueueStates();
			assertStartNotAbandoned();

			// Subscribed before the workers, so a cancel published for a run acquired right away is
			// not missed. `ready` never rejects.
			const createdPubSub = core.currentPubSub() === undefined;
			const ps = core.getPubSub();
			offCancel?.();
			offCancel = ps.onCancel(abortLocal);
			try {
				// Review 3 item 1: bounded (5 s); a timeout is a `MinitriggerError` (review 4 item 2).
				await runtime.awaitSubscriberReady(ps);
				assertStartNotAbandoned();
			} catch (error) {
				offCancel?.();
				offCancel = undefined;
				// Close what this attempt opened, so a later start() begins from a clean state.
				// Iterations sharing it end quietly when stop() caused this (as stop() ends them).
				if (createdPubSub) await discardPubSub(!shuttingDown);
				throw error;
			}

			try {
				// On failure `startWorkers` closes whatever workers it already created.
				await transport.startWorkers({
					...(options.queues ? { queues: options.queues } : {}),
					processRun: async (queue, data) => {
						const execution = executeRun(deps, {
							runId: data.runId,
							seq: data.seq,
							queue,
							// Postgres transport only: the run arrives already claimed (never over BullMQ).
							...(data.claimed ? { claimed: data.claimed } : {}),
						});
						inFlight.add(execution);
						try {
							await execution;
						} finally {
							inFlight.delete(execution);
						}
					},
					processSystem,
				});
			} catch (error) {
				offCancel?.();
				offCancel = undefined;
				// Review 3 item 1: `startWorkers` closed its partial workers, but their dedicated
				// connections belong to us (a `Worker` never closes a connection it was given).
				await runtime.onWorkersStartFailed();
				throw error;
			}

			// Review 3: stop() may have begun while the workers were starting (and may have stopped
			// waiting for this start()). Its `closeWorkers` closes every worker created before it
			// began, and the transport refuses to create one after; the outbox listener and the
			// leader loop must not start either.
			if (shuttingDown) {
				offCancel?.();
				offCancel = undefined;
				throw new MinitriggerError("engine.stop() was called while start() was in progress");
			}
			const afterWorkers = runtime.afterWorkersStarted();
			if (afterWorkers) {
				await afterWorkers;
				// stop() may have begun while it was pending: the leader loop must not start.
				assertStartNotAbandoned();
			}
			startLeaderLoop();
		})();
		startPromise = attempt.then(
			() => {
				if (lifecycle === "starting") lifecycle = "started";
			},
			(error: unknown) => {
				// Only "started" once everything is up; a failed start can be retried.
				if (lifecycle === "starting") lifecycle = "idle";
				throw error;
			}
		);
		return startPromise;
	}

	/**
	 * Review 4 item 4: stop() may have begun (and even resolved, giving up on this start() after its
	 * bounded wait) while a start() step was pending; every later step would then still write to
	 * Redis/Postgres (prefix claim, declarative schedules, queue states). Checked between steps.
	 */
	function assertStartNotAbandoned(): void {
		if (shuttingDown) {
			throw new MinitriggerError("engine.stop() was called while start() was in progress");
		}
	}

	/** Drops the subscriber created by a failed start() (Review 3 item 1). */
	async function discardPubSub(startFailed: boolean): Promise<void> {
		const ps = core.discardPubSub();
		// `runs.subscribe` iterations begun during this attempt took this same pub/sub (every one
		// begun before it would have created it first, and then it isn't discarded): end them
		// rather than leave them on a closed subscriber, noticing changes only via the re-read.
		// Review 4 item 5: they end with an error, not quietly: without a terminal status the caller
		// could not tell this apart from a subscription that simply finished.
		core.endSubscriptions(
			startFailed
				? new MinitriggerError(
						"runs.subscribe() ended before the run reached a terminal status: the pub/sub " +
							"subscriber it used was discarded because engine.start() failed; subscribe again " +
							"(after a successful start(), or on an engine that only triggers)"
					)
				: undefined
		);
		await ps?.close().catch(() => {});
		runtime.dropSubscriber();
	}

	/**
	 * Item 6: `outbox.notify: false` (decision 26) means every `trigger(..., { tx })` waits for the
	 * poll fallback (or a late catch-up sweep on reconnect) to be dispatched, instead of the
	 * near-instant `pg_notify` wake-up. Combined with a `pollIntervalMs` this high, that trade is
	 * very likely accidental (the option exists to avoid the commit-time NOTIFY lock, not to make
	 * dispatch slow), so this only warns -- the config is honored either way.
	 */
	function warnOutboxConfig(): void {
		if (resolved.outbox.notify) return;
		if (resolved.outbox.pollIntervalMs <= OUTBOX_NOTIFY_FALSE_WARN_THRESHOLD_MS) return;
		logger.warn(
			{ pollIntervalMs: resolved.outbox.pollIntervalMs },
			`outbox.notify is false and outbox.pollIntervalMs is ${resolved.outbox.pollIntervalMs}ms ` +
				`(above ${OUTBOX_NOTIFY_FALSE_WARN_THRESHOLD_MS}ms): trigger(..., { tx }) runs will wait ` +
				"up to that long to dispatch instead of the near-instant NOTIFY wake-up; this is likely " +
				"unintentional -- lower pollIntervalMs, or leave notify at its default (true)"
		);
	}

	function startLeaderLoop(): void {
		if (shuttingDown) return;
		leader = createLeaderElector({
			// Review 3 item 1: the same bounded connect as the pool, so a leader tick can't wait
			// forever on a Postgres that accepts TCP but never answers.
			clientConfig: runtime.dedicatedClientConfig(),
			schema: resolved.schema,
			logger,
			onAcquire: () =>
				logger.info(
					{ schema: resolved.schema },
					"this engine is now the leader (reconciler, retention)"
				),
			onLose: () => {
				skipNextAcquire = true;
				logger.warn(
					{ schema: resolved.schema },
					"leadership lost (dedicated client died); will try to reacquire on a later tick"
				);
			},
		});
		const tick = () => {
			// Never overlap: a slow tick (big backlog, slow DB) makes the next ones skip.
			if (leaderLoopStopped || leaderTick) return;
			leaderTick = runLeaderTick()
				.catch((error: unknown) => logger.error({ err: error }, "leader tick failed"))
				.finally(() => {
					leaderTick = undefined;
				});
		};
		leaderTimer = setInterval(tick, resolved.reconciler.intervalMs);
		leaderTimer.unref();
		tick();
	}

	/**
	 * One leader tick: contend for leadership (decision 16) and, only as leader, run the reconciler
	 * (decisions 1, 3, 6) and make sure this retention window's `-system` job exists (decision 8).
	 * If leadership is lost mid-tick the tick still finishes: every reconciler action is safe to run
	 * concurrently with a new leader's (idempotent enqueues, re-checks under row locks).
	 */
	async function runLeaderTick(): Promise<void> {
		if (!leader) return;
		if (skipNextAcquire) {
			skipNextAcquire = false;
			return;
		}
		if (!(await leader.tryAcquire()) || leaderLoopStopped) return;
		const report = await reconciler.runOnce();
		const acted =
			report.staleQueued +
			report.staleWaiting +
			report.orphanWaiting +
			report.stalled.requeued +
			report.stalled.failed +
			report.expiredWaitpoints +
			report.queueStates;
		if (acted > 0 || report.errors > 0) logger.info({ ...report }, "reconciler tick");
		if (leaderLoopStopped) return;
		await scheduleRetention();
	}

	/**
	 * Enqueues `retention` once per `retention.intervalMs` window, with jobId
	 * `retention-${window}` where the window is derived from the DB clock (decision 17). The jobId
	 * dedupes it across processes while the job exists; because jobs are removed on completion, this
	 * process also remembers the last window it enqueued. A leadership change inside a window can
	 * still cause a second sweep in it: harmless, retention is idempotent. (The test engine has no
	 * leader loop, so this Postgres clock read never runs there.)
	 */
	async function scheduleRetention(): Promise<void> {
		const { rows } = await pool.query<{ now_ms: string }>(
			"SELECT floor(extract(epoch FROM now()) * 1000)::bigint AS now_ms"
		);
		const nowMs = Number(rows[0]?.now_ms);
		if (!Number.isFinite(nowMs)) return;
		const window = Math.floor(nowMs / resolved.retentionIntervalMs);
		if (window === lastRetentionWindow) return;
		await transport.enqueueSystem("retention", {}, { jobId: `retention-${window}` });
		lastRetentionWindow = window;
	}

	/** Stops the leader loop: no new tick, waits for the running one, releases leadership. */
	async function stopLeaderLoop(): Promise<void> {
		leaderLoopStopped = true;
		clearInterval(leaderTimer);
		// Bounded (item 7): a tick stuck on an unreachable Redis must not hold `stop()`. Whatever it
		// still does afterwards is safe (idempotent enqueues, re-checks under row locks); with Redis
		// down there is nothing to wait for.
		if (leaderTick) {
			const tick = leaderTick;
			const done = await withTimeout(tick, redisReady() ? LEADER_TICK_STOP_TIMEOUT_MS : 0);
			if (!done) {
				logger.warn({}, "stop(): the leader tick is still running; releasing leadership anyway");
			}
		}
		await leader?.close();
	}

	/** Whether the transport's connection is up (`stop()` doesn't wait on it when it isn't). */
	function redisReady(): boolean {
		return runtime.isReady();
	}

	/** Waits (bounded) for every in-flight `executeRun`, including ones that start meanwhile. */
	async function drainInFlight(timeoutMs: number): Promise<void> {
		const deadline = Date.now() + timeoutMs;
		while (inFlight.size > 0) {
			const remaining = deadline - Date.now();
			if (remaining <= 0) {
				logger.warn(
					{ inFlight: inFlight.size },
					"stop(): executions still in flight after the drain timeout; closing the queues anyway " +
						"(the reconciler recovers any enqueue that was lost)"
				);
				return;
			}
			let timer: ReturnType<typeof setTimeout> | undefined;
			await Promise.race([
				Promise.allSettled(Array.from(inFlight)),
				new Promise<void>(resolve => {
					timer = setTimeout(resolve, remaining);
				}),
			]);
			clearTimeout(timer);
		}
	}

	/** Decision 20: abort, release (no attempt increment, `enqueue_seq + 1`) and re-enqueue. */
	async function handBackActiveRuns(): Promise<void> {
		await Promise.all(
			active.values().map(async execution => {
				// Marks it handed back, aborts it and flushes its pending progress value while the
				// run is still owned (after the release the ownership guard rejects that write).
				// Bounded: `setProgress` has its own statement_timeout (heartbeatTimeoutMs) and
				// retries once, so a Postgres outage can't hold stop() up longer than this.
				const handBackMs = 2 * resolved.heartbeatTimeoutMs + HAND_BACK_FLUSH_MARGIN_MS;
				if (!(await withTimeout(execution.handBack(), handBackMs))) {
					logger.warn(
						{ runId: execution.runId, timeoutMs: handBackMs },
						"pending writes of a run handed back on shutdown did not finish in time; releasing it"
					);
				}
				try {
					const result = await store.runs.release(execution.owner);
					if (result.kind === "released") {
						notifier.status({
							runId: execution.runId,
							taskId: execution.run.taskId,
							status: result.enqueue.status,
							attempt: execution.run.attempt,
							error: execution.run.error,
							updatedAt: result.enqueue.updatedAt,
							transition: "queued",
						});
						// Bounded (item 7): with Redis unreachable the enqueue could wait for many
						// reconnection attempts. The run is already `queued` in Postgres; a lost
						// enqueue is recovered by the reconciler (decision 1).
						const enqueued = await withTimeout(
							enqueue(deps, result.enqueue),
							redisReady() ? HAND_BACK_ENQUEUE_TIMEOUT_MS : 0
						);
						if (enqueued) {
							logger.info(
								{ runId: execution.runId, taskId: execution.taskId },
								"run handed back to the queue on shutdown"
							);
						} else {
							logger.warn(
								{ runId: execution.runId, taskId: execution.taskId },
								"run released on shutdown but its re-enqueue did not complete in time; the " +
									"reconciler will re-enqueue it"
							);
						}
					}
				} catch (error) {
					logger.error(
						{ runId: execution.runId, err: error },
						"failed to release a run on shutdown; the stall reconciler will recover it"
					);
				}
			})
		);
	}

	/**
	 * Decision 20, in order: stop the leader loop (await its tick, release leadership) -> stop
	 * fetching -> wait for active runs up to `shutdownTimeoutMs` -> abort + release the
	 * still-active ones -> wait (bounded) for every in-flight `executeRun`, so a run acquired during
	 * shutdown is handed back and executor-side enqueues (retry, parent resume) land -> detach
	 * pub/sub and flush pending publishes -> close the queues -> close the connections and the pool.
	 */
	function stop(): Promise<void> {
		if (stopPromise) return stopPromise;
		const wasStarting = lifecycle === "starting";
		lifecycle = "stopping";
		shuttingDown = true;
		shutdownController.abort();
		// No new leader tick from now on; the running one is awaited below.
		leaderLoopStopped = true;
		clearInterval(leaderTimer);
		stopPromise = (async () => {
			try {
				// A concurrent start() must finish (or fail) first, or its workers would leak.
				// Review 3 item 1: bounded. `shuttingDown` makes it fail before starting workers; a
				// start() stuck on the subscriber is unblocked by dropping that connection.
				if (wasStarting && startPromise) {
					const settled = startPromise.then(
						() => undefined,
						() => undefined
					);
					if (!(await withTimeout(settled, START_SETTLE_GRACE_MS))) {
						runtime.dropSubscriber();
						if (!(await withTimeout(settled, START_SETTLE_TIMEOUT_MS))) {
							logger.warn(
								{ timeoutMs: START_SETTLE_GRACE_MS + START_SETTLE_TIMEOUT_MS },
								"stop(): start() is still in progress; stopping anyway"
							);
						}
					}
				}
				// Item 6: the workers stop fetching right away (`closeWorkers` first), BEFORE waiting
				// for the leader tick: otherwise every job fetched meanwhile is acquired, handed back
				// (`enqueue_seq + 1`) and re-enqueued at once, over and over. The tick is awaited in
				// `beforeClose`, while the queues it may enqueue into are still open.
				await transport.closeWorkers({
					timeoutMs: resolved.shutdownTimeoutMs,
					beforeClose: async () => {
						await Promise.all([
							stopLeaderLoop().catch((error: unknown) =>
								logger.warn({ err: error }, "error stopping the leader loop")
							),
							// The backend's bounded close step (BullMQ: the outbox listener; Postgres:
							// the LISTEN connection).
							runtime.beforeClose(),
						]);
						await handBackActiveRuns();
						await drainInFlight(IN_FLIGHT_DRAIN_MS);
						core.endSubscriptions();
						runtime.closeReaders();
						await core.currentPubSub()?.close();
						// Item 7: the subscriber is dedicated to this engine; dropped at once.
						runtime.dropSubscriber();
						if (redisReady()) await core.drainNotifications(NOTIFY_DRAIN_MS);
					},
				});
			} finally {
				// From here on the connections are gone: reads, runs.cancel and tokens.complete (which
				// kept working while stopping, for a graceful drain) now fail with the clean error.
				closed = true;
				await runtime.close().catch((error: unknown) => {
					logger.warn({ err: error }, "error closing the transport backend");
				});
				// Decision 14: only the pool the engine created. Bounded (review item b): a client
				// still checked out would keep `pool.end()` pending forever.
				if (ownsPool) {
					const ended = await withTimeout(
						pool.end().catch(error => {
							logger.warn({ err: error }, "error closing the postgres pool");
						}),
						POOL_END_TIMEOUT_MS
					);
					if (!ended) {
						logger.warn(
							{
								timeoutMs: POOL_END_TIMEOUT_MS,
								totalCount: pool.totalCount,
								idleCount: pool.idleCount,
							},
							"the postgres pool did not close in time (a client is still checked out); " +
								"stop() resolves anyway and the pool finishes closing once it is released"
						);
					}
				}
			}
		})();
		return stopPromise;
	}

	return {
		migrate: () => {
			// Review 4 item 1: `migrate()` starts new work, like `start()`/`trigger()`, so it is
			// refused from the moment stop() is called (`assertNotStopped`), not only once the pool
			// is closed (`assertNotClosed`, for the reads a graceful drain still needs): a migration
			// has no part in draining, and otherwise an engine-owned pool would surface pg's raw
			// "Cannot use a pool after calling end" once stop() resolved.
			try {
				assertNotStopped();
			} catch (error) {
				return Promise.reject(error);
			}
			return migrate(pool, resolved.schema);
		},
		start,
		stop,
		trigger: core.trigger,
		runs: core.runs,
		tokens: core.tokens,
		schedules: core.schedules,
		events: core.events,
		queues: core.queues,
		logger,
	};
}

const RUN_EVENT_NAMES: ReadonlySet<string> = new Set<RunEventName>([
	"run:queued",
	"run:started",
	"run:waiting",
	"run:completed",
	"run:failed",
	"run:canceled",
	"run:retrying",
]);

function statusOfTransition(transition: RunTransition): RunStatus {
	switch (transition) {
		case "started":
			return "running";
		case "retrying":
			return "queued";
		default:
			return transition;
	}
}

function isTerminal(status: RunStatus): boolean {
	return (TERMINAL_RUN_STATUSES as readonly RunStatus[]).includes(status);
}

/**
 * How many runs of one `cancelMany`/`retryFailed` page are handled at once: each one is its own
 * short transaction (plus Redis calls), so a small fan-out cuts the wall time without taking more
 * than a few pool connections.
 */
const BATCH_CONCURRENCY = 4;

/**
 * `fn` over `items` with at most `concurrency` in flight, results in input order. On the first
 * failure no new item starts; the ones in flight are awaited, then that error is thrown (the batch
 * operations are safe to call again: cancel skips finished runs, retry is keyed per run).
 */
async function mapBounded<T, R>(
	items: readonly T[],
	concurrency: number,
	fn: (item: T) => Promise<R>
): Promise<R[]> {
	const results: R[] = new Array(items.length);
	let next = 0;
	let failure: { error: unknown } | undefined;
	async function worker(): Promise<void> {
		while (failure === undefined && next < items.length) {
			const index = next++;
			try {
				results[index] = await fn(items[index] as T);
			} catch (error) {
				failure ??= { error };
			}
		}
	}
	await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
	if (failure) throw failure.error;
	return results;
}

/** Item 2: a compact `{ name, message }` for `CancelManyResult.failed`/skip reporting. */
function batchRunError(error: unknown): BatchRunError {
	return {
		name: error instanceof Error ? error.name : "Error",
		message: error instanceof Error ? error.message : String(error),
	};
}

/** Retries `fn` once on a Postgres deadlock (`40P01`); every other error propagates. */
async function retryOnDeadlock<T>(fn: () => Promise<T>): Promise<T> {
	try {
		return await fn();
	} catch (error) {
		if ((error as { code?: unknown } | null)?.code !== "40P01") throw error;
		return fn();
	}
}

/** Validates a {@link BatchRunsFilter} into the store's shape (without `purpose`). */
function validBatchFilter(method: string, filter: unknown): Omit<BatchCandidatesFilter, "purpose"> {
	if (filter === null || typeof filter !== "object") {
		throw new ConfigError(
			`${method}: pass a filter object with a limit, e.g. { taskId, limit: 100 }; got ${describeOffendingValue(filter)}`
		);
	}
	const f = filter as Partial<Record<keyof BatchRunsFilter, unknown>>;
	const { limit } = f;
	if (
		typeof limit !== "number" ||
		!Number.isInteger(limit) ||
		limit < 1 ||
		limit > MAX_BATCH_LIMIT
	) {
		throw new ConfigError(
			`${method}: filter.limit is required and must be an integer from 1 to ${MAX_BATCH_LIMIT}, got ${describeOffendingValue(limit)}`
		);
	}
	const out: Omit<BatchCandidatesFilter, "purpose"> = { limit };
	for (const key of ["taskId", "queue"] as const) {
		const value = f[key];
		if (value === undefined) continue;
		out[key] = validFilterText(`${method}: filter.${key}`, value);
	}
	for (const key of ["createdFrom", "createdTo"] as const) {
		const value = f[key];
		if (value === undefined) continue;
		if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
			throw new ConfigError(
				`${method}: filter.${key} must be a valid Date, got ${describeOffendingValue(value)}`
			);
		}
		out[key] = value;
	}
	if (f.cursor !== undefined) {
		const cursor = f.cursor as Partial<RunCursor> | null;
		if (
			cursor === null ||
			typeof cursor !== "object" ||
			!(cursor.createdAt instanceof Date) ||
			Number.isNaN(cursor.createdAt.getTime()) ||
			!isUuid(cursor.id)
		) {
			throw new ConfigError(
				`${method}: filter.cursor must be the nextCursor of a previous call, got ${describeOffendingValue(f.cursor)}`
			);
		}
		out.cursor = { createdAt: cursor.createdAt, id: cursor.id };
	}
	return out;
}

/**
 * Review 3 item 5: a string filter that Postgres `text` can't hold (NUL, or a lone UTF-16
 * surrogate -- the same rule as step names and keys, `src/utils/identifiers.ts`) is a
 * `ConfigError` here instead of the raw pg error (SQLSTATE 22021) from the query.
 */
function validFilterText(label: string, value: unknown): string {
	if (typeof value !== "string") {
		throw new ConfigError(`${label} must be a string, got ${describeOffendingValue(value)}`);
	}
	if (value.includes("\u0000")) {
		throw new ConfigError(`${label} contains a NUL character (U+0000), which Postgres rejects`);
	}
	if (!value.isWellFormed()) {
		throw new ConfigError(
			`${label} is not well-formed Unicode (contains an unpaired surrogate), which Postgres rejects`
		);
	}
	return value;
}

/** `runs.list` `status`: one status or an array of them, each checked like {@link validFilterText}. */
function validStatusFilter(label: string, value: unknown): RunStatus | readonly RunStatus[] {
	if (Array.isArray(value)) {
		return value.map((v, i) => validFilterText(`${label}[${i}]`, v) as RunStatus);
	}
	return validFilterText(label, value) as RunStatus;
}

function clampLimit(limit: number, max: number): number {
	if (!Number.isFinite(limit)) return max;
	return Math.min(max, Math.max(1, Math.floor(limit)));
}

function isUuid(value: unknown): value is string {
	return typeof value === "string" && UUID_PATTERN.test(value);
}

/** `limit` of `runs.list`/`runs.logs`: `undefined` -> the default; anything but a finite number throws. */
function validLimit(method: string, limit: unknown, fallback: number): number {
	if (limit === undefined) return fallback;
	if (typeof limit !== "number" || !Number.isFinite(limit)) {
		throw new ConfigError(
			`${method} options.limit must be a finite number, got ${describeOffendingValue(limit)}`
		);
	}
	return limit;
}

function runIdOf(run: string | RunHandle<unknown>): string {
	return typeof run === "string" ? run : run.id;
}

/**
 * Structural equality of two progress values (decision 23's `runs.subscribe` `isNewer` check),
 * ignoring key order: jsonb (and the memory store's own `normalizeKeys`) can reorder an object's
 * keys, so a JSON-text comparison (`JSON.stringify(a) === JSON.stringify(b)`) would treat an
 * unchanged value written back under a different key order as a change.
 */
function jsonEqual(a: Json | null, b: Json | null): boolean {
	if (a === b) return true;
	if (a === null || b === null) return false;
	if (Array.isArray(a) || Array.isArray(b)) {
		if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
		return a.every((v, i) => jsonEqual(v, b[i] as Json));
	}
	if (typeof a !== "object" || typeof b !== "object") return a === b;
	const ao = a as { [key: string]: Json | undefined };
	const bo = b as { [key: string]: Json | undefined };
	const ak = Object.keys(ao);
	const bk = Object.keys(bo);
	if (ak.length !== bk.length) return false;
	return ak.every(k => {
		const av = ao[k];
		const bv = bo[k];
		return av === undefined ? bv === undefined : bv !== undefined && jsonEqual(av, bv);
	});
}

/**
 * Whether a candidate run-status update is newer than the last one emitted by `runs.subscribe`
 * (decision 23, `subscribeRun`'s ordering rule): only a strictly newer `updatedAt` wins outright.
 * JS `Date` drops Postgres' microseconds, so two writes can share the same millisecond; a
 * same-millisecond update still counts as newer when its `status` differs from the last emitted
 * one, or -- decision 23 -- when only the progress value differs, compared structurally via
 * {@link jsonEqual} rather than by JSON text.
 *
 * One more same-millisecond case is filtered out instead of counting as "status differs": a
 * `"running"` status carrying a `progress` field is always a `ctx.progress.set` write's own status
 * message (`RunNotifier.progress`), and that write is always flushed strictly BEFORE any
 * terminal/suspension write -- so it can only ever precede a same-millisecond non-running status,
 * never follow one. If the last emitted update is already non-running and a same-millisecond
 * `"running"` + `progress` message shows up after it, that is the earlier progress write arriving
 * late (pub/sub redelivery/reordering), not a real transition back to running: dropping it
 * prevents a `waiting` -> `running` -> `waiting` flicker in what `runs.subscribe` emits. A genuine
 * re-acquire at the same millisecond publishes its own `"running"` status message without a
 * `progress` field, so it is unaffected by this rule.
 *
 * (Two `ctx.progress.set` writes landing within the same millisecond can't be ordered against each
 * other from the timestamp alone either -- that's inherent and transient, corrected by the next
 * message or `runs.subscribe`'s periodic re-read, not something this function tries to fix.)
 *
 * @internal exported for `test/unit/subscribe-ordering.test.ts`.
 */
export function isNewerRunUpdate(
	last: { status: RunStatus; updatedAt: Date; progress: Json | null },
	candidate: { status: RunStatus; updatedAt: Date; progress: Json | undefined }
): boolean {
	const delta = candidate.updatedAt.getTime() - last.updatedAt.getTime();
	if (delta > 0) return true;
	if (delta < 0) return false;
	if (
		candidate.status === "running" &&
		candidate.progress !== undefined &&
		last.status !== "running"
	) {
		return false;
	}
	return (
		candidate.status !== last.status ||
		(candidate.progress !== undefined && !jsonEqual(candidate.progress, last.progress))
	);
}

function toPublicRun<O>(record: RunRecord): Run<O> {
	return {
		id: record.id,
		taskId: record.taskId,
		queue: record.queue,
		status: record.status,
		payload: record.payload,
		output: record.output as O | null,
		error: record.error,
		attempt: record.attempt,
		maxAttempts: record.maxAttempts,
		parentRunId: record.parentRunId,
		batchIndex: record.batchIndex,
		runAfter: record.runAfter,
		startedAt: record.startedAt,
		finishedAt: record.finishedAt,
		createdAt: record.createdAt,
		updatedAt: record.updatedAt,
		progress: record.progress,
		concurrencyKey: record.concurrencyKey,
	};
}

/** Decision 24: validates `trigger`'s `debounce` option into the store's shape. @internal */
export function resolveDebounce(
	options: TriggerOptions,
	idempotency: { key: string } | undefined
): NewRunDebounce {
	const debounce = options.debounce;
	if (debounce === null || typeof debounce !== "object") {
		throw new ConfigError(
			`trigger options.debounce must be an object like { key, delay }, got ${describeOffendingValue(debounce)}`
		);
	}
	if (idempotency !== undefined) {
		throw new ConfigError(
			"trigger options.debounce and options.idempotencyKey cannot be combined: debounce " +
				"already returns the pending run of its key; drop one of them"
		);
	}
	if (options.delay !== undefined) {
		throw new ConfigError(
			"trigger options.delay cannot be combined with options.debounce; the debounce window " +
				"(options.debounce.delay) already delays the run"
		);
	}
	const key = validateTriggerKey("debounce key", "options.debounce.key", debounce.key);
	const delayMs = parseDuration(debounce.delay);
	if (delayMs <= 0) {
		throw new ConfigError(
			`trigger options.debounce.delay must resolve to more than 0 ms, got ${describeOffendingValue(debounce.delay)}`
		);
	}
	const payload = debounce.payload ?? "last";
	if (payload !== "last" && payload !== "first") {
		throw new ConfigError(
			`trigger options.debounce.payload must be "last" or "first", got ${describeOffendingValue(payload)}`
		);
	}
	let maxDelayMs: number | null = null;
	if (debounce.maxDelay !== undefined) {
		maxDelayMs = parseDuration(debounce.maxDelay);
		if (maxDelayMs < delayMs) {
			throw new ConfigError(
				`trigger options.debounce.maxDelay (${maxDelayMs} ms) must be >= options.debounce.delay (${delayMs} ms)`
			);
		}
	}
	return { key, delayMs, payload, maxDelayMs };
}

/**
 * Decision 26: `trigger`'s `tx` must be a client with `query()`, not a `pg.Pool` (whose queries
 * each run on a different pooled connection, outside the caller's transaction). @internal
 */
export function validateTransactionClient(tx: unknown): TransactionClient {
	const orm = ormHandleHint(tx);
	if (orm !== undefined) {
		throw new ConfigError(
			`trigger options.tx looks like ${orm.what}; wrap it with ${orm.wrap} from ` +
				`"@rivia-labs/minitrigger/${orm.subpath}" (e.g. { tx: ${orm.wrap} }) so the run commits ` +
				"or rolls back with that transaction"
		);
	}
	if (
		tx === null ||
		typeof tx !== "object" ||
		typeof (tx as { query?: unknown }).query !== "function"
	) {
		throw new ConfigError(
			`trigger options.tx must be a node-postgres client (or adapter) with query(), got ${describeOffendingValue(tx)}`
		);
	}
	const maybePool = tx as { totalCount?: unknown; idleCount?: unknown };
	if (typeof maybePool.totalCount === "number" && typeof maybePool.idleCount === "number") {
		throw new ConfigError(
			"trigger options.tx is a pg.Pool: pass the client of your transaction instead " +
				"(const client = await pool.connect(); await client.query('BEGIN'); ... " +
				"trigger(task, payload, { tx: client }); await client.query('COMMIT'))"
		);
	}
	return tx as TransactionClient;
}

/**
 * Decision 26: a raw ORM transaction handle passed as `tx` is recognized (structurally, nothing is
 * imported) only to point at its adapter subpath; it is never wrapped implicitly. A TypeORM
 * manager even has a `query()` that would "work" with the wrong result shape.
 */
function ormHandleHint(tx: unknown): { what: string; wrap: string; subpath: string } | undefined {
	if (tx === null || (typeof tx !== "object" && typeof tx !== "function")) return undefined;
	const has = (key: string) => typeof (tx as Record<string, unknown>)[key] === "function";
	if (has("$queryRawUnsafe")) {
		return { what: "a Prisma client", wrap: "prismaTx(tx)", subpath: "prisma" };
	}
	if (typeof tx === "function" && has("raw")) {
		return { what: "a knex transaction/instance", wrap: "knexTx(trx)", subpath: "knex" };
	}
	// Review 3 item 2: the real non-transactional `dataSource.manager` (and the DataSource itself)
	// has neither `startTransaction` nor an own `queryRunner` key; TypeORM tags its classes with
	// `"@instanceof"` (Symbol.for(<class name>)), and both share query + transaction +
	// getRepository. Unrecognized, its `query()` would run the INSERT in autocommit.
	const tag = (tx as Record<string, unknown>)["@instanceof"];
	const typeormTagged =
		tag === Symbol.for("EntityManager") ||
		tag === Symbol.for("DataSource") ||
		tag === Symbol.for("QueryRunner");
	if (
		typeormTagged ||
		(has("query") &&
			(has("startTransaction") ||
				"queryRunner" in tx ||
				(has("transaction") && has("getRepository"))))
	) {
		return {
			what: "a TypeORM EntityManager/QueryRunner/DataSource",
			wrap: "typeormTx(manager)",
			subpath: "typeorm",
		};
	}
	if (has("execute") && has("select") && !has("query")) {
		return {
			what: "a Drizzle transaction/database",
			wrap: "drizzleTx(tx, sql)",
			subpath: "drizzle",
		};
	}
	return undefined;
}

/** Decision 25: validates `trigger`'s `concurrencyKey` against the task's queue. @internal */
export function resolveConcurrencyKey(registry: Registry, task: AnyTask, value: unknown): string {
	const key = validateTriggerKey("concurrency key", "options.concurrencyKey", value);
	const queue = taskQueueName(task);
	if (registry.queues.get(queue)?.concurrencyKey === undefined) {
		throw new ConfigError(
			`trigger options.concurrencyKey was given for task "${task.id}", but its queue "${queue}" ` +
				"declares no concurrencyKey; add queue: { name, concurrencyKey: { limit: 1 } } to the task"
		);
	}
	return key;
}

/** Per-task option checks the registry doesn't do (retry, backoff, maxDurationMs, reserved queue). @internal */
export function validateTasks(registry: Registry, config: ResolvedConfig): void {
	const reserved = systemQueueName(config.prefix);
	for (const queue of registry.queues.values()) {
		if (queue.name === reserved) {
			throw new ConfigError(
				`queue name "${queue.name}" is reserved for the internal system queue of prefix "${config.prefix}"`
			);
		}
	}
	for (const task of registry.tasks.values()) {
		const { retry, maxDurationMs } = task.options;
		if (retry !== undefined) {
			if (!Number.isInteger(retry.maxAttempts) || retry.maxAttempts < 1) {
				throw new ConfigError(
					`task "${task.id}": retry.maxAttempts must be an integer >= 1, got ${String(retry.maxAttempts)}`
				);
			}
			if (retry.backoff !== undefined) {
				try {
					computeBackoffMs(retry.backoff, 1);
				} catch (error) {
					throw new ConfigError(
						`task "${task.id}": invalid retry.backoff (${(error as Error).message})`
					);
				}
			}
		}
		if (maxDurationMs !== undefined) {
			if (!Number.isInteger(maxDurationMs) || maxDurationMs <= 0) {
				throw new ConfigError(
					`task "${task.id}": maxDurationMs must be a positive integer, got ${String(maxDurationMs)}`
				);
			}
			// The per-attempt timer is a `setTimeout` (see `executor/execute-run.ts`).
			assertTimerMs(`task "${task.id}": maxDurationMs`, maxDurationMs);
		}
	}
}
