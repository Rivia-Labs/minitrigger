/**
 * `createEngine(config)` and the engine lifecycle (SPEC.md §3, Phase 3).
 *
 * No global state (decision 13): everything (pool, Redis connections, BullMQ queues/workers,
 * active executions) hangs off the object returned here, so several engines with different
 * schema/prefix can live in one process.
 */

import { randomUUID } from "node:crypto";
import type { Redis } from "ioredis";
import { Client, Pool } from "pg";
import {
	assertTimerMs,
	assertTtlWithinRetention,
	type EngineConfig,
	type ResolvedConfig,
	resolveConfig,
} from "./config.js";
import { checkEnvironment } from "./env-check.js";
import { ConfigError, MinitriggerError } from "./errors.js";
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
import {
	createPubSub,
	createPubSubPublisher,
	type PubSub,
	type PubSubPublisher,
	type RunStatusMessage,
} from "./pubsub.js";
import { createRedisConnections } from "./queue/connections.js";
import { systemQueueName } from "./queue/ids.js";
import { createTransport, type Transport } from "./queue/transport.js";
import { createReconciler } from "./reconciler.js";
import { buildRegistry, getTask, type Registry } from "./registry.js";
import { createRetention } from "./retention.js";
import {
	createReconcileSchedulesHook,
	createSchedulesApi,
	type EngineSchedules,
	syncDeclarativeSchedules,
} from "./schedules.js";
import { createSqlStore } from "./store/sql/index.js";
import {
	type Json,
	type LogLevel,
	type RunCursor,
	type RunRecord,
	type RunStatus,
	type SerializedError,
	type StepKind,
	type StepStatus,
	type Store,
	TERMINAL_RUN_STATUSES,
	type WaitpointStatus,
} from "./store/types.js";
import { createStreams, type Streams } from "./streams.js";
import type { AnyTask, PayloadArgs, RunHandle, TaskOutput, TaskPayload } from "./task.js";
import { computeBackoffMs } from "./utils/backoff.js";
import { type Duration, parseDuration } from "./utils/duration.js";
import { describeOffendingValue } from "./utils/error-format.js";
import { validateIdempotencyKey } from "./utils/identifiers.js";
import { assertSerializable } from "./utils/serialization.js";

const DEFAULT_LIST_LIMIT = 20;
/**
 * How long `stop()` waits, after handing back the still-active runs, for the in-flight
 * `executeRun` calls (a late acquire being handed back, a retry or parent-resume enqueue) so their
 * enqueues land before the queues close. Normally near-instant: an aborted execution stops waiting
 * for the task function right away.
 */
const IN_FLIGHT_DRAIN_MS = 5_000;
/** How long `stop()` waits for pending best-effort status/cancel publishes before closing Redis. */
const NOTIFY_DRAIN_MS = 2_000;
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
	/** Delays the first execution (`run_after = now() + delay`, DB clock). */
	delay?: Duration;
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
 * - `timed_out`: the token's own timeout fired first;
 * - `run_finished`: the token's run already completed, failed or was canceled (every terminal
 *   transition times out the run's pending tokens, so nothing will ever read this output).
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
	 * only if it is `waiting` with nothing else blocking. When the token's run already finished
	 * (completed/failed/canceled) the result is `{ completed: false, reason: "run_finished" }`,
	 * not `timed_out`, even though the finished run timed out its pending tokens.
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
	/** Local run events of this process. */
	events: EngineEvents;
}

/**
 * The surface `Engine` and `TestEngine` (`src/testing/test-engine.ts`, Phase 9.2 L4) share: enough
 * to trigger tasks and query/react to runs from code that does not care whether it is talking to
 * the real engine or a test one (e.g. a helper shared by production code and tests). Deliberately
 * excludes `migrate`/`start`/`stop`, which only `Engine` has in this shape (`TestEngine.stop()`
 * differs -- it ends open subscriptions/stream reads instead of a graceful worker shutdown).
 */
export type EngineClient = Pick<Engine, "trigger" | "runs" | "tokens" | "schedules" | "events">;

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
}

/** Lifecycle state owned by the engine wrapping the core. @internal */
export interface EngineCoreLifecycle {
	/** True once `stop()` began (see `ExecutorDeps.isShuttingDown`). */
	isShuttingDown(): boolean;
	/** Throws once the engine was stopped. */
	assertNotStopped(): void;
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
	/** The subscriber side, created on first use. */
	getPubSub(): PubSub;
	/** The subscriber side if it was already created. */
	currentPubSub(): PubSub | undefined;
	/** Ends every open `runs.subscribe` iteration. */
	endSubscriptions(): void;
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
	const notifier: RunNotifier = {
		status(info) {
			notifyStatus(info);
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
	/** Ends every open `runs.subscribe` iteration (on `stop()`). */
	const openSubscriptions = new Set<() => void>();

	async function createRun(
		task: AnyTask,
		payload: unknown,
		delayMs: number,
		idempotency: { key: string; ttlMs: number } | undefined
	): Promise<RunHandle<never>> {
		assertSerializable(payload, {
			label: `payload of task "${task.id}"`,
			maxBytes: resolved.limits.maxPayloadBytes,
		});
		const result = await store.runs.create({
			id: randomUUID(),
			taskId: task.id,
			queue: taskQueueName(task),
			payload: payload as Json,
			maxAttempts: childTaskSpec(task).maxAttempts,
			delayMs,
			...(idempotency ? { idempotency } : {}),
		});
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
		}
		return { id: result.run.id, taskId: result.run.taskId };
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

		return createRun(task, payload, delayMs, idempotency);
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

	const runs: EngineRuns = {
		async get<O>(run: string | RunHandle<O>): Promise<Run<O> | null> {
			const record = await store.runs.get(runIdOf(run));
			return record ? toPublicRun<O>(record) : null;
		},

		async list(options: ListRunsOptions = {}): Promise<RunList> {
			const result = await store.runs.list({
				limit: options.limit ?? DEFAULT_LIST_LIMIT,
				...(options.taskId !== undefined ? { taskId: options.taskId } : {}),
				...(options.status !== undefined ? { status: options.status } : {}),
				...(options.createdFrom !== undefined ? { createdFrom: options.createdFrom } : {}),
				...(options.createdTo !== undefined ? { createdTo: options.createdTo } : {}),
				...(options.cursor !== undefined ? { cursor: options.cursor } : {}),
			});
			return { items: result.items.map(r => toPublicRun(r)), nextCursor: result.nextCursor };
		},

		async cancel(run: string | RunHandle<unknown>): Promise<CancelRunOutcome> {
			const result = await store.runs.cancel(runIdOf(run), "canceled via engine.runs.cancel()");
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
			const record = await store.runs.get(runId);
			if (!record) throw new MinitriggerError(`run ${runId} not found`);
			const task = getTask(registry, record.taskId);
			return createRun(task, record.payload, 0, undefined);
		},

		async logs(run, options = {}): Promise<RunLogPage> {
			const limit = clampLimit(options.limit ?? DEFAULT_LOGS_LIMIT, MAX_LOGS_LIMIT);
			const runId = runIdOf(run);
			if (!UUID_PATTERN.test(runId)) return { items: [], nextAfterId: null };
			const entries = await store.logs.list(runId, {
				limit,
				...(options.afterId != null ? { afterId: options.afterId } : {}),
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
		const pending: RunStatusMessage[] = [];
		let wake: (() => void) | undefined;
		let ended = false;
		const notify = () => {
			const resolve = wake;
			wake = undefined;
			resolve?.();
		};
		const end = () => {
			ended = true;
			notify();
		};
		const ps = getPubSub();
		// (1) Before the read, so nothing published after it can be missed.
		const unsubscribe = ps.subscribeStatus(runId, message => {
			pending.push(message);
			notify();
		});
		signal?.addEventListener("abort", end, { once: true });
		openSubscriptions.add(end);
		try {
			await ps.ready;
			if (ended) return;
			// (2) Postgres is the source of truth for the starting point.
			const row = await store.runs.get(runId);
			if (!row || ended) return;
			let last: RunStatusUpdate = { runId, status: row.status, updatedAt: row.updatedAt };
			yield last;
			if (isTerminal(last.status)) return;

			// (3) Only strictly newer changes (a same-millisecond change counts when its status
			// differs: JS dates drop Postgres' microseconds).
			const isNewer = (status: RunStatus, updatedAt: Date) => {
				const delta = updatedAt.getTime() - last.updatedAt.getTime();
				return delta > 0 || (delta === 0 && status !== last.status);
			};
			while (!ended) {
				const message = pending.shift();
				if (message) {
					const updatedAt = new Date(message.updatedAt);
					if (Number.isNaN(updatedAt.getTime()) || !isNewer(message.status, updatedAt)) continue;
					last = { runId, status: message.status, updatedAt };
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
				if (!timedOut || ended) continue;
				wake = undefined;
				// Safety net: pub/sub may drop messages (e.g. on a reconnect).
				const current = await store.runs.get(runId);
				if (!current) return;
				if (isNewer(current.status, current.updatedAt)) {
					pending.push({
						runId,
						status: current.status,
						updatedAt: current.updatedAt.toISOString(),
					});
				}
			}
		} finally {
			unsubscribe();
			signal?.removeEventListener("abort", end);
			openSubscriptions.delete(end);
		}
	}

	const tokens: EngineTokens = {
		async complete(tokenId: string, output?: unknown): Promise<CompleteTokenOutcome> {
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
		getPubSub,
		currentPubSub: () => pubsub,
		endSubscriptions() {
			for (const end of Array.from(openSubscriptions)) end();
		},
		drainNotifications,
	};
}

export function createEngine(config: EngineConfig): Engine {
	const resolved = resolveConfig(config);
	const { logger } = resolved;
	const registry = buildRegistry(resolved.tasks);
	validateTasks(registry, resolved);

	const ownsPool = typeof resolved.postgres === "string";
	const pool =
		typeof resolved.postgres === "string"
			? new Pool({ connectionString: resolved.postgres, max: resolved.poolMax })
			: resolved.postgres.pool;
	if (ownsPool) {
		// An idle client error would otherwise crash the process (unhandled "error" event).
		pool.on("error", error => logger.error({ err: error }, "postgres pool error"));
	}

	const store: Store = createSqlStore(pool, resolved.schema);
	const connections = createRedisConnections(resolved.redis);
	const transport: Transport = createTransport({
		prefix: resolved.prefix,
		connections,
		queues: Array.from(registry.queues.values()),
		systemConcurrency: resolved.systemConcurrency,
		schema: resolved.schema,
	});

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

	const streams = createStreams({ redis: connections.shared(), prefix: resolved.prefix });
	/** Dedicated connections of the open `runs.readStream` iterations (disconnected on `stop()`). */
	const streamReaders = new Set<Redis>();

	/** `runs.readStream`: one dedicated connection per reader (decision 14), closed at the end. */
	async function* readRunStream(
		runId: string,
		name: string,
		signal: AbortSignal | undefined
	): AsyncGenerator<string | Json, void, undefined> {
		if (signal?.aborted) return;
		// A duplicate (not `connections.dedicated`, which keeps every connection until stop()):
		// readers come and go, so each one is closed as soon as its iteration ends.
		const reader = connections.shared().duplicate({ maxRetriesPerRequest: null });
		reader.on("error", (error: unknown) =>
			logger.debug({ err: error, runId, name }, "stream reader connection error")
		);
		streamReaders.add(reader);
		try {
			yield* streams.read(runId, name, { redis: reader, ...(signal ? { signal } : {}) });
		} finally {
			streamReaders.delete(reader);
			reader.disconnect();
		}
	}

	const core = createEngineCore(
		resolved,
		registry,
		{
			store,
			transport,
			// Publishing uses the shared connection, so a trigger-only engine (decision 19) still
			// publishes what it causes; only a started engine subscribes (dedicated connection).
			publisher: createPubSubPublisher({
				publisher: connections.shared(),
				prefix: resolved.prefix,
			}),
			createPubSub() {
				const subscriber = connections.dedicated("subscriber");
				// ioredis reconnects (and re-subscribes) on its own; without a listener it prints
				// every connection error to the console.
				subscriber.on("error", (error: unknown) =>
					logger.debug({ err: error }, "pub/sub subscriber connection error")
				);
				return createPubSub({
					publisher: connections.shared(),
					subscriber,
					prefix: resolved.prefix,
					logger,
				});
			},
			streams,
			readStream: readRunStream,
		},
		{ isShuttingDown: () => shuttingDown, assertNotStopped }
	);
	const { deps, active, notifier, notifyStatus, abortLocal } = core;

	// --- Leader loop (Phase 6, decision 16): reconciler + retention scheduling, leader only. ---
	const reconciler = createReconciler({
		store,
		transport,
		logger,
		graceMs: resolved.reconciler.graceMs,
		stallTimeoutMs: resolved.stallTimeoutMs,
		batchSize: resolved.reconciler.batchSize,
		reconcileSchedules: createReconcileSchedulesHook({
			store,
			transport,
			logger,
			schema: resolved.schema,
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
			// Decision 22: a check that cannot run only warns; a wrong answer throws (Postgres
			// version, Redis eviction policy, pending migrations -- `autoMigrate: false` because
			// `start()` does not call `migrate()` itself; a pending migration must be applied via
			// `engine.migrate()` first, and the thrown `ConfigError` says so).
			// `checkEnvironment` itself calls `logger.warn` per warning; nothing else to do with the
			// returned report here.
			await checkEnvironment({
				pool,
				redis: connections.shared(),
				schema: resolved.schema,
				logger,
				autoMigrate: false,
			});

			// On failure `startWorkers` closes whatever workers it already created.
			await transport.startWorkers({
				...(options.queues ? { queues: options.queues } : {}),
				processRun: async (_queue, data) => {
					const execution = executeRun(deps, { runId: data.runId, seq: data.seq });
					inFlight.add(execution);
					try {
						await execution;
					} finally {
						inFlight.delete(execution);
					}
				},
				processSystem: createSystemDispatcher({
					config: resolved,
					store,
					transport,
					active,
					notify: notifier,
					runRetention: () => retention.runOnce(),
					registry,
				}),
			});

			// Phase 8, decision 21: create/update every `defineTask({ schedule })` in Postgres + BullMQ.
			await syncDeclarativeSchedules({
				store,
				transport,
				registry,
				logger,
				maxPayloadBytes: resolved.limits.maxPayloadBytes,
				pruneDeclarative: resolved.schedules.pruneDeclarative,
			});

			const ps = core.getPubSub();
			ps.onCancel(abortLocal);
			await ps.ready;

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

	function startLeaderLoop(): void {
		leader = createLeaderElector({
			...(typeof resolved.postgres === "string"
				? { connectionString: resolved.postgres }
				: { clientFactory: () => new Client(pool.options) }),
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
			report.expiredWaitpoints;
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
		await leaderTick;
		await leader?.close();
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
				execution.handedBack = true;
				execution.abort("shutdown");
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
						await enqueue(deps, result.enqueue);
						logger.info(
							{ runId: execution.runId, taskId: execution.taskId },
							"run handed back to the queue on shutdown"
						);
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
		lifecycle = "stopping";
		shuttingDown = true;
		stopPromise = (async () => {
			try {
				// A concurrent start() must finish (or fail) first, or its workers would leak.
				await startPromise?.catch(() => {});
				await stopLeaderLoop();
				await transport.closeWorkers({
					timeoutMs: resolved.shutdownTimeoutMs,
					beforeClose: async () => {
						await handBackActiveRuns();
						await drainInFlight(IN_FLIGHT_DRAIN_MS);
						core.endSubscriptions();
						for (const reader of Array.from(streamReaders)) reader.disconnect();
						await core.currentPubSub()?.close();
						await core.drainNotifications(NOTIFY_DRAIN_MS);
					},
				});
			} finally {
				if (ownsPool) {
					await pool.end().catch(error => {
						logger.warn({ err: error }, "error closing the postgres pool");
					});
				}
			}
		})();
		return stopPromise;
	}

	return {
		migrate: () => migrate(pool, resolved.schema),
		start,
		stop,
		trigger: core.trigger,
		runs: core.runs,
		tokens: core.tokens,
		schedules: core.schedules,
		events: core.events,
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

function clampLimit(limit: number, max: number): number {
	if (!Number.isFinite(limit)) return max;
	return Math.min(max, Math.max(1, Math.floor(limit)));
}

function runIdOf(run: string | RunHandle<unknown>): string {
	return typeof run === "string" ? run : run.id;
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
	};
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
