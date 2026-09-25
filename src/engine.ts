/**
 * `createEngine(config)` and the engine lifecycle (SPEC.md §3, Phase 3).
 *
 * No global state (decision 13): everything (pool, Redis connections, BullMQ queues/workers,
 * active executions) hangs off the object returned here, so several engines with different
 * schema/prefix can live in one process.
 */

import { randomUUID } from "node:crypto";
import { Client, Pool } from "pg";
import {
	assertTimerMs,
	assertTtlWithinRetention,
	type EngineConfig,
	type ResolvedConfig,
	resolveConfig,
} from "./config.js";
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
	taskQueueName,
} from "./executor/index.js";
import { createLeaderElector, type LeaderElector } from "./leader.js";
import { type MigrateResult, migrate } from "./migrations/migrate.js";
import { createPubSub, createPubSubPublisher, type PubSub } from "./pubsub.js";
import { createRedisConnections } from "./queue/connections.js";
import { systemQueueName } from "./queue/ids.js";
import { createTransport, type Transport } from "./queue/transport.js";
import { createReconciler } from "./reconciler.js";
import { buildRegistry, getTask, type Registry } from "./registry.js";
import { createRetention } from "./retention.js";
import { createSqlStore } from "./store/sql/index.js";
import type {
	Json,
	RunCursor,
	RunRecord,
	RunStatus,
	SerializedError,
	Store,
	WaitpointStatus,
} from "./store/types.js";
import type { AnyTask, PayloadArgs, RunHandle, TaskOutput, TaskPayload } from "./task.js";
import { computeBackoffMs } from "./utils/backoff.js";
import { type Duration, parseDuration } from "./utils/duration.js";
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

export interface EngineRuns {
	get<O = unknown>(run: string | RunHandle<O>): Promise<Run<O> | null>;
	list(options?: ListRunsOptions): Promise<RunList>;
	/** Cancels the run and, recursively, its pending descendants. */
	cancel(run: string | RunHandle<unknown>): Promise<CancelRunOutcome>;
	/** New run of the same task with the same payload (no idempotency key). */
	replay<O = unknown>(run: string | RunHandle<O>): Promise<RunHandle<O>>;
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
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

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
	});

	const active = new ActiveExecutions();
	/** Set synchronously by `stop()`; `executeRun` hands back any run it acquires after that. */
	let shuttingDown = false;

	// --- Pub/sub (Phase 6 cancel broadcast, Phase 7 status seam). Publishing uses the shared
	// connection, so a trigger-only engine (decision 19) still publishes what it causes; only a
	// started engine subscribes (dedicated connection, decision 14). ---
	const publisher = createPubSubPublisher({
		publisher: connections.shared(),
		prefix: resolved.prefix,
	});
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
	const notifier: RunNotifier = {
		status(runId, known) {
			trackNotification(
				(async () => {
					const row = known ?? (await store.runs.get(runId));
					if (!row) return;
					await publisher.publishStatus({
						runId,
						status: row.status,
						updatedAt: row.updatedAt.toISOString(),
					});
				})(),
				"run status"
			);
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

	const deps: ExecutorDeps = {
		store,
		transport,
		registry,
		config: resolved,
		active,
		isShuttingDown: () => shuttingDown,
		notify: notifier,
	};

	// --- Leader loop (Phase 6, decision 16): reconciler + retention scheduling, leader only. ---
	const reconciler = createReconciler({
		store,
		transport,
		logger,
		graceMs: resolved.reconciler.graceMs,
		stallTimeoutMs: resolved.stallTimeoutMs,
		batchSize: resolved.reconciler.batchSize,
		onCanceled: runIds => {
			abortLocal(runIds);
			for (const runId of runIds) notifier.status(runId);
			notifier.canceled(runIds);
		},
		onStatusChange: runId => notifier.status(runId),
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

	let lifecycle: "idle" | "starting" | "started" | "stopping" = "idle";
	let startPromise: Promise<void> | undefined;
	let stopPromise: Promise<void> | undefined;

	function assertNotStopped(): void {
		if (lifecycle === "stopping") {
			throw new MinitriggerError("this engine was stopped; create a new one");
		}
	}

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
			notifier.status(result.run.id, result.run);
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
			if (ttlMs <= 0) throw new ConfigError("idempotencyKeyTtl must be greater than 0");
			assertTtlWithinRetention(ttlMs, resolved.retentionDays, "idempotencyKeyTtl");
			idempotency = { key, ttlMs };
		} else if (options.idempotencyKeyTtl !== undefined) {
			throw new ConfigError("idempotencyKeyTtl was given without an idempotencyKey");
		}
		const delayMs = options.delay === undefined ? 0 : parseDuration(options.delay);

		return createRun(task, payload, delayMs, idempotency);
	}

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
			// Decision 22: a check that cannot run only warns; a wrong answer throws.
			const warning = await transport.checkRedis();
			if (warning !== undefined) logger.warn({}, warning);

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
				}),
			});

			const subscriber = connections.dedicated("subscriber");
			// ioredis reconnects (and re-subscribes) on its own; without a listener it prints every
			// connection error to the console.
			subscriber.on("error", (error: unknown) =>
				logger.debug({ err: error }, "pub/sub subscriber connection error")
			);
			pubsub = createPubSub({
				publisher: connections.shared(),
				subscriber,
				prefix: resolved.prefix,
				logger,
			});
			pubsub.onCancel(abortLocal);
			await pubsub.ready;

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
	 * still cause a second sweep in it: harmless, retention is idempotent.
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
						notifier.status(execution.runId);
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
						await pubsub?.close();
						await drainNotifications(NOTIFY_DRAIN_MS);
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
	};

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
		migrate: () => migrate(pool, resolved.schema),
		start,
		stop,
		trigger,
		runs,
		tokens,
	};
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

/** Per-task option checks the registry doesn't do (retry, backoff, maxDurationMs, reserved queue). */
function validateTasks(registry: Registry, config: ResolvedConfig): void {
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
