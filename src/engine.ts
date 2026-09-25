/**
 * `createEngine(config)` and the engine lifecycle (SPEC.md §3, Phase 3).
 *
 * No global state (decision 13): everything (pool, Redis connections, BullMQ queues/workers,
 * active executions) hangs off the object returned here, so several engines with different
 * schema/prefix can live in one process.
 */

import { randomUUID } from "node:crypto";
import { Pool } from "pg";
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
	createSystemDispatcher,
	type ExecutorDeps,
	enqueue,
	executeRun,
} from "./executor/index.js";
import { type MigrateResult, migrate } from "./migrations/migrate.js";
import { createRedisConnections } from "./queue/connections.js";
import { systemQueueName, waitpointExpireJobId } from "./queue/ids.js";
import { createTransport, type Transport } from "./queue/transport.js";
import { buildRegistry, getTask, type Registry } from "./registry.js";
import { createSqlStore } from "./store/sql/index.js";
import type {
	Json,
	RunCursor,
	RunRecord,
	RunStatus,
	SerializedError,
	Store,
	WaitpointResolution,
} from "./store/types.js";
import type { AnyTask, PayloadArgs, RunHandle, TaskOutput, TaskPayload } from "./task.js";
import { computeBackoffMs, DEFAULT_RETRY } from "./utils/backoff.js";
import { type Duration, parseDuration } from "./utils/duration.js";
import { validateIdempotencyKey } from "./utils/identifiers.js";
import { assertSerializable } from "./utils/serialization.js";

/** Queue of a task that declares none (see `task.ts` / `registry.ts`). */
const DEFAULT_QUEUE_NAME = "default";
const DEFAULT_LIST_LIMIT = 20;
/**
 * How long `stop()` waits, after handing back the still-active runs, for the in-flight
 * `executeRun` calls (a late acquire being handed back, a retry or parent-resume enqueue) so their
 * enqueues land before the queues close. Normally near-instant: an aborted execution stops waiting
 * for the task function right away.
 */
const IN_FLIGHT_DRAIN_MS = 5_000;

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
	});

	const active = new ActiveExecutions();
	/** Set synchronously by `stop()`; `executeRun` hands back any run it acquires after that. */
	let shuttingDown = false;
	const deps: ExecutorDeps = {
		store,
		transport,
		registry,
		config: resolved,
		active,
		isShuttingDown: () => shuttingDown,
	};
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
			queue: queueOf(task),
			payload: payload as Json,
			maxAttempts: task.options.retry?.maxAttempts ?? DEFAULT_RETRY.maxAttempts,
			delayMs,
			...(idempotency ? { idempotency } : {}),
		});
		// Decision 9: only a newly created run is enqueued; an existing one is returned as-is.
		if (result.kind === "created") await enqueue(deps, result.enqueue);
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
				processSystem: createSystemDispatcher({ logger }),
			});
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
	 * Decision 20, in order: stop fetching -> wait for active runs up to `shutdownTimeoutMs` ->
	 * abort + release the still-active ones -> wait (bounded) for every in-flight `executeRun`, so
	 * a run acquired during shutdown is handed back and executor-side enqueues (retry, parent
	 * resume) land -> close the queues -> close the connections and the pool.
	 */
	function stop(): Promise<void> {
		if (stopPromise) return stopPromise;
		lifecycle = "stopping";
		shuttingDown = true;
		stopPromise = (async () => {
			try {
				// A concurrent start() must finish (or fail) first, or its workers would leak.
				await startPromise?.catch(() => {});
				await transport.closeWorkers({
					timeoutMs: resolved.shutdownTimeoutMs,
					beforeClose: async () => {
						await handBackActiveRuns();
						await drainInFlight(IN_FLIGHT_DRAIN_MS);
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

	async function resolveParent(parent: WaitpointResolution | null): Promise<void> {
		if (!parent) return;
		if (parent.resume) await enqueue(deps, parent.resume);
		if (parent.hasExpireJob) {
			await transport.removeSystemJob(waitpointExpireJobId(parent.waitpointId));
		}
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

			for (const canceled of result.canceled) {
				// Local executions only; cross-process abort via pub/sub comes in Phase 6. Either way
				// ownership is already gone, so a zombie can't write anything.
				active.get(canceled.runId)?.abort("canceled");
				await transport.removeRunJob(canceled.queue, canceled.runId, canceled.enqueueSeq);
			}
			for (const waitpointId of result.expireJobsToRemove) {
				await transport.removeSystemJob(waitpointExpireJobId(waitpointId));
			}
			await resolveParent(result.parent);
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

	return {
		migrate: () => migrate(pool, resolved.schema),
		start,
		stop,
		trigger,
		runs,
	};
}

function queueOf(task: AnyTask): string {
	return task.options.queue?.name ?? DEFAULT_QUEUE_NAME;
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
