import { DelayedError, Job, Queue, Worker } from "bullmq";
import type { EngineLogger } from "../config.js";
import { ConfigError, MinitriggerError } from "../errors.js";
import { SystemJobDelayed } from "../executor/system.js";
import type { RunRecord } from "../store/types.js";
import { validateQueueName, validateSqlIdentifier } from "../utils/identifiers.js";
import type { RedisConnections } from "./connections.js";
import { runJobId, SCHEDULE_TICK_JOB_NAME, systemQueueName } from "./ids.js";
import { checkRedisEvictionPolicy } from "./redis-check.js";

/**
 * The queue shape `createTransport` needs from a resolved task-queue config.
 * Defined locally (a structural type, not imported from `src/registry.ts`,
 * which is owned by a different task in this phase) so any object with this
 * shape -- in particular the registry's own resolved-queue records -- works
 * here without a hard dependency.
 */
export interface ResolvedQueueConfig {
	name: string;
	concurrency: number;
	globalConcurrency?: number;
	rateLimit?: { max: number; durationMs: number };
}

export interface RunJobData {
	runId: string;
	seq: number;
	/**
	 * Set only by the Postgres transport (`src/pg/transport.ts`, decision 28(a)): the run already
	 * claimed in the same statement that picked up this job, so the executor (`RunJob.claimed`)
	 * skips its own `acquire()` and runs it directly. The BullMQ path below (`enqueueRun`) never
	 * sets it on a job it adds -- it would be dead weight to serialize into
	 * Redis (a BullMQ job is a real persisted payload, unlike a Postgres claim's in-memory result)
	 * and unsafe as stale data if that same job were ever redelivered.
	 */
	claimed?: RunRecord;
}

export interface CreateTransportOptions {
	prefix: string;
	connections: RedisConnections;
	queues: ResolvedQueueConfig[];
	/** Concurrency of the always-on `-system` worker (decision 8). */
	systemConcurrency?: number;
	/**
	 * This engine's Postgres schema (fix 8, `docs/HANDOFF.md`/`CLAUDE.md` Phase 8 notes): tagged onto
	 * every `upsertJobScheduler` job template's `data` (`{ scheduleId, schema }`) and readable back via
	 * {@link JobSchedulerInfo.schema}, so `createReconcileSchedulesHook` can tell a scheduler that
	 * belongs to a DIFFERENT engine sharing this Redis `prefix` apart from a genuine orphan and never
	 * delete it. Optional: omitted in tests/call sites that don't need the distinction (every
	 * scheduler is then untagged, matching the pre-fix behavior). `prefix` should still be unique per
	 * schema in practice (documented in `README.md`); this tag is a safety net, not a substitute.
	 */
	schema?: string;
	/**
	 * Receives BullMQ `Queue`/`Worker` `error` events (connection drops, script failures), which
	 * would otherwise crash the process as unhandled `error` events: `warn` while running
	 * (throttled per emitter), `debug` once `closeWorkers` started (expected shutdown noise).
	 * Omitted => those errors are ignored.
	 */
	logger?: EngineLogger;
}

export interface EnqueueRunInput {
	queue: string;
	runId: string;
	seq: number;
	delayMs?: number;
}

export interface EnqueueSystemOptions {
	jobId?: string;
	delayMs?: number;
}

/** Input of {@link Transport.upsertJobScheduler} (Phase 8, decision 8). */
export interface UpsertJobSchedulerInput {
	/** The schedule's own id (`schedules.id`); used verbatim as the BullMQ job scheduler id. */
	id: string;
	/** Cron pattern (`RepeatOptions.pattern`). */
	pattern: string;
	/** IANA timezone (`RepeatOptions.tz`); omitted = BullMQ's default (UTC-based `cron-parser`). */
	timezone?: string;
}

/** One BullMQ job scheduler's live cron/timezone, as read back from Redis. */
export interface JobSchedulerInfo {
	id: string;
	pattern: string | undefined;
	timezone: string | undefined;
	/**
	 * The schema tag from the job template's `data` (fix 8), when this scheduler was created by a
	 * `createTransport` given a `schema` (every engine started after that fix). Omitted for a
	 * scheduler created before this fix, by a caller that omitted `schema`, or by a plain test fake.
	 */
	schema?: string;
}

export type ProcessRunFn = (queue: string, data: RunJobData, job: Job<RunJobData>) => Promise<void>;
export type ProcessSystemFn = (name: string, data: unknown, job: Job) => Promise<void>;

export interface StartWorkersOptions {
	/** Filters which *task* queues get a worker; the `-system` worker always starts (decision 8). */
	queues?: string[];
	processRun: ProcessRunFn;
	processSystem: ProcessSystemFn;
}

export interface CloseWorkersOptions {
	timeoutMs: number;
	/**
	 * Called after the wait (timed out or not) and BEFORE any `Queue`/connection is closed, with
	 * the jobs still active at that point. The engine uses it to release still-running runs and
	 * re-enqueue them (decision 20), which needs the queues to still be open. Errors are logged
	 * (`warn`) and closing continues.
	 */
	beforeClose?: (stillActive: StillActiveJob[]) => Promise<void>;
}

/** A job that was still active when {@link Transport.closeWorkers}'s timeout elapsed. */
export interface StillActiveJob {
	queue: string;
	jobId: string;
}

export interface CloseWorkersResult {
	stillActive: StillActiveJob[];
}

export interface Transport {
	enqueueRun(input: EnqueueRunInput): Promise<void>;
	/** Best-effort: ignores "locked" (job currently active) and "not found". */
	removeRunJob(queue: string, runId: string, seq: number): Promise<void>;
	enqueueSystem(
		name: "waitpoint-expire" | "retention" | "reconcile" | string,
		data: unknown,
		opts?: EnqueueSystemOptions
	): Promise<void>;
	/** Best-effort: ignores "locked" and "not found" (decision 10: expiry-job removal happens outside the resolving transaction). */
	removeSystemJob(jobId: string): Promise<void>;
	/**
	 * Creates or updates a BullMQ job scheduler on the `-system` queue (decision 8): each tick it
	 * produces is dispatched as a `schedule-tick` job (`queue/ids.ts#SCHEDULE_TICK_JOB_NAME`),
	 * carrying `{ scheduleId: input.id }` and BullMQ's own deterministic id
	 * `repeat:<input.id>:<nextMillis>`. Idempotent: re-upserting the same pattern/timezone is a
	 * no-op tick-wise (BullMQ only replaces the pending next iteration).
	 */
	upsertJobScheduler(input: UpsertJobSchedulerInput): Promise<void>;
	/** True if a scheduler with this id existed and was removed; false if there was none. */
	removeJobScheduler(id: string): Promise<boolean>;
	/** `undefined` if no scheduler with this id exists. */
	getJobScheduler(id: string): Promise<JobSchedulerInfo | undefined>;
	/** Every job scheduler currently registered on the `-system` queue. */
	listJobSchedulers(): Promise<JobSchedulerInfo[]>;
	startWorkers(opts: StartWorkersOptions): Promise<void>;
	/**
	 * Stops fetching new jobs immediately, waits up to `timeoutMs` for jobs
	 * already in flight, then closes every `Queue`/`Worker` and every
	 * connection this transport owns -- regardless of whether jobs were still
	 * active, per decision 20 ("don't force-kill; return which were still
	 * active so the engine can release them"). Ownership loss on the
	 * Postgres side (decision 5) is what actually makes a not-yet-finished
	 * processor's writes into safe no-ops; this method never aborts a running
	 * `processRun`/`processSystem` call itself.
	 */
	closeWorkers(opts: CloseWorkersOptions): Promise<CloseWorkersResult>;
	/** Decision 22: warns (does not throw) when the check itself is unavailable; throws only on a wrong answer. */
	checkRedis(): Promise<string | undefined>;
	/**
	 * Decision 27: BullMQ's queue-level (global, Redis) pause of a task queue: every worker of every
	 * process stops fetching new jobs of it; active jobs are not touched. Jobs can still be added
	 * (they wait). Idempotent. Any valid task-queue name (a producer-only `Queue` on demand, like
	 * {@link enqueueRun}); the `-system` queue is refused (`ConfigError`).
	 */
	pauseQueue(queue: string): Promise<void>;
	/** Undoes {@link pauseQueue}; idempotent. */
	resumeQueue(queue: string): Promise<void>;
	/** Whether the queue-level pause flag is set in Redis. */
	isQueuePaused(queue: string): Promise<boolean>;
	/**
	 * This process's worker concurrency for `queue`, or `undefined` when this process runs no worker
	 * for it (not started, filtered out by `start({ queues })`, or a test transport).
	 */
	workerConcurrency(queue: string): number | undefined;
	/**
	 * Changes this process's worker concurrency for `queue` at runtime (BullMQ reads it on every
	 * fetch-loop iteration: a decrease stops new fetches right away, running jobs finish; an
	 * increase applies at the worker's next fetch). Returns false when there is no such worker.
	 */
	setWorkerConcurrency(queue: string, concurrency: number): boolean;
}

const RUN_JOB_NAME = "run";
const SYSTEM_QUEUE_LABEL = "-system";

/**
 * BullMQ redelivers a job it believes stalled (its own lock expired without
 * renewal) by moving it back to `wait`, up to `maxStalledCount` times before
 * giving up and moving it to `failed`. minitrigger does not use BullMQ's lock
 * state for stall detection at all (decision 6: Postgres `heartbeat_at` is
 * authoritative) -- a redelivered run job is just a second `acquire()` that
 * either no-ops (rejected by `enqueue_seq`/ownership, decision 5) or is a
 * legitimate resume. So BullMQ's own stalled-job bookkeeping must never be
 * allowed to reach `failed` on its own: `maxStalledCount` is set as large as
 * practically possible, and lock renewal is left enabled (`skipLockRenewal`
 * is not set, so it keeps its `false` default) so a live worker's lock simply
 * never expires in the first place -- stall recovery only fires for a worker
 * that is actually gone, and recovering it is a safe no-op either way.
 */
const NEVER_FAIL_ON_STALL = Number.MAX_SAFE_INTEGER;

/**
 * BullMQ's own lock/stall timings, set explicitly instead of relying on library defaults.
 * Decision 6: the Postgres heartbeat (`runs.heartbeat_at` vs `stallTimeoutMs`) is what decides
 * that a run stalled, never BullMQ's lock. These only govern BullMQ's *job* bookkeeping: a worker
 * that died leaves its job locked; after `lockDuration` without renewal, the stalled-job check
 * (every `stalledInterval`) moves it back to `wait`, and the redelivery is a harmless no-op at
 * `acquire` (decision 5: the run is `running` under another `execution_id`, or its `enqueue_seq`
 * already moved on). Together with {@link NEVER_FAIL_ON_STALL} this means BullMQ's stall handling
 * can never fail or duplicate a run; the values just keep the Redis-side cleanup reasonably
 * prompt while a live worker (renewing every `lockDuration / 2`) never loses its lock.
 */
const LOCK_DURATION_MS = 30_000;
const STALLED_INTERVAL_MS = 30_000;

/**
 * After `beforeClose` (which waits for the in-flight processors), how long `closeWorkers` still
 * waits for the `Worker.close()` calls before closing the connections under them. Closing a
 * connection while a worker is still finishing a job can leave BullMQ spinning on it.
 */
const WORKER_CLOSE_GRACE_MS = 2_000;

/** With Redis unreachable, how long `closeWorkers` waits for the `Queue.close()` calls at most. */
const REDIS_DOWN_CLOSE_BOUND_MS = 250;

/** Resolves when `promise` settles or after `ms`, whichever comes first (never rejects). */
async function bounded(promise: Promise<unknown>, ms: number): Promise<void> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	await Promise.race([
		promise.then(
			() => undefined,
			() => undefined
		),
		new Promise<void>(resolve => {
			timer = setTimeout(resolve, ms);
		}),
	]);
	clearTimeout(timer);
}

/**
 * Thrown (internally) by {@link guardRetryLoopOnClose} instead of re-running a BullMQ Redis call
 * once the worker is closing. Its message must NOT look like a connection error (no
 * `"Connection is closed."`, no `ECONNREFUSED`): BullMQ's `isNotConnectionError` must classify it
 * as a regular error so `retryIfFailed` stops retrying.
 */
class WorkerClosedError extends Error {
	override name = "WorkerClosedError";
	constructor() {
		super("minitrigger: BullMQ worker is closing; not retrying its Redis call");
	}
}

/** The non-public BullMQ `Worker` members {@link guardRetryLoopOnClose} relies on. */
interface WorkerRetryInternals {
	retryIfFailed?: (fn: () => Promise<unknown>, opts: unknown) => Promise<unknown>;
	closing?: unknown;
	closed?: unknown;
}

/**
 * Works around a BullMQ bug (verified against bullmq 6.3.8 and 6.3.9, `dist/cjs/classes/worker.js`
 * `Worker.retryIfFailed`): it retries a failing call forever when the error is a *connection*
 * error (`maxRetries` defaults to `Infinity`), and it skips its `delayInMs` pause once the worker
 * is `closing`/`closed`. So a job whose processor returns while or after the worker closes and
 * whose `moveToCompleted`/`moveToFailed` then fails with a connection error ("Connection is
 * closed." because `closeWorkers` already closed the connections, or `ECONNREFUSED` with Redis
 * down) re-runs that call in a zero-delay loop: the process spins at 100% CPU, timers never fire
 * and it never exits. Same for the fetch loop (`_getNextJob`) and a scheduler's next-iteration
 * upsert.
 *
 * `retryIfFailed` is private API, hence the guarded cast: if a future BullMQ drops or renames
 * it, this silently does nothing. The wrapper lets the FIRST attempt of every call through
 * untouched and leaves retries alone while the worker is running (BullMQ keeps its delay then,
 * so a Redis blip mid-run still recovers). Only a retry once our transport or the worker is
 * closing throws {@link WorkerClosedError}, a non-connection error, which makes `retryIfFailed`
 * rethrow (or return, with `onlyEmitError`) and ends the loop.
 *
 * Abandoning that BullMQ call is safe (decision 1: BullMQ is only transport): the job stays
 * active in Redis until its lock expires and the stalled check of a live worker moves it back to
 * `wait`, where its redelivery is a no-op `acquire` (stale `enqueue_seq`, decision 5). The run
 * itself was already settled in Postgres (completed/failed/suspended) or handed back (`queued`,
 * decision 20), and a lost re-enqueue is recovered by the reconciler.
 */
function guardRetryLoopOnClose(worker: Worker, transportClosing: () => boolean): void {
	const internals = worker as unknown as WorkerRetryInternals;
	const original = internals.retryIfFailed;
	if (typeof original !== "function") return;
	internals.retryIfFailed = function guardedRetryIfFailed(
		this: unknown,
		fn: () => Promise<unknown>,
		opts: unknown
	): Promise<unknown> {
		let attempts = 0;
		const guarded = (): Promise<unknown> => {
			attempts++;
			if (
				attempts > 1 &&
				(transportClosing() || Boolean(internals.closing) || Boolean(internals.closed))
			) {
				return Promise.reject(new WorkerClosedError());
			}
			return fn();
		};
		return original.call(this, guarded, opts);
	};
}

/** Minimum interval between two `warn`s for errors of the same `Queue`/`Worker`. */
const ERROR_LOG_INTERVAL_MS = 10_000;

/**
 * `schedule-tick` jobs (Phase 8) are the one deliberate exception to decision 4's `attempts: 1`
 * (which is about *run* jobs, not `-system` jobs -- see `CLAUDE.md`, Phase 8 notes): a tick that
 * fails (e.g. this process doesn't have the schedule's task registered mid rolling-deploy) is safe
 * to let BullMQ itself retry with backoff, because the run each tick creates is guarded by the
 * `__schedule:${id}:${ts}` idempotency key (decision 9) -- a retried tick that already succeeded
 * elsewhere is a harmless no-op `"existing"`, never a duplicate run.
 */
const SCHEDULE_TICK_ATTEMPTS = 5;
const SCHEDULE_TICK_BACKOFF_DELAY_MS = 1_000;

export function createTransport(options: CreateTransportOptions): Transport {
	const prefix = validateSqlIdentifier("prefix", options.prefix);
	const systemConcurrency = options.systemConcurrency ?? 5;
	const connections = options.connections;
	const schema = options.schema;

	const resolvedQueues = options.queues.map(q => ({ ...q, name: validateQueueName(q.name) }));
	const sysQueueName = systemQueueName(prefix);

	const logger = options.logger;
	/** Set when `closeWorkers` starts: errors after that are expected shutdown noise. */
	let closing = false;

	/**
	 * Every `Queue`/`Worker` gets an `error` listener: Node throws for an `error` event with no
	 * listener. Logged via `logger` (audit item d) instead of silenced: `warn` at most once per
	 * {@link ERROR_LOG_INTERVAL_MS} per emitter (a Redis outage makes BullMQ emit on every
	 * reconnect attempt), with the number of suppressed repeats; `debug` while closing.
	 */
	function logErrors(
		emitter: { on(event: "error", listener: (err: Error) => void): unknown },
		kind: "queue" | "worker",
		queue: string
	): void {
		let lastLoggedAt = 0;
		let suppressed = 0;
		emitter.on("error", err => {
			if (!logger) return;
			try {
				if (closing) {
					logger.debug({ err, queue, kind }, `BullMQ ${kind} error during shutdown`);
					return;
				}
				const now = Date.now();
				if (now - lastLoggedAt < ERROR_LOG_INTERVAL_MS) {
					suppressed++;
					return;
				}
				lastLoggedAt = now;
				const fields = { err, queue, kind, ...(suppressed > 0 ? { suppressed } : {}) };
				suppressed = 0;
				logger.warn(fields, `BullMQ ${kind} error on queue "${queue}"`);
			} catch {
				// A broken injected logger must never throw from an EventEmitter listener.
			}
		});
	}

	function openTaskQueue(name: string): Queue<RunJobData> {
		const queue = new Queue<RunJobData>(name, { connection: connections.shared(), prefix });
		logErrors(queue, "queue", name);
		taskQueues.set(name, queue);
		return queue;
	}

	const taskQueues = new Map<string, Queue<RunJobData>>();
	for (const q of resolvedQueues) openTaskQueue(q.name);
	const systemQueue = new Queue(sysQueueName, { connection: connections.shared(), prefix });
	logErrors(systemQueue, "queue", sysQueueName);

	const workers: Worker[] = [];
	// Keyed by `${queueLabel}:${jobId}` so a run queue and the system queue can never collide.
	const activeJobs = new Map<string, StillActiveJob>();

	/**
	 * The `Queue` handle of a task queue. A queue this process doesn't declare (audit item 10: a
	 * rolling deploy where the leader still runs the old code and has to re-enqueue runs of a queue
	 * only the new code knows) gets a producer-only `Queue` on demand, on the same shared
	 * connection (decision 14: no new connection, nothing to close beyond the `Queue` itself) and
	 * prefix. It never gets a worker (decision 19: `startWorkers` only covers declared queues).
	 * The name is validated like a declared one (`ConfigError` otherwise).
	 */
	function getTaskQueue(queue: string): Queue<RunJobData> {
		return taskQueues.get(queue) ?? openTaskQueue(validateQueueName(queue));
	}

	function trackActive<T>(
		label: string,
		jobId: string | undefined,
		run: () => Promise<T>
	): Promise<T> {
		const key = `${label}:${jobId ?? ""}`;
		activeJobs.set(key, { queue: label, jobId: jobId ?? "" });
		return run().finally(() => {
			activeJobs.delete(key);
		});
	}

	async function enqueueRun(input: EnqueueRunInput): Promise<void> {
		const q = getTaskQueue(input.queue);
		const jobId = runJobId(input.runId, input.seq);
		// Adding an existing jobId is idempotent in BullMQ: while a prior job
		// instance with this id still exists in Redis, `add()` silently
		// resolves without creating a duplicate (api-notes.md §1.6).
		await q.add(
			RUN_JOB_NAME,
			{ runId: input.runId, seq: input.seq },
			{
				jobId,
				attempts: 1,
				removeOnComplete: true,
				removeOnFail: true,
				...(input.delayMs !== undefined ? { delay: input.delayMs } : {}),
			}
		);
	}

	async function removeRunJob(queue: string, runId: string, seq: number): Promise<void> {
		try {
			const q = getTaskQueue(queue);
			// `Queue.remove` returns 0 (not a throw) for "locked"/"not found"; the
			// try/catch below is only a safety net for unexpected connection errors.
			await q.remove(runJobId(runId, seq));
		} catch {
			// Best-effort.
		}
	}

	async function enqueueSystem(
		name: string,
		data: unknown,
		opts: EnqueueSystemOptions = {}
	): Promise<void> {
		await systemQueue.add(name, data, {
			attempts: 1,
			removeOnComplete: true,
			removeOnFail: true,
			...(opts.jobId !== undefined ? { jobId: opts.jobId } : {}),
			...(opts.delayMs !== undefined ? { delay: opts.delayMs } : {}),
		});
	}

	async function removeSystemJob(jobId: string): Promise<void> {
		try {
			await systemQueue.remove(jobId);
		} catch {
			// Best-effort.
		}
	}

	async function upsertJobScheduler(input: UpsertJobSchedulerInput): Promise<void> {
		await systemQueue.upsertJobScheduler(
			input.id,
			{ pattern: input.pattern, ...(input.timezone !== undefined ? { tz: input.timezone } : {}) },
			{
				name: SCHEDULE_TICK_JOB_NAME,
				data: { scheduleId: input.id, ...(schema !== undefined ? { schema } : {}) },
				opts: {
					removeOnComplete: true,
					removeOnFail: true,
					// Fix 2/3: see the module-level comment on `SCHEDULE_TICK_ATTEMPTS`.
					attempts: SCHEDULE_TICK_ATTEMPTS,
					backoff: { type: "exponential", delay: SCHEDULE_TICK_BACKOFF_DELAY_MS },
				},
			}
		);
	}

	async function removeJobScheduler(id: string): Promise<boolean> {
		return systemQueue.removeJobScheduler(id);
	}

	function schedulerInfo(
		id: string,
		scheduler: { pattern?: string; tz?: string; template?: { data?: unknown } }
	): JobSchedulerInfo {
		const data = scheduler.template?.data;
		const taggedSchema =
			data !== null &&
			typeof data === "object" &&
			typeof (data as { schema?: unknown }).schema === "string"
				? (data as { schema: string }).schema
				: undefined;
		// `exactOptionalPropertyTypes`: `schema` must be omitted, not set to `undefined`, when untagged.
		return {
			id,
			pattern: scheduler.pattern,
			timezone: scheduler.tz,
			...(taggedSchema !== undefined ? { schema: taggedSchema } : {}),
		};
	}

	async function getJobScheduler(id: string): Promise<JobSchedulerInfo | undefined> {
		const scheduler = await systemQueue.getJobScheduler(id);
		if (!scheduler) return undefined;
		return schedulerInfo(id, scheduler);
	}

	async function listJobSchedulers(): Promise<JobSchedulerInfo[]> {
		// Default `start=0, end=-1` (BullMQ's own default) returns every scheduler.
		const schedulers = await systemQueue.getJobSchedulers();
		return schedulers.map(s => schedulerInfo(s.key, s));
	}

	async function startWorkers(opts: StartWorkersOptions): Promise<void> {
		const names = opts.queues;
		const targets = names ? resolvedQueues.filter(q => names.includes(q.name)) : resolvedQueues;
		if (workers.length > 0) {
			throw new ConfigError("startWorkers() was already called on this transport");
		}

		if (names) {
			const known = new Set(resolvedQueues.map(q => q.name));
			const unknown = names.filter(n => !known.has(n));
			if (unknown.length > 0) {
				throw new ConfigError(
					`start({ queues }) references unknown queue(s): ${unknown.join(", ")}`
				);
			}
		}

		try {
			await createWorkers(opts, targets);
		} catch (error) {
			// Leave nothing half-started behind, so a later startWorkers() can be retried.
			const partial = workers.splice(0);
			await Promise.allSettled(partial.map(w => w.close()));
			throw error;
		}
	}

	/**
	 * Review 3: `closeWorkers()` may run while `createWorkers` awaits (a stop() that gave up on a
	 * slow start()); a Worker created after it would never be closed.
	 */
	function assertNotClosing(): void {
		if (closing) {
			throw new MinitriggerError(
				"the transport is closing (engine.stop() was called); no new worker"
			);
		}
	}

	async function createWorkers(
		opts: StartWorkersOptions,
		targets: typeof resolvedQueues
	): Promise<void> {
		for (const qc of targets) {
			assertNotClosing();
			const worker = new Worker<RunJobData>(
				qc.name,
				job => trackActive(qc.name, job.id, () => opts.processRun(qc.name, job.data, job)),
				{
					connection: connections.dedicated("worker"),
					prefix,
					concurrency: qc.concurrency,
					maxStalledCount: NEVER_FAIL_ON_STALL,
					lockDuration: LOCK_DURATION_MS,
					stalledInterval: STALLED_INTERVAL_MS,
					...(qc.rateLimit
						? { limiter: { max: qc.rateLimit.max, duration: qc.rateLimit.durationMs } }
						: {}),
				}
			);
			logErrors(worker, "worker", qc.name);
			guardRetryLoopOnClose(worker, () => closing);
			workers.push(worker);

			// Global concurrency lives in Redis and outlives this process: a queue that no longer
			// declares one must drop a value set by an earlier deploy.
			const queueHandle = getTaskQueue(qc.name);
			if (qc.globalConcurrency !== undefined) {
				await queueHandle.setGlobalConcurrency(qc.globalConcurrency);
			} else {
				await queueHandle.removeGlobalConcurrency();
			}
		}

		// The system worker always starts (decision 8): `queues` only filters task queues.
		assertNotClosing();
		const systemWorker = new Worker(
			sysQueueName,
			job =>
				trackActive(SYSTEM_QUEUE_LABEL, job.id, () =>
					opts.processSystem(job.name, job.data, job).catch((error: unknown) => {
						// The handler already moved the job with `job.moveToDelayed`; BullMQ needs its
						// own `DelayedError` to leave it there instead of failing it.
						if (error instanceof SystemJobDelayed) throw new DelayedError();
						throw error;
					})
				),
			{
				connection: connections.dedicated("worker"),
				prefix,
				concurrency: systemConcurrency,
				maxStalledCount: NEVER_FAIL_ON_STALL,
				lockDuration: LOCK_DURATION_MS,
				stalledInterval: STALLED_INTERVAL_MS,
			}
		);
		logErrors(systemWorker, "worker", sysQueueName);
		guardRetryLoopOnClose(systemWorker, () => closing);
		workers.push(systemWorker);
	}

	async function closeWorkers(opts: CloseWorkersOptions): Promise<CloseWorkersResult> {
		closing = true;
		// `Worker.close()` stops fetching new jobs immediately and then waits
		// (unboundedly -- api-notes.md §1.11) for in-flight jobs. Race it
		// against the deadline instead of forcing it, per decision 20.
		const closePromises = workers.map(w => w.close());
		const settled = Promise.all(closePromises);
		const timedOut = Symbol("timeout");
		let timer: ReturnType<typeof setTimeout> | undefined;
		const deadline = new Promise<typeof timedOut>(resolve => {
			timer = setTimeout(() => resolve(timedOut), opts.timeoutMs);
		});
		// Whether or not we waited for them, never leave the close() calls as
		// unhandled rejections once they do eventually settle.
		const settledQuietly = settled.then(
			() => undefined,
			() => undefined
		);
		const raceResult = await Promise.race([settledQuietly, deadline]);
		// Never leave a pending shutdown-deadline timer behind: it would keep the process alive
		// for up to `timeoutMs` after a stop that finished early.
		clearTimeout(timer);

		const stillActive = raceResult === timedOut ? Array.from(activeJobs.values()) : [];
		if (opts.beforeClose) {
			// Logged, never rethrown: the workers, queues and connections must still close.
			await opts.beforeClose(stillActive).catch((error: unknown) => {
				try {
					logger?.warn(
						{ err: error, stillActive: stillActive.length },
						"closeWorkers: beforeClose failed; closing the workers anyway"
					);
				} catch {
					// A broken injected logger must not abort the close either.
				}
			});
		}
		// Redis not reachable (engine stopping during an outage): nothing BullMQ does from here on
		// can complete, so don't wait the close grace out; force-close the workers and bound the
		// queue closes instead, so `stop()` stays fast.
		const redisDown = connections.shared().status !== "ready";
		if (raceResult === timedOut && !redisDown) {
			// The processors were just released by `beforeClose`: give the workers a short, bounded
			// chance to finish closing before their connections go away.
			await bounded(settledQuietly, WORKER_CLOSE_GRACE_MS);
		}
		if (redisDown) {
			for (const w of workers) void w.close(true).catch(() => {});
		}

		const queueCloses = Promise.allSettled(
			[...taskQueues.values(), systemQueue].map(q => q.close())
		);
		await (redisDown ? bounded(queueCloses, REDIS_DOWN_CLOSE_BOUND_MS) : queueCloses);
		await connections.closeAll();

		workers.length = 0;
		return { stillActive };
	}

	async function checkRedis(): Promise<string | undefined> {
		return checkRedisEvictionPolicy(connections.shared());
	}

	/** Decision 27: a task queue's `Queue` handle for pause/resume; never the `-system` queue. */
	function pausableQueue(queue: string): Queue<RunJobData> {
		if (queue === sysQueueName) {
			throw new ConfigError(`the internal system queue "${queue}" cannot be paused`);
		}
		return getTaskQueue(queue);
	}

	function taskWorker(queue: string): Worker | undefined {
		return queue === sysQueueName ? undefined : workers.find(w => w.name === queue);
	}

	return {
		enqueueRun,
		removeRunJob,
		enqueueSystem,
		removeSystemJob,
		upsertJobScheduler,
		removeJobScheduler,
		getJobScheduler,
		listJobSchedulers,
		startWorkers,
		closeWorkers,
		checkRedis,
		async pauseQueue(queue) {
			await pausableQueue(queue).pause();
		},
		async resumeQueue(queue) {
			await pausableQueue(queue).resume();
		},
		async isQueuePaused(queue) {
			return pausableQueue(queue).isPaused();
		},
		workerConcurrency(queue) {
			return taskWorker(queue)?.concurrency;
		},
		setWorkerConcurrency(queue, concurrency) {
			const worker = taskWorker(queue);
			if (!worker) return false;
			worker.concurrency = concurrency;
			return true;
		},
	};
}
