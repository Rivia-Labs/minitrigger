import { Job, Queue, Worker } from "bullmq";
import { ConfigError } from "../errors.js";
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
	 * re-enqueue them (decision 20), which needs the queues to still be open. Errors are ignored.
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

	const taskQueues = new Map<string, Queue<RunJobData>>();
	for (const q of resolvedQueues) {
		const queue = new Queue<RunJobData>(q.name, { connection: connections.shared(), prefix });
		silenceUnhandledErrors(queue);
		taskQueues.set(q.name, queue);
	}
	const systemQueue = new Queue(sysQueueName, { connection: connections.shared(), prefix });
	silenceUnhandledErrors(systemQueue);

	const workers: Worker[] = [];
	// Keyed by `${queueLabel}:${jobId}` so a run queue and the system queue can never collide.
	const activeJobs = new Map<string, StillActiveJob>();

	function getTaskQueue(queue: string): Queue<RunJobData> {
		const q = taskQueues.get(queue);
		if (!q) {
			throw new ConfigError(
				`unknown queue "${queue}": it is not one of the queues this engine was configured with`
			);
		}
		return q;
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
		const q = taskQueues.get(queue);
		if (!q) return; // Unknown queue: nothing to remove.
		try {
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

	async function createWorkers(
		opts: StartWorkersOptions,
		targets: typeof resolvedQueues
	): Promise<void> {
		for (const qc of targets) {
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
			silenceUnhandledErrors(worker);
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
		const systemWorker = new Worker(
			sysQueueName,
			job =>
				trackActive(SYSTEM_QUEUE_LABEL, job.id, () => opts.processSystem(job.name, job.data, job)),
			{
				connection: connections.dedicated("worker"),
				prefix,
				concurrency: systemConcurrency,
				maxStalledCount: NEVER_FAIL_ON_STALL,
				lockDuration: LOCK_DURATION_MS,
				stalledInterval: STALLED_INTERVAL_MS,
			}
		);
		silenceUnhandledErrors(systemWorker);
		workers.push(systemWorker);
	}

	async function closeWorkers(opts: CloseWorkersOptions): Promise<CloseWorkersResult> {
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
			await opts.beforeClose(stillActive).catch(() => {});
		}
		if (raceResult === timedOut) {
			// The processors were just released by `beforeClose`: give the workers a short, bounded
			// chance to finish closing before their connections go away.
			let graceTimer: ReturnType<typeof setTimeout> | undefined;
			await Promise.race([
				settledQuietly,
				new Promise<void>(resolve => {
					graceTimer = setTimeout(resolve, WORKER_CLOSE_GRACE_MS);
				}),
			]);
			clearTimeout(graceTimer);
		}

		await Promise.allSettled([...taskQueues.values(), systemQueue].map(q => q.close()));
		await connections.closeAll();

		workers.length = 0;
		return { stillActive };
	}

	async function checkRedis(): Promise<string | undefined> {
		return checkRedisEvictionPolicy(connections.shared());
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
	};
}

/**
 * `Queue`/`Worker` are `EventEmitter`s that emit `"error"` for connection-level
 * hiccups (e.g. a command racing an in-progress `closeWorkers`/shutdown). Node
 * throws an unhandled exception for an `"error"` event with zero listeners, so
 * every instance this transport creates gets a no-op listener -- these errors
 * are expected background noise around shutdown, not something the transport
 * itself can act on (the processor's own error handling, decision 4/20, is
 * what actually matters for correctness).
 */
function silenceUnhandledErrors(emitter: {
	on(event: "error", listener: (err: Error) => void): unknown;
}): void {
	emitter.on("error", () => {});
}
