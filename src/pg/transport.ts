/**
 * `Transport` implementation over Postgres (`runs`, `system_jobs`, batched `NOTIFY`; decision 28,
 * `docs/superpowers/specs/2026-09-26-postgres-transport-design.md` §3.4-§3.6). There is no queue
 * to enqueue into: a run's row already carries its own `run_after` (written by the store before
 * `enqueueRun` is ever called), and a claim loop (`src/pg/workers.ts`) picks up due rows on its
 * own; this module only wires those loops to the shared `Transport` interface (`src/queue/
 * transport.ts`) so the rest of the engine (executor, reconciler, schedules, queue control) never
 * has to know which transport it is talking to.
 *
 * No `bullmq`/`ioredis` at runtime (only `import type`, erased by `verbatimModuleSyntax`): this
 * file is part of the Postgres backend, which must load without either optional peer installed
 * (`docs/superpowers/sdd/2026-09-26-postgres-transport/constraints.md`). No module-level state
 * (decision 13): everything lives in the closure `createPgTransport` returns.
 */

import { randomUUID } from "node:crypto";
import type { Job } from "bullmq";
import type { EngineLogger } from "../config.js";
import { ConfigError, MinitriggerError } from "../errors.js";
import type {
	CloseWorkersOptions,
	CloseWorkersResult,
	EnqueueRunInput,
	EnqueueSystemOptions,
	JobSchedulerInfo,
	ResolvedQueueConfig,
	StartWorkersOptions,
	StillActiveJob,
	Transport,
	UpsertJobSchedulerInput,
} from "../queue/transport.js";
import type { SqlStore } from "../store/sql/index.js";
import type { Json } from "../store/types.js";
import { channelName } from "./listener.js";
import type { NotifyBatcher } from "./notify-batcher.js";
import { nextOccurrence, scheduleTickJobId } from "./schedulers.js";
import {
	createQueueWorker,
	createSystemWorker,
	parseScheduleTickData,
	type QueueWorker,
	type SystemWorker,
	type WorkerPolling,
} from "./workers.js";

/** BullMQ job **name** the `-system` dispatcher switches on for a schedule's recurring tick. */
const SCHEDULE_TICK_NAME = "schedule-tick";

/** The label used for the `-system` loop's claim rows in {@link StillActiveJob}, mirroring the
 * BullMQ transport's `-system` queue label (`src/queue/transport.ts`'s `SYSTEM_QUEUE_LABEL`). */
const SYSTEM_QUEUE_LABEL = "-system";

/**
 * Exponential backoff for a `system_jobs` row that failed its handler (§3.5: "abaixo de
 * max_attempts → run_after = now() + backoff exponencial"; the implementation plan's
 * `SystemWorkerOptions.backoffMs` doc pins the exact formula: "exponencial 1 s * 2^attempt, máx
 * 60 s", the same one `test/unit/pg-workers.test.ts` already exercises this function against).
 */
const SYSTEM_JOB_BACKOFF_BASE_MS = 1_000;
const SYSTEM_JOB_BACKOFF_MAX_MS = 60_000;
function systemJobBackoffMs(attempt: number): number {
	return Math.min(SYSTEM_JOB_BACKOFF_MAX_MS, SYSTEM_JOB_BACKOFF_BASE_MS * 2 ** attempt);
}

export interface PgTransportOptions {
	store: SqlStore;
	batcher: NotifyBatcher;
	schema: string;
	queues: ResolvedQueueConfig[];
	systemConcurrency: number;
	stallTimeoutMs: number;
	polling: WorkerPolling;
	/** Fed by the wake channel's listener (Task 10b): `queue` is the (untrusted) NOTIFY payload. */
	wake: { subscribe(handler: (queue: string | undefined) => void): () => void };
	logger: EngineLogger;
}

/**
 * Reads a `schedule-tick:<id>` row back as a {@link JobSchedulerInfo}, or `undefined` (and a
 * logged `error`) if its `data` does not parse as a {@link ScheduleTickData} -- these rows are
 * only ever written by {@link createPgTransport}'s own `upsertJobScheduler`, so a parse failure
 * here means corruption, not a normal outcome; treating it as "no scheduler" is the safe default
 * for callers like the schedules reconciler.
 */
function toSchedulerInfo(
	jobId: string,
	data: Json,
	logger: EngineLogger
): JobSchedulerInfo | undefined {
	const parsed = parseScheduleTickData(data);
	if (!parsed) {
		logger.error({ jobId, data }, "schedule-tick row has invalid data; ignoring it as a scheduler");
		return undefined;
	}
	return {
		id: parsed.scheduleId,
		pattern: parsed.cron,
		timezone: parsed.timezone,
		schema: parsed.schema,
	};
}

export function createPgTransport(options: PgTransportOptions): Transport {
	const { store, batcher, schema, systemConcurrency, stallTimeoutMs, polling, logger } = options;
	const wakeChannel = channelName("wake", schema);

	// Already blocked at the config layer (a queue's `rateLimit` is rejected before an engine ever
	// reaches here); this is a defensive backstop in case a caller builds the transport directly.
	for (const q of options.queues) {
		if (q.rateLimit) {
			throw new ConfigError(
				`queue "${q.name}": rateLimit is not supported with transport "postgres" (decision 28(f))`
			);
		}
	}

	let queueWorkers: Map<string, QueueWorker> | undefined;
	let systemWorker: SystemWorker | undefined;
	let unsubscribeWake: (() => void) | undefined;
	/** Set at the top of `closeWorkers`; permanent (mirrors the BullMQ transport's own `closing`). */
	let closing = false;

	/** Mirrors the BullMQ transport's `assertNotClosing()`: same error type and message. */
	function assertNotClosing(): void {
		if (closing) {
			throw new MinitriggerError(
				"the transport is closing (engine.stop() was called); no new worker"
			);
		}
	}

	async function enqueueRun(input: EnqueueRunInput): Promise<void> {
		// The row's `run_after` (delayed or not) was already written by the store; the NOTIFY is
		// only a hint so a worker recomputes `min(run_after)` instead of waiting out a poll interval
		// (decision 28(b)). `batcher.notify` is synchronous and documented to never throw.
		batcher.notify(wakeChannel, input.queue);
	}

	async function removeRunJob(): Promise<void> {
		// No-op: the run *is* the row (decision 28's claim reads straight off `runs`; there is no
		// separate job to remove).
	}

	async function enqueueSystem(
		name: string,
		data: unknown,
		opts: EnqueueSystemOptions = {}
	): Promise<void> {
		await store.systemJobs.upsert({
			jobId: opts.jobId ?? randomUUID(),
			name,
			data: data as Json,
			maxAttempts: 1,
			...(opts.delayMs !== undefined ? { delayMs: opts.delayMs } : {}),
		});
		// Same-process wake only (§3.5/Task 8 mapping): another process's `-system` worker picks
		// this up on its own poll, bounded by `polling.intervalMs`/`nextDueInMs`.
		systemWorker?.wake();
	}

	async function removeSystemJob(jobId: string): Promise<void> {
		// Never removes a currently-locked (active) row -- decision 10, same as the BullMQ transport.
		// Best-effort (Transport contract, src/queue/transport.ts): callers (execute-run.ts's
		// after-resolution cleanup, the reconciler's case 5) don't wrap this themselves.
		try {
			await store.systemJobs.remove(jobId);
		} catch (error) {
			logger.debug({ jobId, err: error }, "removeSystemJob failed; best-effort, ignoring");
		}
	}

	async function upsertJobScheduler(input: UpsertJobSchedulerInput): Promise<void> {
		const dbNow = await store.systemJobs.now();
		const runAt = nextOccurrence(input.pattern, input.timezone, dbNow);
		await store.systemJobs.upsertScheduleTick({
			jobId: scheduleTickJobId(input.id),
			data: {
				scheduleId: input.id,
				cron: input.pattern,
				schema,
				prevMillis: runAt.getTime(),
				...(input.timezone !== undefined ? { timezone: input.timezone } : {}),
			},
			runAt,
		});
	}

	async function removeJobScheduler(id: string): Promise<boolean> {
		return store.systemJobs.remove(scheduleTickJobId(id), { evenIfLocked: true });
	}

	async function getJobScheduler(id: string): Promise<JobSchedulerInfo | undefined> {
		const rec = await store.systemJobs.get(scheduleTickJobId(id));
		if (!rec) return undefined;
		return toSchedulerInfo(rec.jobId, rec.data, logger);
	}

	async function listJobSchedulers(): Promise<JobSchedulerInfo[]> {
		const rows = await store.systemJobs.listByName(SCHEDULE_TICK_NAME);
		const result: JobSchedulerInfo[] = [];
		for (const rec of rows) {
			const info = toSchedulerInfo(rec.jobId, rec.data, logger);
			if (info) result.push(info);
		}
		return result;
	}

	async function startWorkers(opts: StartWorkersOptions): Promise<void> {
		assertNotClosing();
		if (queueWorkers) {
			throw new ConfigError("startWorkers() was already called on this transport");
		}
		const names = opts.queues;
		const targets = names ? options.queues.filter(q => names.includes(q.name)) : options.queues;
		if (names) {
			const known = new Set(options.queues.map(q => q.name));
			const unknown = names.filter(n => !known.has(n));
			if (unknown.length > 0) {
				throw new ConfigError(
					`start({ queues }) references unknown queue(s): ${unknown.join(", ")}`
				);
			}
		}

		const workers = new Map<string, QueueWorker>();
		for (const q of targets) {
			const hasGlobal = q.globalConcurrency !== undefined;
			const worker = createQueueWorker({
				queue: q.name,
				concurrency: q.concurrency,
				...(hasGlobal ? { globalConcurrency: q.globalConcurrency } : {}),
				claim: store.claim,
				process: run => {
					const data = { runId: run.id, seq: run.enqueueSeq, claimed: run };
					const fakeJob = { id: `${run.id}-${run.enqueueSeq}`, data };
					return opts.processRun(q.name, data, fakeJob as unknown as Job);
				},
				// The worker that frees a slot on a globally-limited queue wakes every process
				// waiting on it (decision 28(e)); an unlimited queue has no such shared cap to free.
				...(hasGlobal ? { onSlotFreed: () => batcher.notify(wakeChannel, q.name) } : {}),
				polling,
				logger,
			});
			workers.set(q.name, worker);
		}

		const sysWorker = createSystemWorker({
			concurrency: systemConcurrency,
			systemJobs: store.systemJobs,
			lockMs: stallTimeoutMs,
			process: opts.processSystem,
			backoffMs: systemJobBackoffMs,
			polling,
			logger,
		});

		queueWorkers = workers;
		systemWorker = sysWorker;

		function wakeAllQueues(): void {
			for (const w of workers.values()) w.wake();
		}

		// The NOTIFY payload is a queue name, but it is untrusted (any role with CONNECT can
		// publish on the channel, decision 28(b)): a hit against a queue this process actually
		// started wakes just that one; anything else (unknown queue, `undefined` from a fresh
		// listener (re)connect) wakes every queue this process runs.
		unsubscribeWake = options.wake.subscribe(queue => {
			if (queue !== undefined && workers.has(queue)) {
				workers.get(queue)?.wake();
				return;
			}
			wakeAllQueues();
		});

		for (const w of workers.values()) w.start();
		sysWorker.start();
	}

	async function closeWorkers(opts: CloseWorkersOptions): Promise<CloseWorkersResult> {
		// Permanent: once a transport starts closing it never accepts a new `startWorkers()` (item
		// 3, fix round 1) -- set unconditionally, even on the "workers were never started" path
		// below, so a `startWorkers()` racing a `stop()` called before `start()` finished is refused
		// too.
		closing = true;
		unsubscribeWake?.();
		unsubscribeWake = undefined;
		const workers = queueWorkers;
		const sysWorker = systemWorker;
		queueWorkers = undefined;
		systemWorker = undefined;

		let stillActive: StillActiveJob[] = [];
		if (workers && sysWorker) {
			for (const w of workers.values()) w.stopClaiming();
			sysWorker.stopClaiming();

			// Drained in parallel, each bounded independently by `timeoutMs`, so the total wall time
			// is ~timeoutMs regardless of how many queues this process runs (not N * timeoutMs).
			const [queueDrains, systemStragglers] = await Promise.all([
				Promise.all(
					[...workers.entries()].map(async ([queue, w]) => ({
						queue,
						items: await w.drain(opts.timeoutMs),
					}))
				),
				sysWorker.drain(opts.timeoutMs),
			]);

			const active: StillActiveJob[] = [];
			for (const { queue, items } of queueDrains) {
				for (const run of items) {
					active.push({ queue, jobId: `${run.id}-${run.enqueueSeq}` });
				}
			}
			for (const rec of systemStragglers) {
				active.push({ queue: SYSTEM_QUEUE_LABEL, jobId: rec.jobId });
			}
			stillActive = active;
		}

		// Always called, even when `startWorkers` never ran (item 2, fix round 1): the engine does
		// leader/pub-sub/subscription shutdown inside `beforeClose` regardless of whether this
		// process ever claimed a run (`src/engine.ts`'s `stop()`).
		if (opts.beforeClose) {
			await opts.beforeClose(stillActive).catch((error: unknown) => {
				try {
					logger.warn(
						{ err: error, stillActive: stillActive.length },
						"closeWorkers: beforeClose failed; closing the workers anyway"
					);
				} catch {
					// A broken injected logger must not abort the close either.
				}
			});
		}

		return { stillActive };
	}

	async function checkRedis(): Promise<string | undefined> {
		return undefined;
	}

	function taskWorker(queue: string): QueueWorker | undefined {
		return queueWorkers?.get(queue);
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
		// Pause (decision 27) is read straight from `queue_state` by the claim query itself; there
		// is no separate flag to flip here.
		async pauseQueue() {},
		async resumeQueue(queue) {
			// Called after `queue_state` committed: wake every process's worker of the queue now
			// instead of at its next poll (`polling.intervalMs`). Never throws (see enqueueRun).
			batcher.notify(wakeChannel, queue);
		},
		async isQueuePaused(queue) {
			const rec = await store.queues.get(queue);
			return rec?.paused ?? false;
		},
		workerConcurrency(queue) {
			return taskWorker(queue)?.concurrency;
		},
		setWorkerConcurrency(queue, concurrency) {
			const worker = taskWorker(queue);
			if (!worker) return false;
			worker.setConcurrency(concurrency);
			return true;
		},
	};
}
