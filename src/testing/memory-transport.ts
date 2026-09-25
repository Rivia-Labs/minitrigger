/**
 * In-memory {@link Transport} of the test mode (Phase 9.2): the same contract as the BullMQ
 * transport (`queue/transport.ts`), without Redis. It is only a job table: jobs are never processed
 * on their own; the test engine (`testing/test-engine.ts`) takes due jobs one at a time and runs
 * them through the real executor / `-system` dispatcher.
 *
 * BullMQ semantics kept on purpose (the engine relies on them):
 * - jobId `${runId}-${seq}` for run jobs (decision 7); adding a jobId that still exists (waiting,
 *   delayed or active) is a no-op (`api-notes.md` §1.6);
 * - `removeRunJob`/`removeSystemJob` are best-effort and never remove an active job;
 * - a delay is relative to the injected clock (the "DB clock" of test mode), never `Date.now()`.
 */
import { ConfigError } from "../errors.js";
import { runJobId, SCHEDULE_TICK_JOB_NAME, systemQueueName } from "../queue/ids.js";
import type {
	CloseWorkersResult,
	EnqueueRunInput,
	EnqueueSystemOptions,
	JobSchedulerInfo,
	ResolvedQueueConfig,
	Transport,
	UpsertJobSchedulerInput,
} from "../queue/transport.js";
import { validateQueueName, validateSqlIdentifier } from "../utils/identifiers.js";

/** One job of the in-memory transport. */
export interface MemoryJob {
	readonly kind: "run" | "system";
	/** Task queue name, or the `-system` queue name. */
	readonly queue: string;
	readonly id: string;
	/** `run` for run jobs; the system job name otherwise. */
	readonly name: string;
	readonly data: unknown;
	/** Extra BullMQ-like job options (`prevMillis` of a schedule tick). */
	readonly opts: { prevMillis?: number };
	/** Clock ms when the job was added. */
	readonly timestamp: number;
	/** Clock ms when the job becomes due. */
	dueAt: number;
	/** Insertion order (FIFO among jobs due at the same time). */
	readonly order: number;
	active: boolean;
}

export interface MemoryTransportOptions {
	prefix: string;
	queues: ResolvedQueueConfig[];
	/** Clock in epoch ms (the same clock as the memory store). */
	now: () => number;
}

export interface MemoryTransport extends Transport {
	/**
	 * Marks the next due job (`dueAt <= now()`, earliest first, then FIFO) active and returns it;
	 * `undefined` when nothing is due. The caller must call {@link finish} when done with it.
	 */
	takeDue(): MemoryJob | undefined;
	/** Removes a job taken with {@link takeDue}, or moves it back to delayed at `redelayAt`. */
	finish(job: MemoryJob, redelayAt?: number): void;
	/** Earliest `dueAt` among the waiting/delayed (not active) jobs of `kind` (any kind if omitted). */
	nextDueAt(kind?: MemoryJob["kind"]): number | undefined;
	/** Every job currently held (waiting, delayed or active), earliest first. */
	jobs(): readonly MemoryJob[];
	/**
	 * Adds a `schedule-tick` job, as a BullMQ job scheduler would for the occurrence `atMs`, and
	 * returns its job id (so the caller can track that specific job, e.g. `TestEngine.tickSchedule`
	 * collecting its failure).
	 */
	enqueueScheduleTick(scheduleId: string, atMs: number): string;
}

const RUN_JOB_NAME = "run";

export function createMemoryTransport(options: MemoryTransportOptions): MemoryTransport {
	const prefix = validateSqlIdentifier("prefix", options.prefix);
	const { now } = options;
	const sysQueue = systemQueueName(prefix);
	const queueNames = new Set(options.queues.map(q => validateQueueName(q.name)));
	/** Keyed by `${queue}\0${jobId}`, so a run queue and the system queue never collide. */
	const table = new Map<string, MemoryJob>();
	const schedulers = new Map<string, JobSchedulerInfo>();
	let order = 0;
	let autoId = 0;

	const keyOf = (queue: string, id: string) => `${queue}\u0000${id}`;

	function add(
		kind: MemoryJob["kind"],
		queue: string,
		id: string,
		name: string,
		data: unknown,
		delayMs: number | undefined,
		opts: MemoryJob["opts"] = {}
	): void {
		const key = keyOf(queue, id);
		// BullMQ: adding an id that still exists (in any state) is silently ignored.
		if (table.has(key)) return;
		const timestamp = now();
		table.set(key, {
			kind,
			queue,
			id,
			name,
			// Job data goes through JSON in Redis.
			data: data === undefined ? undefined : JSON.parse(JSON.stringify(data)),
			opts,
			timestamp,
			dueAt: timestamp + Math.max(0, delayMs ?? 0),
			order: order++,
			active: false,
		});
	}

	function remove(queue: string, id: string): void {
		const key = keyOf(queue, id);
		const job = table.get(key);
		// BullMQ never removes a locked (active) job.
		if (job && !job.active) table.delete(key);
	}

	function sorted(): MemoryJob[] {
		return Array.from(table.values()).sort((a, b) => a.dueAt - b.dueAt || a.order - b.order);
	}

	return {
		async enqueueRun(input: EnqueueRunInput): Promise<void> {
			if (!queueNames.has(input.queue)) {
				throw new ConfigError(
					`unknown queue "${input.queue}": it is not one of the queues this engine was configured with`
				);
			}
			add(
				"run",
				input.queue,
				runJobId(input.runId, input.seq),
				RUN_JOB_NAME,
				{ runId: input.runId, seq: input.seq },
				input.delayMs
			);
		},

		async removeRunJob(queue, runId, seq) {
			remove(queue, runJobId(runId, seq));
		},

		async enqueueSystem(name, data, opts: EnqueueSystemOptions = {}) {
			add("system", sysQueue, opts.jobId ?? `memory-${++autoId}`, name, data, opts.delayMs);
		},

		async removeSystemJob(jobId) {
			remove(sysQueue, jobId);
		},

		async upsertJobScheduler(input: UpsertJobSchedulerInput) {
			// Ticks are not produced on their own in test mode: see `TestEngine.tickSchedule`.
			schedulers.set(input.id, {
				id: input.id,
				pattern: input.pattern,
				timezone: input.timezone,
			});
		},

		async removeJobScheduler(id) {
			return schedulers.delete(id);
		},

		async getJobScheduler(id) {
			const info = schedulers.get(id);
			return info ? { ...info } : undefined;
		},

		async listJobSchedulers() {
			return Array.from(schedulers.values(), info => ({ ...info }));
		},

		async startWorkers() {
			throw new ConfigError(
				"the in-memory transport has no workers: the test engine processes jobs itself"
			);
		},

		async closeWorkers(opts): Promise<CloseWorkersResult> {
			await opts.beforeClose?.([]).catch(() => {});
			table.clear();
			return { stillActive: [] };
		},

		async checkRedis() {
			return undefined;
		},

		takeDue() {
			const t = now();
			for (const job of sorted()) {
				if (job.active) continue;
				if (job.dueAt > t) return undefined;
				job.active = true;
				return job;
			}
			return undefined;
		},

		finish(job, redelayAt) {
			job.active = false;
			if (redelayAt === undefined) {
				const key = keyOf(job.queue, job.id);
				if (table.get(key) === job) table.delete(key);
				return;
			}
			job.dueAt = redelayAt;
		},

		nextDueAt(kind) {
			for (const job of sorted()) {
				if (job.active || (kind !== undefined && job.kind !== kind)) continue;
				return job.dueAt;
			}
			return undefined;
		},

		jobs() {
			return sorted();
		},

		enqueueScheduleTick(scheduleId, atMs) {
			const id = `repeat:${scheduleId}:${atMs}`;
			add("system", sysQueue, id, SCHEDULE_TICK_JOB_NAME, { scheduleId }, 0, { prevMillis: atMs });
			return id;
		},
	};
}
