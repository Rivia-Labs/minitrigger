/**
 * Worker loops of the Postgres-only transport (`docs/superpowers/specs/
 * 2026-09-26-postgres-transport-design.md` §3.4 and §3.5).
 *
 * - {@link createQueueWorker}: one adaptive claim loop per task queue over `RunClaimStore`.
 * - {@link createSystemWorker}: the same loop over `system_jobs`, handing each claimed row to the
 *   shared `-system` dispatcher as a minimal `Job`-like object, with every write fenced by the
 *   claim's `locked_by`.
 *
 * No `bullmq`/`ioredis` at runtime (only `import type`): this file is part of the Postgres backend,
 * which must load without either optional peer installed. No module-level state (decision 13).
 */

import type { Job } from "bullmq";
import type { EngineLogger } from "../config.js";
import { SystemJobDelayed } from "../executor/system.js";
import type { ProcessSystemFn } from "../queue/transport.js";
import type {
	Json,
	RunClaimStore,
	RunRecord,
	ScheduleTickData,
	SystemJobRecord,
	SystemJobStore,
} from "../store/types.js";
import { nextOccurrence, scheduleIdOfJob } from "./schedulers.js";

/** First claim-error backoff; doubles per consecutive failure up to {@link CLAIM_BACKOFF_MAX_MS}. */
const CLAIM_BACKOFF_MIN_MS = 500;
const CLAIM_BACKOFF_MAX_MS = 30_000;
/** At most one "claim failed" warning per worker in this window. */
const CLAIM_WARN_INTERVAL_MS = 10_000;
const SCHEDULE_TICK = "schedule-tick";
/** Shortest sleep after a pass that left free slots, so no claim loop can spin. */
export const MIN_IDLE_SLEEP_MS = 5;
/**
 * How long `drain()` waits for the claim statement in flight when `stopClaiming()` ran, on its own
 * budget (not `timeoutMs`, which bounds the executions): abandoning a slow claim would launch the
 * runs it brings after `stop()` resolved, outside the drain and the hand-back (spec §3.4). The
 * BullMQ path bounds the equivalent in-flight `acquire` the same way (`IN_FLIGHT_DRAIN_MS`).
 */
export const CLAIM_DRAIN_TIMEOUT_MS = 5_000;

export interface WorkerPolling {
	/** Longest sleep between two claims when nothing is due sooner. */
	intervalMs: number;
	/** Random extra sleep in `[0, jitterMs)`, so processes do not claim in lockstep. */
	jitterMs: number;
}

interface ClockInjection {
	now?: () => number;
	setTimeoutFn?: typeof setTimeout;
	random?: () => number;
}

export interface QueueWorkerOptions extends ClockInjection {
	queue: string;
	/** Mutable via `setConcurrency`. */
	concurrency: number;
	globalConcurrency?: number;
	claim: RunClaimStore;
	/** Runs an already claimed run (the executor's `claimed` entry point). */
	process(run: RunRecord): Promise<void>;
	/** With `globalConcurrency`: called at the end of each execution (the freed slot wakes other processes). */
	onSlotFreed?(): void;
	polling: WorkerPolling;
	logger: EngineLogger;
}

/** A claim loop; `T` is what it claims (runs for a task queue, rows for `-system`). */
export interface ClaimWorker<T> {
	start(): void;
	/** NOTIFY or external poll; repeated calls during a claim collapse into one extra pass. */
	wake(): void;
	setConcurrency(n: number): void;
	readonly concurrency: number;
	readonly inFlight: number;
	/** Nothing is claimed after this. */
	stopClaiming(): void;
	/**
	 * Waits for the claim in progress (and the launch of whatever it brought), then for the
	 * executions, up to `timeoutMs` in total. Resolves with what is still in flight afterwards.
	 */
	drain(timeoutMs: number): Promise<T[]>;
}

export type QueueWorker = ClaimWorker<RunRecord>;
/** Stragglers of the system worker are rows: their lock simply expires and another worker retries. */
export type SystemWorker = ClaimWorker<SystemJobRecord>;

export interface SystemWorkerOptions extends ClockInjection {
	concurrency: number;
	systemJobs: SystemJobStore;
	/** Lock duration of a claim (`stallTimeoutMs`). */
	lockMs: number;
	/** The `-system` dispatcher; receives a {@link PgSystemJob} as its `Job`. */
	process: ProcessSystemFn;
	/** Retry delay after failed attempt number `attempt` (0-based count of previous failures). */
	backoffMs(attempt: number): number;
	polling: WorkerPolling;
	logger: EngineLogger;
}

/** What the `-system` dispatcher receives as its `Job` under the Postgres transport. */
export interface PgSystemJob {
	id: string;
	name: string;
	data: unknown;
	opts: { prevMillis?: number; repeatJobKey?: string };
	timestamp: number;
	/** The claim's `locked_by` (fencing token). */
	token: string;
	repeatJobKey?: string;
	attemptsMade: number;
	moveToDelayed(timestamp: number, token?: string): Promise<void>;
}

interface LoopSpec<T> {
	label: Record<string, unknown>;
	concurrency: number;
	claim(limit: number): Promise<T[]>;
	/** Delay until the next future row, computed on the DB clock (decision 17); null = none. */
	nextDueInMs(): Promise<number | null>;
	process(item: T): Promise<void>;
	onProcessError(item: T, error: unknown): void;
	onFinished?(): void;
	polling: WorkerPolling;
	logger: EngineLogger;
	clock: Required<ClockInjection>;
}

/**
 * The shared loop: a single re-entrant `pass()` (never two concurrent passes); the promise of the
 * pass in progress is kept in `current` so `drain()` can wait for it.
 */
function createClaimLoop<T>(spec: LoopSpec<T>): ClaimWorker<T> {
	const { now, setTimeoutFn, random } = spec.clock;
	let concurrency = spec.concurrency;
	let inFlight = 0;
	let again = false;
	let started = false;
	let stopped = false;
	let failures = 0;
	let lastWarnAt: number | undefined;
	let timer: ReturnType<typeof setTimeout> | undefined;
	let current: Promise<void> | undefined;
	const active = new Set<{ item: T; done: Promise<void> }>();

	function pass(): void {
		if (!started || stopped) return;
		if (current) {
			again = true;
			return;
		}
		current = loop().finally(() => {
			current = undefined;
			if (again && !stopped) pass();
		});
	}

	async function loop(): Promise<void> {
		try {
			while (!stopped) {
				again = false;
				const free = concurrency - inFlight;
				if (free <= 0) return; // the end of an execution calls pass()
				const items = await spec.claim(free);
				failures = 0;
				// Launched even after stopClaiming(): the executor hands them back (isShuttingDown).
				for (const item of items) launch(item);
				if (stopped) return;
				if (items.length < free) {
					const dueInMs = await spec.nextDueInMs();
					if (stopped) return;
					// Never a zero sleep after an empty pass, even with `jitterMs: 0`: a row due "now"
					// by the DB clock is claimed on the next pass, not in a tight loop.
					const delay =
						dueInMs === null
							? spec.polling.intervalMs
							: Math.min(spec.polling.intervalMs, Math.max(MIN_IDLE_SLEEP_MS, dueInMs));
					schedule(delay);
					if (!again) return;
				}
			}
		} catch (error) {
			failures++;
			// A wake that arrived during the failing claim does not bypass the backoff.
			again = false;
			const backoffMs = Math.min(
				CLAIM_BACKOFF_MAX_MS,
				CLAIM_BACKOFF_MIN_MS * 2 ** Math.min(failures - 1, 6)
			);
			const at = now();
			if (lastWarnAt === undefined || at - lastWarnAt >= CLAIM_WARN_INTERVAL_MS) {
				lastWarnAt = at;
				spec.logger.warn({ ...spec.label, err: error, backoffMs }, "claim failed; backing off");
			}
			schedule(backoffMs);
		}
	}

	function launch(item: T): void {
		inFlight++;
		let running: Promise<void>;
		try {
			running = spec.process(item);
		} catch (error) {
			running = Promise.reject(error);
		}
		const entry = {
			item,
			done: running
				.catch(error => spec.onProcessError(item, error))
				.finally(() => {
					inFlight--;
					active.delete(entry);
					try {
						spec.onFinished?.();
					} catch (error) {
						spec.logger.warn({ ...spec.label, err: error }, "slot-freed hook failed");
					}
					pass();
				}),
		};
		active.add(entry);
	}

	function schedule(ms: number): void {
		if (stopped) return;
		clearTimeout(timer);
		const jitter = spec.polling.jitterMs > 0 ? Math.floor(random() * spec.polling.jitterMs) : 0;
		timer = setTimeoutFn(() => {
			timer = undefined;
			pass();
		}, ms + jitter);
		timer.unref?.();
	}

	return {
		start() {
			started = true;
			pass();
		},
		wake() {
			if (!started || stopped || failures > 0) return; // backing off: the timer retries
			if (concurrency - inFlight <= 0) return; // full: no query (an ending run calls pass())
			if (!current) {
				clearTimeout(timer);
				timer = undefined;
			}
			pass();
		},
		setConcurrency(n) {
			concurrency = n;
			pass();
		},
		get concurrency() {
			return concurrency;
		},
		get inFlight() {
			return inFlight;
		},
		stopClaiming() {
			stopped = true;
			clearTimeout(timer);
			timer = undefined;
		},
		async drain(timeoutMs) {
			const deadline = now() + timeoutMs;
			const bounded = async (p: Promise<unknown>, until: number): Promise<void> => {
				let t: ReturnType<typeof setTimeout> | undefined;
				const expired = new Promise<void>(resolve => {
					t = setTimeoutFn(resolve, Math.max(0, until - now()));
					t.unref?.();
				});
				try {
					await Promise.race([p, expired]);
				} finally {
					clearTimeout(t);
				}
			};
			// The claim in flight, and the launch of what it brings: its own bound, so the runs it
			// claimed are launched (and handed back by the executor) before stop() moves on.
			if (current) await bounded(current, Math.max(deadline, now() + CLAIM_DRAIN_TIMEOUT_MS));
			if (active.size > 0) {
				await bounded(Promise.allSettled([...active].map(a => a.done)), deadline);
			}
			return [...active].map(a => a.item);
		},
	};
}

function clockOf(o: ClockInjection): Required<ClockInjection> {
	return {
		now: o.now ?? Date.now,
		setTimeoutFn: o.setTimeoutFn ?? setTimeout,
		random: o.random ?? Math.random,
	};
}

export function createQueueWorker(o: QueueWorkerOptions): QueueWorker {
	const withGlobal = o.globalConcurrency !== undefined;
	return createClaimLoop<RunRecord>({
		label: { queue: o.queue },
		concurrency: o.concurrency,
		claim: limit =>
			o.claim.claimRuns({
				queue: o.queue,
				limit,
				...(o.globalConcurrency !== undefined ? { globalConcurrency: o.globalConcurrency } : {}),
			}),
		nextDueInMs: () => o.claim.nextDueInMs(o.queue),
		process: run => o.process(run),
		onProcessError: (run, err) =>
			o.logger.error({ queue: o.queue, runId: run.id, err }, "run processing failed unexpectedly"),
		...(withGlobal && o.onSlotFreed ? { onFinished: () => o.onSlotFreed?.() } : {}),
		polling: o.polling,
		logger: o.logger,
		clock: clockOf(o),
	});
}

/**
 * Runtime guard for a `schedule-tick` row's `data` (stored as `Json`, never trusted blindly).
 * `timezone: null` is accepted as "no timezone".
 */
export function parseScheduleTickData(data: Json): ScheduleTickData | undefined {
	if (typeof data !== "object" || data === null || Array.isArray(data)) return undefined;
	const { scheduleId, cron, timezone, schema, prevMillis } = data as Record<string, Json>;
	if (typeof scheduleId !== "string" || typeof cron !== "string") return undefined;
	if (typeof prevMillis !== "number" || !Number.isFinite(prevMillis)) return undefined;
	if (timezone !== undefined && timezone !== null && typeof timezone !== "string") return undefined;
	if (typeof schema !== "string") return undefined;
	return {
		scheduleId,
		cron,
		schema,
		prevMillis,
		...(typeof timezone === "string" ? { timezone } : {}),
	};
}

export function createSystemWorker(o: SystemWorkerOptions): SystemWorker {
	const clock = clockOf(o);
	const { logger, systemJobs } = o;

	const fenced = (jobId: string, ok: boolean): void => {
		if (!ok) logger.debug({ jobId }, "system job lock lost; result discarded");
	};

	/** Moves a tick row to its next occurrence after the DB clock (decision 17). */
	async function rescheduleTick(
		rec: SystemJobRecord,
		token: string,
		tick: ScheduleTickData
	): Promise<void> {
		const dbNow = await systemJobs.now();
		const after = new Date(Math.max(dbNow.getTime(), tick.prevMillis));
		let runAt: Date;
		try {
			runAt = nextOccurrence(tick.cron, tick.timezone, after);
		} catch (error) {
			// Validated before being written, so this should not happen; the reconciler recreates
			// the row from `schedules`.
			logger.error(
				{ jobId: rec.jobId, cron: tick.cron, timezone: tick.timezone, err: error },
				"schedule tick row has an invalid cron; removing it"
			);
			await systemJobs.remove(rec.jobId, { evenIfLocked: true });
			return;
		}
		fenced(
			rec.jobId,
			await systemJobs.rescheduleTick(rec.jobId, token, { runAt, prevMillis: runAt.getTime() })
		);
	}

	async function processRecord(rec: SystemJobRecord): Promise<void> {
		const token = rec.lockedBy;
		if (token === null) {
			logger.error({ jobId: rec.jobId }, "claimed system job without a lock token ignored");
			return;
		}
		const isTick = rec.name === SCHEDULE_TICK;
		let tick: ScheduleTickData | undefined;
		if (isTick) {
			tick = parseScheduleTickData(rec.data);
			if (!tick) {
				logger.error(
					{ jobId: rec.jobId, data: rec.data },
					"schedule tick row has invalid data; removing it"
				);
				await systemJobs.remove(rec.jobId, { evenIfLocked: true });
				return;
			}
		}
		const repeatJobKey = tick ? (scheduleIdOfJob(rec.jobId) ?? tick.scheduleId) : undefined;
		let movedToDelayed = false;
		const job: PgSystemJob = {
			id: rec.jobId,
			name: rec.name,
			data: rec.data,
			opts: {
				...(tick ? { prevMillis: tick.prevMillis } : {}),
				...(repeatJobKey !== undefined ? { repeatJobKey } : {}),
			},
			timestamp: rec.runAfter.getTime(),
			token,
			...(repeatJobKey !== undefined ? { repeatJobKey } : {}),
			attemptsMade: rec.attempts,
			async moveToDelayed(timestamp) {
				movedToDelayed = true;
				// The handler computes `Date.now() + delayFromTheDb`: only the delay is kept.
				const delayMs = Math.max(1, Math.round(timestamp - clock.now()));
				fenced(rec.jobId, await systemJobs.delay(rec.jobId, token, delayMs));
			},
		};

		let failure: { error: unknown } | undefined;
		try {
			await o.process(rec.name, rec.data, job as unknown as Job);
		} catch (error) {
			failure = { error };
		}
		// Already rescheduled in place by moveToDelayed (with or without the signal).
		if (movedToDelayed || failure?.error instanceof SystemJobDelayed) return;

		if (!failure) {
			if (tick) await rescheduleTick(rec, token, tick);
			else fenced(rec.jobId, await systemJobs.complete(rec.jobId, token));
			return;
		}
		const { error } = failure;
		// `fail()` deletes an exhausted row whatever its name: a tick decides exhaustion itself and
		// moves on to the next occurrence instead of disappearing.
		if (tick && rec.attempts + 1 >= rec.maxAttempts) {
			logger.error(
				{ jobId: rec.jobId, scheduleId: tick.scheduleId, err: error },
				"schedule tick failed permanently; moving to the next occurrence"
			);
			await rescheduleTick(rec, token, tick);
			return;
		}
		const outcome = await systemJobs.fail(rec.jobId, token, o.backoffMs(rec.attempts));
		if (outcome === "lost") {
			fenced(rec.jobId, false);
			return;
		}
		if (outcome === "exhausted") {
			logger.error(
				{ jobId: rec.jobId, name: rec.name, err: error },
				"system job failed permanently"
			);
		} else {
			logger.warn(
				{ jobId: rec.jobId, name: rec.name, err: error },
				"system job failed; will retry"
			);
		}
	}

	return createClaimLoop<SystemJobRecord>({
		label: { queue: "-system" },
		concurrency: o.concurrency,
		claim: limit => systemJobs.claim(limit, o.lockMs),
		nextDueInMs: () => systemJobs.nextDueInMs(),
		process: processRecord,
		// Only bookkeeping writes reach here (handler errors are handled above): the lock expires
		// and another claim retries the job.
		onProcessError: (rec, err) =>
			logger.warn(
				{ jobId: rec.jobId, name: rec.name, err },
				"system job bookkeeping failed; it will be retried when its lock expires"
			),
		polling: o.polling,
		logger,
		clock,
	});
}
