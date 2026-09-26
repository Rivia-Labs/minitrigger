/**
 * `createTestEngine(config)` (Phase 9.2): runs the user's real task code with no Redis and no
 * Postgres. It is the real engine core (`engine.ts#createEngineCore`) over an in-memory backend:
 *
 * - the in-memory store (`store/memory`, same contract as the SQL store);
 * - an in-memory transport (`memory-transport.ts`) instead of BullMQ; its jobs are processed by the
 *   REAL executor (`executeRun`: acquire, replay/memoization, suspension, retries) and the REAL
 *   `-system` dispatcher (waitpoint expiry, schedule ticks), one job at a time, only when the test
 *   drives it (`runInline`, `drive`, `drain`, `clock.advance`, `tickSchedule`);
 * - an injectable clock that is the "database clock" of every store operation (decision 17) and of
 *   every job delay: `sleep`, retry backoff and `trigger({ delay })` resolve instantly by default
 *   (the clock jumps to the next due run job), or only on `clock.advance()` with
 *   `autoAdvance: false`.
 *
 * Only what needs a real multi-process deployment is left out: `migrate`/`start` (no workers, no
 * leader loop, reconciler or retention sweeps, no environment checks), cron ticks produced on
 * their own (`tickSchedule` fires one explicitly) and `maxDurationMs`, heartbeats and log flushes,
 * which keep using real timers.
 *
 * **Clock and `autoAdvance` (L1):** with the default `autoAdvance: true`, whenever nothing is
 * currently due but a run job is delayed (a `sleep`, a retry backoff, `trigger({ delay })`),
 * draining jumps the clock straight to the *earliest* delayed job of *any* kind -- a token or
 * `triggerAndWait` timeout included, even one no execution has reached `waitForToken`/
 * `triggerAndWait` for yet. A run that first sleeps past a shorter token timeout it already
 * created will see that token time out before the sleep resolves, exactly as it would with a real
 * clock (the token's own expiry does not wait for anyone to start waiting on it). Use
 * `autoAdvance: false` and `clock.advance()` to interleave instead (e.g. complete a token before
 * its timeout, even though a later step would have slept past it).
 *
 * **Declarative schedules (L2):** every `defineTask({ schedule })` is synced into the store lazily,
 * once, the first time any of `schedules.*`, `drain`/`drive`/`runInline`, `clock.advance`,
 * `tickSchedule` or `trigger` is called (mirrors `engine.start()`'s one-time sync); a fresh engine's
 * `schedules.list()` sees them without triggering or draining anything first.
 */
import { DelayedError, type Job } from "bullmq";
import { type EngineConfig, resolveConfig } from "../config.js";
import {
	createEngineCore,
	type Engine,
	type EngineCore,
	type EngineEvents,
	type EngineQueues,
	type EngineRuns,
	type EngineTokens,
	type TriggerOptions,
	validateTasks,
} from "../engine.js";
import { ConfigError, MinitriggerError } from "../errors.js";
import { createSystemDispatcher, executeRun } from "../executor/index.js";
import { SCHEDULE_TICK_JOB_NAME } from "../queue/ids.js";
import type { RunJobData } from "../queue/transport.js";
import { buildRegistry } from "../registry.js";
import { type EngineSchedules, syncDeclarativeSchedules } from "../schedules.js";
import { createMemoryStore } from "../store/memory/index.js";
import type { RunStatus, SerializedError } from "../store/types.js";
import type { AnyTask, PayloadArgs, RunHandle, TaskOutput, TaskPayload } from "../task.js";
import { type Duration, parseDuration } from "../utils/duration.js";
import { createMemoryPubSubHub, createMemoryStreams } from "./memory-pubsub.js";
import { createMemoryTransport, type MemoryJob } from "./memory-transport.js";

const DEFAULT_MAX_JOBS_PER_DRAIN = 10_000;
const NOTIFY_DRAIN_MS = 2_000;
/** Bounded wait for the drain loop on `stop()` (M1): a task that never settles must not hang it. */
const STOP_DRAIN_TIMEOUT_MS = 2_000;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface TestClockOptions {
	/** Initial clock time. Default: the real time when the engine is created. */
	start?: Date | number;
	/**
	 * `true` (default): whenever nothing is due but a run job is delayed (a `sleep`, a retry
	 * backoff, `trigger({ delay })`), draining jumps the clock to the next due job, so time-based
	 * waits resolve instantly and in order. Waitpoint timeouts alone never move the clock (a run
	 * waiting on a token stays `waiting` until the test completes the token or advances the clock).
	 * `false`: the clock only moves with {@link TestClock.advance}.
	 */
	autoAdvance?: boolean;
}

export interface TestEngineConfig
	extends Pick<EngineConfig, "tasks" | "limits" | "logging" | "logger" | "idempotencyKeyTtl"> {
	/**
	 * Only `days` (default 30), for the same idempotency-key TTL check as the real engine
	 * (`idempotencyKeyTtl` and `trigger({ idempotencyKeyTtl })` must not exceed it). The test
	 * engine never runs retention sweeps.
	 */
	retention?: { days?: number };
	clock?: TestClockOptions;
	/**
	 * Safety net: one drain processing more jobs than this throws (e.g. a task that sleeps in an
	 * endless loop under `autoAdvance`). Default 10 000.
	 */
	maxJobsPerDrain?: number;
}

/** The test engine's clock: the "database clock" of every store operation and job delay. */
export interface TestClock {
	now(): Date;
	/** Moves the clock forward by `duration`, then drains every job that became due. */
	advance(duration: Duration): Promise<void>;
	readonly autoAdvance: boolean;
}

/** Where a run stands after the engine was driven: terminal, or blocked (e.g. `waiting` on a token). */
export interface InlineRunResult<O = unknown> {
	runId: string;
	status: RunStatus;
	/** Set only when `completed`. */
	output: O | null;
	/** Final error when `failed` (e.g. `NonDeterminismError`), last attempt's error otherwise. */
	error: SerializedError | null;
	attempt: number;
}

/** A token of a run that nobody completed yet. */
export interface PendingToken {
	/** Pass it to `tokens.complete`. */
	id: string;
	/** The `createToken`/`waitForToken` step name. */
	name: string;
}

export interface TestEngineTokens extends EngineTokens {
	/** The run's tokens still `pending`, in creation order. */
	pending(run: string | RunHandle<unknown>): Promise<PendingToken[]>;
}

export interface TestEngine {
	/** Creates the run and enqueues it in memory; nothing runs until the engine is driven. */
	trigger: Engine["trigger"];
	runs: EngineRuns;
	/** `complete` only resumes the waiting run's job; drive the engine again to run it. */
	tokens: TestEngineTokens;
	schedules: EngineSchedules;
	events: EngineEvents;
	/**
	 * Decision 27, in memory: a paused queue's jobs are held (never taken by `drain`, never move
	 * the clock) until `resume`; `resume` does not drain by itself (drive the engine again).
	 * `list().activeLocal` counts executions in progress; `concurrency` is always null and `scale`
	 * throws (`ConfigError`): the test engine has no workers, it runs one job at a time.
	 */
	queues: EngineQueues;
	clock: TestClock;
	/**
	 * Triggers `task` and drives the engine until nothing else can run (children of
	 * `triggerAndWait` included), then returns where the run stands: `completed`/`failed`/
	 * `canceled`, or `waiting` (e.g. on a token, or on a sleep with `autoAdvance: false`).
	 */
	runInline<T extends AnyTask>(
		task: T,
		...args: PayloadArgs<TaskPayload<T>, [options?: TriggerOptions]>
	): Promise<InlineRunResult<TaskOutput<T>>>;
	/** Drains the engine, then returns where `run` stands (e.g. after `tokens.complete`). */
	drive<O = unknown>(run: string | RunHandle<O>): Promise<InlineRunResult<O>>;
	/**
	 * Processes every due job, one at a time, through the real executor (and, with `autoAdvance`,
	 * jumps the clock to delayed run jobs) until nothing else can run.
	 */
	drain(): Promise<void>;
	/**
	 * Fires one occurrence (`at`, default `clock.now()`) of schedule `scheduleId`, as the cron job
	 * scheduler would, then drains. `defineTask({ schedule })` schedules use the task id.
	 */
	tickSchedule(scheduleId: string, options?: { at?: Date }): Promise<void>;
	/** Ends open subscriptions/stream reads; the engine cannot be used afterwards. Idempotent. */
	stop(): Promise<void>;
}

export function createTestEngine(config: TestEngineConfig): TestEngine {
	const { clock: clockOptions = {}, maxJobsPerDrain = DEFAULT_MAX_JOBS_PER_DRAIN } = config;
	if (!Number.isInteger(maxJobsPerDrain) || maxJobsPerDrain < 1) {
		throw new MinitriggerError(
			`maxJobsPerDrain must be a positive integer, got ${String(maxJobsPerDrain)}`
		);
	}
	// Placeholder connections: `resolveConfig` validates them, nothing ever connects.
	const resolved = resolveConfig({
		...config,
		postgres: "memory://test-engine",
		redis: "memory://test-engine",
	});
	const { logger } = resolved;
	const registry = buildRegistry(resolved.tasks);
	validateTasks(registry, resolved);

	const start = clockOptions.start;
	let nowMs = Math.floor(
		start === undefined ? Date.now() : start instanceof Date ? start.getTime() : start
	);
	if (!Number.isFinite(nowMs)) throw new MinitriggerError("clock.start must be a valid time");
	const autoAdvance = clockOptions.autoAdvance ?? true;
	const now = () => nowMs;

	const store = createMemoryStore({ now });
	const transport = createMemoryTransport({
		prefix: resolved.prefix,
		queues: Array.from(registry.queues.values()),
		now,
	});
	const hub = createMemoryPubSubHub();
	const streams = createMemoryStreams();

	let stopped = false;
	function assertNotStopped(): void {
		if (stopped) throw new MinitriggerError("this test engine was stopped; create a new one");
	}

	const core: EngineCore = createEngineCore(
		resolved,
		registry,
		{
			store,
			transport,
			publisher: hub.publisher,
			createPubSub: hub.createPubSub,
			streams,
			readStream: (runId, name, signal) => streams.read(runId, name, signal),
		},
		{ isShuttingDown: () => stopped, assertNotStopped }
	);

	const processSystem = createSystemDispatcher({
		config: resolved,
		store,
		transport,
		active: core.active,
		notify: core.notifier,
		registry,
	});

	/**
	 * `schedule-tick` jobs are only ever added by `tickSchedule` (M2): keyed by job id so it can
	 * collect and rethrow the failure of the specific job it just enqueued, instead of the generic
	 * `logger.error` + swallow every other system job failure gets (BullMQ would retry those; there
	 * is no retry in test mode, and swallowing here would make `tickSchedule` lie about success).
	 */
	const scheduleTickErrors = new Map<string, unknown>();

	async function processJob(job: MemoryJob): Promise<void> {
		if (job.kind === "run") {
			try {
				await executeRun(core.deps, { ...(job.data as RunJobData), queue: job.queue });
			} finally {
				transport.finish(job);
			}
			return;
		}
		let redelayAt: number | undefined;
		// The subset of a BullMQ `Job` the `-system` handlers read.
		const bullJob = {
			id: job.id,
			name: job.name,
			data: job.data,
			opts: { ...job.opts },
			timestamp: job.timestamp,
			token: "memory",
			async moveToDelayed(timestamp: number) {
				// Handlers compute `Date.now() + delayFromTheDb`: keep only the delay.
				redelayAt = nowMs + Math.max(1, timestamp - Date.now());
			},
		} as unknown as Job;
		try {
			await processSystem(job.name, job.data, bullJob);
		} catch (error) {
			if (!(error instanceof DelayedError)) {
				logger.error({ err: error, job: job.name, jobId: job.id }, "system job failed");
				if (job.name === SCHEDULE_TICK_JOB_NAME) scheduleTickErrors.set(job.id, error);
			}
		} finally {
			transport.finish(job, redelayAt);
		}
	}

	/**
	 * Lazy, one-time sync of every `defineTask({ schedule })` into the store (L2), mirroring
	 * `engine.start()`'s single call. Called by `schedules.*`, `drain`, `tickSchedule` and `trigger`
	 * (see the header doc), memoized so only the first call does any work; a failure clears the
	 * memo so the next call retries (`syncDeclarativeSchedules` itself isolates one bad task's
	 * schedule and never throws, but this stays defensive against a future change that does).
	 */
	let schedulesSyncPromise: Promise<void> | undefined;
	function ensureSchedulesSynced(): Promise<void> {
		schedulesSyncPromise ??= syncDeclarativeSchedules({
			store,
			transport,
			registry,
			logger,
			maxPayloadBytes: resolved.limits.maxPayloadBytes,
			pruneDeclarative: false,
		}).catch(error => {
			schedulesSyncPromise = undefined;
			throw error;
		});
		return schedulesSyncPromise;
	}

	async function drainLoop(): Promise<void> {
		let processed = 0;
		while (!stopped) {
			const job = transport.takeDue();
			if (job) {
				if (++processed > maxJobsPerDrain) {
					transport.finish(job, job.dueAt);
					throw new MinitriggerError(
						`test engine: more than ${maxJobsPerDrain} jobs in one drain; a run probably never ` +
							"settles (e.g. an endless loop of sleeps or retries). Raise maxJobsPerDrain if " +
							"this is expected"
					);
				}
				await processJob(job);
				continue;
			}
			// Only a delayed run job moves the clock: a token timeout alone must not fire before
			// the test had a chance to complete the token.
			if (!autoAdvance || transport.nextDueAt("run") === undefined) return;
			// The earliest job of any kind, so an expiry due before the next run job fires first.
			const next = transport.nextDueAt();
			if (next === undefined) return;
			nowMs = Math.max(nowMs, next);
		}
	}

	let draining: Promise<void> | undefined;
	function drain(): Promise<void> {
		try {
			assertNotStopped();
		} catch (error) {
			return Promise.reject(error);
		}
		const inProgress = draining;
		if (inProgress) {
			// L5: a drain is already in progress. Its own loop picks up anything enqueued WHILE it
			// runs, but something can still land after its last `takeDue()` check and before it
			// actually settles (e.g. a concurrent `tokens.complete()`/`trigger()`/second `drain()`
			// racing its tail) -- wait for it, then check again instead of returning the same
			// promise every concurrent caller would otherwise share.
			return inProgress.then(() => drain());
		}
		draining = ensureSchedulesSynced()
			.then(() => drainLoop())
			.finally(() => {
				draining = undefined;
			});
		return draining;
	}

	async function resultOf<O>(run: string | RunHandle<O>): Promise<InlineRunResult<O>> {
		const runId = typeof run === "string" ? run : run.id;
		const record = await core.runs.get<O>(runId);
		if (!record) throw new MinitriggerError(`run ${runId} not found`);
		return {
			runId: record.id,
			status: record.status,
			output: record.output,
			error: record.error,
			attempt: record.attempt,
		};
	}

	async function drive<O>(run: string | RunHandle<O>): Promise<InlineRunResult<O>> {
		await drain();
		return resultOf(run);
	}

	async function trigger<T extends AnyTask>(
		task: T,
		...args: PayloadArgs<TaskPayload<T>, [options?: TriggerOptions]>
	): Promise<RunHandle<TaskOutput<T>>> {
		await ensureSchedulesSynced();
		return core.trigger(task, ...args);
	}

	async function runInline<T extends AnyTask>(
		task: T,
		...args: PayloadArgs<TaskPayload<T>, [options?: TriggerOptions]>
	): Promise<InlineRunResult<TaskOutput<T>>> {
		const handle = await trigger(task, ...args);
		return drive(handle);
	}

	const clock: TestClock = {
		now: () => new Date(nowMs),
		async advance(duration) {
			assertNotStopped();
			nowMs += parseDuration(duration);
			await drain();
		},
		autoAdvance,
	};

	const tokens: TestEngineTokens = {
		complete: core.tokens.complete,
		async pending(run) {
			const runId = typeof run === "string" ? run : run.id;
			if (!UUID_PATTERN.test(runId)) return [];
			const pending: PendingToken[] = [];
			for (const step of await store.steps.list(runId)) {
				if (step.kind !== "token") continue;
				const id = (step.output as { id?: unknown } | null)?.id;
				if (typeof id !== "string") continue;
				const token = await store.waitpoints.getToken(id);
				if (token?.status === "pending") pending.push({ id, name: step.name });
			}
			return pending;
		},
	};

	const schedules: EngineSchedules = {
		async upsert(input) {
			await ensureSchedulesSynced();
			return core.schedules.upsert(input);
		},
		async delete(id) {
			await ensureSchedulesSynced();
			return core.schedules.delete(id);
		},
		async list() {
			await ensureSchedulesSynced();
			return core.schedules.list();
		},
	};

	/** M2: unknown schedule id, or a failure of the tick job it enqueued, both reject. */
	async function tickSchedule(scheduleId: string, options: { at?: Date } = {}): Promise<void> {
		assertNotStopped();
		await ensureSchedulesSynced();
		if ((await store.schedules.get(scheduleId)) === null) {
			throw new ConfigError(
				`tickSchedule: no schedule "${scheduleId}" (neither declared via defineTask({ schedule }) ` +
					"nor created with engine.schedules.upsert())"
			);
		}
		const jobId = transport.enqueueScheduleTick(scheduleId, options.at?.getTime() ?? nowMs);
		scheduleTickErrors.delete(jobId);
		await drain();
		const error = scheduleTickErrors.get(jobId);
		if (error !== undefined) {
			scheduleTickErrors.delete(jobId);
			throw error;
		}
	}

	let stopPromise: Promise<void> | undefined;
	function stop(): Promise<void> {
		stopPromise ??= (async () => {
			const running = draining;
			stopped = true;
			// Decision 20's spirit: a task the drain loop is awaiting may never settle (e.g. a
			// promise the user's code never resolves). Mark every active execution handed back (so
			// nothing writes after this point, mirroring `handBackActiveRuns`) and abort it, then wait
			// for the drain loop bounded, so `stop()` always resolves even if the drain never does.
			// `handBack()` also flushes a pending progress value (bounded like the drain wait).
			const handBacks = Promise.all(Array.from(core.active.values(), e => e.handBack()));
			let flushTimer: ReturnType<typeof setTimeout> | undefined;
			await Promise.race([
				handBacks,
				new Promise<void>(resolve => {
					flushTimer = setTimeout(resolve, STOP_DRAIN_TIMEOUT_MS);
				}),
			]);
			clearTimeout(flushTimer);
			if (running) {
				let timer: ReturnType<typeof setTimeout> | undefined;
				await Promise.race([
					running.catch(() => {}),
					new Promise<void>(resolve => {
						timer = setTimeout(resolve, STOP_DRAIN_TIMEOUT_MS);
					}),
				]);
				clearTimeout(timer);
			}
			core.endSubscriptions();
			streams.close();
			await core.currentPubSub()?.close();
			await core.drainNotifications(NOTIFY_DRAIN_MS);
		})();
		return stopPromise;
	}

	return {
		trigger,
		runs: core.runs,
		tokens,
		schedules,
		events: core.events,
		queues: core.queues,
		clock,
		runInline,
		drive,
		drain,
		tickSchedule,
		stop,
	};
}
