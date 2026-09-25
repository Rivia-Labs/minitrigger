import { randomUUID } from "node:crypto";
import type { ResolvedConfig } from "../config.js";
import {
	AbortTaskRunError,
	ConfigError,
	ExecutionLostError,
	MinitriggerError,
	TaskNotRegisteredError,
} from "../errors.js";
import { systemQueueName, waitpointExpireJobId } from "../queue/ids.js";
import type { Transport } from "../queue/transport.js";
import type { Registry } from "../registry.js";
import type {
	EnqueueRequest,
	ExecutionSnapshot,
	RunRecord,
	SerializedError,
	Store,
	WaitpointResolution,
} from "../store/types.js";
import type { Streams } from "../streams.js";
import type { AnyTask, TaskContext } from "../task.js";
import { computeBackoffMs, DEFAULT_RETRY } from "../utils/backoff.js";
import { describeOffendingValue, formatError } from "../utils/error-format.js";
import { validateQueueName, validateTaskId } from "../utils/identifiers.js";
import { assertSerializable, type Json, normalizeOutput } from "../utils/serialization.js";
import { type AbortKind, type ActiveExecutions, ExecutionState } from "./execution.js";
import { createRunLogger } from "./logger.js";
import type { RunNotifier } from "./notify.js";
import { createRunProgress, type RunProgress } from "./progress.js";
import { type ChildTaskSpec, createStepContext, SuspendSignal } from "./step-context.js";

export interface ExecutorDeps {
	store: Store;
	transport: Pick<Transport, "enqueueRun" | "removeSystemJob" | "enqueueSystem">;
	registry: Registry;
	config: ResolvedConfig;
	/** Executions of this engine; `executeRun` registers each acquired run here while it runs. */
	active: ActiveExecutions;
	/**
	 * True once `engine.stop()` began. A run acquired after that is released right away (no
	 * attempt increment) and re-enqueued instead of executed: `stop()` already took its snapshot of
	 * the active executions, so nobody else would hand it back (decision 20).
	 */
	isShuttingDown?: () => boolean;
	/** Status/cancel publishing (best-effort, Phase 6/7). Default: nothing is published. */
	notify?: RunNotifier;
	/** Redis Streams transport behind `ctx.stream` (Phase 7). Absent => `ctx.stream` rejects. */
	streams?: Pick<Streams, "pipe">;
}

/** What the after-commit helpers need: enqueue a run, remove a `-system` job, log. */
export interface EnqueueDeps {
	transport: Pick<Transport, "enqueueRun" | "removeSystemJob">;
	config: Pick<ResolvedConfig, "logger">;
}

/** The payload of a run job: jobId `${runId}-${seq}` (decision 7). */
export interface RunJob {
	runId: string;
	seq: number;
	/**
	 * The task queue the job was delivered on. When this process's registry says that queue
	 * declares `concurrencyKey` (decision 25), acquisition goes through
	 * `acquireWithConcurrencyLimit`; unknown or omitted => plain `acquire`.
	 */
	queue?: string;
}

/**
 * How the task function ended (or why the executor stopped waiting for it). `suspended`: a
 * `SuspendSignal` was thrown (decision 2); the function's own outcome is ignored, the in-flight
 * steps were settled and `store.runs.suspend` runs next.
 */
type Outcome =
	| { kind: "success"; output: unknown }
	| { kind: "error"; error: unknown }
	| { kind: "aborted"; reason: AbortKind }
	| { kind: "suspended" };

/**
 * Executes one delivery of a run job (Phase 3; decisions 4, 5, 6, 12, 20):
 *
 * 1. `acquire` with the job's `enqueue_seq` (decision 5). `null` => stale/duplicate job: no-op.
 * 2. Builds `ctx` (`signal`, `run`, `logger`, `step`), starts the heartbeat and the per-attempt
 *    `maxDurationMs` timer (which starts at acquisition and never runs while suspended).
 * 3. Runs the task function OUTSIDE any transaction, racing it against `ctx.signal`: once the
 *    signal fires the executor stops waiting (a function that ignores the signal becomes a zombie
 *    whose later writes are rejected by the ownership guard).
 * 4. Records the outcome with guarded writes: `complete`, `retry` (+ enqueue at the DB-computed
 *    delay) or `fail`. Any `lost` result aborts the signal and stops silently.
 *
 * Never throws for run-level problems; infrastructure errors (e.g. the database is down) are
 * logged and the run is left to stall recovery (Phase 6).
 */
export async function executeRun(deps: ExecutorDeps, job: RunJob): Promise<void> {
	const log = deps.config.logger;
	const executionId = randomUUID();

	let run: RunRecord | null;
	try {
		const limit =
			job.queue === undefined
				? undefined
				: deps.registry.queues.get(job.queue)?.concurrencyKey?.limit;
		if (limit === undefined) {
			run = await deps.store.runs.acquire(job.runId, executionId, job.seq);
		} else {
			const acquired = await deps.store.runs.acquireWithConcurrencyLimit(
				job.runId,
				executionId,
				job.seq,
				{ limit }
			);
			if (acquired.kind === "limited") {
				// Decision 25: not acquired, no attempt spent, no status change to publish.
				log.debug(
					{ runId: job.runId, seq: job.seq, nextSeq: acquired.enqueue.seq },
					"concurrency key at its limit; run re-enqueued"
				);
				await enqueue(deps, acquired.enqueue);
				return;
			}
			run = acquired.kind === "acquired" ? acquired.run : null;
		}
	} catch (error) {
		log.error({ runId: job.runId, seq: job.seq, err: error }, "failed to acquire run");
		return;
	}
	if (!run) {
		log.debug({ runId: job.runId, seq: job.seq }, "stale or duplicate run job ignored");
		return;
	}
	// Checked in the same synchronous step as `active.add` below: a run is either visible to
	// `stop()`'s hand-back or handed back here, never neither.
	if (deps.isShuttingDown?.()) {
		await handBackLateAcquire(deps, run, executionId);
		return;
	}

	const state = new ExecutionState(run, executionId);
	deps.active.add(state);
	deps.notify?.status({
		runId: run.id,
		taskId: run.taskId,
		status: run.status,
		attempt: run.attempt,
		error: run.error,
		updatedAt: run.updatedAt,
		transition: "started",
	});
	try {
		await runAcquired(deps, state);
	} catch (error) {
		log.error(
			{ runId: run.id, taskId: run.taskId, err: error },
			"run execution failed unexpectedly; it will be recovered by the stall reconciler"
		);
	} finally {
		deps.active.delete(state);
	}
}

/** Decision 20 for a run acquired after `stop()` began: release (no attempt) and re-enqueue. */
async function handBackLateAcquire(
	deps: ExecutorDeps,
	run: RunRecord,
	executionId: string
): Promise<void> {
	const fields = { runId: run.id, taskId: run.taskId };
	try {
		const result = await deps.store.runs.release({ runId: run.id, executionId });
		if (result.kind === "released") {
			deps.notify?.status({
				runId: run.id,
				taskId: run.taskId,
				status: result.enqueue.status,
				attempt: run.attempt,
				error: run.error,
				updatedAt: result.enqueue.updatedAt,
				transition: "queued",
			});
			await enqueue(deps, result.enqueue);
			deps.config.logger.info(fields, "run acquired during shutdown handed back to the queue");
		}
	} catch (error) {
		deps.config.logger.error(
			{ ...fields, err: error },
			"failed to release a run acquired during shutdown; the stall reconciler will recover it"
		);
	}
}

async function runAcquired(deps: ExecutorDeps, state: ExecutionState): Promise<void> {
	const { run } = state;
	const task = deps.registry.tasks.get(run.taskId);
	const logger = createRunLogger(
		{ logs: deps.store.logs, config: deps.config },
		{
			runId: run.id,
			taskId: run.taskId,
			attempt: run.attempt,
			ownershipGone: () => ownershipGone(state),
		}
	);
	const progress = createRunProgress(
		{ runs: deps.store.runs, config: deps.config, ...(deps.notify ? { notify: deps.notify } : {}) },
		{
			runId: run.id,
			executionId: state.executionId,
			taskId: run.taskId,
			ownershipGone: () => ownershipGone(state),
		}
	);

	// Decision 20: `engine.stop()` flushes the pending progress value before releasing the run.
	state.onHandBack(() => progress.flushForHandBack());

	const heartbeat = startHeartbeat(deps, state);
	const maxDurationMs = task?.options.maxDurationMs;
	const timer =
		maxDurationMs === undefined
			? undefined
			: setTimeout(
					() => state.abort("timeout", timeoutMessage(run.taskId, maxDurationMs)),
					maxDurationMs
				);

	try {
		let outcome = await execute(deps, task, state, logger, progress);
		if (outcome.kind === "success" && state.inFlight.size > 0) {
			outcome = await settleBeforeComplete(deps, state, outcome);
		}
		// A fatal error (decision: coordinator, deviating from the original plan): `ctx.step.*`
		// refuses to persist anything once `state.fatalError` is set (every later call rethrows it),
		// and the run fails without retry regardless of what any in-flight step does -- waiting only
		// delays the terminal write and risks a hang if that step never settles on its own (e.g. one
		// that only stops when `ctx.signal` aborts, and no `maxDurationMs` is configured).
		if (
			outcome.kind === "error" &&
			state.inFlight.size > 0 &&
			!ownershipGone(state) &&
			state.fatalError === undefined
		) {
			outcome = await settleBeforeFail(deps, state, outcome);
		}
		if (outcome.kind === "suspended") outcome = await settleInFlight(state);
		clearTimeout(timer);
		heartbeat.stop();
		// Pending logs/progress land BEFORE the terminal/suspension write (dropped if ownership is
		// gone).
		await logger.close();
		await progress.close();
		await settle(deps, state, task, outcome, maxDurationMs);
	} finally {
		// The attempt is over (terminal write, retry, suspension, release or an unexpected
		// error): stop whatever user code is still running, e.g. the other steps of a rejected
		// `Promise.all`, before the next attempt re-runs them. No-op if already aborted.
		state.abort("finished");
		clearTimeout(timer);
		heartbeat.stop();
		await logger.close();
		await progress.close();
	}
}

/** Loads the step snapshot (ONE query, Phase 4), builds `ctx` and runs the task function. */
async function execute(
	deps: ExecutorDeps,
	task: AnyTask | undefined,
	state: ExecutionState,
	logger: TaskContext["logger"],
	progress: RunProgress
): Promise<Outcome> {
	if (!task) {
		// E.g. a rolling deploy where this process doesn't know the task yet: count it as a failed
		// (retryable) attempt, so another worker that knows the task can pick up the retry.
		return {
			kind: "error",
			error: new TaskNotRegisteredError(
				`task "${state.taskId}" is not registered in the engine that picked up run ${state.runId}`
			),
		};
	}
	const { limits } = deps.config;
	let snapshot: ExecutionSnapshot;
	try {
		snapshot = await deps.store.steps.loadForExecution(
			state.runId,
			limits.maxInlineStepOutputBytes
		);
	} catch (error) {
		return { kind: "error", error };
	}
	if (state.abortKind !== undefined) return { kind: "aborted", reason: state.abortKind };

	const steps = createStepContext(
		state,
		{
			steps: deps.store.steps,
			maxStepOutputBytes: limits.maxStepOutputBytes,
			waits: {
				runs: deps.store.runs,
				waitpoints: deps.store.waitpoints,
				maxPayloadBytes: limits.maxPayloadBytes,
				resolveTask: child =>
					resolveChildTask(deps.registry, child, { prefix: deps.config.prefix }),
				// Only used for the children `triggerChildren` just created: fresh runs, always
				// `attempt: 1` with no error yet (`triggerChildren`'s SQL/memory insert), so no
				// `runs.get` is needed to notify them.
				enqueueRun: request => {
					deps.notify?.status({
						runId: request.runId,
						taskId: request.taskId,
						status: request.status,
						attempt: 1,
						error: null,
						updatedAt: request.updatedAt,
						transition: "queued",
					});
					return enqueue(deps, request);
				},
				scheduleExpire: (waitpointId, delayMs) => scheduleExpire(deps, waitpointId, delayMs),
			},
		},
		snapshot
	);
	const { run } = state;
	const ctx: TaskContext = {
		step: steps.api,
		logger,
		signal: state.signal,
		run: { id: run.id, taskId: run.taskId, attempt: run.attempt, createdAt: run.createdAt },
		stream: (name, source) => streamTo(deps, state, name, source),
		progress: {
			async set(value) {
				// Not a step (decision 23): no `register()`/`seq_index`, and this never touches
				// `state.fatalError` -- an oversized/non-serializable value is a normal error the
				// task can catch, unlike the argument errors `ctx.step.*` treats as fatal bugs.
				assertSerializable(value, {
					label: `progress of run ${run.id}`,
					maxBytes: limits.maxProgressBytes,
				});
				// Snapshot the now-validated value before handing it to the coalescer: `set()` may
				// return well before the write actually happens (coalesced up to `intervalMs`), and
				// the caller is free to keep mutating the object it passed in. Without this, a later
				// mutation would bypass the validation just performed above (and could still be
				// caught by nothing -- `jsonParam` has no size/serializability check of its own).
				progress.set(structuredClone(value) as Json);
			},
		},
	};
	return invoke(deps, task, state, ctx);
}

/**
 * Decision 2: before the suspension transaction, wait for every step still in flight in this
 * execution (so `Promise.all([step.run, step.sleep])` persists the `step.run` instead of re-running
 * it on replay). A step that fails meanwhile was never persisted: it re-runs on replay and no
 * attempt is counted (the suspension wins). Heartbeat and `maxDurationMs` keep running; an abort
 * ends the wait: cancel/lost/shutdown write nothing more; a timeout still runs the suspension
 * (see `settle`), since the execution still owns the run.
 */
async function settleInFlight(state: ExecutionState): Promise<Outcome> {
	return (await drainInFlight(state)) ?? { kind: "suspended" };
}

/**
 * `Promise.allSettled` of the in-flight steps, repeated until none is left, racing an abort.
 * Returns the abort outcome if the execution was aborted meanwhile, else `undefined`.
 */
async function drainInFlight(state: ExecutionState): Promise<Outcome | undefined> {
	const aborted = new Promise<Outcome>(resolve => {
		state.onAbort(reason => resolve({ kind: "aborted", reason }));
	});
	while (state.inFlight.size > 0) {
		const settled = Promise.allSettled(Array.from(state.inFlight.keys())).then(() => undefined);
		const result = await Promise.race([settled, aborted]);
		if (result !== undefined) return result;
	}
	return undefined;
}

/**
 * The task function returned while steps it did not await were still in flight (e.g.
 * `void ctx.step.trigger(...)`). Completing right away would make their guarded writes come back
 * `lost` (a child silently never created) or leave a waitpoint of a finished run. So, with a
 * warning, the executor first settles them exactly like before a suspension (decision 2): a
 * trigger creates its child, a `step.run` is persisted. If settling suspends the execution (a
 * `waitForToken`/`sleep`/`triggerAndWait` that was not awaited), the suspension wins, as it
 * always does (decision 2): the run suspends instead of completing, and the replay returns the
 * output once nothing is pending. An abort meanwhile ends the wait as in {@link settleInFlight}.
 */
async function settleBeforeComplete(
	deps: ExecutorDeps,
	state: ExecutionState,
	outcome: Outcome
): Promise<Outcome> {
	const steps = Array.from(state.inFlight.values());
	deps.config.logger.warn(
		{ runId: state.runId, taskId: state.taskId, steps },
		`task function returned with steps still in flight (${steps
			.map(s => `step "${s}" was not awaited`)
			.join("; ")}); settling them before completing the run. Await every ctx.step call`
	);
	const aborted = await drainInFlight(state);
	if (aborted !== undefined) return aborted;
	return state.suspended ? { kind: "suspended" } : outcome;
}

/**
 * The task function threw (e.g. one step of a `Promise.all` rejected) while other steps were still
 * in flight. Failing right away would let them keep running alongside the retry, which re-runs
 * them from scratch (a side effect twice). So the executor first waits for them, WITHOUT aborting
 * `ctx.signal`: a step that completes meanwhile is recorded and memoized for the retry. Heartbeat
 * and `maxDurationMs` keep running; a timeout ends the wait and the attempt still fails with the
 * ORIGINAL error (not a `TimeoutError`).
 *
 * The wait is also bounded by `stallTimeoutMs` (external review item 4): without it, a step that
 * never settles on its own (e.g. the losing side of `Promise.race([ctx.step.run(...), deadline])`
 * in a task without `maxDurationMs`) would hold the worker slot forever, the heartbeat keeping the
 * run from ever looking stalled. Past the bound, `ctx.signal` aborts (`"finished"`: the attempt is
 * over, so code that honors the signal stops) and the attempt fails/retries with the ORIGINAL error.
 * `stallTimeoutMs` is the natural bound: it is how long the engine already tolerates a run making
 * no visible progress. `maxDurationMs`, if shorter, still ends the wait first.
 *
 * A step that suspends meanwhile is ignored: the function already failed, so the error wins over
 * the suspension. The caller skips this entirely for a fatal error (`state.fatalError` set,
 * decision: coordinator, deviating from the original plan): `ctx.step.*` refuses to persist
 * anything after a fatal error and the run fails without retry regardless, so waiting would only
 * delay the terminal write (and hang if the in-flight step never settles on its own). Only
 * lost/canceled/shutdown (ownership gone) replace the outcome, since nothing may be written anymore.
 */
async function settleBeforeFail(
	deps: ExecutorDeps,
	state: ExecutionState,
	outcome: Extract<Outcome, { kind: "error" }>
): Promise<Outcome> {
	const steps = Array.from(state.inFlight.values());
	const boundMs = deps.config.stallTimeoutMs;
	deps.config.logger.warn(
		{ runId: state.runId, taskId: state.taskId, steps },
		`task function threw while steps are still in flight (${steps
			.map(s => `step "${s}" is still in flight`)
			.join("; ")}); waiting for them (up to stallTimeoutMs, ${boundMs} ms, or maxDurationMs if ` +
			"sooner) before recording the failure"
	);
	let timer: ReturnType<typeof setTimeout> | undefined;
	const bound = new Promise<"bound">(resolve => {
		timer = setTimeout(() => resolve("bound"), boundMs);
	});
	const result = await Promise.race([drainInFlight(state), bound]);
	clearTimeout(timer);
	if (result === "bound") {
		const stillInFlight = Array.from(state.inFlight.values());
		deps.config.logger.warn(
			{ runId: state.runId, taskId: state.taskId, steps: stillInFlight },
			`gave up waiting for steps still in flight after ${boundMs} ms (stallTimeoutMs); aborting ` +
				"ctx.signal and recording the original failure. A step that ignores ctx.signal keeps " +
				"running alongside the retry"
		);
		state.abort(
			"finished",
			`run ${state.runId}: the task function threw and its in-flight steps did not settle within ` +
				`${boundMs} ms (stallTimeoutMs); this attempt is over`
		);
		return outcome;
	}
	if (result?.kind === "aborted" && result.reason !== "timeout") return result;
	return outcome;
}

function invoke(
	deps: ExecutorDeps,
	task: AnyTask,
	state: ExecutionState,
	ctx: TaskContext
): Promise<Outcome> {
	const fnOutcome = (async () => task.options.run(state.run.payload, ctx))().then(
		(output): Outcome => ({ kind: "success", output }),
		(error: unknown): Outcome => ({ kind: "error", error })
	);
	// Suspension is authoritative (decision 2): whatever the function does after a SuspendSignal is
	// ignored. Ending any other way than by that signal means the user caught it: warn.
	void fnOutcome.then(outcome => {
		if (!state.suspended) return;
		if (outcome.kind === "error" && outcome.error instanceof SuspendSignal) return;
		deps.config.logger.warn(
			{ runId: state.runId, taskId: state.taskId, outcome: outcome.kind },
			"task function did not end with the suspension signal (it was caught by user code?); " +
				"its result is ignored and the run stays suspended. Do not catch errors around ctx.step " +
				"calls without rethrowing them"
		);
	});
	const abortOutcome = new Promise<Outcome>(resolve => {
		state.onAbort(reason => resolve({ kind: "aborted", reason }));
	});
	const suspendOutcome = new Promise<Outcome>(resolve => {
		state.onSuspend(() => resolve({ kind: "suspended" }));
	});
	return Promise.race([fnOutcome, abortOutcome, suspendOutcome]).then(outcome =>
		state.suspended && outcome.kind !== "aborted" ? { kind: "suspended" } : outcome
	);
}

async function settle(
	deps: ExecutorDeps,
	state: ExecutionState,
	task: AnyTask | undefined,
	outcome: Outcome,
	maxDurationMs: number | undefined
): Promise<void> {
	const log = deps.config.logger;
	const fields = { runId: state.runId, taskId: state.taskId, attempt: state.run.attempt };

	// `engine.stop()` took the run back (decision 20): it releases it, we write nothing.
	if (state.handedBack) return;

	// A code bug detected by ctx.step (NonDeterminismError, DuplicateStepError): fail without
	// retry, whatever the function did afterwards (caught it, suspended, returned). Replaying the
	// same code would hit it again. An abort that already cleared ownership still wins.
	const ownershipGone = outcome.kind === "aborted" && outcome.reason !== "timeout";
	if (state.fatalError !== undefined && !ownershipGone) {
		return failAttempt(deps, state, task, state.fatalError, false);
	}

	switch (outcome.kind) {
		case "suspended":
			return suspendRun(deps, state);

		case "aborted":
			switch (outcome.reason) {
				case "timeout":
					// `maxDurationMs` expired during the pre-suspension settle (the execution still
					// owns the run): the suspension wins (decision 2), no attempt is counted. The
					// in-flight steps that did not finish were never persisted and re-run on replay.
					if (state.suspended) return suspendRun(deps, state);
					return failAttempt(
						deps,
						state,
						task,
						timeoutError(state.taskId, maxDurationMs ?? 0),
						true
					);
				case "shutdown":
					return;
				case "lost":
					log.warn(fields, "execution lost ownership of the run; stopping without writing");
					return;
				case "canceled":
					log.debug(fields, "run canceled while executing");
					return;
				case "suspended":
				case "finished": // never an outcome (fired after `settle`, or by `settleBeforeFail` which keeps the error outcome)
					return;
			}
			return;

		case "error":
			if (outcome.error instanceof ExecutionLostError) {
				state.abort("lost");
				log.warn(fields, "execution lost ownership of the run; stopping without writing");
				return;
			}
			return failAttempt(
				deps,
				state,
				task,
				outcome.error,
				!(outcome.error instanceof AbortTaskRunError)
			);

		case "success": {
			// Task outputs are stored in `runs.output` and read back by a parent's
			// `triggerAndWait`/`batchTriggerAndWait` straight from `runs` (decision 10), so they
			// follow the payload limit (`maxPayloadBytes`), not `maxStepOutputBytes`.
			let output: Json;
			try {
				output = normalizeOutput(outcome.output) as Json;
				assertSerializable(output, {
					label: `output of task "${state.taskId}"`,
					maxBytes: deps.config.limits.maxPayloadBytes,
				});
			} catch (error) {
				// Deterministic for the same code path: retrying would not help.
				return failAttempt(deps, state, task, error, false);
			}
			const result = await deps.store.runs.complete(state.owner, output);
			if (result.kind === "lost") return onLost(deps, state);
			deps.notify?.status({
				runId: state.runId,
				taskId: state.taskId,
				status: "completed",
				attempt: state.run.attempt,
				error: null,
				updatedAt: result.updatedAt,
				transition: "completed",
			});
			return afterResolution(deps, result.parent, result.expireJobsToRemove);
		}
	}
}

/**
 * The suspension transaction (decision 3), after the in-flight steps settled. `attempt` is never
 * incremented. The BullMQ job then completes, so a suspended run does not hold a worker slot.
 */
async function suspendRun(deps: ExecutorDeps, state: ExecutionState): Promise<void> {
	const fields = { runId: state.runId, taskId: state.taskId, attempt: state.run.attempt };
	const result = await deps.store.runs.suspend(state.owner);
	if (result.kind !== "lost") {
		// `suspend` never touches `error`; unchanged since acquisition.
		const status = result.kind === "waiting_on_waitpoints" ? "waiting" : result.enqueue.status;
		const updatedAt =
			result.kind === "waiting_on_waitpoints" ? result.updatedAt : result.enqueue.updatedAt;
		deps.notify?.status({
			runId: state.runId,
			taskId: state.taskId,
			status,
			attempt: state.run.attempt,
			error: state.run.error,
			updatedAt,
			transition: result.kind === "requeued" ? "queued" : "waiting",
		});
	}
	switch (result.kind) {
		case "lost":
			return onLost(deps, state);
		case "waiting_on_waitpoints":
			// Whoever resolves the last pending waitpoint enqueues the run.
			deps.config.logger.debug(fields, "run suspended waiting on waitpoints");
			break;
		case "sleeping":
			deps.config.logger.debug(
				{ ...fields, delayMs: result.enqueue.delayMs },
				"run suspended until its sleeps are due"
			);
			await enqueue(deps, result.enqueue);
			break;
		case "requeued":
			// Everything it waited for was already resolved: resume right away.
			deps.config.logger.debug(fields, "run suspended and requeued immediately");
			await enqueue(deps, result.enqueue);
			break;
	}
	// Stops a task function that swallowed the SuspendSignal (its outcome is ignored anyway).
	state.abort("suspended");
}

/**
 * A failed attempt (decision 4): definitive `fail` for a non-retryable error or when attempts are
 * exhausted; otherwise `retry` with backoff and enqueue the run at the DB-computed delay.
 */
async function failAttempt(
	deps: ExecutorDeps,
	state: ExecutionState,
	task: AnyTask | undefined,
	error: unknown,
	retryable: boolean
): Promise<void> {
	const { run } = state;
	const serialized: SerializedError = formatError(error, retryable);
	const log = deps.config.logger;
	const fields = { runId: run.id, taskId: run.taskId, attempt: run.attempt };

	if (!retryable || run.attempt >= run.maxAttempts) {
		const result = await deps.store.runs.fail(state.owner, serialized);
		if (result.kind === "lost") return onLost(deps, state);
		deps.notify?.status({
			runId: run.id,
			taskId: run.taskId,
			status: "failed",
			attempt: run.attempt,
			error: serialized,
			updatedAt: result.updatedAt,
			transition: "failed",
		});
		log.warn({ ...fields, error: serialized }, "run failed");
		return afterResolution(deps, result.parent, result.expireJobsToRemove);
	}

	const backoff = task?.options.retry?.backoff ?? DEFAULT_RETRY.backoff;
	const backoffMs = backoff ? computeBackoffMs(backoff, run.attempt) : 0;
	const result = await deps.store.runs.retry(state.owner, serialized, backoffMs);
	if (result.kind === "lost") return onLost(deps, state);
	deps.notify?.status({
		runId: run.id,
		taskId: result.enqueue.taskId,
		status: result.enqueue.status,
		attempt: result.attempt,
		error: serialized,
		updatedAt: result.enqueue.updatedAt,
		transition: "retrying",
	});
	log.info({ ...fields, error: serialized, delayMs: result.enqueue.delayMs }, "run will retry");
	await enqueue(deps, result.enqueue);
}

/** The execution no longer owns the run: nothing more may be written for it (decision 5). */
function ownershipGone(state: ExecutionState): boolean {
	return (
		state.handedBack ||
		state.abortKind === "lost" ||
		state.abortKind === "canceled" ||
		state.abortKind === "shutdown"
	);
}

/**
 * `ctx.stream(name, source)` (Phase 7): appends every item of `source` to the Redis Stream
 * `${prefix}:stream:${runId}:${name}` (`XADD MAXLEN ~`), then writes the end marker and sets the
 * key's TTL (also on error or abort, with an error end marker). Not a step: it is not memoized, so a replay streams again.
 */
async function streamTo(
	deps: ExecutorDeps,
	state: ExecutionState,
	name: string,
	source: AsyncIterable<Json | string>
): Promise<void> {
	if (typeof name !== "string" || name.length === 0) {
		throw new ConfigError("ctx.stream: name must be a non-empty string");
	}
	if (!deps.streams) throw new MinitriggerError("ctx.stream is not available in this engine");
	// `ctx.signal` ends the stream (with an error end marker) if the attempt is aborted or
	// finishes while the source is still producing, so readers never block forever.
	await deps.streams.pipe(state.runId, name, source, { signal: state.signal });
}

function onLost(deps: ExecutorDeps, state: ExecutionState): void {
	state.abort("lost");
	deps.config.logger.warn(
		{ runId: state.runId, taskId: state.taskId },
		"execution lost ownership of the run; its result was discarded"
	);
}

/**
 * After `complete`/`fail` (and every other terminal transition: cancel, stall recovery, expiry):
 * resume the parent if this resolved its waitpoint, drop that waitpoint's expire job and the
 * expire jobs of the run's own waitpoints the transition timed out (decision 10, best-effort).
 */
export async function afterResolution(
	deps: EnqueueDeps,
	parent: WaitpointResolution | null,
	expireJobsToRemove: readonly string[]
): Promise<void> {
	if (parent?.resume) await enqueue(deps, parent.resume);
	if (parent?.hasExpireJob) {
		await deps.transport.removeSystemJob(waitpointExpireJobId(parent.waitpointId));
	}
	for (const waitpointId of expireJobsToRemove) {
		await deps.transport.removeSystemJob(waitpointExpireJobId(waitpointId));
	}
}

/** Queue of a task that declares none (see `registry.ts`). */
const DEFAULT_QUEUE_NAME = "default";

/** The queue a task's runs go to. */
export function taskQueueName(task: AnyTask): string {
	return task.options.queue?.name ?? DEFAULT_QUEUE_NAME;
}

/** What `triggerChildren` needs from a (registered) child task. */
export function childTaskSpec(task: AnyTask): ChildTaskSpec {
	return {
		id: task.id,
		queue: taskQueueName(task),
		maxAttempts: task.options.retry?.maxAttempts ?? DEFAULT_RETRY.maxAttempts,
	};
}

/**
 * The child spec of `ctx.step.trigger*`'s `task` argument. A task registered in this process wins
 * (its registered definition, even if the passed object differs). Otherwise the child is built
 * from the passed object itself: a worker of another process (e.g. a service that owns that task)
 * executes it, since the transport enqueues onto queues this process never declared. The object is
 * checked first (string `id`, object `options`, valid queue name that is not this engine's
 * `-system` queue, integer `retry.maxAttempts >= 1`); a malformed one is a bug in the calling
 * code: throws `ConfigError` (the step context marks it fatal: the run fails without retry).
 */
export function resolveChildTask(
	registry: Registry,
	task: AnyTask,
	opts: { prefix?: string } = {}
): ChildTaskSpec {
	const candidate = task as unknown;
	if (
		candidate === null ||
		typeof candidate !== "object" ||
		typeof (candidate as { id?: unknown }).id !== "string"
	) {
		throw new ConfigError(
			`child task must be a task returned by defineTask(), got ${describeOffendingValue(candidate)}`
		);
	}
	const registered = registry.tasks.get(task.id);
	if (registered !== undefined) return childTaskSpec(registered);

	const options = (candidate as { options?: unknown }).options;
	const where = `child task "${task.id}" (not registered in this engine)`;
	if (options === null || typeof options !== "object") {
		throw new ConfigError(
			`${where}: must be a task returned by defineTask() (its "options" is ${describeOffendingValue(options)})`
		);
	}
	validateTaskId(task.id);
	const queue = (options as { queue?: unknown }).queue;
	if (queue !== undefined) {
		const name = (queue as { name?: unknown } | null)?.name;
		if (typeof name !== "string") {
			throw new ConfigError(
				`${where}: queue.name must be a string, got ${describeOffendingValue(name)}`
			);
		}
		validateQueueName(name);
		if (opts.prefix !== undefined && name === systemQueueName(opts.prefix)) {
			throw new ConfigError(
				`${where}: queue name "${name}" is reserved for the internal system queue of prefix "${opts.prefix}"`
			);
		}
	}
	const retry = (options as { retry?: unknown }).retry;
	if (retry !== undefined) {
		const maxAttempts = (retry as { maxAttempts?: unknown } | null)?.maxAttempts;
		if (typeof maxAttempts !== "number" || !Number.isInteger(maxAttempts) || maxAttempts < 1) {
			throw new ConfigError(
				`${where}: retry.maxAttempts must be an integer >= 1, got ${describeOffendingValue(maxAttempts)}`
			);
		}
	}
	return childTaskSpec(task);
}

/**
 * Schedules the `-system` job `waitpoint-expire` with the deterministic jobId
 * `wp-${waitpointId}-expire` (decision 10), at the DB-computed delay. Re-adding it while it
 * exists is a BullMQ no-op, so a replay can safely call this again. A failure is only logged:
 * the reconciler's case 5 expires overdue waitpoints from Postgres.
 */
export async function scheduleExpire(
	deps: { transport: Pick<Transport, "enqueueSystem">; config: Pick<ResolvedConfig, "logger"> },
	waitpointId: string,
	delayMs: number
): Promise<void> {
	try {
		await deps.transport.enqueueSystem(
			"waitpoint-expire",
			{ waitpointId },
			{ jobId: waitpointExpireJobId(waitpointId), delayMs }
		);
	} catch (error) {
		deps.config.logger.warn(
			{ waitpointId, err: error },
			"failed to schedule the waitpoint expiry; the reconciler will expire it from Postgres"
		);
	}
}

/**
 * Enqueue after commit. A failure is only logged: Postgres already holds `run_after`, so the
 * reconciler re-enqueues the run (decision 1).
 */
export async function enqueue(deps: EnqueueDeps, request: EnqueueRequest): Promise<void> {
	try {
		await deps.transport.enqueueRun({
			queue: request.queue,
			runId: request.runId,
			seq: request.seq,
			delayMs: request.delayMs,
		});
	} catch (error) {
		deps.config.logger.warn(
			{ runId: request.runId, seq: request.seq, err: error },
			"enqueue failed; the reconciler will re-enqueue the run from Postgres"
		);
	}
}

/** Decision 6: heartbeat every `heartbeatIntervalMs`; failures only warn; `lost` aborts. */
function startHeartbeat(deps: ExecutorDeps, state: ExecutionState): { stop(): void } {
	const { config, store } = deps;
	let stopped = false;
	let inFlight = false;

	const tick = async (): Promise<void> => {
		if (stopped || inFlight) return;
		inFlight = true;
		try {
			const result = await store.runs.heartbeat(
				state.runId,
				state.executionId,
				config.heartbeatTimeoutMs
			);
			if (result === "lost" && !stopped) {
				config.logger.warn(
					{ runId: state.runId, taskId: state.taskId },
					"heartbeat found the run no longer owned by this execution; aborting"
				);
				state.abort("lost");
			}
		} catch (error) {
			config.logger.warn(
				{ runId: state.runId, taskId: state.taskId, err: error },
				"heartbeat failed (the run keeps executing; stallTimeoutMs covers repeated failures)"
			);
		} finally {
			inFlight = false;
		}
	};

	const interval = setInterval(() => void tick(), config.heartbeatIntervalMs);
	interval.unref();
	const stop = () => {
		stopped = true;
		clearInterval(interval);
	};
	state.onAbort(kind => {
		if (kind !== "timeout") stop();
	});
	return { stop };
}

function timeoutMessage(taskId: string, maxDurationMs: number): string {
	return `task "${taskId}" exceeded maxDurationMs (${maxDurationMs} ms)`;
}

/** The error recorded for a `maxDurationMs` expiry: retryable, `name: "TimeoutError"`. */
function timeoutError(taskId: string, maxDurationMs: number): Error {
	const error = new Error(timeoutMessage(taskId, maxDurationMs));
	error.name = "TimeoutError";
	return error;
}
