import { randomUUID } from "node:crypto";
import type { ResolvedConfig } from "../config.js";
import { AbortTaskRunError, ExecutionLostError, TaskNotRegisteredError } from "../errors.js";
import { waitpointExpireJobId } from "../queue/ids.js";
import type { Transport } from "../queue/transport.js";
import type { Registry } from "../registry.js";
import type {
	EnqueueRequest,
	RunRecord,
	SerializedError,
	Store,
	WaitpointResolution,
} from "../store/types.js";
import type { AnyTask, TaskContext } from "../task.js";
import { computeBackoffMs, DEFAULT_RETRY } from "../utils/backoff.js";
import { formatError } from "../utils/error-format.js";
import { assertSerializable, type Json, normalizeOutput } from "../utils/serialization.js";
import { type AbortKind, type ActiveExecutions, ExecutionState } from "./execution.js";
import { createRunLogger } from "./logger.js";
import { createStepApi } from "./step-context.js";

export interface ExecutorDeps {
	store: Store;
	transport: Pick<Transport, "enqueueRun" | "removeSystemJob">;
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
}

/** The payload of a run job: jobId `${runId}-${seq}` (decision 7). */
export interface RunJob {
	runId: string;
	seq: number;
}

/**
 * How the task function ended (or why the executor stopped waiting for it).
 * Phase 4 adds `{ kind: "suspended" }` (a `SuspendSignal` was thrown: await `Promise.allSettled`
 * of `state.inFlight`, then `store.runs.suspend`, and ignore the function's own outcome).
 */
type Outcome =
	| { kind: "success"; output: unknown }
	| { kind: "error"; error: unknown }
	| { kind: "aborted"; reason: AbortKind };

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
		run = await deps.store.runs.acquire(job.runId, executionId, job.seq);
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
	const logger = createRunLogger(deps.config.logger, {
		runId: run.id,
		taskId: run.taskId,
		attempt: run.attempt,
	});

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
		const ctx: TaskContext = {
			step: createStepApi(state),
			logger,
			signal: state.signal,
			run: { id: run.id, taskId: run.taskId, attempt: run.attempt, createdAt: run.createdAt },
			stream: () => Promise.reject(new Error("ctx.stream is implemented in Phase 7")),
		};
		const outcome = await invoke(task, state, ctx);
		clearTimeout(timer);
		heartbeat.stop();
		await settle(deps, state, task, outcome, maxDurationMs);
	} finally {
		clearTimeout(timer);
		heartbeat.stop();
		await logger.flush();
	}
}

function invoke(
	task: AnyTask | undefined,
	state: ExecutionState,
	ctx: TaskContext
): Promise<Outcome> {
	if (!task) {
		// E.g. a rolling deploy where this process doesn't know the task yet: count it as a failed
		// (retryable) attempt, so another worker that knows the task can pick up the retry.
		return Promise.resolve({
			kind: "error",
			error: new TaskNotRegisteredError(
				`task "${state.taskId}" is not registered in the engine that picked up run ${state.runId}`
			),
		});
	}

	const fnOutcome = (async () => task.options.run(state.run.payload, ctx))().then(
		(output): Outcome => ({ kind: "success", output }),
		(error: unknown): Outcome => ({ kind: "error", error })
	);
	const abortOutcome = new Promise<Outcome>(resolve => {
		state.onAbort(reason => resolve({ kind: "aborted", reason }));
	});
	return Promise.race([fnOutcome, abortOutcome]);
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

	switch (outcome.kind) {
		case "aborted":
			switch (outcome.reason) {
				case "timeout":
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
			return afterTerminal(deps, state, result.parent);
		}
	}
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
		log.warn({ ...fields, error: serialized }, "run failed");
		return afterTerminal(deps, state, result.parent);
	}

	const backoff = task?.options.retry?.backoff ?? DEFAULT_RETRY.backoff;
	const backoffMs = backoff ? computeBackoffMs(backoff, run.attempt) : 0;
	const result = await deps.store.runs.retry(state.owner, serialized, backoffMs);
	if (result.kind === "lost") return onLost(deps, state);
	log.info({ ...fields, error: serialized, delayMs: result.enqueue.delayMs }, "run will retry");
	await enqueue(deps, result.enqueue);
}

function onLost(deps: ExecutorDeps, state: ExecutionState): void {
	state.abort("lost");
	deps.config.logger.warn(
		{ runId: state.runId, taskId: state.taskId },
		"execution lost ownership of the run; its result was discarded"
	);
}

/** After `complete`/`fail`: resume the parent (if this resolved its waitpoint) and drop its expiry job. */
async function afterTerminal(
	deps: ExecutorDeps,
	_state: ExecutionState,
	parent: WaitpointResolution | null
): Promise<void> {
	if (!parent) return;
	if (parent.resume) await enqueue(deps, parent.resume);
	if (parent.hasExpireJob) {
		await deps.transport.removeSystemJob(waitpointExpireJobId(parent.waitpointId));
	}
}

/**
 * Enqueue after commit. A failure is only logged: Postgres already holds `run_after`, so the
 * reconciler re-enqueues the run (decision 1).
 */
export async function enqueue(
	deps: Pick<ExecutorDeps, "transport" | "config">,
	request: EnqueueRequest
): Promise<void> {
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
