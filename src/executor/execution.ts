import { ExecutionLostError } from "../errors.js";
import type { Ownership, RunRecord } from "../store/types.js";

/**
 * Why an execution's `ctx.signal` fired:
 * - `timeout`: `maxDurationMs` elapsed (per attempt, from acquisition) -> the attempt fails with a
 *   retryable `TimeoutError`;
 * - `shutdown`: `engine.stop()` gave up waiting (decision 20) -> `stop()` releases the run, the
 *   execution writes nothing more;
 * - `lost`: a guarded write/heartbeat reported lost ownership (decision 5) -> stop silently;
 * - `canceled`: `engine.runs.cancel()` or a waitpoint expiry, in this or any other process (via
 *   pub/sub) -> stop silently (the cancel already cleared ownership);
 * - `suspended`: the execution was suspended (decision 2) and the suspension transaction already
 *   ran; fired only so a task function that swallowed the `SuspendSignal` stops its own work;
 * - `finished`: the attempt ended (terminal write, retry scheduled, release, or any other exit of
 *   the executor): fired so work still running (a step left behind by a rejected `Promise.all`,
 *   an un-awaited promise) stops instead of duplicating side effects alongside the next attempt.
 *   Also fired just BEFORE the failure write when the task function threw and its in-flight steps
 *   did not settle within `stallTimeoutMs` (`settleBeforeFail`, external review item 4); the
 *   attempt then fails/retries with the original error.
 */
export type AbortKind = "timeout" | "shutdown" | "lost" | "canceled" | "suspended" | "finished";

/**
 * Mutable state of one execution (one successful `acquire`). Shared by the executor, the
 * heartbeat, `ctx.step` and the engine (`stop()`/`runs.cancel()`).
 */
export class ExecutionState {
	readonly runId: string;
	readonly taskId: string;
	readonly executionId: string;
	readonly owner: Ownership;
	readonly run: RunRecord;
	readonly controller = new AbortController();
	/** First abort reason, if any. Later aborts are ignored. */
	abortKind: AbortKind | undefined;
	/**
	 * Set by `engine.stop()` right before it releases the run (decision 20): from then on the
	 * executor must not write anything (not even a retry), `stop()` owns the hand-back.
	 */
	handedBack = false;
	/**
	 * Promises of the steps started in this execution and not yet settled, with their step names;
	 * the executor awaits `Promise.allSettled` of these before running the suspension transaction
	 * (decision 2) and before completing a run whose function returned without awaiting them.
	 */
	readonly inFlight = new Map<Promise<unknown>, string>();
	/**
	 * Set once a `SuspendSignal` was thrown (suspension is authoritative, decision 2): every later
	 * `step.*` call rethrows it and the task function's own outcome is ignored.
	 */
	suspended = false;
	/** The first `SuspendSignal` of this execution (rethrown by later `step.*` calls). */
	suspendSignal: Error | undefined;
	/**
	 * A code bug detected by `ctx.step` (`NonDeterminismError`, `DuplicateStepError`). It fails the
	 * run without retry whatever the task function does afterwards (it may catch the error, or a
	 * concurrent step may suspend): replaying the same code would hit it again.
	 */
	fatalError: Error | undefined;

	private readonly abortListeners: Array<(kind: AbortKind) => void> = [];
	private readonly handBackFlushers: Array<() => Promise<void>> = [];
	private readonly suspendListeners: Array<() => void> = [];

	constructor(run: RunRecord, executionId: string) {
		this.run = run;
		this.runId = run.id;
		this.taskId = run.taskId;
		this.executionId = executionId;
		this.owner = { runId: run.id, executionId };
	}

	get signal(): AbortSignal {
		return this.controller.signal;
	}

	/**
	 * Fires `ctx.signal` (once) and notifies listeners synchronously.
	 *
	 * `"finished"` (fired by `settleBeforeFail` right before the failure write, ahead of the real
	 * terminal write) can still be overwritten once by `"lost"`/`"canceled"`/`"shutdown"` arriving
	 * afterwards (external review item 1): the signal is already aborted, so the abort reason and
	 * listeners are not re-fired, but {@link abortKind} is corrected so `ownershipGone(state)`
	 * (read live by the run logger/progress writer at flush/close time) reflects that ownership is
	 * really gone, instead of staying stuck on `"finished"` and letting a log/progress write race
	 * past an actual cancel/lost/shutdown.
	 */
	abort(kind: AbortKind, message?: string): void {
		if (this.abortKind !== undefined) {
			if (this.abortKind === "finished" && isOwnershipLostKind(kind)) this.abortKind = kind;
			return;
		}
		this.abortKind = kind;
		this.controller.abort(abortReason(kind, this.runId, message));
		for (const listener of this.abortListeners.splice(0)) listener(kind);
	}

	/** Calls `listener` once on the first abort (immediately if already aborted). */
	onAbort(listener: (kind: AbortKind) => void): void {
		if (this.abortKind !== undefined) {
			listener(this.abortKind);
			return;
		}
		this.abortListeners.push(listener);
	}

	/** Marks the execution suspended (once) and notifies {@link onSuspend} listeners synchronously. */
	markSuspended(signal: Error): void {
		if (this.suspended) return;
		this.suspended = true;
		this.suspendSignal = signal;
		for (const listener of this.suspendListeners.splice(0)) listener();
	}

	/** Calls `listener` once when the execution suspends (immediately if it already did). */
	onSuspend(listener: () => void): void {
		if (this.suspended) {
			listener();
			return;
		}
		this.suspendListeners.push(listener);
	}

	/**
	 * Registers work to finish before `engine.stop()` releases this run (decision 20), e.g. the
	 * pending `ctx.progress` value, written while this execution still owns the run.
	 */
	onHandBack(flush: () => Promise<void>): void {
		this.handBackFlushers.push(flush);
	}

	/**
	 * What `engine.stop()` does to a still-active execution before releasing its run (decision 20):
	 * marks it {@link handedBack} (the executor writes nothing more), aborts `ctx.signal` with
	 * `"shutdown"` and starts the {@link onHandBack} flushes synchronously, so none of them can be
	 * dropped by the executor first. Await the returned promise (bounded by the caller) BEFORE the
	 * release: after it, the ownership guard rejects the writes. Never rejects.
	 */
	handBack(): Promise<void> {
		this.handedBack = true;
		this.abort("shutdown");
		const flushes = this.handBackFlushers.splice(0).map(flush => {
			try {
				return flush();
			} catch (error) {
				return Promise.reject(error);
			}
		});
		return Promise.allSettled(flushes).then(() => undefined);
	}

	/** Records the first fatal (non-retryable) step error; see {@link fatalError}. */
	markFatal(error: Error): void {
		this.fatalError ??= error;
	}

	/** Registers an in-flight step promise (of step `name`) until it settles. */
	trackInFlight<T>(promise: Promise<T>, name: string): Promise<T> {
		this.inFlight.set(promise, name);
		const remove = () => {
			this.inFlight.delete(promise);
		};
		promise.then(remove, remove);
		return promise;
	}
}

/** The three abort kinds under which ownership of the run is really gone (decision 5). */
function isOwnershipLostKind(kind: AbortKind): boolean {
	return kind === "lost" || kind === "canceled" || kind === "shutdown";
}

function abortReason(kind: AbortKind, runId: string, message: string | undefined): Error {
	switch (kind) {
		case "timeout":
			return new DOMException(message ?? `run ${runId} exceeded maxDurationMs`, "TimeoutError");
		case "lost":
			return new ExecutionLostError(message ?? `run ${runId}: this execution lost ownership`);
		case "shutdown":
			return new DOMException(message ?? "the engine is shutting down", "AbortError");
		case "canceled":
			return new DOMException(message ?? `run ${runId} was canceled`, "AbortError");
		case "suspended":
			return new DOMException(message ?? `run ${runId} was suspended`, "AbortError");
		case "finished":
			return new DOMException(
				message ?? `this execution of run ${runId} finished; its attempt is over`,
				"AbortError"
			);
	}
}

/** Executions currently running in this engine, keyed by run id (at most one per run). */
export class ActiveExecutions {
	private readonly byRunId = new Map<string, ExecutionState>();

	add(state: ExecutionState): void {
		this.byRunId.set(state.runId, state);
	}

	delete(state: ExecutionState): void {
		if (this.byRunId.get(state.runId) === state) this.byRunId.delete(state.runId);
	}

	get(runId: string): ExecutionState | undefined {
		return this.byRunId.get(runId);
	}

	values(): ExecutionState[] {
		return Array.from(this.byRunId.values());
	}

	get size(): number {
		return this.byRunId.size;
	}
}
