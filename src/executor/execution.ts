import { ExecutionLostError } from "../errors.js";
import type { Ownership, RunRecord } from "../store/types.js";

/**
 * Why an execution's `ctx.signal` fired:
 * - `timeout`: `maxDurationMs` elapsed (per attempt, from acquisition) -> the attempt fails with a
 *   retryable `TimeoutError`;
 * - `shutdown`: `engine.stop()` gave up waiting (decision 20) -> `stop()` releases the run, the
 *   execution writes nothing more;
 * - `lost`: a guarded write/heartbeat reported lost ownership (decision 5) -> stop silently;
 * - `canceled`: `engine.runs.cancel()` in this process (cross-process pub/sub: Phase 6) -> stop
 *   silently (the cancel already cleared ownership).
 */
export type AbortKind = "timeout" | "shutdown" | "lost" | "canceled";

/**
 * Mutable state of one execution (one successful `acquire`). Shared by the executor, the
 * heartbeat, `ctx.step` (Phase 4) and the engine (`stop()`/`runs.cancel()`).
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
	 * Phase 4 seam: promises of the steps started in this execution and not yet settled; the
	 * executor awaits `Promise.allSettled` of these before running the suspension transaction.
	 */
	readonly inFlight = new Set<Promise<unknown>>();
	/** Phase 4 seam: set once a `SuspendSignal` was thrown (suspension is authoritative, decision 2). */
	suspended = false;

	private readonly abortListeners: Array<(kind: AbortKind) => void> = [];

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

	/** Fires `ctx.signal` (once) and notifies listeners synchronously. */
	abort(kind: AbortKind, message?: string): void {
		if (this.abortKind !== undefined) return;
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

	/** Phase 4 seam: registers an in-flight step promise until it settles. */
	trackInFlight<T>(promise: Promise<T>): Promise<T> {
		this.inFlight.add(promise);
		const remove = () => {
			this.inFlight.delete(promise);
		};
		promise.then(remove, remove);
		return promise;
	}
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
