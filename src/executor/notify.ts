import type { RunStatus, SerializedError } from "../store/types.js";

/**
 * Which transition this process just made (Phase 7 local events, `engine.events.on`):
 * `started` = acquired (`running`); `retrying` = a failed attempt re-queued with backoff (or a
 * stall recovered with an attempt increment); `queued` = any other (re)queue (trigger, child
 * creation, suspension with nothing pending, shutdown hand-back).
 */
export type RunTransition =
	| "queued"
	| "started"
	| "waiting"
	| "completed"
	| "failed"
	| "canceled"
	| "retrying";

/**
 * Everything {@link RunNotifier.status} needs, taken from the store result (or the caller's own
 * `ExecutionState`/`RunRecord`) the caller already holds: no `runs.get` (Phase 7 perf). `updatedAt`
 * always comes from the database clock (decision 17), never `new Date()`.
 */
export interface RunStatusInfo {
	runId: string;
	taskId: string;
	status: RunStatus;
	/** Current attempt (unchanged by most transitions; incremented by retry/stall recovery). */
	attempt: number;
	/** Last attempt's error (kept while retrying), the final error, or the cancel reason. */
	error: SerializedError | null;
	/** The run row's `updated_at` at the moment this was computed (same DB write, no `runs.get`). */
	updatedAt: Date;
	/** Names the transition this process made; when omitted it is derived from `status`. */
	transition?: RunTransition;
}

/**
 * Cross-process notifications of what an executor/engine did to a run (Phase 6/7). Every method is
 * best-effort: it never throws and is never awaited on the run's critical path, so a Redis or
 * Postgres hiccup here can never fail, delay or retry a run.
 */
export interface RunNotifier {
	/**
	 * Publishes the run's status change (`publishStatus`, consumed by `runs.subscribe`) and emits
	 * the local `engine.events` event, entirely from `info` (Phase 7 perf: no `runs.get`). A
	 * duplicate publish is harmless: subscribers order by `updatedAt`.
	 */
	status(info: RunStatusInfo): void;
	/** Aborts the executions of these (already canceled) runs in every process (pub/sub). */
	canceled(runIds: readonly string[]): void;
}

export const noopNotifier: RunNotifier = {
	status() {},
	canceled() {},
};

/** The transition implied by a status alone (used when the caller didn't name one). */
export function transitionOfStatus(status: RunStatus): RunTransition {
	return status === "running" ? "started" : status;
}
