import type { RunRecord, RunStatus } from "../store/types.js";

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
 * Cross-process notifications of what an executor/engine did to a run (Phase 6/7). Every method is
 * best-effort: it never throws and is never awaited on the run's critical path, so a Redis or
 * Postgres hiccup here can never fail, delay or retry a run.
 */
export interface RunNotifier {
	/**
	 * Publishes the run's status change (`publishStatus`, consumed by `runs.subscribe`) and emits
	 * the local `engine.events` event. `known` is the row just written (status + `updated_at`
	 * straight from the DB); without it the current row is read back, so the published `updatedAt`
	 * always comes from the database clock (decision 17). A duplicate publish is harmless:
	 * subscribers order by `updatedAt`. `transition` names the transition this process made; when
	 * omitted it is derived from the row's status.
	 */
	status(runId: string, known?: RunRecord, transition?: RunTransition): void;
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
