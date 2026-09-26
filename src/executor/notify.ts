import type { Json, RunStatus, SerializedError } from "../store/types.js";

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
	/**
	 * Wall-clock start of THIS EXECUTION -- the current attempt, from its most recent acquisition
	 * or resume, not the whole attempt's history: `run.updatedAt` right after `acquire` (decision
	 * 17, DB clock -- acquire's `UPDATE` sets `updated_at = now()` in the same statement that starts
	 * the attempt). A suspended-then-resumed attempt (sleep, a waited waitpoint) restarts this clock
	 * on each resume; it is never the sum across suspensions, and never the attempt's original
	 * start if it has suspended since. Carried by the executor (`execute-run.ts`) on the writes that
	 * end or suspend an execution (`completed`/`failed`/`retrying`/`waiting`, i.e. a suspend that
	 * left something pending); absent elsewhere -- e.g. `queued` from a fresh trigger, `canceled`,
	 * stall recovery, AND a suspend that requeues right away because everything it waited for was
	 * already resolved by the time the suspension transaction ran (`transition: "queued"`, not an
	 * execution suspending with something still pending) -- where there is no execution to time.
	 */
	startedAt?: Date;
	/**
	 * How long THIS EXECUTION ran, from its acquisition/resume to this write (monotonic clock,
	 * `performance.now()` in the executor -- never the DB clock, so it is immune to clock
	 * skew/adjustment). Telemetry only: never persisted to Postgres. Same presence rule and same
	 * "current execution, not the whole attempt" caveat as {@link startedAt}.
	 */
	durationMs?: number;
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
	/**
	 * Publishes a `ctx.progress.set` write (decision 23) as a `running`-status pub/sub message
	 * carrying the new value, so `runs.subscribe` sees progress-only changes too. Best-effort, like
	 * every other method here; `updatedAt` is the run row's `updated_at` from the same write
	 * (decision 17, no extra `runs.get`).
	 */
	progress(runId: string, updatedAt: Date, value: Json | null): void;
	/** Aborts the executions of these (already canceled) runs in every process (pub/sub). */
	canceled(runIds: readonly string[]): void;
}

export const noopNotifier: RunNotifier = {
	status() {},
	progress() {},
	canceled() {},
};

/** The transition implied by a status alone (used when the caller didn't name one). */
export function transitionOfStatus(status: RunStatus): RunTransition {
	return status === "running" ? "started" : status;
}
