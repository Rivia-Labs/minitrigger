import type { RunStatus } from "../store/types.js";

/**
 * Cross-process notifications of what an executor/engine did to a run (Phase 6/7). Every method is
 * best-effort: it never throws and is never awaited on the run's critical path, so a Redis or
 * Postgres hiccup here can never fail, delay or retry a run.
 */
export interface RunNotifier {
	/**
	 * Publishes the run's status change (`publishStatus`, consumed by Phase 7's `runs.subscribe`).
	 * `known` is the row just written (status + `updated_at` straight from the DB); without it the
	 * current row is read back, so the published `updatedAt` always comes from the database clock
	 * (decision 17). A duplicate publish is harmless: subscribers order by `updatedAt`.
	 */
	status(runId: string, known?: { status: RunStatus; updatedAt: Date }): void;
	/** Aborts the executions of these (already canceled) runs in every process (pub/sub). */
	canceled(runIds: readonly string[]): void;
}

export const noopNotifier: RunNotifier = {
	status() {},
	canceled() {},
};
