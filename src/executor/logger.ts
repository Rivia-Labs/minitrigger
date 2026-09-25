import type { EngineLogger } from "../config.js";
import type { TaskLogger } from "../task.js";
import type { Json } from "../utils/serialization.js";

export interface RunLoggerContext {
	runId: string;
	taskId: string;
	attempt: number;
}

/**
 * Builds `ctx.logger` for one execution. For now every entry is mirrored to the engine's
 * `config.logger` with the run's identifiers.
 *
 * Phase 7 seam: persisted, batched run logs (`run_logs` via `LogStore.insertBatch`, flushed every
 * 500 ms / 50 entries and at the end of the execution, `maxLogEntryBytes` truncation, minimum
 * level) plug in here as an extra sink next to the mirror; the executor already calls
 * {@link RunLogger.flush} when the execution ends.
 */
export interface RunLogger extends TaskLogger {
	/** Flushes buffered entries (no-op until Phase 7). Never throws. */
	flush(): Promise<void>;
}

export function createRunLogger(engineLogger: EngineLogger, ctx: RunLoggerContext): RunLogger {
	const base = { runId: ctx.runId, taskId: ctx.taskId, attempt: ctx.attempt };
	const emit =
		(level: keyof EngineLogger) =>
		(message: string, data?: Json): void => {
			try {
				engineLogger[level](data === undefined ? base : { ...base, data }, message);
			} catch {
				// A broken user logger must never fail a run.
			}
		};
	return {
		debug: emit("debug"),
		info: emit("info"),
		warn: emit("warn"),
		error: emit("error"),
		flush: async () => {},
	};
}
