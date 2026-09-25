import type { ResolvedConfig } from "../config.js";
import { createLogBuffer, type LogBufferMirror } from "../logs/buffer.js";
import type { LogStore, NewLogEntry } from "../store/types.js";
import type { TaskLogger } from "../task.js";

export interface RunLoggerContext {
	runId: string;
	taskId: string;
	attempt: number;
	/**
	 * True once this execution no longer owns the run (lost, canceled, handed back on shutdown).
	 * Entries still buffered then are dropped with one local `warn` (decision 5's spirit: nothing
	 * more is written for an execution that lost the run).
	 */
	ownershipGone: () => boolean;
}

/** `ctx.logger` of one execution, plus the executor-side `close`. */
export interface RunLogger extends TaskLogger {
	/**
	 * Flushes whatever is buffered and disables further logging (later calls are no-ops). Called
	 * by the executor BEFORE the terminal/suspension write, and again (idempotent) when the
	 * execution ends. Never throws.
	 */
	close(): Promise<void>;
}

/**
 * Builds `ctx.logger` for one execution (Phase 7): a {@link createLogBuffer} that persists to
 * `run_logs` via `store.logs.insertBatch` (one `unnest` statement per batch; flushed every 500 ms,
 * at 50 entries and on close), filtered by `logging.minLevel`, truncated to `maxLogEntryBytes`,
 * and mirrored to `config.logger` (with `runId`, `taskId`, `attempt`) when `logging.mirror`.
 */
export function createRunLogger(
	deps: { logs: LogStore; config: Pick<ResolvedConfig, "logger" | "logging" | "limits"> },
	ctx: RunLoggerContext
): RunLogger {
	const { logger: engineLogger, logging, limits } = deps.config;
	const ids = { runId: ctx.runId, taskId: ctx.taskId, attempt: ctx.attempt };
	let droppedWarned = false;

	const mirror: LogBufferMirror | undefined = logging.mirror
		? {
				debug: (fields, message) => engineLogger.debug({ ...ids, ...fields }, message),
				info: (fields, message) => engineLogger.info({ ...ids, ...fields }, message),
				warn: (fields, message) => engineLogger.warn({ ...ids, ...fields }, message),
				error: (fields, message) => engineLogger.error({ ...ids, ...fields }, message),
			}
		: undefined;

	const buffer = createLogBuffer({
		runId: ctx.runId,
		minLevel: logging.minLevel,
		maxLogEntryBytes: limits.maxLogEntryBytes,
		...(mirror ? { mirror } : {}),
		insertBatch: async (entries: NewLogEntry[]) => {
			if (ctx.ownershipGone()) {
				if (!droppedWarned) {
					droppedWarned = true;
					safeLog(() =>
						engineLogger.warn(
							{ ...ids, dropped: entries.length },
							"run logs dropped: this execution no longer owns the run"
						)
					);
				}
				return;
			}
			await deps.logs.insertBatch(entries);
		},
		onFlushError: (error, entries) =>
			safeLog(() =>
				engineLogger.warn(
					{ ...ids, err: error, dropped: entries.length },
					"failed to persist run logs (retried once); the batch was dropped"
				)
			),
	});

	return {
		debug: buffer.debug,
		info: buffer.info,
		warn: buffer.warn,
		error: buffer.error,
		close: () => buffer.close(),
	};
}

function safeLog(fn: () => void): void {
	try {
		fn();
	} catch {
		// A broken engine logger must never fail a run.
	}
}
