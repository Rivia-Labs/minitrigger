/**
 * `attachDefaultLogger(engine, logger?)` (opt-in, never wired automatically): subscribes to
 * `engine.events` and writes one structured line per run transition, so a consumer gets
 * production-usable observability with a single call instead of writing their own
 * `engine.events.on(...)` handlers.
 *
 * Security (owner priority): a failure line NEVER carries the full `SerializedError` (which can
 * include a `stack` and a chained `cause`) or any payload/output -- only `error.name`/`message`.
 * Nothing here reads `run.payload`/`run.output`: the local `RunEvent` this listens to doesn't even
 * carry them (see `src/engine.ts`).
 */

import { defaultLogger, type EngineLogger } from "./config.js";
import type { EngineClient, RunEvent, RunEventName } from "./engine.js";
import { ConfigError } from "./errors.js";

/** What `attachDefaultLogger` needs from an engine: its `events` (any `Engine`/`TestEngine`). */
export type EngineEventSource = Pick<EngineClient, "events">;

/**
 * `defaultLogger` (used when `config.logger` is omitted) drops `debug`/`info`, which would make
 * `run:started`/`run:completed` vanish silently -- the opposite of what attaching a logger asks
 * for. So when the target is that built-in logger, its `info` goes to `console.info` (same
 * `[minitrigger]` prefix as its warn/error); every other level is left as is.
 */
const defaultLoggerWithInfo: EngineLogger = {
	...defaultLogger,
	info: (fields, message) => console.info(`[minitrigger] ${message}`, fields),
};

/** Only the transitions this logs; `run:queued`/`run:waiting` are left to the caller (noisier, less actionable by default). */
const LOGGED_EVENTS: readonly RunEventName[] = [
	"run:started",
	"run:completed",
	"run:failed",
	"run:retrying",
	"run:canceled",
];

/** `{ name, message }` of a `RunEvent.error`, never the full `SerializedError` (no `stack`/`cause`). */
function errorSummary(event: RunEvent): { name: string; message: string } | null {
	return event.error ? { name: event.error.name, message: event.error.message } : null;
}

/** Common fields of every line; `durationMs` only when the event carries it (see `RunEvent`). */
function baseFields(event: RunEvent): Record<string, unknown> {
	const fields: Record<string, unknown> = {
		runId: event.runId,
		taskId: event.taskId,
		attempt: event.attempt,
	};
	if (event.durationMs !== undefined) fields.durationMs = event.durationMs;
	return fields;
}

function logEvent(logger: EngineLogger, event: RunEvent): void {
	switch (event.name) {
		case "run:started":
			logger.info(baseFields(event), `run "${event.taskId}" started`);
			return;
		case "run:completed":
			logger.info(baseFields(event), `run "${event.taskId}" completed`);
			return;
		case "run:failed":
			logger.error(
				{ ...baseFields(event), error: errorSummary(event) },
				`run "${event.taskId}" failed`
			);
			return;
		case "run:retrying":
			logger.warn(
				{ ...baseFields(event), error: errorSummary(event) },
				`run "${event.taskId}" will retry`
			);
			return;
		case "run:canceled":
			logger.warn(baseFields(event), `run "${event.taskId}" canceled`);
			return;
		default:
			return;
	}
}

/**
 * Subscribes `logger` (or, when omitted, the `logger` this `engine` was created with --
 * `config.logger`, pino- or console-compatible) to `engine.events` and logs one line per run
 * transition:
 *
 * | Event | Level | Fields |
 * |---|---|---|
 * | `run:started` | `info` | `runId`, `taskId`, `attempt` |
 * | `run:completed` | `info` | + `durationMs` |
 * | `run:failed` | `error` | + `durationMs`, `error: { name, message }` (never the full error/stack, never payload/output) |
 * | `run:retrying` | `warn` | + `durationMs`, `error: { name, message }` |
 * | `run:canceled` | `warn` | `runId`, `taskId`, `attempt` |
 *
 * Levels: your logger's own level filter applies (a pino logger at `warn` drops the `info`
 * lines). The one exception is the built-in default logger (no `config.logger` given), which
 * normally drops `info`: here its `info` lines are written to `console.info` instead, since
 * attaching a logger and seeing no `run:started`/`run:completed` would be surprising.
 *
 * `engine` can be any engine exposing `events` (`Engine`, `TestEngine`); `logger` is required
 * when the engine doesn't carry its own (`TestEngine`).
 *
 * Opt-in only -- `createEngine`/`createTestEngine` never call this on their own. Handlers run
 * off the run's critical path (like every `engine.events` handler) and never throw back into it:
 * a logging call that itself throws is caught, and only the FIRST such failure is reported
 * through `logger.error` (a broken logger would otherwise report itself on every event); later
 * failures are dropped silently. A `logger.error` that throws while reporting is swallowed too.
 *
 * Returns an unsubscribe function (idempotent, like every `engine.events.on` unsubscribe).
 */
export function attachDefaultLogger(
	engine: EngineEventSource & { readonly logger: EngineLogger },
	logger?: EngineLogger
): () => void;
export function attachDefaultLogger(engine: EngineEventSource, logger: EngineLogger): () => void;
export function attachDefaultLogger(
	engine: EngineEventSource & { readonly logger?: EngineLogger },
	logger?: EngineLogger
): () => void {
	const configured = logger ?? engine.logger;
	if (configured === undefined) {
		throw new ConfigError(
			"attachDefaultLogger(engine, logger): this engine has no logger of its own (e.g. a " +
				"TestEngine); pass one as the second argument"
		);
	}
	const target = configured === defaultLogger ? defaultLoggerWithInfo : configured;
	let reportedFailure = false;
	const unsubscribes = LOGGED_EVENTS.map(name =>
		engine.events.on(name, event => {
			try {
				logEvent(target, event);
			} catch (error) {
				if (reportedFailure) return;
				reportedFailure = true;
				try {
					target.error(
						{ err: error, event: event.name, runId: event.runId },
						"attachDefaultLogger: logging a run event failed (further failures are not reported)"
					);
				} catch {
					// The logger is broken beyond reporting; never throw into engine.events.
				}
			}
		})
	);
	let unsubscribed = false;
	return () => {
		if (unsubscribed) return;
		unsubscribed = true;
		for (const off of unsubscribes) off();
	};
}
