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

import type { EngineLogger } from "./config.js";
import type { Engine, RunEvent, RunEventName } from "./engine.js";

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
 * Opt-in only -- `createEngine`/`createTestEngine` never call this on their own. Handlers run
 * off the run's critical path (like every `engine.events` handler) and never throw back into it;
 * a logging call that itself throws is caught and reported through `logger.error` once, so one bad
 * logger implementation can't crash the process from inside an event handler.
 *
 * Returns an unsubscribe function (idempotent, like every `engine.events.on` unsubscribe).
 */
export function attachDefaultLogger(engine: Engine, logger?: EngineLogger): () => void {
	const target = logger ?? engine.logger;
	const unsubscribes = LOGGED_EVENTS.map(name =>
		engine.events.on(name, event => {
			try {
				logEvent(target, event);
			} catch (error) {
				target.error(
					{ err: error, event: event.name, runId: event.runId },
					"attachDefaultLogger: logging a run event failed"
				);
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
