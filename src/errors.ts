/**
 * Base class for every error thrown by minitrigger. Subclasses only need to
 * extend this, call `super(message, options)`, and declare their own
 * `override name = "TheirClassName"` field, so `err.name` always matches the
 * concrete class (e.g. `"ConfigError"`), which keeps `formatError` (see
 * `utils/error-format.ts`) and any `instanceof` checks consistent.
 *
 * `name` is set from an explicit string literal on each class rather than derived from
 * `this.constructor.name`: a minifier/bundler is free to rename classes (mangling
 * `constructor.name`), which would silently change `err.name` (and anything matching on it,
 * like `formatError`'s stored `name` or a caller's `instanceof`-free string check) after a
 * build step, well outside the library's control.
 */
export class MinitriggerError extends Error {
	override name = "MinitriggerError";

	constructor(message: string, options?: ErrorOptions) {
		super(message, options);
	}
}

/** Invalid configuration: durations, identifiers, queue definitions, retry/backoff options, etc. */
export class ConfigError extends MinitriggerError {
	override name = "ConfigError";
}

/** A value failed the JSON-serializability walk or exceeded a configured size limit. */
export class SerializationError extends MinitriggerError {
	override name = "SerializationError";
}

/**
 * User-facing: thrown from inside a task's `run` (or a `step.run` callback) to
 * fail the run permanently, without the engine scheduling a retry.
 */
export class AbortTaskRunError extends MinitriggerError {
	override name = "AbortTaskRunError";
}

/** The step sequence recorded during replay diverged from what was executed originally. */
export class NonDeterminismError extends MinitriggerError {
	override name = "NonDeterminismError";
}

/** The same step name was used twice within a single execution. */
export class DuplicateStepError extends MinitriggerError {
	override name = "DuplicateStepError";
}

/** The executing process lost ownership of the run (canceled, timed out, stolen by the reconciler, ...). */
export class ExecutionLostError extends MinitriggerError {
	override name = "ExecutionLostError";
}

/** A task id was referenced (e.g. via `step.trigger`) that isn't registered on the engine. */
export class TaskNotRegisteredError extends MinitriggerError {
	override name = "TaskNotRegisteredError";
}
