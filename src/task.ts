/**
 * Public task types and {@link defineTask} (SPEC.md §3, Phase 3 task 3.1).
 *
 * `src/registry.ts` groups tasks by declared queue and resolves shared queue
 * settings; the executor (later phases) reads `Task.options` to run a task and
 * fills in `TaskContext`/`StepApi` for `run`.
 */

import type { RetryOptions } from "./utils/backoff.js";
import type { Duration } from "./utils/duration.js";
import { validateTaskId } from "./utils/identifiers.js";
import type { Json, Serializable } from "./utils/serialization.js";

export type { SerializedError } from "./utils/error-format.js";

// ---------------------------------------------------------------------------
// Queue / retry / schedule option shapes
// ---------------------------------------------------------------------------

/**
 * Queue settings a task can declare. Several tasks may reference the same
 * `name`; see `registry.ts` for how conflicting declarations are resolved.
 */
export type TaskQueueOptions = {
	name: string;
	concurrency?: number;
	globalConcurrency?: number;
	rateLimit?: { max: number; durationMs: number };
};

/** Declarative cron schedule (Phase 8). Synced by `engine.start()`. */
export type TaskScheduleOptions<P> = {
	cron: string;
	timezone?: string;
	payload?: P;
};

// ---------------------------------------------------------------------------
// TaskOptions / defineTask
// ---------------------------------------------------------------------------

/**
 * Options passed to {@link defineTask}.
 *
 * When `queue` is omitted, the task belongs to a queue named **`"default"`**
 * (concurrency `10`, decided by `registry.ts`'s `buildRegistry` when nobody
 * declares a concurrency for that queue explicitly).
 */
export type TaskOptions<Id extends string, P, O> = {
	id: Id;
	/** Defaults to the `"default"` queue (see above) when omitted. */
	queue?: TaskQueueOptions;
	retry?: RetryOptions;
	/** Per attempt; the timer does not run while the run is suspended (decision 4). */
	maxDurationMs?: number;
	schedule?: TaskScheduleOptions<P>;
	run: (payload: P, ctx: TaskContext) => Promise<O>;
};

/**
 * Defines a task, inferring `P` (payload) and `O` (output) from `run`.
 *
 * Payload/output serializability (decision 12) is checked on the **options
 * object**, not on `run`'s parameter: a constraint written directly on the
 * parameter (e.g. `run: (payload: P & Serializable<P>) => ...`) is silently
 * ineffective, because function parameters are checked contravariantly, so it
 * never actually rejects a non-serializable `P` at the call site (see
 * `CLAUDE.md`, "Notas para as próximas fases"). Instead, the two branches
 * below add a required, differently-named property to the expected argument
 * type whenever `P`/`O` (as inferred from `run`) isn't `Serializable`,
 * producing a readable "missing property" compile error that names the
 * problem via `Serializable`'s own branded error message.
 *
 * `O` = `void` (e.g. `run: async () => {}`) is allowed: `Serializable<void>`
 * resolves to `void` itself, so the output branch is a no-op, and the engine
 * stores such an output as `null` (see `utils/serialization.ts#normalizeOutput`).
 */
export function defineTask<Id extends string, P, O>(
	opts: TaskOptions<Id, P, O> &
		([Serializable<P>] extends [P]
			? unknown
			: { readonly __payloadNotSerializable: Serializable<P> }) &
		([Serializable<O>] extends [O]
			? unknown
			: { readonly __outputNotSerializable: Serializable<O> })
): Task<Id, P, O> {
	validateTaskId(opts.id);
	return { id: opts.id, options: opts };
}

// ---------------------------------------------------------------------------
// Task / AnyTask / TaskPayload / TaskOutput
// ---------------------------------------------------------------------------

/**
 * The object returned by {@link defineTask}. `__payload`/`__output` never
 * exist at runtime (always `undefined`); they're phantom fields so
 * {@link TaskPayload}/{@link TaskOutput} can recover `P`/`O` from a `Task`
 * value, e.g. for `engine.trigger(task, payload)`'s parameter type.
 */
export type Task<Id extends string, P, O> = {
	readonly id: Id;
	readonly options: TaskOptions<Id, P, O>;
	readonly __payload?: P;
	readonly __output?: O;
};

/** A `Task` of unknown id/payload/output, for APIs that accept any registered task. */
export type AnyTask = Task<string, any, any>;

/**
 * Extracts the payload type `P` of a `Task`. A task whose `run` declares no payload parameter
 * infers `P = unknown`; that is mapped to `void` so `trigger(task)` needs (and accepts) no payload.
 * `any` is kept as-is (escape hatch).
 */
export type TaskPayload<T extends AnyTask> =
	T extends Task<any, infer P, any> ? NormalizePayload<P> : never;

type NormalizePayload<P> = 0 extends 1 & P ? P : unknown extends P ? NoPayload : P;

/** Exactly `void`, spelled as a return type (the only place Biome's `noConfusingVoidType` allows it). */
type NoPayload = ReturnType<() => void>;

/**
 * The `(payload, ...rest)` arguments of a trigger-like call: `payload` is optional when the task
 * takes none (`TaskPayload<T>` is `void`/`undefined`), required otherwise.
 */
export type PayloadArgs<P, Rest extends unknown[]> = [P] extends [NoPayload]
	? [payload?: P, ...rest: Rest]
	: [payload: P, ...rest: Rest];

/** Extracts the output type `O` of a `Task`. */
export type TaskOutput<T extends AnyTask> = T extends Task<any, any, infer O> ? O : never;

// ---------------------------------------------------------------------------
// TaskContext / TaskLogger
// ---------------------------------------------------------------------------

/**
 * `ctx.logger`: entries at or above `logging.minLevel` are stored in `run_logs` in batches (see
 * `engine.runs.logs`) and mirrored to the engine logger. `data` larger than `maxLogEntryBytes`
 * is truncated with a marker.
 */
export type TaskLogger = {
	debug(message: string, data?: Json): void;
	info(message: string, data?: Json): void;
	warn(message: string, data?: Json): void;
	error(message: string, data?: Json): void;
};

/** Context passed as the second argument of a task's `run`. */
export type TaskContext = {
	step: StepApi;
	logger: TaskLogger;
	/** Fires on cancellation, `maxDurationMs` expiry, loss of run ownership, or graceful shutdown. */
	signal: AbortSignal;
	run: { id: string; taskId: string; attempt: number; createdAt: Date };
	/**
	 * Publishes an async sequence (e.g. LLM tokens) to the Redis Stream `name` of this run
	 * (`XADD MAXLEN ~`), readable live with `engine.runs.readStream(runId, name)`. Resolves once
	 * `source` is exhausted and the end marker is written (the stream then expires after 1 day).
	 * Not a step: not memoized, so a replay streams again.
	 */
	stream(name: string, source: AsyncIterable<Json | string>): Promise<void>;
};

// ---------------------------------------------------------------------------
// StepApi
// ---------------------------------------------------------------------------

export type TokenHandle = { id: string; name: string };

/** Result of a durable wait (`waitForToken`, `triggerAndWait`, `batchTriggerAndWait` per item). */
export type WaitResult<T> =
	| { ok: true; output: T }
	| { ok: false; error: import("./utils/error-format.js").SerializedError };

/**
 * A run handle returned by `trigger`/`step.trigger`. `__output` is a phantom
 * field (never present at runtime) carrying `O`, so a later `engine.runs.get`
 * can be typed from the handle it was given.
 */
export type RunHandle<O = unknown> = {
	id: string;
	taskId: string;
	readonly __output?: O;
};

/** Options accepted by `step.trigger` (idempotency is managed internally; see decision 9). */
export type StepTriggerOptions = {
	delay?: Duration;
};

/**
 * A step's return value, guarded the same way as {@link defineTask}'s `P`/`O`
 * (an intersection that adds a required, differently-named property when `T`
 * isn't {@link Serializable}), but applied to `fn`'s **return** type instead
 * of a parameter: `T extends Serializable<T>` as a type parameter's own
 * constraint would work here too in principle (`T` is inferred from a
 * covariant position, so there's no contravariance issue), but TS rejects
 * that self-referential constraint outright (`TS2313: circular constraint`),
 * so the guard is applied at the return-type call site instead, exactly like
 * `defineTask`.
 */
type StepRunResult<T> = [Serializable<T>] extends [T]
	? T
	: T & { readonly __stepOutputNotSerializable: Serializable<T> };

/** A child task's payload as accepted by `ctx.step.*` (checked for {@link Serializable}). */
type StepPayload<T extends AnyTask> = [TaskPayload<T>] extends [NoPayload]
	? TaskPayload<T>
	: TaskPayload<T> & Serializable<TaskPayload<T>>;

/** Full type surface of `ctx.step`, implemented across Phases 4-5. */
export type StepApi = {
	/** `T` must be {@link Serializable} (`void` allowed). */
	run<T>(name: string, fn: () => Promise<StepRunResult<T>> | StepRunResult<T>): Promise<T>;
	sleep(name: string, duration: Duration): Promise<void>;
	createToken(name: string, opts?: { timeout?: Duration }): Promise<TokenHandle>;
	waitForToken<T = Json>(tokenOrName: TokenHandle | string): Promise<WaitResult<T>>;
	/** The payload is optional for a task that takes none. */
	trigger<T extends AnyTask>(
		name: string,
		task: T,
		...args: PayloadArgs<StepPayload<T>, [opts?: StepTriggerOptions]>
	): Promise<RunHandle<TaskOutput<T>>>;
	/** The payload is optional for a task that takes none. */
	triggerAndWait<T extends AnyTask>(
		name: string,
		task: T,
		...args: PayloadArgs<StepPayload<T>, [opts?: { timeout?: Duration }]>
	): Promise<WaitResult<TaskOutput<T>>>;
	batchTriggerAndWait<T extends AnyTask>(
		name: string,
		task: T,
		payloads: ReadonlyArray<TaskPayload<T> & Serializable<TaskPayload<T>>>,
		opts?: { timeout?: Duration }
	): Promise<WaitResult<TaskOutput<T>>[]>;
};
