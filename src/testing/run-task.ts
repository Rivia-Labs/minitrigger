/**
 * `runTask(task, payload, options?)` (testing subpath only): runs a task's `run` function
 * directly, with **no engine, no store, no queue** -- for fast unit tests of a single task's
 * logic. `ctx.step.run` executes `fn` immediately (memoization/replay don't exist here: there is
 * nothing to replay against), `ctx.step.sleep` resolves instantly, and `ctx.step.createToken`/
 * `waitForToken`/`trigger*` need a test double supplied in `options` -- there is no waiting engine
 * to resolve them for real.
 *
 * This intentionally does **not** exercise replay, memoization across attempts, suspension,
 * non-determinism detection or durability: use `createTestEngine` (`./test-engine.js`) for that.
 * What it DOES reuse from the real implementation, so a task that passes here behaves the same in
 * production: step-name validation (`validateStepName`), payload/output `Serializable` + size
 * checks (`assertSerializable`), and `ctx.logger`'s own buffering/sanitization (`createLogBuffer`).
 */
import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { DEFAULTS } from "../config.js";
import {
	AbortTaskRunError,
	ConfigError,
	DuplicateStepError,
	MinitriggerError,
	NestedStepError,
} from "../errors.js";
import { createLogBuffer } from "../logs/buffer.js";
import type { LogLevel, NewLogEntry, StepKind } from "../store/types.js";
import type {
	AnyTask,
	PayloadArgs,
	RunHandle,
	StepApi,
	StepTriggerOptions,
	TaskContext,
	TaskOutput,
	TaskPayload,
	TokenHandle,
	WaitResult,
} from "../task.js";
import { type Duration, parseDuration } from "../utils/duration.js";
import { formatError, type SerializedError } from "../utils/error-format.js";
import { validateStepName } from "../utils/identifiers.js";
import { assertSerializable, type Json, normalizeOutput } from "../utils/serialization.js";

/** A test double that stands in for a `ctx.step.trigger*` child, keyed by the child task's id. */
export type ChildStub = (payload: Json) => unknown | Promise<unknown>;

export interface RunTaskOptions {
	/**
	 * Resolves `ctx.step.waitForToken(name)`, keyed by the `createToken`/`waitForToken` step name.
	 * The value becomes the wait's `{ ok: true, output }` -- or, if it's a function, its (possibly
	 * async) return value does. A `waitForToken` call with no entry here throws (no engine exists
	 * to resolve it later); `createToken` alone, never waited on, needs no entry.
	 * `runTask` doesn't simulate timeouts/failures this way; use `createTestEngine` for that.
	 */
	tokens?: Record<string, unknown>;
	/**
	 * Stands in for a child task's own `run`, keyed by the child task's id, for
	 * `ctx.step.trigger`/`triggerAndWait`/`batchTriggerAndWait`. A call whose task id has no entry
	 * throws. A stub that throws/rejects fails only its own `triggerAndWait`/`batchTriggerAndWait`
	 * result (`{ ok: false, error }`); `trigger` (fire-and-forget) propagates it like any step.
	 */
	children?: Record<string, ChildStub>;
	/** `ctx.signal`; never fires on its own when omitted. */
	signal?: AbortSignal;
	/**
	 * When the task throws with steps still in flight, how long to wait for them before reporting
	 * the error anyway (production bounds this wait by `stallTimeoutMs`; default: its default,
	 * 60 s). A fatal error never waits; a successful return always waits for every step.
	 */
	settleTimeoutMs?: number;
	/** `ctx.run.attempt` (default 1) -- set it to exercise attempt-dependent logic. */
	attempt?: number;
	/** Overrides the size limits normally read from `EngineConfig.limits` (see `config.ts`). */
	limits?: {
		maxPayloadBytes?: number;
		maxStepOutputBytes?: number;
		maxLogEntryBytes?: number;
		maxProgressBytes?: number;
	};
}

/** One step call recorded by `runTask`, in call order. */
export interface RunTaskStep {
	name: string;
	kind: StepKind;
	/** `run`: the memoized output. `token`/`trigger`: `{ id }`. Waits: `null` (no waitpoint here). */
	output: Json | null;
}

export interface RunTaskLogEntry {
	level: LogLevel;
	message: string;
	data: Json | null;
}

/** One `ctx.step.trigger`/`triggerAndWait`/`batchTriggerAndWait` call recorded by `runTask`. */
export interface RunTaskTriggered {
	name: string;
	taskId: string;
	mode: "trigger" | "triggerAndWait" | "batchTriggerAndWait";
	payload: Json;
	/** Set unless the stub threw (`triggerAndWait`/`batchTriggerAndWait` only). */
	output?: Json | null;
	error?: SerializedError;
}

export interface RunTaskResult<O = unknown> {
	/** Set only when `run` resolved and its output validated. */
	output: O | null;
	/** Set when `run` rejected, or its output failed validation. Never both `output` and `error`. */
	error: SerializedError | null;
	steps: RunTaskStep[];
	logs: RunTaskLogEntry[];
	/** Every value passed to `ctx.progress.set`, in call order. */
	progress: Json[];
	triggered: RunTaskTriggered[];
	/** Items collected per `ctx.stream(name, ...)` call, in the order they were read. */
	streams: Record<string, (Json | string)[]>;
}

/** A stub/resolver required by `runTask`'s options was missing; always thrown, never returned. */
class MissingTestDoubleError extends ConfigError {}

/**
 * Runs `task.run(payload, ctx)` directly. Never rejects for an error the task's own `run` threw
 * (or an invalid output) -- that comes back as `result.error`, exactly like `TestEngine#runInline`.
 * It DOES reject (synchronously, before `run` is even called) for an invalid `payload`, and
 * (after the task finishes, even if the task's own code caught it) for a
 * `ctx.step.waitForToken`/`trigger*` call missing its `options.tokens`/`options.children` stub --
 * both are test-authoring mistakes, not task bugs.
 *
 * Like production: steps started but never awaited are settled before it returns; after a fatal
 * error (invalid step name/argument, duplicate or nested step) every later `ctx.step.*` call
 * rethrows it and it is the reported `result.error` (`retryable: false`).
 */
export async function runTask<T extends AnyTask>(
	task: T,
	...args: PayloadArgs<TaskPayload<T>, [options?: RunTaskOptions]>
): Promise<RunTaskResult<TaskOutput<T>>> {
	const [rawPayload, options = {}] = args as [unknown, RunTaskOptions | undefined];
	const payload = rawPayload === undefined ? null : rawPayload;
	const limits = { ...DEFAULTS.limits, ...options.limits };
	const settleTimeoutMs = options.settleTimeoutMs ?? DEFAULTS.stallTimeoutMs;

	assertSerializable(payload, {
		label: `payload of task "${task.id}"`,
		maxBytes: limits.maxPayloadBytes,
	});

	const steps: RunTaskStep[] = [];
	const logEntries: RunTaskLogEntry[] = [];
	const progress: Json[] = [];
	const triggered: RunTaskTriggered[] = [];
	const streams: Record<string, (Json | string)[]> = {};
	const usedNames = new Set<string>();
	/** Names registered by `createToken` (production's `StepContext.tokens`), for `waitForToken(name)`. */
	const tokenNames = new Set<string>();
	/** Step promises not settled yet (production's `state.inFlight`), drained before returning. */
	const inFlight = new Set<Promise<unknown>>();
	/**
	 * The first missing test double: rethrown at the end of `runTask` even if the task's own code
	 * caught it (otherwise a `try/catch` in the task would let a test pass without the double).
	 */
	let missingDoubleError: MissingTestDoubleError | undefined;
	let fatal = false;
	/**
	 * The first fatal error (mirrors production's `state.fatalError`): kept even if the task's own
	 * code catches it (e.g. a nested step call inside a `try`) and lets `run` resolve normally --
	 * the run still fails without retry, whatever the function did afterwards (see the check right
	 * after `anyTask.options.run` below).
	 */
	let fatalError: unknown;
	/**
	 * Item 4(a): mirrors production's `StepContext.stepScope` (`src/executor/step-context.ts`) --
	 * an `AsyncLocalStorage` local to this `runTask` call (no module-level/global state, decision
	 * 13), set only while a `step.run` fn is executing, so a `ctx.step.*` call made from inside it
	 * (directly, or via anything it awaits) is detected as nested.
	 */
	const stepScope = new AsyncLocalStorage<string>();

	function markFatal<E>(error: E): E {
		fatal = true;
		fatalError ??= error;
		return error;
	}

	function missingDouble(message: string): MissingTestDoubleError {
		const error = new MissingTestDoubleError(message);
		missingDoubleError ??= error;
		return error;
	}

	/**
	 * Registers a step call, mirroring production's `StepContext.register()`: nested call check,
	 * then the earlier fatal error (user code that caught a code bug must not keep running steps),
	 * then duplicate names. Must run synchronously at call time.
	 */
	function register(method: string, name: string): void {
		assertNotNested(method, name);
		if (fatal) throw fatalError;
		markUsed(name);
	}

	/**
	 * Tracks a step promise as in flight until it settles. The `then(remove, remove)` also marks a
	 * rejection handled, like production's `rejected()`/`trackInFlight`: `void ctx.step.run("a:b")`
	 * must not become an `unhandledRejection` (awaiters still see the rejection).
	 */
	function track<R>(promise: Promise<R>): Promise<R> {
		inFlight.add(promise);
		const remove = () => {
			inFlight.delete(promise);
		};
		promise.then(remove, remove);
		return promise;
	}

	/**
	 * Waits for in-flight steps, like production before a terminal write. With `timeoutMs`, gives
	 * up after that long (production's bounded `settleBeforeFail`) and returns `false`.
	 */
	async function settleInFlight(timeoutMs?: number): Promise<boolean> {
		const drain = (async () => {
			while (inFlight.size > 0) await Promise.allSettled([...inFlight]);
		})();
		if (timeoutMs === undefined) {
			await drain;
			return true;
		}
		let timer: NodeJS.Timeout | undefined;
		const expired = new Promise<false>(resolve => {
			timer = setTimeout(() => resolve(false), timeoutMs);
		});
		try {
			return await Promise.race([drain.then(() => true as const), expired]);
		} finally {
			clearTimeout(timer);
		}
	}

	/** Throws (fatal, like production's `register()`) when called from inside a `step.run` fn. */
	function assertNotNested(method: string, name: string): void {
		const outer = stepScope.getStore();
		if (outer !== undefined) {
			throw markFatal(
				new NestedStepError(
					`ctx.step.${method}("${name}") called inside step.run("${outer}"): nested steps are ` +
						"not supported; call steps from the task body"
				)
			);
		}
	}

	function markUsed(name: string): void {
		if (usedNames.has(name)) {
			throw markFatal(
				new DuplicateStepError(
					`step "${name}" was called twice in this run of task "${task.id}"; step names must ` +
						"be unique within a run"
				)
			);
		}
		usedNames.add(name);
	}

	function parseDurationArg(value: Duration): number {
		try {
			return parseDuration(value);
		} catch (error) {
			throw markFatal(error as Error);
		}
	}

	function validateName(name: string): void {
		try {
			validateStepName(name);
		} catch (error) {
			throw markFatal(error as Error);
		}
	}

	function childPayload(taskId: string, payload: unknown): Json {
		const value = payload === undefined ? null : payload;
		try {
			assertSerializable(value, {
				label: `payload of task "${taskId}"`,
				maxBytes: limits.maxPayloadBytes,
			});
		} catch (error) {
			throw markFatal(error as Error);
		}
		return value as Json;
	}

	function childTaskId(taskArg: AnyTask, name: string, method: string): string {
		if (taskArg === null || typeof taskArg !== "object" || typeof taskArg.id !== "string") {
			throw markFatal(
				new ConfigError(
					`ctx.step.${method}("${name}", task, ...): task must be the object returned by defineTask, got ${typeof taskArg}`
				)
			);
		}
		return taskArg.id;
	}

	function childStub(taskId: string, name: string, method: string): ChildStub {
		const stub =
			options.children !== undefined && Object.hasOwn(options.children, taskId)
				? options.children[taskId]
				: undefined;
		if (stub === undefined) {
			throw missingDouble(
				`runTask: step "${name}" calls ctx.step.${method}(..., task) for task "${taskId}", but ` +
					`no stub was given for it; pass { children: { "${taskId}": (payload) => output } } ` +
					"to runTask(task, payload, options)"
			);
		}
		return stub;
	}

	async function run(name: string, fn: () => unknown): Promise<unknown> {
		validateName(name);
		if (typeof fn !== "function") {
			throw markFatal(
				new ConfigError(`ctx.step.run("${name}", fn): fn must be a function, got ${typeof fn}`)
			);
		}
		register("run", name);
		const output = normalizeOutput(await stepScope.run(name, fn));
		// A nested step (or another code bug) inside fn failed the run, even if fn caught it: like
		// production, do not record an output computed around it.
		if (fatal) throw fatalError;
		// Item 4(b): NOT wrapped in `markFatal` -- mirrors production (`step-context.ts`'s `run()`),
		// where an oversized/non-serializable step output is an ordinary `SerializationError` handed
		// back to the task's own code, not a fatal/no-retry condition. Uncaught, it becomes a normal
		// (retryable) failure of the run, exactly like any other error thrown by `fn`.
		assertSerializable(output, {
			label: `output of step "${name}" (task "${task.id}")`,
			maxBytes: limits.maxStepOutputBytes,
		});
		steps.push({ name, kind: "run", output: output as Json });
		return output;
	}

	async function sleep(name: string, duration: Duration): Promise<void> {
		validateName(name);
		parseDurationArg(duration);
		register("sleep", name);
		steps.push({ name, kind: "sleep", output: null });
	}

	async function createToken(name: string, opts?: { timeout?: Duration }): Promise<TokenHandle> {
		validateName(name);
		if (opts?.timeout !== undefined) parseDurationArg(opts.timeout);
		register("createToken", name);
		tokenNames.add(name);
		// Item 4(c): the recorded step output must be the SAME id the caller gets back (it wasn't --
		// the step used to record `{ id: name }`, the step name, instead of the token's actual id).
		const id = randomUUID();
		steps.push({ name, kind: "token", output: { id } });
		return { id, name };
	}

	async function waitForToken(tokenOrName: TokenHandle | string): Promise<WaitResult<unknown>> {
		let name: string;
		if (typeof tokenOrName === "string") {
			name = tokenOrName;
			// Like production: a name without an earlier `createToken` is `createToken(name)` first,
			// so a name already used by another step kind (e.g. `step.run("x")`) is a duplicate.
			// `createToken` has no `await` before registering, so this stays synchronous.
			if (!tokenNames.has(name)) await createToken(name);
		} else {
			if (
				tokenOrName === null ||
				typeof tokenOrName !== "object" ||
				typeof tokenOrName.id !== "string" ||
				typeof tokenOrName.name !== "string"
			) {
				throw markFatal(
					new MinitriggerError(
						"ctx.step.waitForToken(token): token must be the handle returned by ctx.step.createToken or a token name"
					)
				);
			}
			validateName(tokenOrName.name);
			name = tokenOrName.name;
		}
		register("waitForToken", `${name}#wait`);
		if (options.tokens === undefined || !Object.hasOwn(options.tokens, name)) {
			throw missingDouble(
				`runTask: step "${name}" calls ctx.step.waitForToken(...), but no resolver was given for ` +
					`it; pass { tokens: { "${name}": <value> } } to runTask(task, payload, options)`
			);
		}
		const resolver = options.tokens[name];
		const value = typeof resolver === "function" ? await (resolver as () => unknown)() : resolver;
		const output = normalizeOutput(value);
		try {
			assertSerializable(output, {
				label: `output of token "${name}" (task "${task.id}")`,
				maxBytes: limits.maxPayloadBytes,
			});
		} catch (error) {
			throw markFatal(error as Error);
		}
		steps.push({ name: `${name}#wait`, kind: "wait", output: null });
		return { ok: true, output };
	}

	async function trigger(
		name: string,
		taskArg: AnyTask,
		payload: unknown,
		opts?: StepTriggerOptions
	): Promise<RunHandle<unknown>> {
		validateName(name);
		const taskId = childTaskId(taskArg, name, "trigger");
		const value = childPayload(taskId, payload);
		if (opts?.delay !== undefined) parseDurationArg(opts.delay);
		register("trigger", name);
		const stub = childStub(taskId, name, "trigger");
		const output = normalizeOutput(await stub(value));
		triggered.push({ name, taskId, mode: "trigger", payload: value, output: output as Json });
		const id = randomUUID();
		steps.push({ name, kind: "trigger", output: { id } });
		return { id, taskId };
	}

	async function triggerAndWait(
		name: string,
		taskArg: AnyTask,
		payload: unknown,
		opts?: { timeout?: Duration }
	): Promise<WaitResult<unknown>> {
		validateName(name);
		const taskId = childTaskId(taskArg, name, "triggerAndWait");
		const value = childPayload(taskId, payload);
		if (opts?.timeout !== undefined) parseDurationArg(opts.timeout);
		register("triggerAndWait", name);
		const stub = childStub(taskId, name, "triggerAndWait");
		steps.push({ name, kind: "trigger_wait", output: null });
		return runChild(name, taskId, value, stub, "triggerAndWait");
	}

	async function batchTriggerAndWait(
		name: string,
		taskArg: AnyTask,
		payloads: readonly unknown[],
		opts?: { timeout?: Duration }
	): Promise<WaitResult<unknown>[]> {
		validateName(name);
		if (!Array.isArray(payloads)) {
			throw markFatal(
				new ConfigError(
					`ctx.step.batchTriggerAndWait("${name}"): payloads must be an array, got ${typeof payloads}`
				)
			);
		}
		const taskId = childTaskId(taskArg, name, "batchTriggerAndWait");
		const values = payloads.map(p => childPayload(taskId, p));
		if (opts?.timeout !== undefined) parseDurationArg(opts.timeout);
		register("batchTriggerAndWait", name);
		// Like production: an empty batch consumes the name (seq_index) but records nothing and
		// needs no stub.
		if (values.length === 0) return [];
		const stub = childStub(taskId, name, "batchTriggerAndWait");
		steps.push({ name, kind: "batch_wait", output: null });
		const results: WaitResult<unknown>[] = [];
		for (const value of values) {
			results.push(await runChild(name, taskId, value, stub, "batchTriggerAndWait"));
		}
		return results;
	}

	async function runChild(
		name: string,
		taskId: string,
		payload: Json,
		stub: ChildStub,
		mode: "triggerAndWait" | "batchTriggerAndWait"
	): Promise<WaitResult<unknown>> {
		try {
			const output = normalizeOutput(await stub(payload));
			triggered.push({ name, taskId, mode, payload, output: output as Json });
			return { ok: true, output };
		} catch (error) {
			const serialized = formatError(error, true);
			triggered.push({ name, taskId, mode, payload, error: serialized });
			return { ok: false, error: serialized };
		}
	}

	const step: StepApi = {
		run: ((name: string, fn: () => unknown) => track(run(name, fn))) as StepApi["run"],
		sleep: (name, duration) => track(sleep(name, duration)),
		createToken: (name, opts) => track(createToken(name, opts)),
		waitForToken: ((tokenOrName: TokenHandle | string) =>
			track(waitForToken(tokenOrName))) as StepApi["waitForToken"],
		trigger: ((name: string, taskArg: AnyTask, payload: unknown, opts?: StepTriggerOptions) =>
			track(trigger(name, taskArg, payload, opts))) as StepApi["trigger"],
		triggerAndWait: ((
			name: string,
			taskArg: AnyTask,
			payload: unknown,
			opts?: { timeout?: Duration }
		) => track(triggerAndWait(name, taskArg, payload, opts))) as StepApi["triggerAndWait"],
		batchTriggerAndWait: ((
			name: string,
			taskArg: AnyTask,
			payloads: readonly unknown[],
			opts?: { timeout?: Duration }
		) =>
			track(batchTriggerAndWait(name, taskArg, payloads, opts))) as StepApi["batchTriggerAndWait"],
	};

	const logBuffer = createLogBuffer({
		runId: `runTask:${task.id}`,
		maxLogEntryBytes: limits.maxLogEntryBytes,
		insertBatch: async (entries: NewLogEntry[]) => {
			for (const entry of entries) {
				logEntries.push({ level: entry.level, message: entry.message, data: entry.data });
			}
		},
	});

	const controller = options.signal === undefined ? new AbortController() : undefined;

	const ctx: TaskContext = {
		step,
		logger: logBuffer,
		signal: options.signal ?? (controller as AbortController).signal,
		run: {
			id: randomUUID(),
			taskId: task.id,
			attempt: options.attempt ?? 1,
			createdAt: new Date(),
		},
		async stream(name, source) {
			if (streams[name] === undefined) streams[name] = [];
			const items = streams[name];
			for await (const item of source) items.push(item);
		},
		progress: {
			async set<V>(value: V): Promise<void> {
				// Same as production (`execute-run.ts`): validate the value as given (`undefined`
				// is rejected, not normalized to null), then snapshot it so a later mutation of the
				// caller's object can't change what was recorded.
				assertSerializable(value, {
					label: `progress of task "${task.id}"`,
					maxBytes: limits.maxProgressBytes,
				});
				progress.push(structuredClone(value) as Json);
			},
		},
	};

	const anyTask = task as AnyTask;
	let outcome: { ok: true; value: unknown } | { ok: false; error: unknown };
	try {
		outcome = { ok: true, value: await anyTask.options.run(payload, ctx) };
	} catch (error) {
		outcome = { ok: false, error };
	}
	// Production settles every step still in flight before the terminal write (success: steps not
	// awaited are still recorded; error: `settleBeforeFail`), so `steps`/`triggered` are complete.
	// Production parity: a fatal error (code bug) fails the run right away, without waiting for
	// steps in flight; a thrown error waits at most `settleTimeoutMs` (production: `stallTimeoutMs`)
	// and then reports the original error; success drains everything (unawaited steps recorded).
	if (!fatal) {
		const settled = await settleInFlight(outcome.ok ? undefined : settleTimeoutMs);
		// Like production's `abort("finished")` after giving up: tell stuck steps the attempt ended.
		if (!settled)
			controller?.abort(new MinitriggerError("runTask finished while steps were in flight"));
	}
	await logBuffer.close();
	// A missing test double is a test-authoring mistake: surface it even if the task caught it.
	if (missingDoubleError !== undefined) throw missingDoubleError;

	const failed = (error: unknown, retryable: boolean): RunTaskResult<TaskOutput<T>> => ({
		output: null,
		error: formatError(error, retryable),
		steps,
		logs: logEntries,
		progress,
		triggered,
		streams,
	});
	// A fatal error (a code bug such as NestedStepError or an invalid step name) fails the run
	// without retry and is the reported error, whatever the function did afterwards -- even if it
	// caught it and resolved, or threw something else (production's `state.fatalError`).
	if (fatal) return failed(fatalError, false);
	if (!outcome.ok) return failed(outcome.error, !(outcome.error instanceof AbortTaskRunError));
	const output = normalizeOutput(outcome.value);
	try {
		assertSerializable(output, {
			label: `output of task "${task.id}"`,
			maxBytes: limits.maxPayloadBytes,
		});
	} catch (error) {
		return failed(error, false);
	}
	return {
		output: output as TaskOutput<T>,
		error: null,
		steps,
		logs: logEntries,
		progress,
		triggered,
		streams,
	};
}
