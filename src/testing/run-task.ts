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
 * (whenever the task's own code reaches it) for a `ctx.step.waitForToken`/`trigger*` call missing
 * its `options.tokens`/`options.children` stub -- both are test-authoring mistakes, not task bugs.
 */
export async function runTask<T extends AnyTask>(
	task: T,
	...args: PayloadArgs<TaskPayload<T>, [options?: RunTaskOptions]>
): Promise<RunTaskResult<TaskOutput<T>>> {
	const [rawPayload, options = {}] = args as [unknown, RunTaskOptions | undefined];
	const payload = rawPayload === undefined ? null : rawPayload;
	const limits = { ...DEFAULTS.limits, ...options.limits };

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

	function childStub(
		taskArg: AnyTask,
		name: string,
		method: string
	): { id: string; stub: ChildStub } {
		if (taskArg === null || typeof taskArg !== "object" || typeof taskArg.id !== "string") {
			throw markFatal(
				new ConfigError(
					`ctx.step.${method}("${name}", task, ...): task must be the object returned by defineTask, got ${typeof taskArg}`
				)
			);
		}
		const stub = options.children?.[taskArg.id];
		if (stub === undefined) {
			throw new MissingTestDoubleError(
				`runTask: step "${name}" calls ctx.step.${method}(..., task) for task "${taskArg.id}", but ` +
					`no stub was given for it; pass { children: { "${taskArg.id}": (payload) => output } } ` +
					"to runTask(task, payload, options)"
			);
		}
		return { id: taskArg.id, stub };
	}

	async function run(name: string, fn: () => unknown): Promise<unknown> {
		validateName(name);
		assertNotNested("run", name);
		markUsed(name);
		if (typeof fn !== "function") {
			throw markFatal(
				new ConfigError(`ctx.step.run("${name}", fn): fn must be a function, got ${typeof fn}`)
			);
		}
		const output = normalizeOutput(await stepScope.run(name, fn));
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
		assertNotNested("sleep", name);
		markUsed(name);
		parseDurationArg(duration);
		steps.push({ name, kind: "sleep", output: null });
	}

	async function createToken(name: string, opts?: { timeout?: Duration }): Promise<TokenHandle> {
		validateName(name);
		assertNotNested("createToken", name);
		markUsed(name);
		if (opts?.timeout !== undefined) parseDurationArg(opts.timeout);
		// Item 4(c): the recorded step output must be the SAME id the caller gets back (it wasn't --
		// the step used to record `{ id: name }`, the step name, instead of the token's actual id).
		const id = randomUUID();
		steps.push({ name, kind: "token", output: { id } });
		return { id, name };
	}

	async function waitForToken(tokenOrName: TokenHandle | string): Promise<WaitResult<unknown>> {
		assertNotNested(
			"waitForToken",
			typeof tokenOrName === "string" ? tokenOrName : (tokenOrName?.name ?? String(tokenOrName))
		);
		let name: string;
		if (typeof tokenOrName === "string") {
			name = tokenOrName;
			if (!usedNames.has(name)) await createToken(name);
		} else {
			if (
				tokenOrName === null ||
				typeof tokenOrName !== "object" ||
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
		markUsed(`${name}#wait`);
		if (options.tokens === undefined || !Object.hasOwn(options.tokens, name)) {
			throw new MissingTestDoubleError(
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
		assertNotNested("trigger", name);
		markUsed(name);
		const { id: taskId, stub } = childStub(taskArg, name, "trigger");
		const value = childPayload(taskId, payload);
		if (opts?.delay !== undefined) parseDurationArg(opts.delay);
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
		assertNotNested("triggerAndWait", name);
		markUsed(name);
		const { id: taskId, stub } = childStub(taskArg, name, "triggerAndWait");
		const value = childPayload(taskId, payload);
		if (opts?.timeout !== undefined) parseDurationArg(opts.timeout);
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
		assertNotNested("batchTriggerAndWait", name);
		markUsed(name);
		if (!Array.isArray(payloads)) {
			throw markFatal(
				new ConfigError(
					`ctx.step.batchTriggerAndWait("${name}"): payloads must be an array, got ${typeof payloads}`
				)
			);
		}
		if (opts?.timeout !== undefined) parseDurationArg(opts.timeout);
		if (payloads.length === 0) {
			steps.push({ name, kind: "batch_wait", output: null });
			return [];
		}
		const { id: taskId, stub } = childStub(taskArg, name, "batchTriggerAndWait");
		steps.push({ name, kind: "batch_wait", output: null });
		const results: WaitResult<unknown>[] = [];
		for (const payload of payloads) {
			const value = childPayload(taskId, payload);
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
		run: run as StepApi["run"],
		sleep,
		createToken,
		waitForToken: waitForToken as StepApi["waitForToken"],
		trigger: trigger as StepApi["trigger"],
		triggerAndWait: triggerAndWait as StepApi["triggerAndWait"],
		batchTriggerAndWait: batchTriggerAndWait as StepApi["batchTriggerAndWait"],
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
				const normalized = normalizeOutput(value) as Json;
				assertSerializable(normalized, {
					label: `progress of task "${task.id}"`,
					maxBytes: limits.maxProgressBytes,
				});
				progress.push(normalized);
			},
		},
	};

	const anyTask = task as AnyTask;
	try {
		const rawOutput = await anyTask.options.run(payload, ctx);
		// A fatal error (e.g. NestedStepError) that the task's own code caught and swallowed still
		// fails the run without retry, exactly like production (`execute-run.ts`'s check of
		// `state.fatalError` after the function returns, regardless of the outcome it produced).
		if (fatal) throw fatalError;
		const output = normalizeOutput(rawOutput);
		try {
			assertSerializable(output, {
				label: `output of task "${task.id}"`,
				maxBytes: limits.maxPayloadBytes,
			});
		} catch (error) {
			throw markFatal(error as Error);
		}
		await logBuffer.close();
		return {
			output: output as TaskOutput<T>,
			error: null,
			steps,
			logs: logEntries,
			progress,
			triggered,
			streams,
		};
	} catch (error) {
		await logBuffer.close();
		if (error instanceof MissingTestDoubleError) throw error;
		const retryable = !fatal && !(error instanceof AbortTaskRunError);
		return {
			output: null,
			error: formatError(error, retryable),
			steps,
			logs: logEntries,
			progress,
			triggered,
			streams,
		};
	}
}
