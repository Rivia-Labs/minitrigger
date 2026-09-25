import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import {
	ConfigError,
	DuplicateStepError,
	ExecutionLostError,
	MinitriggerError,
	NestedStepError,
	NonDeterminismError,
} from "../errors.js";
import {
	CANCELED_ERROR_NAME,
	type ChildResult,
	type EnqueueRequest,
	type ExecutionSnapshot,
	type LoadedStep,
	type RunStore,
	type SerializedError,
	type StepKind,
	type StepStore,
	type TriggerChildrenInput,
	WAITPOINT_TIMEOUT_REASON,
	type WaitpointStore,
	type WaitpointSummary,
} from "../store/types.js";
import type { AnyTask, RunHandle, StepApi, TokenHandle, WaitResult } from "../task.js";
import { type Duration, parseDuration } from "../utils/duration.js";
import { validateStepName } from "../utils/identifiers.js";
import { assertSerializable, type Json, normalizeOutput } from "../utils/serialization.js";
import type { ExecutionState } from "./execution.js";

/**
 * Internal suspension signal (decision 2). Thrown by `ctx.step.*` when the execution must stop and
 * wait (a sleep that is not due yet, and in Phase 5 tokens/child runs). The executor catches the
 * suspension through {@link ExecutionState.onSuspend}, never through the task function's outcome,
 * so a user `try/catch` that swallows it cannot turn it into a success or a retry. Not exported
 * from the package.
 */
export class SuspendSignal extends Error {
	override name = "SuspendSignal";

	constructor(runId: string) {
		super(
			`run ${runId} is suspending. This is an internal minitrigger signal: do not catch it ` +
				"(or rethrow it) around ctx.step calls"
		);
	}
}

/**
 * Deferred outputs (window query over `maxInlineStepOutputBytes`) are loaded in aligned
 * `seq_index` ranges of this size, on demand as the replay reaches them.
 */
export const DEFERRED_OUTPUT_BATCH_SIZE = 50;

/**
 * Step kinds (`steps.kind`, compared exactly on replay). Phase 4 implements `run` and `sleep`;
 * Phase 5 adds `token` (`createToken`), `wait` (`${name}#wait`, via
 * {@link StepContext.startInternal}), `trigger`, `trigger_wait` (`triggerAndWait`) and
 * `batch_wait` (`batchTriggerAndWait`) on top of {@link StepContext.register}.
 */
export type { StepKind };

/** The `ctx.step` method of each kind (error messages). */
const METHOD_BY_KIND: Record<StepKind, string> = {
	run: "run",
	sleep: "sleep",
	token: "createToken",
	wait: "waitForToken",
	trigger: "trigger",
	trigger_wait: "triggerAndWait",
	batch_wait: "batchTriggerAndWait",
};

/** A step call after registration: its name, its call-time `seq_index` and the saved row, if any. */
export interface StepRegistration {
	kind: StepKind;
	name: string;
	seqIndex: number;
	/** The step recorded at this `seq_index` by an earlier execution (same name, checked). */
	saved: LoadedStep | undefined;
}

/** What a child run needs from its task definition (`triggerChildren`). */
export interface ChildTaskSpec {
	id: string;
	queue: string;
	maxAttempts: number;
}

/**
 * Phase 5 dependencies (tokens and subtasks). `enqueueRun` and `scheduleExpire` run after the
 * store transaction committed and never throw: a lost enqueue is recovered by the reconciler
 * from Postgres (decision 1; case 5 for a missing expire job).
 */
export interface StepWaitDeps {
	runs: Pick<RunStore, "triggerChildren" | "listChildResults">;
	waitpoints: Pick<WaitpointStore, "createToken" | "getToken">;
	/** Child payloads follow the payload limit (decision 12). */
	maxPayloadBytes: number;
	/** Registered task -> child spec; throws `TaskNotRegisteredError`. */
	resolveTask(task: AnyTask): ChildTaskSpec;
	enqueueRun(request: EnqueueRequest): Promise<void>;
	/** Adds `waitpoint-expire` with jobId `wp-${id}-expire` (deterministic: re-adding is a no-op). */
	scheduleExpire(waitpointId: string, delayMs: number): Promise<void>;
}

export interface StepContextDeps {
	steps: StepStore;
	maxStepOutputBytes: number;
	waits: StepWaitDeps;
}

/** `error.name` of a `WaitResult` whose waitpoint timed out. */
export const TIMEOUT_ERROR_NAME = "Timeout";

/**
 * `ctx.step` of one execution (Phase 4; decisions 2, 3, 5, 11, 12, 17).
 *
 * Built from the {@link ExecutionSnapshot} loaded with ONE query at the start of the execution.
 * Every `step.*` call goes through {@link register}, synchronously at call time (before any
 * `await`): it rethrows a pending suspension, validates uniqueness, assigns the next `seq_index`
 * and checks it against the snapshot (name at that index, index of that name, step kind). The
 * returned promise is tracked in {@link ExecutionState.inFlight}, so the executor can
 * `Promise.allSettled` them before running the suspension transaction.
 */
export class StepContext {
	private readonly state: ExecutionState;
	private readonly deps: StepContextDeps;
	private readonly snapshot: ExecutionSnapshot;
	/** Monotonic time at which `snapshot.now` (DB clock) was received. */
	private readonly loadedAtMono: number;
	private nextSeq = 0;
	private readonly usedNames = new Set<string>();
	private readonly savedBySeq = new Map<number, LoadedStep>();
	private readonly savedByName = new Map<string, LoadedStep>();
	private readonly deferredOutputs = new Map<number, Json | null>();
	private readonly deferredBatches = new Map<number, Promise<void>>();
	/**
	 * Name of the `step.run` whose callback is executing in the current async context. Set only
	 * while a step fn runs (and inherited by everything it awaits/spawns), so a `step.*` call made
	 * from inside a step fn is detected as nested; calls from the task body see `undefined`, even
	 * while other step fns are in flight (`Promise.all`).
	 */
	private readonly stepScope = new AsyncLocalStorage<string>();
	/** Waitpoints of the run at load time, by `step_name` (token: the `createToken` name). */
	private readonly waitpointsByStep = new Map<string, WaitpointSummary>();
	/** `createToken` calls registered in this execution, by name (for `waitForToken(name)`). */
	private readonly tokens = new Map<string, Promise<TokenHandle>>();

	readonly api: StepApi;

	constructor(state: ExecutionState, deps: StepContextDeps, snapshot: ExecutionSnapshot) {
		this.state = state;
		this.deps = deps;
		this.snapshot = snapshot;
		this.loadedAtMono = performance.now();
		for (const step of snapshot.steps) {
			this.savedBySeq.set(step.seqIndex, step);
			this.savedByName.set(step.name, step);
		}
		for (const wp of snapshot.waitpoints) this.waitpointsByStep.set(wp.stepName, wp);

		this.api = {
			run: ((name: string, fn: () => unknown) => this.run(name, fn)) as StepApi["run"],
			sleep: (name, duration) => this.sleep(name, duration),
			createToken: (name, opts) => this.createToken(name, opts),
			waitForToken: (<T>(tokenOrName: TokenHandle | string) =>
				this.waitForToken(tokenOrName) as Promise<WaitResult<T>>) as StepApi["waitForToken"],
			trigger: ((name: string, task: AnyTask, payload?: unknown, opts?: { delay?: Duration }) =>
				this.trigger(name, task, payload, opts)) as StepApi["trigger"],
			triggerAndWait: ((
				name: string,
				task: AnyTask,
				payload?: unknown,
				opts?: { timeout?: Duration }
			) => this.triggerAndWait(name, task, payload, opts)) as StepApi["triggerAndWait"],
			batchTriggerAndWait: ((
				name: string,
				task: AnyTask,
				payloads: readonly unknown[],
				opts?: { timeout?: Duration }
			) => this.batchTriggerAndWait(name, task, payloads, opts)) as StepApi["batchTriggerAndWait"],
		};
	}

	// -------------------------------------------------------------------------
	// Shared machinery (Phase 5 builds its step kinds on these)
	// -------------------------------------------------------------------------

	/**
	 * Registers a step call. MUST be called synchronously when the user calls `step.*`, so that
	 * `Promise.all` of several steps gets a deterministic `seq_index` order (decision 3). `name` is
	 * the stored step name: callers validate user names with `validateStepName` first (internal
	 * names such as `${name}#wait` skip that check).
	 *
	 * Throws (synchronously): {@link NestedStepError} (called inside a `step.run` fn), the
	 * execution's earlier fatal error, the pending `SuspendSignal` (authoritative suspension), the
	 * abort reason of an aborted execution, {@link DuplicateStepError} or
	 * {@link NonDeterminismError}. Nested/duplicate/non-deterministic calls are recorded as the
	 * execution's fatal error: the run fails without retry.
	 */
	register(kind: StepKind, name: string): StepRegistration {
		const outer = this.stepScope.getStore();
		if (outer !== undefined) {
			throw this.fatal(
				new NestedStepError(
					`ctx.step.${METHOD_BY_KIND[kind]}("${name}") called inside step.run("${outer}"): ` +
						"nested steps are not supported; call steps from the task body"
				)
			);
		}
		this.assertCanStart();
		if (this.usedNames.has(name)) {
			throw this.fatal(
				new DuplicateStepError(
					`step "${name}" was called twice in one execution of task "${this.state.taskId}"; ` +
						"step names must be unique within a run"
				)
			);
		}
		const seqIndex = this.nextSeq++;
		this.usedNames.add(name);

		const atSeq = this.savedBySeq.get(seqIndex);
		if (atSeq !== undefined && atSeq.name !== name) {
			throw this.fatal(
				new NonDeterminismError(
					`non-deterministic replay of task "${this.state.taskId}" (run ${this.state.runId}): ` +
						`step #${seqIndex} is "${name}" now, but was "${atSeq.name}" in an earlier execution`
				)
			);
		}
		const byName = this.savedByName.get(name);
		if (byName !== undefined && byName.seqIndex !== seqIndex) {
			throw this.fatal(
				new NonDeterminismError(
					`non-deterministic replay of task "${this.state.taskId}" (run ${this.state.runId}): ` +
						`step "${name}" is step #${seqIndex} now, but was step #${byName.seqIndex} in an earlier execution`
				)
			);
		}
		if (atSeq !== undefined && atSeq.kind !== kind) {
			throw this.fatal(
				new NonDeterminismError(
					`non-deterministic replay of task "${this.state.taskId}" (run ${this.state.runId}): ` +
						`step "${name}" is a ${kind} step now, but was a ${atSeq.kind} step in an earlier execution`
				)
			);
		}
		return { kind, name, seqIndex, saved: atSeq };
	}

	/**
	 * A user-facing step call: validates the user's step name (an invalid name is a code bug:
	 * fatal, no retry), runs `prepare` (argument checks), registers synchronously, then tracks
	 * `body`'s promise as in flight. Errors come back as a rejected promise (never a synchronous
	 * throw).
	 */
	start<T>(
		kind: StepKind,
		userName: string,
		prepare: () => void,
		body: (reg: StepRegistration) => Promise<T>
	): Promise<T> {
		try {
			validateStepName(userName);
		} catch (error) {
			return rejected(this.fatal(error as Error));
		}
		return this.begin(kind, userName, prepare, body);
	}

	/**
	 * An internal step name (e.g. `${name}#wait` of `waitForToken`): skips the user-name
	 * validation (`#` is reserved for these) but otherwise behaves like {@link start}.
	 */
	startInternal<T>(
		kind: StepKind,
		name: string,
		body: (reg: StepRegistration) => Promise<T>
	): Promise<T> {
		return this.begin(kind, name, noop, body);
	}

	private begin<T>(
		kind: StepKind,
		name: string,
		prepare: () => void,
		body: (reg: StepRegistration) => Promise<T>
	): Promise<T> {
		let reg: StepRegistration;
		try {
			prepare();
			reg = this.register(kind, name);
		} catch (error) {
			return rejected(error);
		}
		return this.state.trackInFlight(body(reg), name);
	}

	/** Marks the execution suspended (decision 2) and returns the signal to throw. */
	suspend(): Error {
		const signal = this.state.suspendSignal ?? new SuspendSignal(this.state.runId);
		this.state.markSuspended(signal);
		return signal;
	}

	/** A guarded write returned `lost` (decision 5): abort `ctx.signal`, return the error to throw. */
	lost(what: string): ExecutionLostError {
		this.state.abort("lost");
		return new ExecutionLostError(
			`run ${this.state.runId}: this execution lost ownership of the run (canceled, timed out ` +
				`or recovered elsewhere) while ${what}; nothing was written`
		);
	}

	/**
	 * Estimate of the database clock now: `snapshot.now` plus the monotonic time elapsed since it
	 * was received. It is a lower bound of the real DB `now()` (decision 17), so "due" by this
	 * estimate means due in the DB too; every decision is still re-checked by the DB statement.
	 */
	dbNowMs(): number {
		return this.snapshot.now.getTime() + (performance.now() - this.loadedAtMono);
	}

	/** Output of a saved step, loading deferred outputs lazily in `seq_index` batches. */
	async savedOutput(step: LoadedStep): Promise<Json | null> {
		if (!this.snapshot.outputsDeferred) return step.output;
		if (!this.deferredOutputs.has(step.seqIndex)) await this.loadDeferredBatch(step.seqIndex);
		const output = this.deferredOutputs.get(step.seqIndex) ?? null;
		// Each step is replayed at most once per execution (names are unique).
		this.deferredOutputs.delete(step.seqIndex);
		return output;
	}

	private loadDeferredBatch(seqIndex: number): Promise<void> {
		const from = Math.floor(seqIndex / DEFERRED_OUTPUT_BATCH_SIZE) * DEFERRED_OUTPUT_BATCH_SIZE;
		let pending = this.deferredBatches.get(from);
		if (pending === undefined) {
			pending = this.deps.steps
				.loadOutputs(this.state.runId, from, from + DEFERRED_OUTPUT_BATCH_SIZE - 1)
				.then(rows => {
					for (const row of rows) this.deferredOutputs.set(row.seqIndex, row.output);
				});
			// A failed load may be retried by a later step of the same batch.
			pending.catch(() => this.deferredBatches.delete(from));
			this.deferredBatches.set(from, pending);
		}
		return pending;
	}

	private assertCanStart(): void {
		// A code bug already failed this execution: user code that caught it must not keep going.
		if (this.state.fatalError !== undefined) throw this.state.fatalError;
		if (this.state.suspended) throw this.state.suspendSignal ?? new SuspendSignal(this.state.runId);
		if (this.state.abortKind !== undefined) {
			const reason: unknown = this.state.signal.reason;
			throw reason instanceof Error ? reason : new ExecutionLostError(String(reason));
		}
	}

	private fatal(error: Error): Error {
		this.state.markFatal(error);
		return error;
	}

	/**
	 * Parses a `Duration` argument passed to a `ctx.step.*` call (`sleep`'s duration, `timeout`,
	 * `delay`), marking a bad one as this execution's fatal error. Like an invalid step name
	 * (`validateStepName`), a malformed duration is a bug in the calling code, not a transient
	 * condition: retrying the same call would fail identically, so the run fails without retry
	 * instead of counting a wasted attempt.
	 */
	private parseDurationArg(value: Duration): number {
		try {
			return parseDuration(value);
		} catch (error) {
			throw this.fatal(error as Error);
		}
	}

	// -------------------------------------------------------------------------
	// Phase 4 step kinds
	// -------------------------------------------------------------------------

	/** `ctx.step.run(name, fn)`: memoized; `fn` runs outside any transaction; its errors propagate. */
	run(name: string, fn: () => unknown): Promise<unknown> {
		return this.start(
			"run",
			name,
			() => {
				if (typeof fn !== "function") {
					throw this.fatal(
						new ConfigError(`ctx.step.run("${name}", fn): fn must be a function, got ${typeof fn}`)
					);
				}
			},
			async reg => {
				if (reg.saved !== undefined) return this.savedOutput(reg.saved);

				const output = normalizeOutput(await this.stepScope.run(reg.name, fn));
				// A nested step (or another code bug) inside fn failed the execution, even if fn
				// caught it: do not persist an output computed around it.
				if (this.state.fatalError !== undefined) throw this.state.fatalError;
				assertSerializable(output, {
					label: `output of step "${reg.name}" (task "${this.state.taskId}")`,
					maxBytes: this.deps.maxStepOutputBytes,
				});
				const result = await this.deps.steps.insertCompleted(this.state.owner, {
					kind: "run",
					name: reg.name,
					seqIndex: reg.seqIndex,
					output: output as Json,
				});
				if (result === "lost") throw this.lost(`saving step "${reg.name}"`);
				return output;
			}
		);
	}

	/**
	 * `ctx.step.sleep(name, duration)`: the first call records a `waiting` step with
	 * `wake_at = now() + duration` (DB clock) and suspends. On replay, a due sleep is completed
	 * (guarded, re-checked against DB `now()`) and the function goes on; otherwise it suspends again.
	 */
	sleep(name: string, duration: Duration): Promise<void> {
		let durationMs = 0;
		return this.start(
			"sleep",
			name,
			() => {
				durationMs = this.parseDurationArg(duration);
			},
			async reg => {
				const saved = reg.saved;
				if (saved !== undefined) {
					if (saved.status === "completed") return;
					if (saved.wakeAt !== null && saved.wakeAt.getTime() <= this.dbNowMs()) {
						const result = await this.deps.steps.completeSleep(this.state.owner, reg.name);
						if (result === "ok") return;
						if (result === "lost") throw this.lost(`completing sleep "${reg.name}"`);
						// "not_due": the DB clock disagrees with the estimate; wait some more.
					}
					throw this.suspend();
				}

				const result = await this.deps.steps.insertSleep(this.state.owner, {
					name: reg.name,
					seqIndex: reg.seqIndex,
					durationMs,
				});
				if (result.kind === "lost") throw this.lost(`recording sleep "${reg.name}"`);
				throw this.suspend();
			}
		);
	}

	// -------------------------------------------------------------------------
	// Phase 5 step kinds: tokens and subtasks (decisions 3, 9, 10, 11, 12)
	// -------------------------------------------------------------------------

	/**
	 * `ctx.step.createToken(name, { timeout })`: step `name` (kind `token`, output `{ id }`) and a
	 * `token` waitpoint in one transaction; with a timeout, the `wp-${id}-expire` job after commit.
	 * On replay the saved `{ id }` is returned; if the token is still pending with an expiry, the
	 * expire job is re-added with the same deterministic jobId (a no-op while it exists; recreates
	 * it if the first execution died between the commit and the enqueue).
	 */
	createToken(name: string, opts?: { timeout?: Duration }): Promise<TokenHandle> {
		let timeoutMs: number | null = null;
		let registered = false;
		const promise = this.start(
			"token",
			name,
			() => {
				if (opts?.timeout !== undefined) timeoutMs = this.parseDurationArg(opts.timeout);
			},
			async reg => {
				registered = true;
				if (reg.saved !== undefined) {
					const id = idOf(await this.savedOutput(reg.saved), reg.name);
					const wp = this.waitpointsByStep.get(reg.name);
					if (wp?.id === id && wp.status === "pending" && wp.expiresAt !== null) {
						const delayMs = Math.max(0, Math.ceil(wp.expiresAt.getTime() - this.dbNowMs()));
						await this.deps.waits.scheduleExpire(id, delayMs);
					}
					return { id, name: reg.name };
				}
				const result = await this.deps.waits.waitpoints.createToken(this.state.owner, {
					waitpointId: randomUUID(),
					name: reg.name,
					seqIndex: reg.seqIndex,
					timeoutMs,
				});
				if (result.kind === "lost") throw this.lost(`creating token "${reg.name}"`);
				if (result.expireDelayMs !== null) {
					await this.deps.waits.scheduleExpire(result.waitpointId, result.expireDelayMs);
				}
				return { id: result.waitpointId, name: reg.name };
			}
		);
		// The body starts synchronously inside `start`: `registered` tells whether the call got a
		// seq_index (a duplicate/invalid name did not).
		if (registered) this.tokens.set(name, promise);
		return promise;
	}

	/**
	 * `ctx.step.waitForToken(tokenOrName)`: step `${name}#wait` (kind `wait`) holding only the
	 * waitpoint reference (decision 10). A name without a `createToken` earlier in this execution
	 * is `createToken(name)` followed by the wait (decision 11). Resolved -> the output read from
	 * `waitpoints.output`; timed out -> `{ ok: false, error: { name: "Timeout" } }`; pending ->
	 * suspends (the suspension transaction re-checks, decision 3).
	 */
	waitForToken(tokenOrName: TokenHandle | string): Promise<WaitResult<unknown>> {
		let name: string;
		let token: Promise<TokenHandle>;
		if (typeof tokenOrName === "string") {
			name = tokenOrName;
			const existing = this.tokens.get(name);
			if (existing !== undefined) {
				token = existing;
			} else {
				token = this.createToken(name);
				// Not registered (invalid/duplicate name, suspended, ...): its rejection says why.
				if (!this.tokens.has(name)) return token as Promise<never>;
			}
		} else {
			if (
				tokenOrName === null ||
				typeof tokenOrName !== "object" ||
				typeof tokenOrName.id !== "string" ||
				typeof tokenOrName.name !== "string"
			) {
				return rejected(
					this.fatal(
						new MinitriggerError(
							"ctx.step.waitForToken(token): token must be the handle returned by ctx.step.createToken or a token name"
						)
					)
				);
			}
			try {
				validateStepName(tokenOrName.name);
			} catch (error) {
				return rejected(this.fatal(error as Error));
			}
			name = tokenOrName.name;
			token = Promise.resolve({ id: tokenOrName.id, name: tokenOrName.name });
		}

		return this.startInternal("wait", `${name}#wait`, async reg => {
			const { id } = await token;
			if (reg.saved === undefined) {
				const result = await this.deps.steps.insertWaitRef(this.state.owner, {
					kind: "wait",
					name: reg.name,
					seqIndex: reg.seqIndex,
					waitpointId: id,
				});
				if (result === "lost") throw this.lost(`recording wait "${reg.name}"`);
			} else {
				// Snapshot shortcut: still pending at load time -> suspend (the suspension
				// transaction re-checks and requeues right away if it resolved meanwhile).
				const wp = this.waitpointsByStep.get(name);
				if (wp?.id === id) {
					if (wp.status === "pending") throw this.suspend();
					if (wp.status === "timed_out") return timedOut(`token "${name}"`);
				}
			}
			// The token may already be resolved (completed while this execution was running):
			// resume without suspending. The result is final, so a replay reads the same value.
			const current = await this.deps.waits.waitpoints.getToken(id);
			if (current === null || current.runId !== this.state.runId) {
				throw this.fatal(
					new MinitriggerError(
						`ctx.step.waitForToken("${name}"): token ${id} does not belong to run ${this.state.runId}`
					)
				);
			}
			switch (current.status) {
				case "completed":
					return { ok: true, output: current.output };
				case "timed_out":
					return timedOut(`token "${name}"`);
				case "pending":
					throw this.suspend();
			}
		});
	}

	/**
	 * `ctx.step.trigger(name, task, payload, { delay })`: child run with the internal key
	 * `__child:${runId}:${name}` (decision 9), no waitpoint, memoized handle (step kind `trigger`,
	 * output `{ id }`). The child is enqueued after commit.
	 */
	trigger(
		name: string,
		task: AnyTask,
		payload: unknown,
		opts: { delay?: Duration } | undefined
	): Promise<RunHandle<unknown>> {
		let spec!: ChildTaskSpec;
		let json: Json = null;
		let delayMs = 0;
		return this.start(
			"trigger",
			name,
			() => {
				spec = this.deps.waits.resolveTask(task);
				json = this.childPayload(spec.id, payload);
				if (opts?.delay !== undefined) delayMs = this.parseDurationArg(opts.delay);
			},
			async reg => {
				if (reg.saved !== undefined) {
					return { id: idOf(await this.savedOutput(reg.saved), reg.name), taskId: spec.id };
				}
				const enqueue = await this.createChildren(reg, "trigger", spec, [json], null, delayMs);
				const first = enqueue[0];
				if (first === undefined) throw new Error("triggerChildren returned no child");
				return { id: first.runId, taskId: spec.id };
			}
		);
	}

	/**
	 * `ctx.step.triggerAndWait(name, task, payload, { timeout })`: child run + `child_run`
	 * waitpoint + waiting step in ONE transaction (decision 10), then enqueue + suspend. On replay
	 * the result is read from the child's `runs` row (never copied into `steps.output`).
	 */
	triggerAndWait(
		name: string,
		task: AnyTask,
		payload: unknown,
		opts: { timeout?: Duration } | undefined
	): Promise<WaitResult<unknown>> {
		let spec!: ChildTaskSpec;
		let json: Json = null;
		let timeoutMs: number | null = null;
		return this.start(
			"trigger_wait",
			name,
			() => {
				spec = this.deps.waits.resolveTask(task);
				json = this.childPayload(spec.id, payload);
				if (opts?.timeout !== undefined) timeoutMs = this.parseDurationArg(opts.timeout);
			},
			async reg => {
				if (reg.saved === undefined) {
					await this.createChildren(reg, "wait", spec, [json], timeoutMs, 0);
					throw this.suspend();
				}
				const [result] = await this.childResults(reg.name, 1);
				if (result === undefined) throw new Error(`step "${reg.name}": child result missing`);
				return result;
			}
		);
	}

	/**
	 * `ctx.step.batchTriggerAndWait(name, task, payloads, { timeout })`: one `batch` waitpoint
	 * with `pending_count`, one child per payload (`batch_index`, key
	 * `__child:${runId}:${name}:${i}`); the parent is resumed once, when the count reaches 0.
	 * Results come back in payload order. An empty `payloads` records nothing and returns `[]`.
	 */
	batchTriggerAndWait(
		name: string,
		task: AnyTask,
		payloads: readonly unknown[],
		opts: { timeout?: Duration } | undefined
	): Promise<WaitResult<unknown>[]> {
		let spec!: ChildTaskSpec;
		let jsons: Json[] = [];
		let timeoutMs: number | null = null;
		return this.start(
			"batch_wait",
			name,
			() => {
				if (!Array.isArray(payloads)) {
					throw this.fatal(
						new ConfigError(
							`ctx.step.batchTriggerAndWait("${name}"): payloads must be an array, ` +
								`got ${typeof payloads}`
						)
					);
				}
				spec = this.deps.waits.resolveTask(task);
				jsons = payloads.map(p => this.childPayload(spec.id, p));
				if (opts?.timeout !== undefined) timeoutMs = this.parseDurationArg(opts.timeout);
			},
			async reg => {
				if (jsons.length === 0) return [];
				if (reg.saved === undefined) {
					await this.createChildren(reg, "batch", spec, jsons, timeoutMs, 0);
					throw this.suspend();
				}
				return this.childResults(reg.name, jsons.length);
			}
		);
	}

	/** Validates a child payload like `engine.trigger` does (`undefined` -> `null`). */
	private childPayload(taskId: string, payload: unknown): Json {
		const value = payload === undefined ? null : payload;
		assertSerializable(value, {
			label: `payload of task "${taskId}"`,
			maxBytes: this.deps.waits.maxPayloadBytes,
		});
		return value as Json;
	}

	/** `triggerChildren` (one transaction), then the enqueues and the expire job after commit. */
	private async createChildren(
		reg: StepRegistration,
		mode: TriggerChildrenInput["mode"],
		spec: ChildTaskSpec,
		payloads: readonly Json[],
		timeoutMs: number | null,
		delayMs: number
	): Promise<EnqueueRequest[]> {
		const baseKey = `__child:${this.state.runId}:${reg.name}`;
		const result = await this.deps.waits.runs.triggerChildren({
			parent: this.state.owner,
			stepName: reg.name,
			seqIndex: reg.seqIndex,
			mode,
			waitpointId: randomUUID(),
			timeoutMs,
			delayMs,
			children: payloads.map((payload, i) => ({
				id: randomUUID(),
				taskId: spec.id,
				queue: spec.queue,
				payload,
				maxAttempts: spec.maxAttempts,
				idempotencyKey: mode === "batch" ? `${baseKey}:${i}` : baseKey,
				batchIndex: mode === "batch" ? i : null,
			})),
		});
		if (result.kind === "lost") throw this.lost(`triggering the children of step "${reg.name}"`);
		for (const request of result.enqueue) await this.deps.waits.enqueueRun(request);
		const wp = result.waitpoint;
		if (wp !== null && wp.expireDelayMs !== null) {
			await this.deps.waits.scheduleExpire(wp.id, wp.expireDelayMs);
		}
		return result.enqueue;
	}

	/**
	 * Replay of a child/batch wait: pending at load time -> suspend; otherwise ONE query by
	 * `parent_waitpoint_id ORDER BY batch_index` (decision 10). In a timed-out batch, only the
	 * children canceled BY the expiry (`error.message === WAITPOINT_TIMEOUT_REASON`) become
	 * `Timeout`; children that completed, failed or were canceled on their own before the timeout
	 * keep their real result.
	 */
	private async childResults(stepName: string, count: number): Promise<WaitResult<unknown>[]> {
		const wp = this.waitpointsByStep.get(stepName);
		if (wp === undefined) {
			throw new MinitriggerError(
				`run ${this.state.runId}: step "${stepName}" has no waitpoint (corrupted state)`
			);
		}
		if (wp.status === "pending") throw this.suspend();
		const timedOutWait = wp.status === "timed_out";
		if (timedOutWait && wp.kind === "child_run") return [timedOut(`step "${stepName}"`)];
		const children = await this.deps.waits.runs.listChildResults(wp.id);
		if (children.length !== count) {
			throw this.fatal(
				new NonDeterminismError(
					`non-deterministic replay of task "${this.state.taskId}" (run ${this.state.runId}): ` +
						`step "${stepName}" has ${count} payload(s) now, but ${children.length} child run(s) ` +
						"were triggered in an earlier execution"
				)
			);
		}
		return children.map(child =>
			timedOutWait && canceledByTimeout(child) ? timedOut(`step "${stepName}"`) : childResult(child)
		);
	}
}

function noop(): void {}

/**
 * A rejected promise already marked handled: a `ctx.step.*` call that fails before starting
 * (invalid name/arguments, execution suspended/aborted/finished) may never be awaited (e.g.
 * `void ctx.step.trigger(...)` in a loop that keeps going after a cancel). Without a handler,
 * Node's default `unhandledRejection` would crash the user's process. Awaiters still see the
 * rejection; the error itself is already recorded (`fatalError`, suspension or abort).
 */
function rejected<T>(error: unknown): Promise<T> {
	const promise = Promise.reject(error as Error);
	promise.catch(noop);
	return promise;
}

/** `{ id }` output of a `token`/`trigger` step. */
function idOf(output: Json | null, stepName: string): string {
	const id = (output as { id?: unknown } | null)?.id;
	if (typeof id !== "string") {
		throw new MinitriggerError(`step "${stepName}" has no saved id (corrupted state)`);
	}
	return id;
}

/** A child canceled by its parent's waitpoint expiry (not failed/canceled on its own before). */
function canceledByTimeout(child: ChildResult): boolean {
	return child.status === "canceled" && child.error?.message === WAITPOINT_TIMEOUT_REASON;
}

function timedOut(what: string): { ok: false; error: SerializedError } {
	return {
		ok: false,
		error: { name: TIMEOUT_ERROR_NAME, message: `${what} timed out`, retryable: false },
	};
}

/** A terminal child run as a `WaitResult` (decision 10). */
function childResult(child: ChildResult): WaitResult<unknown> {
	switch (child.status) {
		case "completed":
			return { ok: true, output: child.output };
		case "failed":
			return {
				ok: false,
				error: child.error ?? { name: "Error", message: "child run failed", retryable: false },
			};
		case "canceled":
			return {
				ok: false,
				error: {
					name: CANCELED_ERROR_NAME,
					message: child.error?.message ?? "child run was canceled",
					retryable: false,
				},
			};
		default:
			// Unreachable: a resolved waitpoint means every child is terminal.
			return {
				ok: false,
				error: {
					name: "Error",
					message: `child run ${child.runId} is ${child.status}`,
					retryable: false,
				},
			};
	}
}

/** Builds `ctx.step` for one execution from its snapshot. */
export function createStepContext(
	state: ExecutionState,
	deps: StepContextDeps,
	snapshot: ExecutionSnapshot
): StepContext {
	return new StepContext(state, deps, snapshot);
}
