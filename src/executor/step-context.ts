import { AsyncLocalStorage } from "node:async_hooks";
import { performance } from "node:perf_hooks";
import {
	DuplicateStepError,
	ExecutionLostError,
	MinitriggerError,
	NestedStepError,
	NonDeterminismError,
} from "../errors.js";
import type { ExecutionSnapshot, LoadedStep, StepKind, StepStore } from "../store/types.js";
import type { StepApi } from "../task.js";
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

export interface StepContextDeps {
	steps: StepStore;
	maxStepOutputBytes: number;
}

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

		const phase5 = (method: string) => (): Promise<never> =>
			Promise.reject(new MinitriggerError(`ctx.step.${method} is implemented in Phase 5`));
		this.api = {
			run: ((name: string, fn: () => unknown) => this.run(name, fn)) as StepApi["run"],
			sleep: (name, duration) => this.sleep(name, duration),
			createToken: phase5("createToken"),
			waitForToken: phase5("waitForToken"),
			trigger: phase5("trigger"),
			triggerAndWait: phase5("triggerAndWait"),
			batchTriggerAndWait: phase5("batchTriggerAndWait"),
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
			return Promise.reject(this.fatal(error as Error));
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
			return Promise.reject(error);
		}
		return this.state.trackInFlight(body(reg));
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
					throw new MinitriggerError(`ctx.step.run("${name}", fn): fn must be a function`);
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
				durationMs = parseDuration(duration);
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
}

function noop(): void {}

/** Builds `ctx.step` for one execution from its snapshot. */
export function createStepContext(
	state: ExecutionState,
	deps: StepContextDeps,
	snapshot: ExecutionSnapshot
): StepContext {
	return new StepContext(state, deps, snapshot);
}
