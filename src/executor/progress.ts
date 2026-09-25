/**
 * `ctx.progress.set` (decision 23), mirroring `src/logs/buffer.ts`'s buffering/serialization
 * shape, but there is only ever one pending value instead of a growing queue of log entries:
 *
 * - keeps only the LATEST value handed to {@link RunProgress.set}: an older one still pending
 *   when a newer one arrives is simply replaced, never queued;
 * - writes at most once every `intervalMs` (default 250): the first `set` after an idle period
 *   writes promptly ("leading"), and every `set` that arrives before the next write is due is
 *   coalesced into a single write once the window elapses ("trailing") -- so a tight loop of
 *   `ctx.progress.set` calls (e.g. one per processed item) costs a handful of `UPDATE`s, not one
 *   per call;
 * - writes are strictly serialized on a promise chain (like `logs/buffer.ts`'s `flushChain`): the
 *   store never sees two concurrent `setProgress` calls for the same run, and a `close()` waiting
 *   on that chain always waits for exactly the write already in flight, if any;
 * - on a write failure (thrown error), retries the SAME value once; if the retry also fails too,
 *   only THAT value is dropped (reported via `config.logger.warn`) -- the writer keeps running for
 *   whatever `set()` comes next, since a transient store hiccup must not silently disable progress
 *   for the rest of the execution (the README's/decision 23's "last value set is never lost" is
 *   about surviving retries/resumes, not weathering an outage forever). Only the store reporting
 *   the ownership guard failed (`"lost"`) -- or {@link RunProgressContext.ownershipGone} already
 *   true -- stops the writer for good, reported once via `config.logger.warn` (the same
 *   `ownershipGone`-driven "drop with a warning" spirit as `executor/logger.ts`'s run logger,
 *   Phase 7's precedent for this executor/store seam);
 * - `close()` flushes a pending value (the last `set` before the run completes/fails/suspends
 *   must never be lost), then disables further writes. Idempotent, like `logs/buffer.ts#close`.
 * - a successful write also notifies `RunNotifier.progress` (best-effort pub/sub), matching how
 *   `logs/buffer.ts` never blocks its own flush loop on the mirror sink.
 *
 * `ctx.progress.set`'s own synchronous validation (`Serializable`, `maxProgressBytes`) is NOT
 * here: it happens once, before `set()` is even called, in `execute-run.ts`'s `ctx` construction
 * (mirroring how `defineTask`'s output validation lives outside `logs/buffer.ts` too) -- this
 * module only ever receives values already known to be valid `Json`.
 */

import type { ResolvedConfig } from "../config.js";
import type { RunStore, SetProgressResult } from "../store/types.js";
import type { Json } from "../utils/serialization.js";
import type { RunNotifier } from "./notify.js";

export interface RunProgressContext {
	runId: string;
	executionId: string;
	taskId: string;
	/**
	 * True once this execution no longer owns the run (lost, canceled, handed back on shutdown):
	 * checked before every write attempt, mirroring `RunLoggerContext.ownershipGone` -- a write
	 * attempted anyway would just come back `"lost"` from the store, so this only saves the
	 * round-trip and lets the "dropped" warning fire without waiting for it.
	 */
	ownershipGone: () => boolean;
}

/** `ctx.progress`'s internal half; the public `TaskContext.progress.set` wraps {@link set}. */
export interface RunProgress {
	/** Records `value` as the latest pending one and (re)schedules the coalesced write. Never throws. */
	set(value: Json): void;
	/**
	 * Waits for a write already in flight, then performs one final write if a value arrived after
	 * it started (or if nothing was ever written yet). Idempotent; never throws.
	 */
	close(): Promise<void>;
}

const DEFAULT_INTERVAL_MS = 250;

export function createRunProgress(
	deps: {
		runs: Pick<RunStore, "setProgress">;
		// `heartbeatTimeoutMs` bounds each `setProgress` write (like `heartbeat`'s own `SET LOCAL
		// statement_timeout`), so a write blocked behind another transaction's lock on the run row
		// can't hang `close()` (awaited after the heartbeat already stopped) indefinitely.
		config: Pick<ResolvedConfig, "logger" | "heartbeatTimeoutMs">;
		/** Best-effort pub/sub of a successful write (Phase progress work); omit to skip it. */
		notify?: Pick<RunNotifier, "progress">;
	},
	ctx: RunProgressContext,
	options: { intervalMs?: number } = {}
): RunProgress {
	const intervalMs = options.intervalMs ?? DEFAULT_INTERVAL_MS;
	const log = deps.config.logger;
	const fields = { runId: ctx.runId, taskId: ctx.taskId };

	let pending: { value: Json } | undefined;
	let writing = false;
	let timer: ReturnType<typeof setTimeout> | undefined;
	let lastWriteStartedAt = -Infinity;
	let closed = false;
	let stopped = false;
	let droppedWarned = false;
	// Serializes writes: `close()` awaits exactly the write already in flight (if any).
	let writeChain: Promise<void> = Promise.resolve();

	function warnDroppedOnce(reason: string, error?: unknown): void {
		if (droppedWarned) return;
		droppedWarned = true;
		try {
			log.warn(
				{ ...fields, ...(error !== undefined ? { err: error } : {}) },
				`run progress updates dropped: ${reason}`
			);
		} catch {
			// A broken engine logger must never fail a run.
		}
	}

	function clearTimer(): void {
		if (timer !== undefined) {
			clearTimeout(timer);
			timer = undefined;
		}
	}

	function kick(): void {
		if (closed || stopped || writing || timer !== undefined || pending === undefined) return;
		if (ctx.ownershipGone()) {
			pending = undefined;
			stopped = true;
			warnDroppedOnce("this execution no longer owns the run");
			return;
		}
		const elapsed = Date.now() - lastWriteStartedAt;
		if (elapsed >= intervalMs) {
			startWrite();
		} else {
			timer = setTimeout(() => {
				timer = undefined;
				startWrite();
			}, intervalMs - elapsed);
			timer.unref?.();
		}
	}

	function startWrite(): void {
		if (stopped || pending === undefined) return;
		if (ctx.ownershipGone()) {
			pending = undefined;
			stopped = true;
			warnDroppedOnce("this execution no longer owns the run");
			return;
		}
		const value = pending.value;
		pending = undefined;
		writing = true;
		lastWriteStartedAt = Date.now();
		writeChain = writeChain
			.then(() => doWrite(value))
			.finally(() => {
				writing = false;
				kick();
			});
	}

	async function doWrite(value: Json): Promise<void> {
		let result: SetProgressResult;
		try {
			result = await deps.runs.setProgress(
				ctx.runId,
				ctx.executionId,
				value,
				deps.config.heartbeatTimeoutMs
			);
		} catch {
			try {
				result = await deps.runs.setProgress(
					ctx.runId,
					ctx.executionId,
					value,
					deps.config.heartbeatTimeoutMs
				);
			} catch (retryError) {
				// Transient: drop only THIS value and warn, but keep the writer running -- a store
				// hiccup must not disable progress for the rest of the execution (unlike `"lost"`
				// below, this isn't an ownership problem, so there's no reason later `set()`s
				// couldn't succeed).
				try {
					log.warn(
						{ ...fields, err: retryError },
						"run progress update dropped: failed to persist (retried once)"
					);
				} catch {
					// A broken engine logger must never fail a run.
				}
				return;
			}
		}
		if (result.kind === "lost") {
			stopped = true;
			warnDroppedOnce("this execution no longer owns the run");
			return;
		}
		try {
			deps.notify?.progress(ctx.runId, result.updatedAt, value);
		} catch {
			// Best-effort notification must never break the writer.
		}
	}

	return {
		set(value) {
			if (closed || stopped) return;
			pending = { value };
			kick();
		},
		async close() {
			if (closed) return;
			closed = true;
			clearTimer();
			await writeChain;
			if (!stopped && pending !== undefined && !ctx.ownershipGone()) {
				const value = pending.value;
				pending = undefined;
				await doWrite(value);
			}
		},
	};
}
