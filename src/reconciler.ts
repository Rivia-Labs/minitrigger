/**
 * Reconciler (Phase 6, decisions 1, 3, 6, 7, 10, 17; `docs/SPEC.md` Phase 6). The safety net that
 * makes a lost `trigger()` enqueue, a redelivery that got rejected by `acquire`'s `enqueue_seq`
 * guard, a heartbeat gap, or an expired waitpoint eventually converge, entirely from what
 * Postgres already knows -- never from `Date.now()` (decision 17): every "is this stale/due"
 * decision is made by the store's SQL against the database clock, this module only reacts to
 * what the store returns.
 *
 * Each of the five cases below is independent and isolated: a case (or one item within it)
 * throwing is logged and does not stop the other cases, or the other items of the same case, from
 * running (`runOnce()` always resolves, it never rejects for an expected/isolated failure).
 *
 * `reconcileSchedules` is a hook only -- Phase 8 fills it in; this module doesn't know anything
 * about schedules.
 */

import type { EngineLogger } from "./config.js";
import { waitpointExpireJobId } from "./queue/ids.js";
import type { EnqueueRunInput, Transport } from "./queue/transport.js";
import type {
	CanceledRun,
	EnqueueRequest,
	RunStatus,
	SerializedError,
	StaleCursor,
	StalePage,
	Store,
	WaitpointResolution,
} from "./store/types.js";
import { formatError } from "./utils/error-format.js";

/**
 * What case 4 (stall recovery) hands `onStatusChange`: everything the caller needs to notify
 * without a `runs.get` (Phase 7 perf). `status` is `"queued"` (requeued, attempt incremented) or
 * `"failed"` (attempts exhausted); the engine derives the transition (`retrying`/`failed`) from it.
 */
export interface StalledStatusChange {
	runId: string;
	taskId: string;
	attempt: number;
	status: Extract<RunStatus, "queued" | "failed">;
	error: SerializedError;
	updatedAt: Date;
}

export interface ReconcilerOptions {
	store: Store;
	transport: Transport;
	logger: EngineLogger;
	/** How long a `queued`/`waiting` run is allowed to sit past its `run_after` before case 1/2 act. */
	graceMs: number;
	/** A `running` run whose `heartbeat_at` is older than this is considered stalled (case 4). */
	stallTimeoutMs: number;
	/** Max rows fetched per query (page size); cases 1/2 may fetch several pages per tick. */
	batchSize: number;
	/**
	 * Cases 1/2 (stale queued/waiting): max pages of `batchSize` walked per {@link
	 * Reconciler.runOnce} (default {@link DEFAULT_MAX_PAGES_PER_TICK}). Those rows stay stale after
	 * a re-enqueue (the job may legitimately exist: rate-limited backlog, queue without a worker),
	 * so each case keeps a `(run_after, id)` cursor across ticks and wraps around after a short
	 * page: a lost enqueue behind any number of them is reached within
	 * `ceil(stale / (batchSize * maxPagesPerTick))` ticks.
	 */
	maxPagesPerTick?: number;
	/** Phase 8 hook: bidirectional reconciliation of schedules. Called last, isolated like the rest. */
	reconcileSchedules?: () => Promise<void>;
	/**
	 * Called with the runs canceled by an expiry (case 5), after their pending run jobs were
	 * removed. The engine aborts local executions and publishes the cancel (Phase 6). A throw is
	 * logged and counted in `errors`; the parent's resume still happens.
	 */
	onCanceled?: (canceled: readonly CanceledRun[]) => void | Promise<void>;
	/**
	 * Called (synchronously, never awaited) after the reconciler changed a run's status (case 4:
	 * stalled run requeued or failed). Must not throw; the engine publishes the status (best-effort).
	 */
	onStatusChange?: (info: StalledStatusChange) => void;
}

/** Default {@link ReconcilerOptions.maxPagesPerTick}. */
export const DEFAULT_MAX_PAGES_PER_TICK = 10;

export interface StalledReport {
	/** Recovered with attempts remaining: requeued (`queued`, `attempt + 1`). */
	requeued: number;
	/** Attempts exhausted: failed definitively. */
	failed: number;
}

/** Per-case counts of items actually acted on (skips like `not_stalled`/`noop`/`not_due` don't count). */
export interface ReconcileReport {
	staleQueued: number;
	staleWaiting: number;
	orphanWaiting: number;
	stalled: StalledReport;
	expiredWaitpoints: number;
	/** Number of case-level or per-item failures encountered (logged, never thrown). */
	errors: number;
}

export interface Reconciler {
	runOnce(): Promise<ReconcileReport>;
}

export function createReconciler(options: ReconcilerOptions): Reconciler {
	const {
		store,
		transport,
		logger,
		graceMs,
		stallTimeoutMs,
		batchSize,
		reconcileSchedules,
		onCanceled,
		onStatusChange,
	} = options;
	const maxPagesPerTick = Math.max(1, options.maxPagesPerTick ?? DEFAULT_MAX_PAGES_PER_TICK);
	/** Per-case keyset cursors, kept across ticks (per reconciler instance; no global state). */
	const cursors: { staleQueued: StaleCursor | null; staleWaiting: StaleCursor | null } = {
		staleQueued: null,
		staleWaiting: null,
	};

	/**
	 * Walks up to `maxPagesPerTick` pages of a stale listing from the case's saved cursor; a short
	 * page resets the cursor so the next tick starts from the oldest again (wrap-around).
	 */
	async function forEachStalePage(
		caseName: "staleQueued" | "staleWaiting",
		list: (after: StaleCursor | null) => Promise<StalePage>,
		fn: (items: readonly EnqueueRequest[]) => Promise<void>
	): Promise<void> {
		for (let page = 0; page < maxPagesPerTick; page++) {
			const { items, nextCursor } = await list(cursors[caseName]);
			cursors[caseName] = nextCursor;
			await fn(items);
			if (!nextCursor) return;
		}
	}

	function toEnqueueInput(req: EnqueueRequest): EnqueueRunInput {
		return { queue: req.queue, runId: req.runId, seq: req.seq, delayMs: req.delayMs };
	}

	/** Enqueues `resolution.resume` (if any) and removes its own expire job (if it had one). */
	async function applyResolution(resolution: WaitpointResolution | null): Promise<void> {
		if (!resolution) return;
		if (resolution.resume) {
			await transport.enqueueRun(toEnqueueInput(resolution.resume));
		}
		if (resolution.hasExpireJob) {
			await transport.removeSystemJob(waitpointExpireJobId(resolution.waitpointId));
		}
	}

	/** Runs `fn` for every item, isolating a single item's failure from the rest of the case. */
	async function forEachItem<T>(
		items: readonly T[],
		caseName: string,
		report: ReconcileReport,
		fn: (item: T) => Promise<void>
	): Promise<void> {
		for (const item of items) {
			try {
				await fn(item);
			} catch (err) {
				report.errors++;
				logger.error(
					{ err, case: caseName, item },
					`reconciler: item failed in case "${caseName}"`
				);
			}
		}
	}

	/** Runs one whole case, isolating it from the others: a throw here never stops `runOnce`. */
	async function runCase(
		caseName: string,
		report: ReconcileReport,
		fn: () => Promise<void>
	): Promise<void> {
		try {
			await fn();
		} catch (err) {
			report.errors++;
			logger.error({ err, case: caseName }, `reconciler: case "${caseName}" failed`);
		}
	}

	// --- Case 1: queued, run_after < now() - grace -> re-enqueue with the CURRENT seq. ---
	async function reconcileStaleQueued(report: ReconcileReport): Promise<void> {
		await forEachStalePage(
			"staleQueued",
			after => store.runs.listStaleQueued(graceMs, batchSize, after),
			items =>
				forEachItem(items, "staleQueued", report, async item => {
					await transport.enqueueRun(toEnqueueInput(item));
					report.staleQueued++;
				})
		);
	}

	// --- Case 2: waiting, no pending waitpoint, run_after < now() - grace -> re-enqueue, SAME seq. ---
	async function reconcileStaleWaiting(report: ReconcileReport): Promise<void> {
		await forEachStalePage(
			"staleWaiting",
			after => store.runs.listStaleWaiting(graceMs, batchSize, after),
			items =>
				forEachItem(items, "staleWaiting", report, async item => {
					await transport.enqueueRun(toEnqueueInput(item));
					report.staleWaiting++;
				})
		);
	}

	// --- Case 3: waiting, run_after IS NULL, no pending waitpoint -> repair (store) + enqueue + warn. ---
	async function reconcileOrphanWaiting(report: ReconcileReport): Promise<void> {
		const items = await store.runs.repairOrphanWaiting(batchSize);
		await forEachItem(items, "orphanWaiting", report, async item => {
			await transport.enqueueRun(toEnqueueInput(item));
			logger.warn(
				{ runId: item.runId },
				"reconciler: repaired a waiting run with run_after=NULL and no pending waitpoint " +
					"(this indicates a bug: some suspension/resolution path left the run without a " +
					"way to wake up)"
			);
			report.orphanWaiting++;
		});
	}

	// --- Case 4: running, heartbeat_at < now() - stallTimeoutMs -> recoverStalled -> enqueue. ---
	async function reconcileStalled(report: ReconcileReport): Promise<void> {
		const ids = await store.runs.listStalled(stallTimeoutMs, batchSize);
		const error = formatError(
			new Error(
				`run stalled: no heartbeat for more than ${stallTimeoutMs}ms; recovered by the reconciler`
			),
			true
		);
		await forEachItem(ids, "stalled", report, async runId => {
			const result = await store.runs.recoverStalled(runId, stallTimeoutMs, error);
			if (result.kind === "not_stalled") return; // Recovered/changed concurrently; not an error.
			if (result.kind === "requeued") {
				onStatusChange?.({
					runId,
					taskId: result.enqueue.taskId,
					attempt: result.attempt,
					status: "queued",
					error,
					updatedAt: result.enqueue.updatedAt,
				});
				await transport.enqueueRun(toEnqueueInput(result.enqueue));
				report.stalled.requeued++;
				return;
			}
			onStatusChange?.({
				runId,
				taskId: result.taskId,
				attempt: result.attempt,
				status: "failed",
				error,
				updatedAt: result.updatedAt,
			});
			report.stalled.failed++;
			await applyResolution(result.parent);
			for (const id of result.expireJobsToRemove) {
				await transport.removeSystemJob(waitpointExpireJobId(id));
			}
		});
	}

	// --- Case 5: pending waitpoints past expires_at -> expire -> cancel children's jobs, enqueue
	// resume, remove expire jobs. ---
	async function reconcileExpiredWaitpoints(report: ReconcileReport): Promise<void> {
		const ids = await store.waitpoints.listExpired(batchSize);
		await forEachItem(ids, "expiredWaitpoints", report, async waitpointId => {
			const result = await store.waitpoints.expire(waitpointId);
			if (result.kind === "noop" || result.kind === "not_due") return; // No-op, per spec.
			// Children canceled by the expiry (decision 10): drop their pending run jobs
			// (best-effort; a leftover job is a no-op anyway, `acquire` rejects a canceled run).
			for (const child of result.canceledChildren) {
				try {
					await transport.removeRunJob(child.queue, child.runId, child.enqueueSeq);
				} catch (err) {
					logger.warn(
						{ err, runId: child.runId },
						"reconciler: failed to remove the run job of a canceled child"
					);
				}
			}
			if (result.canceledChildren.length > 0 && onCanceled) {
				try {
					await onCanceled(result.canceledChildren);
				} catch (err) {
					// Isolated: the parent's resume below must still happen.
					report.errors++;
					logger.error({ err, waitpointId }, "reconciler: onCanceled hook failed");
				}
			}
			await applyResolution(result.resolution);
			// This waitpoint's own expire job already fired (that's how it got here); best-effort
			// cleanup in case a duplicate/late one is still scheduled (decision 10: aggressive removal).
			await transport.removeSystemJob(waitpointExpireJobId(waitpointId));
			for (const id of result.expireJobsToRemove) {
				await transport.removeSystemJob(waitpointExpireJobId(id));
			}
			report.expiredWaitpoints++;
		});
	}

	async function runOnce(): Promise<ReconcileReport> {
		const report: ReconcileReport = {
			staleQueued: 0,
			staleWaiting: 0,
			orphanWaiting: 0,
			stalled: { requeued: 0, failed: 0 },
			expiredWaitpoints: 0,
			errors: 0,
		};

		await runCase("staleQueued", report, () => reconcileStaleQueued(report));
		await runCase("staleWaiting", report, () => reconcileStaleWaiting(report));
		await runCase("orphanWaiting", report, () => reconcileOrphanWaiting(report));
		await runCase("stalled", report, () => reconcileStalled(report));
		await runCase("expiredWaitpoints", report, () => reconcileExpiredWaitpoints(report));
		if (reconcileSchedules) {
			await runCase("schedules", report, reconcileSchedules);
		}

		return report;
	}

	return { runOnce };
}
