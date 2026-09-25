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
import type { EnqueueRequest, Store, WaitpointResolution } from "./store/types.js";
import { formatError } from "./utils/error-format.js";

export interface ReconcilerOptions {
	store: Store;
	transport: Transport;
	logger: EngineLogger;
	/** How long a `queued`/`waiting` run is allowed to sit past its `run_after` before case 1/2 act. */
	graceMs: number;
	/** A `running` run whose `heartbeat_at` is older than this is considered stalled (case 4). */
	stallTimeoutMs: number;
	/** Max rows fetched/processed per case, per {@link Reconciler.runOnce} call. */
	batchSize: number;
	/** Phase 8 hook: bidirectional reconciliation of schedules. Called last, isolated like the rest. */
	reconcileSchedules?: () => Promise<void>;
}

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
	const { store, transport, logger, graceMs, stallTimeoutMs, batchSize, reconcileSchedules } =
		options;

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
		const items = await store.runs.listStaleQueued(graceMs, batchSize);
		await forEachItem(items, "staleQueued", report, async item => {
			await transport.enqueueRun(toEnqueueInput(item));
			report.staleQueued++;
		});
	}

	// --- Case 2: waiting, no pending waitpoint, run_after < now() - grace -> re-enqueue, SAME seq. ---
	async function reconcileStaleWaiting(report: ReconcileReport): Promise<void> {
		const items = await store.runs.listStaleWaiting(graceMs, batchSize);
		await forEachItem(items, "staleWaiting", report, async item => {
			await transport.enqueueRun(toEnqueueInput(item));
			report.staleWaiting++;
		});
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
				await transport.enqueueRun(toEnqueueInput(result.enqueue));
				report.stalled.requeued++;
				return;
			}
			report.stalled.failed++;
			await applyResolution(result.parent);
		});
	}

	// --- Case 5: pending waitpoints past expires_at -> expire -> enqueue resume, remove expire jobs. ---
	async function reconcileExpiredWaitpoints(report: ReconcileReport): Promise<void> {
		const ids = await store.waitpoints.listExpired(batchSize);
		await forEachItem(ids, "expiredWaitpoints", report, async waitpointId => {
			const result = await store.waitpoints.expire(waitpointId);
			if (result.kind === "noop" || result.kind === "not_due") return; // No-op, per spec.
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
