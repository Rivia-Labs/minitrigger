/**
 * Shared internal operations (mirror of `store/sql/resolve.ts`): run on an open {@link MemTx},
 * synchronously, so each store method is atomic. See docs/store-contract.md for the rules.
 */
import type {
	CanceledRun,
	EnqueueRequest,
	RunRecord,
	RunStatus,
	SerializedError,
	WaitpointResolution,
} from "../types.js";
import { CANCELED_ERROR_NAME } from "../types.js";
import {
	cloneJson,
	delayMs,
	type MemTx,
	type RunRow,
	type StepRow,
	toDate,
	type WaitpointRow,
} from "./state.js";

/** How a repository runs one atomic operation (pool: own transaction; tx: the caller's). */
export interface Exec {
	readonly kind: "pool" | "tx";
	run<T>(fn: (tx: MemTx) => T): Promise<T>;
}

export function isTerminal(status: RunStatus): boolean {
	return status === "completed" || status === "failed" || status === "canceled";
}

export function canceledError(reason: string): SerializedError {
	return { name: CANCELED_ERROR_NAME, message: reason, retryable: false };
}

export function mapRun(row: RunRow): RunRecord {
	return {
		id: row.id,
		taskId: row.taskId,
		queue: row.queue,
		payload: cloneJson(row.payload),
		status: row.status,
		output: cloneJson(row.output),
		error: cloneJson(row.error),
		attempt: row.attempt,
		maxAttempts: row.maxAttempts,
		executionId: row.executionId,
		enqueueSeq: row.enqueueSeq,
		heartbeatAt: toDate(row.heartbeatAt),
		runAfter: toDate(row.runAfter),
		parentRunId: row.parentRunId,
		parentWaitpointId: row.parentWaitpointId,
		parentStepName: row.parentStepName,
		batchIndex: row.batchIndex,
		startedAt: toDate(row.startedAt),
		finishedAt: toDate(row.finishedAt),
		createdAt: new Date(row.createdAt),
		updatedAt: new Date(row.updatedAt),
	};
}

export function enqueueOf(row: RunRow, now: number): EnqueueRequest {
	return {
		runId: row.id,
		taskId: row.taskId,
		queue: row.queue,
		seq: row.enqueueSeq,
		delayMs: row.runAfter === null ? 0 : delayMs(row.runAfter, now),
		status: row.status,
		updatedAt: toDate(row.updatedAt),
	};
}

/** Replaces a run row with `patch` applied; returns the new row. */
export function updateRun(tx: MemTx, row: RunRow, patch: Partial<RunRow>): RunRow {
	const next = { ...row, ...patch };
	tx.set(tx.db.runs, row.id, next);
	return next;
}

/** The ownership guard (decision 5): the run row if `execution_id` and `running` match. */
export function owned(tx: MemTx, runId: string, executionId: string): RunRow | null {
	const row = tx.db.runs.get(runId);
	return row && row.status === "running" && row.executionId === executionId ? row : null;
}

export function stepsOf(tx: MemTx, runId: string): StepRow[] {
	const inner = tx.db.steps.get(runId);
	return inner ? [...inner.values()] : [];
}

export function waitpointsOf(tx: MemTx, runId: string): WaitpointRow[] {
	const out: WaitpointRow[] = [];
	for (const wp of tx.db.waitpoints.values()) if (wp.runId === runId) out.push(wp);
	return out;
}

/** `max(wake_at)` of the run's waiting sleep steps, or null. */
export function maxWakeAt(tx: MemTx, runId: string): number | null {
	let max: number | null = null;
	for (const s of stepsOf(tx, runId)) {
		if (s.status === "waiting" && s.wakeAt !== null && (max === null || s.wakeAt > max)) {
			max = s.wakeAt;
		}
	}
	return max;
}

/**
 * `blockingWaitpointExistsSql`: a pending waitpoint of the run that is a `child_run`/`batch`, or a
 * `token` whose `${step_name}#wait` step (kind `wait`) exists (owner's decision 3).
 */
export function hasBlockingWaitpoint(tx: MemTx, runId: string): boolean {
	const steps = tx.db.steps.get(runId);
	for (const wp of tx.db.waitpoints.values()) {
		if (wp.runId !== runId || wp.status !== "pending") continue;
		if (wp.kind !== "token") return true;
		if (steps?.get(`${wp.stepName}#wait`)?.kind === "wait") return true;
	}
	return false;
}

/** Resume rule (decision 3), with the run "locked". */
export function applyResumeRule(tx: MemTx, runId: string): EnqueueRequest | null {
	const row = tx.db.runs.get(runId);
	if (row?.status !== "waiting" || hasBlockingWaitpoint(tx, runId)) return null;
	const wake = maxWakeAt(tx, runId);
	const next = updateRun(tx, row, {
		enqueueSeq: row.enqueueSeq + 1,
		runAfter: wake === null ? tx.now : Math.max(wake, tx.now),
		updatedAt: tx.now,
	});
	return enqueueOf(next, tx.now);
}

/** Child resolution (decision 10) of `waitpointId` after one of its children became terminal. */
export function resolveChildWaitpoint(tx: MemTx, waitpointId: string): WaitpointResolution | null {
	const wp = tx.db.waitpoints.get(waitpointId);
	if (wp?.status !== "pending" || (wp.kind !== "child_run" && wp.kind !== "batch")) {
		return null;
	}
	if (wp.kind === "batch") {
		const left = (wp.pendingCount ?? 0) - 1;
		if (left > 0) {
			tx.set(tx.db.waitpoints, wp.id, { ...wp, pendingCount: left, resolvedAt: null });
			return { waitpointId, hasExpireJob: false, resume: null };
		}
		tx.set(tx.db.waitpoints, wp.id, {
			...wp,
			pendingCount: left,
			status: "completed",
			resolvedAt: tx.now,
		});
	} else {
		tx.set(tx.db.waitpoints, wp.id, { ...wp, status: "completed", resolvedAt: tx.now });
	}
	const resume = applyResumeRule(tx, wp.runId);
	return { waitpointId, hasExpireJob: wp.expiresAt !== null, resume };
}

/** Sets the given non-terminal runs to `canceled`. */
export function markCanceled(tx: MemTx, ids: readonly string[], reason: string): void {
	const error = canceledError(reason);
	for (const id of ids) {
		const row = tx.db.runs.get(id);
		if (!row || isTerminal(row.status)) continue;
		updateRun(tx, row, {
			status: "canceled",
			executionId: null,
			runAfter: null,
			finishedAt: tx.now,
			error,
			updatedAt: tx.now,
		});
	}
}

function byId(a: RunRow, b: RunRow): number {
	return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

function childLevel(tx: MemTx, parentIds: ReadonlySet<string>): RunRow[] {
	if (parentIds.size === 0) return [];
	const out: RunRow[] = [];
	for (const r of tx.db.runs.values()) {
		if (r.parentRunId !== null && parentIds.has(r.parentRunId)) out.push(r);
	}
	return out.sort(byId);
}

/**
 * Cancels descendants level by level (ascending id per level), walking through terminal nodes.
 * The first level is the children of `parentRunIds` or those attached to `waitpointId`. No parent
 * waitpoint resolution for these runs.
 */
export function cancelDescendants(
	tx: MemTx,
	start: { parentRunIds: readonly string[] } | { waitpointId: string },
	reason: string
): CanceledRun[] {
	const canceled: CanceledRun[] = [];
	const visited = new Set<string>();
	let level: RunRow[];
	if ("waitpointId" in start) {
		level = [...tx.db.runs.values()]
			.filter(r => r.parentWaitpointId === start.waitpointId)
			.sort(byId);
	} else {
		for (const id of start.parentRunIds) visited.add(id);
		level = childLevel(tx, new Set(start.parentRunIds));
	}
	while (level.length > 0) {
		const fresh = level.filter(r => !visited.has(r.id));
		for (const r of fresh) visited.add(r.id);
		const toCancel = fresh.filter(r => !isTerminal(r.status));
		markCanceled(
			tx,
			toCancel.map(r => r.id),
			reason
		);
		for (const r of toCancel) {
			canceled.push({
				runId: r.id,
				queue: r.queue,
				enqueueSeq: r.enqueueSeq,
				previousStatus: r.status as CanceledRun["previousStatus"],
				updatedAt: toDate(tx.now),
			});
		}
		level = childLevel(tx, new Set(fresh.map(r => r.id)));
	}
	return canceled;
}

/**
 * Every pending waitpoint owned by `runIds` becomes `timed_out`; returns the (sorted) ids of the
 * ones with `expires_at`, whose `wp-${id}-expire` jobs the caller removes.
 */
export function timeOutWaitpointsOf(tx: MemTx, runIds: readonly string[]): string[] {
	if (runIds.length === 0) return [];
	const ids = new Set(runIds);
	const withExpiry: string[] = [];
	for (const wp of [...tx.db.waitpoints.values()]) {
		if (!ids.has(wp.runId) || wp.status !== "pending") continue;
		tx.set(tx.db.waitpoints, wp.id, { ...wp, status: "timed_out", resolvedAt: tx.now });
		if (wp.expiresAt !== null) withExpiry.push(wp.id);
	}
	return withExpiry.sort();
}
