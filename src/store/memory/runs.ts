import type {
	CanceledRun,
	CancelRunResult,
	ChildResult,
	CreateRunResult,
	EnqueueRequest,
	Json,
	NewRun,
	Ownership,
	RecoverStalledResult,
	RunStore,
	SerializedError,
	StepKind,
	TerminalRunStatus,
	TriggerChildrenInput,
	TriggerChildrenResult,
	WaitpointResolution,
} from "../types.js";
import { MAX_LIST_LIMIT } from "../types.js";
import { claimKey } from "./idempotency.js";
import {
	applyResumeRule,
	cancelDescendants,
	canceledError,
	type Exec,
	enqueueOf,
	hasBlockingWaitpoint,
	isTerminal,
	mapRun,
	markCanceled,
	maxWakeAt,
	owned,
	resolveChildWaitpoint,
	timeOutWaitpointsOf,
	updateRun,
} from "./resolve.js";
import {
	cloneJson,
	delayMs,
	keyOf,
	type MemTx,
	type RunRow,
	sqlLimit,
	toDate,
	toJsonb,
	truncMs,
	uniqueViolation,
	uuid,
} from "./state.js";
import { insertStep } from "./steps.js";

const STEP_KIND_BY_MODE: Record<TriggerChildrenInput["mode"], StepKind> = {
	trigger: "trigger",
	wait: "trigger_wait",
	batch: "batch_wait",
};

type CreateOnce = CreateRunResult | "retry";

interface InsertRun {
	id: string;
	taskId: string;
	queue: string;
	payload: Json;
	maxAttempts: number;
	delayMs: number;
	parentRunId: string | null;
	parentWaitpointId: string | null;
	parentStepName: string | null;
	batchIndex: number | null;
}

function insertRun(tx: MemTx, input: InsertRun): RunRow {
	const id = uuid(input.id);
	if (tx.db.runs.has(id)) throw uniqueViolation("runs_pkey");
	const row: RunRow = {
		id,
		taskId: input.taskId,
		queue: input.queue,
		payload: toJsonb(input.payload),
		status: "queued",
		output: null,
		error: null,
		attempt: 1,
		maxAttempts: input.maxAttempts,
		executionId: null,
		enqueueSeq: 1,
		heartbeatAt: null,
		runAfter: tx.now + input.delayMs,
		parentRunId: input.parentRunId,
		parentWaitpointId: input.parentWaitpointId,
		parentStepName: input.parentStepName,
		batchIndex: input.batchIndex,
		startedAt: null,
		finishedAt: null,
		createdAt: truncMs(tx.now),
		updatedAt: tx.now,
	};
	tx.set(tx.db.runs, id, row);
	return row;
}

function createOnce(tx: MemTx, input: NewRun): CreateOnce {
	const mark = tx.mark();
	const row = insertRun(tx, {
		...input,
		parentRunId: null,
		parentWaitpointId: null,
		parentStepName: null,
		batchIndex: null,
	});
	if (input.idempotency) {
		const claim = claimKey(tx, {
			taskId: input.taskId,
			key: input.idempotency.key,
			runId: row.id,
			ttlMs: input.idempotency.ttlMs,
		});
		if (claim === null || !claim.claimed) {
			tx.rollbackTo(mark);
			if (claim === null) return "retry";
			const existing = tx.db.runs.get(claim.existingRunId);
			return existing ? { kind: "existing", run: mapRun(existing) } : "retry";
		}
	}
	return { kind: "created", run: mapRun(row), enqueue: enqueueOf(row, tx.now) };
}

type Finished =
	| { lost: true }
	| {
			lost: false;
			updatedAt: Date;
			parent: WaitpointResolution | null;
			expireJobsToRemove: string[];
	  };

function finish(
	tx: MemTx,
	owner: Ownership,
	status: "completed" | "failed",
	output: Json | null,
	error: SerializedError | null
): Finished {
	const row = owned(tx, uuid(owner.runId), uuid(owner.executionId));
	if (!row) return { lost: true };
	updateRun(tx, row, {
		status,
		output: status === "completed" ? toJsonb(output) : null,
		error: error === null ? null : toJsonb(error),
		finishedAt: tx.now,
		runAfter: null,
		executionId: null,
		updatedAt: tx.now,
	});
	const expireJobsToRemove = timeOutWaitpointsOf(tx, [row.id]);
	const wp = row.parentWaitpointId;
	return {
		lost: false,
		updatedAt: toDate(tx.now),
		parent: wp ? resolveChildWaitpoint(tx, wp) : null,
		expireJobsToRemove,
	};
}

export function createRunStore(exec: Exec): RunStore {
	return {
		async create(input) {
			for (let i = 0; i < 2; i++) {
				const result = await exec.run(tx => createOnce(tx, input));
				if (result !== "retry") return result;
			}
			throw new Error(`run owning idempotency key of task ${input.taskId} kept vanishing`);
		},

		async triggerChildren(input: TriggerChildrenInput): Promise<TriggerChildrenResult> {
			const n = input.children.length;
			if (input.mode === "batch" ? n < 1 : n !== 1) {
				throw new Error(`triggerChildren mode ${input.mode} got ${n} children`);
			}
			return exec.run(tx => {
				const parentId = uuid(input.parent.runId);
				if (!owned(tx, parentId, uuid(input.parent.executionId))) return { kind: "lost" };

				let waitpoint: { id: string; expiresAt: Date | null; expireDelayMs: number | null } | null =
					null;
				if (input.mode !== "trigger") {
					const id = uuid(input.waitpointId);
					if (tx.db.waitpoints.has(id)) throw uniqueViolation("waitpoints_pkey");
					for (const wp of tx.db.waitpoints.values()) {
						if (wp.runId === parentId && wp.stepName === input.stepName) {
							throw uniqueViolation("waitpoints_run_id_step_name_key");
						}
					}
					const expiresAt = input.timeoutMs === null ? null : tx.now + input.timeoutMs;
					tx.set(tx.db.waitpoints, id, {
						id,
						runId: parentId,
						stepName: input.stepName,
						kind: input.mode === "batch" ? "batch" : "child_run",
						status: "pending",
						pendingCount: input.mode === "batch" ? n : null,
						output: null,
						expiresAt,
						createdAt: tx.now,
						resolvedAt: null,
					});
					waitpoint = {
						id,
						expiresAt: expiresAt === null ? null : new Date(expiresAt),
						expireDelayMs: expiresAt === null ? null : delayMs(expiresAt, tx.now),
					};
				}

				const enqueue: EnqueueRequest[] = [];
				const childIds: string[] = [];
				for (const c of input.children) {
					const row = insertRun(tx, {
						id: c.id,
						taskId: c.taskId,
						queue: c.queue,
						payload: c.payload,
						maxAttempts: c.maxAttempts,
						delayMs: input.delayMs ?? 0,
						parentRunId: parentId,
						parentWaitpointId: waitpoint ? waitpoint.id : null,
						parentStepName: input.stepName,
						batchIndex: c.batchIndex,
					});
					childIds.push(row.id);
					enqueue.push(enqueueOf(row, tx.now));
				}
				// Internal keys never expire; any conflict is an invariant violation -> throws.
				input.children.forEach((c, i) => {
					const k = keyOf(c.taskId, c.idempotencyKey);
					if (tx.db.keys.has(k)) throw uniqueViolation("idempotency_keys_pkey");
					tx.set(tx.db.keys, k, {
						taskId: c.taskId,
						key: c.idempotencyKey,
						runId: childIds[i] as string,
						expiresAt: null,
					});
				});
				insertStep(
					tx,
					{
						runId: parentId,
						name: input.stepName,
						seqIndex: input.seqIndex,
						kind: STEP_KIND_BY_MODE[input.mode],
						status: input.mode === "trigger" ? "completed" : "waiting",
						output:
							input.mode === "trigger"
								? { id: childIds[0] ?? null }
								: { waitpointId: uuid(input.waitpointId) },
						wakeAt: null,
					},
					"error"
				);
				return { kind: "created", enqueue, waitpoint };
			});
		},

		acquire(runId, executionId, seq) {
			return exec.run(tx => {
				const row = tx.db.runs.get(uuid(runId));
				const execId = uuid(executionId);
				if (!row || (row.status !== "queued" && row.status !== "waiting")) return null;
				if (row.enqueueSeq !== seq) return null;
				return mapRun(
					updateRun(tx, row, {
						status: "running",
						executionId: execId,
						heartbeatAt: tx.now,
						startedAt: row.startedAt ?? tx.now,
						updatedAt: tx.now,
					})
				);
			});
		},

		async heartbeat(runId, executionId, timeoutMs) {
			if (exec.kind === "tx") {
				throw new Error("heartbeat must not run inside a StoreTx (SET LOCAL would leak)");
			}
			const ms = Math.max(1, Math.floor(timeoutMs));
			if (!Number.isSafeInteger(ms)) throw new Error(`invalid heartbeat timeout: ${timeoutMs}`);
			return exec.run(tx => {
				const row = owned(tx, uuid(runId), uuid(executionId));
				if (!row) return "lost";
				// Touches ONLY heartbeat_at (no updated_at), like the HOT update of decision 6.
				updateRun(tx, row, { heartbeatAt: tx.now });
				return "ok";
			});
		},

		async complete(owner, output) {
			const r = await exec.run(tx => finish(tx, owner, "completed", output, null));
			return r.lost
				? { kind: "lost" }
				: {
						kind: "completed",
						updatedAt: r.updatedAt,
						parent: r.parent,
						expireJobsToRemove: r.expireJobsToRemove,
					};
		},

		async fail(owner, error) {
			const r = await exec.run(tx => finish(tx, owner, "failed", null, error));
			return r.lost
				? { kind: "lost" }
				: {
						kind: "failed",
						updatedAt: r.updatedAt,
						parent: r.parent,
						expireJobsToRemove: r.expireJobsToRemove,
					};
		},

		retry(owner, error, backoffMs) {
			return exec.run(tx => {
				const row = owned(tx, uuid(owner.runId), uuid(owner.executionId));
				if (!row) return { kind: "lost" };
				const next = updateRun(tx, row, {
					status: "queued",
					attempt: row.attempt + 1,
					error: toJsonb(error),
					executionId: null,
					runAfter: tx.now + backoffMs,
					enqueueSeq: row.enqueueSeq + 1,
					updatedAt: tx.now,
				});
				return { kind: "retrying", attempt: next.attempt, enqueue: enqueueOf(next, tx.now) };
			});
		},

		release(owner) {
			return exec.run(tx => {
				const row = owned(tx, uuid(owner.runId), uuid(owner.executionId));
				if (!row) return { kind: "lost" };
				const next = updateRun(tx, row, {
					status: "queued",
					executionId: null,
					runAfter: tx.now,
					enqueueSeq: row.enqueueSeq + 1,
					updatedAt: tx.now,
				});
				return { kind: "released", enqueue: enqueueOf(next, tx.now) };
			});
		},

		suspend(owner) {
			return exec.run(tx => {
				const row = owned(tx, uuid(owner.runId), uuid(owner.executionId));
				if (!row) return { kind: "lost" };
				if (hasBlockingWaitpoint(tx, row.id)) {
					updateRun(tx, row, {
						status: "waiting",
						runAfter: null,
						executionId: null,
						enqueueSeq: row.enqueueSeq + 1,
						updatedAt: tx.now,
					});
					return { kind: "waiting_on_waitpoints", status: "waiting", updatedAt: toDate(tx.now) };
				}
				const wake = maxWakeAt(tx, row.id);
				const sleeping = wake !== null && wake > tx.now;
				const next = updateRun(tx, row, {
					status: sleeping ? "waiting" : "queued",
					executionId: null,
					enqueueSeq: row.enqueueSeq + 1,
					runAfter: sleeping ? wake : tx.now,
					updatedAt: tx.now,
				});
				const enqueue = enqueueOf(next, tx.now);
				return sleeping ? { kind: "sleeping", enqueue } : { kind: "requeued", enqueue };
			});
		},

		cancel(runId, reason): Promise<CancelRunResult> {
			return exec.run(tx => {
				const id = uuid(runId);
				const root = tx.db.runs.get(id);
				if (!root) return { kind: "not_found" };
				if (isTerminal(root.status)) {
					return { kind: "already_finished", status: root.status as TerminalRunStatus };
				}
				markCanceled(tx, [id], reason);
				const canceled: CanceledRun[] = [
					{
						runId: id,
						taskId: root.taskId,
						queue: root.queue,
						enqueueSeq: root.enqueueSeq,
						previousStatus: root.status as CanceledRun["previousStatus"],
						attempt: root.attempt,
						error: canceledError(reason),
						updatedAt: toDate(tx.now),
					},
				];
				canceled.push(...cancelDescendants(tx, { parentRunIds: [id] }, reason));
				const expireJobsToRemove = timeOutWaitpointsOf(
					tx,
					canceled.map(c => c.runId)
				);
				const wp = root.parentWaitpointId;
				const parent = wp ? resolveChildWaitpoint(tx, wp) : null;
				return { kind: "canceled", canceled, parent, expireJobsToRemove };
			});
		},

		get(runId) {
			return exec.run(tx => {
				const row = tx.db.runs.get(uuid(runId));
				return row ? mapRun(row) : null;
			});
		},

		list(filter) {
			return exec.run(tx => {
				const limit = Math.min(MAX_LIST_LIMIT, Math.max(1, Math.floor(filter.limit)));
				const statuses =
					filter.status === undefined
						? null
						: new Set(typeof filter.status === "string" ? [filter.status] : filter.status);
				const from = filter.createdFrom?.getTime();
				const to = filter.createdTo?.getTime();
				const cursor = filter.cursor
					? { at: filter.cursor.createdAt.getTime(), id: uuid(filter.cursor.id) }
					: null;
				const rows = [...tx.db.runs.values()].filter(
					r =>
						(filter.taskId === undefined || r.taskId === filter.taskId) &&
						(statuses === null || statuses.has(r.status)) &&
						(from === undefined || r.createdAt >= from) &&
						(to === undefined || r.createdAt < to) &&
						(cursor === null ||
							r.createdAt < cursor.at ||
							(r.createdAt === cursor.at && r.id < cursor.id))
				);
				rows.sort((a, b) =>
					a.createdAt !== b.createdAt ? b.createdAt - a.createdAt : a.id < b.id ? 1 : -1
				);
				const items = rows.slice(0, limit).map(mapRun);
				const last = items[items.length - 1];
				return {
					items,
					nextCursor:
						items.length === limit && last ? { createdAt: last.createdAt, id: last.id } : null,
				};
			});
		},

		listChildResults(waitpointId) {
			return exec.run(tx => {
				const wp = uuid(waitpointId);
				return [...tx.db.runs.values()]
					.filter(r => r.parentWaitpointId === wp)
					.sort((a, b) => (a.batchIndex ?? Infinity) - (b.batchIndex ?? Infinity))
					.map(
						(r): ChildResult => ({
							runId: r.id,
							batchIndex: r.batchIndex,
							status: r.status,
							output: cloneJson(r.output),
							error: cloneJson(r.error),
						})
					);
			});
		},

		listStaleQueued(graceMs, limit) {
			return exec.run(tx =>
				[...tx.db.runs.values()]
					.filter(
						r => r.status === "queued" && r.runAfter !== null && r.runAfter < tx.now - graceMs
					)
					.sort((a, b) => (a.runAfter ?? 0) - (b.runAfter ?? 0))
					.slice(0, sqlLimit(limit))
					.map(r => ({ ...enqueueOf(r, tx.now), delayMs: 0 }))
			);
		},

		listStaleWaiting(graceMs, limit) {
			return exec.run(tx =>
				[...tx.db.runs.values()]
					.filter(
						r =>
							r.status === "waiting" &&
							r.runAfter !== null &&
							r.runAfter < tx.now - graceMs &&
							!hasBlockingWaitpoint(tx, r.id)
					)
					.sort((a, b) => (a.runAfter ?? 0) - (b.runAfter ?? 0))
					.slice(0, sqlLimit(limit))
					.map(r => ({ ...enqueueOf(r, tx.now), delayMs: 0 }))
			);
		},

		repairOrphanWaiting(limit) {
			return exec.run(tx => {
				const ids = [...tx.db.runs.values()]
					.filter(
						r => r.status === "waiting" && r.runAfter === null && !hasBlockingWaitpoint(tx, r.id)
					)
					.map(r => r.id)
					.sort()
					.slice(0, sqlLimit(limit));
				const out: EnqueueRequest[] = [];
				for (const id of ids) {
					const enqueue = applyResumeRule(tx, id);
					if (enqueue) out.push(enqueue);
				}
				return out;
			});
		},

		listStalled(stallTimeoutMs, limit) {
			return exec.run(tx =>
				[...tx.db.runs.values()]
					.filter(
						r =>
							r.status === "running" &&
							r.heartbeatAt !== null &&
							r.heartbeatAt < tx.now - stallTimeoutMs
					)
					.sort((a, b) => (a.heartbeatAt ?? 0) - (b.heartbeatAt ?? 0))
					.slice(0, sqlLimit(limit))
					.map(r => r.id)
			);
		},

		recoverStalled(runId, stallTimeoutMs, error): Promise<RecoverStalledResult> {
			return exec.run(tx => {
				const row = tx.db.runs.get(uuid(runId));
				if (
					row?.status !== "running" ||
					row.heartbeatAt === null ||
					!(row.heartbeatAt < tx.now - stallTimeoutMs)
				) {
					return { kind: "not_stalled" };
				}
				if (row.attempt < row.maxAttempts) {
					const next = updateRun(tx, row, {
						status: "queued",
						attempt: row.attempt + 1,
						error: toJsonb(error),
						executionId: null,
						runAfter: tx.now,
						enqueueSeq: row.enqueueSeq + 1,
						updatedAt: tx.now,
					});
					return { kind: "requeued", attempt: next.attempt, enqueue: enqueueOf(next, tx.now) };
				}
				updateRun(tx, row, {
					status: "failed",
					error: toJsonb(error),
					executionId: null,
					runAfter: null,
					finishedAt: tx.now,
					updatedAt: tx.now,
				});
				const expireJobsToRemove = timeOutWaitpointsOf(tx, [row.id]);
				const wp = row.parentWaitpointId;
				return {
					kind: "failed",
					taskId: row.taskId,
					attempt: row.attempt,
					updatedAt: toDate(tx.now),
					parent: wp ? resolveChildWaitpoint(tx, wp) : null,
					expireJobsToRemove,
				};
			});
		},

		deleteFinishedBatch(olderThanMs, limit) {
			return exec.run(tx => {
				const runs = tx.db.runs;
				const victims = [...runs.values()]
					.filter(r => {
						if (!isTerminal(r.status)) return false;
						if (r.finishedAt === null || !(r.finishedAt < tx.now - olderThanMs)) return false;
						const parent = r.parentRunId === null ? undefined : runs.get(r.parentRunId);
						return parent === undefined || isTerminal(parent.status);
					})
					.slice(0, sqlLimit(limit))
					.map(r => r.id);
				for (const id of victims) deleteRunCascade(tx, id);
				return victims.length;
			});
		},
	};
}

/**
 * `DELETE FROM runs` with the schema's FK actions: steps, waitpoints, logs and keys cascade;
 * `parent_run_id` of children and `parent_waitpoint_id` pointing at deleted waitpoints SET NULL.
 */
function deleteRunCascade(tx: MemTx, runId: string): void {
	const { db } = tx;
	if (!tx.delete(db.runs, runId)) return;
	tx.delete(db.steps, runId);
	const deletedWps = new Set<string>();
	for (const wp of [...db.waitpoints.values()]) {
		if (wp.runId === runId) {
			tx.delete(db.waitpoints, wp.id);
			deletedWps.add(wp.id);
		}
	}
	for (const log of [...db.logs.values()]) if (log.runId === runId) tx.delete(db.logs, log.id);
	for (const [k, key] of [...db.keys]) if (key.runId === runId) tx.delete(db.keys, k);
	for (const r of [...db.runs.values()]) {
		const orphanParent = r.parentRunId === runId;
		const orphanWp = r.parentWaitpointId !== null && deletedWps.has(r.parentWaitpointId);
		if (orphanParent || orphanWp) {
			updateRun(tx, r, {
				...(orphanParent ? { parentRunId: null } : {}),
				...(orphanWp ? { parentWaitpointId: null } : {}),
			});
		}
	}
}
