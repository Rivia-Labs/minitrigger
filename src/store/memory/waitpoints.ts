import type {
	CompleteTokenResult,
	CreateTokenResult,
	ExpireWaitpointResult,
	WaitpointStore,
} from "../types.js";
import { WAITPOINT_TIMEOUT_REASON } from "../types.js";
import {
	applyResumeRule,
	cancelDescendants,
	type Exec,
	isTerminal,
	owned,
	timeOutWaitpointsOf,
} from "./resolve.js";
import {
	cloneJson,
	delayMs,
	sqlLimit,
	toDate,
	toJsonb,
	uniqueViolation,
	uuid,
	type WaitpointRow,
} from "./state.js";
import { insertStep } from "./steps.js";

export function createWaitpointStore(exec: Exec): WaitpointStore {
	return {
		createToken(owner, token): Promise<CreateTokenResult> {
			return exec.run(tx => {
				const runId = uuid(owner.runId);
				if (!owned(tx, runId, uuid(owner.executionId))) return { kind: "lost" };
				let row: WaitpointRow | undefined;
				for (const wp of tx.db.waitpoints.values()) {
					if (wp.runId === runId && wp.stepName === token.name) row = wp;
				}
				let delay: number | null;
				if (row) {
					// Idempotent by (run_id, step_name).
					if (row.kind !== "token") {
						throw new Error(
							`createToken: step "${token.name}" of run ${runId} already has a ${row.kind} waitpoint`
						);
					}
					// `greatest(0, NULL)` is 0 in Postgres: a pending token without timeout reports 0.
					delay =
						row.status === "pending"
							? row.expiresAt === null
								? 0
								: delayMs(row.expiresAt, tx.now)
							: null;
				} else {
					const id = uuid(token.waitpointId);
					if (tx.db.waitpoints.has(id)) throw uniqueViolation("waitpoints_pkey");
					row = {
						id,
						runId,
						stepName: token.name,
						kind: "token",
						status: "pending",
						pendingCount: null,
						output: null,
						expiresAt: token.timeoutMs === null ? null : tx.now + token.timeoutMs,
						createdAt: tx.now,
						resolvedAt: null,
					};
					tx.set(tx.db.waitpoints, id, row);
					delay = row.expiresAt === null ? 0 : delayMs(row.expiresAt, tx.now);
				}
				insertStep(
					tx,
					{
						runId,
						name: token.name,
						seqIndex: token.seqIndex,
						kind: "token",
						status: "completed",
						output: { id: row.id },
						wakeAt: null,
					},
					"ignore"
				);
				return {
					kind: "created",
					waitpointId: row.id,
					expiresAt: toDate(row.expiresAt),
					expireDelayMs: delay,
				};
			});
		},

		completeToken(waitpointId, output): Promise<CompleteTokenResult> {
			return exec.run(tx => {
				const wp = tx.db.waitpoints.get(uuid(waitpointId));
				if (wp?.kind !== "token") return { kind: "not_found" };
				const runStatus = tx.db.runs.get(wp.runId)?.status;
				if (wp.status !== "pending") {
					if (wp.status === "timed_out" && runStatus !== undefined && isTerminal(runStatus)) {
						return { kind: "run_finished" };
					}
					return { kind: "not_pending", status: wp.status };
				}
				tx.set(tx.db.waitpoints, wp.id, {
					...wp,
					status: "completed",
					output: toJsonb(output),
					resolvedAt: tx.now,
				});
				const resume = applyResumeRule(tx, wp.runId);
				return {
					kind: "completed",
					resolution: { waitpointId: wp.id, hasExpireJob: wp.expiresAt !== null, resume },
				};
			});
		},

		expire(waitpointId): Promise<ExpireWaitpointResult> {
			return exec.run(tx => {
				const wp = tx.db.waitpoints.get(uuid(waitpointId));
				if (wp?.status !== "pending" || wp.expiresAt === null) return { kind: "noop" };
				if (wp.expiresAt > tx.now) {
					return { kind: "not_due", delayMs: delayMs(wp.expiresAt, tx.now) };
				}
				const canceledChildren =
					wp.kind === "token"
						? []
						: cancelDescendants(tx, { waitpointId: wp.id }, WAITPOINT_TIMEOUT_REASON);
				const expireJobsToRemove = timeOutWaitpointsOf(
					tx,
					canceledChildren.map(c => c.runId)
				);
				const current = tx.db.waitpoints.get(wp.id);
				if (current?.status === "pending") {
					tx.set(tx.db.waitpoints, wp.id, { ...current, status: "timed_out", resolvedAt: tx.now });
				}
				const resume = applyResumeRule(tx, wp.runId);
				return {
					kind: "expired",
					resolution: { waitpointId: wp.id, hasExpireJob: true, resume },
					canceledChildren,
					expireJobsToRemove,
				};
			});
		},

		listExpired(limit) {
			return exec.run(tx => {
				const due = [...tx.db.waitpoints.values()].filter(
					w => w.status === "pending" && w.expiresAt !== null && w.expiresAt <= tx.now
				);
				due.sort((a, b) => (a.expiresAt ?? 0) - (b.expiresAt ?? 0));
				return due.slice(0, sqlLimit(limit)).map(w => w.id);
			});
		},

		getToken(waitpointId) {
			return exec.run(tx => {
				const wp = tx.db.waitpoints.get(uuid(waitpointId));
				return wp ? { status: wp.status, output: cloneJson(wp.output), runId: wp.runId } : null;
			});
		},
	};
}
