import type {
	ExecutionSnapshot,
	InsertSleepResult,
	Json,
	StepKind,
	StepRecord,
	StepStatus,
	StepStore,
} from "../types.js";
import { type Exec, owned, stepsOf, waitpointsOf } from "./resolve.js";
import {
	cloneJson,
	delayMs,
	jsonBytes,
	type MemTx,
	pgText,
	type StepRow,
	toDate,
	toJsonb,
	uniqueViolation,
	uuid,
} from "./state.js";

/**
 * Inserts a step row enforcing `PRIMARY KEY (run_id, name)` and `UNIQUE (run_id, seq_index)`.
 * `onNameConflict: "ignore"` is `ON CONFLICT (run_id, name) DO NOTHING` (returns false); a
 * seq_index collision under another name always throws. `output: undefined` means SQL NULL.
 */
export function insertStep(
	tx: MemTx,
	step: {
		runId: string;
		name: string;
		seqIndex: number;
		kind: StepKind;
		status: StepStatus;
		output?: Json | null;
		wakeAt: number | null;
	},
	onNameConflict: "error" | "ignore"
): boolean {
	const inner = tx.db.steps.get(step.runId);
	if (inner?.has(step.name)) {
		if (onNameConflict === "ignore") return false;
		throw uniqueViolation("steps_pkey");
	}
	for (const s of inner?.values() ?? []) {
		if (s.seqIndex === step.seqIndex) throw uniqueViolation("steps_run_id_seq_index_key");
	}
	const output = step.output === undefined ? null : toJsonb(step.output);
	const row: StepRow = {
		runId: step.runId,
		name: pgText(step.name),
		seqIndex: step.seqIndex,
		kind: step.kind,
		status: step.status,
		output,
		outputBytes: step.output === undefined ? null : jsonBytes(output),
		wakeAt: step.wakeAt,
		createdAt: tx.now,
	};
	const next = new Map(inner ?? []);
	next.set(step.name, row);
	tx.set(tx.db.steps, step.runId, next);
	return true;
}

function sortedSteps(tx: MemTx, runId: string): StepRow[] {
	return stepsOf(tx, runId).sort((a, b) => a.seqIndex - b.seqIndex);
}

export function createStepStore(exec: Exec): StepStore {
	return {
		loadForExecution(runId, maxInlineBytes): Promise<ExecutionSnapshot> {
			return exec.run(tx => {
				const id = uuid(runId);
				const rows = sortedSteps(tx, id);
				// Window query: sum of the (non-NULL) output sizes; NULL when every output is NULL.
				let total: number | null = null;
				for (const r of rows) if (r.outputBytes !== null) total = (total ?? 0) + r.outputBytes;
				const outputsDeferred = total !== null && total > maxInlineBytes;
				const inline = total !== null && total <= maxInlineBytes;
				return {
					now: new Date(tx.now),
					steps: rows.map(r => ({
						name: r.name,
						seqIndex: r.seqIndex,
						kind: r.kind,
						status: r.status,
						wakeAt: toDate(r.wakeAt),
						output: inline ? cloneJson(r.output) : null,
					})),
					outputsDeferred,
					waitpoints: waitpointsOf(tx, id).map(w => ({
						id: w.id,
						stepName: w.stepName,
						kind: w.kind,
						status: w.status,
						pendingCount: w.pendingCount,
						expiresAt: toDate(w.expiresAt),
					})),
				};
			});
		},

		loadOutputs(runId, fromSeq, toSeq) {
			return exec.run(tx =>
				sortedSteps(tx, uuid(runId))
					.filter(r => r.seqIndex >= fromSeq && r.seqIndex <= toSeq)
					.map(r => ({ name: r.name, seqIndex: r.seqIndex, output: cloneJson(r.output) }))
			);
		},

		insertCompleted(owner, step) {
			return exec.run(tx => {
				const runId = uuid(owner.runId);
				if (!owned(tx, runId, uuid(owner.executionId))) return "lost";
				insertStep(
					tx,
					{ runId, ...step, status: "completed", output: step.output ?? null, wakeAt: null },
					"ignore"
				);
				return "ok";
			});
		},

		insertSleep(owner, step): Promise<InsertSleepResult> {
			return exec.run(tx => {
				const runId = uuid(owner.runId);
				if (!owned(tx, runId, uuid(owner.executionId))) return { kind: "lost" };
				const wakeAt = tx.now + step.durationMs;
				insertStep(
					tx,
					{
						runId,
						name: step.name,
						seqIndex: step.seqIndex,
						kind: "sleep",
						status: "waiting",
						wakeAt,
					},
					"error"
				);
				return { kind: "inserted", wakeAt: new Date(wakeAt), delayMs: delayMs(wakeAt, tx.now) };
			});
		},

		completeSleep(owner, name) {
			return exec.run(tx => {
				const runId = uuid(owner.runId);
				if (!owned(tx, runId, uuid(owner.executionId))) return "lost";
				const inner = tx.db.steps.get(runId);
				const row = inner?.get(name);
				if (!inner || !row || row.status !== "waiting" || row.wakeAt === null) return "not_due";
				if (row.wakeAt > tx.now) return "not_due";
				const next = new Map(inner);
				next.set(name, { ...row, status: "completed" });
				tx.set(tx.db.steps, runId, next);
				return "ok";
			});
		},

		insertWaitRef(owner, step) {
			return exec.run(tx => {
				const runId = uuid(owner.runId);
				if (!owned(tx, runId, uuid(owner.executionId))) return "lost";
				insertStep(
					tx,
					{
						runId,
						name: step.name,
						seqIndex: step.seqIndex,
						kind: step.kind,
						status: "waiting",
						output: { waitpointId: uuid(step.waitpointId) },
						wakeAt: null,
					},
					"ignore"
				);
				return "ok";
			});
		},

		list(runId) {
			return exec.run(tx =>
				sortedSteps(tx, uuid(runId)).map(
					(r): StepRecord => ({
						runId: r.runId,
						name: r.name,
						seqIndex: r.seqIndex,
						kind: r.kind,
						status: r.status,
						output: cloneJson(r.output),
						wakeAt: toDate(r.wakeAt),
						createdAt: new Date(r.createdAt),
					})
				)
			);
		},
	};
}
