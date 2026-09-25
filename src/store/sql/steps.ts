import type {
	ExecutionSnapshot,
	InsertSleepResult,
	Json,
	LoadedStep,
	Ownership,
	StepKind,
	StepRecord,
	StepStatus,
	StepStore,
	WaitpointKind,
	WaitpointStatus,
	WaitpointSummary,
} from "../types.js";
import { direct, type SqlContext, type SqlExecutor } from "./db.js";
import { jsonParam } from "./resolve.js";

// ---------------------------------------------------------------------------
// Row shapes
// ---------------------------------------------------------------------------

interface StepWindowRow {
	name: string;
	seq_index: number;
	kind: StepKind;
	status: StepStatus;
	wake_at: Date | null;
	// Verbatim Phase-4 window query (docs/SPEC.md §5 Phase 4, docs/store-contract.md
	// `StepStore.loadForExecution`): when all recorded outputs are NULL (e.g. a run made only of
	// sleep/wait steps), `sum(...) OVER ()` is itself NULL, so both `output` and `outputs_deferred`
	// come back NULL here; that's handled on read (an absent/NULL output is already correct, and a
	// NULL `outputs_deferred` means "not deferred").
	output: Json | null;
	outputs_deferred: boolean | null;
}

interface ExecutionMetaRow {
	now: Date;
	id: string | null;
	step_name: string | null;
	kind: WaitpointKind | null;
	status: WaitpointStatus | null;
	pending_count: number | null;
	expires_at: Date | null;
}

interface OutputRow {
	name: string;
	seq_index: number;
	output: Json | null;
}

interface StepRow {
	run_id: string;
	name: string;
	seq_index: number;
	kind: StepKind;
	status: StepStatus;
	output: Json | null;
	wake_at: Date | null;
	created_at: Date;
}

interface InsertCompletedRow {
	guard_count: number;
	inserted_count: number;
}

interface InsertSleepRow {
	guard_count: number;
	wake_at: Date | null;
	delay_ms: string | null;
}

interface CompleteSleepRow {
	guard_count: number;
	updated_count: number;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createStepStore(exec: SqlExecutor, ctx: SqlContext): StepStore {
	const db = direct(exec);

	return {
		async loadForExecution(runId: string, maxInlineBytes: number): Promise<ExecutionSnapshot> {
			// Statement 1: the Phase-4 window query, verbatim.
			const { rows: stepRows } = await db.query<StepWindowRow>(
				`SELECT name, seq_index, kind, status, wake_at,
				        CASE WHEN sum(pg_column_size(output)) OVER () <= $2 THEN output END AS output,
				        sum(pg_column_size(output)) OVER () > $2 AS outputs_deferred
				 FROM ${ctx.t("steps")}
				 WHERE run_id = $1
				 ORDER BY seq_index`,
				[runId, maxInlineBytes]
			);

			// Statement 2 (the one extra statement the task allows): DB `now()` plus the run's
			// waitpoints, in a single round trip via a LEFT JOIN so we always get exactly one row
			// even when the run has no waitpoints at all.
			const { rows: metaRows } = await db.query<ExecutionMetaRow>(
				`SELECT n.now AS now, w.id, w.step_name, w.kind, w.status, w.pending_count, w.expires_at
				 FROM (SELECT now() AS now) n
				 LEFT JOIN (
				   SELECT id, step_name, kind, status, pending_count, expires_at
				   FROM ${ctx.t("waitpoints")}
				   WHERE run_id = $1
				 ) w ON true`,
				[runId]
			);

			const firstMeta = metaRows[0];
			if (firstMeta === undefined) {
				// Unreachable: the `(SELECT now() AS now)` side always yields exactly one row.
				throw new Error("loadForExecution: expected exactly one meta row");
			}

			const steps: LoadedStep[] = stepRows.map(row => ({
				name: row.name,
				seqIndex: row.seq_index,
				kind: row.kind,
				status: row.status,
				wakeAt: row.wake_at,
				output: row.output,
			}));

			const outputsDeferred = stepRows[0]?.outputs_deferred === true;

			const waitpoints: WaitpointSummary[] = metaRows
				.filter(
					(
						row
					): row is ExecutionMetaRow & {
						id: string;
						step_name: string;
						kind: WaitpointKind;
						status: WaitpointStatus;
					} => row.id !== null
				)
				.map(row => ({
					id: row.id,
					stepName: row.step_name,
					kind: row.kind,
					status: row.status,
					pendingCount: row.pending_count,
					expiresAt: row.expires_at,
				}));

			return { now: firstMeta.now, steps, outputsDeferred, waitpoints };
		},

		async loadOutputs(runId: string, fromSeq: number, toSeq: number) {
			const { rows } = await db.query<OutputRow>(
				`SELECT name, seq_index, output
				 FROM ${ctx.t("steps")}
				 WHERE run_id = $1 AND seq_index BETWEEN $2 AND $3
				 ORDER BY seq_index`,
				[runId, fromSeq, toSeq]
			);
			return rows.map(row => ({ name: row.name, seqIndex: row.seq_index, output: row.output }));
		},

		async insertCompleted(
			owner: Ownership,
			step: { kind: StepKind; name: string; seqIndex: number; output: Json | null }
		): Promise<"ok" | "lost"> {
			// Single statement: the ownership guard and the guarded insert share one CTE so
			// `rowCount = 0` on the insert can be told apart from a lost execution without a second
			// round trip. `inserted_count = 0` with `guard_count > 0` means the ON CONFLICT DO
			// NOTHING fired because the step already existed (idempotent replay) -- not a loss of
			// ownership -- so it is still reported as "ok". The conflict target is `(run_id, name)`
			// only: a `(run_id, seq_index)` collision under a different name is a determinism bug and
			// raises a unique violation instead of being swallowed.
			const { rows } = await db.query<InsertCompletedRow>(
				`WITH locked AS (
				   SELECT 1 FROM ${ctx.t("runs")}
				   WHERE id = $1 AND execution_id = $5 AND status = 'running'
				   FOR SHARE
				 ),
				 guard AS (
				   -- FOR SHARE cannot be combined with an aggregate in the same SELECT, hence the
				   -- separate "locked" CTE above.
				   SELECT count(*)::int AS c FROM locked
				 ),
				 ins AS (
				   INSERT INTO ${ctx.t("steps")} (run_id, name, seq_index, kind, status, output)
				   SELECT $1, $2, $3::int, $6, 'completed', $4::jsonb
				   WHERE (SELECT c FROM guard) > 0
				   ON CONFLICT (run_id, name) DO NOTHING
				   RETURNING 1
				 )
				 SELECT (SELECT c FROM guard) AS guard_count,
				        (SELECT count(*) FROM ins)::int AS inserted_count`,
				// jsonb is always bound as JSON text: pg would send a JS string raw (invalid JSON)
				// and turn a JS array into a PG array literal.
				[
					owner.runId,
					step.name,
					step.seqIndex,
					jsonParam(step.output),
					owner.executionId,
					step.kind,
				]
			);
			const row = rows[0];
			if (row === undefined || row.guard_count === 0) return "lost";
			return "ok";
		},

		async insertSleep(
			owner: Ownership,
			step: { name: string; seqIndex: number; durationMs: number }
		): Promise<InsertSleepResult> {
			const { rows } = await db.query<InsertSleepRow>(
				`WITH locked AS (
				   SELECT 1 FROM ${ctx.t("runs")}
				   WHERE id = $1 AND execution_id = $5 AND status = 'running'
				   FOR SHARE
				 ),
				 guard AS (
				   SELECT count(*)::int AS c FROM locked
				 ),
				 n AS (
				   SELECT now() AS now
				 ),
				 ins AS (
				   INSERT INTO ${ctx.t("steps")} (run_id, name, seq_index, kind, status, wake_at)
				   SELECT $1, $2, $3::int, 'sleep', 'waiting', n.now + ($4::numeric * interval '1 millisecond')
				   FROM n
				   WHERE (SELECT c FROM guard) > 0
				   RETURNING
				     wake_at,
				     greatest(0, round(extract(epoch FROM (wake_at - (SELECT now FROM n))) * 1000))::bigint
				       AS delay_ms
				 )
				 SELECT (SELECT c FROM guard) AS guard_count,
				        (SELECT wake_at FROM ins) AS wake_at,
				        (SELECT delay_ms FROM ins) AS delay_ms`,
				[owner.runId, step.name, step.seqIndex, step.durationMs, owner.executionId]
			);
			const row = rows[0];
			if (row === undefined || row.guard_count === 0) return { kind: "lost" };
			if (row.wake_at === null || row.delay_ms === null) {
				// Unreachable: guard_count > 0 implies the insert ran and RETURNING produced a row.
				throw new Error("insertSleep: guard passed but no row was inserted");
			}
			return { kind: "inserted", wakeAt: row.wake_at, delayMs: Number(row.delay_ms) };
		},

		async completeSleep(owner: Ownership, name: string): Promise<"ok" | "not_due" | "lost"> {
			const { rows } = await db.query<CompleteSleepRow>(
				`WITH locked AS (
				   SELECT 1 FROM ${ctx.t("runs")}
				   WHERE id = $1 AND execution_id = $3 AND status = 'running'
				   FOR SHARE
				 ),
				 guard AS (
				   SELECT count(*)::int AS c FROM locked
				 ),
				 upd AS (
				   UPDATE ${ctx.t("steps")} SET status = 'completed'
				   WHERE run_id = $1 AND name = $2 AND status = 'waiting' AND wake_at <= now()
				     AND (SELECT c FROM guard) > 0
				   RETURNING 1
				 )
				 SELECT (SELECT c FROM guard) AS guard_count,
				        (SELECT count(*) FROM upd)::int AS updated_count`,
				[owner.runId, name, owner.executionId]
			);
			const row = rows[0];
			if (row === undefined || row.guard_count === 0) return "lost";
			return row.updated_count > 0 ? "ok" : "not_due";
		},

		async insertWaitRef(
			owner: Ownership,
			step: { kind: StepKind; name: string; seqIndex: number; waitpointId: string }
		): Promise<"ok" | "lost"> {
			// Same shape as insertCompleted: an existing `(run_id, name)` step is an idempotent
			// replay ("ok"), a failed guard is "lost", a seq_index collision raises.
			const { rows } = await db.query<InsertCompletedRow>(
				`WITH locked AS (
				   SELECT 1 FROM ${ctx.t("runs")}
				   WHERE id = $1 AND execution_id = $5 AND status = 'running'
				   FOR SHARE
				 ),
				 guard AS (
				   SELECT count(*)::int AS c FROM locked
				 ),
				 ins AS (
				   INSERT INTO ${ctx.t("steps")} (run_id, name, seq_index, kind, status, output)
				   SELECT $1, $2, $3::int, $6, 'waiting', jsonb_build_object('waitpointId', $4::uuid)
				   WHERE (SELECT c FROM guard) > 0
				   ON CONFLICT (run_id, name) DO NOTHING
				   RETURNING 1
				 )
				 SELECT (SELECT c FROM guard) AS guard_count,
				        (SELECT count(*) FROM ins)::int AS inserted_count`,
				[owner.runId, step.name, step.seqIndex, step.waitpointId, owner.executionId, step.kind]
			);
			const row = rows[0];
			if (row === undefined || row.guard_count === 0) return "lost";
			return "ok";
		},

		async list(runId: string): Promise<StepRecord[]> {
			const { rows } = await db.query<StepRow>(
				`SELECT run_id, name, seq_index, kind, status, output, wake_at, created_at
				 FROM ${ctx.t("steps")}
				 WHERE run_id = $1
				 ORDER BY seq_index`,
				[runId]
			);
			return rows.map(row => ({
				runId: row.run_id,
				name: row.name,
				seqIndex: row.seq_index,
				kind: row.kind,
				status: row.status,
				output: row.output,
				wakeAt: row.wake_at,
				createdAt: row.created_at,
			}));
		},
	};
}
