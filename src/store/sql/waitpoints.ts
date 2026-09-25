import type {
	CompleteTokenResult,
	CreateTokenResult,
	ExpireWaitpointResult,
	Json,
	WaitpointKind,
	WaitpointStatus,
	WaitpointStore,
} from "../types.js";
import { atomic, direct, type SqlContext, type SqlExecutor } from "./db.js";
import {
	applyResumeRule,
	cancelDescendants,
	jsonParam,
	nowPlusMs,
	timeOutWaitpointsOf,
} from "./resolve.js";

/** `cancel` reason written on children canceled because their parent's waitpoint timed out. */
export const WAITPOINT_TIMEOUT_REASON = "Parent waitpoint timed out";

export function createWaitpointStore(exec: SqlExecutor, ctx: SqlContext): WaitpointStore {
	const runs = ctx.t("runs");
	const waitpoints = ctx.t("waitpoints");

	return {
		createToken(owner, token): Promise<CreateTokenResult> {
			return atomic(exec, async db => {
				const guard = await db.query(
					`SELECT 1 FROM ${runs}
					 WHERE id = $1 AND execution_id = $2 AND status = 'running' FOR SHARE`,
					[owner.runId, owner.executionId]
				);
				if (guard.rowCount === 0) return { kind: "lost" };
				const { rows } = await db.query<{ expires_at: Date | null; delay_ms: number | null }>(
					`INSERT INTO ${waitpoints} (id, run_id, step_name, kind, status, expires_at)
					 VALUES ($1, $2, $3, 'token', 'pending',
					   CASE WHEN $4::float8 IS NULL THEN NULL ELSE ${nowPlusMs("$4")} END)
					 RETURNING expires_at,
					   greatest(0, round(extract(epoch from (expires_at - now())) * 1000))::float8
					     AS delay_ms`,
					[token.waitpointId, owner.runId, token.name, token.timeoutMs]
				);
				await db.query(
					`INSERT INTO ${ctx.t("steps")} (run_id, name, seq_index, status, output)
					 VALUES ($1, $2, $3, 'completed', $4::jsonb)`,
					[owner.runId, token.name, token.seqIndex, jsonParam({ id: token.waitpointId })]
				);
				const row = rows[0];
				return {
					kind: "created",
					waitpointId: token.waitpointId,
					expiresAt: row?.expires_at ?? null,
					expireDelayMs: row?.delay_ms ?? null,
				};
			});
		},

		completeToken(waitpointId, output: Json): Promise<CompleteTokenResult> {
			return atomic(exec, async db => {
				const { rows: found } = await db.query<{ run_id: string; kind: WaitpointKind }>(
					`SELECT run_id, kind FROM ${waitpoints} WHERE id = $1`,
					[waitpointId]
				);
				const wp = found[0];
				if (wp?.kind !== "token") return { kind: "not_found" };
				// Decision 3: lock the waiting run before touching its waitpoints.
				await db.query(`SELECT 1 FROM ${runs} WHERE id = $1 FOR UPDATE`, [wp.run_id]);
				const { rows } = await db.query<{ has_expiry: boolean }>(
					`UPDATE ${waitpoints}
					 SET status = 'completed', output = $2::jsonb, resolved_at = now()
					 WHERE id = $1 AND kind = 'token' AND status = 'pending'
					 RETURNING expires_at IS NOT NULL AS has_expiry`,
					[waitpointId, jsonParam(output)]
				);
				const updated = rows[0];
				if (!updated) {
					const { rows: cur } = await db.query<{ status: WaitpointStatus }>(
						`SELECT status FROM ${waitpoints} WHERE id = $1`,
						[waitpointId]
					);
					const status = cur[0]?.status;
					return status ? { kind: "not_pending", status } : { kind: "not_found" };
				}
				const resume = await applyResumeRule(db, ctx, wp.run_id);
				return {
					kind: "completed",
					resolution: { waitpointId, hasExpireJob: updated.has_expiry, resume },
				};
			});
		},

		expire(waitpointId): Promise<ExpireWaitpointResult> {
			return atomic(exec, async db => {
				const { rows: found } = await db.query<{ run_id: string }>(
					`SELECT run_id FROM ${waitpoints} WHERE id = $1`,
					[waitpointId]
				);
				const wp = found[0];
				if (!wp) return { kind: "noop" };
				// Lock order: run -> descendants -> waitpoint rows. Under the run lock the waitpoint
				// can't change (every resolver locks this run first), so the check below is stable.
				await db.query(`SELECT 1 FROM ${runs} WHERE id = $1 FOR UPDATE`, [wp.run_id]);
				const { rows: due } = await db.query<{
					kind: WaitpointKind;
					is_due: boolean;
					delay_ms: number;
				}>(
					`SELECT kind, expires_at <= now() AS is_due,
					   greatest(0, round(extract(epoch from (expires_at - now())) * 1000))::float8
					     AS delay_ms
					 FROM ${waitpoints}
					 WHERE id = $1 AND status = 'pending' AND expires_at IS NOT NULL`,
					[waitpointId]
				);
				const current = due[0];
				if (!current) return { kind: "noop" };
				// Fired early (BullMQ delay vs DB clock skew): the handler re-delays instead of
				// leaving it to the reconciler.
				if (!current.is_due) return { kind: "not_due", delayMs: current.delay_ms };
				const canceledChildren =
					current.kind === "token"
						? []
						: await cancelDescendants(db, ctx, { waitpointId }, WAITPOINT_TIMEOUT_REASON);
				// Waitpoint rows last: the canceled children's own pending waitpoints, then this one.
				const expireJobsToRemove = await timeOutWaitpointsOf(
					db,
					ctx,
					canceledChildren.map(c => c.runId)
				);
				await db.query(
					`UPDATE ${waitpoints} SET status = 'timed_out', resolved_at = now()
					 WHERE id = $1 AND status = 'pending'`,
					[waitpointId]
				);
				const resume = await applyResumeRule(db, ctx, wp.run_id);
				return {
					kind: "expired",
					resolution: { waitpointId, hasExpireJob: true, resume },
					canceledChildren,
					expireJobsToRemove,
				};
			});
		},

		async listExpired(limit) {
			const { rows } = await direct(exec).query<{ id: string }>(
				`SELECT id FROM ${waitpoints}
				 WHERE status = 'pending' AND expires_at <= now()
				 ORDER BY expires_at LIMIT $1`,
				[limit]
			);
			return rows.map(r => r.id);
		},

		async getToken(waitpointId) {
			const { rows } = await direct(exec).query<{
				status: WaitpointStatus;
				output: Json | null;
				run_id: string;
			}>(`SELECT status, output, run_id FROM ${waitpoints} WHERE id = $1`, [waitpointId]);
			const row = rows[0];
			return row ? { status: row.status, output: row.output, runId: row.run_id } : null;
		},
	};
}
