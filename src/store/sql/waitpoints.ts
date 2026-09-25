import type {
	CompleteTokenResult,
	CreateTokenResult,
	ExpireWaitpointResult,
	Json,
	WaitpointKind,
	WaitpointStatus,
	WaitpointStore,
} from "../types.js";
import { WAITPOINT_TIMEOUT_REASON } from "../types.js";
import { atomic, direct, type SqlContext, type SqlExecutor } from "./db.js";
import {
	applyResumeRule,
	cancelDescendants,
	jsonParam,
	nowPlusMs,
	timeOutWaitpointsOf,
} from "./resolve.js";

/**
 * Remaining time until `expires_at` in ms (DB clock, decision 17); NULL without `expires_at`.
 * `greatest(0, NULL)` alone would be 0 in Postgres (GREATEST/LEAST ignore NULL arguments), so the
 * NULL case is handled explicitly instead.
 */
const EXPIRE_DELAY_SQL =
	"CASE WHEN expires_at IS NULL THEN NULL " +
	"ELSE greatest(0, round(extract(epoch from (expires_at - now())) * 1000)) " +
	"END::float8";

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
				// Idempotent by (run_id, step_name): a token already recorded under this name (same
				// step) is returned as-is instead of creating a second waitpoint.
				const inserted = await db.query<{
					id: string;
					delay_ms: number | null;
					expires_at: Date | null;
				}>(
					`INSERT INTO ${waitpoints} (id, run_id, step_name, kind, status, expires_at)
					 VALUES ($1, $2, $3, 'token', 'pending',
					   CASE WHEN $4::float8 IS NULL THEN NULL ELSE ${nowPlusMs("$4")} END)
					 ON CONFLICT (run_id, step_name) DO NOTHING
					 RETURNING id, expires_at, ${EXPIRE_DELAY_SQL} AS delay_ms`,
					[token.waitpointId, owner.runId, token.name, token.timeoutMs]
				);
				let row = inserted.rows[0];
				if (!row) {
					const { rows: existing } = await db.query<{
						id: string;
						kind: WaitpointKind;
						expires_at: Date | null;
						delay_ms: number | null;
					}>(
						`SELECT id, kind, expires_at,
						   CASE WHEN status = 'pending' THEN ${EXPIRE_DELAY_SQL} END AS delay_ms
						 FROM ${waitpoints} WHERE run_id = $1 AND step_name = $2`,
						[owner.runId, token.name]
					);
					const found = existing[0];
					if (found?.kind !== "token") {
						throw new Error(
							`createToken: step "${token.name}" of run ${owner.runId} already has a ` +
								`${found?.kind ?? "missing"} waitpoint`
						);
					}
					row = found;
				}
				await db.query(
					`INSERT INTO ${ctx.t("steps")} (run_id, name, seq_index, kind, status, output)
					 VALUES ($1, $2, $3, 'token', 'completed', $4::jsonb)
					 ON CONFLICT (run_id, name) DO NOTHING`,
					[owner.runId, token.name, token.seqIndex, jsonParam({ id: row.id })]
				);
				return {
					kind: "created",
					waitpointId: row.id,
					expiresAt: row.expires_at,
					expireDelayMs: row.delay_ms,
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
					// `run_finished` only when the token was timed out BY the run's own terminal exit
					// (every terminal transition times out the run's pending tokens in the same
					// transaction, so `resolved_at = finished_at` exactly, same `now()`), never for a
					// token that expired on its own in an earlier transaction whose run later
					// finished. Computed in SQL (not by comparing JS `Date`s) to avoid any precision
					// loss from the driver's timestamp mapping.
					const { rows: cur } = await db.query<{
						status: WaitpointStatus;
						run_finished: boolean;
					}>(
						`SELECT w.status,
						   (r.finished_at IS NOT NULL AND w.resolved_at = r.finished_at) AS run_finished
						 FROM ${waitpoints} w
						 LEFT JOIN ${runs} r ON r.id = w.run_id
						 WHERE w.id = $1`,
						[waitpointId]
					);
					const row = cur[0];
					if (!row) return { kind: "not_found" };
					if (row.status === "timed_out" && row.run_finished) {
						return { kind: "run_finished" };
					}
					return { kind: "not_pending", status: row.status };
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
