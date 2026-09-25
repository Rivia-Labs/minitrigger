import { DelayedError, type Job } from "bullmq";
import type { ResolvedConfig } from "../config.js";
import type { ProcessSystemFn, Transport } from "../queue/transport.js";
import type { CanceledRun, Store } from "../store/types.js";
import { afterResolution } from "./execute-run.js";
import type { ActiveExecutions } from "./execution.js";

/** Names of the jobs on the internal `${prefix}-system` queue (decision 8). */
export type SystemJobName = "waitpoint-expire" | "reconcile" | "retention" | "schedule-tick";

export interface SystemDeps {
	config: Pick<ResolvedConfig, "logger">;
	store: Store;
	transport: Pick<Transport, "enqueueRun" | "removeRunJob" | "removeSystemJob">;
	/** Local executions, aborted when the expiry cancels their run. */
	active: ActiveExecutions;
}

/**
 * Dispatcher for the `-system` worker. `waitpoint-expire` (Phase 5) is implemented here;
 * `reconcile` and `retention` (Phase 6) and `schedule-tick` (Phase 8) are acknowledged as no-ops
 * until then. Handlers never throw for expected conditions (BullMQ jobs use `attempts: 1`).
 */
export function createSystemDispatcher(deps: SystemDeps): ProcessSystemFn {
	const { logger } = deps.config;
	return async (name, data, job) => {
		switch (name as SystemJobName) {
			case "waitpoint-expire":
				return expireWaitpoint(deps, data, job);
			case "reconcile":
			case "retention":
			case "schedule-tick":
				logger.debug({ job: name, data }, "system job has no handler yet; ignored");
				return;
			default:
				logger.warn({ job: name }, "unknown system job ignored");
		}
	};
}

/**
 * `wp-${waitpointId}-expire` (decision 10). `store.waitpoints.expire` only acts on a `pending`
 * waitpoint whose `expires_at <= now()` (DB clock); anything else is a no-op.
 *
 * `not_due` (BullMQ's clock fired before the DB's): the job is moved back to `delayed` IN PLACE
 * with `job.moveToDelayed(Date.now() + delayMs, job.token)` + `DelayedError` (the documented
 * BullMQ pattern), keeping the same jobId. Re-adding `wp-${id}-expire` from inside the handler
 * would be silently dropped (the id still belongs to this active job, api-notes §1.6), and a
 * different id would escape the resolvers' removal. `delayMs` comes from the DB (decision 17);
 * only the absolute timestamp BullMQ needs is based on the local clock.
 */
async function expireWaitpoint(deps: SystemDeps, data: unknown, job: Job): Promise<void> {
	const { logger } = deps.config;
	const waitpointId = (data as { waitpointId?: unknown } | null)?.waitpointId;
	if (typeof waitpointId !== "string") {
		logger.warn({ jobId: job.id, data }, "waitpoint-expire job without a waitpointId ignored");
		return;
	}
	const result = await deps.store.waitpoints.expire(waitpointId);
	switch (result.kind) {
		case "noop":
			return;
		case "not_due":
			logger.debug(
				{ waitpointId, delayMs: result.delayMs },
				"waitpoint expiry fired early; re-delayed"
			);
			await job.moveToDelayed(Date.now() + Math.max(1, result.delayMs), job.token);
			throw new DelayedError();
		case "expired":
			await cancelLocally(deps, result.canceledChildren);
			// The waitpoint's own job is this one (active): nothing to remove for it.
			await afterResolution(
				deps,
				{ ...result.resolution, hasExpireJob: false },
				result.expireJobsToRemove
			);
			logger.debug(
				{ waitpointId, canceledChildren: result.canceledChildren.length },
				"waitpoint expired"
			);
			return;
	}
}

/** Aborts local executions of canceled runs and removes their pending run jobs (best-effort). */
export async function cancelLocally(
	deps: Pick<SystemDeps, "transport" | "active">,
	canceled: readonly CanceledRun[]
): Promise<void> {
	for (const run of canceled) {
		// Cross-process abort via pub/sub comes in Phase 6; ownership is already gone anyway.
		deps.active.get(run.runId)?.abort("canceled");
		await deps.transport.removeRunJob(run.queue, run.runId, run.enqueueSeq);
	}
}
