import { DelayedError, type Job } from "bullmq";
import type { ResolvedConfig } from "../config.js";
import type { ProcessSystemFn, Transport } from "../queue/transport.js";
import type { Registry } from "../registry.js";
import { processScheduleTick } from "../schedules.js";
import type { CanceledRun, Store } from "../store/types.js";
import { afterResolution } from "./execute-run.js";
import type { ActiveExecutions } from "./execution.js";
import type { RunNotifier } from "./notify.js";

/** Names of the jobs on the internal `${prefix}-system` queue (decision 8). */
export type SystemJobName = "waitpoint-expire" | "reconcile" | "retention" | "schedule-tick";

export interface SystemDeps {
	/**
	 * `schema` (optional so unit tests can pass just a logger; the engine passes its whole resolved
	 * config) reaches `processScheduleTick`: a scheduler tagged with another schema is never removed
	 * by the tick's self-heal (audit item 8).
	 */
	config: Pick<ResolvedConfig, "logger"> & Partial<Pick<ResolvedConfig, "schema">>;
	store: Store;
	transport: Pick<
		Transport,
		| "enqueueRun"
		| "removeRunJob"
		| "removeSystemJob"
		| "upsertJobScheduler"
		| "removeJobScheduler"
		| "getJobScheduler"
	>;
	/** Local executions, aborted when the expiry cancels their run. */
	active: ActiveExecutions;
	/** Publishes cancels/status changes to the other processes (best-effort). */
	notify?: RunNotifier;
	/**
	 * `retention` job (Phase 6): one retention sweep, returning the number of runs deleted. Absent
	 * => the job is ignored.
	 */
	runRetention?: () => Promise<number>;
	/** Needed by `schedule-tick` (Phase 8) to resolve the schedule's task (queue, maxAttempts). */
	registry: Registry;
}

/**
 * Dispatcher for the `-system` worker: `waitpoint-expire` (Phase 5), `retention` (Phase 6) and
 * `schedule-tick` (Phase 8, `src/schedules.ts#processScheduleTick`). `reconcile` is not a job (the
 * leader runs the reconciler inline on its own timer, see `engine.ts`): acknowledged as a no-op.
 * Handlers never throw for expected conditions (BullMQ jobs use `attempts: 1`).
 *
 * `retention` is enqueued only by the leader, once per `retention.intervalMs` window (deterministic
 * jobId), but it may be processed by the `-system` worker of ANY started engine: it does not check
 * leadership. That is safe because a sweep is idempotent (`deleteFinishedBatch` locks with
 * `FOR UPDATE SKIP LOCKED` and only deletes what is still eligible), so a rare second sweep in the
 * same window (e.g. right after a leadership change) only costs a few cheap queries. `schedule-tick`
 * is the same: any `-system` worker may process a tick, made safe by the run's idempotency key
 * (decision 9) rather than by leadership.
 */
export function createSystemDispatcher(deps: SystemDeps): ProcessSystemFn {
	const { logger } = deps.config;
	return async (name, data, job) => {
		switch (name as SystemJobName) {
			case "waitpoint-expire":
				return logFailure(logger, "waitpoint-expire", expireFields(data), () =>
					expireWaitpoint(deps, data, job)
				);
			case "retention":
				return logFailure(logger, "retention", {}, () => retention(deps));
			case "schedule-tick":
				return scheduleTick(deps, data, job);
			case "reconcile":
				logger.debug({ job: name, data }, "system job has no handler yet; ignored");
				return;
			default:
				logger.warn({ job: name }, "unknown system job ignored");
		}
	};
}

/**
 * Audit item (d): system jobs use `removeOnFail: true` and nobody listens to the worker's `failed`
 * event, so an unexpected failure of `waitpoint-expire` or `retention` would vanish silently. Logs
 * it (`error`, with the job's identifying fields) and rethrows so BullMQ still records the failure.
 * `DelayedError` is the documented re-delay signal of `not_due`, not a failure.
 */
async function logFailure(
	logger: ResolvedConfig["logger"],
	job: SystemJobName,
	fields: Record<string, unknown>,
	fn: () => Promise<void>
): Promise<void> {
	try {
		await fn();
	} catch (error) {
		if (!(error instanceof DelayedError)) {
			logger.error({ ...fields, err: error, job }, `system job "${job}" failed`);
		}
		throw error;
	}
}

function expireFields(data: unknown): Record<string, unknown> {
	const waitpointId = (data as { waitpointId?: unknown } | null)?.waitpointId;
	return typeof waitpointId === "string" ? { waitpointId } : {};
}

/**
 * `job.repeatJobKey` is the schedule's own id, set by BullMQ itself on every job a job scheduler
 * produces (a top-level `Job` property, NOT under `job.opts` -- verified against the installed
 * `bullmq` version directly: `Job`'s constructor pulls `repeatJobKey` out of `opts` and assigns it
 * to `this.repeatJobKey`, `dist/cjs/classes/job.js`; `docs/research/api-notes.md` §1.10 describes it
 * as `opts.repeatJobKey`, which is only true of the `Job` instance `upsertJobScheduler()` itself
 * returns, not of the job as received by a worker's processor). `data.scheduleId` (set by
 * `Transport.upsertJobScheduler`'s job template) is a fallback for a job added some other way.
 * `job.opts.prevMillis` is this tick's logical scheduled time; `job.timestamp` (when the job record
 * was created) is the fallback if it is somehow missing. Both `job.repeatJobKey` and
 * `job.opts.prevMillis` are properties of the underlying Redis job hash, so they survive a BullMQ
 * retry of the same job unchanged (verified against `bullmq` 6.3.8: a retry re-processes the exact
 * same `Job` record, it does not produce a new tick from the scheduler) -- the retry (fix 2/3,
 * `upsertJobScheduler`'s `attempts: 5` + exponential backoff template opts) resolves the same
 * `scheduleId`/`scheduledAtMs` every time, so it targets the same `__schedule:${id}:${ts}`
 * idempotency key as the attempt(s) before it.
 *
 * Every failure -- including `processScheduleTick` propagating `TaskNotRegisteredError` when this
 * process doesn't have `schedule.taskId` registered (fix 2/3: that case is deliberately NOT
 * swallowed here or in `processScheduleTick`, so BullMQ's retry gets a chance to land on a process
 * that does) -- is logged with `scheduleId`/`scheduledAtMs` before being rethrown, so it's always
 * visible which occurrence of which schedule failed, even though the handler itself never catches
 * it into a no-op.
 */
async function scheduleTick(deps: SystemDeps, data: unknown, job: Job): Promise<void> {
	const { logger } = deps.config;
	const scheduleId =
		typeof job.repeatJobKey === "string"
			? job.repeatJobKey
			: (data as { scheduleId?: unknown } | null)?.scheduleId;
	if (typeof scheduleId !== "string") {
		logger.warn(
			{ jobId: job.id, data },
			"schedule-tick job without a resolvable scheduleId ignored"
		);
		return;
	}
	const scheduledAtMs =
		typeof job.opts.prevMillis === "number" ? job.opts.prevMillis : job.timestamp;
	try {
		await processScheduleTick(
			{
				store: deps.store,
				transport: deps.transport,
				registry: deps.registry,
				logger,
				...(deps.notify ? { notify: deps.notify } : {}),
				...(deps.config.schema !== undefined ? { schema: deps.config.schema } : {}),
			},
			scheduleId,
			scheduledAtMs
		);
	} catch (error) {
		logger.error(
			{ err: error, scheduleId, scheduledAtMs },
			"schedule tick failed; BullMQ will retry it per the job scheduler's attempts/backoff"
		);
		throw error;
	}
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

async function retention(deps: SystemDeps): Promise<void> {
	const { logger } = deps.config;
	if (!deps.runRetention) {
		logger.debug({}, "retention job ignored: retention is not configured in this engine");
		return;
	}
	const deleted = await deps.runRetention();
	logger.info({ deleted }, `retention: deleted ${deleted} finished run(s)`);
}

/**
 * Aborts local executions of canceled runs, removes their pending run jobs (best-effort) and
 * publishes the cancel so executions in other processes abort too (their ownership is already
 * gone either way: a zombie cannot write anything, decision 5).
 */
export async function cancelLocally(
	deps: Pick<SystemDeps, "transport" | "active" | "notify">,
	canceled: readonly CanceledRun[]
): Promise<void> {
	if (canceled.length === 0) return;
	for (const run of canceled) {
		deps.active.get(run.runId)?.abort("canceled");
		deps.notify?.status({
			runId: run.runId,
			taskId: run.taskId,
			status: "canceled",
			attempt: run.attempt,
			error: run.error,
			updatedAt: run.updatedAt,
			transition: "canceled",
		});
	}
	deps.notify?.canceled(canceled.map(run => run.runId));
	for (const run of canceled) {
		await deps.transport.removeRunJob(run.queue, run.runId, run.enqueueSeq);
	}
}
