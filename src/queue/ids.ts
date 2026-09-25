import { ConfigError } from "../errors.js";

/**
 * Deterministic BullMQ job id for a run's enqueue attempt (decision 7):
 * `${runId}-${seq}`, where `seq` is the run's `enqueue_seq` at the time of
 * enqueue. Re-enqueuing the same `seq` reuses the same jobId, which BullMQ
 * treats as an idempotent add (see `docs/research/api-notes.md` §1.6):
 * adding an existing jobId is a silent no-op while that job instance still
 * exists in Redis.
 */
export function runJobId(runId: string, seq: number): string {
	if (!Number.isInteger(seq) || seq < 0) {
		throw new ConfigError(`enqueue seq ${seq} is invalid: must be a non-negative integer`);
	}
	return validateJobId(`${runId}-${seq}`);
}

/**
 * Deterministic BullMQ job id for a waitpoint's expiration tick (decision 10),
 * enqueued on the `-system` queue: `wp-${waitpointId}-expire`.
 */
export function waitpointExpireJobId(waitpointId: string): string {
	return validateJobId(`wp-${waitpointId}-expire`);
}

/** The internal system queue's BullMQ queue name (decision 8): `${prefix}-system`. */
export function systemQueueName(prefix: string): string {
	return `${prefix}-system`;
}

/**
 * BullMQ job **name** (not id) given to every job a `upsertJobScheduler` job scheduler produces
 * for minitrigger (Phase 8, decision 8): the `-system` worker's dispatcher (`executor/system.ts`)
 * switches on it. Each tick's job **id** is BullMQ's own deterministic
 * `repeat:<jobSchedulerId>:<nextMillis>` (see `docs/research/api-notes.md` §1.10); the schedule's
 * own id is carried on `job.opts.repeatJobKey` (and mirrored into `job.data.scheduleId` as a
 * fallback).
 */
export const SCHEDULE_TICK_JOB_NAME = "schedule-tick";

/**
 * Guards against the two hard BullMQ jobId restrictions (verified in
 * `dist/cjs/classes/job.js`, see `docs/research/api-notes.md` §1.5): a `:` in
 * the id (BullMQ throws, except for a narrow 3-segment legacy repeatable-job
 * shape we never produce) and an id that is purely numeric (collides with
 * BullMQ's own auto-generated numeric ids). Both would otherwise surface as an
 * opaque throw from deep inside BullMQ's `Job` constructor instead of a clear
 * `ConfigError` at the call site that built the id.
 */
function validateJobId(jobId: string): string {
	if (jobId.includes(":")) {
		throw new ConfigError(`job id "${jobId}" is invalid: must not contain ":"`);
	}
	if (`${Number.parseInt(jobId, 10)}` === jobId) {
		throw new ConfigError(`job id "${jobId}" is invalid: must not be purely numeric`);
	}
	return jobId;
}
