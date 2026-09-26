/**
 * Cron math for the Postgres-only transport (Task 6; `docs/superpowers/specs/
 * 2026-09-26-postgres-transport-design.md` §3.6). No dependency on `bullmq`/`ioredis` -- this file
 * is imported by the shared Postgres backend, which must stay loadable without either peer
 * installed (`docs/superpowers/sdd/2026-09-26-postgres-transport/constraints.md`).
 *
 * `nextOccurrence` is now the single source of truth for "what's the next tick after this
 * instant" in BOTH transports: `src/schedules.ts#validateCronPattern` (used by every mode) calls
 * it just to validate a pattern/timezone pair (discarding the result), and Task 8's
 * `upsertJobScheduler`/tick handler for the Postgres backend call it to compute `run_after` for
 * the next `schedule-tick:<id>` row. `cron-parser` (`^5.10.1`, the exact version BullMQ 6.3.8
 * already resolves transitively -- `pnpm ls cron-parser` shows one copy) is a direct dependency
 * now, not merely BullMQ's implementation detail: the Postgres transport has no BullMQ to lean on.
 */

import { CronExpressionParser } from "cron-parser";
import { ConfigError } from "../errors.js";

/** Prefix for the Postgres-backend `-system` job id of a schedule's recurring tick. */
const SCHEDULE_TICK_PREFIX = "schedule-tick:";

/**
 * The next time `pattern` (optionally interpreted in `timezone`) fires strictly after `after`.
 * Time always comes from the caller (decision 17): `after` is expected to be `now()` read from
 * Postgres, never `Date.now()`, so a cluster of processes agrees on "next" the same way `sleep`
 * and `run_after` already do elsewhere in this codebase.
 *
 * Throws `ConfigError` for a syntactically invalid pattern OR an unrecognized timezone --
 * `cron-parser` itself rejects both (verified against the installed 5.10.1 `.d.ts` and runtime
 * behavior, not assumed from memory): a malformed field value throws a plain `Error` from deep in
 * its constraint checking, and a bad IANA zone throws once `next()`/date construction actually
 * tries to interpret a timestamp in it. Both are wrapped here into one actionable `ConfigError`.
 */
export function nextOccurrence(pattern: string, timezone: string | undefined, after: Date): Date {
	try {
		const expr = CronExpressionParser.parse(pattern, {
			currentDate: after,
			...(timezone !== undefined ? { tz: timezone } : {}),
		});
		return expr.next().toDate();
	} catch (error) {
		const reason = error instanceof Error ? error.message : String(error);
		throw new ConfigError(
			`invalid cron pattern "${pattern}"${timezone ? ` (timezone ${timezone})` : ""}: ${reason}`
		);
	}
}

/** The Postgres-backend `-system` job id for a schedule's recurring tick (mirrors decision 7: no `:` inside the id itself, the prefix supplies the only one). */
export function scheduleTickJobId(scheduleId: string): string {
	return `${SCHEDULE_TICK_PREFIX}${scheduleId}`;
}

/** Inverse of {@link scheduleTickJobId}; `undefined` for any job id that isn't a schedule tick (e.g. `wp-<id>-expire`). */
export function scheduleIdOfJob(jobId: string): string | undefined {
	return jobId.startsWith(SCHEDULE_TICK_PREFIX)
		? jobId.slice(SCHEDULE_TICK_PREFIX.length)
		: undefined;
}
