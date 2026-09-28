/**
 * `system_jobs` (migration 009): the `-system` queue of the Postgres transport (decision 28).
 * A claim sets `locked_until = now() + lockMs` and a fresh `locked_by` token; every write a
 * worker makes afterwards is fenced by `job_id AND locked_by`, so a worker whose lock expired
 * (and was re-claimed by another) never completes, delays or reschedules the row. Times always
 * come from the database (decision 17).
 */
import { MinitriggerError } from "../../errors.js";
import type { Json, ScheduleTickData, SystemJobRecord, SystemJobStore } from "../types.js";
import { boundedClaim } from "./claim.js";
import { type Db, direct, type SqlContext, type SqlExecutor, wrap } from "./db.js";
import { DELAY_UNTIL_MIN_RUN_AFTER, jsonParam, nowPlusMs } from "./resolve.js";

interface SystemJobRow {
	job_id: string;
	name: string;
	data: Json;
	run_after: Date;
	attempts: number;
	max_attempts: number;
	locked_by: string | null;
}

const COLUMNS = "job_id, name, data, run_after, attempts, max_attempts, locked_by";
const QUALIFIED_COLUMNS = COLUMNS.split(",")
	.map(column => `j.${column.trim()}`)
	.join(", ");

/** Name of the cron tick job (same as the BullMQ `jobTemplate.name`). */
const SCHEDULE_TICK_NAME = "schedule-tick";
/** Attempts of a `schedule-tick` (the exception to decision 4, as in the BullMQ transport). */
const SCHEDULE_TICK_MAX_ATTEMPTS = 5;

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function mapSystemJob(row: SystemJobRow): SystemJobRecord {
	return {
		jobId: row.job_id,
		name: row.name,
		data: row.data,
		runAfter: row.run_after,
		attempts: row.attempts,
		maxAttempts: row.max_attempts,
		lockedBy: row.locked_by,
	};
}

export function createSystemJobStore(exec: SqlExecutor, ctx: SqlContext): SystemJobStore {
	const table = ctx.t("system_jobs");
	const db = () => direct(exec);

	/**
	 * A token that is not a UUID can never match `locked_by` (a uuid column); checking here avoids
	 * the `invalid input syntax for type uuid` error and reports it as a lost lock.
	 */
	function isToken(lockedBy: string): boolean {
		return UUID_PATTERN.test(lockedBy);
	}

	return {
		async upsert({ jobId, name, data, delayMs, maxAttempts }) {
			await db().query(
				`INSERT INTO ${table} (job_id, name, data, run_after, max_attempts)
				 VALUES ($1, $2, $3::jsonb, ${nowPlusMs("$4")}, $5::int)
				 ON CONFLICT (job_id) DO NOTHING`,
				[jobId, name, jsonParam(data ?? {}), delayMs ?? 0, maxAttempts ?? 1]
			);
		},

		async upsertScheduleTick({
			jobId,
			data,
			runAt,
		}: {
			jobId: string;
			data: ScheduleTickData;
			runAt: Date;
		}) {
			// Same pattern (cron + timezone) → no-op, so a restart never skips a due tick that was
			// not processed yet (as BullMQ's upsertJobScheduler). A changed pattern rewrites the row
			// and releases the lock: the worker still processing the old tick loses its fencing.
			const { rows } = await db().query<{ inserted: boolean }>(
				`INSERT INTO ${table} (job_id, name, data, run_after, max_attempts)
				 VALUES ($1, $2, $3::jsonb, $4::timestamptz, $5::int)
				 ON CONFLICT (job_id) DO UPDATE
				   SET name = EXCLUDED.name, data = EXCLUDED.data, run_after = EXCLUDED.run_after,
				       max_attempts = EXCLUDED.max_attempts, attempts = 0,
				       locked_until = NULL, locked_by = NULL
				   WHERE ${table}.data->>'cron' IS DISTINCT FROM EXCLUDED.data->>'cron'
				      OR ${table}.data->>'timezone' IS DISTINCT FROM EXCLUDED.data->>'timezone'
				 RETURNING (xmax = 0) AS inserted`,
				[
					jobId,
					SCHEDULE_TICK_NAME,
					jsonParam(data as unknown as Json),
					runAt,
					SCHEDULE_TICK_MAX_ATTEMPTS,
				]
			);
			const row = rows[0];
			if (!row) return "unchanged";
			return row.inserted ? "created" : "updated";
		},

		async claim(limit, lockMs, remainingMs) {
			if (limit <= 0) return [];
			// Same bounded transaction as the run claim (review 5, A6): once `stop()` set the drain
			// deadline, a claim never commits after the drain gave up on it. Hence
			// `statement_timestamp()`, not `now()` (frozen at that BEGIN): a job that came due after
			// the BEGIN would be skipped, and the lock would start in the past.
			const statement = (q: Db) =>
				q.query<SystemJobRow>(
					`WITH picked AS (
				   SELECT job_id FROM ${table}
				   WHERE run_after <= statement_timestamp()
				     AND (locked_until IS NULL OR locked_until <= statement_timestamp())
				   ORDER BY run_after
				   LIMIT $1::int
				   FOR UPDATE SKIP LOCKED
				 )
				 UPDATE ${table} j
				 SET locked_until = statement_timestamp() + $2::float8 * interval '1 millisecond',
				     locked_by = gen_random_uuid()
				 FROM picked WHERE j.job_id = picked.job_id
				 RETURNING ${QUALIFIED_COLUMNS}`,
					[limit, lockMs]
				);
			const rows =
				exec.kind === "tx"
					? (await statement(wrap(exec.client))).rows
					: ((await boundedClaim(exec.pool, remainingMs, async (q, bound) => {
							await bound();
							return (await statement(q)).rows;
						})) ?? []);
			// UPDATE ... RETURNING does not guarantee order: re-sort like the SELECT.
			return rows.map(mapSystemJob).sort((a, b) => a.runAfter.getTime() - b.runAfter.getTime());
		},

		async complete(jobId, lockedBy) {
			if (!isToken(lockedBy)) return false;
			const { rowCount } = await db().query(
				`DELETE FROM ${table} WHERE job_id = $1 AND locked_by = $2::uuid`,
				[jobId, lockedBy]
			);
			return rowCount > 0;
		},

		async delay(jobId, lockedBy, delayMs) {
			if (!isToken(lockedBy)) return false;
			const { rowCount } = await db().query(
				`UPDATE ${table}
				 SET run_after = ${nowPlusMs("$3")}, locked_until = NULL, locked_by = NULL
				 WHERE job_id = $1 AND locked_by = $2::uuid`,
				[jobId, lockedBy, delayMs]
			);
			return rowCount > 0;
		},

		async fail(jobId, lockedBy, backoffMs) {
			if (!isToken(lockedBy)) return "lost";
			// Two CTEs with disjoint predicates over the same snapshot: a DELETE in one CTE on a row
			// another CTE updated would be silently ignored.
			const { rows } = await db().query<{ gone: number; bumped: number }>(
				`WITH gone AS (
				   DELETE FROM ${table}
				   WHERE job_id = $1 AND locked_by = $2::uuid AND attempts + 1 >= max_attempts
				   RETURNING 1
				 ),
				 bumped AS (
				   UPDATE ${table}
				   SET attempts = attempts + 1, run_after = ${nowPlusMs("$3")},
				       locked_until = NULL, locked_by = NULL
				   WHERE job_id = $1 AND locked_by = $2::uuid AND attempts + 1 < max_attempts
				   RETURNING 1
				 )
				 SELECT (SELECT count(*) FROM gone)::int AS gone,
				        (SELECT count(*) FROM bumped)::int AS bumped`,
				[jobId, lockedBy, backoffMs]
			);
			const row = rows[0];
			if (row && row.gone > 0) return "exhausted";
			if (row && row.bumped > 0) return "retried";
			return "lost";
		},

		async rescheduleTick(jobId, lockedBy, next) {
			if (!isToken(lockedBy)) return false;
			const { rowCount } = await db().query(
				`UPDATE ${table}
				 SET run_after = $3::timestamptz, attempts = 0,
				     data = jsonb_set(data, '{prevMillis}', to_jsonb($4::float8)),
				     locked_until = NULL, locked_by = NULL
				 WHERE job_id = $1 AND locked_by = $2::uuid`,
				[jobId, lockedBy, next.runAt, next.prevMillis]
			);
			return rowCount > 0;
		},

		async remove(jobId, options) {
			// Like BullMQ, removal never takes an active (locked) job, unless the caller is removing
			// a scheduler: the running tick then finishes and its fenced write matches nothing.
			const { rowCount } = options?.evenIfLocked
				? await db().query(`DELETE FROM ${table} WHERE job_id = $1`, [jobId])
				: await db().query(
						`DELETE FROM ${table}
						 WHERE job_id = $1 AND (locked_until IS NULL OR locked_until <= now())`,
						[jobId]
					);
			return rowCount > 0;
		},

		async get(jobId) {
			const { rows } = await db().query<SystemJobRow>(
				`SELECT ${COLUMNS} FROM ${table} WHERE job_id = $1`,
				[jobId]
			);
			const row = rows[0];
			return row ? mapSystemJob(row) : null;
		},

		async listByName(name) {
			const { rows } = await db().query<SystemJobRow>(
				`SELECT ${COLUMNS} FROM ${table} WHERE name = $1 ORDER BY run_after, job_id`,
				[name]
			);
			return rows.map(mapSystemJob);
		},

		async nextDueInMs() {
			// Only future `run_after` (indexed). A job whose lock expires later is found by the
			// worker's regular poll (`polling.intervalMs`), not by this wake-up hint.
			// The delay is computed in SQL (decision 17): the app clock may drift from the DB's.
			const { rows } = await db().query<{ ms: number | null }>(
				`SELECT ${DELAY_UNTIL_MIN_RUN_AFTER} AS ms FROM ${table} WHERE run_after > now()`
			);
			return rows[0]?.ms ?? null;
		},

		async now() {
			const { rows } = await db().query<{ now: Date }>("SELECT now() AS now");
			const row = rows[0];
			if (!row) throw new MinitriggerError("SELECT now() returned no row");
			return row.now;
		},
	};
}
