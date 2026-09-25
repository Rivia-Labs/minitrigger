import { ConfigError } from "../../errors.js";
import type {
	Json,
	ListSchedulesFilter,
	NewSchedule,
	ScheduleRecord,
	ScheduleSource,
	ScheduleStore,
	UpsertScheduleResult,
} from "../types.js";
import { atomic, direct, type SqlContext, type SqlExecutor } from "./db.js";
import { jsonParam } from "./resolve.js";

/** Same shape as `TASK_ID_PATTERN` in `src/utils/identifiers.ts`: schedule ids end up embedded in
 * the idempotency key `__schedule:${id}:${ts}` and are used verbatim as BullMQ job scheduler ids
 * (decision 7's spirit -- no `:`). Kept local to this file rather than imported so the error
 * message stays specific to schedules. */
const SCHEDULE_ID_PATTERN = /^[a-zA-Z0-9_.-]{1,128}$/;

function validateScheduleId(id: string): string {
	if (!SCHEDULE_ID_PATTERN.test(id)) {
		throw new ConfigError(
			`schedule id "${id}" is invalid: must match ${SCHEDULE_ID_PATTERN.source}`
		);
	}
	return id;
}

const SCHEDULE_COLUMNS = "id, task_id, cron, timezone, payload, source, created_at, updated_at";

interface ScheduleRow {
	id: string;
	task_id: string;
	cron: string;
	timezone: string | null;
	payload: Json | null;
	source: ScheduleSource;
	created_at: Date;
	updated_at: Date;
}

function mapSchedule(row: ScheduleRow): ScheduleRecord {
	return {
		id: row.id,
		taskId: row.task_id,
		cron: row.cron,
		timezone: row.timezone,
		payload: row.payload,
		source: row.source,
		createdAt: row.created_at,
		updatedAt: row.updated_at,
	};
}

export function createScheduleStore(exec: SqlExecutor, ctx: SqlContext): ScheduleStore {
	const schedules = ctx.t("schedules");

	async function listSchedules(filter: ListSchedulesFilter): Promise<ScheduleRecord[]> {
		const params: unknown[] = [];
		let where = "";
		if (filter.source !== undefined) {
			params.push(filter.source);
			where = `WHERE source = $${params.length}`;
		}
		const { rows } = await direct(exec).query<ScheduleRow>(
			`SELECT ${SCHEDULE_COLUMNS} FROM ${schedules} ${where} ORDER BY id`,
			params
		);
		return rows.map(mapSchedule);
	}

	return {
		async upsert(input: NewSchedule): Promise<UpsertScheduleResult> {
			validateScheduleId(input.id);
			return atomic(exec, async db => {
				// Lock any existing row first so a concurrent upsert of the same id can't read a
				// stale "previous" cron/timezone while this one is still in flight.
				const existing = await db.query<{ cron: string; timezone: string | null }>(
					`SELECT cron, timezone FROM ${schedules} WHERE id = $1 FOR UPDATE`,
					[input.id]
				);
				const prev = existing.rows[0];

				const { rows } = await db.query<ScheduleRow>(
					`INSERT INTO ${schedules} (id, task_id, cron, timezone, payload, source, created_at, updated_at)
					 VALUES ($1, $2, $3, $4, $5::jsonb, $6, now(), now())
					 ON CONFLICT (id) DO UPDATE
					   SET task_id = EXCLUDED.task_id,
					       cron = EXCLUDED.cron,
					       timezone = EXCLUDED.timezone,
					       payload = EXCLUDED.payload,
					       source = EXCLUDED.source,
					       updated_at = now()
					 RETURNING ${SCHEDULE_COLUMNS}`,
					[
						input.id,
						input.taskId,
						input.cron,
						input.timezone,
						jsonParam(input.payload),
						input.source,
					]
				);
				const row = rows[0];
				if (!row) throw new Error("schedules upsert returned no row");

				return {
					schedule: mapSchedule(row),
					created: !prev,
					cronChanged: !prev || prev.cron !== input.cron,
					timezoneChanged: !prev || prev.timezone !== input.timezone,
				};
			});
		},

		async delete(id: string): Promise<boolean> {
			const { rowCount } = await direct(exec).query(`DELETE FROM ${schedules} WHERE id = $1`, [id]);
			return rowCount > 0;
		},

		async get(id: string): Promise<ScheduleRecord | null> {
			const { rows } = await direct(exec).query<ScheduleRow>(
				`SELECT ${SCHEDULE_COLUMNS} FROM ${schedules} WHERE id = $1`,
				[id]
			);
			const row = rows[0];
			return row ? mapSchedule(row) : null;
		},

		async list(filter: ListSchedulesFilter = {}): Promise<ScheduleRecord[]> {
			return listSchedules(filter);
		},

		listDeclarative(): Promise<ScheduleRecord[]> {
			return listSchedules({ source: "declarative" });
		},

		async deleteDeclarativeExcept(ids: readonly string[]): Promise<number> {
			const { rowCount } = await direct(exec).query(
				`DELETE FROM ${schedules}
				 WHERE source = 'declarative' AND NOT (id = ANY($1::text[]))`,
				[[...ids]]
			);
			return rowCount;
		},
	};
}
