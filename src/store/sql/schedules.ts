import { ConfigError } from "../../errors.js";
import type {
	Json,
	ListSchedulesFilter,
	NewSchedule,
	ScheduleRecord,
	ScheduleSource,
	ScheduleStore,
	UpsertScheduleOptions,
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
		async upsert(
			input: NewSchedule,
			options: UpsertScheduleOptions = {}
		): Promise<UpsertScheduleResult> {
			validateScheduleId(input.id);
			return atomic(exec, async db => {
				// Audit item (b): a missing row can't be locked, so two concurrent upserts of a NEW
				// id both see "no row". The insert is therefore `ON CONFLICT DO NOTHING`: when a
				// concurrent insert won (this statement waits for it and then inserts nothing), loop
				// and take the existing-row path under `FOR UPDATE`, where the source guard and the
				// cron/timezone comparison see the committed row. Never `xmax` (see the contract).
				for (let attempt = 0; attempt < 3; attempt++) {
					const existing = await db.query<ScheduleRow>(
						`SELECT ${SCHEDULE_COLUMNS} FROM ${schedules} WHERE id = $1 FOR UPDATE`,
						[input.id]
					);
					const prev = existing.rows[0];

					if (!prev) {
						const { rows } = await db.query<ScheduleRow>(
							`INSERT INTO ${schedules}
							   (id, task_id, cron, timezone, payload, source, created_at, updated_at)
							 VALUES ($1, $2, $3, $4, $5::jsonb, $6, now(), now())
							 ON CONFLICT (id) DO NOTHING
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
						if (!row) continue; // A concurrent upsert created it first: re-read it locked.
						return {
							schedule: mapSchedule(row),
							created: true,
							cronChanged: true,
							timezoneChanged: true,
						};
					}

					if (options.requireSourceMatch && prev.source !== input.source) {
						// Fix 4: a declarative sync must not clobber an `api` row with the same id and
						// vice-versa. Still holding the row lock, writing nothing: report the conflict
						// and let the caller (`src/schedules.ts`) decide.
						return {
							schedule: mapSchedule(prev),
							created: false,
							cronChanged: false,
							timezoneChanged: false,
							conflict: { existingSource: prev.source },
						};
					}

					const { rows } = await db.query<ScheduleRow>(
						`UPDATE ${schedules}
						 SET task_id = $2, cron = $3, timezone = $4, payload = $5::jsonb, source = $6,
						     updated_at = now()
						 WHERE id = $1
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
					if (!row) throw new Error("schedules upsert: row disappeared under FOR UPDATE");
					return {
						schedule: mapSchedule(row),
						created: false,
						cronChanged: prev.cron !== input.cron,
						timezoneChanged: prev.timezone !== input.timezone,
					};
				}
				throw new Error(`schedules upsert: schedule "${input.id}" kept appearing and vanishing`);
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
