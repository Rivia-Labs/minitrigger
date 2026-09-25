import { ConfigError } from "../../errors.js";
import type {
	ListSchedulesFilter,
	ScheduleRecord,
	ScheduleStore,
	UpsertScheduleResult,
} from "../types.js";
import type { Exec } from "./resolve.js";
import { cloneJson, type MemTx, type ScheduleRow, toJsonb } from "./state.js";

/** Same rule as the SQL store (`^[a-zA-Z0-9_.-]{1,128}$`, no `:`). */
const SCHEDULE_ID_PATTERN = /^[a-zA-Z0-9_.-]{1,128}$/;

function mapSchedule(row: ScheduleRow): ScheduleRecord {
	return {
		id: row.id,
		taskId: row.taskId,
		cron: row.cron,
		timezone: row.timezone,
		payload: cloneJson(row.payload),
		source: row.source,
		createdAt: new Date(row.createdAt),
		updatedAt: new Date(row.updatedAt),
	};
}

/** `ORDER BY id`: code-unit order (the SQL store uses the database collation). */
function listSchedules(tx: MemTx, filter: ListSchedulesFilter): ScheduleRecord[] {
	return [...tx.db.schedules.values()]
		.filter(s => filter.source === undefined || s.source === filter.source)
		.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
		.map(mapSchedule);
}

export function createScheduleStore(exec: Exec): ScheduleStore {
	return {
		async upsert(input): Promise<UpsertScheduleResult> {
			if (!SCHEDULE_ID_PATTERN.test(input.id)) {
				throw new ConfigError(
					`schedule id "${input.id}" is invalid: must match ${SCHEDULE_ID_PATTERN.source}`
				);
			}
			return exec.run(tx => {
				const prev = tx.db.schedules.get(input.id);
				const row: ScheduleRow = {
					id: input.id,
					taskId: input.taskId,
					cron: input.cron,
					timezone: input.timezone,
					payload: toJsonb(input.payload),
					source: input.source,
					createdAt: prev ? prev.createdAt : tx.now,
					updatedAt: tx.now,
				};
				tx.set(tx.db.schedules, input.id, row);
				return {
					schedule: mapSchedule(row),
					created: !prev,
					cronChanged: !prev || prev.cron !== input.cron,
					timezoneChanged: !prev || prev.timezone !== input.timezone,
				};
			});
		},

		delete(id) {
			return exec.run(tx => tx.delete(tx.db.schedules, id));
		},

		get(id) {
			return exec.run(tx => {
				const row = tx.db.schedules.get(id);
				return row ? mapSchedule(row) : null;
			});
		},

		list(filter = {}) {
			return exec.run(tx => listSchedules(tx, filter));
		},

		listDeclarative() {
			return exec.run(tx => listSchedules(tx, { source: "declarative" }));
		},

		deleteDeclarativeExcept(ids) {
			return exec.run(tx => {
				const keep = new Set(ids);
				let n = 0;
				for (const s of [...tx.db.schedules.values()]) {
					if (s.source === "declarative" && !keep.has(s.id)) {
						tx.delete(tx.db.schedules, s.id);
						n++;
					}
				}
				return n;
			});
		},
	};
}
