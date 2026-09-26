import type { QueueStateRecord, QueueStateStore } from "../types.js";
import { direct, type SqlContext, type SqlExecutor } from "./db.js";

interface QueueStateRow {
	queue: string;
	paused: boolean;
	updated_at: Date;
}

function mapQueueState(row: QueueStateRow): QueueStateRecord {
	return { queue: row.queue, paused: row.paused, updatedAt: row.updated_at };
}

/** Decision 27: `queue_state` (migration 007). */
export function createQueueStateStore(exec: SqlExecutor, ctx: SqlContext): QueueStateStore {
	const table = ctx.t("queue_state");
	return {
		async setPaused(queue, paused) {
			// One statement: the previous value is read from the conflicting row's pre-update
			// version (`q` in DO UPDATE refers to it), never via a separate SELECT.
			const { rows } = await direct(exec).query<QueueStateRow & { was_paused: boolean | null }>(
				`WITH prev AS (SELECT paused FROM ${table} WHERE queue = $1)
				 INSERT INTO ${table} AS q (queue, paused, updated_at) VALUES ($1, $2, now())
				 ON CONFLICT (queue) DO UPDATE SET paused = EXCLUDED.paused, updated_at = now()
				 RETURNING q.queue, q.paused, q.updated_at, (SELECT paused FROM prev) AS was_paused`,
				[queue, paused]
			);
			const row = rows[0];
			if (!row) throw new Error("queue_state upsert returned no row");
			return {
				record: mapQueueState(row),
				changed: (row.was_paused ?? false) !== paused,
			};
		},
		async get(queue) {
			const { rows } = await direct(exec).query<QueueStateRow>(
				`SELECT queue, paused, updated_at FROM ${table} WHERE queue = $1`,
				[queue]
			);
			const row = rows[0];
			return row ? mapQueueState(row) : null;
		},
		async list() {
			const { rows } = await direct(exec).query<QueueStateRow>(
				`SELECT queue, paused, updated_at FROM ${table} ORDER BY queue`
			);
			return rows.map(mapQueueState);
		},
	};
}
