import type { LogEntry, LogLevel, LogStore, NewLogEntry } from "../types.js";
import { direct, type SqlContext, type SqlExecutor } from "./db.js";

interface LogRow {
	id: string;
	run_id: string;
	level: LogLevel;
	message: string;
	data: LogEntry["data"];
	ts: Date;
}

export function createLogStore(exec: SqlExecutor, ctx: SqlContext): LogStore {
	const db = direct(exec);

	return {
		async insertBatch(entries: readonly NewLogEntry[]): Promise<void> {
			if (entries.length === 0) return;

			const runIds = entries.map(entry => entry.runId);
			const levels = entries.map(entry => entry.level);
			const messages = entries.map(entry => entry.message);
			// Pre-stringify: `data` may be any `Json` value (including a bare string or number), and
			// pg's own array serialization only JSON-encodes plain objects/arrays, not primitives --
			// a bare string element would be sent as unquoted text and fail the `::jsonb[]` cast.
			// Stringifying every element ourselves makes the array uniformly `text[]`-shaped input
			// that `::jsonb[]` always parses correctly; `null` stays a real SQL NULL.
			const dataArr = entries.map(entry =>
				entry.data === null ? null : JSON.stringify(entry.data)
			);
			const tsArr = entries.map(entry => entry.ts ?? null);

			// ONE statement via `unnest`, never multi-row VALUES (Phase 2 / Phase 7 requirement).
			await db.query(
				`INSERT INTO ${ctx.t("run_logs")} (run_id, level, message, data, ts)
				 SELECT entry.run_id, entry.level, entry.message, entry.data, coalesce(entry.ts, now())
				 FROM unnest($1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::timestamptz[])
				   AS entry(run_id, level, message, data, ts)`,
				[runIds, levels, messages, dataArr, tsArr]
			);
		},

		async list(runId: string, opts: { afterId?: string; limit: number }): Promise<LogEntry[]> {
			const params: unknown[] = [runId];
			let where = "run_id = $1";
			if (opts.afterId !== undefined) {
				params.push(opts.afterId);
				where += ` AND id > $${params.length}::bigint`;
			}
			params.push(opts.limit);

			const { rows } = await db.query<LogRow>(
				`SELECT id, run_id, level, message, data, ts
				 FROM ${ctx.t("run_logs")}
				 WHERE ${where}
				 ORDER BY id ASC
				 LIMIT $${params.length}`,
				params
			);

			return rows.map(row => ({
				id: row.id,
				runId: row.run_id,
				level: row.level,
				message: row.message,
				data: row.data,
				ts: row.ts,
			}));
		},
	};
}
