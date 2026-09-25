import type { LogEntry, LogStore } from "../types.js";
import type { Exec } from "./resolve.js";
import { cloneJson, fkViolation, type LogRow, pgText, sqlLimit, toJsonb, uuid } from "./state.js";

export function createLogStore(exec: Exec): LogStore {
	return {
		async insertBatch(entries) {
			if (entries.length === 0) return;
			await exec.run(tx => {
				const rows = entries.map(entry => {
					const runId = uuid(entry.runId);
					if (!tx.db.runs.has(runId)) throw fkViolation("run_logs", "run_logs_run_id_fkey");
					return { entry, runId };
				});
				for (const { entry, runId } of rows) {
					// bigserial: consumed even if the transaction later rolls back.
					tx.db.logSeq += 1n;
					const row: LogRow = {
						id: tx.db.logSeq,
						runId,
						level: entry.level,
						message: pgText(entry.message),
						data: entry.data === null ? null : toJsonb(entry.data),
						ts: entry.ts ? entry.ts.getTime() : tx.now,
					};
					tx.set(tx.db.logs, row.id, row);
				}
			});
		},

		list(runId, opts) {
			return exec.run(tx => {
				const id = uuid(runId);
				const after = opts.afterId === undefined ? null : BigInt(opts.afterId);
				const rows = [...tx.db.logs.values()]
					.filter(l => l.runId === id && (after === null || l.id > after))
					.sort((a, b) => (a.id < b.id ? -1 : 1))
					.slice(0, sqlLimit(opts.limit));
				return rows.map(
					(l): LogEntry => ({
						id: String(l.id),
						runId: l.runId,
						level: l.level,
						message: l.message,
						data: cloneJson(l.data),
						ts: new Date(l.ts),
					})
				);
			});
		},
	};
}
