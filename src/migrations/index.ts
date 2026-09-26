import initSql from "./001_init.sql";
import schedulesSql from "./002_schedules.sql";
import runsCreatedIdxSql from "./003_runs_created_idx.sql";
import runProgressSql from "./004_run_progress.sql";
import debounceConcurrencySql from "./005_debounce_concurrency.sql";
import outboxSql from "./006_outbox.sql";
import queueStateSql from "./007_queue_state.sql";

/** One embedded, versioned migration (decision 18: no reading `.sql` files from disk at runtime). */
export interface Migration {
	readonly version: number;
	readonly name: string;
	readonly sql: string;
}

/** Ordered list of embedded migrations, applied in order by `migrate()` (src/migrations/migrate.ts). */
export const MIGRATIONS: readonly Migration[] = [
	{ version: 1, name: "init", sql: initSql },
	{ version: 2, name: "schedules", sql: schedulesSql },
	{ version: 3, name: "runs_created_idx", sql: runsCreatedIdxSql },
	{ version: 4, name: "run_progress", sql: runProgressSql },
	{ version: 5, name: "debounce_concurrency", sql: debounceConcurrencySql },
	{ version: 6, name: "outbox", sql: outboxSql },
	{ version: 7, name: "queue_state", sql: queueStateSql },
];
