import initSql from "./001_init.sql";
import schedulesSql from "./002_schedules.sql";
import runsCreatedIdxSql from "./003_runs_created_idx.sql";
import runProgressSql from "./004_run_progress.sql";

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
];
