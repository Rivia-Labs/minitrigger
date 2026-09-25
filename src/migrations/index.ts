import initSql from "./001_init.sql";
import schedulesSql from "./002_schedules.sql";

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
];
