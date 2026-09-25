/**
 * Standalone startup environment checks (SPEC.md Phase 9, decision 22).
 *
 * `checkEnvironment` runs three independent checks -- Postgres server version, Redis
 * `maxmemory-policy`, and pending migrations -- and applies decision 22 uniformly to all three:
 *
 * - A check whose *command* is unavailable (blocked `CONFIG GET` on managed Redis, a `SHOW`
 *   blocked by a restrictive proxy/permission set, a transient connection error while reading
 *   pending migrations) is a **warning**: logged via `logger.warn` and returned in the report,
 *   never thrown.
 * - A check that *answers* and the answer is definitively wrong (Postgres too old, eviction
 *   policy not `noeviction`, migrations pending while not auto-applied) **throws**
 *   {@link ConfigError} with an actionable message (what's wrong, how to fix it).
 *
 * Called by `engine.start()` (see {@link checkEnvironment}); it does not import `./engine.js`.
 */

import type { Pool } from "pg";
import type { EngineLogger } from "./config.js";
import { ConfigError } from "./errors.js";
import { pendingMigrations } from "./migrations/migrate.js";
import { checkRedisEvictionPolicy, type RedisConfigClient } from "./queue/redis-check.js";

/**
 * Minimal shape `checkPostgresVersion`/`checkPendingMigrations` need from a `pg.Pool` -- easy to
 * satisfy with a fake in unit tests, and a real `Pool`/`PoolClient` already structurally matches
 * it (its `query` overloads include this shape).
 */
export interface PgQueryClient {
	query<T extends Record<string, unknown> = never>(
		text: string,
		values?: readonly unknown[]
	): Promise<{ rows: T[] }>;
}

/**
 * Postgres 13 is the floor: `SKIP LOCKED` (used by the reconciler/retention, decision 6/17)
 * needs 9.5+, and nothing in the schema/queries needs anything from 14-16 specifically -- 13 is
 * chosen as a conservative "still commonly available on managed providers" baseline rather than
 * pinned to the 16 used by `docker-compose.yml` for local dev/CI. Not a SPEC-fixed number (SPEC
 * §2 doesn't set one): adjust here if the project wants a stricter/looser floor.
 */
export const MIN_POSTGRES_VERSION_NUM = 130000;

export type EnvCheckName = "postgres-version" | "redis-eviction-policy" | "pending-migrations";

export interface EnvCheckWarning {
	readonly check: EnvCheckName;
	readonly message: string;
}

export interface EnvCheckReport {
	/** Checks that ran but couldn't get a definitive answer (decision 22) -- never a reason to fail. */
	readonly warnings: readonly EnvCheckWarning[];
}

export interface CheckEnvironmentOptions {
	pool: PgQueryClient;
	redis: RedisConfigClient;
	/** Postgres schema pending migrations are checked against. */
	schema: string;
	/** Receives one `warn` call per {@link EnvCheckWarning}. Omit to only get them in the returned report. */
	logger?: EngineLogger;
	/**
	 * Whether pending migrations will be applied automatically (e.g. the caller runs `migrate()`
	 * right after this check, or on every `start()`). When `true`, pending migrations are not an
	 * error. Default `false` (migrations must already be applied, matching `engine.migrate()`
	 * being an explicit call today -- see the module doc comment).
	 */
	autoMigrate?: boolean;
	/** Overrides {@link MIN_POSTGRES_VERSION_NUM}. */
	minPostgresVersionNum?: number;
}

/**
 * Runs every environment check and returns the accumulated warnings.
 *
 * Errors from the underlying checks (see decision 22 above) are collected across *all* checks
 * before anything is thrown, so a caller who fixes Postgres first, reruns, and hits the Redis
 * problem next isn't stuck debugging one problem at a time -- a single {@link ConfigError} lists
 * every definitive failure found in this call. Warnings are still surfaced (via `logger.warn` and
 * the report) for checks that did *not* fail, even when another check throws.
 *
 * Called by `engine.start()` before any worker starts, with `autoMigrate: false` (pending
 * migrations fail fast and tell the user to call `engine.migrate()`). Also exported for callers
 * that want to run the checks themselves (e.g. a readiness probe).
 */
export async function checkEnvironment(options: CheckEnvironmentOptions): Promise<EnvCheckReport> {
	const { pool, redis, schema, logger, autoMigrate = false, minPostgresVersionNum } = options;

	const checks: ReadonlyArray<{
		readonly check: EnvCheckName;
		readonly run: () => Promise<string | undefined>;
	}> = [
		{
			check: "postgres-version",
			run: () => checkPostgresVersion(pool, minPostgresVersionNum),
		},
		{ check: "redis-eviction-policy", run: () => checkRedisEvictionPolicy(redis) },
		{
			check: "pending-migrations",
			run: () => checkPendingMigrations(pool, schema, { autoMigrate }),
		},
	];

	const warnings: EnvCheckWarning[] = [];
	const errors: ConfigError[] = [];

	for (const { check, run } of checks) {
		try {
			const warning = await run();
			if (warning !== undefined) {
				warnings.push({ check, message: warning });
				logger?.warn({ check }, warning);
			}
		} catch (error) {
			if (!(error instanceof ConfigError)) throw error; // not a decision-22 case: a real bug
			errors.push(error);
		}
	}

	const [onlyError] = errors;
	if (errors.length > 0) {
		throw new ConfigError(
			errors.length === 1 && onlyError !== undefined
				? onlyError.message
				: `environment check failed with ${errors.length} problems:\n` +
						errors.map((error, index) => `  ${index + 1}. ${error.message}`).join("\n")
		);
	}

	return { warnings };
}

/**
 * Decision 22: `SHOW server_version_num` failing outright (e.g. blocked by a restrictive
 * proxy/permission set in front of a managed Postgres) is a warning, not a failure -- the check
 * simply couldn't run. A value that *is* read and is below `minVersionNum` throws
 * {@link ConfigError}.
 */
export async function checkPostgresVersion(
	pool: PgQueryClient,
	minVersionNum: number = MIN_POSTGRES_VERSION_NUM
): Promise<string | undefined> {
	let versionNum: number | undefined;
	try {
		const result = await pool.query<{ server_version_num: string }>("SHOW server_version_num");
		const raw = result.rows[0]?.server_version_num;
		versionNum = raw === undefined ? undefined : Number(raw);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		return (
			`could not read Postgres's "server_version_num" ("SHOW server_version_num" is unavailable ` +
			`or blocked, e.g. behind some connection poolers/proxies) -- skipping this check: ${message}`
		);
	}

	if (versionNum === undefined || !Number.isFinite(versionNum)) {
		return 'Postgres replied to "SHOW server_version_num" with an unexpected value -- skipping this check';
	}

	if (versionNum < minVersionNum) {
		throw new ConfigError(
			`Postgres server version ${formatVersionNum(versionNum)} is older than the minimum ` +
				`minitrigger requires (${formatVersionNum(minVersionNum)}). Upgrade the Postgres server ` +
				`you're connecting to (or point "postgres" at a different one) to at least ` +
				`${formatVersionNum(minVersionNum)}.`
		);
	}
	return undefined;
}

/**
 * Decision 22: a `pendingMigrations` call that fails outright (connection/permission error while
 * reading `schema_migrations`) is a warning. Pending migrations that *were* read successfully are
 * an error unless `autoMigrate` is set, since a stale schema would otherwise fail confusingly
 * later (missing columns/tables) instead of with one clear message up front.
 */
export async function checkPendingMigrations(
	pool: PgQueryClient,
	schema: string,
	options: { autoMigrate?: boolean } = {}
): Promise<string | undefined> {
	let pending: number[];
	try {
		// `pendingMigrations` only ever calls `pool.query(...)` (see `src/migrations/migrate.ts`),
		// so a `PgQueryClient` satisfies everything it actually uses at runtime; the cast below
		// only widens the *type* to the `Pool` its signature declares.
		pending = await pendingMigrations(pool as unknown as Pool, schema);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		return `could not check for pending migrations in schema "${schema}" -- skipping this check: ${message}`;
	}

	if (pending.length === 0) return undefined;
	if (options.autoMigrate === true) return undefined;

	throw new ConfigError(
		`schema "${schema}" has ${pending.length} pending migration(s) not yet applied: ` +
			`${pending.join(", ")}. Call migrate(pool, ${JSON.stringify(schema)}) (or ` +
			`engine.migrate()) before starting the engine, or pass { autoMigrate: true } to this ` +
			`check if migrations are applied automatically at startup.`
	);
}

/** `server_version_num` encodes major*10000 + minor for Postgres >= 10 (e.g. 160001 -> "16.1"). */
function formatVersionNum(versionNum: number): string {
	const major = Math.floor(versionNum / 10_000);
	const minor = versionNum % 10_000;
	return `${major}.${minor}`;
}
