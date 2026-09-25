/**
 * Engine configuration (SPEC.md §3, Phase 3): the public {@link EngineConfig} accepted by
 * `createEngine`, and {@link resolveConfig}, which validates it and fills in every default so the
 * rest of the engine only ever reads a fully-defaulted {@link ResolvedConfig}.
 */

import type { Redis } from "ioredis";
import type { Pool } from "pg";
import { ConfigError } from "./errors.js";
import type { LogLevel } from "./store/types.js";
import type { AnyTask } from "./task.js";
import { type Duration, parseDuration } from "./utils/duration.js";
import { describeOffendingValue } from "./utils/error-format.js";
import { validateSqlIdentifier } from "./utils/identifiers.js";

/**
 * A log method compatible with both `console` (`(...data) => void`) and pino
 * (`(obj, msg?) => void`): the engine always calls it as `(fields, message)`.
 */
export type EngineLogFn = (fields: Record<string, unknown>, message: string) => void;

/** Minimal logger shape (pino- and console-compatible). */
export interface EngineLogger {
	debug: EngineLogFn;
	info: EngineLogFn;
	warn: EngineLogFn;
	error: EngineLogFn;
}

export interface EngineLimits {
	/** Trigger payloads, token outputs and **task outputs** (`runs.output`). Default 1 MB. */
	maxPayloadBytes?: number;
	/** `step.run` outputs (`steps.output`, Phase 4). Default 256 KB. */
	maxStepOutputBytes?: number;
	/** One `ctx.logger` entry's `data` (truncated with a marker, Phase 7). Default 16 KB. */
	maxLogEntryBytes?: number;
	/** `$2` of the Phase 4 step-window query. Default 16 MB. */
	maxInlineStepOutputBytes?: number;
}

export interface EngineConfig {
	/**
	 * Connection string (the engine creates, and on `stop()` closes, its own `pg.Pool`), or an
	 * existing pool (never closed by the engine). With an injected pool, make sure
	 * `pool.max >= total worker concurrency + 5`; a dedicated pool for the lib is recommended.
	 */
	postgres: string | { pool: Pool };
	/**
	 * Connection string, or an existing ioredis instance. An injected instance is only used for
	 * non-blocking producers; workers/subscribers get `duplicate()`s (decision 14).
	 */
	redis: string | { connection: Redis };
	/** Postgres schema (decision 15). Default `"minitrigger"`. */
	schema?: string;
	/** BullMQ key prefix (decision 15). Default `"minitrigger"`. */
	prefix?: string;
	tasks: readonly AnyTask[];
	/** Only used when the engine creates the pool. */
	pool?: { max?: number };
	/** Default 10 000 ms. Must be smaller than `stallTimeoutMs`. */
	heartbeatIntervalMs?: number;
	/** `SET LOCAL statement_timeout` of each heartbeat query. Default 2 000 ms. */
	heartbeatTimeoutMs?: number;
	/** A `running` run whose heartbeat is older than this is stalled (Phase 6). Default 60 000 ms. */
	stallTimeoutMs?: number;
	/** How long `stop()` waits for active runs before handing them back (decision 20). Default 30 000 ms. */
	shutdownTimeoutMs?: number;
	/** Default TTL of user idempotency keys (decision 9). Default `"7d"`; must not exceed `retention.days`. */
	idempotencyKeyTtl?: Duration;
	/**
	 * Finished runs older than `days` are deleted (decision 10 parent rule) by a `-system` job the
	 * leader schedules once per `intervalMs` window, `batchSize` rows per statement.
	 */
	retention?: {
		/** Default 30. */
		days?: number;
		/** Default 1 h. */
		intervalMs?: number;
		/** Default 1 000. */
		batchSize?: number;
	};
	/** Declarative schedules (`defineTask({ schedule })`, Phase 8, decision 21). */
	schedules?: {
		/**
		 * On `start()`, also delete (Postgres row + BullMQ job scheduler) every declarative schedule
		 * no longer declared by any registered task. Default `false`: a schedule removed from code is
		 * left alone (decision 21 -- declarative sync never deletes by default).
		 */
		pruneDeclarative?: boolean;
	};
	/** Reconciler (Phase 6), run by the leader only (decision 16). */
	reconciler?: {
		/** How often every started engine tries to become leader and, if leader, reconciles. Default 15 000 ms. */
		intervalMs?: number;
		/**
		 * How long a `queued`/`waiting` run may sit past its `run_after` before it is re-enqueued.
		 * Default 30 000 ms.
		 */
		graceMs?: number;
		/** Rows handled per reconciler case per tick. Default 100. */
		batchSize?: number;
	};
	limits?: EngineLimits;
	/** `ctx.logger` (Phase 7): persisted to `run_logs` in batches and mirrored to `logger`. */
	logging?: {
		/** Entries below this level are neither stored nor mirrored. Default `"info"`. */
		minLevel?: LogLevel;
		/** Also send every stored entry to `logger` (with `runId`, `taskId`, `attempt`). Default `true`. */
		mirror?: boolean;
	};
	/** Concurrency of the internal `${prefix}-system` worker (decision 8). Default 5. */
	systemConcurrency?: number;
	/** Engine-level logger (pino/console-compatible). Default: warn/error to `console`. */
	logger?: EngineLogger;
}

export interface ResolvedConfig {
	postgres: string | { pool: Pool };
	redis: string | { connection: Redis };
	schema: string;
	prefix: string;
	tasks: readonly AnyTask[];
	poolMax: number;
	heartbeatIntervalMs: number;
	heartbeatTimeoutMs: number;
	stallTimeoutMs: number;
	shutdownTimeoutMs: number;
	idempotencyKeyTtlMs: number;
	retentionDays: number;
	retentionIntervalMs: number;
	retentionBatchSize: number;
	schedules: { pruneDeclarative: boolean };
	reconciler: { intervalMs: number; graceMs: number; batchSize: number };
	limits: Required<EngineLimits>;
	logging: { minLevel: LogLevel; mirror: boolean };
	systemConcurrency: number;
	logger: EngineLogger;
}

export const DAY_MS = 86_400_000;

/**
 * Largest delay `setTimeout`/`setInterval` accept (2^31 - 1 ms, ~24.8 days). Node clamps anything
 * larger to 1 ms (with only a `TimeoutOverflowWarning`), so a bigger timer-fed value would fire at
 * once instead of "never": every option that feeds a timer is capped to this at `createEngine`.
 */
export const MAX_TIMER_MS = 2_147_483_647;

export const DEFAULTS = {
	schema: "minitrigger",
	prefix: "minitrigger",
	poolMax: 10,
	heartbeatIntervalMs: 10_000,
	heartbeatTimeoutMs: 2_000,
	stallTimeoutMs: 60_000,
	shutdownTimeoutMs: 30_000,
	idempotencyKeyTtlMs: 7 * DAY_MS,
	retentionDays: 30,
	retentionIntervalMs: 3_600_000,
	retentionBatchSize: 1_000,
	schedules: { pruneDeclarative: false },
	reconciler: { intervalMs: 15_000, graceMs: 30_000, batchSize: 100 },
	systemConcurrency: 5,
	logging: { minLevel: "info", mirror: true },
	limits: {
		maxPayloadBytes: 1024 * 1024,
		maxStepOutputBytes: 256 * 1024,
		maxLogEntryBytes: 16 * 1024,
		maxInlineStepOutputBytes: 16 * 1024 * 1024,
	},
} as const;

const LOG_LEVELS: readonly LogLevel[] = ["debug", "info", "warn", "error"];

const noop: EngineLogFn = () => {};

/** Default logger: drops debug/info, forwards warn/error to `console`. */
export const defaultLogger: EngineLogger = {
	debug: noop,
	info: noop,
	warn: (fields, message) => console.warn(`[minitrigger] ${message}`, fields),
	error: (fields, message) => console.error(`[minitrigger] ${message}`, fields),
};

/** Validates `config` and returns it with every default applied. Throws {@link ConfigError}. */
export function resolveConfig(config: EngineConfig): ResolvedConfig {
	if (config === null || typeof config !== "object") {
		throw new ConfigError(
			`createEngine(config): config must be an object with { postgres, redis, tasks }, got ` +
				describeOffendingValue(config)
		);
	}
	validateConnection("postgres", config.postgres, "pool");
	validateConnection("redis", config.redis, "connection");
	if (!Array.isArray(config.tasks)) {
		throw new ConfigError(
			"config.tasks must be an array of tasks created with defineTask(), got " +
				describeOffendingValue(config.tasks)
		);
	}

	const schema = validateSqlIdentifier("schema", config.schema ?? DEFAULTS.schema);
	const prefix = validateSqlIdentifier("prefix", config.prefix ?? DEFAULTS.prefix);

	const poolMax = positiveInt("pool.max", config.pool?.max ?? DEFAULTS.poolMax);
	const heartbeatIntervalMs = timerMs(
		"heartbeatIntervalMs",
		config.heartbeatIntervalMs ?? DEFAULTS.heartbeatIntervalMs
	);
	const heartbeatTimeoutMs = timerMs(
		"heartbeatTimeoutMs",
		config.heartbeatTimeoutMs ?? DEFAULTS.heartbeatTimeoutMs
	);
	const stallTimeoutMs = timerMs(
		"stallTimeoutMs",
		config.stallTimeoutMs ?? DEFAULTS.stallTimeoutMs
	);
	if (heartbeatIntervalMs >= stallTimeoutMs) {
		throw new ConfigError(
			`heartbeatIntervalMs (${heartbeatIntervalMs}) must be smaller than stallTimeoutMs ` +
				`(${stallTimeoutMs}), otherwise every healthy run would look stalled; lower ` +
				"heartbeatIntervalMs or raise stallTimeoutMs"
		);
	}
	const shutdownTimeoutMs = assertTimerMs(
		"shutdownTimeoutMs",
		nonNegativeInt("shutdownTimeoutMs", config.shutdownTimeoutMs ?? DEFAULTS.shutdownTimeoutMs)
	);

	const retentionDays = positiveNumber(
		"retention.days",
		config.retention?.days ?? DEFAULTS.retentionDays
	);
	const retentionIntervalMs = timerMs(
		"retention.intervalMs",
		config.retention?.intervalMs ?? DEFAULTS.retentionIntervalMs
	);
	const retentionBatchSize = positiveInt(
		"retention.batchSize",
		config.retention?.batchSize ?? DEFAULTS.retentionBatchSize
	);
	const pruneDeclarative =
		config.schedules?.pruneDeclarative ?? DEFAULTS.schedules.pruneDeclarative;
	if (typeof pruneDeclarative !== "boolean") {
		throw new ConfigError(
			`schedules.pruneDeclarative must be a boolean, got ${describeOffendingValue(pruneDeclarative)}`
		);
	}
	const schedules = { pruneDeclarative };

	const reconciler = {
		intervalMs: timerMs(
			"reconciler.intervalMs",
			config.reconciler?.intervalMs ?? DEFAULTS.reconciler.intervalMs
		),
		graceMs: nonNegativeInt(
			"reconciler.graceMs",
			config.reconciler?.graceMs ?? DEFAULTS.reconciler.graceMs
		),
		batchSize: positiveInt(
			"reconciler.batchSize",
			config.reconciler?.batchSize ?? DEFAULTS.reconciler.batchSize
		),
	};
	const idempotencyKeyTtlMs =
		config.idempotencyKeyTtl === undefined
			? DEFAULTS.idempotencyKeyTtlMs
			: parseDuration(config.idempotencyKeyTtl);
	assertTtlWithinRetention(idempotencyKeyTtlMs, retentionDays, "idempotencyKeyTtl");
	if (idempotencyKeyTtlMs <= 0) {
		const from =
			config.idempotencyKeyTtl === undefined
				? ""
				: ` (parsed from ${describeOffendingValue(config.idempotencyKeyTtl)})`;
		throw new ConfigError(
			`idempotencyKeyTtl must resolve to more than 0 ms, got ${idempotencyKeyTtlMs} ms${from}`
		);
	}

	const limits: Required<EngineLimits> = {
		maxPayloadBytes: positiveInt(
			"limits.maxPayloadBytes",
			config.limits?.maxPayloadBytes ?? DEFAULTS.limits.maxPayloadBytes
		),
		maxStepOutputBytes: positiveInt(
			"limits.maxStepOutputBytes",
			config.limits?.maxStepOutputBytes ?? DEFAULTS.limits.maxStepOutputBytes
		),
		maxLogEntryBytes: positiveInt(
			"limits.maxLogEntryBytes",
			config.limits?.maxLogEntryBytes ?? DEFAULTS.limits.maxLogEntryBytes
		),
		maxInlineStepOutputBytes: positiveInt(
			"limits.maxInlineStepOutputBytes",
			config.limits?.maxInlineStepOutputBytes ?? DEFAULTS.limits.maxInlineStepOutputBytes
		),
	};

	const minLevel = config.logging?.minLevel ?? DEFAULTS.logging.minLevel;
	if (!LOG_LEVELS.includes(minLevel)) {
		throw new ConfigError(
			`logging.minLevel must be one of ${LOG_LEVELS.join(", ")}, got ` +
				describeOffendingValue(minLevel)
		);
	}
	const mirror = config.logging?.mirror ?? DEFAULTS.logging.mirror;
	if (typeof mirror !== "boolean") {
		throw new ConfigError(
			`logging.mirror must be a boolean, got ${describeOffendingValue(mirror)}`
		);
	}
	const logging = { minLevel, mirror };

	const systemConcurrency = positiveInt(
		"systemConcurrency",
		config.systemConcurrency ?? DEFAULTS.systemConcurrency
	);

	const logger = config.logger ?? defaultLogger;
	for (const level of ["debug", "info", "warn", "error"] as const) {
		if (typeof logger[level] !== "function") {
			throw new ConfigError(
				`config.logger.${level} must be a function, got ${describeOffendingValue(logger[level])} ` +
					"(config.logger must implement { debug, info, warn, error }, pino- or console-compatible)"
			);
		}
	}

	return {
		postgres: config.postgres,
		redis: config.redis,
		schema,
		prefix,
		tasks: config.tasks,
		poolMax,
		heartbeatIntervalMs,
		heartbeatTimeoutMs,
		stallTimeoutMs,
		shutdownTimeoutMs,
		idempotencyKeyTtlMs,
		retentionDays,
		retentionIntervalMs,
		retentionBatchSize,
		schedules,
		reconciler,
		limits,
		logging,
		systemConcurrency,
		logger,
	};
}

/** Decision 9: a key must not outlive the runs it points to (retention would delete them first). */
export function assertTtlWithinRetention(
	ttlMs: number,
	retentionDays: number,
	label: string
): void {
	if (ttlMs > retentionDays * DAY_MS) {
		throw new ConfigError(
			`${label} (${ttlMs} ms) must not be larger than retention.days (${retentionDays} days): ` +
				"the run a key points to would be deleted by retention while the key is still alive; " +
				`shorten ${label} or raise retention.days`
		);
	}
}

/**
 * Validates `config.postgres`/`config.redis`: never echoes the value itself (a connection string
 * commonly carries a password) -- only its shape/type, per the phase 9 "actionable errors" task.
 * `marker` is a method both accepted shapes' raw instances have (`pg.Pool#query`,
 * `ioredis#duplicate`), used only to name what's missing/present in the message, never called.
 */
function validateConnection(
	name: "postgres" | "redis",
	value: unknown,
	injectedField: "pool" | "connection"
): void {
	const instanceLabel = name === "postgres" ? "pg.Pool" : "ioredis Redis";
	const marker = name === "postgres" ? "query" : "duplicate";

	if (typeof value === "string") {
		if (value.length === 0) {
			throw new ConfigError(
				`config.${name} must not be an empty string; pass a non-empty connection string, or ` +
					`{ ${injectedField}: <existing ${instanceLabel} instance> }`
			);
		}
		return;
	}

	if (value !== null && typeof value === "object" && injectedField in value) {
		if ((value as Record<string, unknown>)[injectedField] == null) {
			throw new ConfigError(
				`config.${name}.${injectedField} must be an existing ${instanceLabel} instance, got ` +
					describeOffendingValue((value as Record<string, unknown>)[injectedField])
			);
		}
		return;
	}

	// Common mistake: passing the raw Pool/Redis instance directly instead of wrapping it.
	const looksLikeRawInstance =
		value !== null &&
		typeof value === "object" &&
		!Array.isArray(value) &&
		typeof (value as Record<string, unknown>)[marker] === "function";
	if (looksLikeRawInstance) {
		throw new ConfigError(
			`config.${name} looks like a raw ${instanceLabel} instance (has .${marker}()), not a ` +
				`connection string or { ${injectedField} } wrapper; pass it as ` +
				`{ ${injectedField}: <that instance> }`
		);
	}

	throw new ConfigError(
		`config.${name} must be a non-empty connection string or { ${injectedField}: <existing ` +
			`${instanceLabel} instance> }, got ${describeOffendingValue(value)}`
	);
}

function positiveInt(name: string, value: number): number {
	if (!Number.isInteger(value) || value <= 0) {
		throw new ConfigError(
			`${name} must be a positive integer, got ${describeOffendingValue(value)}`
		);
	}
	return value;
}

/** A positive integer that feeds a timer (see {@link MAX_TIMER_MS}). */
function timerMs(name: string, value: number): number {
	return assertTimerMs(name, positiveInt(name, value));
}

/** Throws {@link ConfigError} when `value` (ms) would overflow `setTimeout`. */
export function assertTimerMs(name: string, value: number): number {
	if (value > MAX_TIMER_MS) {
		throw new ConfigError(
			`${name} must be at most ${MAX_TIMER_MS} ms (the setTimeout limit, ~24.8 days), got ${value} ms`
		);
	}
	return value;
}

function nonNegativeInt(name: string, value: number): number {
	if (!Number.isInteger(value) || value < 0) {
		throw new ConfigError(
			`${name} must be a non-negative integer, got ${describeOffendingValue(value)}`
		);
	}
	return value;
}

function positiveNumber(name: string, value: number): number {
	if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
		throw new ConfigError(
			`${name} must be a positive number, got ${describeOffendingValue(value)}`
		);
	}
	return value;
}
