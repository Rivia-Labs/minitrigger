/**
 * Engine configuration (SPEC.md §3, Phase 3): the public {@link EngineConfig} accepted by
 * `createEngine`, and {@link resolveConfig}, which validates it and fills in every default so the
 * rest of the engine only ever reads a fully-defaulted {@link ResolvedConfig}.
 */

import type { Redis } from "ioredis";
import type { Pool } from "pg";
import { ConfigError } from "./errors.js";
import type { AnyTask } from "./task.js";
import { type Duration, parseDuration } from "./utils/duration.js";
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
	retention?: { days?: number };
	limits?: EngineLimits;
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
	limits: Required<EngineLimits>;
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
	systemConcurrency: 5,
	limits: {
		maxPayloadBytes: 1024 * 1024,
		maxStepOutputBytes: 256 * 1024,
		maxLogEntryBytes: 16 * 1024,
		maxInlineStepOutputBytes: 16 * 1024 * 1024,
	},
} as const;

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
		throw new ConfigError("createEngine: config must be an object");
	}
	validateConnection("postgres", config.postgres, "pool");
	validateConnection("redis", config.redis, "connection");
	if (!Array.isArray(config.tasks)) {
		throw new ConfigError("config.tasks must be an array of tasks created with defineTask()");
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
				`(${stallTimeoutMs}), otherwise every healthy run would look stalled`
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
	const idempotencyKeyTtlMs =
		config.idempotencyKeyTtl === undefined
			? DEFAULTS.idempotencyKeyTtlMs
			: parseDuration(config.idempotencyKeyTtl);
	assertTtlWithinRetention(idempotencyKeyTtlMs, retentionDays, "idempotencyKeyTtl");
	if (idempotencyKeyTtlMs <= 0) {
		throw new ConfigError("idempotencyKeyTtl must be greater than 0");
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

	const systemConcurrency = positiveInt(
		"systemConcurrency",
		config.systemConcurrency ?? DEFAULTS.systemConcurrency
	);

	const logger = config.logger ?? defaultLogger;
	for (const level of ["debug", "info", "warn", "error"] as const) {
		if (typeof logger[level] !== "function") {
			throw new ConfigError(`config.logger.${level} must be a function`);
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
		limits,
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
				"the run a key points to would be deleted by retention while the key is still alive"
		);
	}
}

function validateConnection(name: string, value: unknown, injectedField: string): void {
	if (typeof value === "string") {
		if (value.length === 0) throw new ConfigError(`config.${name} must not be empty`);
		return;
	}
	if (value !== null && typeof value === "object" && injectedField in value) {
		if ((value as Record<string, unknown>)[injectedField] == null) {
			throw new ConfigError(`config.${name}.${injectedField} must be provided`);
		}
		return;
	}
	throw new ConfigError(
		`config.${name} must be a connection string or { ${injectedField} } with an existing instance`
	);
}

function positiveInt(name: string, value: number): number {
	if (!Number.isInteger(value) || value <= 0) {
		throw new ConfigError(`${name} must be a positive integer, got ${String(value)}`);
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
			`${name} must be at most ${MAX_TIMER_MS} ms (the setTimeout limit, ~24.8 days), got ${String(value)}`
		);
	}
	return value;
}

function nonNegativeInt(name: string, value: number): number {
	if (!Number.isInteger(value) || value < 0) {
		throw new ConfigError(`${name} must be a non-negative integer, got ${String(value)}`);
	}
	return value;
}

function positiveNumber(name: string, value: number): number {
	if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
		throw new ConfigError(`${name} must be a positive number, got ${String(value)}`);
	}
	return value;
}
