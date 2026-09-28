/**
 * Engine configuration (SPEC.md §3, Phase 3): the public {@link EngineConfig} accepted by
 * `createEngine`, and {@link resolveConfig}, which validates it and fills in every default so the
 * rest of the engine only ever reads a fully-defaulted {@link ResolvedConfig}.
 */

import type { Pool } from "pg";
import { ConfigError } from "./errors.js";
import type { LogLevel } from "./store/types.js";
import type { AnyTask } from "./task.js";
import { type Duration, parseDuration } from "./utils/duration.js";
import { describeOffendingValue } from "./utils/error-format.js";
import { validateSqlIdentifier } from "./utils/identifiers.js";

/** `"bullmq"` (default) uses Redis + BullMQ as the transport; `"postgres"` needs no Redis at all. */
export type TransportKind = "bullmq" | "postgres";

/**
 * Structural subset of the `ioredis` `Redis` instance the engine needs for an injected connection
 * (decision 14): declared here rather than imported, so the public types never depend on
 * `ioredis` being installed at all (it becomes an optional peer once the Postgres transport
 * exists). A real `Redis` instance satisfies this structurally; `src/queue/connections.ts` (the
 * only place this crosses back into a real `Redis`, since that's the only module the BullMQ
 * backend needs it in) casts it back internally.
 */
export interface RedisLike {
	duplicate(options?: object): RedisLike;
	publish(channel: string, message: string): Promise<number>;
	on(event: string, listener: (...args: unknown[]) => void): unknown;
}

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
	/** `ctx.progress.set` value (decision 23; `runs.progress`). Default 16 KB. */
	maxProgressBytes?: number;
}

/**
 * Decision 26 (transactional trigger / outbox): tuning for the dedicated LISTEN/NOTIFY
 * dispatcher of `trigger(..., { tx })` runs. Only meaningful with `transport: "bullmq"` (the
 * default): in Postgres mode the transactional trigger already writes straight into the `runs`
 * row the claim reads, so there is no separate outbox to dispatch (decision 28).
 */
export interface OutboxConfig {
	/**
	 * How often every started engine sweeps for committed `{ tx }` runs even without a
	 * notification (bounds the dispatch latency when a notification is lost). Default 2 000 ms.
	 */
	pollIntervalMs?: number;
	/** Rows claimed per `claimOutbox` statement; a full batch triggers another one right away. Default 500. */
	batchSize?: number;
	/** Upper bound of the dedicated LISTEN client's reconnect backoff. Default 30 000 ms, minimum 500 ms. */
	reconnectMaxMs?: number;
	/**
	 * `false` skips the `pg_notify` call in the `trigger(..., { tx })` insert statement (the run
	 * is still flagged `outbox_pending` for the poll sweep): trades near-instant dispatch for
	 * avoiding the commit-time NOTIFY lock (Postgres serializes commits that notify). Default
	 * `true`.
	 */
	notify?: boolean;
}

/** Fields shared by both transports (spec §3.10). */
export interface CommonConfig {
	/**
	 * Connection string (the engine creates, and on `stop()` closes, its own `pg.Pool`), or an
	 * existing pool (never closed by the engine). With an injected pool, make sure
	 * `pool.max >= total worker concurrency + 5`; a dedicated pool for the lib is recommended.
	 */
	postgres: string | { pool: Pool };
	/** Postgres schema (decision 15). Default `"minitrigger"`. */
	schema?: string;
	tasks: readonly AnyTask[];
	/** Only used when the engine creates the pool (string `postgres`). */
	pool?: {
		/**
		 * Default: the total `concurrency` of every task queue + 5 (the README's sizing guide), at
		 * least 10 (Postgres mode also adds `systemConcurrency`, decision 14). `start()` warns when
		 * an explicit value is below that for the queues it starts.
		 */
		max?: number;
		/**
		 * `connectionTimeoutMillis` of the pool: how long a query (a heartbeat included) waits for a
		 * free connection before failing. A failed heartbeat only warns (decision 6). Default 10 000 ms.
		 */
		connectionTimeoutMs?: number;
	};
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

/**
 * Default transport (spec §3.10): Redis + BullMQ. `R` (review 5 item B3) lets a caller keep its
 * own connection type when reading the config back, e.g. `BullmqEngineConfig<Redis>` so that
 * `config.redis.connection` is an ioredis `Redis` again, as in 0.3.x.
 */
export interface BullmqTransportConfig<R extends RedisLike = RedisLike> {
	transport?: "bullmq";
	/**
	 * Connection string, or an existing ioredis `Redis` instance (not a `Cluster`, checked when
	 * the backend loads). An injected instance is only used for non-blocking producers;
	 * workers/subscribers get `duplicate()`s (decision 14).
	 */
	redis: string | { connection: R };
	/** BullMQ key prefix (decision 15). Default `"minitrigger"`. */
	prefix?: string;
	outbox?: OutboxConfig;
	polling?: never;
}

/**
 * Postgres-only transport (spec §3.10, decision 28): no Redis, no BullMQ. `prefix`/`redis`/
 * `outbox` are rejected at runtime (a `ConfigError`, never silently ignored): `prefix` has no
 * meaning without Redis keys (NOTIFY channels derive from `schema` instead), and `outbox` has no
 * effect because the transactional trigger already writes straight to the queue.
 */
export interface PostgresTransportConfig {
	transport: "postgres";
	/**
	 * Adaptive poll fallback for a missed `NOTIFY` (wake, status, cancel). Default 1 000/50 ms;
	 * minimum 100/0 ms.
	 */
	polling?: { intervalMs?: number; jitterMs?: number };
	redis?: never;
	prefix?: never;
	outbox?: never;
}

/**
 * Engine configuration (spec §3.10): a discriminated union on `transport` so the compiler
 * rejects the wrong combination (`redis`/`prefix`/`outbox` with `transport: "postgres"`, or
 * `polling` with `transport: "bullmq"`/omitted) at the type level; `resolveConfig` repeats the
 * same checks at runtime for plain JS callers or a cast.
 */
export type EngineConfig = BullmqEngineConfig | PostgresEngineConfig;

/**
 * One arm of {@link EngineConfig} (review 5 item B3): an object type (not a union), so it works
 * where 0.3.x code used `EngineConfig` itself -- `interface X extends BullmqEngineConfig`,
 * `Omit<BullmqEngineConfig, "tasks">` + spread, `Partial<BullmqEngineConfig>`, a required
 * `config.redis`. `BullmqEngineConfig<Redis>` also types `config.redis.connection` as `Redis`.
 */
export type BullmqEngineConfig<R extends RedisLike = RedisLike> = CommonConfig &
	BullmqTransportConfig<R>;

/** The `transport: "postgres"` arm of {@link EngineConfig}; see {@link BullmqEngineConfig}. */
export type PostgresEngineConfig = CommonConfig & PostgresTransportConfig;

export interface ResolvedConfig {
	postgres: string | { pool: Pool };
	transport: TransportKind;
	/** `undefined` with `transport: "postgres"` (redis is not used with that transport). */
	redis: string | { connection: RedisLike } | undefined;
	schema: string;
	/** With `transport: "postgres"`, equals `schema` (shared code that needs a name; never sent to Redis). */
	prefix: string;
	tasks: readonly AnyTask[];
	/** `undefined`: the engine derives it from the task queues' concurrency (see `EngineConfig.pool`). */
	poolMax: number | undefined;
	poolConnectionTimeoutMs: number;
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
	outbox: { pollIntervalMs: number; batchSize: number; reconnectMaxMs: number; notify: boolean };
	/** Adaptive poll fallback (`transport: "postgres"` only; defaulted either way). */
	polling: { intervalMs: number; jitterMs: number };
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

/** Item 6: below this, the outbox listener's poll fallback becomes a busy-loop against Postgres. */
export const MIN_OUTBOX_POLL_INTERVAL_MS = 100;

/** Below this, the Postgres transport's poll fallback becomes a busy-loop against Postgres. */
export const MIN_POLLING_INTERVAL_MS = 100;

/**
 * Review 3: lowest `outbox.reconnectMaxMs`, the outbox listener's first reconnect delay
 * (`RECONNECT_MIN_MS` in `src/outbox.ts`).
 */
export const MIN_OUTBOX_RECONNECT_MAX_MS = 500;

/** Item 6: above this, one `claimOutbox` batch could hold a statement/transaction open too long. */
export const MAX_OUTBOX_BATCH_SIZE = 10_000;

/** Item 6: `queues.scale(name, n)` (`src/engine.ts`) rejects a concurrency above this. */
export const MAX_QUEUE_SCALE_CONCURRENCY = 1_000;

export const DEFAULTS = {
	schema: "minitrigger",
	prefix: "minitrigger",
	/** Floor of the derived pool size (and pg's own default `max`). */
	poolMax: 10,
	/** Headroom over the total worker concurrency (README "Pool sizing", decision 14). */
	poolHeadroom: 5,
	poolConnectionTimeoutMs: 10_000,
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
	outbox: { pollIntervalMs: 2_000, batchSize: 500, reconnectMaxMs: 30_000, notify: true },
	polling: { intervalMs: 1_000, jitterMs: 50 },
	systemConcurrency: 5,
	logging: { minLevel: "info", mirror: true },
	limits: {
		maxPayloadBytes: 1024 * 1024,
		maxStepOutputBytes: 256 * 1024,
		maxLogEntryBytes: 16 * 1024,
		maxInlineStepOutputBytes: 16 * 1024 * 1024,
		maxProgressBytes: 16 * 1024,
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

/**
 * Escape hatch for the `transport` discriminated union: every field that differs across branches
 * (`redis`/`prefix`/`outbox`/`polling`/`transport` itself) is validated by hand below against the
 * resolved `transport`, so a single permissive alias is simpler than fighting the compiler over
 * which union member is active (no `any` involved -- every field keeps its real, if broadened,
 * type).
 */
type PermissiveConfig = CommonConfig & {
	transport?: TransportKind;
	redis?: string | { connection: RedisLike };
	prefix?: string;
	outbox?: OutboxConfig;
	polling?: { intervalMs?: number; jitterMs?: number };
};

/** Validates `config` and returns it with every default applied. Throws {@link ConfigError}. */
export function resolveConfig(config: EngineConfig): ResolvedConfig {
	if (config === null || typeof config !== "object") {
		throw new ConfigError(
			`createEngine(config): config must be an object with { postgres, redis, tasks }, got ` +
				describeOffendingValue(config)
		);
	}
	const c = config as PermissiveConfig;
	validateConnection("postgres", c.postgres, "pool");
	if (!Array.isArray(c.tasks)) {
		throw new ConfigError(
			"config.tasks must be an array of tasks created with defineTask(), got " +
				describeOffendingValue(c.tasks)
		);
	}

	const transport = c.transport ?? "bullmq";
	if (transport !== "bullmq" && transport !== "postgres") {
		throw new ConfigError(
			`transport must be "bullmq" or "postgres", got ${describeOffendingValue(transport)}`
		);
	}
	if (transport === "bullmq") {
		if (c.redis === undefined) {
			throw new ConfigError('redis is required with transport "bullmq"');
		}
		validateConnection("redis", c.redis, "connection");
		if (c.polling !== undefined) {
			throw new ConfigError('polling is only used with transport "postgres"');
		}
	} else {
		if (c.redis !== undefined) {
			throw new ConfigError(
				'redis is not used with transport "postgres"; remove it or set transport: "bullmq"'
			);
		}
		if (c.prefix !== undefined) {
			throw new ConfigError(
				'prefix is not used with transport "postgres"; NOTIFY channels derive from schema'
			);
		}
		if (c.outbox !== undefined) {
			throw new ConfigError(
				'outbox is not used with transport "postgres"; trigger(..., { tx }) already writes to the queue'
			);
		}
	}

	const schema = validateSqlIdentifier("schema", c.schema ?? DEFAULTS.schema);
	const prefix =
		transport === "postgres"
			? schema
			: validateSqlIdentifier("prefix", c.prefix ?? DEFAULTS.prefix);

	const polling = {
		intervalMs: intInRange(
			"polling.intervalMs",
			c.polling?.intervalMs ?? DEFAULTS.polling.intervalMs,
			MIN_POLLING_INTERVAL_MS,
			MAX_TIMER_MS
		),
		jitterMs: nonNegativeIntInRange(
			"polling.jitterMs",
			c.polling?.jitterMs ?? DEFAULTS.polling.jitterMs,
			0,
			MAX_TIMER_MS
		),
	};

	const poolMax =
		config.pool?.max === undefined ? undefined : positiveInt("pool.max", config.pool.max);
	const poolConnectionTimeoutMs = timerMs(
		"pool.connectionTimeoutMs",
		config.pool?.connectionTimeoutMs ?? DEFAULTS.poolConnectionTimeoutMs
	);
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

	const outboxNotify = config.outbox?.notify ?? DEFAULTS.outbox.notify;
	if (typeof outboxNotify !== "boolean") {
		throw new ConfigError(
			`outbox.notify must be a boolean, got ${describeOffendingValue(outboxNotify)}`
		);
	}
	const outbox = {
		// Item 6: bounded so a misconfiguration can't turn the outbox listener into a busy-loop
		// against Postgres (too-low pollIntervalMs) or hold a claim statement/transaction open for a
		// long time (too-high batchSize).
		pollIntervalMs: intInRange(
			"outbox.pollIntervalMs",
			config.outbox?.pollIntervalMs ?? DEFAULTS.outbox.pollIntervalMs,
			MIN_OUTBOX_POLL_INTERVAL_MS,
			MAX_TIMER_MS
		),
		batchSize: intInRange(
			"outbox.batchSize",
			config.outbox?.batchSize ?? DEFAULTS.outbox.batchSize,
			1,
			MAX_OUTBOX_BATCH_SIZE
		),
		// Review 3: the listener's reconnect backoff starts at 500 ms; a smaller cap collapses the
		// backoff to the cap itself, a near-continuous reconnect loop while Postgres is down.
		reconnectMaxMs: intInRange(
			"outbox.reconnectMaxMs",
			config.outbox?.reconnectMaxMs ?? DEFAULTS.outbox.reconnectMaxMs,
			MIN_OUTBOX_RECONNECT_MAX_MS,
			MAX_TIMER_MS
		),
		notify: outboxNotify,
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
		maxProgressBytes: positiveInt(
			"limits.maxProgressBytes",
			config.limits?.maxProgressBytes ?? DEFAULTS.limits.maxProgressBytes
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
		postgres: c.postgres,
		transport,
		redis: transport === "bullmq" ? c.redis : undefined,
		schema,
		prefix,
		tasks: c.tasks,
		poolMax,
		poolConnectionTimeoutMs,
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
		outbox,
		polling,
		limits,
		logging,
		systemConcurrency,
		logger,
	};
}

/**
 * README "Pool sizing": total worker concurrency + `DEFAULTS.poolHeadroom`, never below
 * `DEFAULTS.poolMax`. With `transport: "postgres"` the `-system` worker also runs against this
 * pool instead of its own BullMQ worker, so `systemConcurrency` is added too; with `transport:
 * "bullmq"` the formula is unchanged (decision 14). Used both by `createEngine`'s own derivation
 * (when `pool.max` is not given) and by the Postgres backend's `warnPoolSizing`, so the two never
 * disagree.
 */
export function derivePoolMax(
	queues: readonly { concurrency: number }[],
	systemConcurrency: number,
	transport: TransportKind
): number {
	const concurrency = queues.reduce((sum, q) => sum + q.concurrency, 0);
	const headroom =
		transport === "postgres" ? systemConcurrency + DEFAULTS.poolHeadroom : DEFAULTS.poolHeadroom;
	return Math.max(DEFAULTS.poolMax, concurrency + headroom);
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

/** A positive integer, additionally bounded to `[min, max]` (both inclusive). */
function intInRange(name: string, value: number, min: number, max: number): number {
	positiveInt(name, value);
	if (value < min || value > max) {
		throw new ConfigError(`${name} must be between ${min} and ${max}, got ${value}`);
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

/** A non-negative integer, additionally bounded to `[min, max]` (both inclusive; unlike {@link intInRange}, allows 0). */
function nonNegativeIntInRange(name: string, value: number, min: number, max: number): number {
	nonNegativeInt(name, value);
	if (value < min || value > max) {
		throw new ConfigError(`${name} must be between ${min} and ${max}, got ${value}`);
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
