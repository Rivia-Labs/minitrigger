/**
 * Helpers shared by `src/engine.ts` and the backends (`src/backends/*.ts`). Nothing here imports
 * `bullmq`/`ioredis`: the Postgres backend (and `engine.ts`, once the BullMQ backend is loaded with
 * a dynamic `import()`) must be able to use them without pulling Redis in.
 */

import { type ClientConfig, escapeIdentifier, type Pool, type PoolClient } from "pg";
import { DEFAULTS, derivePoolMax, type ResolvedConfig } from "../config.js";
import { ConfigError, MinitriggerError } from "../errors.js";
import type { ResolvedQueue } from "../registry.js";
import type { SqlQueryable } from "../store/sql/db.js";
import type { SqlStore } from "../store/sql/index.js";
import type { BackendContext } from "./types.js";

/** `true` if `promise` fulfilled within `ms`, `false` on timeout; rejections propagate. */
export async function withTimeout(promise: Promise<unknown>, ms: number): Promise<boolean> {
	if (ms <= 0) {
		promise.catch(() => {});
		return false;
	}
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise.then(() => true),
			new Promise<boolean>(resolve => {
				timer = setTimeout(() => resolve(false), ms);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
}

/**
 * First Postgres step of `start()` in both transports (decision 28(h), review 4 item 2): an
 * unreachable database is transient infrastructure, a `MinitriggerError` with the driver error as
 * `cause` -- never the raw `pg` error nor `ConfigError` -- and `start()` can be called again.
 *
 * Bounded by its own timer (`timeoutMs`: the same bound as the dedicated clients,
 * {@link dedicatedClientConfig}), whatever the pool: an injected `pg.Pool` without
 * `connectionTimeoutMillis` would otherwise wait for a host that accepts TCP but never answers
 * until the OS TCP timeout. A connect that completes after the bound leaves no state: its client
 * is destroyed (`release(true)`), never returned to the pool, and a query still in flight when
 * the bound fires gets its client destroyed too.
 */
export async function assertPostgresReachable(pool: Pool, timeoutMs: number): Promise<void> {
	let timedOut = false;
	let client: PoolClient | undefined;
	let released = false;
	const discard = (c: PoolClient, error?: Error) => {
		if (released) return;
		released = true;
		c.release(error ?? true);
	};
	const probe = (async () => {
		const acquired = await pool.connect();
		if (timedOut) {
			discard(acquired);
			return;
		}
		client = acquired;
		try {
			await acquired.query("SELECT 1");
		} catch (error) {
			discard(acquired, error instanceof Error ? error : new Error(String(error)));
			throw error;
		}
		if (timedOut) discard(acquired);
		else if (!released) {
			released = true;
			acquired.release();
		}
	})();
	let reached: boolean;
	try {
		reached = await withTimeout(probe, timeoutMs);
	} catch (error) {
		const reason = describeDriverError(error);
		throw new MinitriggerError(
			`cannot reach Postgres (${reason}); check config.postgres (host, port, credentials) ` +
				"and that Postgres accepts connections, then call start() again",
			{ cause: error }
		);
	}
	if (reached) return;
	timedOut = true;
	probe.catch(() => {});
	const gotClient = client !== undefined;
	if (client !== undefined) discard(client);
	// No connection at all while the pool was full (or had checkouts queued): the database may be
	// fine and every slot busy (an injected pool shared with the app, or slots held by abandoned
	// connection attempts), so the message must not claim it is unreachable.
	const poolMax = (pool.options as { max?: number }).max ?? 10;
	if (!gotClient && (pool.totalCount >= poolMax || pool.waitingCount > 0)) {
		throw new MinitriggerError(
			`could not get a connection within ${timeoutMs} ms: Postgres unreachable or every ` +
				`connection of the pool (max ${poolMax}) stayed busy; check config.postgres, the pool ` +
				"size and what holds its connections, then call start() again"
		);
	}
	throw new MinitriggerError(
		`cannot reach Postgres within ${timeoutMs} ms (config.pool.connectionTimeoutMs, or the ` +
			"injected pool's connectionTimeoutMillis); check config.postgres and that Postgres " +
			"answers, then call start() again"
	);
}

/**
 * Review 6 L2: a readable reason for a connect/query failure. Node's happy-eyeballs connect
 * (`localhost` resolving to ::1 and 127.0.0.1) rejects with an `AggregateError` whose own message
 * is empty: use its `code` and first inner error instead.
 */
export function describeDriverError(error: unknown): string {
	if (!(error instanceof Error)) return String(error);
	if (error.message) return error.message;
	const code = (error as { code?: unknown }).code;
	const inner = error instanceof AggregateError ? error.errors[0] : undefined;
	const innerMessage =
		inner instanceof Error ? describeDriverError(inner) : inner === undefined ? "" : String(inner);
	const parts = [typeof code === "string" ? code : "", innerMessage].filter(p => p !== "");
	return parts.length > 0 ? parts.join(": ") : error.name;
}

/**
 * Decision 15 amendment (spec §3.8): records the schema's transport in `schema_meta` on the first
 * `start()` (both modes) and refuses another mode on the same schema: a BullMQ engine and a
 * Postgres engine on one schema would be two sources of `-system` jobs.
 */
export async function assertSchemaTransport(
	store: Pick<SqlStore, "meta">,
	resolved: Pick<ResolvedConfig, "schema" | "transport">
): Promise<void> {
	const mode = await store.meta.setIfAbsent("transport", resolved.transport);
	if (mode !== resolved.transport) throw mixedModeError(resolved, mode);
}

/** SQLSTATE `undefined_table`. */
const UNDEFINED_TABLE = "42P01";

/** How long "no engine recorded a transport yet" is trusted before the key is read again. */
export const SCHEMA_TRANSPORT_UNSET_RECHECK_MS = 30_000;

/**
 * Review 5 item B1: the mixed-mode guard of {@link assertSchemaTransport}, for the write paths of
 * an engine that may never call `start()` (a trigger-only producer). Reads `schema_meta.transport`
 * -- never writes it, only `start()` does -- and throws the same `ConfigError` when another mode
 * recorded it. A match or a mismatch is final (the key never changes), so it costs one query per
 * engine; an absent key (no engine started yet), or no `schema_meta` table at all (a schema not
 * migrated to 009 yet), allows the write and is read again after
 * {@link SCHEMA_TRANSPORT_UNSET_RECHECK_MS}; any other failed read is not memoized (the write
 * fails).
 */
export function createSchemaTransportGuard(
	store: Pick<SqlStore, "meta">,
	resolved: Pick<ResolvedConfig, "schema" | "transport">,
	now: () => number = Date.now
): (tx?: SqlQueryable) => Promise<void> {
	let settled: { ok: true } | { ok: false; mode: string } | undefined;
	let unsetUntil = 0;
	let reading: Promise<void> | undefined;
	const table = `${escapeIdentifier(resolved.schema)}.${escapeIdentifier("schema_meta")}`;

	function record(mode: string | null): void {
		if (mode === null) {
			unsetUntil = now() + SCHEMA_TRANSPORT_UNSET_RECHECK_MS;
			return;
		}
		settled = mode === resolved.transport ? { ok: true } : { ok: false, mode };
	}

	async function readViaPool(): Promise<void> {
		let mode: string | null;
		try {
			mode = await store.meta.get("transport");
		} catch (error) {
			// Review 5 fix round 1: a schema not migrated to 009 yet (rolling upgrade: a 0.4 producer
			// deployed before the engine that runs migrate()) has no `schema_meta` table -- the same
			// as an absent key, never a failed write.
			if ((error as { code?: unknown } | null)?.code !== UNDEFINED_TABLE) throw error;
			mode = null;
		}
		record(mode);
	}

	/**
	 * Review 6 M1: on the `tx` path the marker is read through the caller's client -- the 0.3.x
	 * `tx` trigger never needed a pool connection, and a caller holding the only one of an
	 * injected pool would otherwise wait for `connectionTimeoutMillis` (forever without one). Two
	 * statements that cannot fail on a missing table (a failed statement would abort the caller's
	 * transaction): `to_regclass` first, the key only when the table exists. No `SET`.
	 * Review 6 fix round 1: the first statement also checks the caller's role may read the table
	 * (`has_table_privilege` only runs once `to_regclass` found it, so it cannot raise on a missing
	 * table): a least-privilege role without SELECT on `schema_meta` would get 42501 on the read,
	 * which aborts the caller's transaction. No privilege = an absent marker (write allowed,
	 * re-read after the same 30 s).
	 */
	async function readViaTx(tx: SqlQueryable): Promise<void> {
		const exists = await tx.query(
			`SELECT CASE WHEN to_regclass($1::text) IS NULL THEN false
			 ELSE has_table_privilege($1::text, 'SELECT') END AS present`,
			[table]
		);
		const present = (exists.rows[0] as { present?: unknown } | undefined)?.present;
		if (present !== true && present !== "t") {
			record(null);
			return;
		}
		const { rows } = await tx.query(`SELECT value FROM ${table} WHERE key = $1`, ["transport"]);
		const value = (rows[0] as { value?: unknown } | undefined)?.value;
		record(typeof value === "string" ? value : null);
	}

	return async (tx?: SqlQueryable) => {
		if (!settled && now() >= unsetUntil) {
			if (tx !== undefined) {
				// Never joins a pool read in flight: that one may be waiting for the connection the
				// caller holds.
				await readViaTx(tx);
			} else {
				reading ??= readViaPool().finally(() => {
					reading = undefined;
				});
				await reading;
			}
		}
		// A fresh error per throw, so its stack points at this caller.
		if (settled && !settled.ok) throw mixedModeError(resolved, settled.mode);
	};
}

function mixedModeError(
	resolved: Pick<ResolvedConfig, "schema" | "transport">,
	mode: string
): ConfigError {
	return new ConfigError(
		`schema "${resolved.schema}" is used by engines with transport "${mode}"; mixed transports ` +
			"on one schema are not supported (see docs/SPEC.md decision 28)"
	);
}

/**
 * Low item (b): warns when the pool is smaller than the README's sizing guide
 * ({@link derivePoolMax} over the started queues: total worker concurrency + 5, plus
 * `systemConcurrency` with `transport: "postgres"`, whose `-system` worker uses the pool) and, for
 * an injected pool, when it has no `connectionTimeoutMillis` (a heartbeat would then wait for a
 * connection without limit instead of failing with a warn, decision 6). The engine-created pool is
 * sized and bounded by default.
 */
export function warnPoolSizing(
	ctx: Pick<BackendContext, "resolved" | "pool" | "ownsPool" | "logger">,
	allQueues: readonly ResolvedQueue[],
	queueFilter: readonly string[] | undefined
): void {
	const { resolved, pool, ownsPool, logger } = ctx;
	const started = queueFilter ? allQueues.filter(q => queueFilter.includes(q.name)) : allQueues;
	const recommended = derivePoolMax(started, resolved.systemConcurrency, resolved.transport);
	const max = effectivePoolMax(ctx, allQueues);
	if (max < recommended) {
		logger.warn(
			{ poolMax: max, recommended, injected: !ownsPool },
			`postgres pool.max (${max}) is below the total worker concurrency + ` +
				`${poolHeadroomLabel(resolved)} (${recommended}): runs will wait for connections and ` +
				"heartbeats may fail; raise pool.max or lower the queues' concurrency"
		);
	}
	const poolOptions = pool.options as { connectionTimeoutMillis?: number };
	if (!ownsPool && !poolOptions.connectionTimeoutMillis) {
		logger.warn(
			{},
			"the injected pg.Pool has no connectionTimeoutMillis: on a saturated pool, heartbeats " +
				"wait for a connection without limit instead of failing with a warning; set one " +
				"(e.g. 10000)"
		);
	}
}

/**
 * `queues.scale()`: the same pool guide as {@link warnPoolSizing}, over the live worker
 * concurrency of this process (`concurrencyOf(queue)`, `undefined` for a queue not started here).
 */
export function warnScaledPoolSizing(
	ctx: Pick<BackendContext, "resolved" | "pool" | "ownsPool" | "logger">,
	allQueues: readonly ResolvedQueue[],
	concurrencyOf: (queue: string) => number | undefined
): void {
	const { resolved, logger } = ctx;
	const live = allQueues.map(q => ({ concurrency: concurrencyOf(q.name) ?? 0 }));
	const recommended = derivePoolMax(live, resolved.systemConcurrency, resolved.transport);
	const max = effectivePoolMax(ctx, allQueues);
	if (max < recommended) {
		logger.warn(
			{ poolMax: max, recommended },
			`postgres pool.max (${max}) is below this process's worker concurrency + ` +
				`${poolHeadroomLabel(resolved)} (${recommended}) after queues.scale(): runs will wait ` +
				"for connections and heartbeats may fail"
		);
	}
}

/** The pool's actual `max`: the engine's derived/configured one, or the injected pool's own. */
function effectivePoolMax(
	ctx: Pick<BackendContext, "resolved" | "pool" | "ownsPool">,
	allQueues: readonly ResolvedQueue[]
): number {
	const { resolved, pool, ownsPool } = ctx;
	if (ownsPool) {
		return (
			resolved.poolMax ?? derivePoolMax(allQueues, resolved.systemConcurrency, resolved.transport)
		);
	}
	return (pool.options as { max?: number }).max ?? DEFAULTS.poolMax;
}

function poolHeadroomLabel(resolved: Pick<ResolvedConfig, "transport" | "systemConcurrency">) {
	return resolved.transport === "postgres"
		? `systemConcurrency (${resolved.systemConcurrency}) + ${DEFAULTS.poolHeadroom}`
		: `${DEFAULTS.poolHeadroom}`;
}

/**
 * Config of the engine's dedicated `pg.Client`s (leader, outbox LISTEN), always with a bounded
 * connect (review 3 item 1, review 4 item 3): the engine's `pool.connectionTimeoutMs` for a
 * connection string; for an injected pool, its own `connectionTimeoutMillis` when set, else
 * that same default (an unbounded connect would let a leader tick or the listener wait forever
 * on a Postgres that accepts TCP but never answers; the pool itself is only warned about).
 */
export function dedicatedClientConfig(resolved: ResolvedConfig, pool: Pool): ClientConfig {
	if (typeof resolved.postgres === "string") {
		return {
			connectionString: resolved.postgres,
			connectionTimeoutMillis: resolved.poolConnectionTimeoutMs,
		};
	}
	const options = pool.options as ClientConfig;
	return {
		...options,
		connectionTimeoutMillis: options.connectionTimeoutMillis || resolved.poolConnectionTimeoutMs,
	};
}
