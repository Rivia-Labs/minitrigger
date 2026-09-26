/**
 * Helpers shared by `src/engine.ts` and the backends (`src/backends/*.ts`). Nothing here imports
 * `bullmq`/`ioredis`: the Postgres backend (and `engine.ts`, once the BullMQ backend is loaded with
 * a dynamic `import()`) must be able to use them without pulling Redis in.
 */

import type { ClientConfig, Pool } from "pg";
import { DEFAULTS, derivePoolMax, type ResolvedConfig } from "../config.js";
import { ConfigError } from "../errors.js";
import type { ResolvedQueue } from "../registry.js";
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
 * Decision 15 amendment (spec §3.8): records the schema's transport in `schema_meta` on the first
 * `start()` (both modes) and refuses another mode on the same schema: a BullMQ engine and a
 * Postgres engine on one schema would be two sources of `-system` jobs.
 */
export async function assertSchemaTransport(
	store: Pick<SqlStore, "meta">,
	resolved: Pick<ResolvedConfig, "schema" | "transport">
): Promise<void> {
	const mode = await store.meta.setIfAbsent("transport", resolved.transport);
	if (mode !== resolved.transport) {
		throw new ConfigError(
			`schema "${resolved.schema}" is used by engines with transport "${mode}"; mixed transports ` +
				"on one schema are not supported (see docs/SPEC.md decision 28)"
		);
	}
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
