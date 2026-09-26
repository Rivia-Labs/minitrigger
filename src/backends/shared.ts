/**
 * Helpers shared by `src/engine.ts` and the backends (`src/backends/*.ts`). Nothing here imports
 * `bullmq`/`ioredis`: the Postgres backend (and `engine.ts`, once the BullMQ backend is loaded with
 * a dynamic `import()`) must be able to use them without pulling Redis in.
 */

import type { ClientConfig, Pool } from "pg";
import { DEFAULTS, type ResolvedConfig } from "../config.js";
import type { ResolvedQueue } from "../registry.js";

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

/** README "Pool sizing": total worker concurrency + 5, never below pg's default 10. */
export function recommendedPoolMax(queues: readonly ResolvedQueue[]): number {
	const concurrency = queues.reduce((sum, q) => sum + q.concurrency, 0);
	return Math.max(DEFAULTS.poolMax, concurrency + DEFAULTS.poolHeadroom);
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
