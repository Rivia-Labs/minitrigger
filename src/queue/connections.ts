import { Redis } from "ioredis";

/** What a dedicated connection is used for; see {@link RedisConnections.dedicated}. */
export type DedicatedPurpose = "worker" | "subscriber" | "stream";

/**
 * Manages Redis connection ownership per decision 14: if the engine was given
 * a connection string, this lib creates every connection it uses and closes
 * all of them on {@link closeAll}. If it was given an already-constructed
 * `ioredis` instance, that instance is:
 *   - returned as-is from {@link shared} for non-blocking BullMQ `Queue`
 *     producers (safe: BullMQ never mutates an injected instance, and `Queue`
 *     is not a "blocking connection" consumer -- verified in
 *     `bullmq/dist/cjs/classes/queue-base.js`, which constructs its backend
 *     with `hasBlockingConnection = false` unless told otherwise; only
 *     `Worker` passes `blockingConnection: true`. A non-blocking connection is
 *     never required to carry `maxRetriesPerRequest: null`, so handing BullMQ
 *     the shared instance directly cannot trip its
 *     "`maxRetriesPerRequest` must be null" throw -- see
 *     `docs/research/api-notes.md` §1.4),
 *   - **never** handed to a `Worker`, a pub/sub subscriber, or an `XREAD
 *     BLOCK` reader: each such exclusive/blocking use gets its own
 *     `duplicate()` from {@link dedicated}, and only those duplicates are
 *     closed by {@link closeAll} -- the original injected instance is left
 *     completely untouched (not even `.quit()`ed), matching "if the lib
 *     received it ready-made, the lib does not close it."
 *
 * Every {@link dedicated} connection is created with `maxRetriesPerRequest:
 * null`. For a `Worker` this is not optional: BullMQ throws
 * `"maxRetriesPerRequest must be null"` at construction time when handed a
 * pre-built ioredis instance used for a blocking connection (its own
 * `checkBlockingOptions`, `docs/research/api-notes.md` §1.4). Subscriber and
 * `XREAD BLOCK` connections are not BullMQ-managed, but they issue the same
 * kind of long-lived blocking Redis commands, where the ioredis default
 * (reject after N retries) is worse than blocking indefinitely while
 * reconnecting -- so the same setting is applied there for consistency and
 * because it is the behavior we actually want for a connection with no
 * command deadline of its own.
 */
export interface RedisConnections {
	/** The connection `Queue` producers should use. Never closed unless the lib created it (see above). */
	shared(): Redis;
	/**
	 * A brand-new connection dedicated to `purpose` -- always a fresh
	 * `duplicate()`/`new Redis(...)`, never the injected instance. Owned by
	 * this `RedisConnections` and closed by {@link closeAll}.
	 */
	dedicated(purpose: DedicatedPurpose): Redis;
	/** Closes every connection this call created. Never closes an injected instance. */
	closeAll(): Promise<void>;
}

const DEDICATED_OPTIONS = { maxRetriesPerRequest: null } as const;

export function createRedisConnections(input: string | { connection: Redis }): RedisConnections {
	const owned: Redis[] = [];

	if (typeof input === "string") {
		const url = input;
		const shared = new Redis(url);
		owned.push(shared);

		return {
			shared: () => shared,
			dedicated: () => {
				const conn = new Redis(url, DEDICATED_OPTIONS);
				owned.push(conn);
				return conn;
			},
			closeAll: () => closeConnections(owned),
		};
	}

	const injected = input.connection;

	return {
		shared: () => injected,
		dedicated: () => {
			const conn = injected.duplicate(DEDICATED_OPTIONS);
			owned.push(conn);
			return conn;
		},
		closeAll: () => closeConnections(owned),
	};
}

/** Best-effort graceful close (falls back to a hard disconnect) of every connection this lib owns. */
async function closeConnections(connections: Redis[]): Promise<void> {
	await Promise.all(
		connections.map(async connection => {
			try {
				await connection.quit();
			} catch {
				connection.disconnect();
			}
		})
	);
}
