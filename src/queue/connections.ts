import { Cluster, Redis } from "ioredis";
import type { RedisLike } from "../config.js";
import { ConfigError } from "../errors.js";

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

export interface RedisConnectionsOptions {
	/**
	 * Listener for the `error` events of every connection this lib creates (never attached to an
	 * injected instance). ioredis reconnects on its own; without a listener it prints each
	 * connection error to the console ("Unhandled error event").
	 */
	onError?: (error: unknown, purpose: DedicatedPurpose | "shared") => void;
}

export function createRedisConnections(
	input: string | { connection: RedisLike },
	options: RedisConnectionsOptions = {}
): RedisConnections {
	const owned: Redis[] = [];
	const { onError } = options;
	const track = (conn: Redis, purpose: DedicatedPurpose | "shared"): Redis => {
		if (onError) conn.on("error", (error: unknown) => onError(error, purpose));
		owned.push(conn);
		return conn;
	};

	if (typeof input === "string") {
		const url = input;
		const shared = track(new Redis(url), "shared");

		return {
			shared: () => shared,
			dedicated: purpose => track(new Redis(url, DEDICATED_OPTIONS), purpose),
			closeAll: () => closeConnections(owned),
		};
	}

	// `RedisLike` is a structural subset of the public types (config.ts's `EngineConfig.redis`, so
	// they never need to import `ioredis`); this module is only reachable by the BullMQ backend, so
	// an injected connection is, in practice, always a real `ioredis` `Redis` instance -- the cast
	// crosses back to it here, the one place that actually needs `.duplicate()` to return something
	// usable as a `Worker`/subscriber connection.
	const injected = assertInjectedRedis(input.connection);

	return {
		shared: () => injected,
		dedicated: purpose => track(injected.duplicate(DEDICATED_OPTIONS), purpose),
		closeAll: () => closeConnections(owned),
	};
}

/**
 * How long a graceful `QUIT` may take before the connection is dropped. With Redis unreachable,
 * `quit()` would otherwise sit in ioredis' offline queue forever (every dedicated connection has
 * `maxRetriesPerRequest: null`), and `engine.stop()` with it.
 */
export const QUIT_TIMEOUT_MS = 1_000;

/** Best-effort graceful close (falls back to a hard disconnect) of every connection this lib owns. */
async function closeConnections(connections: Redis[]): Promise<void> {
	await Promise.all(connections.map(connection => closeConnection(connection)));
}

/**
 * `QUIT` when the connection is ready, bounded by {@link QUIT_TIMEOUT_MS}; a connection that is
 * not ready (connecting, reconnecting, already closed) is disconnected at once, since a `QUIT`
 * would only be queued offline until Redis comes back.
 */
export async function closeConnection(
	connection: Redis,
	timeoutMs: number = QUIT_TIMEOUT_MS
): Promise<void> {
	if (connection.status === "end") return;
	if (connection.status !== "ready") {
		connection.disconnect();
		return;
	}
	let timer: ReturnType<typeof setTimeout> | undefined;
	const timedOut = new Promise<"timeout">(resolve => {
		timer = setTimeout(() => resolve("timeout"), timeoutMs);
	});
	try {
		const result = await Promise.race([connection.quit(), timedOut]);
		if (result === "timeout") connection.disconnect();
	} catch {
		connection.disconnect();
	} finally {
		clearTimeout(timer);
	}
}

/** Methods an injected connection needs (as ioredis `Redis` has them), beyond `RedisLike`. */
const REDIS_METHODS = ["duplicate", "connect", "disconnect", "quit", "publish", "subscribe"];

/**
 * Review 5 item B3: `RedisLike` (the public type) is structural, so a `Cluster` or a hand-made
 * object type-checks and would only fail deep inside BullMQ. An ioredis `Redis` is accepted; so is
 * an object shaped like one (another copy of ioredis, where `instanceof` fails); a `Cluster`
 * (never supported: BullMQ's cluster mode needs hash-tagged prefixes) or anything else is a
 * `ConfigError`.
 */
export function assertInjectedRedis(connection: RedisLike): Redis {
	if (connection instanceof Redis) return connection;
	const candidate = connection as unknown as Record<string, unknown> | null;
	if (connection instanceof Cluster || candidate?.["isCluster"] === true) {
		throw new ConfigError(
			"config.redis.connection is an ioredis Cluster, which minitrigger does not support; " +
				"pass a single-node ioredis Redis instance or a connection string"
		);
	}
	const redisShaped =
		typeof candidate === "object" &&
		candidate !== null &&
		typeof candidate["status"] === "string" &&
		REDIS_METHODS.every(name => typeof candidate[name] === "function");
	if (!redisShaped) {
		throw new ConfigError(
			"config.redis.connection must be an ioredis Redis instance (or a connection string); " +
				"the object given lacks ioredis methods such as connect/disconnect/quit/subscribe"
		);
	}
	return connection as unknown as Redis;
}
