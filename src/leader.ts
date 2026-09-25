/**
 * Leader election (Phase 6, decision 16): `pg_try_advisory_lock(hashtext(schema), 2)` on a
 * dedicated `pg.Client` kept open while this process is leader. Migrations use key `1`
 * (`src/migrations/migrate.ts`, via `pg_advisory_xact_lock`); the leader uses key `2`. Both are
 * derived from `hashtext(schema)` as the first of the two-int advisory lock keys, so a schema's
 * migration lock and its leader lock never contend with each other, and two schemas never
 * contend with each other either.
 *
 * Decision 16 is explicit that this must never run on a connection borrowed from a `pg.Pool`: a
 * pooled connection can be silently handed back and reused by something else while the lock is
 * still "held" from Postgres's point of view (advisory locks are session-scoped), which would
 * either leak the lock or let unrelated code accidentally inherit leadership. So this module only
 * ever talks to a `Client` it created and connected itself, and never returns it to anything.
 *
 * Reconnection policy (decision 16: "if this client dies, treat the event, discard the client,
 * mark leadership lost, and try to reacquire with a new client next cycle"): both the client's
 * `'error'` and `'end'` events discard the client and, if this elector was leader, call `onLose`
 * exactly once. The handlers never throw -- a broken `onLose`/`onAcquire` callback is swallowed,
 * because an unhandled exception inside an `EventEmitter` listener is exactly the kind of crash
 * this module exists to prevent (see `docs/research/api-notes.md` §3.4: an unhandled `'error'` on
 * a bare `Client` -- unlike a `Pool`, which recovers on its own -- crashes the process).
 *
 * Idle-connection policy: when `tryAcquire()` does *not* win the lock (someone else is leader),
 * the client is closed rather than kept open for a cheap retry next cycle. This elector is meant
 * to be driven by the reconciler's own loop (this module starts no timers of its own), typically
 * every ~15s (Phase 6 default), so the cost of a fresh `connect()` per losing attempt is small
 * next to that interval, and it means a non-leader process holds zero idle Postgres connections
 * for this purpose -- only the process that's actually leader pays for one. If a future caller
 * needs faster re-contention, keeping the client open across a failed attempt (and only retrying
 * the lock query, not `connect()`) is the alternative; this module deliberately does not do that.
 */

import { Client, type ClientConfig } from "pg";
import { ConfigError } from "./errors.js";
import { validateSqlIdentifier } from "./utils/identifiers.js";

/**
 * TCP keepalive of the dedicated leader client (audit item c): without it, a half-open socket
 * (peer gone, no FIN/RST: NAT/firewall drop, VM freeze) is never noticed by an idle client, so the
 * process keeps believing it is leader while Postgres may still hold the session (and the lock)
 * until its own `tcp_keepalives_*` fire. First probe after this delay; the OS settings decide the
 * probe interval/count.
 */
export const LEADER_KEEPALIVE_INITIAL_DELAY_MS = 10_000;

/** Default bound of {@link LeaderElector.release}/{@link LeaderElector.close}. */
export const DEFAULT_LEADER_RELEASE_TIMEOUT_MS = 5_000;

/**
 * `base` with TCP keepalive on, for the dedicated leader client. The engine should build its
 * `clientFactory` client from it (e.g. `() => new Client(leaderClientConfig(pool.options))`) or
 * pass `clientConfig` instead; the `connectionString`/`clientConfig` forms use it already.
 */
export function leaderClientConfig(base: ClientConfig): ClientConfig {
	return {
		...base,
		keepAlive: true,
		keepAliveInitialDelayMillis: LEADER_KEEPALIVE_INITIAL_DELAY_MS,
	};
}

/** Minimal logger shape (pino- and console-compatible), structurally the same as `EngineLogger`. */
export interface LeaderElectorLogger {
	warn: (fields: Record<string, unknown>, message: string) => void;
	error: (fields: Record<string, unknown>, message: string) => void;
}

export interface LeaderElectorConfig {
	/**
	 * Connection string used to build a dedicated `pg.Client` (`new Client({ connectionString })`,
	 * with TCP keepalive, see {@link leaderClientConfig}). Give exactly one of `connectionString`,
	 * `clientConfig` and `clientFactory`.
	 */
	connectionString?: string;
	/**
	 * Client config used to build each dedicated client, e.g. `pool.options` when the engine only
	 * has an injected `pg.Pool`; TCP keepalive is forced on ({@link leaderClientConfig}).
	 */
	clientConfig?: ClientConfig;
	/**
	 * For callers that only have a `pg.Pool` (e.g. the engine, when `config.postgres` is
	 * `{ pool }`): a factory that *constructs* (does not connect) a fresh dedicated `Client`, e.g.
	 * `() => new Client(pool.options)`. This module calls `.connect()` on whatever it returns, and
	 * calls it again to build a replacement client after a loss. The factory owns the client's
	 * config: build it with {@link leaderClientConfig} to get TCP keepalive (prefer `clientConfig`).
	 */
	clientFactory?: () => Client;
	/**
	 * Bound of {@link LeaderElector.release}/{@link LeaderElector.close} (default
	 * {@link DEFAULT_LEADER_RELEASE_TIMEOUT_MS}). Past it, the client's socket is destroyed: the
	 * server then ends the session, which frees the session-level advisory lock.
	 */
	releaseTimeoutMs?: number;
	/** Diagnostics/test hook: called with every dedicated client right after it is built. */
	onClientCreated?: (client: Client) => void;
	/** Postgres schema this elector contends leadership of (lock key `(hashtext(schema), 2)`). */
	schema: string;
	logger?: LeaderElectorLogger;
	/**
	 * Called synchronously from a winning `tryAcquire()`, at most once per acquisition. A throwing
	 * callback is swallowed.
	 */
	onAcquire?: () => void;
	/**
	 * Called at most once per loss, from the dedicated client's `'error'` or `'end'` handler, only
	 * when this elector was leader at the time. Never called for a deliberate `release()`/`close()`
	 * (those are not a "loss", they're this process giving up leadership on purpose). A throwing
	 * callback is swallowed.
	 */
	onLose?: () => void;
}

export interface LeaderElector {
	/**
	 * Tries to become (or remain) leader for `schema`.
	 *
	 * - Already leader: returns `true` immediately, without touching Postgres.
	 * - Not leader: connects a fresh dedicated client if none is open, then runs
	 *   `SELECT pg_try_advisory_lock(hashtext($1), 2)`. On success this elector becomes leader and
	 *   `onAcquire()` fires. On failure -- lock held elsewhere, or a connection/query error -- the
	 *   client is closed (see the idle-connection policy above) and `false` is returned.
	 *
	 * Never throws: connection and query failures are logged via `logger.warn` and resolve to
	 * `false`, so a caller (the reconciler loop) can call this every cycle without its own
	 * try/catch. No timers run inside -- the caller decides the cadence.
	 */
	tryAcquire(): Promise<boolean>;
	/** True while this elector currently holds the lock. */
	isLeader(): boolean;
	/**
	 * `pg_advisory_unlock(hashtext(schema), 2)` (best-effort -- logged, not thrown, if it fails,
	 * e.g. because the client already died) then ends the dedicated client. Safe to call when not
	 * leader (no-op). Does not call `onLose` (this is a deliberate release, not a loss).
	 */
	release(): Promise<void>;
	/**
	 * `release()`s and stops accepting further `tryAcquire()` calls (which then just resolve
	 * `false`). Idempotent -- a second `close()` is a no-op.
	 */
	close(): Promise<void>;
}

export function createLeaderElector(config: LeaderElectorConfig): LeaderElector {
	if (config === null || typeof config !== "object") {
		throw new ConfigError("createLeaderElector: config must be an object");
	}
	const { connectionString, clientConfig, clientFactory } = config;
	const sources = [connectionString, clientConfig, clientFactory].filter(v => v !== undefined);
	if (sources.length === 0) {
		throw new ConfigError(
			"createLeaderElector: config must have either connectionString or clientFactory"
		);
	}
	if (sources.length > 1) {
		throw new ConfigError(
			"createLeaderElector: config must have only one of connectionString, clientConfig or " +
				"clientFactory, not several"
		);
	}
	const releaseTimeoutMs = config.releaseTimeoutMs ?? DEFAULT_LEADER_RELEASE_TIMEOUT_MS;
	const schema = validateSqlIdentifier("schema", config.schema);
	const logger = config.logger;
	const onAcquire = config.onAcquire;
	const onLose = config.onLose;

	let client: Client | undefined;
	let leader = false;
	let closed = false;

	function makeClient(): Client {
		const c = clientFactory
			? clientFactory()
			: new Client(leaderClientConfig(clientConfig ?? { connectionString }));
		const onCreated = config.onClientCreated;
		if (onCreated) safeCall(() => onCreated(c));
		return c;
	}

	function safeCall(fn: (() => void) | undefined): void {
		if (!fn) return;
		try {
			fn();
		} catch (err) {
			logWarn("leader elector callback threw", err);
		}
	}

	function logWarn(message: string, err: unknown): void {
		try {
			logger?.warn({ schema, err }, message);
		} catch {
			// A broken injected logger must never throw from here.
		}
	}

	/** Discards `c` and, if it was still leading through `c`, marks leadership lost exactly once. */
	function forceDiscard(c: Client): void {
		if (client !== c) return; // stale event from an already-replaced/discarded client
		const wasLeader = leader;
		client = undefined;
		leader = false;
		if (wasLeader) {
			safeCall(onLose);
		}
	}

	function attach(c: Client): void {
		c.on("error", err => {
			if (client !== c) return;
			logWarn("leader election client error; discarding client", err);
			forceDiscard(c);
		});
		c.on("end", () => {
			forceDiscard(c);
		});
	}

	async function ensureClient(): Promise<Client> {
		if (client) return client;
		const c = makeClient();
		attach(c);
		try {
			await c.connect();
		} catch (err) {
			await c.end().catch(() => {});
			throw err;
		}
		client = c;
		return c;
	}

	/** Clears `client` (if it's still `c`) and ends `c`, swallowing any error from `end()`. */
	async function closeClient(c: Client): Promise<void> {
		if (client === c) {
			client = undefined;
		}
		try {
			await c.end();
		} catch {
			// Best-effort: the connection may already be dead, which is exactly why we're closing it.
		}
	}

	async function tryAcquire(): Promise<boolean> {
		if (closed) return false;
		if (leader) return true;

		let c: Client;
		try {
			c = await ensureClient();
		} catch (err) {
			logWarn("failed to connect leader election client", err);
			return false;
		}

		try {
			const { rows } = await c.query<{ locked: boolean }>(
				"SELECT pg_try_advisory_lock(hashtext($1), 2) AS locked",
				[schema]
			);
			if (rows[0]?.locked === true) {
				leader = true;
				safeCall(onAcquire);
				return true;
			}
			await closeClient(c);
			return false;
		} catch (err) {
			logWarn("leader election lock query failed", err);
			await closeClient(c);
			return false;
		}
	}

	function isLeader(): boolean {
		return leader;
	}

	/**
	 * Unlock + end, bounded by `releaseTimeoutMs` (audit item c): on a half-open socket neither
	 * the unlock query nor `end()` ever completes. Past the bound the socket is destroyed; the
	 * server then ends the session, which releases the advisory lock anyway.
	 */
	async function release(): Promise<void> {
		if (!client) {
			leader = false;
			return;
		}
		const c = client;
		const wasLeader = leader;
		leader = false;
		if (client === c) client = undefined;
		const graceful = (async () => {
			if (wasLeader) {
				try {
					await c.query("SELECT pg_advisory_unlock(hashtext($1), 2)", [schema]);
				} catch (err) {
					logWarn("failed to unlock during release (client may already be dead)", err);
				}
			}
			await closeClient(c);
		})();
		let timer: ReturnType<typeof setTimeout> | undefined;
		const timedOut = await Promise.race([
			graceful.then(() => false),
			new Promise<boolean>(resolve => {
				timer = setTimeout(() => resolve(true), releaseTimeoutMs);
				timer.unref?.();
			}),
		]);
		clearTimeout(timer);
		if (timedOut) {
			logWarn(
				`leader release did not finish within ${releaseTimeoutMs}ms; destroying the socket`,
				undefined
			);
			try {
				c.connection.stream.destroy();
			} catch {
				// Best-effort: already destroyed.
			}
			// The pending query/end settle (rejected/ended) once the socket is gone; nobody awaits.
			void graceful.catch(() => {});
		}
	}

	async function close(): Promise<void> {
		if (closed) return;
		closed = true;
		await release();
	}

	return { tryAcquire, isLeader, release, close };
}
