/**
 * Generic multi-channel dedicated `LISTEN` client (Postgres-only transport, Task 4). A single
 * `pg.Client` outside the pool (decision 16: `LISTEN` is session state, and a pooled connection
 * can be silently handed back to something else mid-session), `LISTEN`ing on every channel in
 * {@link PgListenerOptions.channels} and dispatching notifications to a single
 * {@link PgListenerOptions.onNotification} callback keyed by channel name.
 *
 * This is exactly the connection-lifecycle half of `src/outbox.ts`'s original listener (connect,
 * reconnect with backoff, `'error'`/`'end'` handling, bounded `close()`) extracted so callers with
 * more than one channel (the Postgres transport's wake/status/cancel channels, Task 10b) and the
 * single-channel outbox listener (Task 26) both build on the same connection logic. The
 * poll/sweep/dispatch behavior stays with the caller (e.g. `src/outbox.ts`); this module only owns
 * the socket.
 */

import { createHash } from "node:crypto";
import { Client, type ClientConfig, escapeIdentifier, type Notification } from "pg";
import type { EngineLogger } from "../config.js";
import { leaderClientConfig } from "../leader.js";
import { validateSqlIdentifier } from "../utils/identifiers.js";

const RECONNECT_MIN_MS = 500;
const RECONNECT_MAX_MS = 30_000;
const CONNECT_TIMEOUT_MS = 10_000;
const CLOSE_TIMEOUT_MS = 5_000;
const WARN_THROTTLE_MS = 10_000;
/** Postgres channel names are identifiers: at most NAMEDATALEN - 1 = 63 bytes. */
const MAX_CHANNEL_BYTES = 63;

export interface PgListenerOptions {
	/** Config of the dedicated client; TCP keepalive is forced on (`leaderClientConfig`). */
	clientConfig: ClientConfig;
	/** Already-validated channel names (<= 63 bytes); `LISTEN`ed in this order on every (re)connect. */
	channels: readonly string[];
	logger: EngineLogger;
	/**
	 * Called for every notification on any of {@link channels} (optional: consumers can also
	 * register with {@link PgListener.subscribe}). Wrapped in `try/catch` (logged, throttled): a
	 * throw here must never take down the listener's connection.
	 */
	onNotification?(channel: string, payload: string): void;
	/** Called after every successful (re)connect, right after all the `LISTEN`s. */
	onConnected?(): void;
	reconnectMinMs?: number;
	reconnectMaxMs?: number;
	connectTimeoutMs?: number;
	/** Bound of {@link PgListener.close} (client end + socket destroy fallback). */
	closeTimeoutMs?: number;
	/** Test hook: builds the dedicated client. Default `(config) => new Client(config)`. */
	createClient?: (config: ClientConfig) => Client;
}

export interface PgListener {
	/**
	 * Connects and `LISTEN`s once (awaited; a failure only warns and retries in the background
	 * with backoff). Resolves either way -- on success, or once the first reconnect attempt has
	 * been scheduled.
	 */
	start(): Promise<void>;
	/** True while the dedicated client is connected and `LISTEN`ing on every channel. */
	isListening(): boolean;
	/**
	 * Skips the reconnect backoff: when started, not closed and not connected or connecting,
	 * clears the pending retry, resets the attempt count and connects right away (a failure
	 * warns and backs off again from the minimum delay). A no-op otherwise.
	 */
	reconnectNow(): void;
	/**
	 * Registers `handler` for the notifications of `channels` (a subset of the channels given at
	 * construction: this never adds a `LISTEN`). Returns the unsubscribe. Works before `start()`
	 * and across reconnects; a throwing handler is logged (throttled) like `onNotification`.
	 */
	subscribe(
		channels: readonly string[],
		handler: (channel: string, payload: string) => void
	): () => void;
	/** Stops everything; ends the client and never reconnects again. Bounded. Idempotent. */
	close(): Promise<void>;
}

/**
 * The `NOTIFY` channel of `schema` for `kind`: `mt_<kind>_s_<schema>` when it fits in 63 bytes,
 * else `mt_<kind>_h_<first 48 hex chars of sha256(schema)>`. The `s_`/`h_` markers keep the two
 * forms from ever colliding. Schema-scoped, so two engines on different schemas never observe
 * each other's notifications. `channelName("outbox", s)` is byte-for-byte what `outboxChannel(s)`
 * (`src/outbox.ts`) returned before this task -- production channel names never change.
 */
export function channelName(kind: "wake" | "status" | "cancel" | "outbox", schema: string): string {
	validateSqlIdentifier("schema", schema);
	const plain = `mt_${kind}_s_${schema}`;
	if (Buffer.byteLength(plain) <= MAX_CHANNEL_BYTES) return plain;
	return `mt_${kind}_h_${createHash("sha256").update(schema).digest("hex").slice(0, 48)}`;
}

export function createPgListener(options: PgListenerOptions): PgListener {
	const { channels, logger, onNotification } = options;
	const reconnectMinMs = options.reconnectMinMs ?? RECONNECT_MIN_MS;
	const reconnectMaxMs = options.reconnectMaxMs ?? RECONNECT_MAX_MS;
	const connectTimeoutMs = options.connectTimeoutMs ?? CONNECT_TIMEOUT_MS;
	const closeTimeoutMs = options.closeTimeoutMs ?? CLOSE_TIMEOUT_MS;
	const createClient = options.createClient ?? ((config: ClientConfig) => new Client(config));
	const clientConfig = leaderClientConfig(options.clientConfig);

	let closed = false;
	let started = false;
	let client: Client | undefined;
	let listening = false;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	let reconnectAttempt = 0;
	/** Consumers registered with `subscribe()`, per channel. */
	const consumers = new Map<string, Set<(channel: string, payload: string) => void>>();
	// One throttle per kind: a connection failure must not hide a notification-handler failure.
	const lastWarnAt = { connection: 0, notification: 0 };

	function warn(
		kind: keyof typeof lastWarnAt,
		fields: Record<string, unknown>,
		message: string
	): void {
		const now = Date.now();
		if (now - lastWarnAt[kind] < WARN_THROTTLE_MS) {
			logger.debug(fields, message);
			return;
		}
		lastWarnAt[kind] = now;
		logger.warn(fields, message);
	}

	function warnConnectFailure(error: unknown): void {
		warn(
			"connection",
			{ err: error, channels },
			`pg listener could not LISTEN on [${channels.join(", ")}]; retrying`
		);
	}

	/** One handler call, isolated: a throw is logged (throttled) and never reaches the socket. */
	function deliver(channel: string, call: () => void): void {
		try {
			call();
		} catch (error) {
			warn("notification", { err: error, channel }, "pg listener onNotification threw; ignoring");
		}
	}

	function destroy(c: Client): void {
		try {
			c.connection.stream.destroy();
		} catch {
			// Already gone.
		}
	}

	function discard(c: Client): void {
		if (client !== c) return;
		client = undefined;
		listening = false;
		destroy(c);
		scheduleReconnect();
	}

	function scheduleReconnect(): void {
		if (closed || reconnectTimer) return;
		const base = Math.min(reconnectMaxMs, reconnectMinMs * 2 ** reconnectAttempt);
		reconnectAttempt++;
		const delay = base + Math.floor(Math.random() * Math.min(250, base));
		reconnectTimer = setTimeout(() => {
			reconnectTimer = undefined;
			connect().catch(error => warnConnectFailure(error));
		}, delay);
		reconnectTimer.unref();
	}

	async function endBounded(c: Client): Promise<void> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		const ended = await Promise.race([
			c.end().then(
				() => true,
				() => true
			),
			new Promise<boolean>(resolve => {
				timer = setTimeout(() => resolve(false), closeTimeoutMs);
				timer.unref();
			}),
		]);
		clearTimeout(timer);
		if (!ended) destroy(c);
	}

	async function connect(): Promise<void> {
		if (closed || client) return;
		let c: Client;
		try {
			c = createClient(clientConfig);
		} catch (error) {
			// A synchronous throw (bad config, an injected factory) must never escape as an
			// unhandled rejection of `start()`/the reconnect timer: warn and retry with backoff.
			warnConnectFailure(error);
			scheduleReconnect();
			return;
		}
		client = c;
		c.on("error", error => {
			if (client !== c) return;
			warn("connection", { err: error, channels }, "pg listener connection error; reconnecting");
			discard(c);
		});
		c.on("end", () => {
			if (client !== c) return;
			warn("connection", { channels }, "pg listener connection ended; reconnecting");
			discard(c);
		});
		c.on("notification", (message: Notification) => {
			if (client !== c) return;
			const payload = message.payload ?? "";
			// Postgres only delivers channels this session LISTENs on; no channel filter needed.
			deliver(message.channel, () => onNotification?.(message.channel, payload));
			const handlers = consumers.get(message.channel);
			if (!handlers) return;
			for (const handler of Array.from(handlers)) {
				deliver(message.channel, () => handler(message.channel, payload));
			}
		});
		let timer: ReturnType<typeof setTimeout> | undefined;
		const connecting = (async () => {
			await c.connect();
			// LISTEN on every channel before anything else observes the connection as ready.
			for (const channel of channels) {
				await c.query(`LISTEN ${escapeIdentifier(channel)}`);
			}
		})();
		// Settles after a timeout too (the socket is destroyed below); never an unhandled rejection.
		connecting.catch(() => {});
		try {
			await Promise.race([
				connecting,
				new Promise<never>((_, reject) => {
					timer = setTimeout(
						() => reject(new Error(`pg listener connect timed out after ${connectTimeoutMs}ms`)),
						connectTimeoutMs
					);
					timer.unref();
				}),
			]);
		} catch (error) {
			clearTimeout(timer);
			if (client === c) {
				warnConnectFailure(error);
				discard(c);
			} else {
				destroy(c);
			}
			return;
		}
		clearTimeout(timer);
		if (closed || client !== c) {
			if (client === c) client = undefined;
			await endBounded(c);
			return;
		}
		listening = true;
		reconnectAttempt = 0;
		options.onConnected?.();
	}

	return {
		async start() {
			if (started || closed) return;
			started = true;
			await connect();
		},
		isListening: () => listening,
		reconnectNow() {
			if (!started || closed || client) return;
			clearTimeout(reconnectTimer);
			reconnectTimer = undefined;
			reconnectAttempt = 0;
			connect().catch(error => warnConnectFailure(error));
		},
		subscribe(subscribed, handler) {
			const entries = [...new Set(subscribed)];
			for (const channel of entries) {
				let set = consumers.get(channel);
				if (!set) {
					set = new Set();
					consumers.set(channel, set);
				}
				set.add(handler);
			}
			return () => {
				for (const channel of entries) {
					const set = consumers.get(channel);
					set?.delete(handler);
					if (set?.size === 0) consumers.delete(channel);
				}
			};
		},
		async close() {
			if (closed) return;
			closed = true;
			clearTimeout(reconnectTimer);
			reconnectTimer = undefined;
			const c = client;
			client = undefined;
			listening = false;
			if (c) await endBounded(c);
		},
	};
}
