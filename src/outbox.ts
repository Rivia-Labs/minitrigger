/**
 * Transactional trigger / outbox (decision 26). `trigger(task, payload, { tx })` inserts the run
 * inside the caller's transaction with `outbox_pending = true` and, in the same statement,
 * `pg_notify(channel, '')` (delivered only on commit; one per transaction, since Postgres collapses
 * identical notifications). Every started engine runs an {@link OutboxListener}: a dedicated
 * `pg.Client` (never a pooled connection: LISTEN is session state) that LISTENs on the schema's
 * channel and, on each notification, on (re)connect and on a short poll, sweeps the committed
 * flagged runs through `store.runs.claimOutbox` (`FOR UPDATE SKIP LOCKED`, so several engines never
 * dispatch the same run twice) and enqueues them.
 *
 * The notification is only a wake-up hint: its payload is ignored and nothing is read from it, so a
 * forged or lost NOTIFY can at most cause (or delay) a sweep. Lost notifications (listener down, a
 * pooler in transaction mode that breaks LISTEN, no started engine at commit time) are covered by
 * the poll sweep (`OUTBOX_POLL_INTERVAL_MS`) and the sweep on every (re)connect/start; an enqueue
 * lost after the claim is recovered by the reconciler from `run_after` (decision 1).
 */

import { createHash } from "node:crypto";
import { Client, type ClientConfig, escapeIdentifier, type Notification } from "pg";
import type { EngineLogger } from "./config.js";
import { leaderClientConfig } from "./leader.js";
import { validateSqlIdentifier } from "./utils/identifiers.js";

/** Rows claimed per `claimOutbox` statement; a full batch triggers another one right away. */
export const OUTBOX_BATCH_SIZE = 500;
/**
 * Every started engine sweeps this often even without a notification: the bound on the dispatch
 * latency when notifications are lost (listener reconnecting, PgBouncer in transaction mode). The
 * sweep is one probe of a partial index that is normally empty.
 */
export const OUTBOX_POLL_INTERVAL_MS = 2_000;
const RECONNECT_MIN_MS = 500;
const RECONNECT_MAX_MS = 30_000;
const CONNECT_TIMEOUT_MS = 10_000;
const CLOSE_TIMEOUT_MS = 5_000;
const WARN_THROTTLE_MS = 10_000;
/** Postgres channel names are identifiers: at most NAMEDATALEN - 1 = 63 bytes. */
const MAX_CHANNEL_BYTES = 63;

/**
 * The NOTIFY channel of `schema`: `mt_outbox_s_<schema>` when it fits in 63 bytes, else
 * `mt_outbox_h_<first 48 hex chars of sha256(schema)>`. The `s_`/`h_` markers keep the two forms
 * from ever colliding. Schema-scoped (like the reconciler), so two engines on different schemas
 * never dispatch each other's runs.
 */
export function outboxChannel(schema: string): string {
	validateSqlIdentifier("schema", schema);
	const plain = `mt_outbox_s_${schema}`;
	if (Buffer.byteLength(plain) <= MAX_CHANNEL_BYTES) return plain;
	return `mt_outbox_h_${createHash("sha256").update(schema).digest("hex").slice(0, 48)}`;
}

export interface OutboxListenerOptions {
	/** Config of the dedicated LISTEN client; TCP keepalive is forced on (`leaderClientConfig`). */
	clientConfig: ClientConfig;
	/** From {@link outboxChannel}. */
	channel: string;
	logger: EngineLogger;
	/**
	 * Claims and enqueues one batch; resolves `true` when there may be more to claim. `signal`
	 * fires on {@link OutboxListener.close}: a batch still running then (e.g. waiting for a pool
	 * connection past the close bound) should not claim anymore, since a claim nobody enqueues is
	 * only recovered by the reconciler after `graceMs`.
	 */
	dispatchBatch(signal: AbortSignal): Promise<boolean>;
	pollIntervalMs?: number;
	reconnectMinMs?: number;
	reconnectMaxMs?: number;
	connectTimeoutMs?: number;
	/** Bound of {@link OutboxListener.close} (client end + running sweep). */
	closeTimeoutMs?: number;
	/** Test hook: every dedicated client right after it is built. */
	onClientCreated?: (client: Client) => void;
}

export interface OutboxListener {
	/**
	 * Starts the poll and connects + LISTENs once (awaited; a failure only warns and retries in the
	 * background with backoff). Then sweeps (catch-up of runs committed while nobody listened).
	 */
	start(): Promise<void>;
	/** Requests a sweep (coalesced with a running one). */
	sweep(): void;
	/** True while the dedicated client is connected and LISTENing. */
	isListening(): boolean;
	/** Stops everything; ends the client and waits for a running sweep, bounded. Idempotent. */
	close(): Promise<void>;
}

export function createOutboxListener(options: OutboxListenerOptions): OutboxListener {
	const { channel, logger, dispatchBatch } = options;
	const pollIntervalMs = options.pollIntervalMs ?? OUTBOX_POLL_INTERVAL_MS;
	const reconnectMinMs = options.reconnectMinMs ?? RECONNECT_MIN_MS;
	const reconnectMaxMs = options.reconnectMaxMs ?? RECONNECT_MAX_MS;
	const connectTimeoutMs = options.connectTimeoutMs ?? CONNECT_TIMEOUT_MS;
	const closeTimeoutMs = options.closeTimeoutMs ?? CLOSE_TIMEOUT_MS;
	const clientConfig = leaderClientConfig(options.clientConfig);

	let closed = false;
	let started = false;
	let client: Client | undefined;
	let listening = false;
	let pollTimer: ReturnType<typeof setInterval> | undefined;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	let reconnectAttempt = 0;
	let sweeping: Promise<void> | undefined;
	let sweepAgain = false;
	const closing = new AbortController();
	// One throttle per kind: a connection failure must not hide a sweep failure (or vice versa).
	const lastWarnAt = { connection: 0, sweep: 0 };

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
			{ err: error, channel },
			"outbox listener could not LISTEN; retrying (runs triggered with { tx } are still " +
				`dispatched by the ${pollIntervalMs} ms poll)`
		);
	}

	function discard(c: Client): void {
		if (client !== c) return;
		client = undefined;
		listening = false;
		destroy(c);
		scheduleReconnect();
	}

	function destroy(c: Client): void {
		try {
			c.connection.stream.destroy();
		} catch {
			// Already gone.
		}
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

	async function connect(): Promise<void> {
		if (closed || client) return;
		let c: Client;
		try {
			c = new Client(clientConfig);
			options.onClientCreated?.(c);
		} catch (error) {
			// A synchronous throw (bad config, hook) must never escape as an unhandled rejection
			// of `start()`/the reconnect timer: warn and retry with the same backoff.
			warnConnectFailure(error);
			scheduleReconnect();
			return;
		}
		client = c;
		c.on("error", error => {
			if (client !== c) return;
			warn("connection", { err: error, channel }, "outbox listener connection error; reconnecting");
			discard(c);
		});
		c.on("end", () => {
			if (client !== c) return;
			warn("connection", { channel }, "outbox listener connection ended; reconnecting");
			discard(c);
		});
		c.on("notification", (message: Notification) => {
			if (client === c && message.channel === channel) sweep();
		});
		let timer: ReturnType<typeof setTimeout> | undefined;
		const connecting = (async () => {
			await c.connect();
			// LISTEN before the catch-up sweep below, so nothing committed in between is missed.
			await c.query(`LISTEN ${escapeIdentifier(channel)}`);
		})();
		// Settles after a timeout too (the socket is destroyed); never an unhandled rejection.
		connecting.catch(() => {});
		try {
			await Promise.race([
				connecting,
				new Promise<never>((_, reject) => {
					timer = setTimeout(
						() =>
							reject(new Error(`outbox listener connect timed out after ${connectTimeoutMs}ms`)),
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
		sweep();
	}

	function sweep(): void {
		if (closed) return;
		if (sweeping) {
			sweepAgain = true;
			return;
		}
		sweeping = (async () => {
			try {
				do {
					sweepAgain = false;
					let more = true;
					// `closed` is re-checked before every batch; a batch already running when close()
					// fires sees `closing.signal` aborted.
					while (more && !closed) more = await dispatchBatch(closing.signal);
				} while (sweepAgain && !closed);
			} catch (error) {
				warn(
					"sweep",
					{ err: error, channel },
					"outbox sweep failed; retrying on the next notification or poll"
				);
			} finally {
				sweeping = undefined;
			}
		})();
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

	return {
		async start() {
			if (started || closed) return;
			started = true;
			pollTimer = setInterval(sweep, pollIntervalMs);
			pollTimer.unref();
			await connect();
			// Catch-up even when LISTEN failed (connect() already swept when it succeeded).
			if (!listening) sweep();
		},
		sweep,
		isListening: () => listening,
		async close() {
			if (closed) return;
			closed = true;
			closing.abort();
			clearInterval(pollTimer);
			clearTimeout(reconnectTimer);
			reconnectTimer = undefined;
			const c = client;
			client = undefined;
			listening = false;
			const running = sweeping;
			await Promise.all([
				c ? endBounded(c) : undefined,
				running
					? Promise.race([
							running,
							new Promise<void>(resolve => setTimeout(resolve, closeTimeoutMs).unref()),
						])
					: undefined,
			]);
		},
	};
}
