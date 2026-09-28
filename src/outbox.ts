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

import { Client, type ClientConfig } from "pg";
import type { EngineLogger } from "./config.js";
import { channelName, createPgListener } from "./pg/listener.js";

/** Rows claimed per `claimOutbox` statement; a full batch triggers another one right away. */
export const OUTBOX_BATCH_SIZE = 500;
/**
 * Every started engine sweeps this often even without a notification: the bound on the dispatch
 * latency when notifications are lost (listener reconnecting, PgBouncer in transaction mode). The
 * sweep is one probe of a partial index that is normally empty.
 */
export const OUTBOX_POLL_INTERVAL_MS = 2_000;
const CLOSE_TIMEOUT_MS = 5_000;
const WARN_THROTTLE_MS = 10_000;

/**
 * The NOTIFY channel of `schema`: `mt_outbox_s_<schema>` when it fits in 63 bytes, else
 * `mt_outbox_h_<first 48 hex chars of sha256(schema)>`. The `s_`/`h_` markers keep the two forms
 * from ever colliding. Schema-scoped (like the reconciler), so two engines on different schemas
 * never dispatch each other's runs. Delegates to {@link channelName} (`src/pg/listener.ts`), which
 * returns byte-for-byte what this function returned before Task 4 (production channel names never
 * change).
 */
export function outboxChannel(schema: string): string {
	return channelName("outbox", schema);
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
	const closeTimeoutMs = options.closeTimeoutMs ?? CLOSE_TIMEOUT_MS;

	let closed = false;
	let started = false;
	let pollTimer: ReturnType<typeof setInterval> | undefined;
	let sweeping: Promise<void> | undefined;
	let sweepAgain = false;
	const closing = new AbortController();
	let lastSweepWarnAt = 0;

	function warnSweepFailure(error: unknown): void {
		const now = Date.now();
		if (now - lastSweepWarnAt < WARN_THROTTLE_MS) {
			logger.debug(
				{ err: error, channel },
				"outbox sweep failed; retrying on the next notification or poll"
			);
			return;
		}
		lastSweepWarnAt = now;
		logger.warn(
			{ err: error, channel },
			"outbox sweep failed; retrying on the next notification or poll"
		);
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
				warnSweepFailure(error);
			} finally {
				sweeping = undefined;
			}
		})();
	}

	const listener = createPgListener({
		clientConfig: options.clientConfig,
		channels: [channel],
		logger,
		// A single channel: every notification is this one, so the payload/channel are irrelevant
		// (the outbox NOTIFY payload has always been empty -- the sweep re-reads the flag column).
		onNotification: () => sweep(),
		// LISTEN before the catch-up sweep, so nothing committed in between is missed.
		onConnected: () => sweep(),
		closeTimeoutMs,
		...(options.reconnectMinMs !== undefined ? { reconnectMinMs: options.reconnectMinMs } : {}),
		...(options.reconnectMaxMs !== undefined ? { reconnectMaxMs: options.reconnectMaxMs } : {}),
		...(options.connectTimeoutMs !== undefined
			? { connectTimeoutMs: options.connectTimeoutMs }
			: {}),
		...(options.onClientCreated
			? {
					createClient: (config: ClientConfig) => {
						const c = new Client(config);
						options.onClientCreated?.(c);
						return c;
					},
				}
			: {}),
	});

	return {
		async start() {
			if (started || closed) return;
			started = true;
			pollTimer = setInterval(sweep, pollIntervalMs);
			pollTimer.unref();
			await listener.start();
			// Catch-up even when LISTEN failed (a successful connect already swept via onConnected).
			if (!listener.isListening()) sweep();
		},
		sweep,
		isListening: () => listener.isListening(),
		async close() {
			if (closed) return;
			closed = true;
			closing.abort();
			clearInterval(pollTimer);
			const running = sweeping;
			await Promise.all([
				listener.close(),
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
