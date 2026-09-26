/**
 * Batched `NOTIFY` publisher (Postgres-only transport, Task 5; `docs/superpowers/specs/
 * 2026-09-26-postgres-transport-design.md` §3.7/§6). Every `NOTIFY` issued outside a transaction is
 * its own commit and takes the cluster-wide lock on the notification queue (§6): without batching,
 * one run's lifecycle (wake, `queued`, `running`, `completed`, plus progress) would cost around
 * four of those commits. This module collapses everything queued within a short window into a
 * single statement:
 *
 * ```sql
 * SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)
 * ```
 *
 * - {@link NotifyBatcher.notify} is synchronous and never throws: it only enqueues (channel,
 *   payload) pairs deduplicated within the current window (a `Map` keyed by the pair, so three
 *   identical notifications become one row) and (re)schedules the flush timer;
 * - the window is `windowMs` (default 5) after the first item queued since the last flush; reaching
 *   `maxItems` (default 500) unique pairs sends immediately instead of waiting out the window;
 * - flushes are serialized on a promise chain (mirrors `src/logs/buffer.ts`'s `flushChain`): the
 *   database never sees two concurrent batch statements, and items queued while a flush is already
 *   running wait for the next one;
 * - a failed statement is `logger.warn`'d (throttled to at most once every 10 s) and the batch is
 *   dropped -- pub/sub here is at-most-once, same as the Redis transport's, so a lost `NOTIFY` can
 *   only delay a wake-up that the poll fallback still covers;
 * - `flush()` sends whatever is pending right now (canceling any scheduled timer first) and waits
 *   for that statement to settle; `close()` does the same, refuses further `notify()` calls
 *   afterward, and is bounded by `closeTimeoutMs` (default 2 s) so a stuck connection can't hang
 *   shutdown -- idempotent, like `LogBuffer.close()`.
 */

import type { Pool } from "pg";
import type { EngineLogger } from "../config.js";

const DEFAULT_WINDOW_MS = 5;
const DEFAULT_MAX_ITEMS = 500;
const CLOSE_TIMEOUT_MS = 2_000;
const WARN_THROTTLE_MS = 10_000;

export interface NotifyBatcher {
	/** Enqueues; goes out on the next window. Never throws: a send failure is a throttled `warn`. */
	notify(channel: string, payload: string): void;
	/** Sends whatever is pending right now (resolves after the statement settles). */
	flush(): Promise<void>;
	/** `flush()` then refuses new items; bounded by `closeTimeoutMs` (default 2 s). Idempotent. */
	close(): Promise<void>;
}

export interface NotifyBatcherOptions {
	db: Pick<Pool, "query">;
	logger: EngineLogger;
	/** Default 5. */
	windowMs?: number;
	/** Default 500: reaching it sends immediately instead of waiting for `windowMs`. */
	maxItems?: number;
	/** Test hook: the timer function used to schedule a window's flush. Default the global `setTimeout`. */
	setTimeoutFn?: typeof setTimeout;
}

export function createNotifyBatcher(options: NotifyBatcherOptions): NotifyBatcher {
	const { db, logger } = options;
	const windowMs = options.windowMs ?? DEFAULT_WINDOW_MS;
	const maxItems = options.maxItems ?? DEFAULT_MAX_ITEMS;
	const setTimeoutFn = options.setTimeoutFn ?? setTimeout;

	// Deduplicated by "channel\u0000payload" within the current window; a payload is always a
	// run/token id or the empty string, never containing NUL, so this separator can't collide.
	let pending = new Map<string, { channel: string; payload: string }>();
	let timer: ReturnType<typeof setTimeout> | undefined;
	// Every flush chains onto this promise: at most one statement in flight, in schedule order.
	let flushChain: Promise<void> = Promise.resolve();
	let closed = false;
	let lastWarnAt = 0;

	function warn(message: string, fields: Record<string, unknown>): void {
		const now = Date.now();
		if (now - lastWarnAt < WARN_THROTTLE_MS) return;
		lastWarnAt = now;
		try {
			logger.warn(fields, message);
		} catch {
			// A broken injected logger must never break the batcher.
		}
	}

	function cancelTimer(): void {
		if (!timer) return;
		clearTimeout(timer);
		timer = undefined;
	}

	function scheduleFlush(): void {
		if (timer) return;
		timer = setTimeoutFn(() => {
			timer = undefined;
			void enqueueFlush();
		}, windowMs);
		timer.unref?.();
	}

	function enqueueFlush(): Promise<void> {
		flushChain = flushChain.then(runFlush);
		return flushChain;
	}

	async function runFlush(): Promise<void> {
		if (pending.size === 0) return;
		const items = [...pending.values()];
		pending = new Map();
		const channels = items.map(item => item.channel);
		const payloads = items.map(item => item.payload);
		try {
			await db.query("SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)", [
				channels,
				payloads,
			]);
		} catch (error) {
			warn("failed to send a batched NOTIFY; dropping the batch", { error, count: items.length });
		}
	}

	return {
		notify(channel, payload) {
			if (closed) return;
			pending.set(`${channel}\u0000${payload}`, { channel, payload });
			if (pending.size >= maxItems) {
				cancelTimer();
				void enqueueFlush();
				return;
			}
			scheduleFlush();
		},
		async flush() {
			cancelTimer();
			await enqueueFlush();
		},
		async close() {
			if (closed) return;
			closed = true;
			cancelTimer();
			await Promise.race([
				enqueueFlush(),
				new Promise<void>(resolve => {
					const boundTimer = setTimeoutFn(resolve, CLOSE_TIMEOUT_MS);
					boundTimer.unref?.();
				}),
			]);
		},
	};
}
