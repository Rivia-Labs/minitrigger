/**
 * Cross-process run-status/cancel pub/sub (Phase 7 seam for `engine.runs.subscribe()`; Phase 6's
 * "pub/sub para `AbortSignal`" cancellation broadcast). Standalone building block: it only knows
 * two already-open `ioredis` connections and a `prefix`, and does not touch Postgres or know
 * anything about the executor/engine -- wiring it in is another agent's job.
 *
 * Two fixed channels, both under `prefix` (decision 15's namespace isolation):
 * `${prefix}:run-status` and `${prefix}:cancel`. Per ioredis 5 / RESP2 pub/sub rules
 * (`docs/research/api-notes.md` §2.2), a connection that calls `subscribe()` enters exclusive
 * "subscriber" mode, so `createPubSub` requires the caller to hand it a connection dedicated to
 * that purpose (e.g. `RedisConnections.dedicated("subscriber")`, decision 14) -- it never
 * `duplicate()`s one itself, and never closes either connection it's given (closing is the
 * connection owner's job, same as decision 14 for every other dedicated use in this codebase).
 *
 * That single subscribed connection is subscribed to *both* channels once, up front; fan-out to
 * many interested callers within this process happens in-process (a `Map<runId, Set<handler>>`
 * for status, a `Set<handler>` for cancel) rather than by opening more Redis subscriptions --
 * "single subscribed connection, fan-out in-process by runId" per the Phase 7 task.
 *
 * Messages are JSON. A message that isn't valid JSON, or doesn't have the expected shape (e.g. a
 * stray publish from an unrelated producer on the same prefix, or a future/older version's
 * payload), is ignored and reported via `logger.warn` -- it never throws and never reaches a
 * handler.
 */

import type { Redis } from "ioredis";
import type { Json, RunStatus } from "./store/types.js";
import { validateSqlIdentifier } from "./utils/identifiers.js";

/** Minimal logger shape (pino- and console-compatible), same shape as `EngineLogger`. */
export interface PubSubLogger {
	warn: (fields: Record<string, unknown>, message: string) => void;
}

export interface PubSubOptions {
	/** Used for `publishStatus`/`publishCancel`. Any connection BullMQ-safe for non-blocking use. */
	publisher: Redis;
	/** A connection DEDICATED to this pub/sub (e.g. `RedisConnections.dedicated("subscriber")). */
	subscriber: Redis;
	prefix: string;
	/** Default: warns to `console` (mirrors `config.ts`'s `defaultLogger`). */
	logger?: PubSubLogger;
}

export interface RunStatusMessage {
	runId: string;
	status: RunStatus;
	/** ISO 8601 (decision 17: the caller reads this from the database clock, not `Date.now()`). */
	updatedAt: string;
	/**
	 * Present only on a message published for a `ctx.progress.set` write (decision 23; always
	 * `status: "running"` then) or when the publisher had the latest value cheaply at hand.
	 * Omitted on an ordinary status transition message; `isStatusPayload` still accepts those
	 * (an older publisher's messages never carried this field either).
	 */
	progress?: Json | null;
}

/** The publishing half of {@link PubSub}; needs no subscriber connection. */
export interface PubSubPublisher {
	publishStatus(message: RunStatusMessage): Promise<void>;
	publishCancel(runIds: readonly string[]): Promise<void>;
}

/** The two channels of `prefix` (decision 15's namespace isolation). */
export function pubSubChannels(prefix: string): { runStatus: string; cancel: string } {
	const valid = validateSqlIdentifier("prefix", prefix);
	return { runStatus: `${valid}:run-status`, cancel: `${valid}:cancel` };
}

/**
 * Publisher only, for an engine that never subscribes (decision 19: a trigger-only engine still
 * publishes the cancels and status changes it causes). `publisher` is any non-subscribed
 * connection (e.g. the shared one); it is never closed here.
 */
export function createPubSubPublisher(options: {
	publisher: Redis;
	prefix: string;
}): PubSubPublisher {
	const channels = pubSubChannels(options.prefix);
	const { publisher } = options;
	return {
		async publishStatus(message) {
			await publisher.publish(channels.runStatus, JSON.stringify(message));
		},
		async publishCancel(runIds) {
			const payload: CancelPayload = { runIds: [...runIds] };
			await publisher.publish(channels.cancel, JSON.stringify(payload));
		},
	};
}

export interface PubSub extends PubSubPublisher {
	/**
	 * Settles once the initial `SUBSCRIBE` was acknowledged (or failed, which is only logged): a
	 * message published before that is not received by this instance. Never rejects.
	 */
	readonly ready: Promise<void>;
	/** Fires for every `publishCancel` from any process sharing this `prefix`. Returns an unsubscribe. */
	onCancel(handler: (runIds: string[]) => void): () => void;
	/** Fires only for `publishStatus` messages whose `runId` matches. Returns an unsubscribe. */
	subscribeStatus(runId: string, handler: (message: RunStatusMessage) => void): () => void;
	/**
	 * Detaches this instance from the subscriber connection (unsubscribes both channels, drops
	 * every registered handler) and makes further `publishStatus`/`publishCancel` calls no-ops.
	 * Does NOT close either connection (decision 14: the caller owns that). Idempotent.
	 */
	close(): Promise<void>;
}

/** Bound on the `UNSUBSCRIBE` of {@link PubSub.close}. */
const UNSUBSCRIBE_TIMEOUT_MS = 1_000;

const defaultLogger: PubSubLogger = {
	warn: (fields, message) => console.warn(`[minitrigger] ${message}`, fields),
};

interface CancelPayload {
	runIds: string[];
}

export function createPubSub(options: PubSubOptions): PubSub {
	const { publisher, subscriber } = options;
	const prefix = validateSqlIdentifier("prefix", options.prefix);
	const logger = options.logger ?? defaultLogger;

	const { runStatus: runStatusChannel, cancel: cancelChannel } = pubSubChannels(prefix);
	const publish = createPubSubPublisher({ publisher, prefix });

	const cancelHandlers = new Set<(runIds: string[]) => void>();
	const statusHandlers = new Map<string, Set<(message: RunStatusMessage) => void>>();

	let closed = false;

	function warn(message: string, fields: Record<string, unknown>): void {
		try {
			logger.warn({ prefix, ...fields }, message);
		} catch {
			// A broken injected logger must never break pub/sub delivery.
		}
	}

	function onMessage(channel: string, raw: string): void {
		if (channel === cancelChannel) {
			handleCancel(raw);
		} else if (channel === runStatusChannel) {
			handleStatus(raw);
		}
	}

	function handleCancel(raw: string): void {
		const parsed = safeParse(raw);
		if (!isCancelPayload(parsed)) {
			warn("received a malformed cancel message; ignoring", { channel: cancelChannel, raw });
			return;
		}
		for (const handler of cancelHandlers) {
			safeCall(() => handler(parsed.runIds));
		}
	}

	function handleStatus(raw: string): void {
		const parsed = safeParse(raw);
		if (!isStatusPayload(parsed)) {
			warn("received a malformed run-status message; ignoring", {
				channel: runStatusChannel,
				raw,
			});
			return;
		}
		const handlers = statusHandlers.get(parsed.runId);
		if (!handlers) return;
		for (const handler of handlers) {
			safeCall(() => handler(parsed));
		}
	}

	subscriber.on("message", onMessage);
	// Fire-and-forget: a failure here is logged, not thrown, so a transient issue subscribing
	// doesn't crash whatever's constructing this pub/sub. `publishStatus`/`publishCancel` on the
	// OTHER side still work regardless (publishing never depends on this connection's state).
	const ready = subscriber.subscribe(runStatusChannel, cancelChannel).then(
		() => undefined,
		(error: unknown) => {
			warn("failed to subscribe to pub/sub channels", { error });
		}
	);

	return {
		ready,
		async publishStatus(message) {
			if (closed) return;
			await publish.publishStatus(message);
		},
		async publishCancel(runIds) {
			if (closed) return;
			await publish.publishCancel(runIds);
		},
		onCancel(handler) {
			cancelHandlers.add(handler);
			return () => {
				cancelHandlers.delete(handler);
			};
		},
		subscribeStatus(runId, handler) {
			let handlers = statusHandlers.get(runId);
			if (!handlers) {
				handlers = new Set();
				statusHandlers.set(runId, handlers);
			}
			handlers.add(handler);
			return () => {
				const current = statusHandlers.get(runId);
				if (!current) return;
				current.delete(handler);
				if (current.size === 0) statusHandlers.delete(runId);
			};
		},
		async close() {
			if (closed) return;
			closed = true;
			subscriber.off("message", onMessage);
			cancelHandlers.clear();
			statusHandlers.clear();
			// Only on a live connection, and bounded: with Redis unreachable the UNSUBSCRIBE would sit
			// in ioredis' offline queue forever (dedicated connections retry without limit), hanging
			// `engine.stop()`. The handlers are already detached; the owner disconnects the connection.
			if (subscriber.status !== "ready") return;
			let timer: ReturnType<typeof setTimeout> | undefined;
			try {
				await Promise.race([
					subscriber.unsubscribe(runStatusChannel, cancelChannel),
					new Promise<void>(resolve => {
						timer = setTimeout(resolve, UNSUBSCRIBE_TIMEOUT_MS);
					}),
				]);
			} catch (error) {
				warn("failed to unsubscribe on close (connection may already be closed)", { error });
			} finally {
				clearTimeout(timer);
			}
		},
	};
}

function safeParse(raw: string): unknown {
	try {
		return JSON.parse(raw);
	} catch {
		return undefined;
	}
}

function isCancelPayload(value: unknown): value is CancelPayload {
	if (value === null || typeof value !== "object") return false;
	const runIds = (value as Record<string, unknown>)["runIds"];
	return Array.isArray(runIds) && runIds.every(id => typeof id === "string");
}

function isStatusPayload(value: unknown): value is RunStatusMessage {
	if (value === null || typeof value !== "object") return false;
	const record = value as Record<string, unknown>;
	return (
		typeof record["runId"] === "string" &&
		typeof record["status"] === "string" &&
		typeof record["updatedAt"] === "string"
	);
}

function safeCall(fn: () => void): void {
	try {
		fn();
	} catch {
		// A handler registered by the caller must never break delivery to the others.
	}
}
