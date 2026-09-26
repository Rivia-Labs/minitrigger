/**
 * Run-status/cancel pub/sub over Postgres `NOTIFY` (Postgres-only transport, Task 5; design §3.7,
 * §5 "Segurança"). Same public shape as the Redis transport's `src/pubsub.ts` (`PubSub`,
 * `PubSubPublisher`, `RunStatusMessage`, the `subscribeStatus`/`onCancel` method names) so
 * `engine.runs.subscribe()` and the cross-process cancel broadcast work unchanged regardless of
 * transport -- only `src/pubsub.ts`'s *types* are reused here (`import type`), never its runtime
 * (which opens `ioredis` connections): this module and everything it imports must stay free of
 * `ioredis`/`bullmq` at runtime (constraints.md).
 *
 * `createPgPublisher` only enqueues into a shared {@link NotifyBatcher} (Task 5's other half):
 * `publishStatus` drops everything but the run id (a hint, not the message -- the subscriber
 * re-reads the row), `publishCancel` enqueues one item per id. Both "publish" calls resolve right
 * away, matching the Redis publisher and the at-most-once nature of pub/sub in general.
 *
 * `createPgPubSub` is the subscribing half, fed notifications by the engine's single shared
 * `PgListener` (Task 4) through the injected `subscribe` callback (one registration per consumer;
 * the listener itself already `LISTEN`s on every channel from `start()`). Every notification
 * carries only a bare run id as its payload -- validated as a UUID before anything else happens,
 * since (§5) any role with `CONNECT` on the database can `LISTEN` and forge one:
 *
 * - **status**: read only when at least one handler is registered for that run id (no handler, no
 *   read); then `store.runs.get(runId)` and the FULL {@link RunStatusMessage} (status, `updatedAt`,
 *   progress) -- all read from the database, never trusted from the notification -- is handed to
 *   every matching handler. A run that no longer exists, or a read failure (`logger.warn`,
 *   throttled), emits nothing.
 * - **cancel**: ids arriving within the same event-loop turn are grouped into one batch (no
 *   handlers registered, no read at all); each id is filtered by
 *   {@link CreatePgPubSubOptions.isActiveLocally} BEFORE any read (an id this process isn't running
 *   is never looked up), then re-read from the store, and only the ones whose `status` really is
 *   `"canceled"` reach `onCancel` handlers. This is the security requirement in §5/the review
 *   focus: a forged `NOTIFY` naming a run that is genuinely `running` must never abort it --
 *   the cancel is authoritative only after this re-read.
 *
 * `ready` resolves immediately: the shared listener is already up by the time this is constructed
 * (its own `start()` already waited for `LISTEN`, bounded, per §3.8). `close()` only calls the
 * unsubscribe returned by `subscribe` and drops local handlers; it does not touch the batcher (the
 * backend owns that) nor the shared listener (the engine owns that).
 */

import type { EngineLogger } from "../config.js";
import type { PubSub, PubSubPublisher, RunStatusMessage } from "../pubsub.js";
import type { Store } from "../store/types.js";
import { channelName } from "./listener.js";
import type { NotifyBatcher } from "./notify-batcher.js";

const WARN_THROTTLE_MS = 10_000;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function isUuid(value: string): boolean {
	return UUID_PATTERN.test(value);
}

function safeCall(fn: () => void): void {
	try {
		fn();
	} catch {
		// A handler registered by the caller must never break delivery to the others.
	}
}

/**
 * `publisher.publishStatus({ runId })` → `batcher.notify(channelName("status", schema), runId)`
 * (everything else in the message is dropped: the subscriber re-reads it). `publishCancel(ids)` →
 * one `batcher.notify(channelName("cancel", schema), id)` per id.
 */
export function createPgPublisher(options: {
	batcher: NotifyBatcher;
	schema: string;
}): PubSubPublisher {
	const { batcher, schema } = options;
	const statusChannel = channelName("status", schema);
	const cancelChannel = channelName("cancel", schema);
	return {
		async publishStatus(message) {
			batcher.notify(statusChannel, message.runId);
		},
		async publishCancel(runIds) {
			for (const runId of runIds) {
				batcher.notify(cancelChannel, runId);
			}
		},
	};
}

export interface CreatePgPubSubOptions {
	publisher: PubSubPublisher;
	store: Pick<Store, "runs">;
	schema: string;
	/** Runs actually running in THIS process (the cancel re-read is scoped to these). */
	isActiveLocally(runId: string): boolean;
	/**
	 * Registers with the shared `PgListener` for these channels; returns an unsubscribe. Called
	 * once, for both channels together, when this pub/sub is constructed.
	 */
	subscribe(
		channels: readonly string[],
		onNotification: (channel: string, payload: string) => void
	): () => void;
	logger: EngineLogger;
}

export function createPgPubSub(options: CreatePgPubSubOptions): PubSub {
	const { store, isActiveLocally, logger } = options;
	const statusChannel = channelName("status", options.schema);
	const cancelChannel = channelName("cancel", options.schema);

	const statusHandlers = new Map<string, Set<(message: RunStatusMessage) => void>>();
	const cancelHandlers = new Set<(runIds: string[]) => void>();

	let closed = false;
	let lastWarnAt = 0;
	// Cancel ids notified within the same event-loop turn are grouped into a single re-read pass.
	let pendingCancelIds: string[] | undefined;

	function warn(message: string, fields: Record<string, unknown>): void {
		const now = Date.now();
		if (now - lastWarnAt < WARN_THROTTLE_MS) return;
		lastWarnAt = now;
		try {
			logger.warn(fields, message);
		} catch {
			// A broken injected logger must never break pub/sub delivery.
		}
	}

	function handleStatus(runId: string): void {
		const handlers = statusHandlers.get(runId);
		if (!handlers || handlers.size === 0) return; // No handler: skip the read entirely.
		void (async () => {
			let run: Awaited<ReturnType<Store["runs"]["get"]>>;
			try {
				run = await store.runs.get(runId);
			} catch (error) {
				warn("failed to re-read run for a status notification; ignoring", { error, runId });
				return;
			}
			if (!run) return;
			const message: RunStatusMessage = {
				runId: run.id,
				status: run.status,
				updatedAt: run.updatedAt.toISOString(),
				progress: run.progress,
			};
			const current = statusHandlers.get(runId);
			if (!current) return;
			for (const handler of current) {
				safeCall(() => handler(message));
			}
		})();
	}

	function scheduleCancel(runId: string): void {
		if (cancelHandlers.size === 0) return; // No handler: skip batching/reading entirely.
		if (pendingCancelIds) {
			pendingCancelIds.push(runId);
			return;
		}
		pendingCancelIds = [runId];
		queueMicrotask(() => {
			const ids = pendingCancelIds ?? [];
			pendingCancelIds = undefined;
			void flushCancel(ids);
		});
	}

	async function flushCancel(ids: readonly string[]): Promise<void> {
		if (cancelHandlers.size === 0) return;
		const localIds = ids.filter(id => isActiveLocally(id));
		if (localIds.length === 0) return; // Not run by this process: never even read.
		const canceled: string[] = [];
		for (const runId of localIds) {
			let run: Awaited<ReturnType<Store["runs"]["get"]>>;
			try {
				run = await store.runs.get(runId);
			} catch (error) {
				warn("failed to re-read run for a cancel notification; ignoring", { error, runId });
				continue;
			}
			if (run?.status === "canceled") canceled.push(runId);
		}
		if (canceled.length === 0) return;
		for (const handler of cancelHandlers) {
			safeCall(() => handler(canceled));
		}
	}

	function onNotification(channel: string, payload: string): void {
		if (closed) return;
		if (!isUuid(payload)) return; // Forged/empty payload: never look anything up.
		if (channel === statusChannel) {
			handleStatus(payload);
		} else if (channel === cancelChannel) {
			scheduleCancel(payload);
		}
	}

	const unsubscribe = options.subscribe([statusChannel, cancelChannel], onNotification);

	return {
		ready: Promise.resolve(),
		async publishStatus(message) {
			if (closed) return;
			await options.publisher.publishStatus(message);
		},
		async publishCancel(runIds) {
			if (closed) return;
			await options.publisher.publishCancel(runIds);
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
			cancelHandlers.clear();
			statusHandlers.clear();
			unsubscribe();
		},
	};
}
