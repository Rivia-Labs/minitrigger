/**
 * In-memory pub/sub and streams of the test mode (Phase 9.2), so `runs.subscribe`,
 * `ctx.stream` and `runs.readStream` work through the same engine code without Redis.
 */
import { MinitriggerError } from "../errors.js";
import type { PubSub, PubSubPublisher, RunStatusMessage } from "../pubsub.js";
import type { PipeOptions } from "../streams.js";
import type { Json } from "../utils/serialization.js";

/** One process-local "Redis" for status/cancel messages. */
export interface MemoryPubSubHub {
	publisher: PubSubPublisher;
	/** A subscriber attached to this hub (the engine creates at most one). */
	createPubSub(): PubSub;
}

export function createMemoryPubSubHub(): MemoryPubSubHub {
	const statusHandlers = new Map<string, Set<(message: RunStatusMessage) => void>>();
	const cancelHandlers = new Set<(runIds: string[]) => void>();

	// Delivered asynchronously, like a message coming back from Redis.
	const publisher: PubSubPublisher = {
		async publishStatus(message) {
			const copy = { ...message };
			queueMicrotask(() => {
				for (const handler of Array.from(statusHandlers.get(copy.runId) ?? [])) handler(copy);
			});
		},
		async publishCancel(runIds) {
			const ids = [...runIds];
			queueMicrotask(() => {
				for (const handler of Array.from(cancelHandlers)) handler([...ids]);
			});
		},
	};

	function createPubSub(): PubSub {
		const detach = new Set<() => void>();
		let closed = false;
		return {
			ready: Promise.resolve(),
			async publishStatus(message) {
				if (!closed) await publisher.publishStatus(message);
			},
			async publishCancel(runIds) {
				if (!closed) await publisher.publishCancel(runIds);
			},
			onCancel(handler) {
				if (closed) return () => {};
				const wrapped = (runIds: string[]) => handler(runIds);
				cancelHandlers.add(wrapped);
				const off = () => {
					cancelHandlers.delete(wrapped);
					detach.delete(off);
				};
				detach.add(off);
				return off;
			},
			subscribeStatus(runId, handler) {
				if (closed) return () => {};
				let set = statusHandlers.get(runId);
				if (!set) {
					set = new Set();
					statusHandlers.set(runId, set);
				}
				const wrapped = (message: RunStatusMessage) => handler(message);
				set.add(wrapped);
				const off = () => {
					const current = statusHandlers.get(runId);
					current?.delete(wrapped);
					if (current?.size === 0) statusHandlers.delete(runId);
					detach.delete(off);
				};
				detach.add(off);
				return off;
			},
			async close() {
				closed = true;
				for (const off of Array.from(detach)) off();
			},
		};
	}

	return { publisher, createPubSub };
}

type StreamEntry =
	| { kind: "data"; value: string | Json }
	| { kind: "end"; error: string | undefined };

/** Same bound as `src/streams.ts` on the error text of an end marker. */
const MAX_END_ERROR_LENGTH = 1_000;

interface MemoryStream {
	readonly entries: StreamEntry[];
	readonly waiters: Set<() => void>;
}

/** `ctx.stream` / `runs.readStream` in memory: an append-only list per `(runId, name)`. */
export interface MemoryStreams {
	/**
	 * Like the Redis version: the end marker is always written, with an error when `source` throws
	 * or `options.signal` fires (then `pipe` rejects with that error).
	 */
	pipe(
		runId: string,
		name: string,
		source: AsyncIterable<string | Json>,
		options?: PipeOptions
	): Promise<void>;
	/**
	 * From the start until the first end marker (like the Redis version), or until `signal`. An end
	 * marker with an error throws a `MinitriggerError` after the chunks before it.
	 */
	read(runId: string, name: string, signal: AbortSignal | undefined): AsyncIterable<string | Json>;
	/** Wakes every pending reader so it can notice an abort/stop. */
	close(): void;
}

export function createMemoryStreams(): MemoryStreams {
	const streams = new Map<string, MemoryStream>();
	let closed = false;

	function get(runId: string, name: string): MemoryStream {
		const key = `${runId}\u0000${name}`;
		let stream = streams.get(key);
		if (!stream) {
			stream = { entries: [], waiters: new Set() };
			streams.set(key, stream);
		}
		return stream;
	}

	function append(stream: MemoryStream, entry: StreamEntry): void {
		stream.entries.push(entry);
		for (const wake of Array.from(stream.waiters)) wake();
	}

	return {
		async pipe(runId, name, source, options) {
			const signal = options?.signal;
			const stream = get(runId, name);
			const iterator = source[Symbol.asyncIterator]();
			try {
				while (true) {
					signal?.throwIfAborted();
					const next = await nextOrAbort(iterator, signal);
					if (next.done === true) break;
					// The Redis version stores `JSON.stringify(chunk)`.
					append(stream, { kind: "data", value: JSON.parse(JSON.stringify(next.value)) });
				}
			} catch (error) {
				if (typeof iterator.return === "function") {
					Promise.resolve()
						.then(() => iterator.return?.())
						.catch(() => {});
				}
				append(stream, { kind: "end", error: errorText(error) });
				throw error;
			}
			append(stream, { kind: "end", error: undefined });
		},

		async *read(runId, name, signal) {
			const stream = get(runId, name);
			let index = 0;
			while (!closed && !signal?.aborted) {
				const entry = stream.entries[index];
				if (entry) {
					index++;
					if (entry.kind === "end") {
						if (entry.error === undefined) return;
						throw new MinitriggerError(
							`stream "${name}" of run ${runId} ended with an error: ${entry.error}`
						);
					}
					yield entry.value;
					continue;
				}
				await new Promise<void>(resolve => {
					const wake = () => {
						stream.waiters.delete(wake);
						signal?.removeEventListener("abort", wake);
						resolve();
					};
					stream.waiters.add(wake);
					signal?.addEventListener("abort", wake, { once: true });
				});
			}
		},

		close() {
			closed = true;
			for (const stream of streams.values()) {
				for (const wake of Array.from(stream.waiters)) wake();
			}
		},
	};
}

/** `name: message` of what ended a stream, bounded (same format as `src/streams.ts`). */
function errorText(error: unknown): string {
	const text =
		error instanceof Error || error instanceof DOMException
			? `${error.name}: ${error.message}`
			: String(error);
	return text.length > MAX_END_ERROR_LENGTH ? `${text.slice(0, MAX_END_ERROR_LENGTH)}…` : text;
}

/** `iterator.next()` raced against `signal` (same as `src/streams.ts`). */
function nextOrAbort<T>(
	iterator: AsyncIterator<T>,
	signal: AbortSignal | undefined
): Promise<IteratorResult<T>> {
	const next = iterator.next();
	if (!signal) return next;
	return new Promise((resolve, reject) => {
		const onAbort = (): void => reject(signal.reason);
		signal.addEventListener("abort", onAbort, { once: true });
		next.then(
			result => {
				signal.removeEventListener("abort", onAbort);
				resolve(result);
			},
			(error: unknown) => {
				signal.removeEventListener("abort", onAbort);
				reject(error);
			}
		);
	});
}
