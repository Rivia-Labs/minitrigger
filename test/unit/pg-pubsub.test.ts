import type { Pool } from "pg";
import { describe, expect, it, vi } from "vitest";
import { createNotifyBatcher } from "../../src/pg/notify-batcher.js";
import { createPgPublisher, createPgPubSub } from "../../src/pg/pubsub.js";
import type { RunRecord } from "../../src/store/types.js";

const quietLogger = { debug() {}, info() {}, warn() {}, error() {} };

/** A fake `Pick<Pool, "query">`: only the single-arg-array call shape this module ever uses. */
function fakeDb(
	query: (text: string, values?: unknown[]) => Promise<unknown>
): Pick<Pool, "query"> {
	return { query } as unknown as Pick<Pool, "query">;
}

function warnCollector() {
	const messages: Array<{ fields: Record<string, unknown>; message: string }> = [];
	const logger = {
		...quietLogger,
		warn: (fields: Record<string, unknown>, message: string) => {
			messages.push({ fields, message });
		},
	};
	return { logger, messages };
}

function fakeRun(overrides: Partial<RunRecord> = {}): RunRecord {
	return {
		id: "r1",
		taskId: "task-a",
		queue: "default",
		payload: null,
		status: "running",
		output: null,
		error: null,
		attempt: 1,
		maxAttempts: 3,
		executionId: "e1",
		enqueueSeq: 1,
		heartbeatAt: null,
		runAfter: null,
		parentRunId: null,
		parentWaitpointId: null,
		parentStepName: null,
		batchIndex: null,
		startedAt: new Date("2026-09-26T00:00:00.000Z"),
		finishedAt: null,
		createdAt: new Date("2026-09-26T00:00:00.000Z"),
		updatedAt: new Date("2026-09-26T00:00:01.000Z"),
		progress: null,
		debounceKey: null,
		concurrencyKey: null,
		...overrides,
	};
}

/** A fully controllable `setTimeout` replacement: schedules never fire on their own. */
function fakeClock() {
	let scheduled: { fn: () => void; delay: number } | undefined;
	const setTimeoutFn = ((fn: () => void, delay?: number) => {
		scheduled = { fn, delay: delay ?? 0 };
		return { unref: () => {} } as unknown as ReturnType<typeof setTimeout>;
	}) as unknown as typeof setTimeout;
	return {
		setTimeoutFn,
		/** Fires the currently scheduled timer, if any (clearing it first). */
		fire() {
			const current = scheduled;
			scheduled = undefined;
			current?.fn();
		},
		hasScheduled: () => scheduled !== undefined,
	};
}

describe("createNotifyBatcher", () => {
	it("sends one deduplicated statement per window", async () => {
		const clock = fakeClock();
		const query = vi.fn(async () => ({ rows: [], rowCount: 0 }));
		const batcher = createNotifyBatcher({
			db: fakeDb(query),
			logger: quietLogger,
			setTimeoutFn: clock.setTimeoutFn,
		});

		batcher.notify("a", "1");
		batcher.notify("a", "1");
		batcher.notify("a", "1");
		batcher.notify("b", "2");

		expect(query).not.toHaveBeenCalled();
		expect(clock.hasScheduled()).toBe(true);

		clock.fire();
		await batcher.flush();

		expect(query).toHaveBeenCalledTimes(1);
		expect(query).toHaveBeenCalledWith(expect.stringContaining("pg_notify"), [
			["a", "b"],
			["1", "2"],
		]);
	});

	it("sends immediately once maxItems unique pairs are queued", async () => {
		const clock = fakeClock();
		const query = vi.fn(async () => ({ rows: [], rowCount: 0 }));
		const batcher = createNotifyBatcher({
			db: fakeDb(query),
			logger: quietLogger,
			maxItems: 2,
			setTimeoutFn: clock.setTimeoutFn,
		});

		batcher.notify("a", "1");
		expect(query).not.toHaveBeenCalled();
		batcher.notify("b", "2");

		// maxItems reached: sent via the microtask queue, without ever calling flush() and without
		// ever firing the window timer (`clock.fire()` is never called in this test) -- proves the
		// send is truly immediate, not incidentally caused by the assertion's own flush() call.
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();

		expect(query).toHaveBeenCalledTimes(1);
		expect(query).toHaveBeenCalledWith(expect.stringContaining("pg_notify"), [
			["a", "b"],
			["1", "2"],
		]);
	});

	it("a slow statement never overlaps with the next, and each one carries at most maxItems rows", async () => {
		const clock = fakeClock();
		const calls: Array<{ channels: string[]; payloads: string[] }> = [];
		let concurrent = 0;
		let maxConcurrent = 0;
		let resolveCurrent: (() => void) | undefined;
		const query = vi.fn(async (_text: string, values?: unknown[]) => {
			concurrent++;
			maxConcurrent = Math.max(maxConcurrent, concurrent);
			const [channels, payloads] = values as [string[], string[]];
			calls.push({ channels, payloads });
			await new Promise<void>(resolve => {
				resolveCurrent = resolve;
			});
			concurrent--;
			return { rows: [], rowCount: 0 };
		});
		const batcher = createNotifyBatcher({
			db: fakeDb(query),
			logger: quietLogger,
			maxItems: 3,
			setTimeoutFn: clock.setTimeoutFn,
		});

		// 7 unique items, cap 3: three statements (3 + 3 + 1), never more than one in flight.
		for (let i = 0; i < 7; i++) batcher.notify(`c${i}`, `p${i}`);

		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();
		expect(query).toHaveBeenCalledTimes(1);
		expect(calls[0]?.channels.length).toBeLessThanOrEqual(3);

		resolveCurrent?.();
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();
		expect(query).toHaveBeenCalledTimes(2);
		expect(calls[1]?.channels.length).toBeLessThanOrEqual(3);

		resolveCurrent?.();
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();
		expect(query).toHaveBeenCalledTimes(3);
		expect(calls[2]?.channels.length).toBeLessThanOrEqual(3);

		resolveCurrent?.();
		await batcher.flush();

		// No 4th statement: the backlog was fully drained by the third.
		expect(query).toHaveBeenCalledTimes(3);
		expect(maxConcurrent).toBe(1);
		for (const call of calls) expect(call.channels.length).toBeLessThanOrEqual(3);
		expect(calls.reduce((sum, call) => sum + call.channels.length, 0)).toBe(7);
	});

	it("never throws when the statement rejects; warns instead", async () => {
		const clock = fakeClock();
		const query = vi.fn(async () => {
			throw new Error("connection reset");
		});
		const { logger, messages } = warnCollector();
		const batcher = createNotifyBatcher({
			db: fakeDb(query),
			logger,
			setTimeoutFn: clock.setTimeoutFn,
		});

		batcher.notify("a", "1");
		await expect(batcher.flush()).resolves.toBeUndefined();

		expect(query).toHaveBeenCalledTimes(1);
		expect(messages).toHaveLength(1);
		expect(messages[0]?.message).toMatch(/failed to send/i);
	});

	it("close() flushes what's pending and makes further notify() calls no-ops", async () => {
		const clock = fakeClock();
		const query = vi.fn(async () => ({ rows: [], rowCount: 0 }));
		const batcher = createNotifyBatcher({
			db: fakeDb(query),
			logger: quietLogger,
			setTimeoutFn: clock.setTimeoutFn,
		});

		batcher.notify("a", "1");
		await batcher.close();

		expect(query).toHaveBeenCalledTimes(1);

		// Idempotent, and refuses new items from here on.
		await batcher.close();
		batcher.notify("b", "2");
		await batcher.flush();
		expect(query).toHaveBeenCalledTimes(1);
	});
});

describe("createPgPublisher", () => {
	it("publishStatus notifies only the run id; publishCancel one per id", async () => {
		const sent: Array<[string, string]> = [];
		const batcher = {
			notify: (c: string, p: string) => {
				sent.push([c, p]);
			},
			flush: async () => {},
			close: async () => {},
		};
		const publisher = createPgPublisher({ batcher, schema: "s" });

		await publisher.publishStatus({
			runId: "r1",
			status: "running",
			updatedAt: "x",
			progress: { big: "y".repeat(20_000) },
		});
		await publisher.publishCancel(["a", "b"]);

		expect(sent).toEqual([
			["mt_status_s_s", "r1"],
			["mt_cancel_s_s", "a"],
			["mt_cancel_s_s", "b"],
		]);
	});
});

describe("createPgPubSub", () => {
	function noopPublisher() {
		return { publishStatus: vi.fn(async () => {}), publishCancel: vi.fn(async () => {}) };
	}

	function fakeSubscribe() {
		let registered: ((channel: string, payload: string) => void) | undefined;
		let unsubscribed = false;
		return {
			subscribe: (
				_channels: readonly string[],
				onNotification: (channel: string, payload: string) => void
			) => {
				registered = onNotification;
				return () => {
					unsubscribed = true;
				};
			},
			emit: (channel: string, payload: string) => registered?.(channel, payload),
			isUnsubscribed: () => unsubscribed,
		};
	}

	it("ready resolves immediately (the shared listener is already up)", async () => {
		const { subscribe } = fakeSubscribe();
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get: vi.fn(async () => null) } as never },
			schema: "s",
			isActiveLocally: () => false,
			subscribe,
			logger: quietLogger,
		});

		await expect(pubsub.ready).resolves.toBeUndefined();
	});

	it("a status notification re-reads the run and emits the full message; invalid payloads ignored", async () => {
		const R1 = "00000000-0000-4000-8000-000000000001";
		const OTHER = "00000000-0000-4000-8000-000000000abc";
		const get = vi.fn(async (runId: string) => {
			if (runId === R1) return fakeRun({ id: R1, status: "completed" });
			return null;
		});
		const { subscribe, emit } = fakeSubscribe();
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: () => false,
			subscribe,
			logger: quietLogger,
		});

		const received: unknown[] = [];
		pubsub.subscribeStatus(R1, message => received.push(message));

		// Invalid/empty payloads: never even read.
		emit("mt_status_s_s", "not-a-uuid");
		emit("mt_status_s_s", "");
		await Promise.resolve();
		expect(get).not.toHaveBeenCalled();

		// A runId with no registered handler: no read either.
		emit("mt_status_s_s", OTHER);
		await Promise.resolve();
		expect(get).not.toHaveBeenCalled();

		// Valid payload with a handler: re-read, full message emitted.
		emit("mt_status_s_s", R1);
		await Promise.resolve();
		await Promise.resolve();

		expect(get).toHaveBeenCalledWith(R1);
		expect(received).toEqual([
			{
				runId: R1,
				status: "completed",
				updatedAt: "2026-09-26T00:00:01.000Z",
				progress: null,
			},
		]);

		// A run that vanished: nothing emitted, no throw.
		received.length = 0;
		get.mockResolvedValueOnce(null);
		emit("mt_status_s_s", R1);
		await Promise.resolve();
		await Promise.resolve();
		expect(received).toEqual([]);
	});

	it("a cancel notification only reaches onCancel handlers for local runs canceled in the database", async () => {
		const R1 = "00000000-0000-4000-8000-000000000001";
		const R2 = "00000000-0000-4000-8000-000000000002";
		const R3 = "00000000-0000-4000-8000-000000000003";
		const get = vi.fn(async (runId: string) => {
			if (runId === R1) return fakeRun({ id: R1, status: "canceled" });
			if (runId === R2) return fakeRun({ id: R2, status: "running" });
			// R3 is never queried: isActiveLocally(R3) is false.
			throw new Error("r3 should never be read");
		});
		const { subscribe, emit } = fakeSubscribe();
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: runId => runId === R1 || runId === R2,
			subscribe,
			logger: quietLogger,
		});

		const received: string[][] = [];
		pubsub.onCancel(runIds => received.push(runIds));

		emit("mt_cancel_s_s", R1);
		emit("mt_cancel_s_s", R2);
		emit("mt_cancel_s_s", R3);

		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();

		expect(get).toHaveBeenCalledTimes(2);
		expect(get).toHaveBeenCalledWith(R1);
		expect(get).toHaveBeenCalledWith(R2);
		expect(received).toEqual([[R1]]);
	});

	it("close() unsubscribes and stops delivering to handlers", async () => {
		// A real UUID, unlike the earlier "r1" placeholder: with a non-UUID payload the invalid-
		// payload guard alone would drop it regardless of close(), so that would prove nothing about
		// close() itself.
		const R1 = "00000000-0000-4000-8000-000000000001";
		const { subscribe, emit, isUnsubscribed } = fakeSubscribe();
		const get = vi.fn(async () => fakeRun({ id: R1 }));
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: () => true,
			subscribe,
			logger: quietLogger,
		});

		const received: unknown[] = [];
		pubsub.subscribeStatus(R1, message => received.push(message));

		await pubsub.close();
		expect(isUnsubscribed()).toBe(true);

		emit("mt_status_s_s", R1);
		await Promise.resolve();
		await Promise.resolve();
		expect(received).toEqual([]);
		expect(get).not.toHaveBeenCalled();
	});

	it("dedupes cancel ids repeated within the same batch before filtering/reading", async () => {
		const R1 = "00000000-0000-4000-8000-000000000001";
		const get = vi.fn(async () => fakeRun({ id: R1, status: "canceled" }));
		const { subscribe, emit } = fakeSubscribe();
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: () => true,
			subscribe,
			logger: quietLogger,
		});

		const received: string[][] = [];
		pubsub.onCancel(runIds => received.push(runIds));

		emit("mt_cancel_s_s", R1);
		emit("mt_cancel_s_s", R1);
		emit("mt_cancel_s_s", R1);
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();

		expect(get).toHaveBeenCalledTimes(1);
		expect(received).toEqual([[R1]]);
	});

	it("a status read failure warns without throwing; the pub/sub keeps working afterward", async () => {
		const R1 = "00000000-0000-4000-8000-000000000001";
		const { logger, messages } = warnCollector();
		const get = vi.fn(async (): Promise<RunRecord | null> => {
			throw new Error("connection reset");
		});
		const { subscribe, emit } = fakeSubscribe();
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: () => false,
			subscribe,
			logger,
		});

		const received: unknown[] = [];
		pubsub.subscribeStatus(R1, message => received.push(message));

		emit("mt_status_s_s", R1);
		await Promise.resolve();
		await Promise.resolve();

		expect(received).toEqual([]);
		expect(messages.some(m => /failed to re-read run/i.test(m.message))).toBe(true);

		// The pub/sub isn't wedged: a later, successful read still reaches the handler.
		get.mockResolvedValueOnce(fakeRun({ id: R1, status: "completed" }));
		emit("mt_status_s_s", R1);
		await Promise.resolve();
		await Promise.resolve();
		expect(received).toHaveLength(1);
	});

	it("a cancel read failure warns without throwing; the rest of the batch still works", async () => {
		const R1 = "00000000-0000-4000-8000-000000000001";
		const R2 = "00000000-0000-4000-8000-000000000002";
		const { logger, messages } = warnCollector();
		const get = vi.fn(async (runId: string) => {
			if (runId === R1) throw new Error("connection reset");
			return fakeRun({ id: R2, status: "canceled" });
		});
		const { subscribe, emit } = fakeSubscribe();
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: runId => runId === R1 || runId === R2,
			subscribe,
			logger,
		});

		const received: string[][] = [];
		pubsub.onCancel(runIds => received.push(runIds));

		// A read failure for R1 is warned and skipped; R2 (canceled) still reaches handlers.
		emit("mt_cancel_s_s", R1);
		emit("mt_cancel_s_s", R2);
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();

		expect(received).toEqual([[R2]]);
		expect(messages.some(m => /failed to re-read run/i.test(m.message))).toBe(true);
	});

	it("isActiveLocally throwing during cancel filtering warns and safely drops the whole batch", async () => {
		const R1 = "00000000-0000-4000-8000-000000000001";
		const R2 = "00000000-0000-4000-8000-000000000002";
		const { logger, messages } = warnCollector();
		const get = vi.fn(async (runId: string) => fakeRun({ id: runId, status: "canceled" }));
		const { subscribe, emit } = fakeSubscribe();
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: () => {
				throw new Error("isActiveLocally boom");
			},
			subscribe,
			logger,
		});

		const received: string[][] = [];
		pubsub.onCancel(runIds => received.push(runIds));

		// A throwing isActiveLocally must never become an unhandled rejection (vitest would
		// surface one and fail this test); the whole batch is safely dropped instead.
		emit("mt_cancel_s_s", R1);
		emit("mt_cancel_s_s", R2);
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();

		expect(received).toEqual([]);
		expect(get).not.toHaveBeenCalled();
		expect(messages.some(m => /isActiveLocally/i.test(m.message))).toBe(true);
	});

	it("safeCall warns instead of throwing when a handler rejects or throws synchronously", async () => {
		const R1 = "00000000-0000-4000-8000-000000000001";
		const { logger, messages } = warnCollector();
		const get = vi.fn(async () => fakeRun({ id: R1, status: "completed" }));
		const { subscribe, emit } = fakeSubscribe();
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: () => false,
			subscribe,
			logger,
		});

		const calls: unknown[] = [];
		// Typed `=> void`, but nothing stops a real caller from returning a rejecting promise or
		// throwing synchronously; both must be swallowed, never an unhandledRejection (vitest would
		// surface one and fail this test) or a thrown error that skips the other handlers.
		pubsub.subscribeStatus(R1, () => {
			calls.push("throws");
			throw new Error("handler boom");
		});
		pubsub.subscribeStatus(R1, () => Promise.reject(new Error("handler rejected")));
		pubsub.subscribeStatus(R1, message => {
			calls.push(message);
		});

		emit("mt_status_s_s", R1);
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();
		await Promise.resolve();

		// The other two handlers still ran despite the throwing/rejecting one; at least one of the
		// two misbehaviors was reported (the two warns are throttled together, so only one is
		// guaranteed to survive -- the safety property under test is "never thrown, never
		// unhandled", not "every distinct warning surfaces").
		expect(calls).toEqual(["throws", expect.objectContaining({ runId: R1 })]);
		expect(messages.length).toBeGreaterThan(0);
		expect(messages.some(m => /handler (threw|rejected)/i.test(m.message))).toBe(true);
	});
});
