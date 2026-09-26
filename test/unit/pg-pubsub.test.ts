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

		// maxItems reached: sent right away, without waiting for the (never-fired) window timer.
		await batcher.flush();
		expect(query).toHaveBeenCalledTimes(1);
		expect(query).toHaveBeenCalledWith(expect.stringContaining("pg_notify"), [
			["a", "b"],
			["1", "2"],
		]);
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
		const { subscribe, emit, isUnsubscribed } = fakeSubscribe();
		const get = vi.fn(async () => fakeRun());
		const pubsub = createPgPubSub({
			publisher: noopPublisher(),
			store: { runs: { get } as never },
			schema: "s",
			isActiveLocally: () => true,
			subscribe,
			logger: quietLogger,
		});

		const received: unknown[] = [];
		pubsub.subscribeStatus("r1", message => received.push(message));

		await pubsub.close();
		expect(isUnsubscribed()).toBe(true);

		emit("mt_status_s_s", "r1");
		await Promise.resolve();
		await Promise.resolve();
		expect(received).toEqual([]);
	});
});
