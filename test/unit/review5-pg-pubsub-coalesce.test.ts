/**
 * Review 5 item B4: the Postgres pub/sub re-reads a run once per notification; a flood of
 * notifications for one run (e.g. progress updates) must coalesce: while a re-read is in flight,
 * further notifications for that id only mark it dirty, and one more re-read follows it.
 */
import { describe, expect, it, vi } from "vitest";
import { createPgPubSub } from "../../src/pg/pubsub.js";
import type { RunRecord } from "../../src/store/types.js";

const quietLogger = { debug() {}, info() {}, warn() {}, error() {} };
const R1 = "00000000-0000-4000-8000-000000000001";
const R2 = "00000000-0000-4000-8000-000000000002";

function fakeRun(id: string, status: RunRecord["status"], n: number): RunRecord {
	return {
		id,
		taskId: "t",
		queue: "q",
		payload: null,
		status,
		output: null,
		error: null,
		attempt: 1,
		maxAttempts: 1,
		executionId: null,
		enqueueSeq: 1,
		heartbeatAt: null,
		runAfter: null,
		parentRunId: null,
		parentWaitpointId: null,
		parentStepName: null,
		batchIndex: null,
		startedAt: null,
		finishedAt: null,
		createdAt: new Date(0),
		updatedAt: new Date(n),
		progress: n,
		debounceKey: null,
		concurrencyKey: null,
	};
}

/** A store whose `runs.get` stays pending until `release()`; each read sees the current `version`. */
function slowStore(status: RunRecord["status"]) {
	let version = 0;
	const waiting: Array<() => void> = [];
	const get = vi.fn(
		(runId: string) =>
			new Promise<RunRecord>(resolve => {
				waiting.push(() => resolve(fakeRun(runId, status, version)));
			})
	);
	return {
		store: { runs: { get } as never },
		get,
		bump: () => {
			version++;
		},
		/** Resolves every pending read, then lets the follow-ups start. */
		async release() {
			for (const resolve of waiting.splice(0)) resolve();
			for (let i = 0; i < 10; i++) await Promise.resolve();
		},
		pending: () => waiting.length,
	};
}

function harness(store: { runs: never }, isActiveLocally: (id: string) => boolean = () => false) {
	let emit: (channel: string, payload: string) => void = () => {};
	const pubsub = createPgPubSub({
		publisher: { publishStatus: async () => {}, publishCancel: async () => {} },
		store,
		schema: "s",
		isActiveLocally,
		subscribe: (_channels, onNotification) => {
			emit = onNotification;
			return () => {};
		},
		logger: quietLogger,
	});
	return { pubsub, emit: (channel: string, payload: string) => emit(channel, payload) };
}

const flush = async () => {
	for (let i = 0; i < 10; i++) await Promise.resolve();
};

describe("review 5 B4: pg pub/sub re-reads coalesce per run", () => {
	it("3000 status notifications for one run: one read in flight, one follow-up, latest state emitted", async () => {
		const s = slowStore("running");
		const { pubsub, emit } = harness(s.store);
		const received: unknown[] = [];
		pubsub.subscribeStatus(R1, m => received.push(m.progress));
		for (let i = 0; i < 3000; i++) {
			if (i === 1) s.bump();
			emit("mt_status_s_s", R1);
		}
		await flush();
		expect(s.get).toHaveBeenCalledTimes(1);
		await s.release();
		expect(s.get).toHaveBeenCalledTimes(2);
		await s.release();
		expect(s.get).toHaveBeenCalledTimes(2);
		expect(received.at(-1)).toBe(1);
	});

	it("different runs are read independently", async () => {
		const s = slowStore("running");
		const { pubsub, emit } = harness(s.store);
		pubsub.subscribeStatus(R1, () => {});
		pubsub.subscribeStatus(R2, () => {});
		emit("mt_status_s_s", R1);
		emit("mt_status_s_s", R2);
		emit("mt_status_s_s", R1);
		await flush();
		expect(s.get).toHaveBeenCalledTimes(2);
		await s.release();
		await s.release();
		expect(s.get).toHaveBeenCalledTimes(3);
	});

	it("cancel notifications for one run arriving in separate turns coalesce too", async () => {
		// Not (yet) canceled in the database: the dirty mark buys exactly one follow-up read.
		const s = slowStore("running");
		const { pubsub, emit } = harness(s.store, () => true);
		const received: string[][] = [];
		pubsub.onCancel(ids => received.push(ids));
		emit("mt_cancel_s_s", R1);
		await flush();
		for (let i = 0; i < 3000; i++) {
			emit("mt_cancel_s_s", R1);
			await Promise.resolve();
		}
		await flush();
		expect(s.get).toHaveBeenCalledTimes(1);
		await s.release();
		expect(s.get).toHaveBeenCalledTimes(2);
		await s.release();
		expect(s.get).toHaveBeenCalledTimes(2);
		expect(received).toEqual([]);
	});

	it("a cancel read that finds the run canceled needs no follow-up read", async () => {
		const s = slowStore("canceled");
		const { pubsub, emit } = harness(s.store, () => true);
		const received: string[][] = [];
		pubsub.onCancel(ids => received.push(ids));
		emit("mt_cancel_s_s", R1);
		await flush();
		for (let i = 0; i < 100; i++) {
			emit("mt_cancel_s_s", R1);
			await Promise.resolve();
		}
		await s.release();
		await s.release();
		expect(s.get).toHaveBeenCalledTimes(1);
		expect(received).toEqual([[R1]]);
	});
});
