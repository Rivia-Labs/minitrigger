import { describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import type { Transport } from "../../src/queue/transport.js";
import { createReconciler } from "../../src/reconciler.js";
import type { EnqueueRequest, Store } from "../../src/store/types.js";

const logger: EngineLogger = {
	debug: vi.fn(),
	info: vi.fn(),
	warn: vi.fn(),
	error: vi.fn(),
};

function staleItem(runId: string): EnqueueRequest {
	return {
		runId,
		queue: "q",
		seq: 1,
		delayMs: 0,
		status: "queued",
		updatedAt: new Date(),
	} as unknown as EnqueueRequest;
}

/** A store whose stale listings always return one item; everything else is empty. */
function fakeStore() {
	const page = (id: string) => async () => ({ items: [staleItem(id)], nextCursor: null });
	const runs = {
		listStaleQueued: vi.fn(page("11111111-1111-4111-8111-111111111111")),
		listStaleWaiting: vi.fn(page("22222222-2222-4222-8222-222222222222")),
		repairOrphanWaiting: vi.fn(async () => []),
		listStalled: vi.fn(async () => []),
		recoverStalled: vi.fn(),
	};
	const store = {
		runs,
		queues: { list: vi.fn(async () => []) },
		waitpoints: { listExpired: vi.fn(async () => []), expire: vi.fn() },
	} as unknown as Store;
	return { store, runs };
}

function fakeTransport() {
	const enqueueRun = vi.fn(async () => {});
	return { transport: { enqueueRun } as unknown as Transport, enqueueRun };
}

const base = { logger, graceMs: 0, stallTimeoutMs: 1_000, batchSize: 10 };

describe("createReconciler: recoverLostEnqueues", () => {
	it("defaults to true: cases 1/2 run and re-enqueue", async () => {
		const { store, runs } = fakeStore();
		const { transport, enqueueRun } = fakeTransport();
		const report = await createReconciler({ ...base, store, transport }).runOnce();
		expect(runs.listStaleQueued).toHaveBeenCalled();
		expect(runs.listStaleWaiting).toHaveBeenCalled();
		expect(enqueueRun).toHaveBeenCalledTimes(2);
		expect(report.staleQueued).toBe(1);
		expect(report.staleWaiting).toBe(1);
		expect(report.skippedLostEnqueues).toBeUndefined();
	});

	it("false (transport postgres): cases 1/2 are skipped and reported, the others still run", async () => {
		const { store, runs } = fakeStore();
		const { transport, enqueueRun } = fakeTransport();
		const report = await createReconciler({
			...base,
			store,
			transport,
			recoverLostEnqueues: false,
		}).runOnce();
		expect(runs.listStaleQueued).not.toHaveBeenCalled();
		expect(runs.listStaleWaiting).not.toHaveBeenCalled();
		expect(enqueueRun).not.toHaveBeenCalled();
		expect(report.staleQueued).toBe(0);
		expect(report.staleWaiting).toBe(0);
		expect(report.skippedLostEnqueues).toBe(true);
		// Case 3 (orphan) and case 4 (stall) are untouched.
		expect(runs.repairOrphanWaiting).toHaveBeenCalled();
		expect(runs.listStalled).toHaveBeenCalled();
		expect(report.errors).toBe(0);
	});
});
