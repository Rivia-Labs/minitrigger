import { randomUUID } from "node:crypto";
import { beforeEach, describe, expect, it } from "vitest";
import { createMemoryStore, memoryStoreTestHooks } from "../../src/store/memory/index.js";
import type { Json, Ownership, Store } from "../../src/store/types.js";
import { defineStoreContractSuite, type StoreHarness } from "../helpers/store-contract-suite.js";

/** Manual clock: the in-memory store's "DB clock". */
function manualClock(start = Date.UTC(2026, 0, 1, 12)) {
	let t = start;
	return {
		now: () => t,
		advance(ms: number) {
			t += ms;
		},
	};
}

let harness: StoreHarness;

beforeEach(() => {
	const clock = manualClock();
	const store = createMemoryStore({ now: clock.now });
	const hooks = memoryStoreTestHooks(store);
	if (!hooks) throw new Error("no test hooks");
	harness = {
		store,
		async advance(ms) {
			clock.advance(ms);
		},
		async now() {
			return new Date(clock.now());
		},
		patchRun: (runId, patch) => hooks.patchRun(runId, patch),
	};
});

defineStoreContractSuite(() => harness);

// ---------------------------------------------------------------------------
// Memory-only behavior
// ---------------------------------------------------------------------------

async function runningIn(store: Store, payload: Json = null) {
	const created = await store.runs.create({
		id: randomUUID(),
		taskId: "t",
		queue: "default",
		payload,
		maxAttempts: 1,
		delayMs: 0,
	});
	if (created.kind !== "created") throw new Error("expected created");
	const executionId = randomUUID();
	await store.runs.acquire(created.run.id, executionId, 1);
	return { runId: created.run.id, executionId } as Ownership;
}

describe("memory store: clock", () => {
	it("uses the injected clock for every timestamp and truncates created_at to ms", async () => {
		let t = 1_000_000.75;
		const store = createMemoryStore({ now: () => t });
		const res = await store.runs.create({
			id: randomUUID(),
			taskId: "t",
			queue: "q",
			payload: null,
			maxAttempts: 1,
			delayMs: 500,
		});
		expect(res.run.createdAt.getTime()).toBe(1_000_000);
		expect(res.run.runAfter?.getTime()).toBe(1_000_500);
		if (res.kind !== "created") throw new Error("expected created");
		expect(res.enqueue.delayMs).toBe(500);
		t = 2_000_000;
		const snap = await store.steps.loadForExecution(res.run.id, 1);
		expect(snap.now.getTime()).toBe(2_000_000);
	});

	it("defaults to Date.now", async () => {
		const store = createMemoryStore();
		const before = Date.now();
		const res = await store.runs.create({
			id: randomUUID(),
			taskId: "t",
			queue: "q",
			payload: null,
			maxAttempts: 1,
			delayMs: 0,
		});
		expect(res.run.createdAt.getTime()).toBeGreaterThanOrEqual(before);
		expect(res.run.createdAt.getTime()).toBeLessThanOrEqual(Date.now());
	});

	it("now() is fixed for a whole transaction (Postgres transaction start time)", async () => {
		let t = 5_000;
		const store = createMemoryStore({ now: () => t });
		const created = await store.withTransaction(async tx => {
			const a = await tx.runs.create({
				id: randomUUID(),
				taskId: "t",
				queue: "q",
				payload: null,
				maxAttempts: 1,
				delayMs: 0,
			});
			t = 9_000;
			const b = await tx.runs.create({
				id: randomUUID(),
				taskId: "t",
				queue: "q",
				payload: null,
				maxAttempts: 1,
				delayMs: 0,
			});
			return [a.run.createdAt.getTime(), b.run.createdAt.getTime()];
		});
		expect(created).toEqual([5_000, 5_000]);
	});
});

describe("memory store: value isolation", () => {
	it("deep-copies values in and out", async () => {
		const store = createMemoryStore();
		const payload = { nested: { list: [1, 2] } };
		const owner = await runningIn(store, payload);
		payload.nested.list.push(3);
		const run = await store.runs.get(owner.runId);
		expect(run?.payload).toEqual({ nested: { list: [1, 2] } });
		if (!run) throw new Error("run not found");
		(run.payload as { nested: { list: number[] } }).nested.list.push(99);
		expect((await store.runs.get(owner.runId))?.payload).toEqual({ nested: { list: [1, 2] } });

		const output = { v: [1] };
		await store.steps.insertCompleted(owner, { kind: "run", name: "a", seqIndex: 0, output });
		output.v.push(2);
		const [step] = await store.steps.list(owner.runId);
		expect(step?.output).toEqual({ v: [1] });
		if (!step) throw new Error("step not found");
		(step.output as { v: number[] }).v.push(3);
		expect((await store.steps.loadOutputs(owner.runId, 0, 0))[0]?.output).toEqual({ v: [1] });
	});

	it("stores objects in jsonb key order", async () => {
		const store = createMemoryStore();
		const owner = await runningIn(store, { bbb: 1, a: 2, cc: 3 });
		expect(Object.keys((await store.runs.get(owner.runId))?.payload as object)).toEqual([
			"a",
			"cc",
			"bbb",
		]);
	});

	it("each store instance is isolated", async () => {
		const a = createMemoryStore();
		const b = createMemoryStore();
		const owner = await runningIn(a);
		expect(await b.runs.get(owner.runId)).toBeNull();
		expect(memoryStoreTestHooks(a)).not.toBeNull();
	});
});

describe("memory store: transactions and serialization", () => {
	it("a transaction blocks other operations until it settles", async () => {
		const store = createMemoryStore();
		const owner = await runningIn(store);
		let release!: () => void;
		const gate = new Promise<void>(resolve => {
			release = resolve;
		});
		const order: string[] = [];
		const txDone = store.withTransaction(async tx => {
			await tx.runs.complete(owner, "tx");
			order.push("tx-write");
			await gate;
			order.push("tx-end");
		});
		const read = store.runs.get(owner.runId).then(r => {
			order.push(`read:${r?.status}`);
		});
		await new Promise(resolve => setTimeout(resolve, 10));
		expect(order).toEqual(["tx-write"]);
		release();
		await Promise.all([txDone, read]);
		expect(order).toEqual(["tx-write", "tx-end", "read:completed"]);
	});

	it("using the pool-bound store inside withTransaction throws instead of deadlocking", async () => {
		const store = createMemoryStore();
		await expect(
			store.withTransaction(async () => {
				await store.runs.get(randomUUID());
			})
		).rejects.toThrow(/inside withTransaction/);
		await expect(
			store.withTransaction(async () => store.withTransaction(async () => 1))
		).rejects.toThrow(/inside withTransaction/);
		// The mutex was released: the store still works.
		expect(await store.runs.get(randomUUID())).toBeNull();
	});

	it("tx repositories are unusable after the transaction settles", async () => {
		const store = createMemoryStore();
		let leaked: Parameters<Parameters<Store["withTransaction"]>[0]>[0] | undefined;
		await store.withTransaction(async tx => {
			leaked = tx;
		});
		await expect(leaked?.runs.get(randomUUID())).rejects.toThrow(/closed/);
	});

	it("a failing standalone operation leaves no partial writes", async () => {
		const store = createMemoryStore();
		const owner = await runningIn(store);
		const waitpointId = randomUUID();
		const child = (i: number) => ({
			id: randomUUID(),
			taskId: "c",
			queue: "default",
			payload: null,
			maxAttempts: 1,
			idempotencyKey: "same-key",
			batchIndex: i,
		});
		await expect(
			store.runs.triggerChildren({
				parent: owner,
				stepName: "b",
				seqIndex: 0,
				mode: "batch",
				waitpointId,
				timeoutMs: null,
				children: [child(0), child(1)],
			})
		).rejects.toThrow(/duplicate key/);
		expect((await store.runs.list({ limit: 100 })).items).toHaveLength(1);
		expect(await store.waitpoints.getToken(waitpointId)).toBeNull();
		expect(await store.steps.list(owner.runId)).toEqual([]);
	});

	it("log ids behave like a sequence (not reused after a rollback)", async () => {
		const store = createMemoryStore();
		const owner = await runningIn(store);
		const entry = { runId: owner.runId, level: "info" as const, message: "m", data: null };
		await store.logs.insertBatch([entry]);
		await expect(
			store.withTransaction(async tx => {
				await tx.logs.insertBatch([entry]);
				throw new Error("rollback");
			})
		).rejects.toThrow("rollback");
		await store.logs.insertBatch([entry]);
		expect((await store.logs.list(owner.runId, { limit: 10 })).map(l => l.id)).toEqual(["1", "3"]);
	});
});
