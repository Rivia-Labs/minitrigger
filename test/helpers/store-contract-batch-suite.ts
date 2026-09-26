/**
 * Store contract scenarios of `runs.listBatchCandidates` (`runs.cancelMany`/`runs.retryFailed`)
 * and `queues` (decision 27, `queue_state`), run against both stores like the main suite.
 */
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
	MAX_BATCH_LIMIT,
	type NewRun,
	type Ownership,
	retryIdempotencyKey,
	type SerializedError,
	type Store,
} from "../../src/store/types.js";
import type { StoreHarness } from "./store-contract-suite.js";

const ERR: SerializedError = { name: "Error", message: "boom", retryable: false };

export function defineBatchQueueContractSuite(get: () => StoreHarness): void {
	const s = (): Store => get().store;

	function newRun(overrides: Partial<NewRun> = {}): NewRun {
		return {
			id: randomUUID(),
			taskId: "task-a",
			queue: "default",
			payload: { n: 1 },
			maxAttempts: 1,
			delayMs: 0,
			...overrides,
		};
	}

	/** Creates a run and advances the clock 1 ms, so `created_at` orders the runs. */
	async function created(overrides: Partial<NewRun> = {}): Promise<string> {
		const res = await s().runs.create(newRun(overrides));
		if (res.kind !== "created") throw new Error("expected created");
		return res.run.id;
	}

	async function acquire(runId: string): Promise<Ownership> {
		const run = await s().runs.get(runId);
		if (!run) throw new Error("no run");
		const executionId = randomUUID();
		if (!(await s().runs.acquire(runId, executionId, run.enqueueSeq))) throw new Error("acquire");
		return { runId, executionId };
	}

	async function failed(overrides: Partial<NewRun> = {}): Promise<string> {
		const id = await created(overrides);
		const owner = await acquire(id);
		expect((await s().runs.fail(owner, ERR)).kind).toBe("failed");
		return id;
	}

	/** `n` runs with strictly increasing `created_at` (the memory clock is frozen otherwise). */
	async function sequence(n: number, make: () => Promise<string>): Promise<string[]> {
		const ids: string[] = [];
		for (let i = 0; i < n; i++) {
			ids.push(await make());
			await tick();
		}
		return ids;
	}

	/** Moves the clock (SQL: shifts the stored rows back), so the next run is strictly newer. */
	async function tick(): Promise<void> {
		await get().advance(5);
	}

	describe("contract: listBatchCandidates", () => {
		it("cancel: only non-terminal runs, oldest first, paged with a cursor", async () => {
			const ids = await sequence(5, () => created());
			const done = await failed();
			const running = await created();
			await acquire(running);

			const page1 = await s().runs.listBatchCandidates({ purpose: "cancel", limit: 3 });
			expect(page1.items.map(r => r.id)).toEqual(ids.slice(0, 3));
			expect(page1.nextCursor).not.toBeNull();
			const page2 = await s().runs.listBatchCandidates({
				purpose: "cancel",
				limit: 3,
				...(page1.nextCursor ? { cursor: page1.nextCursor } : {}),
			});
			expect(page2.items.map(r => r.id)).toEqual([...ids.slice(3), running]);
			expect(page2.nextCursor).toBeNull();
			expect(page2.items.map(r => r.id)).not.toContain(done);
		});

		it("an exactly full last page reports no next cursor", async () => {
			const ids = await sequence(2, () => created());
			const page = await s().runs.listBatchCandidates({ purpose: "cancel", limit: 2 });
			expect(page.items.map(r => r.id)).toEqual(ids);
			expect(page.nextCursor).toBeNull();
		});

		it("filters by task, queue and creation window", async () => {
			const a = await created({ taskId: "t1", queue: "q1" });
			await tick();
			const b = await created({ taskId: "t1", queue: "q2" });
			await created({ taskId: "t2", queue: "q1" });
			const bAt = (await s().runs.get(b))?.createdAt;
			if (!bAt) throw new Error("no createdAt");

			const byTask = await s().runs.listBatchCandidates({
				purpose: "cancel",
				limit: 10,
				taskId: "t1",
			});
			expect(byTask.items.map(r => r.id)).toEqual([a, b]);
			const byQueue = await s().runs.listBatchCandidates({
				purpose: "cancel",
				limit: 10,
				queue: "q2",
			});
			expect(byQueue.items.map(r => r.id)).toEqual([b]);
			const before = await s().runs.listBatchCandidates({
				purpose: "cancel",
				limit: 10,
				taskId: "t1",
				createdTo: bAt,
			});
			expect(before.items.map(r => r.id)).toEqual([a]);
			const from = await s().runs.listBatchCandidates({
				purpose: "cancel",
				limit: 10,
				taskId: "t1",
				createdFrom: bAt,
			});
			expect(from.items.map(r => r.id)).toEqual([b]);
		});

		it("retry: only failed top-level runs not retried yet", async () => {
			const f1 = await failed();
			await tick();
			const f2 = await failed();
			await tick();
			await created(); // queued: not a candidate
			// A failed child (has a parent): not a candidate.
			const parent = await acquire(await created({ taskId: "parent" }));
			const childId = randomUUID();
			const spawned = await s().runs.triggerChildren({
				parent,
				stepName: "c",
				seqIndex: 0,
				mode: "trigger",
				waitpointId: randomUUID(),
				timeoutMs: null,
				children: [
					{
						id: childId,
						taskId: "task-a",
						queue: "default",
						payload: null,
						maxAttempts: 1,
						idempotencyKey: `__child:${parent.runId}:c`,
						batchIndex: null,
					},
				],
			});
			expect(spawned.kind).toBe("created");
			expect((await s().runs.fail(await acquire(childId), ERR)).kind).toBe("failed");

			const before = await s().runs.listBatchCandidates({ purpose: "retry", limit: 10 });
			expect(before.items.map(r => r.id)).toEqual([f1, f2]);

			// Retrying f1 claims its reserved key: it is no longer a candidate.
			const retry = await s().runs.create(
				newRun({ idempotency: { key: retryIdempotencyKey(f1), ttlMs: null } })
			);
			expect(retry.kind).toBe("created");
			const again = await s().runs.create(
				newRun({ idempotency: { key: retryIdempotencyKey(f1), ttlMs: null } })
			);
			expect(again.kind).toBe("existing");
			const after = await s().runs.listBatchCandidates({ purpose: "retry", limit: 10 });
			expect(after.items.map(r => r.id)).toEqual([f2]);
		});

		it("clamps the limit to MAX_BATCH_LIMIT and at least 1", async () => {
			await created();
			await tick();
			await created();
			const one = await s().runs.listBatchCandidates({ purpose: "cancel", limit: 0 });
			expect(one.items).toHaveLength(1);
			expect(one.nextCursor).not.toBeNull();
			const big = await s().runs.listBatchCandidates({
				purpose: "cancel",
				limit: MAX_BATCH_LIMIT * 10,
			});
			expect(big.items).toHaveLength(2);
		});
	});

	describe("contract: queue state (decision 27)", () => {
		it("setPaused upserts, reports changes, lists ordered by queue", async () => {
			expect(await s().queues.get("emails")).toBeNull();
			expect(await s().queues.list()).toEqual([]);

			const first = await s().queues.setPaused("emails", true);
			expect(first.changed).toBe(true);
			expect(first.record).toMatchObject({ queue: "emails", paused: true });
			expect(first.record.updatedAt).toBeInstanceOf(Date);

			expect((await s().queues.setPaused("emails", true)).changed).toBe(false);
			// A missing row already means "not paused".
			expect((await s().queues.setPaused("billing", false)).changed).toBe(false);
			const resumed = await s().queues.setPaused("emails", false);
			expect(resumed.changed).toBe(true);
			expect(resumed.record.paused).toBe(false);

			expect((await s().queues.list()).map(q => [q.queue, q.paused])).toEqual([
				["billing", false],
				["emails", false],
			]);
			expect(await s().queues.get("emails")).toMatchObject({ queue: "emails", paused: false });
		});

		it("is available inside withTransaction and rolls back", async () => {
			await expect(
				s().withTransaction(async tx => {
					await tx.queues.setPaused("q", true);
					expect((await tx.queues.get("q"))?.paused).toBe(true);
					throw new Error("rollback");
				})
			).rejects.toThrow("rollback");
			expect(await s().queues.get("q")).toBeNull();
		});
	});
}
