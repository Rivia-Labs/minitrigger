/**
 * Store contract scenarios for debounce (decision 24) and concurrency keys (decision 25), run
 * against BOTH stores like `store-contract-suite.ts` (same harness): the SQL store in
 * test/integration/store-contract-sql.test.ts and the memory store in test/unit/memory-store.test.ts.
 */
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { NewRun, NewRunDebounce, Ownership } from "../../src/store/types.js";
import type { StoreHarness } from "./store-contract-suite.js";

/** Slack for the SQL store, whose `now()` keeps moving between statements. */
const SLACK_MS = 2_000;

export function defineDebounceConcurrencyContractSuite(get: () => StoreHarness): void {
	const s = () => get().store;

	function debounce(overrides: Partial<NewRunDebounce> = {}): NewRunDebounce {
		return {
			key: "k1",
			delayMs: 10_000,
			payload: "last",
			maxDelayMs: null,
			...overrides,
		};
	}

	function newRun(overrides: Partial<NewRun> = {}): NewRun {
		return {
			id: randomUUID(),
			taskId: "task-d",
			queue: "q",
			payload: { n: 0 },
			maxAttempts: 3,
			delayMs: 0,
			...overrides,
		};
	}

	/** `run_after - now()` of a run, in ms. */
	async function dueIn(runId: string): Promise<number> {
		const run = await s().runs.get(runId);
		if (!run?.runAfter) throw new Error(`run ${runId} has no run_after`);
		return run.runAfter.getTime() - (await get().now()).getTime();
	}

	describe("contract: debounced create (decision 24)", () => {
		it("a new debounced run stores the key, delay = debounce.delayMs (NewRun.delayMs ignored)", async () => {
			const res = await s().runs.create(newRun({ delayMs: 999_999, debounce: debounce() }));
			if (res.kind !== "created") throw new Error("expected created");
			expect(res.run.debounceKey).toBe("k1");
			expect(res.run.concurrencyKey).toBeNull();
			expect(res.enqueue.delayMs).toBeGreaterThan(10_000 - SLACK_MS);
			expect(res.enqueue.delayMs).toBeLessThanOrEqual(10_000);
		});

		it("trailing + last: collapses, replaces the payload, pushes run_after, bumps the seq", async () => {
			const first = await s().runs.create(newRun({ debounce: debounce() }));
			await get().advance(4_000);
			const second = await s().runs.create(newRun({ payload: { n: 1 }, debounce: debounce() }));
			if (second.kind !== "debounced") throw new Error(`expected debounced, got ${second.kind}`);
			expect(second.run.id).toBe(first.run.id);
			expect(second.staleSeq).toBe(1);
			expect(second.enqueue).toMatchObject({ runId: first.run.id, seq: 2, status: "queued" });
			expect(second.enqueue.delayMs).toBeGreaterThan(10_000 - SLACK_MS);
			expect(second.run.payload).toEqual({ n: 1 });
			expect(second.run.enqueueSeq).toBe(2);
			expect(second.run.status).toBe("queued");
			expect(second.run.attempt).toBe(1);
			const due = await dueIn(first.run.id);
			expect(due).toBeGreaterThan(10_000 - SLACK_MS);
			expect(due).toBeLessThanOrEqual(10_000);
			expect((await s().runs.list({ taskId: "task-d", limit: 10 })).items).toHaveLength(1);
		});

		it("payload first keeps the original payload", async () => {
			const first = await s().runs.create(newRun({ debounce: debounce({ payload: "first" }) }));
			const second = await s().runs.create(
				newRun({ payload: { n: 2 }, debounce: debounce({ payload: "first" }) })
			);
			expect(second.kind).toBe("debounced");
			expect((await s().runs.get(first.run.id))?.payload).toEqual({ n: 0 });
		});

		it("maxDelayMs caps the postponement at first trigger + maxDelay", async () => {
			const opts = debounce({ delayMs: 1_000, maxDelayMs: 1_500 });
			const first = await s().runs.create(newRun({ debounce: opts }));
			await get().advance(800);
			await s().runs.create(newRun({ debounce: opts }));
			// least(now + 1000, deadline) = deadline, 700 ms away.
			const due = await dueIn(first.run.id);
			expect(due).toBeGreaterThan(700 - SLACK_MS);
			expect(due).toBeLessThanOrEqual(700);
		});

		it("maxDelayMs smaller than delayMs bounds the first run_after too", async () => {
			const res = await s().runs.create(
				newRun({ debounce: debounce({ delayMs: 5_000, maxDelayMs: 1_000 }) })
			);
			expect(res.kind).toBe("created");
			expect(await dueIn(res.run.id)).toBeLessThanOrEqual(1_000);
		});

		it("the stale seq cannot acquire; the new seq acquires and releases the key", async () => {
			const first = await s().runs.create(newRun({ debounce: debounce() }));
			const second = await s().runs.create(newRun({ debounce: debounce() }));
			if (second.kind !== "debounced") throw new Error("expected debounced");
			expect(await s().runs.acquire(first.run.id, randomUUID(), second.staleSeq)).toBeNull();
			const run = await s().runs.acquire(first.run.id, randomUUID(), second.enqueue.seq);
			expect(run?.status).toBe("running");
			expect(run?.debounceKey).toBeNull();
			// Running: a new trigger creates a new pending run; the next one collapses into it.
			const third = await s().runs.create(newRun({ payload: { n: 3 }, debounce: debounce() }));
			expect(third.kind).toBe("created");
			expect(third.run.id).not.toBe(first.run.id);
			const fourth = await s().runs.create(newRun({ payload: { n: 4 }, debounce: debounce() }));
			expect(fourth.kind).toBe("debounced");
			expect(fourth.run.id).toBe(third.run.id);
			expect(fourth.run.payload).toEqual({ n: 4 });
		});

		it("a run back in queued after it started never absorbs triggers", async () => {
			const first = await s().runs.create(newRun({ debounce: debounce() }));
			const exec = randomUUID();
			await s().runs.acquire(first.run.id, exec, 1);
			const owner: Ownership = { runId: first.run.id, executionId: exec };
			const retried = await s().runs.retry(
				owner,
				{ name: "Error", message: "x", retryable: true },
				0
			);
			expect(retried.kind).toBe("retrying");
			const next = await s().runs.create(newRun({ debounce: debounce() }));
			expect(next.kind).toBe("created");
			expect(next.run.id).not.toBe(first.run.id);
		});

		it("keys are scoped by task; a canceled pending run releases its key", async () => {
			const a = await s().runs.create(newRun({ debounce: debounce() }));
			const b = await s().runs.create(newRun({ taskId: "task-other", debounce: debounce() }));
			expect(b.kind).toBe("created");
			expect(b.run.id).not.toBe(a.run.id);
			expect((await s().runs.cancel(a.run.id, "x")).kind).toBe("canceled");
			const c = await s().runs.create(newRun({ debounce: debounce() }));
			expect(c.kind).toBe("created");
			expect(c.run.id).not.toBe(a.run.id);
		});

		it("debounce together with idempotency is rejected", async () => {
			await expect(
				s().runs.create(newRun({ debounce: debounce(), idempotency: { key: "i", ttlMs: 1_000 } }))
			).rejects.toThrow(/debounce/);
		});

		it("concurrent debounced creates of one key: exactly one run", async () => {
			const results = await Promise.all(
				Array.from({ length: 6 }, (_, i) =>
					s().runs.create(newRun({ payload: { n: i }, debounce: debounce({ key: "race" }) }))
				)
			);
			expect(new Set(results.map(r => r.run.id)).size).toBe(1);
			expect(results.filter(r => r.kind === "created")).toHaveLength(1);
			expect(results.filter(r => r.kind === "debounced")).toHaveLength(5);
			const run = await s().runs.get(results[0]?.run.id ?? "");
			expect(run?.enqueueSeq).toBe(6);
		});

		it("inside a transaction the debounce collapse works too", async () => {
			const first = await s().runs.create(newRun({ debounce: debounce() }));
			await s().withTransaction(async tx => {
				const r = await tx.runs.create(newRun({ payload: { n: 9 }, debounce: debounce() }));
				expect(r.kind).toBe("debounced");
				expect(r.run.id).toBe(first.run.id);
			});
			expect((await s().runs.get(first.run.id))?.payload).toEqual({ n: 9 });
		});

		it("collapse-vs-acquire race: a debounced create blocks behind an in-flight acquire, then makes a new run", async () => {
			const firstResult = await s().runs.create(newRun({ debounce: debounce() }));
			if (firstResult.kind !== "created") throw new Error("expected created");
			const first = firstResult;
			let releaseTx: () => void = () => {};
			const gate = new Promise<void>(resolve => {
				releaseTx = resolve;
			});
			const txDone = s().withTransaction(async tx => {
				const acquired = await tx.runs.acquire(first.run.id, randomUUID(), first.enqueue.seq);
				expect(acquired?.status).toBe("running");
				// Hold the transaction open (uncommitted) until the test releases it below, so a
				// concurrent debounced create of the same key has to wait on the row lock.
				await gate;
			});
			// Give the acquire time to take the row lock before racing the create.
			await new Promise(resolve => setTimeout(resolve, 50));
			let createSettled = false;
			const createDone = s()
				.runs.create(newRun({ payload: { n: 42 }, debounce: debounce() }))
				.then(r => {
					createSettled = true;
					return r;
				});
			await new Promise(resolve => setTimeout(resolve, 200));
			expect(createSettled).toBe(false);
			releaseTx();
			await txDone;
			const second = await createDone;
			expect(second.kind).toBe("created");
			expect(second.run.id).not.toBe(first.run.id);
			const originalRow = await s().runs.get(first.run.id);
			expect(originalRow?.status).toBe("running");
			expect(originalRow?.enqueueSeq).toBe(first.enqueue.seq);
		});
	});

	describe("contract: acquireWithConcurrencyLimit (decision 25)", () => {
		const opts = { limit: 1 };

		async function created(overrides: Partial<NewRun> = {}) {
			const res = await s().runs.create(newRun(overrides));
			if (res.kind !== "created") throw new Error("expected created");
			return res;
		}

		it("stores the key; a run without key is acquired normally", async () => {
			const keyed = await created({ concurrencyKey: "c1" });
			expect(keyed.run.concurrencyKey).toBe("c1");
			const plain = await created();
			const r = await s().runs.acquireWithConcurrencyLimit(plain.run.id, randomUUID(), 1, opts);
			expect(r.kind).toBe("acquired");
			if (r.kind === "acquired") expect(r.run.status).toBe("running");
			const again = await s().runs.acquireWithConcurrencyLimit(plain.run.id, randomUUID(), 1, opts);
			expect(again.kind).toBe("stale");
		});

		it("limit reached -> limited: seq+1, run_after = now + backoff, attempt/status unchanged", async () => {
			const a = await created({ concurrencyKey: "c1" });
			const b = await created({ concurrencyKey: "c1" });
			const ra = await s().runs.acquireWithConcurrencyLimit(a.run.id, randomUUID(), 1, opts);
			expect(ra.kind).toBe("acquired");
			const rb = await s().runs.acquireWithConcurrencyLimit(b.run.id, randomUUID(), 1, opts);
			if (rb.kind !== "limited") throw new Error(`expected limited, got ${rb.kind}`);
			expect(rb.enqueue).toMatchObject({ runId: b.run.id, seq: 2, status: "queued", queue: "q" });
			// First limited attempt: base backoff (1 s) + jitter (<= 250 ms).
			expect(rb.enqueue.delayMs).toBeGreaterThan(1_000 - SLACK_MS);
			expect(rb.enqueue.delayMs).toBeLessThanOrEqual(1_250);
			const bRow = await s().runs.get(b.run.id);
			expect(bRow).toMatchObject({
				status: "queued",
				attempt: 1,
				enqueueSeq: 2,
				executionId: null,
			});
			// The old seq is now stale.
			expect(
				(await s().runs.acquireWithConcurrencyLimit(b.run.id, randomUUID(), 1, opts)).kind
			).toBe("stale");
			// Other key, other queue: independent.
			const c = await created({ concurrencyKey: "c2" });
			expect(
				(await s().runs.acquireWithConcurrencyLimit(c.run.id, randomUUID(), 1, opts)).kind
			).toBe("acquired");
			const d = await created({ queue: "q2", concurrencyKey: "c1" });
			expect(
				(await s().runs.acquireWithConcurrencyLimit(d.run.id, randomUUID(), 1, opts)).kind
			).toBe("acquired");
			// A finished run frees its slot.
			if (ra.kind !== "acquired") throw new Error("unreachable");
			await s().runs.complete({ runId: a.run.id, executionId: ra.run.executionId ?? "" }, null);
			const rb2 = await s().runs.acquireWithConcurrencyLimit(b.run.id, randomUUID(), 2, opts);
			expect(rb2.kind).toBe("acquired");
		});

		it("limit 2 allows two; a waiting run of the key does not hold a slot", async () => {
			const two = { limit: 2 };
			const runs = [];
			for (let i = 0; i < 3; i++) runs.push(await created({ concurrencyKey: "c3" }));
			const [r0, r1, r2] = runs;
			if (!r0 || !r1 || !r2) throw new Error("unreachable");
			const e0 = randomUUID();
			expect((await s().runs.acquireWithConcurrencyLimit(r0.run.id, e0, 1, two)).kind).toBe(
				"acquired"
			);
			expect(
				(await s().runs.acquireWithConcurrencyLimit(r1.run.id, randomUUID(), 1, two)).kind
			).toBe("acquired");
			expect(
				(await s().runs.acquireWithConcurrencyLimit(r2.run.id, randomUUID(), 1, two)).kind
			).toBe("limited");
			// r0 suspends on a sleep-less resume: it becomes queued again (requeued), freeing a slot.
			const susp = await s().runs.suspend({ runId: r0.run.id, executionId: e0 });
			expect(susp.kind).toBe("requeued");
			expect(
				(await s().runs.acquireWithConcurrencyLimit(r2.run.id, randomUUID(), 2, two)).kind
			).toBe("acquired");
		});

		it("concurrent acquisitions of one key never exceed the limit", async () => {
			const runs = [];
			for (let i = 0; i < 6; i++) runs.push(await created({ concurrencyKey: "c4" }));
			const results = await Promise.all(
				runs.map(r => s().runs.acquireWithConcurrencyLimit(r.run.id, randomUUID(), 1, { limit: 2 }))
			);
			expect(results.filter(r => r.kind === "acquired")).toHaveLength(2);
			expect(results.filter(r => r.kind === "limited")).toHaveLength(4);
		});

		it("a limited waiting run stays waiting", async () => {
			const a = await created({ concurrencyKey: "c5" });
			const b = await created({ concurrencyKey: "c5" });
			await s().runs.acquireWithConcurrencyLimit(a.run.id, randomUUID(), 1, opts);
			await get().patchRun(b.run.id, { status: "waiting" });
			const r = await s().runs.acquireWithConcurrencyLimit(b.run.id, randomUUID(), 1, opts);
			if (r.kind !== "limited") throw new Error("expected limited");
			expect(r.enqueue.status).toBe("waiting");
			expect((await s().runs.get(b.run.id))?.status).toBe("waiting");
		});

		it("consecutive limited attempts grow the delay; acquiring resets it (M1)", async () => {
			const a = await created({ concurrencyKey: "c6" });
			const b = await created({ concurrencyKey: "c6" });
			const ea = await s().runs.acquireWithConcurrencyLimit(a.run.id, randomUUID(), 1, opts);
			if (ea.kind !== "acquired") throw new Error("expected acquired");

			// b is limited three times in a row while a keeps holding the key: roughly doubling.
			const delays: number[] = [];
			let seq = 1;
			for (let i = 0; i < 3; i++) {
				const r = await s().runs.acquireWithConcurrencyLimit(b.run.id, randomUUID(), seq, opts);
				if (r.kind !== "limited") throw new Error(`expected limited, got ${r.kind}`);
				delays.push(r.enqueue.delayMs);
				seq = r.enqueue.seq;
			}
			expect(delays[0]).toBeGreaterThan(1_000 - SLACK_MS);
			expect(delays[0]).toBeLessThanOrEqual(1_250);
			expect(delays[1]).toBeGreaterThan(2_000 - SLACK_MS);
			expect(delays[1]).toBeLessThanOrEqual(2_250);
			expect(delays[2]).toBeGreaterThan(4_000 - SLACK_MS);
			expect(delays[2]).toBeLessThanOrEqual(4_250);

			// a finishes, freeing the slot: b's next (still-limited-seq) attempt acquires, resetting
			// its own concurrency_waits back to 0.
			await s().runs.complete({ runId: a.run.id, executionId: ea.run.executionId ?? "" }, null);
			const acquiredB = await s().runs.acquireWithConcurrencyLimit(
				b.run.id,
				randomUUID(),
				seq,
				opts
			);
			if (acquiredB.kind !== "acquired")
				throw new Error(`expected acquired, got ${acquiredB.kind}`);

			// b later retries (simulating a failure), which frees the slot; a fresh run c then takes
			// it, and b's next attempt is limited again: the delay starts back at the base, not
			// continuing from the 4 s+ it reached earlier (its `concurrency_waits` reset at
			// `acquiredB` above).
			const owner: Ownership = { runId: b.run.id, executionId: acquiredB.run.executionId ?? "" };
			const retried = await s().runs.retry(
				owner,
				{ name: "Error", message: "x", retryable: true },
				0
			);
			if (retried.kind !== "retrying") throw new Error("expected retrying");
			const c = await created({ concurrencyKey: "c6" });
			const ec = await s().runs.acquireWithConcurrencyLimit(c.run.id, randomUUID(), 1, opts);
			expect(ec.kind).toBe("acquired");
			const rb2 = await s().runs.acquireWithConcurrencyLimit(
				b.run.id,
				randomUUID(),
				retried.enqueue.seq,
				opts
			);
			if (rb2.kind !== "limited") throw new Error(`expected limited, got ${rb2.kind}`);
			expect(rb2.enqueue.delayMs).toBeGreaterThan(1_000 - SLACK_MS);
			expect(rb2.enqueue.delayMs).toBeLessThanOrEqual(1_250);
		});
	});
}
