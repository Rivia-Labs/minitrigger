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
			if (second.enqueue === null) throw new Error("expected an enqueue: run_after moved");
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

		it("a pending run already due no longer absorbs a trigger: its key is released and a new run opens", async () => {
			const first = await s().runs.create(newRun({ payload: { n: 1 }, debounce: debounce() }));
			if (first.kind !== "created") throw new Error("expected created");
			// Its delay has elapsed (run_after = now() - 1s) but nothing acquired it yet.
			await get().advance(11_000);
			const before = await s().runs.get(first.run.id);

			const second = await s().runs.create(newRun({ payload: { n: 2 }, debounce: debounce() }));
			if (second.kind !== "created") throw new Error(`expected created, got ${second.kind}`);
			expect(second.run.id).not.toBe(first.run.id);
			expect(second.run.debounceKey).toBe("k1");
			expect(second.run.payload).toEqual({ n: 2 });
			expect(second.enqueue.delayMs).toBeGreaterThan(10_000 - SLACK_MS);

			const old = await s().runs.get(first.run.id);
			expect(old?.debounceKey).toBeNull();
			expect(old?.status).toBe("queued");
			expect(old?.payload).toEqual({ n: 1 });
			expect(old?.runAfter?.getTime()).toBe(before?.runAfter?.getTime());
			expect(old?.enqueueSeq).toBe(before?.enqueueSeq);

			// The next trigger collapses into the new run, never into the due one.
			const third = await s().runs.create(newRun({ payload: { n: 3 }, debounce: debounce() }));
			if (third.kind !== "debounced") throw new Error(`expected debounced, got ${third.kind}`);
			expect(third.run.id).toBe(second.run.id);
			expect((await s().runs.get(first.run.id))?.payload).toEqual({ n: 1 });
		});

		it("maxDelay reached: the pinned run stops absorbing once due and the key rolls to a new run", async () => {
			const opts = debounce({ delayMs: 1_000, maxDelayMs: 3_000 });
			const first = await s().runs.create(newRun({ debounce: opts }));
			for (let i = 0; i < 3; i++) {
				await get().advance(900);
				const collapsed = await s().runs.create(newRun({ payload: { n: i }, debounce: opts }));
				expect(collapsed.run.id).toBe(first.run.id);
			}
			await get().advance(1_000); // past the 3 s deadline
			const next = await s().runs.create(newRun({ payload: { n: 9 }, debounce: opts }));
			expect(next.kind).toBe("created");
			expect(next.run.id).not.toBe(first.run.id);
			expect((await s().runs.get(first.run.id))?.debounceKey).toBeNull();
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

		it("collapse after run_after is pinned at the deadline doesn't bump the seq again (audit item 1)", async () => {
			const opts = debounce({ delayMs: 1_000, maxDelayMs: 1_200 });
			const first = await s().runs.create(newRun({ debounce: opts }));
			expect(first.kind).toBe("created");
			// Push past the deadline: run_after actually changes (now+1000 -> the pinned deadline),
			// so this collapse legitimately bumps the seq and must be re-enqueued.
			await get().advance(300);
			const pinning = await s().runs.create(newRun({ payload: { n: 1 }, debounce: opts }));
			if (pinning.kind !== "debounced") throw new Error(`expected debounced, got ${pinning.kind}`);
			expect(pinning.enqueue).not.toBeNull();
			expect(pinning.run.enqueueSeq).toBe(2);
			// Further collapses before the deadline recompute the SAME pinned run_after: nothing to
			// enqueue, so the seq must stay put (the bug: it used to bump every time, fencing the
			// job that's already correctly scheduled for the deadline before it can be acquired).
			// `advance()` itself shifts every stored timestamp (including this run's `run_after` and
			// `debounce_deadline`, together, on the SQL store -- see the harness above) to simulate
			// time passing, so each "unchanged" check below compares against a value read AFTER that
			// shift but BEFORE the create() call, isolating the collapse's own effect from advance()'s.
			await get().advance(100);
			const beforeAgain = (await s().runs.get(first.run.id))?.runAfter?.getTime();
			const again = await s().runs.create(newRun({ payload: { n: 2 }, debounce: opts }));
			if (again.kind !== "debounced") throw new Error(`expected debounced, got ${again.kind}`);
			expect(again.enqueue).toBeNull();
			expect(again.run.enqueueSeq).toBe(2);
			expect(again.run.payload).toEqual({ n: 2 });
			expect((await s().runs.get(first.run.id))?.runAfter?.getTime()).toBe(beforeAgain);
			await get().advance(50);
			const beforeYetAgain = (await s().runs.get(first.run.id))?.runAfter?.getTime();
			const yetAgain = await s().runs.create(newRun({ payload: { n: 3 }, debounce: opts }));
			if (yetAgain.kind !== "debounced")
				throw new Error(`expected debounced, got ${yetAgain.kind}`);
			expect(yetAgain.enqueue).toBeNull();
			expect(yetAgain.run.enqueueSeq).toBe(2);
			expect((await s().runs.get(first.run.id))?.runAfter?.getTime()).toBe(beforeYetAgain);
			// The still-current seq (2) can acquire; nothing stale was ever produced by the no-op
			// collapses.
			expect(await s().runs.acquire(first.run.id, randomUUID(), 2)).not.toBeNull();
		});

		it("the stale seq cannot acquire; the new seq acquires and releases the key", async () => {
			const first = await s().runs.create(newRun({ debounce: debounce() }));
			// Moves run_after (not just the clock staying put), so this collapse legitimately bumps
			// the seq -- the scenario this test is about (a stale job left over from a real reschedule).
			await get().advance(1_000);
			const second = await s().runs.create(newRun({ debounce: debounce() }));
			if (second.kind !== "debounced") throw new Error("expected debounced");
			if (second.enqueue === null) throw new Error("expected an enqueue: run_after moved");
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
			const runId = results[0]?.run.id ?? "";
			const run = await s().runs.get(runId);
			if (!run) throw new Error("run disappeared");
			// How many of the 6 serialized collapses actually moved `run_after` (and so bumped the
			// seq, audit item 1) is a clock-resolution artifact: real Postgres' `now()` ticks
			// between statements, the memory store's injected clock does not move on its own between
			// transactions (docs/store-contract.md). What must hold regardless of that: the run's
			// CURRENT seq is always the live, acquirable one.
			expect(await s().runs.acquire(runId, randomUUID(), run.enqueueSeq)).not.toBeNull();
		});

		it("a debounce collapse never resets concurrency_waits, nor pulls run_after earlier than a pending backoff (audit item 1, review item 4)", async () => {
			// Verifies the audit's secondary concern: does the collapse UPDATE reset backoff for a
			// run that also carries a concurrencyKey? It must not -- `concurrency_waits` is reset
			// only by `acquireWithConcurrencyLimit`'s own acquire path, never by the debounce
			// collapse (docs/store-contract.md, "Schema assumptions for 005").
			const holder = await s().runs.create(newRun({ concurrencyKey: "ck" }));
			if (holder.kind !== "created") throw new Error("expected created");
			expect(
				(await s().runs.acquireWithConcurrencyLimit(holder.run.id, randomUUID(), 1, { limit: 1 }))
					.kind
			).toBe("acquired");

			const opts = { limit: 1 };
			const b = await s().runs.create(
				newRun({ concurrencyKey: "ck", debounce: debounce({ key: "d1", delayMs: 50 }) })
			);
			if (b.kind !== "created") throw new Error("expected created");
			// holder still holds the only slot: b is limited once, bumping its concurrency_waits
			// 0 -> 1 (and its seq), pushing its run_after out to roughly a 1s backoff.
			const limited1 = await s().runs.acquireWithConcurrencyLimit(b.run.id, randomUUID(), 1, opts);
			if (limited1.kind !== "limited") throw new Error(`expected limited, got ${limited1.kind}`);
			expect(limited1.enqueue.delayMs).toBeGreaterThan(1_000 - SLACK_MS);
			expect(limited1.enqueue.delayMs).toBeLessThanOrEqual(1_250);

			// Review item 4: a debounce collapse of the same key arrives with a much shorter delay
			// (50ms) than the pending backoff (~1s). The run is still `queued` (a limited attempt
			// never leaves `queued`), so it collapses -- but must NOT pull run_after back to the
			// debounce's own (sooner) deadline while `concurrency_waits > 0`: doing so would let the
			// very next acquisition attempt through before the backoff it's owed, skipping it
			// entirely. Nothing to reschedule => no enqueue, no seq bump either (audit item 1's rule).
			// `advance()` shifts every stored timestamp together (docs/store-contract.md), so the
			// "unchanged" snapshot below is taken AFTER it, isolating the collapse's own effect.
			await get().advance(10);
			const runAfterBackoff = (await s().runs.get(b.run.id))?.runAfter?.getTime();
			const collapsed = await s().runs.create(
				newRun({ concurrencyKey: "ck", debounce: debounce({ key: "d1", delayMs: 50 }) })
			);
			if (collapsed.kind !== "debounced")
				throw new Error(`expected debounced, got ${collapsed.kind}`);
			expect(collapsed.enqueue).toBeNull();
			expect(collapsed.run.id).toBe(b.run.id);
			expect(collapsed.run.enqueueSeq).toBe(limited1.enqueue.seq);
			// run_after is unchanged (never moved earlier); the payload still updates ("last").
			expect((await s().runs.get(b.run.id))?.runAfter?.getTime()).toBe(runAfterBackoff);
			expect(collapsed.run.payload).toEqual({ n: 0 });

			// If the collapse had reset concurrency_waits back to 0, this next limited attempt would
			// report the BASE backoff again; since it must not touch it, this is the SECOND
			// consecutive limited attempt and reports the next (roughly doubled) tier. Reuses
			// `limited1`'s seq: the collapse above left it as the current, live one.
			const limited2 = await s().runs.acquireWithConcurrencyLimit(
				b.run.id,
				randomUUID(),
				collapsed.run.enqueueSeq,
				opts
			);
			if (limited2.kind !== "limited") throw new Error(`expected limited, got ${limited2.kind}`);
			expect(limited2.enqueue.delayMs).toBeGreaterThan(2_000 - SLACK_MS);
			expect(limited2.enqueue.delayMs).toBeLessThanOrEqual(2_250);
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

		it("release with backoff: growing run_after from concurrency_waits, seq+1, reset on acquire", async () => {
			const a = await created({ concurrencyKey: "c1" });
			const owner = async (seq: number): Promise<Ownership> => {
				const executionId = randomUUID();
				const r = await s().runs.acquireWithConcurrencyLimit(a.run.id, executionId, seq, {
					limit: 5,
				});
				if (r.kind !== "acquired") throw new Error(`expected acquired, got ${r.kind}`);
				return { runId: a.run.id, executionId };
			};
			// A plain release (hand-back) is due right away and leaves concurrency_waits alone.
			const plain = await s().runs.release(await owner(1));
			if (plain.kind !== "released") throw new Error("expected released");
			expect(plain.enqueue.delayMs).toBe(0);

			const first = await s().runs.release(await owner(2), { backoff: true });
			if (first.kind !== "released") throw new Error("expected released");
			expect(first.enqueue).toMatchObject({ seq: 3, status: "queued" });
			expect(first.enqueue.delayMs).toBeGreaterThan(1_000 - SLACK_MS);
			expect(first.enqueue.delayMs).toBeLessThanOrEqual(1_250);
			expect(await s().runs.get(a.run.id)).toMatchObject({
				status: "queued",
				attempt: 1,
				executionId: null,
			});
			// An acquisition resets the waits: the next backoff starts over at the base delay (the
			// Postgres claim does not reset them, so there it grows: test/integration/pg-engine).
			const o = await owner(3);
			const sec = await s().runs.release(o, { backoff: true });
			if (sec.kind !== "released") throw new Error("expected released");
			expect(sec.enqueue.delayMs).toBeLessThanOrEqual(1_250);
			// Not owned any more: lost.
			expect((await s().runs.release(o, { backoff: true })).kind).toBe("lost");
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

	describe("contract: recheckConcurrencyLimit (decision 25 after a Postgres-transport claim)", () => {
		async function created(overrides: Partial<NewRun> = {}) {
			const res = await s().runs.create(newRun(overrides));
			if (res.kind !== "created") throw new Error("expected created");
			return res.run;
		}

		/**
		 * What `claim.claimRuns` writes for a keyed run, portably: `running` + a new execution id,
		 * leaving `enqueue_seq`, `concurrency_waits` and (keyed) `started_at` untouched.
		 */
		async function claim(runId: string): Promise<string> {
			const executionId = randomUUID();
			await get().patchRun(runId, { status: "running", executionId });
			return executionId;
		}

		it("limit reached -> limited: back to queued, no owner, run_after = now + 1 s backoff", async () => {
			const a = await created({ concurrencyKey: "r1" });
			const b = await created({ concurrencyKey: "r1" });
			const ea = await claim(a.id);
			const eb = await claim(b.id);
			expect((await s().runs.recheckConcurrencyLimit(a.id, ea, { limit: 1 })).kind).toBe("limited");
			// a (limited) left the slot: b now fits, and a's own row does not count against b.
			const rb = await s().runs.recheckConcurrencyLimit(b.id, eb, { limit: 1 });
			expect(rb.kind).toBe("ok");
			const after = await s().runs.get(a.id);
			expect(after?.status).toBe("queued");
			expect(after?.executionId).toBeNull();
			expect(after?.attempt).toBe(1);
			expect(after?.enqueueSeq).toBe(a.enqueueSeq);
			expect(after?.startedAt).toBeNull();
			const due = await dueIn(a.id);
			expect(due).toBeGreaterThan(1_000 - SLACK_MS);
			expect(due).toBeLessThanOrEqual(1_250);
		});

		it("the claim never resets concurrency_waits: the backoff keeps growing (review focus 7)", async () => {
			const holder = await created({ concurrencyKey: "r2" });
			const eh = await claim(holder.id);
			expect((await s().runs.recheckConcurrencyLimit(holder.id, eh, { limit: 1 })).kind).toBe("ok");
			const b = await created({ concurrencyKey: "r2" });
			const delays: number[] = [];
			for (let i = 0; i < 3; i++) {
				const eb = await claim(b.id);
				const r = await s().runs.recheckConcurrencyLimit(b.id, eb, { limit: 1 });
				if (r.kind !== "limited") throw new Error(`expected limited, got ${r.kind}`);
				delays.push(await dueIn(b.id));
			}
			expect(delays[0]).toBeLessThanOrEqual(1_250);
			expect(delays[1]).toBeGreaterThan(2_000 - SLACK_MS);
			expect(delays[1]).toBeLessThanOrEqual(2_250);
			expect(delays[2]).toBeGreaterThan(4_000 - SLACK_MS);
			expect(delays[2]).toBeLessThanOrEqual(4_250);
		});

		it("under the limit -> ok: started_at set, concurrency_waits reset", async () => {
			const a = await created({ concurrencyKey: "r3" });
			const b = await created({ concurrencyKey: "r3" });
			const eb = await claim(b.id);
			expect((await s().runs.recheckConcurrencyLimit(b.id, eb, { limit: 1 })).kind).toBe("ok");
			// a is limited once (waits = 1) while b holds the only slot.
			const ea = await claim(a.id);
			expect((await s().runs.recheckConcurrencyLimit(a.id, ea, { limit: 1 })).kind).toBe("limited");
			const ea2 = await claim(a.id);
			const ok = await s().runs.recheckConcurrencyLimit(a.id, ea2, { limit: 2 });
			if (ok.kind !== "ok") throw new Error(`expected ok, got ${ok.kind}`);
			expect(ok.run.id).toBe(a.id);
			expect(ok.run.status).toBe("running");
			expect(ok.run.executionId).toBe(ea2);
			expect(ok.run.startedAt).toBeInstanceOf(Date);
			// waits were reset: a later limited result of a starts back at the base delay (~1 s,
			// not the ~2 s that waits = 1 would give).
			const ea3 = await claim(a.id);
			expect((await s().runs.recheckConcurrencyLimit(a.id, ea3, { limit: 1 })).kind).toBe(
				"limited"
			);
			expect(await dueIn(a.id)).toBeLessThanOrEqual(1_250);
		});

		it("without a limit -> ok with started_at set", async () => {
			const a = await created({ concurrencyKey: "r4" });
			const b = await created({ concurrencyKey: "r4" });
			await claim(b.id);
			const ea = await claim(a.id);
			const r = await s().runs.recheckConcurrencyLimit(a.id, ea, {});
			if (r.kind !== "ok") throw new Error(`expected ok, got ${r.kind}`);
			expect(r.run.startedAt).toBeInstanceOf(Date);
			expect(r.run.status).toBe("running");
		});

		it("an execution that no longer owns the run -> lost", async () => {
			const a = await created({ concurrencyKey: "r5" });
			await claim(a.id);
			expect((await s().runs.recheckConcurrencyLimit(a.id, randomUUID(), { limit: 1 })).kind).toBe(
				"lost"
			);
			const queued = await created({ concurrencyKey: "r5" });
			expect(
				(await s().runs.recheckConcurrencyLimit(queued.id, randomUUID(), { limit: 1 })).kind
			).toBe("lost");
		});
	});
}
