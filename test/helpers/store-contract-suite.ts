/**
 * Store contract scenarios (docs/store-contract.md) run against BOTH implementations: the SQL
 * store (test/integration/store-contract-sql.test.ts) and the in-memory store
 * (test/unit/memory-store.test.ts). Only the public `Store` API is used, plus a tiny harness for
 * what the SQL tests do with raw SQL: moving the clock and patching a run row.
 */
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type {
	ChildResult,
	Json,
	NewRun,
	Ownership,
	RunRecord,
	RunStatus,
	SerializedError,
	Store,
	WaitpointResolution,
} from "../../src/store/types.js";

export interface StoreHarness {
	readonly store: Store;
	/** Moves the store's clock forward (SQL: shifts every stored timestamp back by `ms`). */
	advance(ms: number): Promise<void>;
	/** The store's `now()`. */
	now(): Promise<Date>;
	/** Raw column overwrite of a run row (the SQL tests' `UPDATE runs SET ...`). */
	patchRun(
		runId: string,
		patch: { status?: RunStatus; executionId?: null; runAfter?: null }
	): Promise<void>;
}

const ERR: SerializedError = { name: "Error", message: "boom", retryable: true };
const DAY_MS = 24 * 60 * 60 * 1000;
const BIG = 16 * 1024 * 1024;

/**
 * Registers the scenarios. `get()` returns the harness for the current test; the caller gives
 * every test a clean store (a fresh memory store, or truncated SQL tables).
 */
export function defineStoreContractSuite(get: () => StoreHarness): void {
	const s = () => get().store;

	function newRun(overrides: Partial<NewRun> = {}): NewRun {
		return {
			id: randomUUID(),
			taskId: "task-a",
			queue: "default",
			payload: { hello: "world" },
			maxAttempts: 3,
			delayMs: 0,
			...overrides,
		};
	}

	async function running(overrides: Partial<NewRun> = {}) {
		const created = await s().runs.create(newRun(overrides));
		if (created.kind !== "created") throw new Error("expected a new run");
		const executionId = randomUUID();
		const run = await s().runs.acquire(created.run.id, executionId, created.enqueue.seq);
		if (!run) throw new Error("acquire failed");
		return { run, owner: { runId: run.id, executionId } as Ownership };
	}

	async function acquire(runId: string): Promise<Ownership> {
		const current = await s().runs.get(runId);
		if (!current) throw new Error(`run ${runId} not found`);
		const executionId = randomUUID();
		const run = await s().runs.acquire(runId, executionId, current.enqueueSeq);
		if (!run) throw new Error(`acquire of ${runId} failed (status ${current.status})`);
		return { runId, executionId };
	}

	async function spawn(
		parent: Ownership,
		mode: "trigger" | "wait" | "batch",
		n = 1,
		name = "sub",
		timeoutMs: number | null = null
	) {
		const waitpointId = randomUUID();
		const children = Array.from({ length: n }, (_, i) => ({
			id: randomUUID(),
			taskId: "child-task",
			queue: "default",
			payload: { i },
			maxAttempts: 1,
			idempotencyKey: `__child:${parent.runId}:${name}${mode === "batch" ? `:${i}` : ""}`,
			batchIndex: mode === "batch" ? i : null,
		}));
		const res = await s().runs.triggerChildren({
			parent,
			stepName: name,
			seqIndex: 50,
			mode,
			waitpointId,
			timeoutMs,
			children,
		});
		if (res.kind !== "created") throw new Error("triggerChildren lost");
		return { childIds: children.map(c => c.id), waitpointId, res };
	}

	/** A token plus (when `awaited`) its `${name}#wait` step: only awaited tokens block. */
	async function token(
		owner: Ownership,
		name: string,
		timeoutMs: number | null = null,
		awaited = true
	) {
		const waitpointId = randomUUID();
		const seqIndex = Number.parseInt(name.replace(/\D/g, "") || "0", 10);
		const res = await s().waitpoints.createToken(owner, { waitpointId, name, seqIndex, timeoutMs });
		if (res.kind !== "created") throw new Error("createToken lost");
		if (awaited) {
			const r = await s().steps.insertWaitRef(owner, {
				kind: "wait",
				name: `${name}#wait`,
				seqIndex: 100 + seqIndex,
				waitpointId,
			});
			if (r !== "ok") throw new Error("insertWaitRef lost");
		}
		return { waitpointId, res };
	}

	async function sleepStep(owner: Ownership, name: string, seqIndex: number, offsetMs: number) {
		const r = await s().steps.insertSleep(owner, { name, seqIndex, durationMs: offsetMs });
		if (r.kind !== "inserted") throw new Error("insertSleep lost");
		return r;
	}

	async function wpOf(runId: string, id: string) {
		const snap = await s().steps.loadForExecution(runId, BIG);
		return snap.waitpoints.find(w => w.id === id);
	}

	function toOutcome(r: ChildResult) {
		if (r.status === "completed") return { ok: true, output: r.output };
		if (r.status === "canceled") return { ok: false, error: { name: r.error?.name } };
		return { ok: false, error: r.error };
	}

	// -------------------------------------------------------------------------
	describe("contract: runs create / acquire / heartbeat", () => {
		it("create: queued, attempt 1, seq 1, delay from the store clock, created_at in ms", async () => {
			const res = await s().runs.create(newRun({ delayMs: 5_000 }));
			if (res.kind !== "created") throw new Error("expected created");
			expect(res.run).toMatchObject({
				status: "queued",
				attempt: 1,
				enqueueSeq: 1,
				executionId: null,
				parentRunId: null,
				output: null,
				error: null,
			});
			expect(res.enqueue.delayMs).toBeGreaterThan(4_000);
			expect(res.enqueue.delayMs).toBeLessThanOrEqual(5_000);
			const got = await s().runs.get(res.run.id);
			expect(got?.createdAt.getTime()).toBe(res.run.createdAt.getTime());
			expect(Number.isInteger(got?.createdAt.getTime())).toBe(true);
		});

		it("acquire: wrong seq -> null; right seq -> running with started/heartbeat", async () => {
			const res = await s().runs.create(newRun());
			if (res.kind !== "created") throw new Error("expected created");
			expect(await s().runs.acquire(res.run.id, randomUUID(), 2)).toBeNull();
			const exec = randomUUID();
			const run = await s().runs.acquire(res.run.id, exec, 1);
			expect(run).toMatchObject({ status: "running", executionId: exec });
			expect(run?.startedAt).toBeInstanceOf(Date);
			expect(run?.heartbeatAt).toBeInstanceOf(Date);
			expect(await s().runs.acquire(res.run.id, randomUUID(), 1)).toBeNull();
			expect(await s().runs.acquire(randomUUID(), randomUUID(), 1)).toBeNull();
		});

		it("concurrent acquires: exactly one wins", async () => {
			const res = await s().runs.create(newRun());
			if (res.kind !== "created") throw new Error("expected created");
			const results = await Promise.all(
				Array.from({ length: 4 }, () => s().runs.acquire(res.run.id, randomUUID(), 1))
			);
			expect(results.filter(r => r !== null)).toHaveLength(1);
		});

		it("heartbeat touches only heartbeat_at; lost after ownership changes; throws in a tx", async () => {
			const { owner } = await running();
			const before = await s().runs.get(owner.runId);
			await get().advance(50);
			expect(await s().runs.heartbeat(owner.runId, owner.executionId, 1_000)).toBe("ok");
			const after = await s().runs.get(owner.runId);
			// SQL advance shifts updated_at back too; either way heartbeat must not rewrite it.
			expect(after?.updatedAt.getTime()).toBeLessThanOrEqual(before?.updatedAt.getTime() ?? 0);
			expect(after?.heartbeatAt?.getTime()).toBeGreaterThan(before?.heartbeatAt?.getTime() ?? 0);
			expect(await s().runs.heartbeat(owner.runId, randomUUID(), 1_000)).toBe("lost");
			await expect(
				s().withTransaction(async tx =>
					(tx.runs as unknown as Store["runs"]).heartbeat(owner.runId, owner.executionId, 1_000)
				)
			).rejects.toThrow(/StoreTx/);
		});

		it("an invalid uuid is rejected like the uuid column type", async () => {
			await expect(s().runs.get("not-a-uuid")).rejects.toThrow(/uuid/);
			const upper = randomUUID().toUpperCase();
			const res = await s().runs.create(newRun({ id: upper }));
			expect(res.run.id).toBe(upper.toLowerCase());
			expect((await s().runs.get(upper))?.id).toBe(upper.toLowerCase());
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: runs terminal transitions and ownership", () => {
		it("complete sets output/finished_at, clears execution_id/run_after/error", async () => {
			const { owner } = await running();
			const retried = await s().runs.retry(owner, ERR, 0);
			if (retried.kind !== "retrying") throw new Error("retry lost");
			const again = await acquire(owner.runId);
			const res = await s().runs.complete(again, [1, "two", { three: 3 }]);
			if (res.kind !== "completed") throw new Error("expected completed");
			const run = await s().runs.get(owner.runId);
			// Phase 7 perf: the result's `updatedAt` is the same DB write, no extra `runs.get`.
			expect(res).toEqual({
				kind: "completed",
				updatedAt: run?.updatedAt,
				parent: null,
				expireJobsToRemove: [],
			});
			expect(run).toMatchObject({
				status: "completed",
				output: [1, "two", { three: 3 }],
				error: null,
				executionId: null,
				runAfter: null,
				attempt: 2,
			});
			expect(run?.finishedAt).toBeInstanceOf(Date);
		});

		it("fail stores the error; a second write from the same execution is lost", async () => {
			const { owner } = await running();
			const res = await s().runs.fail(owner, ERR);
			if (res.kind !== "failed") throw new Error("expected failed");
			const run = await s().runs.get(owner.runId);
			expect(res).toEqual({
				kind: "failed",
				updatedAt: run?.updatedAt,
				parent: null,
				expireJobsToRemove: [],
			});
			expect(run?.error).toEqual(ERR);
			expect(run?.output).toBeNull();
			expect(await s().runs.complete(owner, 1)).toEqual({ kind: "lost" });
		});

		it("retry: queued, attempt+1, seq+1, delay from backoff", async () => {
			const { owner } = await running();
			const res = await s().runs.retry(owner, ERR, 2_000);
			if (res.kind !== "retrying") throw new Error("expected retrying");
			expect(res.attempt).toBe(2);
			expect(res.enqueue.seq).toBe(2);
			expect(res.enqueue.delayMs).toBeGreaterThan(1_000);
			expect(res.enqueue.delayMs).toBeLessThanOrEqual(2_000);
			const run = await s().runs.get(owner.runId);
			expect(run).toMatchObject({
				status: "queued",
				executionId: null,
				error: ERR,
			});
			// Phase 7 perf: `enqueue.status`/`updatedAt` come from the same write as `runs.get`.
			expect(res.enqueue.status).toBe("queued");
			expect(res.enqueue.updatedAt).toEqual(run?.updatedAt);
			expect(await s().runs.retry(owner, ERR, 0)).toEqual({ kind: "lost" });
		});

		it("release keeps attempt, bumps seq; the zombie cannot write", async () => {
			const { owner } = await running();
			const res = await s().runs.release(owner);
			expect(res).toMatchObject({
				kind: "released",
				enqueue: { seq: 2, delayMs: 0, status: "queued" },
			});
			const run = await s().runs.get(owner.runId);
			expect(run).toMatchObject({
				status: "queued",
				attempt: 1,
				executionId: null,
			});
			if (res.kind === "released") expect(res.enqueue.updatedAt).toEqual(run?.updatedAt);
			expect(await s().runs.complete(owner, null)).toEqual({ kind: "lost" });
			expect(await s().runs.heartbeat(owner.runId, owner.executionId, 1_000)).toBe("lost");
			expect(await s().runs.suspend(owner)).toEqual({ kind: "lost" });
			expect(
				await s().steps.insertCompleted(owner, { kind: "run", name: "x", seqIndex: 0, output: 1 })
			).toBe("lost");
		});

		it("cancel then complete -> lost; already_finished; not_found", async () => {
			const { owner } = await running();
			const res = await s().runs.cancel(owner.runId, "user asked");
			const run = await s().runs.get(owner.runId);
			expect(res).toEqual({
				kind: "canceled",
				canceled: [
					{
						runId: owner.runId,
						taskId: "task-a",
						queue: "default",
						previousStatus: "running",
						enqueueSeq: 1,
						attempt: 1,
						error: { name: "Canceled", message: "user asked", retryable: false },
						updatedAt: run?.updatedAt,
					},
				],
				parent: null,
				expireJobsToRemove: [],
			});
			expect(await s().runs.complete(owner, 1)).toEqual({ kind: "lost" });
			expect(await s().runs.fail(owner, ERR)).toEqual({ kind: "lost" });
			expect(await s().runs.get(owner.runId)).toMatchObject({
				status: "canceled",
				executionId: null,
				runAfter: null,
				error: { name: "Canceled", message: "user asked", retryable: false },
			});
			expect(await s().runs.cancel(owner.runId, "again")).toEqual({
				kind: "already_finished",
				status: "canceled",
			});
			expect(await s().runs.cancel(randomUUID(), "x")).toEqual({ kind: "not_found" });
		});

		it("payloads/outputs round-trip as jsonb (strings, arrays, undefined props dropped)", async () => {
			const a = await running({ payload: "plain string" });
			const b = await running({ payload: [1, { two: [3] }] });
			const c = await running({ payload: { keep: 1, drop: undefined } });
			expect((await s().runs.get(a.owner.runId))?.payload).toBe("plain string");
			expect((await s().runs.get(b.owner.runId))?.payload).toEqual([1, { two: [3] }]);
			const cp = (await s().runs.get(c.owner.runId))?.payload as Record<string, Json>;
			expect(Object.keys(cp)).toEqual(["keep"]);
			await s().runs.complete(a.owner, "done");
			await s().runs.complete(b.owner, { z: 1, arr: [undefined as unknown as Json, 2] });
			expect((await s().runs.get(a.owner.runId))?.output).toBe("done");
			expect((await s().runs.get(b.owner.runId))?.output).toEqual({ z: 1, arr: [null, 2] });
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: triggerChildren", () => {
		it("trigger mode: child queued, step completed { id }, no waitpoint, key without expiry", async () => {
			const { owner } = await running();
			const { childIds, res } = await spawn(owner, "trigger");
			expect(res.waitpoint).toBeNull();
			const child = await s().runs.get(childIds[0] ?? "");
			// Phase 7 perf: each child's enqueue already carries its status/updatedAt (no `runs.get`).
			expect(res.enqueue).toEqual([
				{
					runId: childIds[0],
					taskId: "child-task",
					queue: "default",
					seq: 1,
					delayMs: 0,
					status: "queued",
					updatedAt: child?.updatedAt,
				},
			]);
			expect(child).toMatchObject({
				parentRunId: owner.runId,
				parentWaitpointId: null,
				parentStepName: "sub",
				batchIndex: null,
			});
			const steps = await s().steps.list(owner.runId);
			expect(steps.map(x => [x.name, x.kind, x.status, x.output])).toEqual([
				["sub", "trigger", "completed", { id: childIds[0] }],
			]);
			expect(await s().idempotency.find("child-task", `__child:${owner.runId}:sub`)).toEqual({
				runId: childIds[0],
				expiresAt: null,
			});
		});

		it("delayMs: children run_after = now + delay", async () => {
			const { owner } = await running();
			const id = randomUUID();
			const res = await s().runs.triggerChildren({
				parent: owner,
				stepName: "later",
				seqIndex: 0,
				mode: "trigger",
				waitpointId: randomUUID(),
				timeoutMs: null,
				delayMs: 60_000,
				children: [
					{
						id,
						taskId: "child-task",
						queue: "default",
						payload: null,
						maxAttempts: 1,
						idempotencyKey: `__child:${owner.runId}:later`,
						batchIndex: null,
					},
				],
			});
			if (res.kind !== "created") throw new Error("lost");
			expect(res.enqueue[0]?.delayMs).toBeGreaterThan(59_000);
			expect(res.enqueue[0]?.delayMs).toBeLessThanOrEqual(60_000);
			const child = await s().runs.get(id);
			expect((child?.runAfter?.getTime() ?? 0) - (child?.createdAt.getTime() ?? 0)).toBeGreaterThan(
				59_000
			);
		});

		it("wait mode without timeout: expireDelayMs is null (not greatest(0, NULL)), expiresAt null", async () => {
			const { owner } = await running();
			const { res, waitpointId } = await spawn(owner, "wait");
			expect(res.waitpoint).toEqual({ id: waitpointId, expiresAt: null, expireDelayMs: null });
			const steps = await s().steps.list(owner.runId);
			expect(steps.map(x => [x.kind, x.status, x.output])).toEqual([
				["trigger_wait", "waiting", { waitpointId }],
			]);
		});

		it("batch mode: pending_count, batch_index order, timeout", async () => {
			const { owner } = await running();
			const { waitpointId, res } = await spawn(owner, "batch", 3, "b", 60_000);
			expect(res.waitpoint?.id).toBe(waitpointId);
			expect(res.waitpoint?.expireDelayMs).toBeGreaterThan(59_000);
			expect(res.waitpoint?.expiresAt).toBeInstanceOf(Date);
			expect(await wpOf(owner.runId, waitpointId)).toMatchObject({
				kind: "batch",
				pendingCount: 3,
				status: "pending",
				stepName: "b",
			});
			const results = await s().runs.listChildResults(waitpointId);
			expect(results.map(r => r.batchIndex)).toEqual([0, 1, 2]);
			expect(results.every(r => r.status === "queued")).toBe(true);
			expect((await s().steps.list(owner.runId))[0]?.kind).toBe("batch_wait");
		});

		it("lost ownership -> lost; key conflict -> throws and rolls back everything", async () => {
			const { owner } = await running();
			await expect(
				spawn({ runId: owner.runId, executionId: randomUUID() }, "wait")
			).rejects.toThrow("lost");
			await spawn(owner, "wait", 1, "dup");
			const before = await s().steps.loadForExecution(owner.runId, BIG);
			await expect(spawn(owner, "wait", 1, "dup")).rejects.toThrow();
			const after = await s().steps.loadForExecution(owner.runId, BIG);
			expect(after.waitpoints).toHaveLength(before.waitpoints.length);
			expect(after.steps).toHaveLength(before.steps.length);
			const list = await s().runs.list({ taskId: "child-task", limit: 100 });
			expect(list.items).toHaveLength(1);
		});

		it("a triggerChildren step name colliding with an existing step throws and rolls back", async () => {
			const { owner } = await running();
			await s().steps.insertCompleted(owner, { kind: "run", name: "x", seqIndex: 50, output: 1 });
			await expect(spawn(owner, "batch", 2, "y")).rejects.toThrow(/duplicate key|unique/i);
			expect((await s().runs.list({ taskId: "child-task", limit: 10 })).items).toHaveLength(0);
			expect(await s().idempotency.find("child-task", `__child:${owner.runId}:y:0`)).toBeNull();
			expect((await s().steps.loadForExecution(owner.runId, BIG)).waitpoints).toEqual([]);
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: recursive cancel", () => {
		it("cancels three levels, root first, walking through terminal nodes", async () => {
			const root = await running();
			const { childIds: level1 } = await spawn(root.owner, "wait", 1, "l1");
			const childOwner = await acquire(level1[0] ?? "");
			const { childIds: level2 } = await spawn(childOwner, "batch", 2, "l2");
			const doneOwner = await acquire(level2[0] ?? "");
			const { childIds: level3 } = await spawn(doneOwner, "trigger", 1, "l3");
			expect((await s().runs.complete(doneOwner, "ok")).kind).toBe("completed");

			const res = await s().runs.cancel(root.owner.runId, "stop");
			if (res.kind !== "canceled") throw new Error("expected canceled");
			const ids = res.canceled.map(c => c.runId);
			expect(ids[0]).toBe(root.owner.runId);
			expect(ids).toHaveLength(4);
			expect(new Set(ids)).toEqual(new Set([root.owner.runId, level1[0], level2[1], level3[0]]));
			expect(res.canceled.find(c => c.runId === level1[0])?.previousStatus).toBe("running");
			expect(res.canceled.find(c => c.runId === level2[1])?.previousStatus).toBe("queued");
			expect(res.parent).toBeNull();
			expect((await s().runs.get(level2[0] ?? ""))?.status).toBe("completed");
			expect((await s().runs.get(level3[0] ?? ""))?.status).toBe("canceled");
			expect(await s().runs.complete(childOwner, 1)).toEqual({ kind: "lost" });
			expect(res.expireJobsToRemove).toEqual([]);
		});

		it("times out the pending waitpoints of every canceled run", async () => {
			const root = await running();
			const rootTok = await token(root.owner, "rt10", 60_000, false);
			const { childIds, waitpointId: childWp } = await spawn(root.owner, "wait", 1, "c");
			const childOwner = await acquire(childIds[0] ?? "");
			const childTok = await token(childOwner, "ct1", null, false);
			const doneTok = await token(childOwner, "dt2", 60_000, false);
			await s().waitpoints.completeToken(doneTok.waitpointId, 1);

			const res = await s().runs.cancel(root.owner.runId, "stop");
			if (res.kind !== "canceled") throw new Error("expected canceled");
			expect(res.expireJobsToRemove).toEqual([rootTok.waitpointId]);
			for (const id of [rootTok.waitpointId, childWp, childTok.waitpointId]) {
				expect((await s().waitpoints.getToken(id))?.status).toBe("timed_out");
			}
			expect((await s().waitpoints.getToken(doneTok.waitpointId))?.status).toBe("completed");
			expect(await s().waitpoints.completeToken(childTok.waitpointId, "late")).toEqual({
				kind: "run_finished",
			});
			expect(await s().waitpoints.completeToken(rootTok.waitpointId, "late")).toEqual({
				kind: "run_finished",
			});
		});

		it("canceling a child resolves the parent's waitpoint and resumes the waiting parent", async () => {
			const parent = await running({ taskId: "parent" });
			const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1);
			expect(await s().runs.suspend(parent.owner)).toMatchObject({
				kind: "waiting_on_waitpoints",
			});
			const res = await s().runs.cancel(childIds[0] ?? "", "nope");
			if (res.kind !== "canceled") throw new Error("expected canceled");
			expect(res.parent).toMatchObject({
				waitpointId,
				hasExpireJob: false,
				resume: { runId: parent.owner.runId, taskId: "parent", seq: 3, delayMs: 0 },
			});
			const [child] = await s().runs.listChildResults(waitpointId);
			expect(child && toOutcome(child)).toEqual({ ok: false, error: { name: "Canceled" } });
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: list", () => {
		it("paginates 105 runs with a composite cursor, limit clamped to 100", async () => {
			for (let i = 0; i < 105; i++) await s().runs.create(newRun({ taskId: "list-task" }));
			const page1 = await s().runs.list({ taskId: "list-task", limit: 1000 });
			expect(page1.items).toHaveLength(100);
			expect(page1.nextCursor).not.toBeNull();
			const page2 = await s().runs.list({
				taskId: "list-task",
				limit: 1000,
				...(page1.nextCursor ? { cursor: page1.nextCursor } : {}),
			});
			expect(page2.items).toHaveLength(5);
			expect(page2.nextCursor).toBeNull();
			const all = [...page1.items, ...page2.items];
			expect(new Set(all.map(r => r.id)).size).toBe(105);
			for (let i = 1; i < all.length; i++) {
				const prev = all[i - 1] as RunRecord;
				const cur = all[i] as RunRecord;
				const t = prev.createdAt.getTime() - cur.createdAt.getTime();
				expect(t > 0 || (t === 0 && prev.id > cur.id)).toBe(true);
			}
		});

		it("walks pages with filters (task, status list, created range)", async () => {
			const created: string[] = [];
			for (let i = 0; i < 7; i++) {
				const r = await s().runs.create(newRun({ taskId: "pager" }));
				created.push(r.run.id);
			}
			await s().runs.create(newRun({ taskId: "other" }));
			const seen: string[] = [];
			let cursor: { createdAt: Date; id: string } | null = null;
			let pages = 0;
			do {
				const page: Awaited<ReturnType<Store["runs"]["list"]>> = await s().runs.list({
					taskId: "pager",
					status: ["queued"],
					limit: 3,
					...(cursor ? { cursor } : {}),
				});
				seen.push(...page.items.map(r => r.id));
				cursor = page.nextCursor;
				pages++;
			} while (cursor && pages < 10);
			expect(seen).toHaveLength(7);
			expect(new Set(seen)).toEqual(new Set(created));
			expect((await s().runs.list({ status: "running", limit: 10 })).items).toHaveLength(0);
			const now = (await get().now()).getTime();
			const future = await s().runs.list({ createdFrom: new Date(now + 3_600_000), limit: 5 });
			expect(future.items).toHaveLength(0);
			const past = await s().runs.list({ createdTo: new Date(now - 3_600_000), limit: 5 });
			expect(past.items).toHaveLength(0);
			const upTo = await s().runs.list({ createdTo: new Date(now + 3_600_000), limit: 100 });
			expect(upTo.items).toHaveLength(8);
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: reconciler queries", () => {
		it("listStaleQueued returns stale queued runs with the current seq", async () => {
			const stale = await s().runs.create(newRun());
			await get().advance(60_000);
			const fresh = await s().runs.create(newRun());
			const res = await s().runs.listStaleQueued(30_000, 10);
			// Fetched after `advance()`: the SQL store shifts stored `updated_at` back, the memory
			// store moves its clock forward instead, so only a fresh read is comparable either way.
			const staleRow = await s().runs.get(stale.run.id);
			expect(res).toEqual([
				{
					runId: stale.run.id,
					taskId: "task-a",
					queue: "default",
					seq: 1,
					delayMs: 0,
					status: "queued",
					updatedAt: staleRow?.updatedAt,
				},
			]);
			expect(res.map(r => r.runId)).not.toContain(fresh.run.id);
		});

		it("listStaleWaiting ignores runs with a blocking waitpoint", async () => {
			const a = await running();
			const b = await running();
			await sleepStep(a.owner, "nap", 0, 1_000);
			expect((await s().runs.suspend(a.owner)).kind).toBe("sleeping");
			await token(b.owner, "t0");
			await sleepStep(b.owner, "nap", 1, 1_000);
			expect(await s().runs.suspend(b.owner)).toMatchObject({ kind: "waiting_on_waitpoints" });
			await get().advance(60_000);
			const res = await s().runs.listStaleWaiting(30_000, 10);
			const aRow = await s().runs.get(a.owner.runId);
			expect(res).toEqual([
				{
					runId: a.owner.runId,
					taskId: "task-a",
					queue: "default",
					seq: 2,
					delayMs: 0,
					status: "waiting",
					updatedAt: aRow?.updatedAt,
				},
			]);
		});

		it("repairOrphanWaiting resumes waiting runs with run_after NULL and nothing pending", async () => {
			const orphan = await running();
			const blocked = await running();
			await token(blocked.owner, "t0");
			await get().patchRun(orphan.owner.runId, {
				status: "waiting",
				executionId: null,
				runAfter: null,
			});
			expect(await s().runs.suspend(blocked.owner)).toMatchObject({
				kind: "waiting_on_waitpoints",
			});
			const res = await s().runs.repairOrphanWaiting(10);
			const orphanRow = await s().runs.get(orphan.owner.runId);
			expect(res).toEqual([
				{
					runId: orphan.owner.runId,
					taskId: "task-a",
					queue: "default",
					seq: 2,
					delayMs: 0,
					status: "waiting",
					updatedAt: orphanRow?.updatedAt,
				},
			]);
			expect(orphanRow?.runAfter).toBeInstanceOf(Date);
			expect(await s().runs.repairOrphanWaiting(10)).toEqual([]);
		});

		it("listStalled + recoverStalled: requeue while attempts remain, else fail", async () => {
			const retryable = await running({ maxAttempts: 2 });
			const exhausted = await running({ maxAttempts: 1 });
			const healthy = await running();
			await get().advance(120_000);
			await s().runs.heartbeat(healthy.owner.runId, healthy.owner.executionId, 1_000);
			const stalled = await s().runs.listStalled(60_000, 10);
			expect(new Set(stalled)).toEqual(new Set([retryable.owner.runId, exhausted.owner.runId]));

			const r1 = await s().runs.recoverStalled(retryable.owner.runId, 60_000, ERR);
			expect(r1).toMatchObject({
				kind: "requeued",
				attempt: 2,
				enqueue: { seq: 2, delayMs: 0, status: "queued" },
			});
			const r2 = await s().runs.recoverStalled(exhausted.owner.runId, 60_000, ERR);
			const exhaustedRun = await s().runs.get(exhausted.owner.runId);
			expect(r2).toEqual({
				kind: "failed",
				taskId: "task-a",
				attempt: 1,
				updatedAt: exhaustedRun?.updatedAt,
				parent: null,
				expireJobsToRemove: [],
			});
			expect(exhaustedRun).toMatchObject({
				status: "failed",
				executionId: null,
				error: ERR,
			});
			expect(await s().runs.recoverStalled(healthy.owner.runId, 60_000, ERR)).toEqual({
				kind: "not_stalled",
			});
			expect(await s().runs.recoverStalled(randomUUID(), 60_000, ERR)).toEqual({
				kind: "not_stalled",
			});
			expect(await s().runs.complete(retryable.owner, 1)).toEqual({ kind: "lost" });
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: retention (deleteFinishedBatch)", () => {
		it("respects the parent rule and the batch size", async () => {
			const parent = await running();
			const { childIds } = await spawn(parent.owner, "trigger");
			const childOwner = await acquire(childIds[0] ?? "");
			await s().runs.complete(childOwner, 1);
			const roots: string[] = [];
			for (let i = 0; i < 3; i++) {
				const r = await running();
				await s().runs.complete(r.owner, i);
				roots.push(r.owner.runId);
			}
			await get().advance(2 * DAY_MS);
			const young = await running();
			await s().runs.complete(young.owner, 1);
			expect(await s().runs.deleteFinishedBatch(DAY_MS, 2)).toBe(2);
			expect(await s().runs.deleteFinishedBatch(DAY_MS, 10)).toBe(1);
			expect(await s().runs.deleteFinishedBatch(DAY_MS, 10)).toBe(0);
			expect(await s().runs.get(childIds[0] ?? "")).not.toBeNull();
			expect(await s().runs.get(young.owner.runId)).not.toBeNull();
			for (const id of roots) expect(await s().runs.get(id)).toBeNull();
		});

		it("cascades steps/waitpoints/logs/keys; children get parent_* NULL and still finish", async () => {
			const parent = await running({ idempotency: { key: "k-ret", ttlMs: null } });
			await s().steps.insertCompleted(parent.owner, {
				kind: "run",
				name: "a",
				seqIndex: 0,
				output: 1,
			});
			await s().logs.insertBatch([
				{ runId: parent.owner.runId, level: "info", message: "x", data: null },
			]);
			const { childIds, waitpointId } = await spawn(parent.owner, "wait", 1, "c");
			await s().runs.complete(parent.owner, "done");
			await get().advance(2 * DAY_MS);
			expect(await s().runs.deleteFinishedBatch(DAY_MS, 10)).toBe(1);
			expect(await s().runs.get(parent.owner.runId)).toBeNull();
			expect(await s().steps.list(parent.owner.runId)).toEqual([]);
			expect(await s().logs.list(parent.owner.runId, { limit: 10 })).toEqual([]);
			expect(await s().idempotency.find("task-a", "k-ret")).toBeNull();
			expect(await s().waitpoints.getToken(waitpointId)).toBeNull();
			const child = await s().runs.get(childIds[0] ?? "");
			expect(child).toMatchObject({
				parentRunId: null,
				parentWaitpointId: null,
				parentStepName: "c",
			});
			const childOwner = await acquire(childIds[0] ?? "");
			const completeRes = await s().runs.complete(childOwner, 1);
			if (completeRes.kind !== "completed") throw new Error("expected completed");
			const childRun = await s().runs.get(childOwner.runId);
			expect(completeRes).toEqual({
				kind: "completed",
				updatedAt: childRun?.updatedAt,
				parent: null,
				expireJobsToRemove: [],
			});
			// The key is free again: a new run can claim it.
			const again = await s().runs.create(newRun({ idempotency: { key: "k-ret", ttlMs: null } }));
			expect(again.kind).toBe("created");
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: idempotency (decision 9)", () => {
		it("new key: creates the run and records the key", async () => {
			const input = newRun({ idempotency: { key: "k-new", ttlMs: DAY_MS } });
			const result = await s().runs.create(input);
			if (result.kind !== "created") throw new Error("expected created");
			expect(result.run.id).toBe(input.id);
			// Phase 7 perf: `enqueue.status`/`updatedAt` mirror `result.run` from the same insert.
			expect(result.enqueue).toEqual({
				runId: input.id,
				taskId: "task-a",
				queue: "default",
				seq: 1,
				delayMs: 0,
				status: "queued",
				updatedAt: result.run.updatedAt,
			});
			expect((await s().idempotency.find("task-a", "k-new"))?.runId).toBe(input.id);
		});

		it("live key: returns the existing run and rolls the losing insert back", async () => {
			const first = newRun({ idempotency: { key: "k-live", ttlMs: DAY_MS } });
			await s().runs.create(first);
			const second = newRun({ idempotency: { key: "k-live", ttlMs: DAY_MS } });
			const result = await s().runs.create(second);
			expect(result.kind).toBe("existing");
			expect(result.run.id).toBe(first.id);
			expect(await s().runs.get(second.id)).toBeNull();
			expect((await s().runs.list({ taskId: "task-a", limit: 10 })).items).toHaveLength(1);
		});

		it("same key under another task is independent", async () => {
			await s().runs.create(newRun({ idempotency: { key: "shared", ttlMs: DAY_MS } }));
			const other = await s().runs.create(
				newRun({ taskId: "task-b", idempotency: { key: "shared", ttlMs: DAY_MS } })
			);
			expect(other.kind).toBe("created");
		});

		it("expired key is taken over by another run; the previous run is untouched", async () => {
			const first = newRun({ idempotency: { key: "k-exp", ttlMs: 1_000 } });
			await s().runs.create(first);
			await get().advance(2_000);
			const second = newRun({ idempotency: { key: "k-exp", ttlMs: DAY_MS } });
			const result = await s().runs.create(second);
			expect(result.kind).toBe("created");
			expect(result.run.id).toBe(second.id);
			expect((await s().idempotency.find("task-a", "k-exp"))?.runId).toBe(second.id);
			expect((await s().runs.get(first.id))?.status).toBe("queued");
		});

		it("concurrent creates with the same key: exactly one run, all get its id", async () => {
			const inputs = Array.from({ length: 4 }, () =>
				newRun({ taskId: "task-race", idempotency: { key: "k-race", ttlMs: DAY_MS } })
			);
			const results = await Promise.all(inputs.map(input => s().runs.create(input)));
			const ids = new Set(results.map(r => r.run.id));
			expect(ids.size).toBe(1);
			expect(results.filter(r => r.kind === "created")).toHaveLength(1);
			expect((await s().runs.list({ taskId: "task-race", limit: 10 })).items).toHaveLength(1);
			expect((await s().idempotency.find("task-race", "k-race"))?.runId).toBe([...ids][0]);
		});

		it("internal key with ttlMs null never expires", async () => {
			const first = newRun({ idempotency: { key: "__child:x:y", ttlMs: null } });
			await s().runs.create(first);
			expect(await s().idempotency.find("task-a", "__child:x:y")).toEqual({
				runId: first.id,
				expiresAt: null,
			});
			await get().advance(365 * DAY_MS);
			const again = await s().runs.create(
				newRun({ idempotency: { key: "__child:x:y", ttlMs: null } })
			);
			expect(again.kind).toBe("existing");
			expect(again.run.id).toBe(first.id);
		});

		it("the TTL lands in expires_at (store clock)", async () => {
			const ttlMs = 7 * DAY_MS;
			await s().runs.create(newRun({ idempotency: { key: "k-ttl", ttlMs } }));
			const found = await s().idempotency.find("task-a", "k-ttl");
			const ms = (found?.expiresAt?.getTime() ?? 0) - (await get().now()).getTime();
			expect(ms).toBeGreaterThan(ttlMs - 60_000);
			expect(ms).toBeLessThanOrEqual(ttlMs);
		});

		it("inside a transaction, a lost claim rolls back only to the savepoint", async () => {
			const first = newRun({ idempotency: { key: "k-tx", ttlMs: DAY_MS } });
			await s().runs.create(first);
			const other = newRun({ taskId: "task-tx" });
			const loser = newRun({ idempotency: { key: "k-tx", ttlMs: DAY_MS } });
			await s().withTransaction(async tx => {
				await tx.runs.create(other);
				const result = await tx.runs.create(loser);
				expect(result.kind).toBe("existing");
				expect(result.run.id).toBe(first.id);
			});
			expect(await s().runs.get(other.id)).not.toBeNull();
			expect(await s().runs.get(loser.id)).toBeNull();
		});

		it("claim() and find() directly; claim for an unknown run violates the FK", async () => {
			const run = newRun();
			await s().runs.create(run);
			expect(
				await s().idempotency.claim({ taskId: "task-a", key: "direct", runId: run.id, ttlMs: 1000 })
			).toEqual({ claimed: true });
			const other = newRun();
			await s().runs.create(other);
			expect(
				await s().idempotency.claim({
					taskId: "task-a",
					key: "direct",
					runId: other.id,
					ttlMs: 1000,
				})
			).toEqual({ claimed: false, existingRunId: run.id });
			expect(await s().idempotency.find("task-a", "missing")).toBeNull();
			await expect(
				s().idempotency.claim({ taskId: "task-a", key: "fk", runId: randomUUID(), ttlMs: null })
			).rejects.toThrow(/foreign key/);
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: tokens", () => {
		it("createToken: pending waitpoint + step { id }; lost without ownership", async () => {
			const { owner } = await running();
			const { waitpointId, res } = await token(owner, "t0", 30_000, false);
			expect(res.expiresAt).toBeInstanceOf(Date);
			expect(res.expireDelayMs).toBeGreaterThan(29_000);
			expect(res.expireDelayMs).toBeLessThanOrEqual(30_000);
			const steps = await s().steps.list(owner.runId);
			expect(steps.map(x => [x.name, x.kind, x.status, x.output])).toEqual([
				["t0", "token", "completed", { id: waitpointId }],
			]);
			expect(await s().waitpoints.getToken(waitpointId)).toEqual({
				status: "pending",
				output: null,
				runId: owner.runId,
			});
			const lost = await s().waitpoints.createToken(
				{ runId: owner.runId, executionId: randomUUID() },
				{ waitpointId: randomUUID(), name: "t1", seqIndex: 1, timeoutMs: null }
			);
			expect(lost).toEqual({ kind: "lost" });
		});

		it("createToken without timeout reports expireDelayMs null (not greatest(0, NULL))", async () => {
			const { owner } = await running();
			const { res } = await token(owner, "t0", null, false);
			expect(res).toMatchObject({ expiresAt: null, expireDelayMs: null });
		});

		it("createToken is idempotent by (run_id, step_name)", async () => {
			const { owner } = await running();
			const first = await token(owner, "t0", 60_000);
			const again = await s().waitpoints.createToken(owner, {
				waitpointId: randomUUID(),
				name: "t0",
				seqIndex: 0,
				timeoutMs: 60_000,
			});
			expect(again).toMatchObject({ kind: "created", waitpointId: first.waitpointId });
			expect(again.kind === "created" && again.expireDelayMs).toBeGreaterThan(50_000);
			expect((await s().steps.loadForExecution(owner.runId, BIG)).waitpoints).toHaveLength(1);
			await s().waitpoints.completeToken(first.waitpointId, 1);
			const done = await s().waitpoints.createToken(owner, {
				waitpointId: randomUUID(),
				name: "t0",
				seqIndex: 0,
				timeoutMs: 60_000,
			});
			expect(done).toMatchObject({ waitpointId: first.waitpointId, expireDelayMs: null });
			// A non-token waitpoint under that name throws.
			await spawn(owner, "wait", 1, "w");
			await expect(
				s().waitpoints.createToken(owner, {
					waitpointId: randomUUID(),
					name: "w",
					seqIndex: 7,
					timeoutMs: null,
				})
			).rejects.toThrow(/child_run/);
		});

		it("fail/complete time out the run's own pending waitpoints", async () => {
			const a = await running();
			const withExpiry = await token(a.owner, "t0", 60_000);
			const noExpiry = await token(a.owner, "t1");
			const failRes = await s().runs.fail(a.owner, ERR);
			if (failRes.kind !== "failed") throw new Error("expected failed");
			const aRun = await s().runs.get(a.owner.runId);
			expect(failRes).toEqual({
				kind: "failed",
				updatedAt: aRun?.updatedAt,
				parent: null,
				expireJobsToRemove: [withExpiry.waitpointId],
			});
			expect((await s().waitpoints.getToken(withExpiry.waitpointId))?.status).toBe("timed_out");
			expect((await s().waitpoints.getToken(noExpiry.waitpointId))?.status).toBe("timed_out");
			expect(await s().waitpoints.completeToken(noExpiry.waitpointId, 1)).toEqual({
				kind: "run_finished",
			});
			const b = await running();
			const tok = await token(b.owner, "t0", 60_000);
			expect(await s().runs.complete(b.owner, 1)).toMatchObject({
				expireJobsToRemove: [tok.waitpointId],
			});
			expect((await s().waitpoints.getToken(tok.waitpointId))?.status).toBe("timed_out");
		});

		it("completeToken: not_found, completed, then not_pending", async () => {
			const { owner } = await running();
			const { waitpointId } = await token(owner, "t0");
			expect(await s().waitpoints.completeToken(randomUUID(), 1)).toEqual({ kind: "not_found" });
			const first = await s().waitpoints.completeToken(waitpointId, { v: [1, 2] });
			expect(first).toEqual({
				kind: "completed",
				resolution: { waitpointId, hasExpireJob: false, resume: null },
			});
			expect((await s().waitpoints.getToken(waitpointId))?.output).toEqual({ v: [1, 2] });
			expect(await s().waitpoints.completeToken(waitpointId, 2)).toEqual({
				kind: "not_pending",
				status: "completed",
			});
			// A child_run waitpoint is not a token.
			const { waitpointId: childWp } = await spawn(owner, "wait", 1, "c");
			expect(await s().waitpoints.completeToken(childWp, 1)).toEqual({ kind: "not_found" });
		});

		it("two tokens completed concurrently on a waiting run: exactly one resume", async () => {
			const { owner } = await running();
			const a = await token(owner, "t0", 60_000);
			const b = await token(owner, "t1");
			expect(await s().runs.suspend(owner)).toMatchObject({ kind: "waiting_on_waitpoints" });
			const results = await Promise.all([
				s().waitpoints.completeToken(a.waitpointId, "a"),
				s().waitpoints.completeToken(b.waitpointId, "b"),
			]);
			const resumes = results.flatMap(r =>
				r.kind === "completed" && r.resolution.resume ? [r.resolution.resume] : []
			);
			expect(resumes).toHaveLength(1);
			expect(resumes[0]).toMatchObject({ runId: owner.runId, seq: 3, delayMs: 0 });
			const withExpire = results.find(
				r => r.kind === "completed" && r.resolution.waitpointId === a.waitpointId
			);
			expect(withExpire?.kind === "completed" && withExpire.resolution.hasExpireJob).toBe(true);
			expect(await s().runs.get(owner.runId)).toMatchObject({ status: "waiting", enqueueSeq: 3 });
		});

		it("stress: 15 rounds of 3 concurrent token completions, always exactly one resume", async () => {
			for (let round = 0; round < 15; round++) {
				const { owner } = await running();
				const tokens = [
					await token(owner, "t0"),
					await token(owner, "t1"),
					await token(owner, "t2"),
				];
				await s().runs.suspend(owner);
				const results = await Promise.all(
					tokens.map(t => s().waitpoints.completeToken(t.waitpointId, round))
				);
				expect(results.filter(r => r.kind === "completed" && r.resolution.resume)).toHaveLength(1);
			}
		});

		it("completeToken on a finished run: run_finished; a completed token stays completed", async () => {
			const { owner } = await running();
			const done = await token(owner, "t0");
			const open = await token(owner, "t1");
			await s().waitpoints.completeToken(done.waitpointId, 1);
			await s().runs.complete(owner, null);
			expect(await s().waitpoints.completeToken(done.waitpointId, 2)).toEqual({
				kind: "not_pending",
				status: "completed",
			});
			expect(await s().waitpoints.completeToken(open.waitpointId, 2)).toEqual({
				kind: "run_finished",
			});
		});

		it("expiry racing completion: exactly one wins, one resume (10 rounds)", async () => {
			for (let round = 0; round < 10; round++) {
				const { owner } = await running();
				const { waitpointId } = await token(owner, "t0", 1_000);
				await s().runs.suspend(owner);
				await get().advance(2_000);
				const [completed, expired] = await Promise.all([
					s().waitpoints.completeToken(waitpointId, round),
					s().waitpoints.expire(waitpointId),
				]);
				const wins = [completed.kind === "completed", expired.kind === "expired"].filter(Boolean);
				expect(wins).toHaveLength(1);
				const resumes = [
					completed.kind === "completed" ? completed.resolution.resume : null,
					expired.kind === "expired" ? expired.resolution.resume : null,
				].filter(r => r !== null);
				expect(resumes).toHaveLength(1);
				const status = (await s().waitpoints.getToken(waitpointId))?.status;
				expect(status).toBe(completed.kind === "completed" ? "completed" : "timed_out");
				if (completed.kind !== "completed") {
					expect(completed).toEqual({ kind: "not_pending", status: "timed_out" });
				} else {
					expect(expired).toEqual({ kind: "noop" });
				}
				expect(await s().runs.get(owner.runId)).toMatchObject({ status: "waiting", enqueueSeq: 3 });
			}
		});

		it("only awaited tokens block: a not-awaited token lets a sleeping run wake up", async () => {
			const { owner } = await running();
			const { waitpointId } = await token(owner, "t0", null, false);
			await sleepStep(owner, "nap", 1, 60_000);
			const suspended = await s().runs.suspend(owner);
			if (suspended.kind !== "sleeping")
				throw new Error(`expected sleeping, got ${suspended.kind}`);
			expect(suspended.enqueue.delayMs).toBeGreaterThan(55_000);
			const res = await s().waitpoints.completeToken(waitpointId, null);
			const resume = res.kind === "completed" ? res.resolution.resume : null;
			expect(resume?.delayMs).toBeGreaterThan(55_000);

			const b = await running();
			await token(b.owner, "t0", null, true);
			await sleepStep(b.owner, "nap", 1, 60_000);
			expect(await s().runs.suspend(b.owner)).toMatchObject({ kind: "waiting_on_waitpoints" });
		});

		it("token + future sleep: completing the token resumes at the sleep's wake_at", async () => {
			const { owner } = await running();
			const { waitpointId } = await token(owner, "t0");
			await sleepStep(owner, "nap", 1, 60_000);
			await s().runs.suspend(owner);
			const res = await s().waitpoints.completeToken(waitpointId, null);
			const resume = res.kind === "completed" ? res.resolution.resume : null;
			expect(resume?.delayMs).toBeGreaterThan(55_000);
			expect(resume?.delayMs).toBeLessThanOrEqual(60_000);
		});

		it("expire: not_due + delay; due -> timed_out and the waiting run resumes; then noop", async () => {
			const { owner } = await running();
			const { waitpointId } = await token(owner, "t0", 60_000);
			await s().runs.suspend(owner);
			const early = await s().waitpoints.expire(waitpointId);
			if (early.kind !== "not_due") throw new Error(`expected not_due, got ${early.kind}`);
			expect(early.delayMs).toBeGreaterThan(55_000);
			expect(early.delayMs).toBeLessThanOrEqual(60_000);
			expect((await s().waitpoints.getToken(waitpointId))?.status).toBe("pending");
			expect(await s().waitpoints.listExpired(10)).toEqual([]);
			await get().advance(61_000);
			expect(await s().waitpoints.listExpired(10)).toEqual([waitpointId]);
			const res = await s().waitpoints.expire(waitpointId);
			expect(res).toMatchObject({
				kind: "expired",
				canceledChildren: [],
				expireJobsToRemove: [],
				resolution: { waitpointId, hasExpireJob: true, resume: { runId: owner.runId, seq: 3 } },
			});
			expect((await s().waitpoints.getToken(waitpointId))?.status).toBe("timed_out");
			expect(await s().waitpoints.expire(waitpointId)).toEqual({ kind: "noop" });
			expect(await s().waitpoints.completeToken(waitpointId, 1)).toEqual({
				kind: "not_pending",
				status: "timed_out",
			});
			expect(await s().waitpoints.expire(randomUUID())).toEqual({ kind: "noop" });
		});

		it("expire: a token without expires_at is a noop", async () => {
			const { owner } = await running();
			const { waitpointId } = await token(owner, "t0", null);
			expect(await s().waitpoints.expire(waitpointId)).toEqual({ kind: "noop" });
		});

		it("completeToken stores string and array outputs as jsonb", async () => {
			const { owner } = await running();
			const a = await token(owner, "t0");
			const b = await token(owner, "t1");
			await s().waitpoints.completeToken(a.waitpointId, "just a string");
			await s().waitpoints.completeToken(b.waitpointId, ["x", { y: [1] }]);
			expect((await s().waitpoints.getToken(a.waitpointId))?.output).toBe("just a string");
			expect((await s().waitpoints.getToken(b.waitpointId))?.output).toEqual(["x", { y: [1] }]);
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: suspend (decision 3)", () => {
		it("blocking waitpoint -> waiting, run_after NULL, seq+1; stale redelivery cannot acquire", async () => {
			const { run, owner } = await running();
			await token(owner, "t0");
			await sleepStep(owner, "nap", 1, 60_000);
			const suspended = await s().runs.suspend(owner);
			const afterSuspend = await s().runs.get(owner.runId);
			// Phase 7 perf: `suspend`'s `waiting_on_waitpoints` carries the row's own status/updatedAt.
			expect(suspended).toEqual({
				kind: "waiting_on_waitpoints",
				status: "waiting",
				updatedAt: afterSuspend?.updatedAt,
			});
			expect(afterSuspend).toMatchObject({
				status: "waiting",
				runAfter: null,
				executionId: null,
				enqueueSeq: 2,
				attempt: 1,
			});
			expect(await s().runs.acquire(owner.runId, randomUUID(), run.enqueueSeq)).toBeNull();
			expect(await s().runs.repairOrphanWaiting(10)).toEqual([]);
			expect(await s().runs.listStaleWaiting(0, 10)).toEqual([]);
		});

		it("only a future sleep -> sleeping with max(wake_at)", async () => {
			const { owner } = await running();
			await sleepStep(owner, "short", 0, 10_000);
			await sleepStep(owner, "long", 1, 45_000);
			const res = await s().runs.suspend(owner);
			if (res.kind !== "sleeping") throw new Error(`expected sleeping, got ${res.kind}`);
			expect(res.enqueue.seq).toBe(2);
			expect(res.enqueue.delayMs).toBeGreaterThan(40_000);
			expect(res.enqueue.delayMs).toBeLessThanOrEqual(45_000);
			expect(await s().runs.get(owner.runId)).toMatchObject({
				status: "waiting",
				executionId: null,
			});
		});

		it("sleep already due -> requeued, delay 0", async () => {
			const { owner } = await running();
			await sleepStep(owner, "past", 0, -1_000);
			const res = await s().runs.suspend(owner);
			expect(res).toMatchObject({ kind: "requeued", enqueue: { seq: 2, delayMs: 0 } });
			expect(await s().runs.get(owner.runId)).toMatchObject({
				status: "queued",
				executionId: null,
			});
		});

		it("race: token completed before suspension -> requeued; second suspend lost", async () => {
			const { owner } = await running();
			const { waitpointId } = await token(owner, "t0");
			const done = await s().waitpoints.completeToken(waitpointId, "early");
			expect(done.kind === "completed" && done.resolution.resume).toBeNull();
			const res = await s().runs.suspend(owner);
			expect(res).toMatchObject({ kind: "requeued", enqueue: { runId: owner.runId, seq: 2 } });
			expect(await s().runs.suspend(owner)).toEqual({ kind: "lost" });
		});

		it("race: suspend concurrent with completeToken -> exactly one enqueue path", async () => {
			for (let round = 0; round < 10; round++) {
				const { owner } = await running();
				const { waitpointId } = await token(owner, "t0");
				const [suspended, completed] = await Promise.all([
					s().runs.suspend(owner),
					s().waitpoints.completeToken(waitpointId, round),
				]);
				const resume = completed.kind === "completed" ? completed.resolution.resume : null;
				const enqueues = [suspended.kind === "requeued" ? suspended : null, resume].filter(
					x => x !== null
				);
				expect(enqueues).toHaveLength(1);
				expect((await s().runs.get(owner.runId))?.enqueueSeq).toBe(
					suspended.kind === "requeued" ? 2 : 3
				);
			}
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: child resolution (decision 10)", () => {
		async function waitingParentWithChild(timeoutMs: number | null = null) {
			const parent = await running({ taskId: "parent" });
			const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1, "s", timeoutMs);
			expect(await s().runs.suspend(parent.owner)).toMatchObject({
				kind: "waiting_on_waitpoints",
			});
			const childOwner = await acquire(childIds[0] ?? "");
			return { parent, waitpointId, childOwner };
		}

		function expectResumed(res: WaitpointResolution | null, parentId: string, waitpointId: string) {
			expect(res).toMatchObject({
				waitpointId,
				hasExpireJob: false,
				resume: { runId: parentId, taskId: "parent", seq: 3, delayMs: 0 },
			});
		}

		it("completed child -> { ok: true, output }", async () => {
			const { parent, waitpointId, childOwner } = await waitingParentWithChild();
			const res = await s().runs.complete(childOwner, { answer: 42 });
			if (res.kind !== "completed") throw new Error("lost");
			expectResumed(res.parent, parent.owner.runId, waitpointId);
			expect((await wpOf(parent.owner.runId, waitpointId))?.status).toBe("completed");
			const [child] = await s().runs.listChildResults(waitpointId);
			expect(child && toOutcome(child)).toEqual({ ok: true, output: { answer: 42 } });
		});

		it("failed child -> { ok: false, error }", async () => {
			const { parent, waitpointId, childOwner } = await waitingParentWithChild();
			const res = await s().runs.fail(childOwner, ERR);
			if (res.kind !== "failed") throw new Error("lost");
			expectResumed(res.parent, parent.owner.runId, waitpointId);
			const [child] = await s().runs.listChildResults(waitpointId);
			expect(child && toOutcome(child)).toEqual({ ok: false, error: ERR });
		});

		it("stalled child failing definitively also resolves the parent", async () => {
			const { parent, waitpointId, childOwner } = await waitingParentWithChild();
			await get().advance(300_000);
			const res = await s().runs.recoverStalled(childOwner.runId, 60_000, ERR);
			if (res.kind !== "failed") throw new Error(`expected failed, got ${res.kind}`);
			expectResumed(res.parent, parent.owner.runId, waitpointId);
		});

		it("waitpoint already timed out -> child completion is a no-op for the parent", async () => {
			// The parent finishes while its child runs: its own waitpoint times out (not canceled).
			const parent = await running({ taskId: "parent" });
			const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1);
			const parentComplete = await s().runs.complete(parent.owner, "early");
			if (parentComplete.kind !== "completed") throw new Error("expected completed");
			const parentRun = await s().runs.get(parent.owner.runId);
			expect(parentComplete).toEqual({
				kind: "completed",
				updatedAt: parentRun?.updatedAt,
				parent: null,
				expireJobsToRemove: [],
			});
			expect((await s().waitpoints.getToken(waitpointId))?.status).toBe("timed_out");
			const childOwner = await acquire(childIds[0] ?? "");
			const childComplete = await s().runs.complete(childOwner, 1);
			if (childComplete.kind !== "completed") throw new Error("expected completed");
			const childRun = await s().runs.get(childOwner.runId);
			expect(childComplete).toEqual({
				kind: "completed",
				updatedAt: childRun?.updatedAt,
				parent: null,
				expireJobsToRemove: [],
			});
		});

		it("parent still running -> waitpoint completed but no enqueue", async () => {
			const parent = await running({ taskId: "parent" });
			const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1);
			const childOwner = await acquire(childIds[0] ?? "");
			const res = await s().runs.complete(childOwner, 1);
			if (res.kind !== "completed") throw new Error("expected completed");
			const childRun = await s().runs.get(childOwner.runId);
			expect(res).toEqual({
				kind: "completed",
				updatedAt: childRun?.updatedAt,
				parent: { waitpointId, hasExpireJob: false, resume: null },
				expireJobsToRemove: [],
			});
			expect(await s().runs.suspend(parent.owner)).toMatchObject({ kind: "requeued" });
		});

		it("batch of 20 finishing concurrently -> pending_count 0 and exactly one enqueue", async () => {
			const parent = await running({ taskId: "parent" });
			const { waitpointId, childIds } = await spawn(parent.owner, "batch", 20, "s", 60_000);
			await s().runs.suspend(parent.owner);
			const owners = await Promise.all(childIds.map(id => acquire(id)));
			const results = await Promise.all(
				owners.map((o, i) => (i % 3 === 0 ? s().runs.fail(o, ERR) : s().runs.complete(o, i)))
			);
			const resolutions = results.map(r => (r.kind === "lost" ? null : r.parent));
			expect(resolutions.every(r => r !== null)).toBe(true);
			const resumes = resolutions.filter(r => r?.resume);
			expect(resumes).toHaveLength(1);
			expect(resumes[0]).toMatchObject({ hasExpireJob: true, resume: { seq: 3 } });
			expect(resolutions.filter(r => r?.hasExpireJob)).toHaveLength(1);
			expect(await wpOf(parent.owner.runId, waitpointId)).toMatchObject({
				status: "completed",
				pendingCount: 0,
			});
			const results2 = await s().runs.listChildResults(waitpointId);
			expect(results2.map(r => r.batchIndex)).toEqual(Array.from({ length: 20 }, (_, i) => i));
		});

		it("expire on a child_run waitpoint cancels the child tree and resumes the parent", async () => {
			const parent = await running({ taskId: "parent" });
			const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1, "s", 1_000);
			const childOwner = await acquire(childIds[0] ?? "");
			const grand = await spawn(childOwner, "trigger", 1, "g");
			await s().runs.suspend(parent.owner);
			await get().advance(2_000);
			const res = await s().waitpoints.expire(waitpointId);
			if (res.kind !== "expired") throw new Error("expected expired");
			expect(res.canceledChildren.map(c => c.runId)).toEqual([childOwner.runId, grand.childIds[0]]);
			expect(res.resolution.resume).toMatchObject({ runId: parent.owner.runId, seq: 3 });
			expect(res.expireJobsToRemove).toEqual([]);
			expect(await s().runs.complete(childOwner, 1)).toEqual({ kind: "lost" });
			const [child] = await s().runs.listChildResults(waitpointId);
			expect(child?.status).toBe("canceled");
			expect(child?.error?.message).toBe("Parent waitpoint timed out");
		});

		it("expire cancels children and times out their own pending waitpoints", async () => {
			const parent = await running({ taskId: "parent" });
			const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1, "s", 1_000);
			const childOwner = await acquire(childIds[0] ?? "");
			const withExpiry = await token(childOwner, "t1", 60_000);
			const noExpiry = await token(childOwner, "t2", null);
			await s().runs.suspend(parent.owner);
			await get().advance(2_000);
			const res = await s().waitpoints.expire(waitpointId);
			if (res.kind !== "expired") throw new Error("expected expired");
			expect(res.canceledChildren.map(c => c.runId)).toEqual([childOwner.runId]);
			expect(res.expireJobsToRemove).toEqual([withExpiry.waitpointId]);
			expect((await s().waitpoints.getToken(withExpiry.waitpointId))?.status).toBe("timed_out");
			expect(await s().waitpoints.completeToken(noExpiry.waitpointId, "late")).toEqual({
				kind: "run_finished",
			});
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: steps", () => {
		it("loadForExecution: inline outputs, ordered by seq_index, with waitpoints and now", async () => {
			const { owner } = await running();
			await s().steps.insertCompleted(owner, {
				kind: "run",
				name: "b",
				seqIndex: 2,
				output: { v: 2 },
			});
			await s().steps.insertCompleted(owner, {
				kind: "run",
				name: "a",
				seqIndex: 1,
				output: { v: 1 },
			});
			const { waitpointId } = await token(owner, "t0", 60_000, false);
			const snap = await s().steps.loadForExecution(owner.runId, BIG);
			expect(snap.outputsDeferred).toBe(false);
			expect(snap.steps.map(x => [x.name, x.kind, x.output])).toEqual([
				["t0", "token", { id: waitpointId }],
				["a", "run", { v: 1 }],
				["b", "run", { v: 2 }],
			]);
			expect(snap.now).toBeInstanceOf(Date);
			expect(snap.waitpoints).toEqual([
				{
					id: waitpointId,
					stepName: "t0",
					kind: "token",
					status: "pending",
					pendingCount: null,
					expiresAt: expect.any(Date),
				},
			]);
		});

		it("loadForExecution defers every output past the byte budget; loadOutputs reads them", async () => {
			const { owner } = await running();
			for (let i = 1; i <= 5; i++) {
				await s().steps.insertCompleted(owner, {
					kind: "run",
					name: `s${i}`,
					seqIndex: i,
					output: { v: "x".repeat(100), i },
				});
			}
			const deferred = await s().steps.loadForExecution(owner.runId, 200);
			expect(deferred.outputsDeferred).toBe(true);
			expect(deferred.steps).toHaveLength(5);
			expect(deferred.steps.every(x => x.output === null)).toBe(true);
			const inline = await s().steps.loadForExecution(owner.runId, 100_000);
			expect(inline.outputsDeferred).toBe(false);
			expect(await s().steps.loadOutputs(owner.runId, 2, 3)).toEqual([
				{ name: "s2", seqIndex: 2, output: { v: "x".repeat(100), i: 2 } },
				{ name: "s3", seqIndex: 3, output: { v: "x".repeat(100), i: 3 } },
			]);
			expect(await s().steps.loadOutputs(owner.runId, 10, 20)).toEqual([]);
		});

		it("loadForExecution of only sleep steps (all outputs NULL) is not deferred", async () => {
			const { owner } = await running();
			await sleepStep(owner, "nap", 0, 1_000);
			const snap = await s().steps.loadForExecution(owner.runId, 0);
			expect(snap.outputsDeferred).toBe(false);
			expect(snap.steps).toMatchObject([{ name: "nap", kind: "sleep", output: null }]);
			const empty = await s().steps.loadForExecution(randomUUID(), BIG);
			expect(empty).toMatchObject({ steps: [], waitpoints: [], outputsDeferred: false });
		});

		it("insertCompleted: idempotent by name (no overwrite), lost without ownership, seq collision throws", async () => {
			const { owner } = await running();
			expect(
				await s().steps.insertCompleted(owner, {
					kind: "run",
					name: "a",
					seqIndex: 1,
					output: "first",
				})
			).toBe("ok");
			expect(
				await s().steps.insertCompleted(owner, {
					kind: "run",
					name: "a",
					seqIndex: 1,
					output: "second",
				})
			).toBe("ok");
			expect((await s().steps.list(owner.runId)).map(x => x.output)).toEqual(["first"]);
			await expect(
				s().steps.insertCompleted(owner, { kind: "run", name: "b", seqIndex: 1, output: 2 })
			).rejects.toThrow(/duplicate key|unique/i);
			expect(
				await s().steps.insertCompleted(
					{ runId: owner.runId, executionId: randomUUID() },
					{ kind: "run", name: "c", seqIndex: 3, output: null }
				)
			).toBe("lost");
		});

		it("insertSleep / completeSleep follow the store clock", async () => {
			const { owner } = await running();
			const r = await sleepStep(owner, "nap", 1, 60_000);
			expect(r.delayMs).toBeGreaterThan(58_000);
			expect(r.delayMs).toBeLessThanOrEqual(60_000);
			expect(await s().steps.completeSleep(owner, "nap")).toBe("not_due");
			expect(await s().steps.completeSleep(owner, "missing")).toBe("not_due");
			await get().advance(61_000);
			expect(await s().steps.completeSleep(owner, "nap")).toBe("ok");
			expect((await s().steps.list(owner.runId))[0]).toMatchObject({
				status: "completed",
				kind: "sleep",
				wakeAt: expect.any(Date),
			});
			expect(
				await s().steps.completeSleep({ runId: owner.runId, executionId: randomUUID() }, "nap")
			).toBe("lost");
			await expect(sleepStep(owner, "nap", 9, 1)).rejects.toThrow(/duplicate key|unique/i);
		});

		it("insertWaitRef: `#wait` step waiting with { waitpointId }; idempotent; seq collision throws", async () => {
			const { owner } = await running();
			const waitpointId = randomUUID();
			const step = { kind: "wait" as const, name: "approval#wait", seqIndex: 2, waitpointId };
			expect(await s().steps.insertWaitRef(owner, step)).toBe("ok");
			expect(await s().steps.insertWaitRef(owner, step)).toBe("ok");
			expect(await s().steps.list(owner.runId)).toMatchObject([
				{
					name: "approval#wait",
					kind: "wait",
					status: "waiting",
					output: { waitpointId },
					wakeAt: null,
				},
			]);
			await expect(
				s().steps.insertWaitRef(owner, { ...step, name: "other#wait", waitpointId: randomUUID() })
			).rejects.toThrow(/duplicate key|unique/i);
			expect(
				await s().steps.insertWaitRef({ runId: owner.runId, executionId: randomUUID() }, step)
			).toBe("lost");
		});

		const shapes: [string, Json | null][] = [
			["a bare string", "hello"],
			["a string with quotes/backslashes", 'say "hi" \\ {x,y}'],
			["an array of scalars", [1, "two", false, null]],
			["a nested object", { a: { b: { c: ["d", { e: null }] } } }],
			["a number", 42],
			["null", null],
		];
		for (const [label, output] of shapes) {
			it(`step outputs round-trip ${label}`, async () => {
				const { owner } = await running();
				await s().steps.insertCompleted(owner, { kind: "run", name: "s", seqIndex: 1, output });
				expect((await s().steps.list(owner.runId))[0]?.output).toEqual(output);
				expect((await s().steps.loadForExecution(owner.runId, BIG)).steps[0]?.output).toEqual(
					output
				);
			});
		}
	});

	// -------------------------------------------------------------------------
	describe("contract: logs", () => {
		it("round-trips data, defaults ts, paginates by id and scopes by run", async () => {
			const { owner } = await running();
			const other = await running();
			const explicitTs = new Date(Date.UTC(2020, 0, 1));
			await s().logs.insertBatch([
				{ runId: owner.runId, level: "info", message: "m0", data: null },
				{ runId: owner.runId, level: "warn", message: "m1", data: "bare", ts: explicitTs },
				{ runId: other.owner.runId, level: "info", message: "other", data: [1, { a: 2 }] },
			]);
			await s().logs.insertBatch(
				Array.from({ length: 23 }, (_, i) => ({
					runId: owner.runId,
					level: "debug" as const,
					message: `m${i + 2}`,
					data: { i },
				}))
			);
			await s().logs.insertBatch([]);
			const first = await s().logs.list(owner.runId, { limit: 10 });
			expect(first.map(l => l.message)).toEqual(Array.from({ length: 10 }, (_, i) => `m${i}`));
			expect(first[0]?.data).toBeNull();
			expect(first[1]).toMatchObject({ data: "bare", level: "warn" });
			expect(first[1]?.ts.toISOString()).toBe(explicitTs.toISOString());
			expect(first[0]?.ts).toBeInstanceOf(Date);
			const second = await s().logs.list(owner.runId, { afterId: first[9]?.id ?? "0", limit: 10 });
			expect(second.map(l => l.message)).toEqual(
				Array.from({ length: 10 }, (_, i) => `m${i + 10}`)
			);
			const third = await s().logs.list(owner.runId, { afterId: second[9]?.id ?? "0", limit: 10 });
			expect(third).toHaveLength(5);
			expect((await s().logs.list(other.owner.runId, { limit: 10 })).map(l => l.data)).toEqual([
				[1, { a: 2 }],
			]);
			expect(typeof first[0]?.id).toBe("string");
		});

		it("an unknown run violates the FK and inserts nothing", async () => {
			const { owner } = await running();
			await expect(
				s().logs.insertBatch([
					{ runId: owner.runId, level: "info", message: "ok", data: null },
					{ runId: randomUUID(), level: "info", message: "orphan", data: null },
				])
			).rejects.toThrow(/foreign key/);
			expect(await s().logs.list(owner.runId, { limit: 10 })).toEqual([]);
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: withTransaction", () => {
		it("commits: writes made through tx are visible afterwards", async () => {
			const { owner } = await running();
			const result = await s().withTransaction(async tx => {
				expect(
					await tx.steps.insertCompleted(owner, { kind: "run", name: "a", seqIndex: 1, output: 1 })
				).toBe("ok");
				return "value";
			});
			expect(result).toBe("value");
			expect(await s().steps.list(owner.runId)).toHaveLength(1);
		});

		it("rolls back every write when the callback throws; the error propagates unchanged", async () => {
			const { owner } = await running();
			class CustomError extends Error {}
			const newId = randomUUID();
			await expect(
				s().withTransaction(async tx => {
					await tx.steps.insertCompleted(owner, {
						kind: "run",
						name: "a",
						seqIndex: 1,
						output: null,
					});
					await tx.logs.insertBatch([
						{ runId: owner.runId, level: "info", message: "hi", data: null },
					]);
					await tx.runs.create(newRun({ id: newId }));
					await tx.runs.complete(owner, "done");
					throw new CustomError("boom");
				})
			).rejects.toBeInstanceOf(CustomError);
			expect(await s().steps.list(owner.runId)).toEqual([]);
			expect(await s().logs.list(owner.runId, { limit: 10 })).toEqual([]);
			expect(await s().runs.get(newId)).toBeNull();
			expect((await s().runs.get(owner.runId))?.status).toBe("running");
		});

		it("a failed statement aborts the transaction: later statements fail and nothing commits", async () => {
			const { owner } = await running();
			await s().withTransaction(async tx => {
				await tx.steps.insertCompleted(owner, {
					kind: "run",
					name: "a",
					seqIndex: 1,
					output: null,
				});
				await expect(
					tx.logs.insertBatch([{ runId: randomUUID(), level: "info", message: "x", data: null }])
				).rejects.toThrow(/foreign key/);
				await expect(tx.runs.get(owner.runId)).rejects.toThrow(/aborted/);
			});
			expect(await s().steps.list(owner.runId)).toEqual([]);
		});
	});

	// -------------------------------------------------------------------------
	describe("contract: schedules", () => {
		const input = (o: Partial<Parameters<Store["schedules"]["upsert"]>[0]> = {}) => ({
			id: "daily",
			taskId: "task-a",
			cron: "0 9 * * *",
			timezone: "America/Belem" as string | null,
			payload: { url: "https://example.com" } as Json | null,
			source: "api" as const,
			...o,
		});

		it("upsert reports created and *Changed flags; overwrites every column", async () => {
			const first = await s().schedules.upsert(input());
			expect(first).toMatchObject({ created: true, cronChanged: true, timezoneChanged: true });
			expect(first.schedule).toMatchObject({
				id: "daily",
				payload: { url: "https://example.com" },
			});
			expect(await s().schedules.upsert(input())).toMatchObject({
				created: false,
				cronChanged: false,
				timezoneChanged: false,
			});
			expect(await s().schedules.upsert(input({ cron: "0 10 * * *" }))).toMatchObject({
				cronChanged: true,
				timezoneChanged: false,
			});
			expect(
				await s().schedules.upsert(input({ cron: "0 10 * * *", timezone: null }))
			).toMatchObject({
				cronChanged: false,
				timezoneChanged: true,
			});
			const last = await s().schedules.upsert(
				input({ taskId: "task-b", payload: null, source: "declarative", timezone: null })
			);
			expect(last.schedule).toMatchObject({
				taskId: "task-b",
				payload: null,
				source: "declarative",
				timezone: null,
				cron: "0 9 * * *",
			});
			expect(last.schedule.createdAt.getTime()).toBe(first.schedule.createdAt.getTime());
			expect(await s().schedules.get("daily")).toEqual(last.schedule);
		});

		it("rejects invalid ids; get/delete unknown ids", async () => {
			await expect(s().schedules.upsert(input({ id: "has:colon" }))).rejects.toThrow(/invalid/);
			await expect(s().schedules.upsert(input({ id: "" }))).rejects.toThrow(/invalid/);
			expect(await s().schedules.get("nope")).toBeNull();
			expect(await s().schedules.delete("nope")).toBe(false);
			await s().schedules.upsert(input());
			expect(await s().schedules.delete("daily")).toBe(true);
			expect(await s().schedules.get("daily")).toBeNull();
		});

		it("list filters by source, orders by id; prune keeps api schedules", async () => {
			await s().schedules.upsert(input({ id: "c", source: "declarative" }));
			await s().schedules.upsert(input({ id: "a", source: "declarative" }));
			await s().schedules.upsert(input({ id: "b", source: "api" }));
			expect((await s().schedules.list()).map(x => x.id)).toEqual(["a", "b", "c"]);
			expect((await s().schedules.listDeclarative()).map(x => x.id)).toEqual(["a", "c"]);
			expect((await s().schedules.list({ source: "api" })).map(x => x.id)).toEqual(["b"]);
			expect(await s().schedules.deleteDeclarativeExcept(["a"])).toBe(1);
			expect((await s().schedules.list()).map(x => x.id)).toEqual(["a", "b"]);
			expect(await s().schedules.deleteDeclarativeExcept([])).toBe(1);
			expect((await s().schedules.list()).map(x => x.id)).toEqual(["b"]);
		});

		// Fix 4 (opus review, Phase 8): `requireSourceMatch` guards a declarative sync
		// (`defineTask({ schedule })`) and an `engine.schedules.upsert()` (source "api") call from
		// clobbering each other's row when they collide on the same id.
		describe("upsert({ requireSourceMatch: true })", () => {
			it("writes normally when there is no existing row", async () => {
				const result = await s().schedules.upsert(input({ id: "fresh" }), {
					requireSourceMatch: true,
				});
				expect(result.conflict).toBeUndefined();
				expect(result.created).toBe(true);
				expect(result.schedule).toMatchObject({ id: "fresh", source: "api" });
			});

			it("writes normally when the existing row already has the same source", async () => {
				await s().schedules.upsert(input({ id: "same-source", source: "api" }));
				const result = await s().schedules.upsert(
					input({ id: "same-source", source: "api", cron: "0 10 * * *" }),
					{ requireSourceMatch: true }
				);
				expect(result.conflict).toBeUndefined();
				expect(result.schedule.cron).toBe("0 10 * * *");
			});

			it("blocks the write and reports the conflict when the existing row has a different source (declarative vs api)", async () => {
				await s().schedules.upsert(
					input({ id: "conflict-1", source: "declarative", cron: "0 9 * * *" })
				);
				const result = await s().schedules.upsert(
					input({ id: "conflict-1", source: "api", cron: "0 10 * * *" }),
					{ requireSourceMatch: true }
				);
				expect(result.conflict).toEqual({ existingSource: "declarative" });
				// Nothing was written: the row is exactly as it was before this call.
				expect(result.schedule).toMatchObject({ source: "declarative", cron: "0 9 * * *" });
				expect(await s().schedules.get("conflict-1")).toMatchObject({
					source: "declarative",
					cron: "0 9 * * *",
				});
			});

			it("blocks the write in the other direction too (api vs declarative)", async () => {
				await s().schedules.upsert(input({ id: "conflict-2", source: "api", cron: "0 9 * * *" }));
				const result = await s().schedules.upsert(
					input({ id: "conflict-2", source: "declarative", cron: "0 10 * * *" }),
					{ requireSourceMatch: true }
				);
				expect(result.conflict).toEqual({ existingSource: "api" });
				expect(await s().schedules.get("conflict-2")).toMatchObject({
					source: "api",
					cron: "0 9 * * *",
				});
			});

			it("without requireSourceMatch, a different source still overwrites (unconditioned upsert)", async () => {
				await s().schedules.upsert(input({ id: "no-guard", source: "declarative" }));
				const result = await s().schedules.upsert(input({ id: "no-guard", source: "api" }));
				expect(result.conflict).toBeUndefined();
				expect(result.schedule.source).toBe("api");
			});
		});
	});
}
