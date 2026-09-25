import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { createSqlContext } from "../../src/store/sql/db.js";
import { createRunStore } from "../../src/store/sql/runs.js";
import type { Ownership, SerializedError } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";
import { useRunFixtures } from "../helpers/run-fixtures.js";

const ns = useTestNamespace();
const f = useRunFixtures(ns);

const ERR: SerializedError = { name: "Error", message: "boom", retryable: true };

async function rawRun(id: string) {
	const rows = await f.sql<{
		execution_id: string | null;
		updated_at: Date;
		heartbeat_at: Date | null;
		run_after: Date | null;
	}>(`SELECT execution_id, updated_at, heartbeat_at, run_after FROM ${f.t("runs")} WHERE id = $1`, [
		id,
	]);
	return rows[0];
}

/** Parent running -> triggerChildren(mode) -> returns child ids + waitpoint id. */
async function spawn(parent: Ownership, mode: "trigger" | "wait" | "batch", n = 1, name = "sub") {
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
	const res = await f.runs.triggerChildren({
		parent,
		stepName: name,
		seqIndex: 0,
		mode,
		waitpointId,
		timeoutMs: null,
		children,
	});
	if (res.kind !== "created") throw new Error("triggerChildren lost");
	return { childIds: children.map(c => c.id), waitpointId, res };
}

describe("RunStore: create / acquire / heartbeat", () => {
	it("create computes run_after and delay from the DB clock", async () => {
		const res = await f.runs.create(f.newRun({ delayMs: 5_000 }));
		if (res.kind !== "created") throw new Error("expected created");
		expect(res.run).toMatchObject({ status: "queued", attempt: 1, enqueueSeq: 1 });
		expect(res.enqueue.delayMs).toBeGreaterThan(4_000);
		expect(res.enqueue.delayMs).toBeLessThanOrEqual(5_000);
		const ms = await f.sql<{ exact: boolean }>(
			`SELECT created_at = date_trunc('milliseconds', created_at) AS exact FROM ${f.t("runs")} WHERE id = $1`,
			[res.run.id]
		);
		expect(ms[0]?.exact).toBe(true);
	});

	it("acquire with a wrong seq returns null; right seq acquires", async () => {
		const res = await f.runs.create(f.newRun());
		if (res.kind !== "created") throw new Error("expected created");
		expect(await f.runs.acquire(res.run.id, randomUUID(), 2)).toBeNull();
		const exec = randomUUID();
		const run = await f.runs.acquire(res.run.id, exec, 1);
		expect(run).toMatchObject({ status: "running", executionId: exec });
		expect(run?.startedAt).toBeInstanceOf(Date);
		expect(run?.heartbeatAt).toBeInstanceOf(Date);
	});

	it("two concurrent acquires: exactly one wins", async () => {
		const res = await f.runs.create(f.newRun());
		if (res.kind !== "created") throw new Error("expected created");
		const results = await Promise.all(
			Array.from({ length: 4 }, () => f.runs.acquire(res.run.id, randomUUID(), 1))
		);
		expect(results.filter(r => r !== null)).toHaveLength(1);
	});

	it("heartbeat touches only heartbeat_at; lost after ownership changes; not on a tx", async () => {
		const { owner } = await f.running();
		const before = await rawRun(owner.runId);
		await f.sql(`SELECT pg_sleep(0.01)`);
		expect(await f.runs.heartbeat(owner.runId, owner.executionId, 1_000)).toBe("ok");
		const after = await rawRun(owner.runId);
		expect(after?.updated_at.getTime()).toBe(before?.updated_at.getTime());
		expect(after?.heartbeat_at?.getTime()).toBeGreaterThan(before?.heartbeat_at?.getTime() ?? 0);
		expect(await f.runs.heartbeat(owner.runId, randomUUID(), 1_000)).toBe("lost");

		const client = await ns.pool.connect();
		try {
			const txRuns = createRunStore({ kind: "tx", client }, createSqlContext(ns.schema));
			await expect(txRuns.heartbeat(owner.runId, owner.executionId, 1_000)).rejects.toThrow();
		} finally {
			client.release();
		}
		// statement_timeout was LOCAL: the pool's sessions still have the default.
		const rows = await f.sql<{ statement_timeout: string }>("SHOW statement_timeout");
		expect(rows[0]?.statement_timeout).toBe("0");
	});
});

describe("RunStore: terminal transitions and ownership", () => {
	it("complete sets output/finished_at and clears execution_id and run_after", async () => {
		const { owner } = await f.running();
		const res = await f.runs.complete(owner, [1, "two", { three: 3 }]);
		if (res.kind !== "completed") throw new Error("expected completed");
		const run = await f.runs.get(owner.runId);
		expect(res).toEqual({
			kind: "completed",
			updatedAt: run?.updatedAt,
			parent: null,
			expireJobsToRemove: [],
		});
		expect(run).toMatchObject({
			status: "completed",
			output: [1, "two", { three: 3 }],
			executionId: null,
			runAfter: null,
		});
		expect(run?.finishedAt).toBeInstanceOf(Date);
	});

	it("fail stores the error; a second write from the same execution is lost", async () => {
		const { owner } = await f.running();
		const res = await f.runs.fail(owner, ERR);
		if (res.kind !== "failed") throw new Error("expected failed");
		const run = await f.runs.get(owner.runId);
		expect(res).toEqual({
			kind: "failed",
			updatedAt: run?.updatedAt,
			parent: null,
			expireJobsToRemove: [],
		});
		expect(run?.error).toEqual(ERR);
		expect(await f.runs.complete(owner, 1)).toEqual({ kind: "lost" });
	});

	it("retry: queued, attempt+1, seq+1, delay from backoff, execution_id NULL", async () => {
		const { owner } = await f.running();
		const res = await f.runs.retry(owner, ERR, 2_000);
		if (res.kind !== "retrying") throw new Error("expected retrying");
		expect(res.attempt).toBe(2);
		expect(res.enqueue.seq).toBe(2);
		expect(res.enqueue.delayMs).toBeGreaterThan(1_000);
		expect(res.enqueue.delayMs).toBeLessThanOrEqual(2_000);
		expect(await f.runs.get(owner.runId)).toMatchObject({
			status: "queued",
			executionId: null,
			error: ERR,
		});
		expect(await f.runs.retry(owner, ERR, 0)).toEqual({ kind: "lost" });
	});

	it("release keeps attempt, bumps seq; the zombie cannot write afterwards", async () => {
		const { owner } = await f.running();
		const res = await f.runs.release(owner);
		expect(res).toMatchObject({ kind: "released", enqueue: { seq: 2, delayMs: 0 } });
		expect(await f.runs.get(owner.runId)).toMatchObject({
			status: "queued",
			attempt: 1,
			executionId: null,
		});
		expect(await f.runs.complete(owner, null)).toEqual({ kind: "lost" });
		expect(await f.runs.heartbeat(owner.runId, owner.executionId, 1_000)).toBe("lost");
		expect(await f.runs.suspend(owner)).toEqual({ kind: "lost" });
	});

	it("cancel then complete -> lost", async () => {
		const { owner } = await f.running();
		const res = await f.runs.cancel(owner.runId, "user asked");
		expect(res).toMatchObject({
			kind: "canceled",
			canceled: [{ runId: owner.runId, previousStatus: "running", enqueueSeq: 1 }],
			parent: null,
		});
		expect(await f.runs.complete(owner, 1)).toEqual({ kind: "lost" });
		expect(await f.runs.fail(owner, ERR)).toEqual({ kind: "lost" });
		const run = await f.runs.get(owner.runId);
		expect(run).toMatchObject({
			status: "canceled",
			executionId: null,
			runAfter: null,
			error: { name: "Canceled", message: "user asked", retryable: false },
		});
		expect(await f.runs.cancel(owner.runId, "again")).toEqual({
			kind: "already_finished",
			status: "canceled",
		});
		expect(await f.runs.cancel(randomUUID(), "x")).toEqual({ kind: "not_found" });
	});

	it("string/array payloads and outputs round-trip as jsonb", async () => {
		const a = await f.running({ payload: "plain string" });
		const b = await f.running({ payload: [1, { two: [3] }] });
		expect((await f.runs.get(a.owner.runId))?.payload).toBe("plain string");
		expect((await f.runs.get(b.owner.runId))?.payload).toEqual([1, { two: [3] }]);
		expect((await f.runs.complete(a.owner, "done")).kind).toBe("completed");
		expect((await f.runs.complete(b.owner, ["x", "y"])).kind).toBe("completed");
		expect((await f.runs.get(a.owner.runId))?.output).toBe("done");
		expect((await f.runs.get(b.owner.runId))?.output).toEqual(["x", "y"]);
		const p = await f.running();
		const res = await f.runs.triggerChildren({
			parent: p.owner,
			stepName: "b",
			seqIndex: 0,
			mode: "batch",
			waitpointId: randomUUID(),
			timeoutMs: null,
			children: ["s", ["a", "b"], null].map((payload, i) => ({
				id: randomUUID(),
				taskId: "child",
				queue: "default",
				payload,
				maxAttempts: 1,
				idempotencyKey: `__child:${p.owner.runId}:b:${i}`,
				batchIndex: i,
			})),
		});
		if (res.kind !== "created") throw new Error("lost");
		const payloads = await Promise.all(res.enqueue.map(e => f.runs.get(e.runId)));
		expect(payloads.map(r => r?.payload)).toEqual(["s", ["a", "b"], null]);
	});
});

describe("RunStore: triggerChildren", () => {
	it("trigger mode: child queued, step completed with { id }, no waitpoint", async () => {
		const { owner } = await f.running();
		const { childIds, res } = await spawn(owner, "trigger");
		expect(res.waitpoint).toBeNull();
		const child = await f.runs.get(childIds[0] ?? "");
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
		expect(child).toMatchObject({ parentRunId: owner.runId, parentWaitpointId: null });
		const steps = await f.sql<{ status: string; output: unknown }>(
			`SELECT status, output FROM ${f.t("steps")} WHERE run_id = $1`,
			[owner.runId]
		);
		expect(steps).toEqual([{ status: "completed", output: { id: childIds[0] } }]);
		expect(
			(await f.idempotency.find("child-task", `__child:${owner.runId}:sub`))?.expiresAt
		).toBeNull();
	});

	it("delayMs: children get run_after = now() + delay and a DB-computed enqueue delay", async () => {
		const { owner } = await f.running();
		const id = randomUUID();
		const res = await f.runs.triggerChildren({
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
		const child = await f.runs.get(id);
		expect((child?.runAfter?.getTime() ?? 0) - (child?.createdAt.getTime() ?? 0)).toBeGreaterThan(
			59_000
		);
	});

	it("batch mode: waitpoint with pending_count, children with batch_index, timeout", async () => {
		const { owner } = await f.running();
		const children = Array.from({ length: 3 }, (_, i) => ({
			id: randomUUID(),
			taskId: "child-task",
			queue: "q2",
			payload: [i],
			maxAttempts: 2,
			idempotencyKey: `__child:${owner.runId}:b:${i}`,
			batchIndex: i,
		}));
		const waitpointId = randomUUID();
		const res = await f.runs.triggerChildren({
			parent: owner,
			stepName: "b",
			seqIndex: 3,
			mode: "batch",
			waitpointId,
			timeoutMs: 60_000,
			children,
		});
		if (res.kind !== "created") throw new Error("lost");
		expect(res.waitpoint?.id).toBe(waitpointId);
		expect(res.waitpoint?.expireDelayMs).toBeGreaterThan(59_000);
		const wp = await f.sql<{ kind: string; pending_count: number }>(
			`SELECT kind, pending_count FROM ${f.t("waitpoints")} WHERE id = $1`,
			[waitpointId]
		);
		expect(wp).toEqual([{ kind: "batch", pending_count: 3 }]);
		const results = await f.runs.listChildResults(waitpointId);
		expect(results.map(r => r.batchIndex)).toEqual([0, 1, 2]);
		expect(results.every(r => r.status === "queued")).toBe(true);
	});

	it("lost ownership -> lost; key conflict -> throws and rolls back everything", async () => {
		const { owner } = await f.running();
		await expect(spawn({ runId: owner.runId, executionId: randomUUID() }, "wait")).rejects.toThrow(
			"lost"
		);
		await spawn(owner, "wait", 1, "dup");
		await expect(spawn(owner, "wait", 1, "dup")).rejects.toThrow();
		const count = await f.sql<{ n: number }>(
			`SELECT count(*)::int AS n FROM ${f.t("runs")} WHERE parent_run_id = $1`,
			[owner.runId]
		);
		expect(count[0]?.n).toBe(1);
	});
});

describe("RunStore: recursive cancel", () => {
	it("cancels three levels, root first, walking through terminal nodes", async () => {
		const root = await f.running();
		const { childIds: level1 } = await spawn(root.owner, "wait", 1, "l1");
		const childOwner = await f.acquire(level1[0] ?? "");
		const { childIds: level2 } = await spawn(childOwner, "batch", 2, "l2");
		// A completed grandchild with its own (queued) great-grandchild.
		const doneOwner = await f.acquire(level2[0] ?? "");
		const { childIds: level3 } = await spawn(doneOwner, "trigger", 1, "l3");
		expect((await f.runs.complete(doneOwner, "ok")).kind).toBe("completed");

		const res = await f.runs.cancel(root.owner.runId, "stop");
		if (res.kind !== "canceled") throw new Error("expected canceled");
		const ids = res.canceled.map(c => c.runId);
		expect(ids[0]).toBe(root.owner.runId);
		expect(new Set(ids)).toEqual(
			new Set([root.owner.runId, level1[0], level2[1], level3[0]].filter(Boolean))
		);
		expect(res.canceled.find(c => c.runId === level1[0])?.previousStatus).toBe("running");
		expect((await f.runs.get(level2[0] ?? ""))?.status).toBe("completed");
		expect((await f.runs.get(level3[0] ?? ""))?.status).toBe("canceled");
		expect(await f.runs.complete(childOwner, 1)).toEqual({ kind: "lost" });
		expect(res.expireJobsToRemove).toEqual([]);
	});

	it("times out the pending waitpoints of every canceled run and reports their expire jobs", async () => {
		const root = await f.running();
		const rootTok = await f.waitpoints.createToken(root.owner, {
			waitpointId: randomUUID(),
			name: "rt",
			seqIndex: 10,
			timeoutMs: 60_000,
		});
		const { childIds, waitpointId: childWp } = await spawn(root.owner, "wait", 1, "c");
		const childOwner = await f.acquire(childIds[0] ?? "");
		const childTok = await f.waitpoints.createToken(childOwner, {
			waitpointId: randomUUID(),
			name: "ct",
			seqIndex: 1,
			timeoutMs: null,
		});
		if (rootTok.kind !== "created" || childTok.kind !== "created") throw new Error("lost");
		// A token that was already completed stays completed.
		const doneTok = await f.waitpoints.createToken(childOwner, {
			waitpointId: randomUUID(),
			name: "dt",
			seqIndex: 2,
			timeoutMs: 60_000,
		});
		if (doneTok.kind !== "created") throw new Error("lost");
		await f.waitpoints.completeToken(doneTok.waitpointId, 1);

		const res = await f.runs.cancel(root.owner.runId, "stop");
		if (res.kind !== "canceled") throw new Error("expected canceled");
		expect(res.expireJobsToRemove).toEqual([rootTok.waitpointId]);
		const rows = await f.sql<{ id: string; status: string; resolved_at: Date | null }>(
			`SELECT id, status, resolved_at FROM ${f.t("waitpoints")} WHERE id = ANY($1::uuid[])`,
			[[rootTok.waitpointId, childWp, childTok.waitpointId, doneTok.waitpointId]]
		);
		const byId = new Map(rows.map(r => [r.id, r]));
		for (const id of [rootTok.waitpointId, childWp, childTok.waitpointId]) {
			expect(byId.get(id)?.status).toBe("timed_out");
			expect(byId.get(id)?.resolved_at).toBeInstanceOf(Date);
		}
		expect(byId.get(doneTok.waitpointId)?.status).toBe("completed");
		// Canceled (terminal) runs: their tokens report run_finished.
		expect(await f.waitpoints.completeToken(childTok.waitpointId, "late")).toEqual({
			kind: "run_finished",
		});
		expect(await f.waitpoints.completeToken(rootTok.waitpointId, "late")).toEqual({
			kind: "run_finished",
		});
	});
});

describe("RunStore: list", () => {
	it("paginates with a composite cursor and ties on created_at", async () => {
		// 105 rows sharing the same created_at: the id tiebreak must not skip/duplicate rows.
		await f.sql(
			`INSERT INTO ${f.t("runs")} (id, task_id, queue, payload, max_attempts, created_at)
			 SELECT gen_random_uuid(), 'list-task', 'default', '{}'::jsonb, 1,
			        date_trunc('milliseconds', now())
			 FROM generate_series(1, 105)`
		);
		const page1 = await f.runs.list({ taskId: "list-task", limit: 1000 });
		expect(page1.items).toHaveLength(100);
		expect(page1.nextCursor).not.toBeNull();
		const page2 = await f.runs.list({
			taskId: "list-task",
			limit: 1000,
			...(page1.nextCursor ? { cursor: page1.nextCursor } : {}),
		});
		expect(page2.items).toHaveLength(5);
		expect(page2.nextCursor).toBeNull();
		const all = new Set([...page1.items, ...page2.items].map(r => r.id));
		expect(all.size).toBe(105);
	});

	it("walks pages of created runs in (created_at desc, id desc) order with filters", async () => {
		const created: string[] = [];
		for (let i = 0; i < 7; i++) {
			const r = await f.runs.create(f.newRun({ taskId: "pager" }));
			created.push(r.run.id);
		}
		await f.runs.create(f.newRun({ taskId: "other" }));
		const seen: string[] = [];
		let cursor: { createdAt: Date; id: string } | null = null;
		let pages = 0;
		do {
			const page = await f.runs.list({
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
		expect((await f.runs.list({ status: "running", limit: 10 })).items).toHaveLength(0);
		const future = await f.runs.list({ createdFrom: new Date(Date.now() + 3_600_000), limit: 5 });
		expect(future.items).toHaveLength(0);
	});
});

/** A token plus its `tok#wait` step: only awaited tokens block a resume (decision 3, owner). */
async function awaitedToken(owner: Ownership): Promise<void> {
	const waitpointId = randomUUID();
	await f.waitpoints.createToken(owner, { waitpointId, name: "tok", seqIndex: 0, timeoutMs: null });
	await f.sql(
		`INSERT INTO ${f.t("steps")} (run_id, name, seq_index, kind, status, output)
		 VALUES ($1, 'tok#wait', 1, 'wait', 'waiting', $2::jsonb)`,
		[owner.runId, JSON.stringify({ waitpointId })]
	);
}

describe("RunStore: reconciler queries", () => {
	it("listStaleQueued returns stale queued runs with the current seq", async () => {
		const stale = await f.runs.create(f.newRun());
		const fresh = await f.runs.create(f.newRun());
		await f.sql(
			`UPDATE ${f.t("runs")} SET run_after = now() - interval '1 minute', enqueue_seq = 4 WHERE id = $1`,
			[stale.run.id]
		);
		const res = await f.runs.listStaleQueued(30_000, 10);
		expect(res).toEqual([
			{
				runId: stale.run.id,
				taskId: "task-a",
				queue: "default",
				seq: 4,
				delayMs: 0,
				status: "queued",
				updatedAt: stale.run.updatedAt,
			},
		]);
		expect(res.map(r => r.runId)).not.toContain(fresh.run.id);
	});

	it("listStaleWaiting ignores runs with a pending (awaited) waitpoint", async () => {
		const a = await f.running();
		const b = await f.running();
		await awaitedToken(b.owner);
		await f.sql(
			`UPDATE ${f.t("runs")} SET status = 'waiting', execution_id = NULL,
			   run_after = now() - interval '1 minute' WHERE id = ANY($1::uuid[])`,
			[[a.owner.runId, b.owner.runId]]
		);
		const res = await f.runs.listStaleWaiting(30_000, 10);
		expect(res.map(r => r.runId)).toEqual([a.owner.runId]);
		expect(res[0]?.seq).toBe(1);
	});

	it("repairOrphanWaiting resumes waiting runs with run_after NULL and nothing pending", async () => {
		const orphan = await f.running();
		const blocked = await f.running();
		await awaitedToken(blocked.owner);
		await f.sql(
			`UPDATE ${f.t("runs")} SET status = 'waiting', execution_id = NULL, run_after = NULL
			 WHERE id = ANY($1::uuid[])`,
			[[orphan.owner.runId, blocked.owner.runId]]
		);
		const res = await f.runs.repairOrphanWaiting(10);
		const orphanRun = await f.runs.get(orphan.owner.runId);
		expect(res).toEqual([
			{
				runId: orphan.owner.runId,
				taskId: "task-a",
				queue: "default",
				seq: 2,
				delayMs: 0,
				status: "waiting",
				updatedAt: orphanRun?.updatedAt,
			},
		]);
		const row = await rawRun(orphan.owner.runId);
		expect(row?.run_after).toBeInstanceOf(Date);
	});

	it("listStalled + recoverStalled: requeue while attempts remain, else fail", async () => {
		const retryable = await f.running({ maxAttempts: 2 });
		const exhausted = await f.running({ maxAttempts: 1 });
		const healthy = await f.running();
		await f.sql(
			`UPDATE ${f.t("runs")} SET heartbeat_at = now() - interval '2 minutes' WHERE id = ANY($1::uuid[])`,
			[[retryable.owner.runId, exhausted.owner.runId]]
		);
		const stalled = await f.runs.listStalled(60_000, 10);
		expect(new Set(stalled)).toEqual(new Set([retryable.owner.runId, exhausted.owner.runId]));

		const r1 = await f.runs.recoverStalled(retryable.owner.runId, 60_000, ERR);
		expect(r1).toMatchObject({
			kind: "requeued",
			attempt: 2,
			enqueue: { seq: 2, delayMs: 0, status: "queued" },
		});
		const r2 = await f.runs.recoverStalled(exhausted.owner.runId, 60_000, ERR);
		const exhaustedRun = await f.runs.get(exhausted.owner.runId);
		expect(r2).toEqual({
			kind: "failed",
			updatedAt: exhaustedRun?.updatedAt,
			parent: null,
			expireJobsToRemove: [],
		});
		expect(exhaustedRun).toMatchObject({
			status: "failed",
			executionId: null,
		});
		expect(await f.runs.recoverStalled(healthy.owner.runId, 60_000, ERR)).toEqual({
			kind: "not_stalled",
		});
		expect(await f.runs.complete(retryable.owner, 1)).toEqual({ kind: "lost" });
	});

	it("deleteFinishedBatch respects the parent rule and the batch size", async () => {
		const parent = await f.running();
		const { childIds } = await spawn(parent.owner, "trigger");
		const childOwner = await f.acquire(childIds[0] ?? "");
		await f.runs.complete(childOwner, 1);
		const roots: string[] = [];
		for (let i = 0; i < 3; i++) {
			const r = await f.running();
			await f.runs.complete(r.owner, i);
			roots.push(r.owner.runId);
		}
		const young = await f.running();
		await f.runs.complete(young.owner, 1);
		await f.sql(
			`UPDATE ${f.t("runs")} SET finished_at = now() - interval '2 days'
			 WHERE id = ANY($1::uuid[])`,
			[[...roots, childIds[0]]]
		);
		expect(await f.runs.deleteFinishedBatch(86_400_000, 2)).toBe(2);
		expect(await f.runs.deleteFinishedBatch(86_400_000, 10)).toBe(1);
		expect(await f.runs.deleteFinishedBatch(86_400_000, 10)).toBe(0);
		// Child of a running parent and the young run survive.
		expect(await f.runs.get(childIds[0] ?? "")).not.toBeNull();
		expect(await f.runs.get(young.owner.runId)).not.toBeNull();
	});
});
