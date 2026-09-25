import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type {
	ChildResult,
	Json,
	Ownership,
	SerializedError,
	WaitpointResolution,
} from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";
import { useRunFixtures } from "../helpers/run-fixtures.js";

const ns = useTestNamespace();
const f = useRunFixtures(ns);

const ERR: SerializedError = { name: "TypeError", message: "bad", retryable: false };

/**
 * A token plus (unless `awaited` is false) the `${name}#wait` step `waitForToken` writes: only an
 * awaited token blocks the run's resume (owner's interpretation of decision 3).
 */
async function token(
	owner: Ownership,
	name: string,
	timeoutMs: number | null = null,
	awaited = true
) {
	const waitpointId = randomUUID();
	const seqIndex = Number.parseInt(name.replace(/\D/g, "") || "0", 10);
	const res = await f.waitpoints.createToken(owner, { waitpointId, name, seqIndex, timeoutMs });
	if (res.kind !== "created") throw new Error("createToken lost");
	if (awaited) {
		await f.sql(
			`INSERT INTO ${f.t("steps")} (run_id, name, seq_index, kind, status, output)
			 VALUES ($1, $2, $3, 'wait', 'waiting', $4::jsonb)`,
			[owner.runId, `${name}#wait`, 100 + seqIndex, JSON.stringify({ waitpointId })]
		);
	}
	return { waitpointId, res };
}

/** Sleep step written directly (steps.ts belongs to another module); wake_at from the DB clock. */
async function sleepStep(runId: string, name: string, seqIndex: number, offsetMs: number) {
	await f.sql(
		`INSERT INTO ${f.t("steps")} (run_id, name, seq_index, kind, status, wake_at)
		 VALUES ($1, $2, $3, 'sleep', 'waiting', now() + $4::float8 * interval '1 millisecond')`,
		[runId, name, seqIndex, offsetMs]
	);
}

async function spawn(
	parent: Ownership,
	mode: "wait" | "batch",
	n: number,
	timeoutMs: number | null = null
) {
	const waitpointId = randomUUID();
	const children = Array.from({ length: n }, (_, i) => ({
		id: randomUUID(),
		taskId: "child-task",
		queue: "default",
		payload: { i },
		maxAttempts: 1,
		idempotencyKey: `__child:${parent.runId}:s${mode === "batch" ? `:${i}` : ""}`,
		batchIndex: mode === "batch" ? i : null,
	}));
	const res = await f.runs.triggerChildren({
		parent,
		stepName: "s",
		seqIndex: 0,
		mode,
		waitpointId,
		timeoutMs,
		children,
	});
	if (res.kind !== "created") throw new Error("triggerChildren lost");
	return { waitpointId, childIds: children.map(c => c.id) };
}

/** Executor-side mapping of a child result (decision 10), used to assert the shapes. */
function toOutcome(
	r: ChildResult
): { ok: true; output: Json | null } | { ok: false; error: unknown } {
	if (r.status === "completed") return { ok: true, output: r.output };
	if (r.status === "canceled") return { ok: false, error: { name: r.error?.name } };
	return { ok: false, error: r.error };
}

async function wpRow(id: string) {
	const rows = await f.sql<{ status: string; pending_count: number | null }>(
		`SELECT status, pending_count FROM ${f.t("waitpoints")} WHERE id = $1`,
		[id]
	);
	return rows[0];
}

describe("WaitpointStore: tokens", () => {
	it("createToken writes a pending waitpoint and the step { id }; lost without ownership", async () => {
		const { owner } = await f.running();
		const { waitpointId, res } = await token(owner, "t0", 30_000, false);
		expect(res.expiresAt).toBeInstanceOf(Date);
		expect(res.expireDelayMs).toBeGreaterThan(29_000);
		expect(res.expireDelayMs).toBeLessThanOrEqual(30_000);
		const steps = await f.sql<{ name: string; status: string; output: unknown }>(
			`SELECT name, status, output FROM ${f.t("steps")} WHERE run_id = $1`,
			[owner.runId]
		);
		expect(steps).toEqual([{ name: "t0", status: "completed", output: { id: waitpointId } }]);
		expect(await f.waitpoints.getToken(waitpointId)).toEqual({
			status: "pending",
			output: null,
			runId: owner.runId,
		});
		const lost = await f.waitpoints.createToken(
			{ runId: owner.runId, executionId: randomUUID() },
			{ waitpointId: randomUUID(), name: "t1", seqIndex: 1, timeoutMs: null }
		);
		expect(lost).toEqual({ kind: "lost" });
	});

	it("createToken is idempotent by (run_id, step_name): the existing token is returned", async () => {
		const { owner } = await f.running();
		const first = await token(owner, "t0", 60_000);
		const again = await f.waitpoints.createToken(owner, {
			waitpointId: randomUUID(),
			name: "t0",
			seqIndex: 0,
			timeoutMs: 60_000,
		});
		expect(again).toMatchObject({ kind: "created", waitpointId: first.waitpointId });
		expect(again.kind === "created" && again.expireDelayMs).toBeGreaterThan(50_000);
		const count = await f.sql<{ n: number }>(
			`SELECT count(*)::int AS n FROM ${f.t("waitpoints")} WHERE run_id = $1`,
			[owner.runId]
		);
		expect(count[0]?.n).toBe(1);
		// No longer pending -> no expire delay to schedule.
		await f.waitpoints.completeToken(first.waitpointId, 1);
		const done = await f.waitpoints.createToken(owner, {
			waitpointId: randomUUID(),
			name: "t0",
			seqIndex: 0,
			timeoutMs: 60_000,
		});
		expect(done).toMatchObject({ waitpointId: first.waitpointId, expireDelayMs: null });
	});

	it("fail/complete time out the run's own pending waitpoints (expireJobsToRemove)", async () => {
		const a = await f.running();
		const withExpiry = await token(a.owner, "t0", 60_000);
		const noExpiry = await token(a.owner, "t1");
		const failRes = await f.runs.fail(a.owner, ERR);
		if (failRes.kind !== "failed") throw new Error("expected failed");
		const aRun = await f.runs.get(a.owner.runId);
		expect(failRes).toEqual({
			kind: "failed",
			updatedAt: aRun?.updatedAt,
			parent: null,
			expireJobsToRemove: [withExpiry.waitpointId],
		});
		expect((await wpRow(withExpiry.waitpointId))?.status).toBe("timed_out");
		expect((await wpRow(noExpiry.waitpointId))?.status).toBe("timed_out");
		// The run is terminal: `run_finished`, not a misleading `timed_out`.
		expect(await f.waitpoints.completeToken(noExpiry.waitpointId, 1)).toEqual({
			kind: "run_finished",
		});

		const b = await f.running();
		const tok = await token(b.owner, "t0", 60_000);
		expect(await f.runs.complete(b.owner, 1)).toMatchObject({
			expireJobsToRemove: [tok.waitpointId],
		});
		expect((await wpRow(tok.waitpointId))?.status).toBe("timed_out");
	});

	it("completeToken: not_found, completed, then not_pending", async () => {
		const { owner } = await f.running();
		const { waitpointId } = await token(owner, "t0");
		expect(await f.waitpoints.completeToken(randomUUID(), 1)).toEqual({ kind: "not_found" });
		const first = await f.waitpoints.completeToken(waitpointId, { v: [1, 2] });
		// Run is still running: no enqueue, no expire job (no timeout).
		expect(first).toEqual({
			kind: "completed",
			resolution: { waitpointId, hasExpireJob: false, resume: null },
		});
		expect((await f.waitpoints.getToken(waitpointId))?.output).toEqual({ v: [1, 2] });
		expect(await f.waitpoints.completeToken(waitpointId, 2)).toEqual({
			kind: "not_pending",
			status: "completed",
		});
	});

	it("two tokens completed concurrently on a waiting run: exactly one resume", async () => {
		const { owner } = await f.running();
		const a = await token(owner, "t0", 60_000);
		const b = await token(owner, "t1");
		expect(await f.runs.suspend(owner)).toMatchObject({ kind: "waiting_on_waitpoints" });
		const results = await Promise.all([
			f.waitpoints.completeToken(a.waitpointId, "a"),
			f.waitpoints.completeToken(b.waitpointId, "b"),
		]);
		const resumes = results.flatMap(r =>
			r.kind === "completed" && r.resolution.resume ? [r] : []
		);
		expect(resumes).toHaveLength(1);
		expect(resumes[0]?.kind === "completed" && resumes[0].resolution.resume).toMatchObject({
			runId: owner.runId,
			seq: 3,
			delayMs: 0,
		});
		const withExpire = results.find(
			r => r.kind === "completed" && r.resolution.waitpointId === a.waitpointId
		);
		expect(withExpire?.kind === "completed" && withExpire.resolution.hasExpireJob).toBe(true);
		expect(await f.runs.get(owner.runId)).toMatchObject({ status: "waiting", enqueueSeq: 3 });
	});

	it("stress: 15 rounds of 3 concurrent token completions, always exactly one resume", async () => {
		for (let round = 0; round < 15; round++) {
			const { owner } = await f.running();
			const tokens = [await token(owner, "t0"), await token(owner, "t1"), await token(owner, "t2")];
			await f.runs.suspend(owner);
			const results = await Promise.all(
				tokens.map(t => f.waitpoints.completeToken(t.waitpointId, round))
			);
			const resumes = results.filter(r => r.kind === "completed" && r.resolution.resume);
			expect(resumes).toHaveLength(1);
		}
	});

	it("completeToken on a finished (completed or canceled) run: run_finished; an already completed token stays completed", async () => {
		const { owner } = await f.running();
		const done = await token(owner, "t0");
		const open = await token(owner, "t1");
		await f.waitpoints.completeToken(done.waitpointId, 1);
		await f.runs.complete(owner, null);
		expect(await f.waitpoints.completeToken(done.waitpointId, 2)).toEqual({
			kind: "not_pending",
			status: "completed",
		});
		expect(await f.waitpoints.completeToken(open.waitpointId, 2)).toEqual({
			kind: "run_finished",
		});
		const c = await f.running();
		const tok = await token(c.owner, "t0");
		await f.runs.cancel(c.owner.runId, "stop");
		expect(await f.waitpoints.completeToken(tok.waitpointId, 1)).toEqual({ kind: "run_finished" });
	});

	it("expiry racing completion: exactly one wins and the run is resumed once (10 rounds)", async () => {
		for (let round = 0; round < 10; round++) {
			const { owner } = await f.running();
			const { waitpointId } = await token(owner, "t0", 60_000);
			await f.runs.suspend(owner);
			await f.sql(
				`UPDATE ${f.t("waitpoints")} SET expires_at = now() - interval '1 second' WHERE id = $1`,
				[waitpointId]
			);
			const [completed, expired] = await Promise.all([
				f.waitpoints.completeToken(waitpointId, round),
				f.waitpoints.expire(waitpointId),
			]);
			const wins = [completed.kind === "completed", expired.kind === "expired"].filter(Boolean);
			expect(wins).toHaveLength(1);
			const resumes = [
				completed.kind === "completed" ? completed.resolution.resume : null,
				expired.kind === "expired" ? expired.resolution.resume : null,
			].filter(r => r !== null);
			expect(resumes).toHaveLength(1);
			const status = (await wpRow(waitpointId))?.status;
			expect(status).toBe(completed.kind === "completed" ? "completed" : "timed_out");
			if (completed.kind !== "completed") {
				expect(completed).toEqual({ kind: "not_pending", status: "timed_out" });
			}
			expect(await f.runs.get(owner.runId)).toMatchObject({ status: "waiting", enqueueSeq: 3 });
		}
	});

	it("only awaited tokens block: a created-but-not-awaited token lets a sleeping run wake up", async () => {
		const { owner } = await f.running();
		const { waitpointId } = await token(owner, "t0", null, false);
		await sleepStep(owner.runId, "nap", 1, 60_000);
		const suspended = await f.runs.suspend(owner);
		if (suspended.kind !== "sleeping") throw new Error(`expected sleeping, got ${suspended.kind}`);
		expect(suspended.enqueue.delayMs).toBeGreaterThan(55_000);
		// Completing it while the run sleeps: the resume still waits for the sleep's wake_at.
		const res = await f.waitpoints.completeToken(waitpointId, null);
		const resume = res.kind === "completed" ? res.resolution.resume : null;
		expect(resume?.delayMs).toBeGreaterThan(55_000);

		// Once awaited (the `#wait` step exists) the same token blocks.
		const b = await f.running();
		await token(b.owner, "t0", null, true);
		await sleepStep(b.owner.runId, "nap", 1, 60_000);
		expect(await f.runs.suspend(b.owner)).toMatchObject({ kind: "waiting_on_waitpoints" });
	});

	it("token + future sleep: completing the token resumes at the sleep's wake_at", async () => {
		const { owner } = await f.running();
		const { waitpointId } = await token(owner, "t0");
		await sleepStep(owner.runId, "nap", 1, 60_000);
		await f.runs.suspend(owner);
		const res = await f.waitpoints.completeToken(waitpointId, null);
		const resume = res.kind === "completed" ? res.resolution.resume : null;
		expect(resume?.delayMs).toBeGreaterThan(55_000);
		expect(resume?.delayMs).toBeLessThanOrEqual(60_000);
	});

	it("expire: token not due -> not_due + DB delay; due -> timed_out and the waiting run resumes", async () => {
		const { owner } = await f.running();
		const { waitpointId } = await token(owner, "t0", 60_000);
		await f.runs.suspend(owner);
		const early = await f.waitpoints.expire(waitpointId);
		if (early.kind !== "not_due") throw new Error(`expected not_due, got ${early.kind}`);
		expect(early.delayMs).toBeGreaterThan(55_000);
		expect(early.delayMs).toBeLessThanOrEqual(60_000);
		expect((await wpRow(waitpointId))?.status).toBe("pending");
		expect(await f.waitpoints.listExpired(10)).toEqual([]);
		await f.sql(
			`UPDATE ${f.t("waitpoints")} SET expires_at = now() - interval '1 second' WHERE id = $1`,
			[waitpointId]
		);
		expect(await f.waitpoints.listExpired(10)).toEqual([waitpointId]);
		const res = await f.waitpoints.expire(waitpointId);
		expect(res).toMatchObject({
			kind: "expired",
			canceledChildren: [],
			expireJobsToRemove: [],
			resolution: { waitpointId, hasExpireJob: true, resume: { runId: owner.runId, seq: 3 } },
		});
		expect((await wpRow(waitpointId))?.status).toBe("timed_out");
		expect(await f.waitpoints.expire(waitpointId)).toEqual({ kind: "noop" });
		expect(await f.waitpoints.completeToken(waitpointId, 1)).toEqual({
			kind: "not_pending",
			status: "timed_out",
		});
		expect(await f.waitpoints.expire(randomUUID())).toEqual({ kind: "noop" });
	});

	it("expire: a token without expires_at is a noop, never not_due", async () => {
		const { owner } = await f.running();
		const { waitpointId } = await token(owner, "t0", null);
		expect(await f.waitpoints.expire(waitpointId)).toEqual({ kind: "noop" });
	});

	it("completeToken stores string and array outputs as jsonb", async () => {
		const { owner } = await f.running();
		const a = await token(owner, "t0");
		const b = await token(owner, "t1");
		expect((await f.waitpoints.completeToken(a.waitpointId, "just a string")).kind).toBe(
			"completed"
		);
		expect((await f.waitpoints.completeToken(b.waitpointId, ["x", { y: [1] }])).kind).toBe(
			"completed"
		);
		expect((await f.waitpoints.getToken(a.waitpointId))?.output).toBe("just a string");
		expect((await f.waitpoints.getToken(b.waitpointId))?.output).toEqual(["x", { y: [1] }]);
	});
});

describe("RunStore.suspend (decision 3)", () => {
	it("pending waitpoint -> waiting, run_after NULL, no enqueue", async () => {
		const { owner } = await f.running();
		await token(owner, "t0");
		await sleepStep(owner.runId, "nap", 1, 60_000);
		const suspended = await f.runs.suspend(owner);
		const afterSuspend = await f.runs.get(owner.runId);
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
		});
	});

	it("waiting_on_waitpoints bumps enqueue_seq: a stale redelivery of the old job cannot acquire", async () => {
		const { run, owner } = await f.running();
		await token(owner, "t0");
		expect(await f.runs.suspend(owner)).toMatchObject({ kind: "waiting_on_waitpoints" });
		expect(await f.runs.acquire(owner.runId, randomUUID(), run.enqueueSeq)).toBeNull();
		expect(await f.runs.get(owner.runId)).toMatchObject({
			status: "waiting",
			attempt: 1,
			enqueueSeq: run.enqueueSeq + 1,
		});
		// Reconciler case 3 still sees it only once nothing is pending (run_after stays NULL).
		expect(await f.runs.repairOrphanWaiting(10)).toEqual([]);
		expect(await f.runs.listStaleWaiting(0, 10)).toEqual([]);
	});

	it("only a future sleep -> waiting + enqueue with the DB-computed delay", async () => {
		const { owner } = await f.running();
		await sleepStep(owner.runId, "short", 0, 10_000);
		await sleepStep(owner.runId, "long", 1, 45_000);
		const res = await f.runs.suspend(owner);
		if (res.kind !== "sleeping") throw new Error(`expected sleeping, got ${res.kind}`);
		expect(res.enqueue.seq).toBe(2);
		expect(res.enqueue.delayMs).toBeGreaterThan(40_000);
		expect(res.enqueue.delayMs).toBeLessThanOrEqual(45_000);
		expect(await f.runs.get(owner.runId)).toMatchObject({ status: "waiting", executionId: null });
	});

	it("nothing pending (sleep already due) -> queued + immediate enqueue", async () => {
		const { owner } = await f.running();
		await sleepStep(owner.runId, "past", 0, -1_000);
		const res = await f.runs.suspend(owner);
		expect(res).toMatchObject({ kind: "requeued", enqueue: { seq: 2, delayMs: 0 } });
		expect(await f.runs.get(owner.runId)).toMatchObject({ status: "queued", executionId: null });
	});

	it("race: token completed between creation and suspension -> run requeued", async () => {
		const { owner } = await f.running();
		const { waitpointId } = await token(owner, "t0");
		const done = await f.waitpoints.completeToken(waitpointId, "early");
		expect(done.kind === "completed" && done.resolution.resume).toBeNull();
		const res = await f.runs.suspend(owner);
		expect(res).toMatchObject({ kind: "requeued", enqueue: { runId: owner.runId, seq: 2 } });
		expect(await f.runs.suspend(owner)).toEqual({ kind: "lost" });
	});
});

describe("Child resolution (decision 10)", () => {
	async function waitingParentWithChild() {
		const parent = await f.running({ taskId: "parent" });
		const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1);
		expect(await f.runs.suspend(parent.owner)).toMatchObject({ kind: "waiting_on_waitpoints" });
		const childOwner = await f.acquire(childIds[0] ?? "");
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
		const res = await f.runs.complete(childOwner, { answer: 42 });
		if (res.kind !== "completed") throw new Error("lost");
		expectResumed(res.parent, parent.owner.runId, waitpointId);
		expect((await wpRow(waitpointId))?.status).toBe("completed");
		const [child] = await f.runs.listChildResults(waitpointId);
		expect(child && toOutcome(child)).toEqual({ ok: true, output: { answer: 42 } });
	});

	it("failed child -> { ok: false, error }", async () => {
		const { parent, waitpointId, childOwner } = await waitingParentWithChild();
		const res = await f.runs.fail(childOwner, ERR);
		if (res.kind !== "failed") throw new Error("lost");
		expectResumed(res.parent, parent.owner.runId, waitpointId);
		const [child] = await f.runs.listChildResults(waitpointId);
		expect(child && toOutcome(child)).toEqual({ ok: false, error: ERR });
	});

	it("canceled child -> { ok: false, error: { name: 'Canceled' } }", async () => {
		const { parent, waitpointId, childOwner } = await waitingParentWithChild();
		const res = await f.runs.cancel(childOwner.runId, "nope");
		if (res.kind !== "canceled") throw new Error("expected canceled");
		expectResumed(res.parent, parent.owner.runId, waitpointId);
		const [child] = await f.runs.listChildResults(waitpointId);
		expect(child && toOutcome(child)).toEqual({ ok: false, error: { name: "Canceled" } });
	});

	it("stalled child failing definitively also resolves the parent", async () => {
		const { parent, waitpointId, childOwner } = await waitingParentWithChild();
		await f.sql(
			`UPDATE ${f.t("runs")} SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1`,
			[childOwner.runId]
		);
		const res = await f.runs.recoverStalled(childOwner.runId, 60_000, ERR);
		if (res.kind !== "failed") throw new Error(`expected failed, got ${res.kind}`);
		expectResumed(res.parent, parent.owner.runId, waitpointId);
	});

	it("waitpoint already timed_out -> child completion is a no-op for the parent", async () => {
		const { waitpointId, childOwner } = await waitingParentWithChild();
		await f.sql(`UPDATE ${f.t("waitpoints")} SET status = 'timed_out' WHERE id = $1`, [
			waitpointId,
		]);
		const completeRes = await f.runs.complete(childOwner, 1);
		if (completeRes.kind !== "completed") throw new Error("expected completed");
		const childRun = await f.runs.get(childOwner.runId);
		expect(completeRes).toEqual({
			kind: "completed",
			updatedAt: childRun?.updatedAt,
			parent: null,
			expireJobsToRemove: [],
		});
	});

	it("parent still running -> waitpoint completed but no enqueue", async () => {
		const parent = await f.running({ taskId: "parent" });
		const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1);
		const childOwner = await f.acquire(childIds[0] ?? "");
		const res = await f.runs.complete(childOwner, 1);
		if (res.kind !== "completed") throw new Error("expected completed");
		const childRun = await f.runs.get(childOwner.runId);
		expect(res).toEqual({
			kind: "completed",
			updatedAt: childRun?.updatedAt,
			parent: { waitpointId, hasExpireJob: false, resume: null },
			expireJobsToRemove: [],
		});
		expect(await f.runs.suspend(parent.owner)).toMatchObject({ kind: "requeued" });
	});

	it("batch of 20 finishing concurrently -> pending_count 0 and exactly one enqueue", async () => {
		const parent = await f.running({ taskId: "parent" });
		const { waitpointId, childIds } = await spawn(parent.owner, "batch", 20, 60_000);
		await f.runs.suspend(parent.owner);
		const owners = await Promise.all(childIds.map(id => f.acquire(id)));
		const results = await Promise.all(
			owners.map((o, i) => (i % 3 === 0 ? f.runs.fail(o, ERR) : f.runs.complete(o, i)))
		);
		const resolutions = results.map(r => (r.kind === "lost" ? null : r.parent));
		expect(resolutions.every(r => r !== null)).toBe(true);
		const resumes = resolutions.filter(r => r?.resume);
		expect(resumes).toHaveLength(1);
		expect(resumes[0]).toMatchObject({ hasExpireJob: true, resume: { seq: 3 } });
		expect(resolutions.filter(r => r?.hasExpireJob)).toHaveLength(1);
		expect(await wpRow(waitpointId)).toEqual({ status: "completed", pending_count: 0 });
		const results2 = await f.runs.listChildResults(waitpointId);
		expect(results2.map(r => r.batchIndex)).toEqual(Array.from({ length: 20 }, (_, i) => i));
	});

	it("expire on a child_run waitpoint cancels the child tree and resumes the parent", async () => {
		const parent = await f.running({ taskId: "parent" });
		const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1, 60_000);
		const childOwner = await f.acquire(childIds[0] ?? "");
		const grand = await f.runs.triggerChildren({
			parent: childOwner,
			stepName: "g",
			seqIndex: 0,
			mode: "trigger",
			waitpointId: randomUUID(),
			timeoutMs: null,
			children: [
				{
					id: randomUUID(),
					taskId: "grand",
					queue: "default",
					payload: null,
					maxAttempts: 1,
					idempotencyKey: `__child:${childOwner.runId}:g`,
					batchIndex: null,
				},
			],
		});
		if (grand.kind !== "created") throw new Error("lost");
		await f.runs.suspend(parent.owner);
		await f.sql(
			`UPDATE ${f.t("waitpoints")} SET expires_at = now() - interval '1 second' WHERE id = $1`,
			[waitpointId]
		);
		const res = await f.waitpoints.expire(waitpointId);
		if (res.kind !== "expired") throw new Error("expected expired");
		expect(res.canceledChildren.map(c => c.runId)).toEqual([
			childOwner.runId,
			grand.enqueue[0]?.runId,
		]);
		expect(res.resolution.resume).toMatchObject({ runId: parent.owner.runId, seq: 3 });
		expect(res.expireJobsToRemove).toEqual([]);
		expect(await f.runs.complete(childOwner, 1)).toEqual({ kind: "lost" });
		const [child] = await f.runs.listChildResults(waitpointId);
		expect(child?.status).toBe("canceled");
	});

	it("expire cancels children and times out their own pending waitpoints (reports expire jobs)", async () => {
		const parent = await f.running({ taskId: "parent" });
		const { waitpointId, childIds } = await spawn(parent.owner, "wait", 1, 60_000);
		const childOwner = await f.acquire(childIds[0] ?? "");
		const withExpiry = await token(childOwner, "t1", 60_000);
		const noExpiry = await token(childOwner, "t2", null);
		await f.runs.suspend(parent.owner);
		await f.sql(
			`UPDATE ${f.t("waitpoints")} SET expires_at = now() - interval '1 second' WHERE id = $1`,
			[waitpointId]
		);
		const res = await f.waitpoints.expire(waitpointId);
		if (res.kind !== "expired") throw new Error("expected expired");
		expect(res.canceledChildren.map(c => c.runId)).toEqual([childOwner.runId]);
		expect(res.expireJobsToRemove).toEqual([withExpiry.waitpointId]);
		expect((await wpRow(withExpiry.waitpointId))?.status).toBe("timed_out");
		expect((await wpRow(noExpiry.waitpointId))?.status).toBe("timed_out");
		// The canceled child is terminal: its tokens report run_finished.
		expect(await f.waitpoints.completeToken(noExpiry.waitpointId, "late")).toEqual({
			kind: "run_finished",
		});
	});
});
