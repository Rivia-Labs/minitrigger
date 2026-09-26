import { randomUUID } from "node:crypto";
import { escapeIdentifier } from "pg";
import { beforeAll, describe, expect, it } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlStore, type SqlStore } from "../../src/store/sql/index.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let store: SqlStore;
let s: string;
let counter = 0;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	store = createSqlStore(ns.pool, ns.schema);
	s = escapeIdentifier(ns.schema);
});

function uniqueQueue(tag: string): string {
	counter += 1;
	return `q${Date.now()}_${counter}${tag}`;
}

async function newRun(queue: string, opts: { delayMs?: number; taskId?: string } = {}) {
	const created = await store.runs.create({
		id: randomUUID(),
		taskId: opts.taskId ?? "t",
		queue,
		payload: null,
		maxAttempts: 1,
		delayMs: opts.delayMs ?? 0,
	});
	if (created.kind !== "created") throw new Error(created.kind);
	return created.run;
}

function first<T>(items: readonly T[]): T {
	const item = items[0];
	if (item === undefined) throw new Error("expected at least one item");
	return item;
}

describe("claimRuns", () => {
	it("claims due runs in run_after order and writes what acquire writes", async () => {
		const q = uniqueQueue("a");
		const a = await newRun(q);
		const b = await newRun(q);
		const later = await newRun(q, { delayMs: 60_000 });
		await ns.pool.query(`UPDATE ${s}.runs SET concurrency_waits = 3 WHERE id = $1`, [b.id]);
		const claimed = await store.claim.claimRuns({ queue: q, limit: 10 });
		expect(claimed.map(r => r.id)).toEqual([a.id, b.id]);
		for (const r of claimed) {
			expect(r.status).toBe("running");
			expect(r.executionId).toMatch(/^[0-9a-f-]{36}$/);
			expect(r.startedAt).toBeInstanceOf(Date);
			expect(r.heartbeatAt).toBeInstanceOf(Date);
		}
		// enqueue_seq is unchanged by the claim; concurrency_waits is not reset (decision 25).
		expect(first(claimed).enqueueSeq).toBe(a.enqueueSeq);
		const { rows } = await ns.pool.query<{ concurrency_waits: number }>(
			`SELECT concurrency_waits FROM ${s}.runs WHERE id = $1`,
			[b.id]
		);
		expect(rows[0]?.concurrency_waits).toBe(3);
		expect((await store.runs.get(later.id))?.status).toBe("queued");
		expect(await store.claim.claimRuns({ queue: q, limit: 10 })).toEqual([]);
	});

	it("clears debounce_key and returns an empty list for limit <= 0", async () => {
		const q = uniqueQueue("d");
		const run = await newRun(q);
		await ns.pool.query(`UPDATE ${s}.runs SET debounce_key = 'dk' WHERE id = $1`, [run.id]);
		expect(await store.claim.claimRuns({ queue: q, limit: 0 })).toEqual([]);
		const claimed = await store.claim.claimRuns({ queue: q, limit: 1 });
		expect(first(claimed).debounceKey).toBeNull();
	});

	it("does not set started_at for a run with a concurrency key (the re-check does)", async () => {
		const q = uniqueQueue("k");
		const run = await newRun(q);
		await ns.pool.query(`UPDATE ${s}.runs SET concurrency_key = 'k' WHERE id = $1`, [run.id]);
		const claimed = await store.claim.claimRuns({ queue: q, limit: 10 });
		expect(claimed.map(r => r.id)).toEqual([run.id]);
		expect(first(claimed).status).toBe("running");
		expect(first(claimed).startedAt).toBeNull();
	});

	it("keeps an existing started_at on a re-claim", async () => {
		const q = uniqueQueue("r");
		const run = await newRun(q);
		const startedAt = new Date("2020-01-01T00:00:00.000Z");
		await ns.pool.query(`UPDATE ${s}.runs SET started_at = $2 WHERE id = $1`, [run.id, startedAt]);
		const claimed = await store.claim.claimRuns({ queue: q, limit: 10 });
		expect(first(claimed).startedAt?.getTime()).toBe(startedAt.getTime());
	});

	it("respects limit, paused queues and globalConcurrency", async () => {
		const q = uniqueQueue("p");
		await newRun(q);
		await newRun(q);
		await newRun(q);
		expect((await store.claim.claimRuns({ queue: q, limit: 2 })).length).toBe(2);
		expect((await store.claim.claimRuns({ queue: q, limit: 5, globalConcurrency: 2 })).length).toBe(
			0
		);
		await store.queues.setPaused(q, true);
		expect((await store.claim.claimRuns({ queue: q, limit: 5 })).length).toBe(0);
		await store.queues.setPaused(q, false);
		expect((await store.claim.claimRuns({ queue: q, limit: 5 })).length).toBe(1);
	});

	it("does not claim runs of another queue", async () => {
		const q = uniqueQueue("x");
		const other = uniqueQueue("y");
		await newRun(other);
		expect(await store.claim.claimRuns({ queue: q, limit: 5 })).toEqual([]);
	});

	it("globalConcurrency is exact under concurrent claims", async () => {
		const q = uniqueQueue("g");
		for (let i = 0; i < 10; i++) await newRun(q);
		const results = await Promise.all(
			Array.from({ length: 4 }, () =>
				store.claim.claimRuns({ queue: q, limit: 5, globalConcurrency: 3 })
			)
		);
		expect(results.flat().length).toBe(3);
		expect(new Set(results.flat().map(r => r.id)).size).toBe(3);
	});

	it("claims a waiting run with run_after set, never one with run_after NULL", async () => {
		const q = uniqueQueue("w");
		const ready = await newRun(q);
		const blocked = await newRun(q);
		await ns.pool.query(
			`UPDATE ${s}.runs SET status = 'waiting', run_after = now() - interval '1 second' WHERE id = $1`,
			[ready.id]
		);
		await ns.pool.query(`UPDATE ${s}.runs SET status = 'waiting', run_after = NULL WHERE id = $1`, [
			blocked.id,
		]);
		const claimed = await store.claim.claimRuns({ queue: q, limit: 10 });
		expect(claimed.map(r => r.id)).toEqual([ready.id]);
	});

	it("skips outbox_pending rows and rows locked by another transaction", async () => {
		const q = uniqueQueue("o");
		const pending = await newRun(q);
		await ns.pool.query(`UPDATE ${s}.runs SET outbox_pending = true WHERE id = $1`, [pending.id]);
		expect(await store.claim.claimRuns({ queue: q, limit: 10 })).toEqual([]);
		const locked = await newRun(q);
		const client = await ns.pool.connect();
		try {
			await client.query("BEGIN");
			await client.query(`SELECT 1 FROM ${s}.runs WHERE id = $1 FOR UPDATE`, [locked.id]);
			expect(await store.claim.claimRuns({ queue: q, limit: 10 })).toEqual([]);
			await client.query("ROLLBACK");
		} finally {
			client.release();
		}
		expect((await store.claim.claimRuns({ queue: q, limit: 10 })).map(r => r.id)).toEqual([
			locked.id,
		]);
	});

	it("nextDueAt returns the earliest future run_after of the queue", async () => {
		const q = uniqueQueue("n");
		expect(await store.claim.nextDueAt(q)).toBeNull();
		await newRun(q, { delayMs: 30_000 });
		await newRun(q, { delayMs: 5_000 });
		await newRun(q); // already due: not "future"
		const next = await store.claim.nextDueAt(q);
		if (next === null) throw new Error("expected a next due time");
		expect(next.getTime() - Date.now()).toBeGreaterThan(3_000);
		expect(next.getTime() - Date.now()).toBeLessThan(6_000);
	});
});

describe("systemJobs", () => {
	async function take(id: string, lockMs = 5_000) {
		return (await store.systemJobs.claim(100, lockMs)).find(j => j.jobId === id);
	}

	it("upsert keeps the existing row; claim locks with a token; complete is fenced", async () => {
		const id = `job-${Date.now()}c`;
		await store.systemJobs.upsert({ jobId: id, name: "retention", data: { a: 1 } });
		await store.systemJobs.upsert({ jobId: id, name: "retention", data: { a: 2 } });
		const stored = await store.systemJobs.get(id);
		expect(stored?.data).toEqual({ a: 1 });
		expect(stored?.lockedBy).toBeNull();
		expect(stored?.maxAttempts).toBe(1);
		expect(stored?.attempts).toBe(0);
		const claimed = await take(id);
		if (claimed === undefined) throw new Error("not claimed");
		expect(claimed.lockedBy).toMatch(/^[0-9a-f-]{36}$/);
		expect(claimed.name).toBe("retention");
		expect(await take(id)).toBeUndefined();
		expect(await store.systemJobs.complete(id, randomUUID())).toBe(false);
		expect(await store.systemJobs.complete(id, claimed.lockedBy ?? "")).toBe(true);
		expect(await store.systemJobs.get(id)).toBeNull();
	});

	it("upsert with delayMs is not claimable before it is due; default data is {}", async () => {
		const id = `job-${Date.now()}d`;
		await store.systemJobs.upsert({ jobId: id, name: "waitpoint-expire", delayMs: 60_000 });
		expect(await take(id)).toBeUndefined();
		const stored = await store.systemJobs.get(id);
		expect(stored?.data).toEqual({});
		expect((stored?.runAfter.getTime() ?? 0) - Date.now()).toBeGreaterThan(50_000);
	});

	it("delay reschedules in place; fail retries then exhausts (and deletes)", async () => {
		const id = `job-${Date.now()}f`;
		await store.systemJobs.upsert({ jobId: id, name: "waitpoint-expire", maxAttempts: 2 });
		let job = await take(id);
		expect(await store.systemJobs.delay(id, job?.lockedBy ?? "", 60_000)).toBe(true);
		const delayed = await store.systemJobs.get(id);
		expect(delayed?.lockedBy).toBeNull();
		expect(await take(id)).toBeUndefined();
		await ns.pool.query(`UPDATE ${s}.system_jobs SET run_after = now() WHERE job_id = $1`, [id]);
		job = await take(id);
		expect(await store.systemJobs.delay(id, randomUUID(), 1)).toBe(false);
		expect(await store.systemJobs.fail(id, job?.lockedBy ?? "", 0)).toBe("retried");
		const retried = await store.systemJobs.get(id);
		expect(retried?.attempts).toBe(1);
		expect(retried?.lockedBy).toBeNull();
		job = await take(id);
		expect(await store.systemJobs.fail(id, randomUUID(), 0)).toBe("lost");
		expect(await store.systemJobs.fail(id, job?.lockedBy ?? "", 0)).toBe("exhausted");
		expect(await store.systemJobs.get(id)).toBeNull();
	});

	it("fail applies the backoff from the database clock", async () => {
		const id = `job-${Date.now()}b`;
		await store.systemJobs.upsert({ jobId: id, name: "reconcile", maxAttempts: 3 });
		const job = await take(id);
		expect(await store.systemJobs.fail(id, job?.lockedBy ?? "", 60_000)).toBe("retried");
		expect(await take(id)).toBeUndefined();
		const stored = await store.systemJobs.get(id);
		expect((stored?.runAfter.getTime() ?? 0) - Date.now()).toBeGreaterThan(50_000);
	});

	it("a lock that expired is claimable again, and the old holder's writes do not match", async () => {
		const id = `job-${Date.now()}l`;
		await store.systemJobs.upsert({ jobId: id, name: "reconcile" });
		const firstClaim = await take(id, 1);
		await new Promise(r => setTimeout(r, 20));
		const second = await take(id);
		if (firstClaim === undefined || second === undefined) throw new Error("not claimed");
		expect(second.lockedBy).not.toBe(firstClaim.lockedBy);
		expect(await store.systemJobs.complete(id, firstClaim.lockedBy ?? "")).toBe(false);
		expect(await store.systemJobs.fail(id, firstClaim.lockedBy ?? "", 0)).toBe("lost");
		expect(await store.systemJobs.get(id)).not.toBeNull();
	});

	it("upsertScheduleTick is a no-op for the same cron/timezone and rewrites on change", async () => {
		const jobId = `schedule-tick:s${Date.now()}`;
		const data = { scheduleId: "s", cron: "* * * * *", schema: ns.schema, prevMillis: 1 };
		const at1 = new Date(Date.now() + 60_000);
		expect(await store.systemJobs.upsertScheduleTick({ jobId, data, runAt: at1 })).toBe("created");
		const created = await store.systemJobs.get(jobId);
		expect(created?.name).toBe("schedule-tick");
		expect(created?.maxAttempts).toBe(5);
		expect(
			await store.systemJobs.upsertScheduleTick({
				jobId,
				data: { ...data, prevMillis: 2 },
				runAt: new Date(Date.now() + 5_000),
			})
		).toBe("unchanged");
		expect((await store.systemJobs.get(jobId))?.runAfter.getTime()).toBe(at1.getTime());
		expect(
			await store.systemJobs.upsertScheduleTick({
				jobId,
				data: { ...data, cron: "*/5 * * * *" },
				runAt: at1,
			})
		).toBe("updated");
		expect(
			await store.systemJobs.upsertScheduleTick({
				jobId,
				data: { ...data, cron: "*/5 * * * *", timezone: "UTC" },
				runAt: at1,
			})
		).toBe("updated");
		expect((await store.systemJobs.get(jobId))?.data).toMatchObject({ timezone: "UTC" });
	});

	it("a schedule change releases the lock of the tick being processed", async () => {
		const jobId = `schedule-tick:c${Date.now()}`;
		const data = { scheduleId: "c", cron: "* * * * *", schema: ns.schema, prevMillis: 1 };
		await store.systemJobs.upsertScheduleTick({ jobId, data, runAt: new Date(Date.now() - 1000) });
		const job = await take(jobId);
		if (job === undefined) throw new Error("not claimed");
		expect(
			await store.systemJobs.upsertScheduleTick({
				jobId,
				data: { ...data, cron: "*/2 * * * *" },
				runAt: new Date(Date.now() + 60_000),
			})
		).toBe("updated");
		expect((await store.systemJobs.get(jobId))?.lockedBy).toBeNull();
		expect(
			await store.systemJobs.rescheduleTick(jobId, job.lockedBy ?? "", {
				runAt: new Date(),
				prevMillis: 9,
			})
		).toBe(false);
	});

	it("rescheduleTick moves the row to the next occurrence, resets attempts and releases the lock", async () => {
		const jobId = `schedule-tick:r${Date.now()}`;
		const data = { scheduleId: "r", cron: "* * * * *", schema: ns.schema, prevMillis: 1 };
		await store.systemJobs.upsertScheduleTick({ jobId, data, runAt: await store.systemJobs.now() });
		let job = await take(jobId);
		expect(await store.systemJobs.fail(jobId, job?.lockedBy ?? "", 0)).toBe("retried");
		expect((await store.systemJobs.get(jobId))?.attempts).toBe(1);
		job = await take(jobId);
		if (job === undefined) throw new Error("not claimed");
		const next = new Date(Date.now() + 120_000);
		expect(
			await store.systemJobs.rescheduleTick(jobId, job.lockedBy ?? "", {
				runAt: next,
				prevMillis: 12345,
			})
		).toBe(true);
		const stored = await store.systemJobs.get(jobId);
		expect(stored?.runAfter.getTime()).toBe(next.getTime());
		expect(stored?.attempts).toBe(0);
		expect(stored?.lockedBy).toBeNull();
		expect(stored?.data).toEqual({ ...data, prevMillis: 12345 });
		expect(
			await store.systemJobs.rescheduleTick(jobId, job.lockedBy ?? "", {
				runAt: next,
				prevMillis: 1,
			})
		).toBe(false);
	});

	it("remove refuses a locked job unless evenIfLocked", async () => {
		const id = `job-${Date.now()}rm`;
		await store.systemJobs.upsert({ jobId: id, name: "waitpoint-expire" });
		expect(await take(id)).toBeDefined();
		expect(await store.systemJobs.remove(id)).toBe(false);
		expect(await store.systemJobs.get(id)).not.toBeNull();
		expect(await store.systemJobs.remove(id, { evenIfLocked: true })).toBe(true);
		expect(await store.systemJobs.get(id)).toBeNull();
		expect(await store.systemJobs.remove(id)).toBe(false);

		const idle = `job-${Date.now()}rm2`;
		await store.systemJobs.upsert({ jobId: idle, name: "waitpoint-expire", delayMs: 60_000 });
		expect(await store.systemJobs.remove(idle)).toBe(true);
	});

	it("listByName, nextDueAt and now", async () => {
		const name = `custom-${Date.now()}`;
		await store.systemJobs.upsert({ jobId: `${name}-2`, name, delayMs: 20_000 });
		await store.systemJobs.upsert({ jobId: `${name}-1`, name, delayMs: 10_000 });
		const listed = await store.systemJobs.listByName(name);
		expect(listed.map(j => j.jobId).sort()).toEqual([`${name}-1`, `${name}-2`]);
		const dbNow = await store.systemJobs.now();
		expect(Math.abs(dbNow.getTime() - Date.now())).toBeLessThan(5_000);
		const next = await store.systemJobs.nextDueAt();
		if (next === null) throw new Error("expected a next due time");
		expect(next.getTime()).toBeGreaterThan(dbNow.getTime());
		expect(next.getTime() - dbNow.getTime()).toBeLessThanOrEqual(10_500);
	});
});

describe("schemaMeta", () => {
	it("setIfAbsent keeps the first value", async () => {
		expect(await store.meta.get("transport")).toBeNull();
		expect(await store.meta.setIfAbsent("transport", "postgres")).toBe("postgres");
		expect(await store.meta.setIfAbsent("transport", "bullmq")).toBe("postgres");
		expect(await store.meta.get("transport")).toBe("postgres");
	});

	it("concurrent setIfAbsent with different values agree on one winner", async () => {
		for (let i = 0; i < 5; i++) {
			const key = `k2-${i}`;
			const results = await Promise.all([
				store.meta.setIfAbsent(key, "a"),
				store.meta.setIfAbsent(key, "b"),
			]);
			expect(results[0]).toBe(results[1]);
			expect(await store.meta.get(key)).toBe(results[0]);
		}
	});
});
