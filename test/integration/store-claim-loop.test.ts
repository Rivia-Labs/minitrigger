/**
 * Review 5, fix round 1 (block A):
 * - Finding 1: a due row the claim can never take (`outbox_pending`, or row-locked by another
 *   transaction) must not make the claim loop poll every few ms. `nextDueInMs` used to count it
 *   (always 0 = "due now"), so the loop went claim -> nextDue -> claim every ~5 ms.
 * - Finding 2: the `-system` claim runs in a transaction (`boundedClaim`), so `now()` is frozen at
 *   its BEGIN: a job that came due between the BEGIN and the claim statement was skipped (and
 *   missed by the system `nextDueInMs`, `run_after > now()`), waiting a full poll interval.
 */
import { randomUUID } from "node:crypto";
import { Client, escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { migrate } from "../../src/migrations/migrate.js";
import { createQueueWorker } from "../../src/pg/workers.js";
import { createSqlStore, type SqlStore } from "../../src/store/sql/index.js";
import type { RunClaimStore } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let store: SqlStore;
let s: string;

const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const MEASURE_MS = 1_000;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	store = createSqlStore(ns.pool, ns.schema);
	s = escapeIdentifier(ns.schema);
});

afterEach(() => {
	vi.restoreAllMocks();
});

async function dueRun(queue: string): Promise<string> {
	const created = await store.runs.create({
		id: randomUUID(),
		taskId: "t",
		queue,
		payload: null,
		maxAttempts: 1,
		delayMs: 0,
	});
	if (created.kind !== "created") throw new Error(created.kind);
	// Due now (the Postgres transport claims by run_after).
	await ns.pool.query(`UPDATE ${s}.runs SET run_after = now() WHERE id = $1`, [created.run.id]);
	return created.run.id;
}

/** Claim passes (claimRuns calls) of a worker over MEASURE_MS, with intervalMs 1000. */
async function claimPasses(queue: string): Promise<{ claims: number; processed: number }> {
	let claims = 0;
	let processed = 0;
	const claim: RunClaimStore = {
		claimRuns: input => {
			claims++;
			return store.claim.claimRuns(input);
		},
		nextDueInMs: q => store.claim.nextDueInMs(q),
	};
	const worker = createQueueWorker({
		queue,
		concurrency: 5,
		claim,
		process: async () => {
			processed++;
		},
		polling: { intervalMs: 1_000, jitterMs: 0 },
		logger: silent,
	});
	worker.start();
	await new Promise(resolve => setTimeout(resolve, MEASURE_MS));
	worker.stopClaiming();
	await worker.drain(1_000);
	return { claims, processed };
}

describe("an unclaimable due row does not make the loop poll fast (finding 1)", () => {
	it("nextDueInMs ignores outbox_pending rows, like the claim", async () => {
		const q = `outbox_${randomUUID().slice(0, 8)}`;
		const id = await dueRun(q);
		await ns.pool.query(`UPDATE ${s}.runs SET outbox_pending = true WHERE id = $1`, [id]);
		expect(await store.claim.nextDueInMs(q)).toBeNull();
	});

	it("a due outbox_pending row: at most ~1 claim pass per intervalMs", async () => {
		const q = `outbox_${randomUUID().slice(0, 8)}`;
		const id = await dueRun(q);
		await ns.pool.query(`UPDATE ${s}.runs SET outbox_pending = true WHERE id = $1`, [id]);
		const { claims, processed } = await claimPasses(q);
		expect(processed).toBe(0);
		expect(claims).toBeLessThanOrEqual(2);
	});

	it("a due row locked by another transaction: at most ~1 re-claim cycle per intervalMs", async () => {
		const q = `locked_${randomUUID().slice(0, 8)}`;
		const id = await dueRun(q);
		const holder = await ns.pool.connect();
		try {
			await holder.query("BEGIN");
			await holder.query(`SELECT 1 FROM ${s}.runs WHERE id = $1 FOR UPDATE`, [id]);
			const { claims, processed } = await claimPasses(q);
			expect(processed).toBe(0);
			// One claim + one immediate re-claim per pass, then intervalMs.
			expect(claims).toBeLessThanOrEqual(4);
		} finally {
			await holder.query("ROLLBACK");
			holder.release();
		}
	});
});

describe("-system claim uses the statement time, not the frozen BEGIN time (finding 2)", () => {
	it("claims a job that came due between the BEGIN and the claim statement, locked from then", async () => {
		const jobId = `retention-${randomUUID()}`;
		const DUE_IN_MS = 300;
		const LOCK_MS = 10_000;
		await store.systemJobs.upsert({ jobId, name: "retention", delayMs: DUE_IN_MS });
		// Delay the claim statement past the job's run_after (after BEGIN and SET LOCAL ran).
		const original = Client.prototype.query;
		vi.spyOn(Client.prototype, "query").mockImplementation(function (
			this: Client,
			...args: unknown[]
		) {
			const text = args[0];
			const call = () => (original as (...a: unknown[]) => unknown).apply(this, args) as never;
			if (
				typeof text === "string" &&
				text.includes("WITH picked") &&
				text.includes("system_jobs")
			) {
				return new Promise(resolve => setTimeout(resolve, DUE_IN_MS + 200)).then(call) as never;
			}
			return call();
		});
		const claimed = await store.systemJobs.claim(10, LOCK_MS);
		vi.restoreAllMocks();
		expect(claimed.map(j => j.jobId)).toEqual([jobId]);
		// locked_until counts from the statement, not from the BEGIN ~500 ms earlier.
		const { rows } = await ns.pool.query<{ left: number }>(
			`SELECT extract(epoch from (locked_until - now())) * 1000 AS left
			 FROM ${s}.system_jobs WHERE job_id = $1`,
			[jobId]
		);
		expect(Number(rows[0]?.left)).toBeGreaterThan(LOCK_MS - 150);
	});
});
