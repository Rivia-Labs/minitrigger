/**
 * Audit item 1 (head-of-line blocking): legitimately queued runs whose jobs already exist in
 * BullMQ (backlog of a rate-limited queue, queue without a worker, ...) stay stale in Postgres,
 * and re-enqueueing them is a no-op. The reconciler must still reach a lost enqueue sitting
 * behind them: it pages with a `(run_after, id)` cursor within a tick (bounded) and across ticks
 * (wrapping around at the end), instead of re-reading the same first page every tick.
 */
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { afterEach, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createRedisConnections } from "../../src/queue/connections.js";
import { runJobId } from "../../src/queue/ids.js";
import { createTransport, type Transport } from "../../src/queue/transport.js";
import { createReconciler, type ReconcilerOptions } from "../../src/reconciler.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import { useTestNamespace } from "../helpers/namespace.js";
import { useRunFixtures } from "../helpers/run-fixtures.js";

const ns = useTestNamespace();
const f = useRunFixtures(ns);

const QUEUE = "default";

const silent: EngineLogger = {
	debug: () => {},
	info: () => {},
	warn: () => {},
	error: () => {},
};

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	await Promise.all(cleanups.splice(0).map(fn => fn().catch(() => {})));
});

function makeTransport(): Transport {
	const t = createTransport({
		prefix: ns.prefix,
		connections: createRedisConnections(ns.redisUrl),
		queues: [{ name: QUEUE, concurrency: 1 }],
	});
	cleanups.push(async () => {
		await t.closeWorkers({ timeoutMs: 500 });
	});
	return t;
}

function rawQueue(): Queue {
	const connection = new Redis(ns.redisUrl, { maxRetriesPerRequest: null });
	const queue = new Queue(QUEUE, { connection, prefix: ns.prefix });
	cleanups.push(async () => {
		await queue.close();
		connection.disconnect();
	});
	return queue;
}

function reconciler(transport: Transport, overrides: Partial<ReconcilerOptions> = {}) {
	return createReconciler({
		store: createSqlStore(ns.pool, ns.schema),
		transport,
		logger: silent,
		graceMs: 1_000,
		stallTimeoutMs: 60_000,
		batchSize: 50,
		...overrides,
	});
}

/**
 * `n` stale queued runs whose jobs DO exist (enqueued, no worker), then one stale queued run whose
 * enqueue was lost, with the most recent `run_after` (last in the reconciler's order).
 */
async function backlogWithLostEnqueue(transport: Transport, n: number) {
	const backlog: string[] = [];
	for (let i = 0; i < n; i++) {
		const created = await f.runs.create(f.newRun());
		if (created.kind !== "created") throw new Error("expected created");
		await transport.enqueueRun({ queue: QUEUE, runId: created.run.id, seq: created.enqueue.seq });
		backlog.push(created.run.id);
	}
	await f.sql(
		`UPDATE ${f.t("runs")} SET run_after = now() - interval '10 minutes' WHERE id = ANY($1::uuid[])`,
		[backlog]
	);
	const lost = await f.runs.create(f.newRun());
	if (lost.kind !== "created") throw new Error("expected created");
	await f.sql(`UPDATE ${f.t("runs")} SET run_after = now() - interval '1 minute' WHERE id = $1`, [
		lost.run.id,
	]);
	return { backlog, lostJobId: runJobId(lost.run.id, lost.enqueue.seq) };
}

describe("reconciler paging (head-of-line blocking)", () => {
	it("recovers a lost enqueue behind 150 stale queued runs whose jobs exist, in one tick", async () => {
		const transport = makeTransport();
		const { lostJobId } = await backlogWithLostEnqueue(transport, 150);
		const queue = rawQueue();
		expect(await queue.getJob(lostJobId)).toBeUndefined();

		const report = await reconciler(transport).runOnce();

		expect(report.errors).toBe(0);
		expect(report.staleQueued).toBe(151);
		expect(await queue.getJob(lostJobId)).toBeDefined();
	});

	it("continues across ticks with a cursor and wraps around after a short page", async () => {
		const transport = makeTransport();
		const { lostJobId } = await backlogWithLostEnqueue(transport, 25);
		const queue = rawQueue();
		const r = reconciler(transport, { batchSize: 10, maxPagesPerTick: 1 });

		expect((await r.runOnce()).staleQueued).toBe(10);
		expect((await r.runOnce()).staleQueued).toBe(10);
		expect(await queue.getJob(lostJobId)).toBeUndefined();
		expect((await r.runOnce()).staleQueued).toBe(6); // short page: 5 backlog + the lost one
		expect(await queue.getJob(lostJobId)).toBeDefined();
		// Wrapped around: starts from the oldest again.
		expect((await r.runOnce()).staleQueued).toBe(10);
	});

	it("pages stale waiting runs the same way", async () => {
		const transport = makeTransport();
		const ids: string[] = [];
		for (let i = 0; i < 5; i++) {
			const { owner } = await f.running();
			ids.push(owner.runId);
		}
		await f.sql(
			`UPDATE ${f.t("runs")} SET status = 'waiting', execution_id = NULL,
			   run_after = now() - interval '1 minute' WHERE id = ANY($1::uuid[])`,
			[ids]
		);
		const r = reconciler(transport, { batchSize: 2, maxPagesPerTick: 2 });
		expect((await r.runOnce()).staleWaiting).toBe(4);
		expect((await r.runOnce()).staleWaiting).toBe(1);
		expect((await r.runOnce()).staleWaiting).toBe(4);
	});
});

describe("reconciler during a rolling deploy (audit item 10)", () => {
	it("re-enqueues a stale run of a queue this process doesn't declare", async () => {
		const transport = makeTransport(); // declares only "default"
		const created = await f.runs.create(f.newRun({ queue: "added-by-new-deploy" }));
		if (created.kind !== "created") throw new Error("expected created");
		await f.sql(`UPDATE ${f.t("runs")} SET run_after = now() - interval '1 minute' WHERE id = $1`, [
			created.run.id,
		]);

		const report = await reconciler(transport).runOnce();

		expect(report).toMatchObject({ staleQueued: 1, errors: 0 });
		const connection = new Redis(ns.redisUrl, { maxRetriesPerRequest: null });
		const queue = new Queue("added-by-new-deploy", { connection, prefix: ns.prefix });
		try {
			expect(await queue.getJob(runJobId(created.run.id, created.enqueue.seq))).toBeDefined();
		} finally {
			await queue.close();
			connection.disconnect();
		}
	});
});
