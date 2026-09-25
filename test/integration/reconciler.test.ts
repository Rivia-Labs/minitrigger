import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { afterEach, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createRedisConnections } from "../../src/queue/connections.js";
import { runJobId, systemQueueName, waitpointExpireJobId } from "../../src/queue/ids.js";
import { createTransport, type Transport } from "../../src/queue/transport.js";
import { createReconciler } from "../../src/reconciler.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import { deleteKeysByPrefix, useTestNamespace } from "../helpers/namespace.js";
import { useRunFixtures } from "../helpers/run-fixtures.js";

const ns = useTestNamespace();
const f = useRunFixtures(ns);

const QUEUE = "default";

type LogRecord = { level: string; message: string; fields: Record<string, unknown> };

function makeLogger(): { logger: EngineLogger; logs: LogRecord[] } {
	const logs: LogRecord[] = [];
	const logger: EngineLogger = {
		debug: (fields, message) => logs.push({ level: "debug", message, fields }),
		info: (fields, message) => logs.push({ level: "info", message, fields }),
		warn: (fields, message) => logs.push({ level: "warn", message, fields }),
		error: (fields, message) => logs.push({ level: "error", message, fields }),
	};
	return { logger, logs };
}

function store() {
	return createSqlStore(ns.pool, ns.schema);
}

const transports: Transport[] = [];
function makeTransport(): Transport {
	const connections = createRedisConnections(ns.redisUrl);
	const t = createTransport({
		prefix: ns.prefix,
		connections,
		queues: [{ name: QUEUE, concurrency: 2 }],
	});
	transports.push(t);
	return t;
}

/** Default reconciler options for a test; individual tests override what they need to assert. */
function makeReconciler(
	transport: Transport,
	logger: EngineLogger,
	overrides: { graceMs?: number } = {}
) {
	return createReconciler({
		store: store(),
		transport,
		logger,
		graceMs: overrides.graceMs ?? 1_000,
		stallTimeoutMs: 60_000,
		batchSize: 50,
	});
}

/** The `${name}#wait` step `waitForToken` writes: only an awaited token blocks a resume. */
async function awaitToken(runId: string, name: string, waitpointId: string, seqIndex = 1) {
	await f.sql(
		`INSERT INTO ${f.t("steps")} (run_id, name, seq_index, kind, status, output)
		 VALUES ($1, $2, $3, 'wait', 'waiting', $4::jsonb)`,
		[runId, `${name}#wait`, seqIndex, JSON.stringify({ waitpointId })]
	);
}

const rawCleanups: Array<() => Promise<void>> = [];
/** A raw BullMQ queue handle (its own connection) for inspecting jobs directly via `getJob`. */
function rawQueue(name: string): Queue {
	const connection = new Redis(ns.redisUrl, { maxRetriesPerRequest: null });
	const queue = new Queue(name, { connection, prefix: ns.prefix });
	rawCleanups.push(async () => {
		await queue.close().catch(() => {});
		connection.disconnect();
	});
	return queue;
}

afterEach(async () => {
	await Promise.all(
		transports.splice(0).map(t => t.closeWorkers({ timeoutMs: 500 }).catch(() => {}))
	);
	await Promise.all(rawCleanups.splice(0).map(fn => fn()));
});

describe("createReconciler: case 1 (stale queued)", () => {
	it("re-enqueues a stale queued run with the current enqueue_seq", async () => {
		const created = await f.runs.create(f.newRun());
		if (created.kind !== "created") throw new Error("expected created");
		await f.sql(`UPDATE ${f.t("runs")} SET run_after = now() - interval '1 minute' WHERE id = $1`, [
			created.run.id,
		]);

		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();

		expect(report.staleQueued).toBe(1);
		expect(report.errors).toBe(0);
		const job = await rawQueue(QUEUE).getJob(runJobId(created.run.id, created.enqueue.seq));
		expect(job).toBeDefined();
	});

	it("leaves a fresh queued run (run_after within grace) alone", async () => {
		const created = await f.runs.create(f.newRun());
		if (created.kind !== "created") throw new Error("expected created");

		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger, { graceMs: 60_000 }).runOnce();

		expect(report.staleQueued).toBe(0);
	});
});

describe("createReconciler: case 2 (stale waiting)", () => {
	it("re-enqueues a waiting run with no pending waitpoint, using the SAME seq", async () => {
		const { run, owner } = await f.running();
		await f.sql(
			`UPDATE ${f.t("runs")} SET status = 'waiting', execution_id = NULL,
			   run_after = now() - interval '1 minute' WHERE id = $1`,
			[owner.runId]
		);

		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();

		expect(report.staleWaiting).toBe(1);
		const current = await store().runs.get(owner.runId);
		expect(current?.enqueueSeq).toBe(run.enqueueSeq);
		const job = await rawQueue(QUEUE).getJob(runJobId(owner.runId, run.enqueueSeq));
		expect(job).toBeDefined();
	});

	it("ignores a waiting run that still has a pending (awaited) waitpoint", async () => {
		const { owner } = await f.running();
		const waitpointId = randomUUID();
		await store().waitpoints.createToken(owner, {
			waitpointId,
			name: "tok",
			seqIndex: 0,
			timeoutMs: null,
		});
		await awaitToken(owner.runId, "tok", waitpointId);
		await f.sql(
			`UPDATE ${f.t("runs")} SET status = 'waiting', execution_id = NULL,
			   run_after = now() - interval '1 minute' WHERE id = $1`,
			[owner.runId]
		);

		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();

		expect(report.staleWaiting).toBe(0);
	});

	it("a pending token that was never awaited does not keep a stale waiting run asleep", async () => {
		const { owner } = await f.running();
		await store().waitpoints.createToken(owner, {
			waitpointId: randomUUID(),
			name: "tok",
			seqIndex: 0,
			timeoutMs: null,
		});
		await f.sql(
			`UPDATE ${f.t("runs")} SET status = 'waiting', execution_id = NULL,
			   run_after = now() - interval '1 minute' WHERE id = $1`,
			[owner.runId]
		);

		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();

		expect(report.staleWaiting).toBe(1);
	});
});

describe("createReconciler: case 3 (orphan waiting)", () => {
	it("repairs a waiting run with run_after NULL and no pending waitpoint, enqueues it, and warns with its runId", async () => {
		const { owner } = await f.running();
		await f.sql(
			`UPDATE ${f.t("runs")} SET status = 'waiting', execution_id = NULL, run_after = NULL
			 WHERE id = $1`,
			[owner.runId]
		);

		const transport = makeTransport();
		const { logger, logs } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();

		expect(report.orphanWaiting).toBe(1);
		const warnLog = logs.find(l => l.level === "warn" && l.fields["runId"] === owner.runId);
		expect(warnLog).toBeDefined();

		const current = await store().runs.get(owner.runId);
		expect(current?.runAfter).toBeInstanceOf(Date);
		const job = await rawQueue(QUEUE).getJob(runJobId(owner.runId, current?.enqueueSeq ?? -1));
		expect(job).toBeDefined();
	});
});

describe("createReconciler: case 4 (stalled)", () => {
	it("recovers a stalled run with attempts left: requeues and re-enqueues with the new seq", async () => {
		const { owner } = await f.running({ maxAttempts: 3 });
		await f.sql(
			`UPDATE ${f.t("runs")} SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1`,
			[owner.runId]
		);

		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();

		expect(report.stalled).toEqual({ requeued: 1, failed: 0 });
		const current = await store().runs.get(owner.runId);
		expect(current).toMatchObject({ status: "queued", attempt: 2, executionId: null });
		const job = await rawQueue(QUEUE).getJob(runJobId(owner.runId, current?.enqueueSeq ?? -1));
		expect(job).toBeDefined();
	});

	it("fails a stalled run with attempts exhausted and resolves its parent's waitpoint", async () => {
		const parent = await f.running({ taskId: "parent" });
		const waitpointId = randomUUID();
		const childId = randomUUID();
		const created = await f.runs.triggerChildren({
			parent: parent.owner,
			stepName: "child",
			seqIndex: 0,
			mode: "wait",
			waitpointId,
			timeoutMs: null,
			children: [
				{
					id: childId,
					taskId: "child-task",
					queue: QUEUE,
					payload: null,
					maxAttempts: 1,
					idempotencyKey: `__child:${parent.owner.runId}:child`,
					batchIndex: null,
				},
			],
		});
		if (created.kind !== "created") throw new Error("expected created");
		expect(await store().runs.suspend(parent.owner)).toMatchObject({
			kind: "waiting_on_waitpoints",
		});

		const childOwner = await f.acquire(childId);
		await f.sql(
			`UPDATE ${f.t("runs")} SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1`,
			[childOwner.runId]
		);

		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();

		expect(report.stalled).toEqual({ requeued: 0, failed: 1 });
		const child = await store().runs.get(childId);
		expect(child?.status).toBe("failed");
		const parentRun = await store().runs.get(parent.owner.runId);
		// Resume rule (decision 3): status stays "waiting"; only run_after/enqueue_seq change.
		expect(parentRun?.status).toBe("waiting");
		const job = await rawQueue(QUEUE).getJob(
			runJobId(parent.owner.runId, parentRun?.enqueueSeq ?? -1)
		);
		expect(job).toBeDefined();
	});

	it("leaves a healthy running run alone", async () => {
		const { owner } = await f.running();
		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();
		expect(report.stalled).toEqual({ requeued: 0, failed: 0 });
		expect((await store().runs.get(owner.runId))?.status).toBe("running");
	});
});

describe("createReconciler: case 5 (expired waitpoints)", () => {
	it("expires a due waitpoint, resumes the waiting run, and removes expire jobs best-effort", async () => {
		const { owner } = await f.running();
		const waitpointId = randomUUID();
		const created = await store().waitpoints.createToken(owner, {
			waitpointId,
			name: "tok",
			seqIndex: 0,
			timeoutMs: 60_000,
		});
		if (created.kind !== "created") throw new Error("expected created");
		await awaitToken(owner.runId, "tok", waitpointId);
		expect(await store().runs.suspend(owner)).toMatchObject({ kind: "waiting_on_waitpoints" });
		await f.sql(
			`UPDATE ${f.t("waitpoints")} SET expires_at = now() - interval '1 second' WHERE id = $1`,
			[waitpointId]
		);

		const transport = makeTransport();
		// As the engine would have scheduled it (decision 10); the reconciler must clean it up.
		await transport.enqueueSystem(
			"waitpoint-expire",
			{ waitpointId },
			{ jobId: waitpointExpireJobId(waitpointId), delayMs: 3_600_000 }
		);

		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();

		expect(report.expiredWaitpoints).toBe(1);
		const wp = await store().waitpoints.getToken(waitpointId);
		expect(wp?.status).toBe("timed_out");
		const current = await store().runs.get(owner.runId);
		// Resume rule (decision 3): status stays "waiting"; only run_after/enqueue_seq change.
		expect(current?.status).toBe("waiting");

		const job = await rawQueue(QUEUE).getJob(runJobId(owner.runId, current?.enqueueSeq ?? -1));
		expect(job).toBeDefined();

		const removedJob = await rawQueue(systemQueueName(ns.prefix)).getJob(
			waitpointExpireJobId(waitpointId)
		);
		expect(removedJob).toBeUndefined();
	});

	it("expiring a child_run waitpoint removes the canceled children's run jobs and calls onCanceled", async () => {
		const parent = await f.running({ taskId: "parent" });
		const waitpointId = randomUUID();
		const childId = randomUUID();
		const created = await f.runs.triggerChildren({
			parent: parent.owner,
			stepName: "child",
			seqIndex: 0,
			mode: "wait",
			waitpointId,
			timeoutMs: 60_000,
			children: [
				{
					id: childId,
					taskId: "child-task",
					queue: QUEUE,
					payload: null,
					maxAttempts: 1,
					idempotencyKey: `__child:${parent.owner.runId}:child`,
					batchIndex: null,
				},
			],
		});
		if (created.kind !== "created") throw new Error("expected created");
		const childJob = created.enqueue[0];
		if (!childJob) throw new Error("expected a child enqueue");
		const transport = makeTransport();
		// Delayed so no worker could pick it up; the reconciler must remove it.
		await transport.enqueueRun({ ...childJob, delayMs: 3_600_000 });
		expect(await rawQueue(QUEUE).getJob(runJobId(childId, childJob.seq))).toBeDefined();
		expect(await store().runs.suspend(parent.owner)).toMatchObject({
			kind: "waiting_on_waitpoints",
		});
		await f.sql(
			`UPDATE ${f.t("waitpoints")} SET expires_at = now() - interval '1 second' WHERE id = $1`,
			[waitpointId]
		);

		const canceled: string[][] = [];
		const { logger } = makeLogger();
		const report = await createReconciler({
			store: store(),
			transport,
			logger,
			graceMs: 1_000,
			stallTimeoutMs: 60_000,
			batchSize: 50,
			onCanceled: runIds => {
				canceled.push([...runIds]);
			},
		}).runOnce();

		expect(report.expiredWaitpoints).toBe(1);
		expect(report.errors).toBe(0);
		expect((await store().runs.get(childId))?.status).toBe("canceled");
		expect(await rawQueue(QUEUE).getJob(runJobId(childId, childJob.seq))).toBeUndefined();
		expect(canceled).toEqual([[childId]]);
	});

	it("leaves a not-yet-due waitpoint alone", async () => {
		const { owner } = await f.running();
		await store().waitpoints.createToken(owner, {
			waitpointId: randomUUID(),
			name: "tok",
			seqIndex: 0,
			timeoutMs: 60_000,
		});
		await store().runs.suspend(owner);

		const transport = makeTransport();
		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger).runOnce();
		expect(report.expiredWaitpoints).toBe(0);
	});
});

describe("createReconciler: case isolation", () => {
	it("a failing case is logged and does not stop the other cases from running", async () => {
		const created = await f.runs.create(f.newRun());
		if (created.kind !== "created") throw new Error("expected created");
		await f.sql(`UPDATE ${f.t("runs")} SET run_after = now() - interval '1 minute' WHERE id = $1`, [
			created.run.id,
		]);

		const transport = makeTransport();
		const brokenListStaleWaiting = () =>
			Promise.reject(new Error("boom: staleWaiting listing broke"));
		const s = store();
		s.runs.listStaleWaiting = brokenListStaleWaiting;
		const { logger, logs } = makeLogger();

		const reconciler = createReconciler({
			store: s,
			transport,
			logger,
			graceMs: 1_000,
			stallTimeoutMs: 60_000,
			batchSize: 50,
		});
		const report = await reconciler.runOnce();

		expect(report.staleQueued).toBe(1); // other cases still ran
		expect(report.errors).toBeGreaterThanOrEqual(1);
		expect(logs.some(l => l.level === "error" && l.fields["case"] === "staleWaiting")).toBe(true);
	});
});

describe("createReconciler: reconcileSchedules hook", () => {
	it("calls the optional hook once at the end, isolated like every other case", async () => {
		const transport = makeTransport();
		const { logger } = makeLogger();
		let calls = 0;
		const reconciler = createReconciler({
			store: store(),
			transport,
			logger,
			graceMs: 1_000,
			stallTimeoutMs: 60_000,
			batchSize: 50,
			reconcileSchedules: async () => {
				calls++;
			},
		});
		await reconciler.runOnce();
		expect(calls).toBe(1);
	});
});

describe("Redis flushed: reconciler recreates lost jobs from Postgres", () => {
	it("recreates jobs for queued, waiting(sleep due), and stalled running runs after Redis loses its keys", async () => {
		const transport = makeTransport();

		// queued, actually enqueued once (as trigger() would).
		const queuedCreated = await f.runs.create(f.newRun());
		if (queuedCreated.kind !== "created") throw new Error("expected created");
		await transport.enqueueRun({
			queue: QUEUE,
			runId: queuedCreated.run.id,
			seq: queuedCreated.enqueue.seq,
		});
		await f.sql(
			`UPDATE ${f.t("runs")} SET run_after = now() - interval '5 seconds' WHERE id = $1`,
			[queuedCreated.run.id]
		);

		// waiting, with a sleep that is now due; the delayed job was enqueued once too.
		const { owner: sleepOwner } = await f.running();
		await f.sql(
			`INSERT INTO ${f.t("steps")} (run_id, name, seq_index, kind, status, wake_at)
			 VALUES ($1, 'nap', 0, 'sleep', 'waiting', now() + interval '1 hour')`,
			[sleepOwner.runId]
		);
		const suspendRes = await store().runs.suspend(sleepOwner);
		if (suspendRes.kind !== "sleeping")
			throw new Error(`expected sleeping, got ${suspendRes.kind}`);
		await transport.enqueueRun({
			queue: QUEUE,
			runId: sleepOwner.runId,
			seq: suspendRes.enqueue.seq,
			delayMs: suspendRes.enqueue.delayMs,
		});
		// Time passes: the sleep becomes due and run_after moves into the past.
		await f.sql(
			`UPDATE ${f.t("steps")} SET wake_at = now() - interval '5 seconds' WHERE run_id = $1`,
			[sleepOwner.runId]
		);
		await f.sql(
			`UPDATE ${f.t("runs")} SET run_after = now() - interval '5 seconds' WHERE id = $1`,
			[sleepOwner.runId]
		);

		// running, heartbeat gone stale (its worker died mid-flight, taking its lock with it).
		const { owner: staleOwner } = await f.running({ maxAttempts: 3 });
		await f.sql(
			`UPDATE ${f.t("runs")} SET heartbeat_at = now() - interval '5 minutes' WHERE id = $1`,
			[staleOwner.runId]
		);

		// Redis loses every key under this test's prefix (never a real FLUSHALL: that guarded test
		// belongs to engine-level work).
		await deleteKeysByPrefix(ns.redis, ns.prefix);
		expect(
			await rawQueue(QUEUE).getJob(runJobId(queuedCreated.run.id, queuedCreated.enqueue.seq))
		).toBeUndefined();

		const { logger } = makeLogger();
		const report = await makeReconciler(transport, logger, { graceMs: 0 }).runOnce();

		expect(report.staleQueued).toBe(1);
		expect(report.staleWaiting).toBe(1);
		expect(report.stalled).toEqual({ requeued: 1, failed: 0 });

		const q = rawQueue(QUEUE);
		expect(await q.getJob(runJobId(queuedCreated.run.id, queuedCreated.enqueue.seq))).toBeDefined();
		expect(await q.getJob(runJobId(sleepOwner.runId, suspendRes.enqueue.seq))).toBeDefined();

		const staleRun = await store().runs.get(staleOwner.runId);
		expect(staleRun).toMatchObject({ status: "queued", attempt: 2 });
		expect(await q.getJob(runJobId(staleOwner.runId, staleRun?.enqueueSeq ?? -1))).toBeDefined();
	});
});
