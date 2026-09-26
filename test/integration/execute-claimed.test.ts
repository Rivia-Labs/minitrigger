/**
 * The executor's `claimed` entry point (Postgres transport, Task 9): a run already moved to
 * `running` by `store.claim.claimRuns` is executed without `acquire`; a claim that lands after
 * `stop()` began is handed back; a keyed run over its concurrency limit goes back to `queued`
 * (no attempt spent, no `enqueue`, no `run:started`).
 */
import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { resolveConfig } from "../../src/config.js";
import { type ExecutorDeps, executeRun } from "../../src/executor/execute-run.js";
import { ActiveExecutions } from "../../src/executor/execution.js";
import type { RunNotifier, RunStatusInfo } from "../../src/executor/notify.js";
import { migrate } from "../../src/migrations/migrate.js";
import { createNotifyBatcher } from "../../src/pg/notify-batcher.js";
import { createPgTransport } from "../../src/pg/transport.js";
import { buildRegistry } from "../../src/registry.js";
import { createSqlStore, type SqlStore } from "../../src/store/sql/index.js";
import type { RunRecord } from "../../src/store/types.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const suffix = randomUUID().slice(0, 8);

const plainQueue = `claimed_${suffix}`;
const keyedQueue = `claimed_keyed_${suffix}`;

const twoSteps = defineTask({
	id: `two-steps-${suffix}`,
	queue: { name: plainQueue },
	run: async (payload: { n: number }, ctx) => {
		const a = await ctx.step.run("a", () => payload.n + 1);
		const b = await ctx.step.run("b", () => a * 10);
		return { b };
	},
});

const keyed = defineTask({
	id: `keyed-${suffix}`,
	queue: { name: keyedQueue, concurrencyKey: { limit: 1 } },
	run: async () => "done",
});

const tasks: AnyTask[] = [twoSteps, keyed];
let store: SqlStore;
let statuses: RunStatusInfo[];

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	store = createSqlStore(ns.pool, ns.schema);
});

const batchers: { flush(): Promise<void> }[] = [];
afterAll(async () => {
	for (const b of batchers) await b.flush();
});

function makeDeps(overrides: Partial<ExecutorDeps> = {}) {
	const resolved = resolveConfig({
		postgres: { pool: ns.pool },
		transport: "postgres",
		schema: ns.schema,
		tasks,
		logger: silent,
	});
	const registry = buildRegistry(tasks);
	const batcher = createNotifyBatcher({ db: ns.pool, logger: silent });
	batchers.push(batcher);
	const transport = createPgTransport({
		store,
		batcher,
		schema: ns.schema,
		queues: [...registry.queues.values()],
		systemConcurrency: 1,
		stallTimeoutMs: resolved.stallTimeoutMs,
		polling: { intervalMs: 1_000, jitterMs: 0 },
		wake: { subscribe: () => () => {} },
		logger: silent,
	});
	const enqueueRun = vi.spyOn(transport, "enqueueRun");
	statuses = [];
	const notify: RunNotifier = {
		status: info => statuses.push(info),
		progress() {},
		canceled() {},
	};
	const deps: ExecutorDeps = {
		store,
		transport,
		registry,
		config: resolved,
		active: new ActiveExecutions(),
		notify,
		...overrides,
	};
	return { deps, enqueueRun };
}

async function createRun(taskId: string, queue: string, payload: unknown, concurrencyKey?: string) {
	const created = await store.runs.create({
		id: randomUUID(),
		taskId,
		queue,
		payload: payload as never,
		maxAttempts: 3,
		delayMs: 0,
		...(concurrencyKey === undefined ? {} : { concurrencyKey }),
	});
	if (created.kind !== "created") throw new Error(created.kind);
	return created.run;
}

async function claimOne(queue: string, runId: string): Promise<RunRecord> {
	const claimed = await store.claim.claimRuns({ queue, limit: 10 });
	const run = claimed.find(r => r.id === runId);
	if (!run) throw new Error(`run ${runId} was not claimed`);
	return run;
}

describe("executeRun with a claimed run", () => {
	it("executes the claimed run to completion without acquiring it again", async () => {
		const { deps } = makeDeps();
		const acquire = vi.spyOn(store.runs, "acquire");
		const run = await createRun(twoSteps.id, plainQueue, { n: 2 });
		const claimed = await claimOne(plainQueue, run.id);
		await executeRun(deps, { runId: run.id, seq: claimed.enqueueSeq, queue: plainQueue, claimed });
		expect(acquire).not.toHaveBeenCalled();
		acquire.mockRestore();
		const done = await store.runs.get(run.id);
		expect(done?.status).toBe("completed");
		expect(done?.output).toEqual({ b: 30 });
		expect(done?.attempt).toBe(1);
		expect(statuses.map(s => s.transition)).toEqual(["started", "completed"]);
	});

	it("a claim that lands after stop() began is handed back to queued", async () => {
		const { deps } = makeDeps({ isShuttingDown: () => true });
		const run = await createRun(twoSteps.id, plainQueue, { n: 1 });
		const claimed = await claimOne(plainQueue, run.id);
		await executeRun(deps, { runId: run.id, seq: claimed.enqueueSeq, queue: plainQueue, claimed });
		const after = await store.runs.get(run.id);
		expect(after?.status).toBe("queued");
		expect(after?.executionId).toBeNull();
		expect(after?.attempt).toBe(1);
		expect(statuses.map(s => s.transition)).toEqual(["queued"]);
	});

	it("a keyed run over its limit goes back to queued: no attempt, no enqueue, no run:started", async () => {
		const { deps, enqueueRun } = makeDeps();
		const holder = await createRun(keyed.id, keyedQueue, null, "k");
		const holderClaim = await claimOne(keyedQueue, holder.id);
		const check = await store.runs.recheckConcurrencyLimit(
			holder.id,
			holderClaim.executionId ?? "",
			{ limit: 1 }
		);
		expect(check.kind).toBe("ok");

		const run = await createRun(keyed.id, keyedQueue, null, "k");
		const claimed = await claimOne(keyedQueue, run.id);
		expect(claimed.startedAt).toBeNull();
		await executeRun(deps, { runId: run.id, seq: claimed.enqueueSeq, queue: keyedQueue, claimed });
		const after = await store.runs.get(run.id);
		expect(after?.status).toBe("queued");
		expect(after?.executionId).toBeNull();
		expect(after?.attempt).toBe(1);
		expect(after?.startedAt).toBeNull();
		expect(enqueueRun).not.toHaveBeenCalled();
		expect(statuses).toEqual([]);
		const { rows } = await ns.pool.query<{ ms: number }>(
			`SELECT extract(epoch FROM (run_after - now())) * 1000 AS ms FROM "${ns.schema}".runs WHERE id = $1`,
			[run.id]
		);
		expect(Number(rows[0]?.ms)).toBeGreaterThan(0);
	});

	it("a keyed run under its limit runs, with started_at set by the re-check", async () => {
		const { deps } = makeDeps();
		const run = await createRun(keyed.id, keyedQueue, null, `solo-${suffix}`);
		const claimed = await claimOne(keyedQueue, run.id);
		await executeRun(deps, { runId: run.id, seq: claimed.enqueueSeq, queue: keyedQueue, claimed });
		const after = await store.runs.get(run.id);
		expect(after?.status).toBe("completed");
		expect(after?.startedAt).toBeInstanceOf(Date);
		expect(statuses.map(s => s.transition)).toEqual(["started", "completed"]);
	});
});
