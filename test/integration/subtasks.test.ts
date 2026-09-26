import { randomUUID } from "node:crypto";
import { escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { AbortTaskRunError } from "../../src/errors.js";
import { createRetention } from "../../src/retention.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const shortId = () => randomUUID().slice(0, 8);
const uniqueQueue = () => `q${shortId()}`;
const uniqueTaskId = (label: string) => `${label}-${shortId()}`;

const noop = () => {};
const testLogger: EngineLogger = { debug: noop, info: noop, warn: noop, error: noop };

const engines: Engine[] = [];

async function startedEngine(tasks: AnyTask[], overrides: Partial<EngineConfig> = {}) {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 10 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 5_000,
		shutdownTimeoutMs: 2_000,
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	await engine.start();
	return engine;
}

const t = (table: string) => `${escapeIdentifier(ns.schema)}.${table}`;

async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 15_000) {
	await waitFor(async () => (await engine.runs.get(id))?.status === status, {
		timeoutMs,
		intervalMs: 20,
	});
	const run = await engine.runs.get(id);
	if (!run) throw new Error("unreachable");
	return run;
}

async function childrenOf(parentId: string) {
	const { rows } = await ns.pool.query<{
		id: string;
		status: string;
		batch_index: number | null;
	}>(
		`SELECT id, status, batch_index FROM ${t("runs")} WHERE parent_run_id = $1
		 ORDER BY batch_index NULLS FIRST, created_at`,
		[parentId]
	);
	return rows;
}

async function keyOf(runId: string) {
	const { rows } = await ns.pool.query<{ key: string; expires_at: Date | null }>(
		`SELECT key, expires_at FROM ${t("idempotency_keys")} WHERE run_id = $1`,
		[runId]
	);
	return rows[0];
}

async function stepsOf(runId: string) {
	const { rows } = await ns.pool.query<{ name: string; kind: string; output: unknown }>(
		`SELECT name, kind, output FROM ${t("steps")} WHERE run_id = $1 ORDER BY seq_index`,
		[runId]
	);
	return rows;
}

/** Polls until `fn` returns an id. */
async function waitForId(fn: () => Promise<string | undefined>): Promise<string> {
	return (await waitFor(fn, { timeoutMs: 10_000, intervalMs: 20 })) as string;
}

/** A child that parks itself (waiting) until canceled. */
function parkedTask(label: string, queue: string) {
	return defineTask({
		id: uniqueTaskId(label),
		queue: { name: queue },
		run: async (_p: { n: number }, ctx) => {
			await ctx.step.sleep("park", "1h");
			return "never";
		},
	});
}

beforeAll(async () => {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks: [],
	});
	await engine.migrate();
	await engine.stop();
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

describe("triggerAndWait", () => {
	it("subtask success: the parent gets { ok: true, output }", async () => {
		const queue = uniqueQueue();
		let parentExecutions = 0;
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: queue },
			run: async (p: { n: number }) => ({ doubled: p.n * 2 }),
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) => {
				parentExecutions++;
				return ctx.step.triggerAndWait("double", child, { n: 21 });
			},
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ ok: true, output: { doubled: 42 } });
		expect(parentExecutions).toBe(2);

		const [c] = await childrenOf(handle.id);
		expect(c?.status).toBe("completed");
		expect(await keyOf(c?.id ?? "")).toEqual({
			key: `__child:${handle.id}:double`,
			expires_at: null,
		});
		// Decision 10: the step holds the waitpoint reference only.
		const steps = await stepsOf(handle.id);
		expect(steps).toHaveLength(1);
		expect(steps[0]).toMatchObject({ name: "double", kind: "trigger_wait" });
		expect(Object.keys(steps[0]?.output as object)).toEqual(["waitpointId"]);
	});

	it("subtask failing: the parent gets { ok: false, error } without throwing", async () => {
		const queue = uniqueQueue();
		const child = defineTask({
			id: uniqueTaskId("child-fail"),
			queue: { name: queue },
			run: async (_p: { n: number }): Promise<number> => {
				throw new AbortTaskRunError("child broke");
			},
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) => {
				const result = await ctx.step.triggerAndWait("c", child, { n: 1 });
				return result.ok ? "unexpected" : result.error;
			},
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toMatchObject({
			name: "AbortTaskRunError",
			message: "child broke",
			retryable: false,
		});
	});

	it("subtask timeout: the parent gets Timeout and the child is canceled", async () => {
		const queue = uniqueQueue();
		const child = parkedTask("child-slow", queue);
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) =>
				ctx.step.triggerAndWait("slow", child, { n: 1 }, { timeout: "500ms" }),
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toMatchObject({ ok: false, error: { name: "Timeout" } });
		const [c] = await childrenOf(handle.id);
		expect(c?.status).toBe("canceled");
	});

	it("runs.cancel(childId) of a triggerAndWait child resumes the parent with Canceled", async () => {
		const queue = uniqueQueue();
		const child = parkedTask("child-cancel", queue);
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) => ctx.step.triggerAndWait("c", child, { n: 1 }),
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		await waitForStatus(engine, handle.id, "waiting");
		const [c] = await childrenOf(handle.id);
		await waitForStatus(engine, c?.id ?? "", "waiting");
		expect(await engine.runs.cancel(c?.id ?? "")).toBe("canceled");
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toMatchObject({ ok: false, error: { name: "Canceled" } });
	});

	it("recursive cancel across three levels", async () => {
		const queue = uniqueQueue();
		const leaf = parkedTask("leaf", queue);
		const middle = defineTask({
			id: uniqueTaskId("middle"),
			queue: { name: queue },
			run: async (_p: { n: number }, ctx) => ctx.step.triggerAndWait("leaf", leaf, { n: 1 }),
		});
		const root = defineTask({
			id: uniqueTaskId("root"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) => ctx.step.triggerAndWait("middle", middle, { n: 1 }),
		});
		const engine = await startedEngine([leaf, middle, root]);
		const handle = await engine.trigger(root, { v: 1 });
		const midId = await waitForId(async () => (await childrenOf(handle.id))[0]?.id);
		const leafId = await waitForId(async () => (await childrenOf(midId))[0]?.id);
		await waitForStatus(engine, leafId, "waiting");
		await waitForStatus(engine, midId, "waiting");
		await waitForStatus(engine, handle.id, "waiting");

		expect(await engine.runs.cancel(handle.id)).toBe("canceled");
		for (const id of [handle.id, midId, leafId]) {
			expect((await engine.runs.get(id))?.status).toBe("canceled");
		}
		const { rows } = await ns.pool.query<{ status: string }>(
			`SELECT status FROM ${t("waitpoints")} WHERE run_id = ANY($1::uuid[])`,
			[[handle.id, midId]]
		);
		expect(rows.map(r => r.status)).toEqual(["timed_out", "timed_out"]);
		await sleep(200);
		expect((await engine.runs.get(handle.id))?.status).toBe("canceled");
	});
});

describe("batchTriggerAndWait", () => {
	it("20 children resume the parent exactly once, results in payload order", async () => {
		const queue = uniqueQueue();
		let parentExecutions = 0;
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: queue, concurrency: 10 },
			run: async (p: { n: number }) => {
				await sleep(Math.floor(Math.random() * 30));
				return p.n * 10;
			},
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) => {
				parentExecutions++;
				const payloads = Array.from({ length: 20 }, (_, n) => ({ n }));
				const results = await ctx.step.batchTriggerAndWait("fan-out", child, payloads);
				return results.map(r => (r.ok ? r.output : -1));
			},
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual(Array.from({ length: 20 }, (_, n) => n * 10));
		await sleep(300);
		expect(parentExecutions).toBe(2);

		const children = await childrenOf(handle.id);
		expect(children.map(c => c.batch_index)).toEqual(Array.from({ length: 20 }, (_, i) => i));
		expect(await keyOf(children[7]?.id ?? "")).toEqual({
			key: `__child:${handle.id}:fan-out:7`,
			expires_at: null,
		});
	});

	it("children with 1 MB outputs each do not hit maxStepOutputBytes", async () => {
		const queue = uniqueQueue();
		const child = defineTask({
			id: uniqueTaskId("child-big"),
			queue: { name: queue },
			run: async (p: { n: number }) => String(p.n).repeat(1_000_000),
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) => {
				const results = await ctx.step.batchTriggerAndWait("big", child, [
					{ n: 1 },
					{ n: 2 },
					{ n: 3 },
				]);
				return results.map(r => (r.ok ? `${r.output[0]}:${r.output.length}` : r.error.name));
			},
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual(["1:1000000", "2:1000000", "3:1000000"]);
		const steps = await stepsOf(handle.id);
		expect(steps[0]).toMatchObject({ name: "big", kind: "batch_wait" });
		expect(Object.keys(steps[0]?.output as object)).toEqual(["waitpointId"]);
	});

	it("runs.cancel(childId) of a batch child: that item is Canceled, the others complete", async () => {
		const queue = uniqueQueue();
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: queue },
			run: async (p: { n: number }, ctx) => {
				if (p.n === 1) await ctx.step.sleep("park", "1h");
				return p.n;
			},
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) =>
				ctx.step.batchTriggerAndWait("b", child, [{ n: 0 }, { n: 1 }, { n: 2 }]),
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const parked = await waitForId(async () => {
			const c = (await childrenOf(handle.id))[1];
			return c?.status === "waiting" ? c.id : undefined;
		});
		expect(await engine.runs.cancel(parked)).toBe("canceled");
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual([
			{ ok: true, output: 0 },
			{ ok: false, error: expect.objectContaining({ name: "Canceled" }) },
			{ ok: true, output: 2 },
		]);
	});

	it("batch timeout: unfinished children are canceled and reported as Timeout", async () => {
		const queue = uniqueQueue();
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: queue },
			run: async (p: { n: number }, ctx) => {
				if (p.n === 1) await ctx.step.sleep("park", "1h");
				return p.n;
			},
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) =>
				ctx.step.batchTriggerAndWait("b", child, [{ n: 0 }, { n: 1 }], { timeout: "700ms" }),
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual([
			{ ok: true, output: 0 },
			{ ok: false, error: expect.objectContaining({ name: "Timeout" }) },
		]);
		expect((await childrenOf(handle.id)).map(c => c.status)).toEqual(["completed", "canceled"]);
	});
});

describe("batch timeout keeps the real outcome of children that ended before it", () => {
	it("failed fast -> its error; canceled directly -> Canceled; running at the timeout -> Timeout", async () => {
		const queue = uniqueQueue();
		const child = defineTask({
			id: uniqueTaskId("child-mixed"),
			queue: { name: queue, concurrency: 5 },
			run: async (p: { n: number }, ctx) => {
				if (p.n === 0) throw new AbortTaskRunError("fast failure");
				if (p.n === 1) await ctx.step.sleep("park", "1h");
				// n === 2: still running when the batch times out (stops on abort).
				await new Promise<void>(resolve => {
					ctx.signal.addEventListener("abort", () => resolve(), { once: true });
				});
				return p.n;
			},
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) =>
				ctx.step.batchTriggerAndWait("b", child, [{ n: 0 }, { n: 1 }, { n: 2 }], {
					timeout: "3s",
				}),
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const parked = await waitForId(async () => {
			const c = (await childrenOf(handle.id))[1];
			return c?.status === "waiting" ? c.id : undefined;
		});
		const runningChild = await waitForId(async () => {
			const c = (await childrenOf(handle.id))[2];
			return c?.status === "running" ? c.id : undefined;
		});
		await waitForId(async () => {
			const c = (await childrenOf(handle.id))[0];
			return c?.status === "failed" ? c.id : undefined;
		});
		expect(await engine.runs.cancel(parked)).toBe("canceled");
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual([
			{
				ok: false,
				error: expect.objectContaining({ name: "AbortTaskRunError", message: "fast failure" }),
			},
			{ ok: false, error: expect.objectContaining({ name: "Canceled" }) },
			{ ok: false, error: expect.objectContaining({ name: "Timeout" }) },
		]);
		expect((await engine.runs.get(runningChild))?.status).toBe("canceled");
	});
});

describe("child finishing before the parent's suspension transaction", () => {
	it("the parent is requeued by the suspension re-check and gets the child's result", async () => {
		const queue = uniqueQueue();
		let parentExecutions = 0;
		const child = defineTask({
			id: uniqueTaskId("child-fast"),
			queue: { name: queue, concurrency: 2 },
			run: async (p: { n: number }) => p.n + 1,
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue, concurrency: 2 },
			run: async (_p: { v: number }, ctx) => {
				parentExecutions++;
				const [result] = await Promise.all([
					ctx.step.triggerAndWait("c", child, { n: 1 }),
					// Holds the pre-suspension allSettled until the child has completed.
					ctx.step.run("hold", async () => {
						await waitFor(
							async () => {
								const { rows } = await ns.pool.query<{ status: string }>(
									`SELECT status FROM ${t("runs")} WHERE parent_run_id = $1`,
									[ctx.run.id]
								);
								return rows[0]?.status === "completed";
							},
							{ timeoutMs: 10_000, intervalMs: 20 }
						);
						return "held";
					}),
				]);
				return result;
			},
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ ok: true, output: 2 });
		await sleep(200);
		expect(parentExecutions).toBe(2);
	});

	// Item 5: `suspendRun`'s "requeued" branch (everything it suspended for was already resolved by
	// the time the suspension transaction ran) used to send `startedAt`/`durationMs` on a
	// `transition: "queued"` event -- same bug class as any other `run:queued`, which the docs
	// (`RunStatusInfo`/`RunEvent` JSDoc, README) already say never carries attempt telemetry.
	it("a requeued suspension's run:queued event carries no startedAt/durationMs", async () => {
		const queue = uniqueQueue();
		const child = defineTask({
			id: uniqueTaskId("child-fast-2"),
			queue: { name: queue, concurrency: 2 },
			run: async (p: { n: number }) => p.n + 1,
		});
		const parent = defineTask({
			id: uniqueTaskId("parent-2"),
			queue: { name: queue, concurrency: 2 },
			run: async (_p: { v: number }, ctx) => {
				const [result] = await Promise.all([
					ctx.step.triggerAndWait("c", child, { n: 1 }),
					ctx.step.run("hold", async () => {
						await waitFor(
							async () => {
								const { rows } = await ns.pool.query<{ status: string }>(
									`SELECT status FROM ${t("runs")} WHERE parent_run_id = $1`,
									[ctx.run.id]
								);
								return rows[0]?.status === "completed";
							},
							{ timeoutMs: 10_000, intervalMs: 20 }
						);
						return "held";
					}),
				]);
				return result;
			},
		});
		const engine = await startedEngine([child, parent]);
		const queuedEvents: Array<{ runId: string; startedAt?: Date; durationMs?: number }> = [];
		engine.events.on("run:queued", e => {
			queuedEvents.push(e);
		});
		const handle = await engine.trigger(parent, { v: 1 });
		await waitForStatus(engine, handle.id, "completed");

		// Two `run:queued` events for this run: the initial trigger, then the requeue-after-suspend
		// (not `run:waiting`, since nothing was left pending by the time the suspension transaction
		// ran). Neither carries startedAt/durationMs -- the second is the one this bug affected.
		const forThisRun = queuedEvents.filter(e => e.runId === handle.id);
		expect(forThisRun).toHaveLength(2);
		for (const e of forThisRun) {
			expect(e.startedAt).toBeUndefined();
			expect(e.durationMs).toBeUndefined();
		}
	});
});

describe("steps the task did not await", () => {
	it("a non-awaited step.trigger still creates its child and the run completes", async () => {
		const queue = uniqueQueue();
		const logs: string[] = [];
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: queue },
			run: async (p: { n: number }) => p.n,
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) => {
				void ctx.step.trigger("fire", child, { n: 7 });
				return "done";
			},
		});
		const engine = await startedEngine([child, parent], {
			logger: { ...testLogger, warn: (_f, message) => logs.push(message) },
		});
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toBe("done");
		const [c] = await childrenOf(handle.id);
		expect(c).toBeDefined();
		expect((await waitForStatus(engine, c?.id ?? "", "completed")).output).toBe(7);
		expect(logs.some(m => m.includes('"fire"') && m.includes("was not awaited"))).toBe(true);
	});

	it("a non-awaited step.run is persisted before the run completes", async () => {
		const queue = uniqueQueue();
		const task = defineTask({
			id: uniqueTaskId("fire-and-forget"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) => {
				void ctx.step.run("slow", async () => {
					await sleep(150);
					return 42;
				});
				return "done";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		await waitForStatus(engine, handle.id, "completed");
		expect(await stepsOf(handle.id)).toEqual([{ name: "slow", kind: "run", output: 42 }]);
	});
});

describe("step.trigger", () => {
	it("memoizes the child handle; the child outlives its parent's retention", async () => {
		const queue = uniqueQueue();
		let release!: () => void;
		const released = new Promise<void>(resolve => {
			release = resolve;
		});
		const handles: string[] = [];
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: queue },
			run: async (p: { n: number }, ctx) => {
				await ctx.step.run("work", async () => {
					await released;
					return 1;
				});
				return p.n + 1;
			},
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 10, jitter: false } },
			run: async (_p: { v: number }, ctx) => {
				const h = await ctx.step.trigger("notify", child, { n: 41 });
				handles.push(h.id);
				if (ctx.run.attempt === 1) throw new Error("force a replay");
				return h.id;
			},
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(handles).toHaveLength(2);
		expect(handles[0]).toBe(handles[1]);
		const childId = handles[0] ?? "";
		expect(run.output).toBe(childId);
		expect(await childrenOf(handle.id)).toHaveLength(1);
		expect(await keyOf(childId)).toEqual({ key: `__child:${handle.id}:notify`, expires_at: null });
		await waitForStatus(engine, childId, "running");

		// Retention with days = 0 deletes the finished parent; the running child survives.
		await ns.pool.query(
			`UPDATE ${t("runs")} SET finished_at = now() - interval '1 day' WHERE id = $1`,
			[handle.id]
		);
		const retention = createRetention({
			store: createSqlStore(ns.pool, ns.schema),
			logger: testLogger,
			days: 0,
			batchSize: 100,
		});
		expect(await retention.runOnce()).toBeGreaterThanOrEqual(1);
		expect(await engine.runs.get(handle.id)).toBeNull();
		expect((await engine.runs.get(childId))?.parentRunId).toBeNull();

		release();
		const childRun = await waitForStatus(engine, childId, "completed");
		expect(childRun.output).toBe(42);
	});

	it("step.trigger with delay schedules the child later", async () => {
		const queue = uniqueQueue();
		const child = defineTask({
			id: uniqueTaskId("child"),
			queue: { name: queue },
			run: async (p: { n: number }) => p.n,
		});
		const parent = defineTask({
			id: uniqueTaskId("parent"),
			queue: { name: queue },
			run: async (_p: { v: number }, ctx) =>
				(await ctx.step.trigger("later", child, { n: 1 }, { delay: "600ms" })).id,
		});
		const engine = await startedEngine([child, parent]);
		const handle = await engine.trigger(parent, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		const childId = run.output as string;
		const queued = await engine.runs.get(childId);
		expect(queued?.status).toBe("queued");
		const childRun = await waitForStatus(engine, childId, "completed");
		expect(
			(childRun.startedAt?.getTime() ?? 0) - (queued?.createdAt.getTime() ?? 0)
		).toBeGreaterThanOrEqual(550);
	});
});
