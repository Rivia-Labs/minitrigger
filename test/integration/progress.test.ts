/**
 * `ctx.progress.set` / `runs.progress` (decision 23): size limit, write coalescing, flush before
 * every terminal/suspension write, survival across a retry, and cross-process delivery via
 * `runs.subscribe`.
 */
import { randomUUID } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { BullmqTransportConfig, CommonConfig } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { SerializationError } from "../../src/errors.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);

const engines: Engine[] = [];
const pools: Pool[] = [];

function makeEngine(
	tasks: AnyTask[],
	overrides: Partial<CommonConfig> & Partial<BullmqTransportConfig> = {}
): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 5 },
		heartbeatIntervalMs: 1_000,
		stallTimeoutMs: 10_000,
		shutdownTimeoutMs: 2_000,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 15_000) {
	await waitFor(async () => (await engine.runs.get(id))?.status === status, {
		timeoutMs,
		intervalMs: 25,
	});
}

/** A pool that counts `UPDATE ... runs SET progress = ...` statements. */
function countingPool(): { pool: Pool; progressUpdates: () => number } {
	const pool = new Pool({ connectionString: ns.databaseUrl, max: 8 });
	pool.on("error", () => {});
	pools.push(pool);
	let count = 0;
	const isProgressUpdate = (text: unknown) =>
		typeof text === "string" && /UPDATE\s+\S*runs\s+SET\s+progress\s*=/i.test(text);
	const sqlOf = (arg: unknown) =>
		typeof arg === "string" ? arg : (arg as { text?: unknown } | null)?.text;
	const wrapped = new WeakSet<PoolClient>();
	pool.on("connect", client => {
		if (wrapped.has(client)) return;
		wrapped.add(client);
		const clientQuery = client.query.bind(client) as (...args: unknown[]) => unknown;
		(client as unknown as { query: (...args: unknown[]) => unknown }).query = (...args) => {
			if (isProgressUpdate(sqlOf(args[0]))) count++;
			return clientQuery(...args);
		};
	});
	return { pool, progressUpdates: () => count };
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
	await Promise.all(pools.splice(0).map(p => p.end()));
});

describe("ctx.progress.set", () => {
	it("rejects an oversized value with SerializationError (caught normally, not fatal)", async () => {
		const task = defineTask({
			id: `progress-limit-${shortId()}`,
			run: async (_payload: undefined, ctx) => {
				try {
					await ctx.progress.set({ big: "x".repeat(1_000) });
					return { caught: null };
				} catch (error) {
					return { caught: error instanceof SerializationError ? "SerializationError" : "other" };
				}
			},
		});
		const engine = makeEngine([task], { limits: { maxProgressBytes: 50 } });
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "completed");
		const run = await engine.runs.get(handle.id);
		expect(run?.output).toEqual({ caught: "SerializationError" });
		// Rejected before ever reaching the store: nothing was written.
		expect(run?.progress).toBeNull();
	});

	it("coalesces a tight loop of sets into a handful of writes, keeping the last value", async () => {
		const total = 800;
		const task = defineTask({
			id: `progress-coalesce-${shortId()}`,
			run: async (_payload: undefined, ctx) => {
				for (let i = 0; i < total; i++) {
					await ctx.progress.set({ i });
				}
				return { done: true };
			},
		});
		const { pool, progressUpdates } = countingPool();
		const engine = makeEngine([task], { postgres: { pool } });
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "completed");

		// Flushed before the terminal write: the very last value is there as soon as the run
		// completes, whatever the loop's actual write count was.
		const run = await engine.runs.get(handle.id);
		expect(run?.progress).toEqual({ i: total - 1 });
		expect(progressUpdates()).toBeGreaterThan(0);
		expect(progressUpdates()).toBeLessThan(50);
	});

	it("flushes the last value before completing", async () => {
		const task = defineTask({
			id: `progress-complete-${shortId()}`,
			run: async (_payload: undefined, ctx) => {
				await ctx.progress.set({ step: 1 });
				await ctx.progress.set({ step: 2 });
				return { ok: true };
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "completed");
		expect((await engine.runs.get(handle.id))?.progress).toEqual({ step: 2 });
	});

	it("flushes the last value before failing", async () => {
		const task = defineTask({
			id: `progress-fail-${shortId()}`,
			retry: { maxAttempts: 1 },
			run: async (_payload: undefined, ctx) => {
				await ctx.progress.set({ step: "before-fail" });
				throw new Error("boom");
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "failed");
		expect((await engine.runs.get(handle.id))?.progress).toEqual({ step: "before-fail" });
	});

	it("flushes the last value before suspending", async () => {
		const task = defineTask({
			id: `progress-suspend-${shortId()}`,
			run: async (_payload: undefined, ctx) => {
				await ctx.progress.set({ step: "before-sleep" });
				await ctx.step.sleep("wait", "5s");
				return { ok: true };
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "waiting");
		expect((await engine.runs.get(handle.id))?.progress).toEqual({ step: "before-sleep" });
	});

	it("stop() hands back a run with a pending (coalesced) value: that value is flushed first", async () => {
		let lastSet = -1;
		let started = false;
		const task = defineTask({
			id: `progress-handback-${shortId()}`,
			run: async (_payload: undefined, ctx) => {
				started = true;
				// Sets far faster than the coalescing window, so one is always pending, and keeps
				// running past shutdownTimeoutMs (only the abort stops it) to be handed back.
				for (let i = 0; !ctx.signal.aborted; i++) {
					void ctx.progress.set({ i });
					lastSet = i;
					await new Promise(resolve => setTimeout(resolve, 5));
				}
				await new Promise(() => {});
			},
		});
		const engine = makeEngine([task], { shutdownTimeoutMs: 200 });
		await engine.start();
		const handle = await engine.trigger(task);
		await waitFor(() => started && lastSet > 100, { timeoutMs: 10_000 });
		await engine.stop();

		const client = makeEngine([]);
		const run = await client.runs.get(handle.id);
		expect(run?.status).toBe("queued");
		expect(run?.progress).toEqual({ i: lastSet });
	});

	it("survives a retry: runs.get keeps the last progress across attempts (decision 23)", async () => {
		let attempts = 0;
		const task = defineTask({
			id: `progress-retry-${shortId()}`,
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 10 } },
			run: async (_payload: undefined, ctx) => {
				attempts++;
				if (attempts === 1) {
					await ctx.progress.set({ attempt: 1 });
					throw new Error("retry me");
				}
				return { attempts };
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "completed");
		// The engine never resets progress on its own; the task's 2nd attempt never called `set`.
		expect((await engine.runs.get(handle.id))?.progress).toEqual({ attempt: 1 });
	});

	it("snapshots the value at set() time: mutating it afterward does not change what's persisted", async () => {
		const task = defineTask({
			id: `progress-mutate-${shortId()}`,
			run: async (_payload: undefined, ctx) => {
				// Leading write: fires (almost) immediately, consuming the coalescing window.
				await ctx.progress.set({ step: "first" });
				const value: { step: string; big?: string } = { step: "second" };
				// Arrives well inside the 250ms coalescing window: deferred, held as `pending`.
				await ctx.progress.set(value);
				// Mutate the SAME object handed to set(), before the deferred write fires. If the
				// coalescer kept a reference instead of a snapshot, the deferred write would pick up
				// this mutation (bypassing the validation already done at set() time).
				value.big = "x".repeat(1_000);
				await new Promise(resolve => setTimeout(resolve, 400)); // let the deferred write land
				return { ok: true };
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "completed");
		expect((await engine.runs.get(handle.id))?.progress).toEqual({ step: "second" });
	});

	it("delivers progress-only updates to runs.subscribe across engines", async () => {
		const task = defineTask({
			id: `progress-subscribe-${shortId()}`,
			run: async (_payload: undefined, ctx) => {
				await ctx.progress.set({ step: 1 });
				await new Promise(resolve => setTimeout(resolve, 300));
				await ctx.progress.set({ step: 2 });
				return { ok: true };
			},
		});
		const worker = makeEngine([task]);
		const subscriber = makeEngine([]);
		const handle = await worker.trigger(task);

		const updates: { status: string; progress: unknown }[] = [];
		const done = (async () => {
			for await (const update of subscriber.runs.subscribe(handle.id)) {
				updates.push({ status: update.status, progress: update.progress });
				if (update.status === "completed" || update.status === "failed") break;
			}
		})();

		await worker.start();
		await done;

		const progresses = updates.map(u => u.progress);
		expect(progresses).toContainEqual({ step: 1 });
		expect(progresses).toContainEqual({ step: 2 });
		expect(updates[updates.length - 1]).toMatchObject({
			status: "completed",
			progress: { step: 2 },
		});
	}, 15_000);
});
