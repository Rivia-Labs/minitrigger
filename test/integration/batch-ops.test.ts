/**
 * `runs.cancelMany` / `runs.retryFailed`: correctness, limits, paging and concurrency.
 */
import { randomUUID } from "node:crypto";
import { Client, escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine, type RunEvent } from "../../src/engine.js";
import { ConfigError } from "../../src/errors.js";
import { defineTask, type RunHandle } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);
const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const engines: Engine[] = [];
const t = (table: string) => `${escapeIdentifier(ns.schema)}.${escapeIdentifier(table)}`;

function makeEngine(tasks: EngineConfig["tasks"], overrides: Partial<EngineConfig> = {}): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 10 },
		shutdownTimeoutMs: 1_000,
		logger: silent,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

/** `created_at` has millisecond precision; ties are ordered by id. Space triggers to keep order. */
const tick = () => new Promise(resolve => setTimeout(resolve, 3));

async function statuses(engine: Engine, ids: readonly string[]) {
	return Promise.all(ids.map(async id => (await engine.runs.get(id))?.status));
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

describe("runs.cancelMany", () => {
	it("cancels matching non-terminal runs oldest first, pages with the cursor, leaves others alone", async () => {
		const queue = `q${shortId()}`;
		const a = defineTask({ id: `a-${shortId()}`, queue: { name: queue }, run: async () => 1 });
		const b = defineTask({ id: `b-${shortId()}`, queue: { name: queue }, run: async () => 2 });
		// Never started: every run stays queued.
		const engine = makeEngine([a, b]);
		const aIds: string[] = [];
		for (let i = 0; i < 5; i++) {
			aIds.push((await engine.trigger(a)).id);
			await tick();
		}
		const bId = (await engine.trigger(b)).id;
		const events: RunEvent[] = [];
		engine.events.on("run:canceled", e => {
			events.push(e);
		});

		const first = await engine.runs.cancelMany({ taskId: a.id, limit: 3 });
		expect(first.canceled).toEqual(aIds.slice(0, 3));
		expect(first.hasMore).toBe(true);
		expect(first.nextCursor).not.toBeNull();
		const second = await engine.runs.cancelMany({
			taskId: a.id,
			limit: 3,
			...(first.nextCursor ? { cursor: first.nextCursor } : {}),
		});
		expect(second.canceled).toEqual(aIds.slice(3));
		expect(second.hasMore).toBe(false);
		expect(second.nextCursor).toBeNull();

		expect(await statuses(engine, aIds)).toEqual(Array(5).fill("canceled"));
		expect((await engine.runs.get(bId))?.status).toBe("queued");
		const run = await engine.runs.get(aIds[0] ?? "");
		expect(run?.error).toMatchObject({ name: "Canceled", retryable: false });
		await waitFor(() => events.length === 5);
		// Nothing left to cancel for that task.
		expect(await engine.runs.cancelMany({ taskId: a.id, limit: 10 })).toEqual({
			canceled: [],
			failed: [],
			hasMore: false,
			nextCursor: null,
		});
	});

	it("filters by queue and creation window", async () => {
		const q1 = `q${shortId()}`;
		const q2 = `q${shortId()}`;
		const one = defineTask({ id: `one-${shortId()}`, queue: { name: q1 }, run: async () => 1 });
		const two = defineTask({ id: `two-${shortId()}`, queue: { name: q2 }, run: async () => 2 });
		const engine = makeEngine([one, two]);
		const early = await engine.trigger(one);
		await new Promise(resolve => setTimeout(resolve, 20));
		const cut = new Date();
		await new Promise(resolve => setTimeout(resolve, 20));
		const late = await engine.trigger(one);
		const other = await engine.trigger(two);

		const res = await engine.runs.cancelMany({ queue: q1, createdFrom: cut, limit: 10 });
		expect(res.canceled).toEqual([late.id]);
		expect(await statuses(engine, [early.id, late.id, other.id])).toEqual([
			"queued",
			"canceled",
			"queued",
		]);
		const before = await engine.runs.cancelMany({ queue: q1, createdTo: cut, limit: 10 });
		expect(before.canceled).toEqual([early.id]);
	});

	it("aborts running executions and cancels descendants like cancel()", async () => {
		const queue = `q${shortId()}`;
		const child = defineTask({
			id: `child-${shortId()}`,
			queue: { name: queue },
			run: async (_p: undefined, ctx) => {
				await ctx.step.sleep("forever", "1h");
				return "never";
			},
		});
		let aborted = false;
		const parent = defineTask({
			id: `parent-${shortId()}`,
			queue: { name: queue, concurrency: 4 },
			run: async (_p: undefined, ctx) => {
				ctx.signal.addEventListener("abort", () => {
					aborted = true;
				});
				await ctx.step.trigger("kid", child);
				await new Promise(resolve => setTimeout(resolve, 60_000));
				return "never";
			},
		});
		const engine = makeEngine([parent, child]);
		await engine.start();
		const p = await engine.trigger(parent);
		const kidId = await waitFor(async () => {
			const steps = await engine.runs.steps(p);
			return (steps.find(s => s.name === "kid")?.output as { id?: string } | null)?.id ?? "";
		});
		await waitFor(async () => (await engine.runs.get(kidId))?.status === "waiting");

		const res = await engine.runs.cancelMany({ taskId: parent.id, limit: 10 });
		expect(res.canceled).toEqual([p.id, kidId]);
		await waitFor(() => aborted);
		expect(await statuses(engine, [p.id, kidId])).toEqual(["canceled", "canceled"]);
	});

	it("two concurrent calls over the same runs cancel each exactly once", async () => {
		const task = defineTask({
			id: `c-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async () => 1,
		});
		const engine = makeEngine([task]);
		const other = makeEngine([task]);
		const ids: string[] = [];
		for (let i = 0; i < 40; i++) ids.push((await engine.trigger(task)).id);
		const [x, y] = await Promise.all([
			engine.runs.cancelMany({ taskId: task.id, limit: 40 }),
			other.runs.cancelMany({ taskId: task.id, limit: 40 }),
		]);
		const all = [...x.canceled, ...y.canceled];
		expect(new Set(all).size).toBe(all.length);
		expect(new Set(all)).toEqual(new Set(ids));
	});

	it("validates the filter", async () => {
		const engine = makeEngine([]);
		const bad: unknown[] = [
			undefined,
			null,
			{},
			{ limit: 0 },
			{ limit: 1_001 },
			{ limit: 2.5 },
			{ limit: "10" },
			{ limit: 10, taskId: 5 },
			{ limit: 10, createdFrom: "yesterday" },
			{ limit: 10, createdTo: new Date(Number.NaN) },
			{ limit: 10, cursor: { createdAt: new Date(), id: "not-a-uuid" } },
		];
		for (const filter of bad) {
			await expect(
				engine.runs.cancelMany(filter as Parameters<Engine["runs"]["cancelMany"]>[0])
			).rejects.toThrow(ConfigError);
			await expect(
				engine.runs.retryFailed(filter as Parameters<Engine["runs"]["retryFailed"]>[0])
			).rejects.toThrow(ConfigError);
		}
		await expect(engine.runs.cancelMany({ limit: 1_000 })).resolves.toMatchObject({
			hasMore: false,
		});
	});

	// Item 2: a non-deadlock error canceling one run must not discard the `canceled` list the others
	// (`mapBounded` used to throw the first error and lose every result -- `cancelOneOfBatch` now
	// catches its own errors and reports them in `failed` instead).
	it("a non-deadlock error for one run is reported in `failed`, without discarding the others", async () => {
		const queue = `q${shortId()}`;
		const task = defineTask({
			id: `boom-${shortId()}`,
			queue: { name: queue },
			run: async () => new Promise(() => {}), // never resolves; stays queued/running
		});
		const engine = makeEngine([task]);
		const boom = await engine.trigger(task);
		await tick();
		const ok = await engine.trigger(task);

		const originalQuery = Client.prototype.query;
		const spy = vi.spyOn(Client.prototype, "query").mockImplementation(function (
			this: Client,
			...args: unknown[]
		) {
			const [text, values] = args;
			if (
				typeof text === "string" &&
				/UPDATE .*\brun/i.test(text) &&
				JSON.stringify(values ?? []).includes(boom.id)
			) {
				return Promise.reject(new Error("boom: injected cancel failure"));
			}
			return (originalQuery as (...a: unknown[]) => unknown).apply(this, args);
		});
		try {
			const res = await engine.runs.cancelMany({ taskId: task.id, limit: 10 });
			expect(res.canceled).toEqual([ok.id]);
			expect(res.failed).toEqual([
				{ runId: boom.id, error: { name: "Error", message: "boom: injected cancel failure" } },
			]);
		} finally {
			spy.mockRestore();
		}
		// Not left half-canceled: the injected failure rolled its own transaction back.
		expect((await engine.runs.get(boom.id))?.status).toBe("queued");
		expect((await engine.runs.get(ok.id))?.status).toBe("canceled");
	});
});

describe("runs.retryFailed", () => {
	it("creates one new run per failed top-level run (like replay) and never retries it twice", async () => {
		const queue = `q${shortId()}`;
		let fail = true;
		const flaky = defineTask({
			id: `flaky-${shortId()}`,
			queue: { name: queue, concurrency: 4 },
			retry: { maxAttempts: 1 },
			run: async (payload: { n: number }) => {
				if (fail) throw new Error(`boom ${payload.n}`);
				return payload.n * 10;
			},
		});
		const engine = makeEngine([flaky]);
		await engine.start();
		const handles: RunHandle<number>[] = [];
		for (const n of [1, 2, 3]) {
			handles.push(await engine.trigger(flaky, { n }));
			await tick();
		}
		// Canceled before it ever ran: not a retry candidate.
		const canceled = await engine.trigger(flaky, { n: 0 }, { delay: "1h" });
		expect(await engine.runs.cancel(canceled)).toBe("canceled");
		await waitFor(async () =>
			(
				await statuses(
					engine,
					handles.map(h => h.id)
				)
			).every(s => s === "failed")
		);
		fail = false;
		const queued: RunEvent[] = [];
		engine.events.on("run:queued", e => {
			queued.push(e);
		});

		const first = await engine.runs.retryFailed({ taskId: flaky.id, limit: 2 });
		expect(first.retried.map(r => r.runId)).toEqual(handles.slice(0, 2).map(h => h.id));
		expect(first.skipped).toEqual([]);
		expect(first.hasMore).toBe(true);
		const second = await engine.runs.retryFailed({
			taskId: flaky.id,
			limit: 2,
			...(first.nextCursor ? { cursor: first.nextCursor } : {}),
		});
		expect(second.retried.map(r => r.runId)).toEqual([handles[2]?.id]);
		expect(second.hasMore).toBe(false);

		const retried = [...first.retried, ...second.retried];
		for (const [i, r] of retried.entries()) {
			await waitFor(async () => (await engine.runs.get(r.newRunId))?.status === "completed");
			const fresh = await engine.runs.get(r.newRunId);
			expect(fresh).toMatchObject({ payload: { n: i + 1 }, output: (i + 1) * 10, attempt: 1 });
			// The failed run itself is left as it was.
			expect((await engine.runs.get(r.runId))?.status).toBe("failed");
		}
		expect(queued.map(e => e.runId).sort()).toEqual(retried.map(r => r.newRunId).sort());
		// Already retried: nothing matches any more, even without a cursor.
		expect(await engine.runs.retryFailed({ taskId: flaky.id, limit: 10 })).toEqual({
			retried: [],
			skipped: [],
			hasMore: false,
			nextCursor: null,
		});
	});

	it("concurrent calls retry each failed run exactly once", async () => {
		const task = defineTask({
			id: `f-${shortId()}`,
			queue: { name: `q${shortId()}` },
			retry: { maxAttempts: 1 },
			run: async () => {
				throw new Error("nope");
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const ids = (await Promise.all(Array.from({ length: 10 }, () => engine.trigger(task)))).map(
			h => h.id
		);
		await waitFor(async () => (await statuses(engine, ids)).every(s => s === "failed"));
		// No worker any more: the new runs stay queued (else they fail and become candidates).
		await engine.stop();
		const x1 = makeEngine([task]);
		const x2 = makeEngine([task]);
		const [x, y] = await Promise.all([
			x1.runs.retryFailed({ taskId: task.id, limit: 10 }),
			x2.runs.retryFailed({ taskId: task.id, limit: 10 }),
		]);
		const retried = [...x.retried, ...y.retried].map(r => r.runId);
		expect(new Set(retried)).toEqual(new Set(ids));
		expect(retried).toHaveLength(ids.length);
		const skipped = [...x.skipped, ...y.skipped];
		expect(skipped.every(s => s.reason === "already_retried")).toBe(true);
		const { rows } = await ns.pool.query<{ n: string }>(
			`SELECT count(*) AS n FROM ${t("runs")} WHERE task_id = $1`,
			[task.id]
		);
		expect(Number(rows[0]?.n)).toBe(20);
	});

	it("skips runs whose task this process does not register, and failed children", async () => {
		const queue = `q${shortId()}`;
		const child = defineTask({
			id: `kid-${shortId()}`,
			queue: { name: queue },
			retry: { maxAttempts: 1 },
			run: async () => {
				throw new Error("child failed");
			},
		});
		const parent = defineTask({
			id: `par-${shortId()}`,
			queue: { name: queue },
			retry: { maxAttempts: 1 },
			run: async (_p: undefined, ctx) => {
				const r = await ctx.step.triggerAndWait("kid", child);
				if (!r.ok) throw new Error("parent failed too");
				return "ok";
			},
		});
		const engine = makeEngine([parent, child]);
		await engine.start();
		const p = await engine.trigger(parent);
		await waitFor(async () => (await engine.runs.get(p))?.status === "failed", {
			timeoutMs: 10_000,
		});

		// Children are never candidates, even filtered by their own task.
		expect((await engine.runs.retryFailed({ taskId: child.id, limit: 10 })).retried).toEqual([]);
		const other = makeEngine([child]);
		expect(await other.runs.retryFailed({ taskId: parent.id, limit: 10 })).toEqual({
			retried: [],
			skipped: [{ runId: p.id, reason: "task_not_registered" }],
			hasMore: false,
			nextCursor: null,
		});
		expect((await engine.runs.retryFailed({ taskId: parent.id, limit: 10 })).retried).toHaveLength(
			1
		);
	});

	// Item 2: one invalid row (payload too big for a lowered maxPayloadBytes) must not make the
	// whole call throw and lose every other candidate on the page.
	it("skips a failed run whose stored payload no longer fits maxPayloadBytes, and still retries the rest", async () => {
		const queue = `q${shortId()}`;
		const flaky = defineTask({
			id: `big-${shortId()}`,
			queue: { name: queue },
			retry: { maxAttempts: 1 },
			run: async (_p: { data: string }) => {
				throw new Error("boom");
			},
		});
		const engine = makeEngine([flaky]);
		await engine.start();
		const big = await engine.trigger(flaky, { data: "x".repeat(2_000) });
		await tick();
		const small = await engine.trigger(flaky, { data: "ok" });
		await waitFor(async () =>
			(await statuses(engine, [big.id, small.id])).every(s => s === "failed")
		);

		// A second engine (same tasks/schema) with a much smaller payload limit: `big`'s stored
		// payload no longer fits, but `small`'s does.
		const limited = makeEngine([flaky], { limits: { maxPayloadBytes: 100 } });
		const res = await limited.runs.retryFailed({ taskId: flaky.id, limit: 10 });
		expect(res.retried.map(r => r.runId)).toEqual([small.id]);
		expect(res.skipped).toEqual([{ runId: big.id, reason: "invalid_payload" }]);
		// Not retried twice by a later call with the original (larger) limit either.
		expect((await engine.runs.get(big.id))?.status).toBe("failed");
	});
});
