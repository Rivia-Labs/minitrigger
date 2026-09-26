/**
 * Phase 7: `ctx.logger` persisted in batches (`run_logs`), `engine.runs.logs` pagination,
 * `logging.minLevel`/`mirror`, `maxLogEntryBytes`.
 */
import { randomUUID } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import {
	describeTransports,
	type EngineOverrides,
	engineConfigFor,
} from "../helpers/engine-factory.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

describeTransports("logs", transport => {
	const ns = useTestNamespace();
	const shortId = () => randomUUID().slice(0, 8);

	type Mirrored = { level: string; message: string; fields: Record<string, unknown> };
	const mirrored: Mirrored[] = [];
	const testLogger: EngineLogger = {
		debug: (fields, message) => mirrored.push({ level: "debug", message, fields }),
		info: (fields, message) => mirrored.push({ level: "info", message, fields }),
		warn: (fields, message) => mirrored.push({ level: "warn", message, fields }),
		error: (fields, message) => mirrored.push({ level: "error", message, fields }),
	};

	const engines: Engine[] = [];
	const pools: Pool[] = [];

	function makeEngine(tasks: AnyTask[], overrides: EngineOverrides = {}): Engine {
		const engine = createEngine(
			engineConfigFor(transport, ns, {
				tasks,
				pool: { max: 5 },
				heartbeatIntervalMs: 1_000,
				stallTimeoutMs: 10_000,
				shutdownTimeoutMs: 2_000,
				logger: testLogger,
				...overrides,
			})
		);
		engines.push(engine);
		return engine;
	}

	async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 15_000) {
		await waitFor(async () => (await engine.runs.get(id))?.status === status, {
			timeoutMs,
			intervalMs: 25,
		});
	}

	/**
	 * A pool whose statements (via `pool.query` or checked-out clients) that insert into `run_logs`
	 * are counted.
	 */
	function countingPool(): { pool: Pool; logInserts: () => number } {
		const pool = new Pool({ connectionString: ns.databaseUrl, max: 8 });
		pool.on("error", () => {});
		pools.push(pool);
		let inserts = 0;
		const isLogInsert = (text: unknown) =>
			typeof text === "string" && /INSERT INTO\s+\S*run_logs/i.test(text);
		const sqlOf = (arg: unknown) =>
			typeof arg === "string" ? arg : (arg as { text?: unknown } | null)?.text;
		// `pool.query` itself runs on a pooled client, so wrapping every client counts everything once.
		const wrapped = new WeakSet<PoolClient>();
		pool.on("connect", client => {
			if (wrapped.has(client)) return;
			wrapped.add(client);
			const clientQuery = client.query.bind(client) as (...args: unknown[]) => unknown;
			(client as unknown as { query: (...args: unknown[]) => unknown }).query = (...args) => {
				if (isLogInsert(sqlOf(args[0]))) inserts++;
				return clientQuery(...args);
			};
		});
		return { pool, logInserts: () => inserts };
	}

	beforeAll(async () => {
		const engine = createEngine(
			engineConfigFor(transport, ns, {
				tasks: [],
			})
		);
		await engine.migrate();
		await engine.stop();
	});

	afterEach(async () => {
		await Promise.all(engines.splice(0).map(e => e.stop()));
		await Promise.all(pools.splice(0).map(p => p.end()));
		mirrored.length = 0;
	});

	describe("ctx.logger persistence", () => {
		it("stores 10k lines of one run in batches, not one query per line", async () => {
			const total = 10_000;
			const task = defineTask({
				id: `noisy-${shortId()}`,
				queue: { name: `q${shortId()}` },
				run: async (_payload: undefined, ctx) => {
					for (let i = 0; i < total; i++) {
						ctx.logger.info(`line ${i}`, { i });
						// Yield now and then so size- and timer-triggered flushes interleave with logging.
						if (i % 1_000 === 999) await new Promise(resolve => setTimeout(resolve, 5));
					}
					return "done";
				},
			});
			const { pool, logInserts } = countingPool();
			const engine = makeEngine([task], { postgres: { pool }, logging: { mirror: false } });
			await engine.start();
			const handle = await engine.trigger(task);
			await waitForStatus(engine, handle.id, "completed");

			// Flushed before the terminal write: everything is there as soon as the run is completed.
			const { rows } = await ns.pool.query<{ n: string }>(
				`SELECT count(*) AS n FROM "${ns.schema}".run_logs WHERE run_id = $1`,
				[handle.id]
			);
			expect(Number(rows[0]?.n)).toBe(total);
			expect(logInserts()).toBeGreaterThan(0);
			expect(logInserts()).toBeLessThanOrEqual(total / 50 + 5);

			// Order is kept (ids are monotonic per insert order).
			const first = await engine.runs.logs(handle.id, { limit: 3 });
			expect(first.items.map(e => e.message)).toEqual(["line 0", "line 1", "line 2"]);
			expect(first.items[0]?.data).toEqual({ i: 0 });
		});

		it("paginates runs.logs with afterId and applies minLevel, mirror and maxLogEntryBytes", async () => {
			const task = defineTask({
				id: `pages-${shortId()}`,
				queue: { name: `q${shortId()}` },
				run: async (_payload: undefined, ctx) => {
					ctx.logger.debug("hidden debug");
					for (let i = 0; i < 250; i++) ctx.logger.info(`entry ${i}`);
					ctx.logger.warn("big", { blob: "x".repeat(4_000) });
					ctx.logger.error("last");
				},
			});
			const engine = makeEngine([task], { limits: { maxLogEntryBytes: 1_024 } });
			await engine.start();
			const handle = await engine.trigger(task);
			await waitForStatus(engine, handle.id, "completed");

			const page1 = await engine.runs.logs(handle.id, { limit: 100 });
			expect(page1.items).toHaveLength(100);
			expect(page1.items[0]?.message).toBe("entry 0");
			expect(page1.nextAfterId).toBe(page1.items[99]?.id);
			const page2 = await engine.runs.logs(handle.id, {
				limit: 100,
				afterId: page1.nextAfterId,
			});
			expect(page2.items[0]?.message).toBe("entry 100");
			const page3 = await engine.runs.logs(handle.id, {
				limit: 100,
				afterId: page2.nextAfterId,
			});
			expect(page3.items.map(e => e.message)).toEqual([
				...Array.from({ length: 50 }, (_, i) => `entry ${200 + i}`),
				"big",
				"last",
			]);
			expect(page3.nextAfterId).toBeNull();
			expect(page3.items.map(e => e.level).slice(-2)).toEqual(["warn", "error"]);
			// `debug` is below the default minLevel ("info"): neither stored nor mirrored.
			const all = [...page1.items, ...page2.items, ...page3.items];
			expect(all.some(e => e.message === "hidden debug")).toBe(false);
			expect(mirrored.some(m => m.message === "hidden debug")).toBe(false);

			// `data` over maxLogEntryBytes is truncated (the mirror gets the original).
			const big = page3.items.find(e => e.message === "big");
			expect(JSON.stringify(big?.data).length).toBeLessThan(2_000);
			const mirroredBig = mirrored.find(m => m.message === "big");
			expect(mirroredBig?.fields).toMatchObject({
				runId: handle.id,
				taskId: task.id,
				attempt: 1,
				blob: "x".repeat(4_000),
			});

			expect(await engine.runs.logs("not-a-uuid")).toEqual({ items: [], nextAfterId: null });
		});

		it("mirror: false keeps logs out of config.logger; minLevel debug stores debug", async () => {
			const task = defineTask({
				id: `quiet-${shortId()}`,
				queue: { name: `q${shortId()}` },
				run: async (_payload: undefined, ctx) => {
					ctx.logger.debug("dbg", { n: 1 });
				},
			});
			const engine = makeEngine([task], { logging: { minLevel: "debug", mirror: false } });
			await engine.start();
			const handle = await engine.trigger(task);
			await waitForStatus(engine, handle.id, "completed");
			const page = await engine.runs.logs(handle.id);
			expect(page.items.map(e => [e.level, e.message, e.data])).toEqual([
				["debug", "dbg", { n: 1 }],
			]);
			expect(mirrored.some(m => m.message === "dbg")).toBe(false);
		});
	});
});
