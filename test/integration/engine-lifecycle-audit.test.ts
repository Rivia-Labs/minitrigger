/**
 * External audit findings on the engine lifecycle (items 6, 7, 8, 11 and low items a/b):
 * - 6: `stop()` must stop fetching before waiting for the leader tick (no hot hand-back loop);
 * - 7: `stop()` must finish with Redis unreachable;
 * - 8: a shared prefix across schemas is refused at `start()`, and a tick tagged with another
 *   schema never removes that schema's scheduler;
 * - 11: a `start()` failing midway leaves nothing running, so it can be retried;
 * - a: non-UUID ids and invalid `list`/`logs` arguments behave consistently;
 * - b: pool sizing/acquisition-timeout warnings.
 */
import { randomUUID } from "node:crypto";
import net from "node:net";
import { Queue } from "bullmq";
import { escapeIdentifier, Pool } from "pg";
import { afterEach, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { ConfigError, MinitriggerError } from "../../src/errors.js";
import { migrate } from "../../src/migrations/migrate.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

const shortId = () => randomUUID().slice(0, 8);
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

type LogRecord = { level: string; message: string; fields: Record<string, unknown> };
const logs: LogRecord[] = [];
const testLogger: EngineLogger = {
	debug: (fields, message) => logs.push({ level: "debug", message, fields }),
	info: (fields, message) => logs.push({ level: "info", message, fields }),
	warn: (fields, message) => logs.push({ level: "warn", message, fields }),
	error: (fields, message) => logs.push({ level: "error", message, fields }),
};

const engines: Engine[] = [];
const cleanups: Array<() => Promise<void>> = [];

function makeEngine(tasks: AnyTask[], overrides: Partial<EngineConfig> = {}): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 8 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 5_000,
		shutdownTimeoutMs: 1_000,
		reconciler: { intervalMs: 60_000, graceMs: 30_000, batchSize: 50 },
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop().catch(() => {})));
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
	// Item 8 tests leave another schema as the prefix owner.
	await ns.redis.del(`${ns.prefix}:owner-schema`);
	logs.length = 0;
});

function simpleTask(label: string, concurrency?: number) {
	return defineTask({
		id: `${label}-${shortId()}`,
		queue: { name: `q${shortId()}`, ...(concurrency !== undefined ? { concurrency } : {}) },
		run: async (payload: { v: number }) => payload.v * 2,
	});
}

const t = (table: string) => `${escapeIdentifier(ns.schema)}.${escapeIdentifier(table)}`;

async function runRow(id: string) {
	const { rows } = await ns.pool.query<{ status: string; enqueue_seq: number }>(
		`SELECT status, enqueue_seq FROM ${t("runs")} WHERE id = $1`,
		[id]
	);
	const row = rows[0];
	if (!row) throw new Error(`run ${id} not found`);
	return row;
}

/** A pool whose `query` can be intercepted (only direct `pool.query` calls, not `pool.connect`). */
function interceptedPool(
	intercept: (sql: string) => Promise<void> | undefined
): Pool & { raw: Pool } {
	const pool = new Pool({ connectionString: ns.databaseUrl, max: 20 });
	pool.on("error", () => {});
	const original = pool.query.bind(pool) as (...args: unknown[]) => Promise<unknown>;
	(pool as unknown as { query: (...args: unknown[]) => Promise<unknown> }).query = async (
		...args: unknown[]
	) => {
		const first = args[0];
		const sql = typeof first === "string" ? first : ((first as { text?: string })?.text ?? "");
		const pending = intercept(sql);
		if (pending) await pending;
		return original(...args);
	};
	cleanups.push(() => pool.end());
	return Object.assign(pool, { raw: pool });
}

describe("item 6: stop() does not hot-loop hand-backs while waiting for the leader tick", () => {
	it("a run enqueued during stop() is not acquired and handed back over and over", async () => {
		const task = simpleTask("hot");
		let hit: (() => void) | undefined;
		const hitPromise = new Promise<void>(resolve => {
			hit = resolve;
		});
		let release: (() => void) | undefined;
		const gate = new Promise<void>(resolve => {
			release = resolve;
		});
		// The leader tick's retention clock read blocks until released: `stop()` has to wait for it.
		const pool = interceptedPool(sql => {
			if (!sql.includes("AS now_ms")) return undefined;
			hit?.();
			return gate;
		});
		const engine = makeEngine([task], { postgres: { pool }, pool: {} });
		await engine.migrate();
		await engine.start();
		await hitPromise;

		const producer = makeEngine([task]);
		const stopping = engine.stop();
		const handle = await producer.trigger(task, { v: 1 });
		const initialSeq = (await runRow(handle.id)).enqueue_seq;
		await sleep(1_500);
		release?.();
		await stopping;

		const row = await runRow(handle.id);
		expect(row.status).toBe("queued");
		// Before the fix every fetch acquired, handed back (enqueue_seq + 1) and re-enqueued at once.
		expect(row.enqueue_seq - initialSeq).toBeLessThanOrEqual(1);
	});
});

/** A TCP proxy to the test Redis that can be killed to simulate Redis going away. */
async function redisProxy(): Promise<{ url: string; kill(): Promise<void> }> {
	const target = new URL(ns.redisUrl);
	const sockets = new Set<net.Socket>();
	const server = net.createServer(client => {
		const upstream = net.connect(Number(target.port || 6379), target.hostname);
		sockets.add(client);
		sockets.add(upstream);
		client.pipe(upstream).pipe(client);
		const drop = () => {
			client.destroy();
			upstream.destroy();
		};
		client.on("error", drop);
		upstream.on("error", drop);
		client.on("close", drop);
		upstream.on("close", drop);
	});
	await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
	const address = server.address() as net.AddressInfo;
	const url = `redis://127.0.0.1:${address.port}${target.pathname}`;
	let killed = false;
	const kill = async () => {
		if (killed) return;
		killed = true;
		const closed = new Promise<void>(resolve => server.close(() => resolve()));
		for (const s of sockets) s.destroy();
		await closed;
	};
	cleanups.push(kill);
	return { url, kill };
}

describe("item 7: stop() with Redis unreachable", () => {
	it("finishes instead of hanging on the subscriber/connections", async () => {
		const proxy = await redisProxy();
		const task = simpleTask("down");
		const engine = makeEngine([task], { redis: proxy.url });
		await engine.migrate();
		await engine.start();
		const handle = await engine.trigger(task, { v: 2 });
		await waitFor(async () => (await runRow(handle.id)).status === "completed");

		await proxy.kill();
		const started = Date.now();
		const outcome = await Promise.race([
			engine.stop().then(() => "stopped" as const),
			sleep(20_000).then(() => "hung" as const),
		]);
		expect(outcome).toBe("stopped");
		expect(Date.now() - started).toBeLessThan(20_000);
	}, 30_000);

	it("finishes with a run still active: the hand-back's enqueue does not block it", async () => {
		const proxy = await redisProxy();
		let started: (() => void) | undefined;
		const running = new Promise<void>(resolve => {
			started = resolve;
		});
		const task = defineTask({
			id: `long-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_p: null, ctx) => {
				started?.();
				await new Promise<void>(resolve => ctx.signal.addEventListener("abort", () => resolve()));
				return null;
			},
		});
		const engine = makeEngine([task], { redis: proxy.url, shutdownTimeoutMs: 500 });
		await engine.migrate();
		await engine.start();
		const handle = await engine.trigger(task, null);
		await running;

		await proxy.kill();
		const t0 = Date.now();
		const outcome = await Promise.race([
			engine.stop().then(() => "stopped" as const),
			sleep(20_000).then(() => "hung" as const),
		]);
		expect(outcome).toBe("stopped");
		expect(Date.now() - t0).toBeLessThan(12_000);
		// Released in Postgres (no attempt increment); the lost enqueue is the reconciler's job.
		expect((await runRow(handle.id)).status).toBe("queued");
	}, 30_000);
});

describe("item 8: prefix shared by two schemas", () => {
	async function otherSchema(): Promise<string> {
		const schema = `${ns.schema.slice(0, 50)}_b`;
		await ns.pool.query(`CREATE SCHEMA IF NOT EXISTS ${escapeIdentifier(schema)}`);
		cleanups.push(async () => {
			await ns.pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(schema)} CASCADE`);
		});
		await migrate(ns.pool, schema);
		return schema;
	}

	it("start() refuses a prefix already owned by another schema (ConfigError)", async () => {
		const schemaB = await otherSchema();
		const task = simpleTask("owner");
		const a = makeEngine([task]);
		await a.migrate();
		await a.start();

		const b = makeEngine([task], { schema: schemaB });
		await expect(b.start()).rejects.toBeInstanceOf(ConfigError);
		await expect(b.start()).rejects.toThrow(/prefix/);
	});

	it("a tick tagged with another schema never removes that schema's scheduler", async () => {
		const schemaB = await otherSchema();
		const task = simpleTask("tick");
		const a = makeEngine([task]);
		await a.migrate();
		await a.start();
		const scheduleId = `s-${shortId()}`;
		await a.schedules.upsert({ id: scheduleId, task, cron: "* * * * * *", payload: { v: 1 } });

		// Simulates a deployment that predates the start() check (or a hand-deleted owner key).
		await ns.redis.del(`${ns.prefix}:owner-schema`);
		const b = makeEngine([task], { schema: schemaB, systemConcurrency: 20 });
		await b.start();
		cleanups.push(async () => {
			await a.schedules.delete(scheduleId).catch(() => {});
		});

		await sleep(4_000);
		// B consumed some of A's ticks: without the guard it found no row in ITS schema and
		// removed A's BullMQ scheduler, silently ending the schedule.
		const list = await a.schedules.list();
		expect(list.map(s => s.id)).toContain(scheduleId);
		const queue = new Queue(`${ns.prefix}-system`, { connection: ns.redis, prefix: ns.prefix });
		try {
			expect(await queue.getJobScheduler(scheduleId)).toBeDefined();
		} finally {
			await queue.close();
		}
		const { rows } = await ns.pool.query<{ n: string }>(
			`SELECT count(*)::text AS n FROM ${escapeIdentifier(schemaB)}.runs`
		);
		expect(Number(rows[0]?.n)).toBe(0);
		expect(logs.some(l => l.level === "error" && /another schema/.test(l.message))).toBe(true);
	}, 20_000);
});

describe("item 11: a start() failing midway can be retried", () => {
	it("a declarative sync that throws once leaves no worker behind", async () => {
		const task = defineTask({
			id: `decl-${shortId()}`,
			queue: { name: `q${shortId()}` },
			schedule: { cron: "0 0 1 1 *", payload: { v: 3 } },
			run: async (payload: { v: number }) => payload.v * 2,
		});
		let failOnce = false;
		const pool = interceptedPool(sql => {
			if (failOnce && sql.includes("schedules")) {
				failOnce = false;
				return Promise.reject(new Error("injected sync failure"));
			}
			return undefined;
		});
		const engine = makeEngine([task], {
			postgres: { pool },
			pool: {},
			schedules: { pruneDeclarative: true },
		});
		await engine.migrate();
		failOnce = true;
		await expect(engine.start()).rejects.toThrow(/injected sync failure/);

		await engine.start();
		const handle = await engine.trigger(task, { v: 4 });
		await waitFor(async () => (await runRow(handle.id)).status === "completed");
		const run = await engine.runs.get(handle);
		expect(run?.output).toBe(8);
	});
});

describe("low (a): argument validation of runs.*", () => {
	it("non-UUID ids: get -> null, cancel -> not_found, replay -> not found error", async () => {
		const engine = makeEngine([simpleTask("ids")]);
		await engine.migrate();
		expect(await engine.runs.get("not-a-uuid")).toBeNull();
		expect(await engine.runs.cancel("not-a-uuid")).toBe("not_found");
		await expect(engine.runs.replay("not-a-uuid")).rejects.toBeInstanceOf(MinitriggerError);
		await expect(engine.runs.replay("not-a-uuid")).rejects.toThrow(/not found/);
	});

	it("runs.list with a non-finite limit and runs.logs with a non-numeric afterId throw ConfigError", async () => {
		const engine = makeEngine([simpleTask("args")]);
		await engine.migrate();
		await expect(engine.runs.list({ limit: Number.NaN })).rejects.toBeInstanceOf(ConfigError);
		await expect(engine.runs.logs(randomUUID(), { afterId: "abc" })).rejects.toBeInstanceOf(
			ConfigError
		);
		await expect(engine.runs.logs(randomUUID(), { limit: Number.NaN })).rejects.toBeInstanceOf(
			ConfigError
		);
		// Still fine:
		expect((await engine.runs.list({ limit: 5, taskId: "none" })).items).toEqual([]);
		expect((await engine.runs.logs(randomUUID(), { afterId: "10" })).items).toEqual([]);
	});
});

describe("low (b): pool sizing and acquisition timeout", () => {
	it("warns at start() when an explicit pool.max is below total concurrency + 5", async () => {
		const engine = makeEngine([simpleTask("size", 10)], { pool: { max: 5 } });
		await engine.migrate();
		await engine.start();
		expect(logs.some(l => l.level === "warn" && /pool\.max/.test(l.message))).toBe(true);
	});

	it("does not warn when the engine sizes its own pool", async () => {
		const engine = makeEngine([simpleTask("size", 10)], { pool: {} });
		await engine.migrate();
		await engine.start();
		expect(logs.some(l => l.level === "warn" && /pool/.test(l.message))).toBe(false);
	});

	it("warns for an injected pool that is too small or has no connection timeout", async () => {
		const pool = new Pool({ connectionString: ns.databaseUrl, max: 3 });
		pool.on("error", () => {});
		cleanups.push(() => pool.end());
		const engine = makeEngine([simpleTask("inj", 4)], { postgres: { pool }, pool: {} });
		await engine.migrate();
		await engine.start();
		const warns = logs.filter(l => l.level === "warn").map(l => l.message);
		expect(warns.some(m => /pool\.max/.test(m))).toBe(true);
		expect(warns.some(m => /connectionTimeoutMillis/.test(m))).toBe(true);
	});
});

describe("start() with Redis unreachable", () => {
	it("rejects with an actionable ConfigError instead of hanging, and stop() still resolves", async () => {
		// A port nothing listens on: with `maxRetriesPerRequest: null` every command would wait in
		// ioredis' offline queue forever without the PING guard.
		const server = net.createServer();
		await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
		const { port } = server.address() as net.AddressInfo;
		await new Promise<void>(resolve => server.close(() => resolve()));

		const engine = makeEngine([simpleTask("down")], { redis: `redis://127.0.0.1:${port}` });
		await engine.migrate();
		const started = Date.now();
		await expect(engine.start()).rejects.toThrow(/cannot reach Redis/);
		expect(Date.now() - started).toBeLessThan(10_000);
		await engine.stop();
	}, 30_000);
});
