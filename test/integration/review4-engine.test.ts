/**
 * Review 4 (engine lifecycle), each case failing before its fix:
 * - 1: `migrate()` after stop() throws the clean lifecycle error, not pg's raw pool error.
 * - 3: the leader and outbox LISTEN clients of an injected pool always have a bounded connect.
 * - 4: a start() that stop() gave up on touches neither Redis nor Postgres after stop resolved.
 * - 5: a `runs.subscribe` whose subscriber a failed start() discarded throws instead of ending
 *   silently; one ended by stop() still ends quietly.
 * (Item 2, the subscriber-ready timeout error class, is covered in review3-engine.test.ts.)
 */
import { randomUUID } from "node:crypto";
import { escapeIdentifier, Pool } from "pg";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { MinitriggerError } from "../../src/errors.js";
import type { LeaderElectorConfig } from "../../src/leader.js";
import { migrate } from "../../src/migrations/migrate.js";
import type { OutboxListenerOptions } from "../../src/outbox.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const leaderConfigs = vi.hoisted(() => [] as unknown[]);
vi.mock("../../src/leader.js", async importOriginal => {
	const original = await importOriginal<typeof import("../../src/leader.js")>();
	return {
		...original,
		createLeaderElector: (config: LeaderElectorConfig) => {
			leaderConfigs.push(config);
			return original.createLeaderElector(config);
		},
	};
});

const outboxConfigs = vi.hoisted(() => [] as unknown[]);
vi.mock("../../src/outbox.js", async importOriginal => {
	const original = await importOriginal<typeof import("../../src/outbox.js")>();
	return {
		...original,
		createOutboxListener: (options: Parameters<typeof original.createOutboxListener>[0]) => {
			outboxConfigs.push(options);
			return original.createOutboxListener(options);
		},
	};
});

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);
const quiet: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };

const engines: Engine[] = [];
const pools: Pool[] = [];

function makeEngine(tasks: AnyTask[], overrides: Partial<EngineConfig> = {}): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 6 },
		shutdownTimeoutMs: 1_000,
		reconciler: { intervalMs: 60_000, graceMs: 30_000, batchSize: 50 },
		logger: quiet,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

function injectedPool(options: { connectionTimeoutMillis?: number } = {}): Pool {
	const pool = new Pool({ connectionString: ns.databaseUrl, max: 6, ...options });
	pool.on("error", () => {});
	pools.push(pool);
	return pool;
}

function simpleTask() {
	return defineTask({
		id: `t-${shortId()}`,
		queue: { name: `q${shortId()}` },
		run: async () => "ok",
	});
}

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
});

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(engines.splice(0).map(e => e.stop().catch(() => {})));
	await Promise.all(pools.splice(0).map(p => p.end().catch(() => {})));
});

describe("item 1: migrate() lifecycle guard", () => {
	it("after stop() resolved (engine-owned pool) it throws the lifecycle error, not pg's", async () => {
		const engine = makeEngine([simpleTask()]);
		await engine.stop();
		const outcome = await engine.migrate().then(
			() => "migrated",
			(error: unknown) => error
		);
		expect(outcome).toBeInstanceOf(MinitriggerError);
		expect((outcome as Error).message).toMatch(/this engine was stopped/);
	});

	it("is refused from the moment stop() is called (new work, like trigger), also with an injected pool", async () => {
		const engine = makeEngine([simpleTask()], { postgres: { pool: injectedPool() } });
		await engine.start();
		const stopping = engine.stop();
		await expect(engine.migrate()).rejects.toThrow(/this engine was stopped/);
		await stopping;
		await expect(engine.migrate()).rejects.toThrow(/this engine was stopped/);
	});

	it("still works on an engine that was never stopped", async () => {
		const engine = makeEngine([simpleTask()]);
		await expect(engine.migrate()).resolves.toBeDefined();
	});
});

describe("item 3: dedicated clients of an injected pool have a bounded connect", () => {
	it("an injected pool without connectionTimeoutMillis: the engine's default (10 s)", async () => {
		leaderConfigs.length = 0;
		outboxConfigs.length = 0;
		const engine = makeEngine([simpleTask()], { postgres: { pool: injectedPool() } });
		await engine.start();
		expect(leaderConfigs).toHaveLength(1);
		expect(outboxConfigs).toHaveLength(1);
		const leader = leaderConfigs[0] as LeaderElectorConfig;
		const listener = outboxConfigs[0] as OutboxListenerOptions;
		expect(leader.clientConfig?.connectionTimeoutMillis).toBe(10_000);
		expect(listener.clientConfig.connectionTimeoutMillis).toBe(10_000);
		// The rest of the pool's config is kept.
		expect(leader.clientConfig?.connectionString).toBe(ns.databaseUrl);
		expect(listener.clientConfig.connectionString).toBe(ns.databaseUrl);
	});

	it("an injected pool with connectionTimeoutMillis: the pool's own value", async () => {
		leaderConfigs.length = 0;
		outboxConfigs.length = 0;
		const engine = makeEngine([simpleTask()], {
			postgres: { pool: injectedPool({ connectionTimeoutMillis: 4_321 }) },
		});
		await engine.start();
		expect((leaderConfigs[0] as LeaderElectorConfig).clientConfig?.connectionTimeoutMillis).toBe(
			4_321
		);
		expect((outboxConfigs[0] as OutboxListenerOptions).clientConfig.connectionTimeoutMillis).toBe(
			4_321
		);
	});

	it("an engine-created pool: the configured pool.connectionTimeoutMs (outbox listener too)", async () => {
		outboxConfigs.length = 0;
		const engine = makeEngine([simpleTask()], { pool: { max: 6, connectionTimeoutMs: 4_321 } });
		await engine.start();
		expect((outboxConfigs[0] as OutboxListenerOptions).clientConfig.connectionTimeoutMillis).toBe(
			4_321
		);
	});
});

describe("item 4: a start() abandoned by stop() stops at the next step", () => {
	it("does not claim the prefix nor sync declarative schedules after stop() resolved", async () => {
		const ownerKey = `${ns.prefix}:owner-schema`;
		await ns.redis.del(ownerKey);
		const scheduled = defineTask({
			id: `sched-${shortId()}`,
			queue: { name: `q${shortId()}` },
			schedule: { cron: "0 3 * * *" },
			run: async () => "ok",
		});
		const pool = injectedPool({ connectionTimeoutMillis: 10_000 });
		// Hold start() inside checkEnvironment (its Postgres version check) until stop() gave up.
		let release!: () => void;
		const gate = new Promise<void>(resolve => {
			release = resolve;
		});
		let reached = false;
		const originalQuery = pool.query.bind(pool) as (...args: unknown[]) => Promise<unknown>;
		vi.spyOn(pool, "query").mockImplementation((async (...args: unknown[]) => {
			if (args[0] === "SHOW server_version_num") {
				reached = true;
				await gate;
			}
			return originalQuery(...args);
		}) as never);
		const engine = makeEngine([scheduled], { postgres: { pool } });
		const start = engine.start();
		start.catch(() => {});
		await waitFor(() => reached, { timeoutMs: 5_000 });

		await engine.stop(); // gives up on start() after its bounded wait
		release();
		const outcome = await start.then(
			() => "started",
			(error: unknown) => error
		);
		expect(outcome).toBeInstanceOf(MinitriggerError);
		expect((outcome as Error).message).toMatch(/stop\(\) was called while start\(\)/);
		await new Promise(resolve => setTimeout(resolve, 300));
		expect(await ns.redis.get(ownerKey)).toBeNull();
		const { rows } = await ns.pool.query(
			`SELECT id FROM ${escapeIdentifier(ns.schema)}.schedules WHERE id = $1`,
			[scheduled.id]
		);
		expect(rows).toEqual([]);
	}, 30_000);
});

describe("item 5: runs.subscribe ended from outside", () => {
	// The discarded-subscriber case (throws) is "runs.subscribe iterations sharing a discarded
	// subscriber throw instead of lingering" in review3-engine.test.ts (it needs its blackhole Redis).
	it("an iteration ended by stop() still ends quietly, without throwing", async () => {
		let running = false;
		const hanging = defineTask({
			id: `hang-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_payload: undefined, ctx) => {
				running = true;
				await new Promise(resolve => ctx.signal.addEventListener("abort", resolve));
			},
		});
		const engine = makeEngine([hanging]);
		await engine.start();
		const handle = await engine.trigger(hanging);
		await waitFor(() => running, { timeoutMs: 5_000 });
		const statuses: string[] = [];
		const drained = (async () => {
			for await (const update of engine.runs.subscribe(handle)) statuses.push(update.status);
			return "ended";
		})();
		await waitFor(() => statuses.length > 0, { timeoutMs: 5_000 });
		await engine.stop();
		await expect(drained).resolves.toBe("ended");
		expect(statuses[0]).toBe("running");
	}, 20_000);
});
