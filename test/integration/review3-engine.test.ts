/**
 * Third external review, engine items against the real infra:
 * - 1: start() never hangs on a subscriber that can't become ready, stop() during such a start()
 *   is bounded, a failed start() closes the workers' dedicated connections, and the leader client
 *   built from a connection string has a connect timeout;
 * - 3: a stopped real engine refuses reads and writes with the lifecycle error;
 * - 5: filters with NUL / lone surrogates are ConfigErrors, not raw pg errors.
 */
import { randomUUID } from "node:crypto";
import net from "node:net";
import { Queue, Worker } from "bullmq";
import { Redis, type RedisOptions } from "ioredis";
import { Client, escapeIdentifier } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { BullmqTransportConfig, CommonConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { ConfigError, MinitriggerError } from "../../src/errors.js";
import type { LeaderElectorConfig } from "../../src/leader.js";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlStore } from "../../src/store/sql/index.js";
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

const outboxDispatchers = vi.hoisted(
	() => [] as Array<{ dispatch: (signal: AbortSignal) => Promise<boolean>; calls: number }>
);
vi.mock("../../src/outbox.js", async importOriginal => {
	const original = await importOriginal<typeof import("../../src/outbox.js")>();
	return {
		...original,
		createOutboxListener: (options: Parameters<typeof original.createOutboxListener>[0]) => {
			const entry = { dispatch: options.dispatchBatch, calls: 0 };
			outboxDispatchers.push(entry);
			return original.createOutboxListener({
				...options,
				dispatchBatch: async signal => {
					try {
						return await entry.dispatch(signal);
					} finally {
						entry.calls++;
					}
				},
			});
		},
	};
});

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);

const quiet: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };

const engines: Engine[] = [];
const cleanups: Array<() => Promise<void> | void> = [];

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
		pool: { max: 6 },
		shutdownTimeoutMs: 1_000,
		reconciler: { intervalMs: 60_000, graceMs: 30_000, batchSize: 50 },
		logger: quiet,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

function simpleTask() {
	return defineTask({
		id: `t-${shortId()}`,
		queue: { name: `q${shortId()}` },
		run: async () => "ok",
	});
}

/** A TCP server that accepts connections and never answers (a Redis that never becomes ready). */
let blackhole: net.Server;
const blackholeSockets = new Set<net.Socket>();
let blackholePort = 0;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	blackhole = net.createServer(socket => {
		blackholeSockets.add(socket);
		socket.on("close", () => blackholeSockets.delete(socket));
		socket.on("error", () => {});
	});
	await new Promise<void>(resolve => blackhole.listen(0, "127.0.0.1", resolve));
	blackholePort = (blackhole.address() as net.AddressInfo).port;
});

afterAll(async () => {
	for (const socket of blackholeSockets) socket.destroy();
	await new Promise<void>(resolve => blackhole.close(() => resolve()));
});

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(engines.splice(0).map(e => e.stop().catch(() => {})));
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

/**
 * An injected ioredis connection whose `duplicate()` calls are recorded; while `hang` is set,
 * the next duplicate (the pub/sub subscriber is the first one start() creates) points at the
 * blackhole server.
 */
function trackedConnection(): {
	connection: Redis;
	duplicates: Redis[];
	control: { hangNext: boolean };
} {
	const connection = new Redis(ns.redisUrl, { maxRetriesPerRequest: 3 });
	connection.on("error", () => {});
	const duplicates: Redis[] = [];
	const control = { hangNext: false };
	const original = connection.duplicate.bind(connection);
	connection.duplicate = ((override?: Partial<RedisOptions>) => {
		let dup: Redis;
		if (control.hangNext) {
			control.hangNext = false;
			dup = new Redis({ host: "127.0.0.1", port: blackholePort, ...override });
		} else {
			dup = original(override);
		}
		dup.on("error", () => {});
		duplicates.push(dup);
		return dup;
	}) as typeof connection.duplicate;
	cleanups.push(() => {
		for (const dup of duplicates) dup.disconnect();
		connection.disconnect();
	});
	return { connection, duplicates, control };
}

describe("item 1: start()/stop() bounded on Redis connection problems", () => {
	it("start() fails (MinitriggerError, not ConfigError) when the subscriber never becomes ready, and can be retried", async () => {
		const { connection, duplicates, control } = trackedConnection();
		const task = simpleTask();
		const engine = makeEngine([task], { redis: { connection } });

		control.hangNext = true;
		const startedAt = Date.now();
		const outcome = await Promise.race([
			engine.start().then(
				() => "started",
				(error: unknown) => error
			),
			new Promise(resolve => setTimeout(() => resolve("hung"), 9_000)),
		]);
		// Review 4 item 2: transient infrastructure, so not a ConfigError (callers retry it).
		expect(outcome).toBeInstanceOf(MinitriggerError);
		expect(outcome).not.toBeInstanceOf(ConfigError);
		expect((outcome as Error).message).toMatch(/subscriber did not become ready/);
		expect(Date.now() - startedAt).toBeLessThan(8_000);
		// The subscriber this attempt opened was dropped.
		const subscriber = duplicates[0];
		expect(subscriber).toBeDefined();
		await waitFor(() => subscriber?.status === "end", { timeoutMs: 2_000 });

		// Restartable once Redis accepts new connections again.
		await engine.start();
		const handle = await engine.trigger(task);
		await waitFor(async () => (await engine.runs.get(handle.id))?.status === "completed", {
			timeoutMs: 5_000,
		});
	}, 20_000);

	it("stop() during a start() stuck on the subscriber resolves quickly", async () => {
		const { connection, control } = trackedConnection();
		const engine = makeEngine([simpleTask()], { redis: { connection } });

		control.hangNext = true;
		const start = engine.start();
		start.catch(() => {});
		// Let start() reach the SUBSCRIBE wait.
		await new Promise(resolve => setTimeout(resolve, 500));
		const stoppedAt = Date.now();
		const stopped = await Promise.race([
			engine.stop().then(() => "stopped"),
			new Promise(resolve => setTimeout(() => resolve("hung"), 9_000)),
		]);
		expect(stopped).toBe("stopped");
		expect(Date.now() - stoppedAt).toBeLessThan(4_000);
		await expect(start).rejects.toBeInstanceOf(MinitriggerError);
	}, 20_000);

	it("a start() failing while creating workers closes their dedicated connections", async () => {
		const { connection, duplicates } = trackedConnection();
		const engine = makeEngine([simpleTask()], { redis: { connection } });
		vi.spyOn(Queue.prototype, "removeGlobalConcurrency").mockRejectedValue(
			new Error("injected failure")
		);

		await expect(engine.start()).rejects.toThrow(/injected failure/);
		// duplicates[0] is the subscriber (kept for a retry); the rest are worker connections.
		const workerConnections = duplicates.slice(1);
		expect(workerConnections.length).toBeGreaterThan(0);
		await waitFor(() => workerConnections.every(c => c.status === "end"), { timeoutMs: 3_000 });
	}, 20_000);

	it("a start() that stop() gave up on starts nothing afterwards (workers, outbox, leader)", async () => {
		let release!: () => void;
		const gate = new Promise<void>(resolve => {
			release = resolve;
		});
		let reached = false;
		vi.spyOn(Queue.prototype, "removeGlobalConcurrency").mockImplementation(async () => {
			reached = true;
			await gate;
			return undefined as never;
		});
		const workerRuns = vi.spyOn(Worker.prototype, "run");
		leaderConfigs.length = 0;
		outboxDispatchers.length = 0;
		const engine = makeEngine([simpleTask()]);
		const start = engine.start();
		start.catch(() => {});
		await waitFor(() => reached, { timeoutMs: 5_000 });
		const workersBefore = workerRuns.mock.calls.length; // the task queue's worker

		await engine.stop(); // gives up on start() after its bounded wait
		release();
		await expect(start).rejects.toThrow();
		await new Promise(resolve => setTimeout(resolve, 300));
		expect(workerRuns.mock.calls.length).toBe(workersBefore); // no -system worker
		expect(outboxDispatchers).toHaveLength(0);
		expect(leaderConfigs).toHaveLength(0);
	}, 30_000);

	it("runs.subscribe iterations sharing a discarded subscriber throw instead of lingering", async () => {
		const { connection, duplicates, control } = trackedConnection();
		const task = simpleTask();
		const engine = makeEngine([task], { redis: { connection } });
		const handle = await engine.trigger(task);

		control.hangNext = true;
		const start = engine.start();
		start.catch(() => {});
		await waitFor(() => duplicates.length >= 1, { timeoutMs: 5_000 });
		const iterator = engine.runs.subscribe(handle)[Symbol.asyncIterator]();
		const drained = (async () => {
			let next = await iterator.next();
			while (!next.done) next = await iterator.next();
			return "ended";
		})().catch((error: unknown) => error);
		await expect(start).rejects.toBeInstanceOf(MinitriggerError);
		const outcome = await Promise.race([
			drained,
			new Promise(resolve => setTimeout(() => resolve("lingering"), 3_000)),
		]);
		// Review 4 item 5: not a silent end (no terminal status was seen): it throws.
		expect(outcome).toBeInstanceOf(MinitriggerError);
		expect((outcome as Error).message).toMatch(/subscriber it used was discarded/);
		await iterator.return?.();
	}, 20_000);

	it("the leader client built from a connection string has the pool's connect timeout", async () => {
		leaderConfigs.length = 0;
		const engine = makeEngine([simpleTask()], { pool: { max: 6, connectionTimeoutMs: 4_321 } });
		await engine.start();
		expect(leaderConfigs).toHaveLength(1);
		const config = leaderConfigs[0] as LeaderElectorConfig;
		expect(config.clientConfig?.connectionTimeoutMillis).toBe(4_321);
		expect(config.clientConfig?.connectionString).toBe(ns.databaseUrl);
	});
});

describe("item 3: a stopping vs a stopped real engine", () => {
	it("while stop() drains, reads, runs.cancel and tokens.complete still work; trigger doesn't", async () => {
		let running = false;
		const hanging = defineTask({
			id: `hang-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async () => {
				running = true;
				await new Promise(() => {}); // ignores ctx.signal: stop() waits shutdownTimeoutMs
			},
		});
		const engine = makeEngine([hanging], { shutdownTimeoutMs: 1_500 });
		await engine.start();
		const handle = await engine.trigger(hanging);
		await waitFor(() => running, { timeoutMs: 5_000 });

		const stopping = engine.stop();
		expect((await engine.runs.get(handle.id))?.status).toBe("running");
		expect((await engine.runs.list()).items.map(r => r.id)).toContain(handle.id);
		expect((await engine.runs.logs(handle.id)).items).toEqual([]);
		expect(await engine.runs.steps(handle.id)).toEqual([]);
		expect(await engine.queues.list()).not.toHaveLength(0);
		expect(await engine.schedules.list()).toEqual([]);
		expect(await engine.tokens.complete(randomUUID())).toEqual({
			completed: false,
			reason: "not_found",
		});
		expect(await engine.runs.cancel(randomUUID())).toBe("not_found");
		await expect(engine.trigger(hanging)).rejects.toThrow(/this engine was stopped/);
		await stopping;
	});

	it("once stop() resolved, every method throws the lifecycle error", async () => {
		const task = simpleTask();
		const engine = makeEngine([task]);
		const handle = await engine.trigger(task);
		await engine.stop();
		const calls: Array<[string, () => Promise<unknown>]> = [
			["runs.get", () => engine.runs.get(handle.id)],
			["runs.list", () => engine.runs.list()],
			["runs.logs", () => engine.runs.logs(handle.id)],
			["runs.steps", () => engine.runs.steps(handle.id)],
			["runs.cancel", () => engine.runs.cancel(handle.id)],
			["tokens.complete", () => engine.tokens.complete(randomUUID())],
			["queues.list", () => engine.queues.list()],
			["schedules.list", () => engine.schedules.list()],
			["schedules.delete", () => engine.schedules.delete("x")],
			["schedules.upsert", () => engine.schedules.upsert({ id: "x", task, cron: "0 3 * * *" })],
		];
		for (const [name, call] of calls) {
			await expect(call(), name).rejects.toThrow(/this engine was stopped/);
		}
	});
});

describe("item 5: filter strings Postgres text can't hold", () => {
	it("the store surfaces the raw pg error (why the engine validates)", async () => {
		const store = createSqlStore(ns.pool, ns.schema);
		await expect(store.runs.list({ limit: 1, taskId: "a\u0000b" })).rejects.toMatchObject({
			code: "22021",
		});
	});

	it.each([
		["NUL", "a\u0000b"],
		["lone surrogate", "a\ud800b"],
	])("%s: runs.list / cancelMany / retryFailed throw ConfigError", async (_label, bad) => {
		const engine = makeEngine([simpleTask()]);
		await expect(engine.runs.list({ taskId: bad })).rejects.toBeInstanceOf(ConfigError);
		await expect(engine.runs.list({ status: [bad as "queued"] })).rejects.toBeInstanceOf(
			ConfigError
		);
		for (const key of ["taskId", "queue"] as const) {
			await expect(engine.runs.cancelMany({ [key]: bad, limit: 10 })).rejects.toBeInstanceOf(
				ConfigError
			);
			await expect(engine.runs.retryFailed({ [key]: bad, limit: 10 })).rejects.toBeInstanceOf(
				ConfigError
			);
		}
	});
});

describe("outbox dispatch (coordinator follow-up)", () => {
	type LogRecord = { level: string; message: string; fields: Record<string, unknown> };
	const logs: LogRecord[] = [];
	const capture: EngineLogger = {
		debug: (fields, message) => logs.push({ level: "debug", message, fields }),
		info: (fields, message) => logs.push({ level: "info", message, fields }),
		warn: (fields, message) => logs.push({ level: "warn", message, fields }),
		error: (fields, message) => logs.push({ level: "error", message, fields }),
	};

	/** A started engine whose outbox only dispatches when the test calls `dispatch`. */
	async function outboxEngine(task: AnyTask) {
		outboxDispatchers.length = 0;
		logs.length = 0;
		const engine = makeEngine([task], {
			logger: capture,
			outbox: { notify: false, pollIntervalMs: 600_000 },
		});
		await engine.start();
		const entry = outboxDispatchers[0];
		if (!entry) throw new Error("no outbox listener was created");
		// The listener's catch-up sweep on connect; after it, nothing dispatches on its own.
		await waitFor(() => entry.calls >= 1, { timeoutMs: 5_000 });
		return { engine, dispatch: entry.dispatch };
	}

	async function triggerInTx(engine: Engine, task: AnyTask, count: number): Promise<string[]> {
		const client = new Client({ connectionString: ns.databaseUrl });
		await client.connect();
		try {
			await client.query("BEGIN");
			const ids: string[] = [];
			for (let i = 0; i < count; i++) {
				ids.push((await engine.trigger(task, undefined as never, { tx: client })).id);
			}
			await client.query("COMMIT");
			return ids;
		} finally {
			await client.end();
		}
	}

	const pendingOf = async (id: string) =>
		(
			await ns.pool.query<{ outbox_pending: boolean }>(
				`SELECT outbox_pending FROM ${escapeIdentifier(ns.schema)}.runs WHERE id = $1`,
				[id]
			)
		).rows[0]?.outbox_pending;

	it("an aborted signal claims nothing", async () => {
		const task = simpleTask();
		const { engine, dispatch } = await outboxEngine(task);
		const [id] = await triggerInTx(engine, task, 1);
		const aborted = new AbortController();
		aborted.abort();
		expect(await dispatch(aborted.signal)).toBe(false);
		expect(await pendingOf(id as string)).toBe(true);

		await dispatch(new AbortController().signal);
		expect(await pendingOf(id as string)).toBe(false);
		await waitFor(async () => (await engine.runs.get(id as string))?.status === "completed", {
			timeoutMs: 5_000,
		});
	});

	it("failed enqueues log one throttled warn per batch, not one per run", async () => {
		const task = simpleTask();
		const { engine, dispatch } = await outboxEngine(task);
		const first = await triggerInTx(engine, task, 3);
		vi.spyOn(Queue.prototype, "add").mockRejectedValue(new Error("redis down"));

		await dispatch(new AbortController().signal);
		const outboxWarns = () =>
			logs.filter(l => l.level === "warn" && /outbox: enqueue/.test(l.message));
		expect(outboxWarns()).toHaveLength(1);
		const warn = outboxWarns()[0];
		expect(warn?.fields.failed).toBe(3);
		expect([...((warn?.fields.runIds as string[] | undefined) ?? [])].sort()).toEqual(
			[...first].sort()
		);
		expect((warn?.fields.err as Error | undefined)?.message).toBe("redis down");
		expect(logs.filter(l => /^enqueue failed/.test(l.message))).toHaveLength(0);
		// The claim happened: the reconciler re-enqueues these from Postgres.
		for (const id of first) expect(await pendingOf(id)).toBe(false);

		// Within the throttle window: no second warn.
		await triggerInTx(engine, task, 2);
		await dispatch(new AbortController().signal);
		expect(outboxWarns()).toHaveLength(1);
	});
});
