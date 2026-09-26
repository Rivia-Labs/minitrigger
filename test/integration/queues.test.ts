/**
 * Decision 27: queue pause/resume across processes (Postgres truth, BullMQ queue-level pause,
 * re-applied on start() and by the leader), `queues.list()` and the local `queues.scale()`.
 */
import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { escapeIdentifier } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type {
	BullmqTransportConfig,
	CommonConfig,
	EngineConfig,
	EngineLogger,
} from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { ConfigError } from "../../src/errors.js";
import { defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const engines: Engine[] = [];
let raw: Redis;
const rawQueues: Array<{ queue: Queue; connection: Redis }> = [];

function makeEngine(
	tasks: EngineConfig["tasks"],
	overrides: Partial<CommonConfig> & Partial<BullmqTransportConfig> = {}
): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 8 },
		shutdownTimeoutMs: 1_000,
		logger: silent,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

/** A raw BullMQ handle on a task queue, to read/tamper with the Redis flag directly. */
function bullQueue(name: string): Queue {
	const connection = raw.duplicate();
	const queue = new Queue(name, { connection, prefix: ns.prefix });
	rawQueues.push({ queue, connection });
	return queue;
}

async function statusOf(engine: Engine, runId: string) {
	return (await engine.runs.get(runId))?.status;
}

const t = (table: string) => `${escapeIdentifier(ns.schema)}.${escapeIdentifier(table)}`;

beforeAll(async () => {
	raw = new Redis(ns.redisUrl, { maxRetriesPerRequest: null });
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

afterAll(async () => {
	await Promise.all(
		rawQueues.splice(0).map(async ({ queue, connection }) => {
			await queue.close();
			connection.disconnect();
		})
	);
	raw.disconnect();
});

function quickTask(queue: string, concurrency = 2) {
	return defineTask({
		id: `quick-${shortId()}`,
		queue: { name: queue, concurrency },
		run: async () => "done",
	});
}

describe("queues.pause / resume across processes", () => {
	it("a pause from an admin engine stops a started engine from starting new runs; resume lets them run", async () => {
		const queue = `q${shortId()}`;
		const task = quickTask(queue);
		const worker = makeEngine([task]);
		await worker.start();
		// Decision 19: the admin engine never starts.
		const admin = makeEngine([task]);

		const before = await admin.trigger(task);
		await waitFor(async () => (await statusOf(admin, before.id)) === "completed");

		const state = await admin.queues.pause(queue);
		expect(state).toMatchObject({ name: queue, paused: true });
		expect(state.updatedAt).toBeInstanceOf(Date);
		expect(await bullQueue(queue).isPaused()).toBe(true);

		const held = await admin.trigger(task);
		await sleep(1_000);
		expect(await statusOf(admin, held.id)).toBe("queued");

		const resumed = await admin.queues.resume(queue);
		expect(resumed.paused).toBe(false);
		await waitFor(async () => (await statusOf(admin, held.id)) === "completed");
	});

	it("pausing does not abort a running execution; it finishes, the next run waits", async () => {
		const queue = `q${shortId()}`;
		let release: () => void = () => {};
		const gate = new Promise<void>(resolve => {
			release = resolve;
		});
		let abortedBeforeReturn: boolean | undefined;
		let started = false;
		const task = defineTask({
			id: `slow-${shortId()}`,
			queue: { name: queue },
			run: async (_p: undefined, ctx) => {
				started = true;
				await gate;
				abortedBeforeReturn = ctx.signal.aborted;
				return "finished";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const running = await engine.trigger(task);
		await waitFor(() => started);

		await engine.queues.pause(queue);
		const listed = (await engine.queues.list()).find(q => q.name === queue);
		expect(listed).toMatchObject({ paused: true, activeLocal: 1, declared: true });
		const next = await engine.trigger(task);
		release();

		await waitFor(async () => (await statusOf(engine, running.id)) === "completed");
		expect(abortedBeforeReturn).toBe(false);
		await sleep(500);
		expect(await statusOf(engine, next.id)).toBe("queued");
		await engine.queues.resume(queue);
		await waitFor(async () => (await statusOf(engine, next.id)) === "completed");
	});

	it("a restarted process honors the persisted pause even when Redis lost the flag", async () => {
		const queue = `q${shortId()}`;
		const task = quickTask(queue);
		const first = makeEngine([task]);
		await first.queues.pause(queue);
		await first.stop();

		// Redis forgot the pause (FLUSHALL, or a racing resume from outside minitrigger).
		await bullQueue(queue).resume();
		const admin = makeEngine([task]);
		const held = await admin.trigger(task);

		const second = makeEngine([task]);
		await second.start();
		expect(await bullQueue(queue).isPaused()).toBe(true);
		await sleep(800);
		expect(await statusOf(second, held.id)).toBe("queued");

		await second.queues.resume(queue);
		await waitFor(async () => (await statusOf(second, held.id)) === "completed");
	});

	it("the leader's reconciler tick re-applies the Postgres state to Redis", async () => {
		const queue = `q${shortId()}`;
		const task = quickTask(queue);
		const engine = makeEngine([task], { reconciler: { intervalMs: 200 } });
		await engine.start();
		await engine.queues.pause(queue);

		const bull = bullQueue(queue);
		await bull.resume();
		await waitFor(() => bull.isPaused(), { timeoutMs: 5_000 });

		// And the other way round: Postgres says resumed, Redis was paused behind its back.
		await engine.queues.resume(queue);
		await bull.pause();
		await waitFor(async () => !(await bull.isPaused()), { timeoutMs: 5_000 });
	});

	it("validates the queue name and refuses unknown or internal queues", async () => {
		const queue = `q${shortId()}`;
		const engine = makeEngine([quickTask(queue)]);
		await expect(engine.queues.pause("nope")).rejects.toThrow(ConfigError);
		await expect(engine.queues.pause("nope")).rejects.toThrow(/unknown queue "nope"/);
		await expect(engine.queues.pause(`${ns.prefix}-system`)).rejects.toThrow(/system queue/);
		await expect(engine.queues.pause("bad:name")).rejects.toThrow(ConfigError);
		await expect(engine.queues.pause(42 as unknown as string)).rejects.toThrow(ConfigError);
		// A queue no longer declared but with a persisted state can still be resumed.
		await ns.pool.query(
			`INSERT INTO ${t("queue_state")} (queue, paused) VALUES ('retired_queue', true)`
		);
		expect((await engine.queues.resume("retired_queue")).paused).toBe(false);
		const listed = await engine.queues.list();
		expect(listed.find(q => q.name === "retired_queue")).toMatchObject({
			declared: false,
			declaredConcurrency: null,
			concurrency: null,
			paused: false,
		});
	});
});

describe("queues.list / scale", () => {
	it("lists declared and persisted queues with the local worker concurrency", async () => {
		const q1 = `a${shortId()}`;
		const q2 = `b${shortId()}`;
		const engine = makeEngine([quickTask(q1, 3), quickTask(q2, 2)]);
		const idle = await engine.queues.list();
		const mine = idle.filter(q => q.name === q1 || q.name === q2);
		expect(mine).toEqual([
			{
				name: q1,
				paused: false,
				updatedAt: null,
				declared: true,
				declaredConcurrency: 3,
				concurrency: null,
				activeLocal: 0,
			},
			{
				name: q2,
				paused: false,
				updatedAt: null,
				declared: true,
				declaredConcurrency: 2,
				concurrency: null,
				activeLocal: 0,
			},
		]);
		expect(idle.some(q => q.name.endsWith("-system"))).toBe(false);

		await engine.start({ queues: [q1] });
		const started = await engine.queues.list();
		expect(started.find(q => q.name === q1)?.concurrency).toBe(3);
		expect(started.find(q => q.name === q2)?.concurrency).toBeNull();
		await expect(engine.queues.scale(q2, 4)).rejects.toThrow(/runs no worker/);
		await expect(engine.queues.scale(q1, 0)).rejects.toThrow(ConfigError);
		await expect(engine.queues.scale(q1, 1.5)).rejects.toThrow(ConfigError);
		// Item 6: bounded so a typo (or a computed value) can't hand BullMQ an absurd concurrency.
		await expect(engine.queues.scale(q1, 1_001)).rejects.toThrow(ConfigError);
		await expect(engine.queues.scale(q1, 1_001)).rejects.toThrow(/1.?000/);
		await expect(engine.queues.scale(q1, 1_000)).resolves.toBeUndefined();
	});

	it("scale changes how many runs this process executes at once", async () => {
		const queue = `q${shortId()}`;
		let running = 0;
		let peak = 0;
		let open = false;
		const task = defineTask({
			id: `conc-${shortId()}`,
			queue: { name: queue, concurrency: 1 },
			run: async () => {
				running++;
				peak = Math.max(peak, running);
				await waitFor(() => open, { timeoutMs: 10_000 });
				running--;
				return "ok";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		await engine.queues.scale(queue, 3);
		expect((await engine.queues.list()).find(q => q.name === queue)?.concurrency).toBe(3);
		const handles = await Promise.all([1, 2, 3].map(() => engine.trigger(task)));
		await waitFor(() => running === 3, { timeoutMs: 5_000 });
		open = true;
		for (const h of handles) {
			await waitFor(async () => (await statusOf(engine, h.id)) === "completed");
		}
		expect(peak).toBe(3);
	});
});
