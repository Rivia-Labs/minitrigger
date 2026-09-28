import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createRedisConnections } from "../../src/queue/connections.js";
import { createTransport, type RunJobData, type Transport } from "../../src/queue/transport.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

function shortId(): string {
	return randomUUID().slice(0, 8);
}

function sleep(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

const created: Transport[] = [];

function makeTransport(
	queues: Array<{
		name: string;
		concurrency: number;
		globalConcurrency?: number;
		rateLimit?: { max: number; durationMs: number };
	}>
): Transport {
	const connections = createRedisConnections(ns.redisUrl);
	const t = createTransport({ prefix: ns.prefix, connections, queues });
	created.push(t);
	return t;
}

afterEach(async () => {
	await Promise.all(
		created.splice(0).map(t => t.closeWorkers({ timeoutMs: 2_000 }).catch(() => {}))
	);
});

describe("createTransport", () => {
	it("delivers an enqueued run job to the worker as {runId, seq}", async () => {
		const queue = `a-${shortId()}`;
		const t = makeTransport([{ name: queue, concurrency: 1 }]);
		const received: RunJobData[] = [];

		await t.startWorkers({
			processRun: async (_queue, data) => {
				received.push(data);
			},
			processSystem: async () => {},
		});

		const runId = randomUUID();
		await t.enqueueRun({ queue, runId, seq: 1 });

		await waitFor(() => received.length > 0, { timeoutMs: 3_000 });
		expect(received).toEqual([{ runId, seq: 1 }]);
	});

	it("adding the same jobId twice processes it only once", async () => {
		const queue = `a-${shortId()}`;
		const t = makeTransport([{ name: queue, concurrency: 1 }]);
		let count = 0;

		await t.startWorkers({
			processRun: async () => {
				count++;
			},
			processSystem: async () => {},
		});

		const runId = randomUUID();
		await t.enqueueRun({ queue, runId, seq: 1 });
		await t.enqueueRun({ queue, runId, seq: 1 });

		await waitFor(() => count > 0, { timeoutMs: 3_000 });
		await sleep(300);
		expect(count).toBe(1);
	});

	it("honors delayMs (~300ms)", async () => {
		const queue = `a-${shortId()}`;
		const t = makeTransport([{ name: queue, concurrency: 1 }]);
		const start = Date.now();
		let receivedAt: number | undefined;

		await t.startWorkers({
			processRun: async () => {
				receivedAt = Date.now();
			},
			processSystem: async () => {},
		});

		await t.enqueueRun({ queue, runId: randomUUID(), seq: 1, delayMs: 300 });

		await waitFor(() => receivedAt !== undefined, { timeoutMs: 3_000 });
		if (receivedAt === undefined)
			throw new Error("unreachable: waitFor guarantees receivedAt is set");
		expect(receivedAt - start).toBeGreaterThanOrEqual(250);
	});

	it("start({queues}) starts a worker for the listed queue and the system worker, but not others", async () => {
		const a = `a-${shortId()}`;
		const b = `b-${shortId()}`;
		const t = makeTransport([
			{ name: a, concurrency: 1 },
			{ name: b, concurrency: 1 },
		]);
		const receivedA: RunJobData[] = [];
		const receivedB: RunJobData[] = [];
		const receivedSystem: Array<{ name: string; data: unknown }> = [];

		await t.startWorkers({
			queues: [a],
			processRun: async (queue, data) => {
				(queue === a ? receivedA : receivedB).push(data);
			},
			processSystem: async (name, data) => {
				receivedSystem.push({ name, data });
			},
		});

		await t.enqueueRun({ queue: a, runId: randomUUID(), seq: 1 });
		await t.enqueueRun({ queue: b, runId: randomUUID(), seq: 1 });
		await t.enqueueSystem("retention", { tick: 1 });

		await waitFor(() => receivedA.length > 0, { timeoutMs: 3_000 });
		await waitFor(() => receivedSystem.length > 0, { timeoutMs: 3_000 });
		await sleep(400);
		expect(receivedB).toHaveLength(0);
	});

	it("respects local concurrency: never more than N jobs run simultaneously", async () => {
		const queue = `a-${shortId()}`;
		const t = makeTransport([{ name: queue, concurrency: 2 }]);
		let active = 0;
		let maxActive = 0;
		let completed = 0;

		await t.startWorkers({
			processRun: async () => {
				active++;
				maxActive = Math.max(maxActive, active);
				await sleep(150);
				active--;
				completed++;
			},
			processSystem: async () => {},
		});

		const runIds = Array.from({ length: 6 }, () => randomUUID());
		await Promise.all(runIds.map(runId => t.enqueueRun({ queue, runId, seq: 1 })));

		await waitFor(() => completed >= 6, { timeoutMs: 4_000 });
		expect(maxActive).toBeGreaterThan(0);
		expect(maxActive).toBeLessThanOrEqual(2);
	});

	it("respects rateLimit: caps jobs processed within the window", async () => {
		const queue = `a-${shortId()}`;
		const t = makeTransport([
			{ name: queue, concurrency: 5, rateLimit: { max: 2, durationMs: 1_000 } },
		]);
		const timestamps: number[] = [];
		const start = Date.now();

		await t.startWorkers({
			processRun: async () => {
				timestamps.push(Date.now() - start);
			},
			processSystem: async () => {},
		});

		const runIds = Array.from({ length: 4 }, () => randomUUID());
		await Promise.all(runIds.map(runId => t.enqueueRun({ queue, runId, seq: 1 })));

		await waitFor(() => timestamps.length >= 4, { timeoutMs: 4_000 });
		const withinFirstWindow = timestamps.filter(ts => ts < 900).length;
		expect(withinFirstWindow).toBeLessThanOrEqual(2);
	});

	it("closeWorkers waits for an active job up to the timeout and reports it as still active", async () => {
		const queue = `a-${shortId()}`;
		const t = makeTransport([{ name: queue, concurrency: 1 }]);
		let started = false;

		await t.startWorkers({
			processRun: async () => {
				started = true;
				await sleep(2_000);
			},
			processSystem: async () => {},
		});

		const runId = randomUUID();
		await t.enqueueRun({ queue, runId, seq: 1 });
		await waitFor(() => started, { timeoutMs: 3_000 });

		const result = await t.closeWorkers({ timeoutMs: 300 });
		expect(result.stillActive).toEqual([{ queue, jobId: `${runId}-1` }]);
	});

	it("closeWorkers reports nothing still active once the job finishes in time", async () => {
		const queue = `a-${shortId()}`;
		const t = makeTransport([{ name: queue, concurrency: 1 }]);

		await t.startWorkers({
			processRun: async () => {
				await sleep(20);
			},
			processSystem: async () => {},
		});

		await t.enqueueRun({ queue, runId: randomUUID(), seq: 1 });
		await sleep(100);

		const result = await t.closeWorkers({ timeoutMs: 2_000 });
		expect(result.stillActive).toEqual([]);
	});
});

describe("global concurrency", () => {
	it("a queue that no longer declares globalConcurrency drops the value an earlier start set", async () => {
		const queue = `g-${shortId()}`;
		const connection = new Redis(ns.redisUrl, { maxRetriesPerRequest: null });
		const raw = new Queue(queue, { connection, prefix: ns.prefix });
		try {
			const first = makeTransport([{ name: queue, concurrency: 1, globalConcurrency: 2 }]);
			await first.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
			expect(await raw.getGlobalConcurrency()).toBe(2);
			await first.closeWorkers({ timeoutMs: 1_000 });

			const second = makeTransport([{ name: queue, concurrency: 1 }]);
			await second.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
			expect(await raw.getGlobalConcurrency()).toBeNull();
		} finally {
			await raw.close();
			connection.disconnect();
		}
	});
});

describe("startWorkers failure", () => {
	it("a failed startWorkers leaves nothing running and can be retried", async () => {
		const queue = `f-${shortId()}`;
		const t = makeTransport([{ name: queue, concurrency: 1 }]);
		const received: RunJobData[] = [];
		const opts = {
			processRun: async (_q: string, data: RunJobData) => {
				received.push(data);
			},
			processSystem: async () => {},
		};
		await expect(t.startWorkers({ ...opts, queues: ["nope"] })).rejects.toThrow(/nope/);
		await t.startWorkers(opts);
		const runId = randomUUID();
		await t.enqueueRun({ queue, runId, seq: 1 });
		await waitFor(() => received.length > 0, { timeoutMs: 3_000 });
		expect(received).toEqual([{ runId, seq: 1 }]);
	});
});

describe("createRedisConnections ownership (decision 14)", () => {
	it("never closes an injected ioredis instance; dedicated() duplicates it instead", async () => {
		const injected = new Redis(ns.redisUrl);
		try {
			await injected.ping();
			const duplicateSpy = vi.spyOn(injected, "duplicate");

			const connections = createRedisConnections({ connection: injected });
			expect(connections.shared()).toBe(injected);

			const worker = connections.dedicated("worker");
			expect(duplicateSpy).toHaveBeenCalledTimes(1);
			expect(worker).not.toBe(injected);

			await connections.closeAll();

			expect(injected.status).toBe("ready");
			// The duplicate this call created should have been closed.
			await waitFor(() => worker.status === "end", { timeoutMs: 2_000 });
		} finally {
			injected.disconnect();
		}
	});

	it("a transport built on an injected instance leaves it open after closeWorkers", async () => {
		const injected = new Redis(ns.redisUrl);
		try {
			await injected.ping();
			const connections = createRedisConnections({ connection: injected });
			const queue = `a-${shortId()}`;
			const t = createTransport({
				prefix: ns.prefix,
				connections,
				queues: [{ name: queue, concurrency: 1 }],
			});

			await t.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
			await t.closeWorkers({ timeoutMs: 1_000 });

			expect(injected.status).toBe("ready");
		} finally {
			injected.disconnect();
		}
	});
});

describe("checkRedis (decision 22)", () => {
	it("resolves without a warning against a real Redis configured for noeviction", async () => {
		const t = makeTransport([{ name: `a-${shortId()}`, concurrency: 1 }]);
		await expect(t.checkRedis()).resolves.toBeUndefined();
	});
});
