/**
 * Audit items 10 and (d):
 * - `enqueueRun`/`removeRunJob` must work for a task queue this process doesn't declare (rolling
 *   deploy: the old leader's reconciler recovers runs of a queue only the new code knows about);
 * - BullMQ `Queue`/`Worker` `error` events are logged through the injected logger instead of being
 *   silenced.
 */
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { afterEach, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { ConfigError } from "../../src/errors.js";
import { createRedisConnections } from "../../src/queue/connections.js";
import { runJobId } from "../../src/queue/ids.js";
import { createTransport, type Transport } from "../../src/queue/transport.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();

type LogRecord = { level: string; message: string; fields: Record<string, unknown> };
function makeLogger(): { logger: EngineLogger; logs: LogRecord[] } {
	const logs: LogRecord[] = [];
	const push = (level: string) => (fields: Record<string, unknown>, message: string) =>
		logs.push({ level, message, fields });
	return {
		logger: { debug: push("debug"), info: push("info"), warn: push("warn"), error: push("error") },
		logs,
	};
}

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	await Promise.all(cleanups.splice(0).map(fn => fn().catch(() => {})));
});

function makeTransport(redisUrl = ns.redisUrl, logger?: EngineLogger): Transport {
	const t = createTransport({
		prefix: ns.prefix,
		connections: createRedisConnections(redisUrl),
		queues: [{ name: "known", concurrency: 1 }],
		...(logger ? { logger } : {}),
	});
	cleanups.push(async () => {
		await t.closeWorkers({ timeoutMs: 500 });
	});
	return t;
}

function rawQueue(name: string): Queue {
	const connection = new Redis(ns.redisUrl, { maxRetriesPerRequest: null });
	const queue = new Queue(name, { connection, prefix: ns.prefix });
	cleanups.push(async () => {
		await queue.close();
		connection.disconnect();
	});
	return queue;
}

describe("transport: queues this process doesn't declare (audit item 10)", () => {
	it("enqueueRun adds the job to the named queue under the same prefix", async () => {
		const t = makeTransport();
		await t.enqueueRun({ queue: "from-new-deploy", runId: "11111111-run", seq: 3 });
		const job = await rawQueue("from-new-deploy").getJob(runJobId("11111111-run", 3));
		expect(job?.data).toEqual({ runId: "11111111-run", seq: 3 });
		expect(job?.opts.attempts).toBe(1);
	});

	it("removeRunJob removes a job from an undeclared queue", async () => {
		const t = makeTransport();
		await t.enqueueRun({ queue: "other", runId: "22222222-run", seq: 1, delayMs: 60_000 });
		const q = rawQueue("other");
		expect(await q.getJob(runJobId("22222222-run", 1))).toBeDefined();
		await t.removeRunJob("other", "22222222-run", 1);
		expect(await q.getJob(runJobId("22222222-run", 1))).toBeUndefined();
	});

	it("still rejects an invalid queue name", async () => {
		const t = makeTransport();
		await expect(t.enqueueRun({ queue: "bad:name", runId: "r", seq: 1 })).rejects.toThrow(
			ConfigError
		);
	});
});

describe("transport: BullMQ error events are logged (audit item d)", () => {
	it("logs Queue connection errors through the injected logger", async () => {
		const { logger, logs } = makeLogger();
		// Nothing listens on port 1: every connection attempt fails and BullMQ emits `error`.
		const t = makeTransport("redis://127.0.0.1:1", logger);
		void t.enqueueSystem("noop", {}).catch(() => {});
		const deadline = Date.now() + 5_000;
		while (Date.now() < deadline && !logs.some(l => l.level === "warn")) {
			await new Promise(resolve => setTimeout(resolve, 50));
		}
		const warn = logs.find(l => l.level === "warn");
		expect(warn?.message).toMatch(/BullMQ/);
		expect(warn?.fields["err"]).toBeInstanceOf(Error);
	});
});

describe("transport: closeWorkers with Redis down (fast stop)", () => {
	it("returns promptly instead of waiting out the worker close grace", async () => {
		const { logger } = makeLogger();
		const t = createTransport({
			prefix: ns.prefix,
			connections: createRedisConnections("redis://127.0.0.1:1"),
			queues: [{ name: "known", concurrency: 1 }],
			logger,
		});
		// startWorkers itself awaits a Redis command (global concurrency): don't wait for it.
		void t
			.startWorkers({ processRun: async () => {}, processSystem: async () => {} })
			.catch(() => {});
		await new Promise(resolve => setTimeout(resolve, 100));
		const started = Date.now();
		await t.closeWorkers({ timeoutMs: 200 });
		expect(Date.now() - started).toBeLessThan(1_500);
	});
});
