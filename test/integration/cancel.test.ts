/**
 * Phase 6: cross-process cancellation via pub/sub, and the run-status publishes Phase 7's
 * `runs.subscribe` will consume.
 */
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { createPubSub, type RunStatusMessage } from "../../src/pubsub.js";
import { defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);

const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const engines: Engine[] = [];
const cleanups: Array<() => Promise<void>> = [];

function makeEngine(tasks: EngineConfig["tasks"], overrides: Partial<EngineConfig> = {}): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 5 },
		// Heartbeats far apart: only the pub/sub message can abort the execution promptly.
		heartbeatIntervalMs: 20_000,
		stallTimeoutMs: 60_000,
		shutdownTimeoutMs: 1_000,
		logger: silent,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

const t = (table: string) => `${escapeIdentifier(ns.schema)}.${escapeIdentifier(table)}`;

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
	for (const cleanup of cleanups.splice(0)) await cleanup();
});

describe("cross-process cancel", () => {
	it("runs.cancel on engine B aborts engine A's execution; nothing is written afterwards", async () => {
		let startedResolve: () => void = () => {};
		const started = new Promise<void>(resolve => {
			startedResolve = resolve;
		});
		let abortReason: unknown;
		let lateWriteError: unknown;
		const task = defineTask({
			id: `long-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_payload: undefined, ctx) => {
				await ctx.step.run("before", () => "ok");
				startedResolve();
				await new Promise<void>(resolve => {
					ctx.signal.addEventListener("abort", () => resolve(), { once: true });
				});
				abortReason = ctx.signal.reason;
				try {
					await ctx.step.run("after-cancel", () => "must not be written");
				} catch (error) {
					lateWriteError = error;
				}
				return "must not complete";
			},
		});

		const a = makeEngine([task]);
		await a.start();
		// B never starts (decision 19): its cancel still reaches A through the shared publisher.
		const b = makeEngine([task]);
		const handle = await b.trigger(task);
		await started;

		const t0 = Date.now();
		expect(await b.runs.cancel(handle.id)).toBe("canceled");
		await waitFor(() => abortReason !== undefined, { timeoutMs: 3_000, intervalMs: 10 });
		expect(Date.now() - t0).toBeLessThan(3_000);
		expect((abortReason as Error).name).toBe("AbortError");
		expect((abortReason as Error).message).toMatch(/canceled/);

		await waitFor(() => lateWriteError !== undefined, { timeoutMs: 3_000 });
		// Give a zombie write every chance to land before checking.
		await new Promise(resolve => setTimeout(resolve, 200));
		const run = await a.runs.get(handle.id);
		expect(run?.status).toBe("canceled");
		expect(run?.output).toBeNull();
		const { rows } = await ns.pool.query<{ name: string; execution_id: string | null }>(
			`SELECT s.name, r.execution_id FROM ${t("steps")} s JOIN ${t("runs")} r ON r.id = s.run_id
			 WHERE s.run_id = $1`,
			[handle.id]
		);
		expect(rows.map(r => r.name)).toEqual(["before"]);
		expect(rows[0]?.execution_id).toBeNull();
	});
});

describe("run status publishes", () => {
	it("publishes queued -> running -> completed with DB updatedAt, best-effort", async () => {
		const subscriber = new Redis(ns.redisUrl);
		const publisher = new Redis(ns.redisUrl);
		const pubsub = createPubSub({ publisher, subscriber, prefix: ns.prefix, logger: silent });
		cleanups.push(async () => {
			await pubsub.close();
			subscriber.disconnect();
			publisher.disconnect();
		});
		await pubsub.ready;

		const task = defineTask({
			id: `quick-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async () => "done",
		});
		const engine = makeEngine([task]);
		await engine.start();

		const messages: RunStatusMessage[] = [];
		// Delayed, so "running" can only be published after we subscribed to the new run id.
		const handle = await engine.trigger(task, undefined, { delay: 500 });
		const unsubscribe = pubsub.subscribeStatus(handle.id, m => messages.push(m));
		cleanups.push(async () => unsubscribe());

		await waitFor(() => messages.some(m => m.status === "completed"), { timeoutMs: 5_000 });
		const run = await engine.runs.get(handle.id);
		const completed = messages.find(m => m.status === "completed");
		expect(completed?.updatedAt).toBe(run?.updatedAt.toISOString());
		// "queued" may be missed (published before we subscribed); the rest follows the DB clock.
		const statuses = messages.map(m => m.status).filter(s => s !== "queued");
		expect(statuses).toEqual(["running", "completed"]);
		const times = messages.map(m => Date.parse(m.updatedAt));
		expect([...times].sort((x, y) => x - y)).toEqual(times);
	});
});
