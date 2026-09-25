/**
 * Phase 6 / decision 1: Redis is only transport. After a `FLUSHALL` (every BullMQ job gone), runs
 * in every state still finish, recovered from Postgres by the reconciler.
 *
 * `FLUSHALL` wipes the WHOLE Redis, so this file lives in its own vitest project (`flushall`), run
 * after the others by `pnpm test` (`pnpm test:flushall` alone), and only against a local Redis
 * with `NODE_ENV=test` (vitest sets it unless it is already set).
 */
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { defineTask } from "../../src/task.js";
import { TEST_REDIS_URL } from "../helpers/env.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);
const redisHost = new URL(TEST_REDIS_URL).hostname;
const enabled = LOCAL_HOSTS.has(redisHost) && process.env["NODE_ENV"] === "test";
if (!enabled) {
	console.warn(
		`[flushall.test] skipped: needs a local Redis (got host "${redisHost}") and NODE_ENV=test ` +
			`(got ${JSON.stringify(process.env["NODE_ENV"])})`
	);
}

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);
const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };

let engine: Engine | undefined;

beforeAll(async () => {
	const migrator = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks: [],
	});
	await migrator.migrate();
	await migrator.stop();
});

afterAll(async () => {
	await engine?.stop();
});

describe("FLUSHALL", () => {
	(enabled ? it : it.skip)(
		"queued, sleeping, token-waiting and running runs all finish after Redis is wiped",
		async () => {
			const queue = `q${shortId()}`;
			let releaseRunning = false;
			const quick = defineTask({
				id: `quick-${shortId()}`,
				queue: { name: queue },
				run: async () => "queued-done",
			});
			const sleeper = defineTask({
				id: `sleeper-${shortId()}`,
				queue: { name: queue },
				run: async (_p: undefined, ctx) => {
					await ctx.step.sleep("nap", "1s");
					return "slept";
				},
			});
			const approval = defineTask({
				id: `approval-${shortId()}`,
				queue: { name: queue },
				run: async (_p: undefined, ctx) => {
					const result = await ctx.step.waitForToken<string>("approval");
					return result.ok ? result.output : "timed out";
				},
			});
			const busy = defineTask({
				id: `busy-${shortId()}`,
				queue: { name: queue, concurrency: 4 },
				run: async () => {
					await waitFor(() => releaseRunning, { timeoutMs: 20_000, intervalMs: 20 });
					return "ran";
				},
			});

			engine = createEngine({
				postgres: ns.databaseUrl,
				redis: ns.redisUrl,
				schema: ns.schema,
				prefix: ns.prefix,
				tasks: [quick, sleeper, approval, busy],
				heartbeatIntervalMs: 100,
				stallTimeoutMs: 2_000,
				shutdownTimeoutMs: 1_000,
				reconciler: { intervalMs: 200, graceMs: 200, batchSize: 50 },
				logger: silent,
			});
			await engine.start();

			const running = await engine.trigger(busy);
			const sleeping = await engine.trigger(sleeper);
			const waiting = await engine.trigger(approval);
			const queued = await engine.trigger(quick, undefined, { delay: 1_500 });

			const status = async (id: string) => (await engine?.runs.get(id))?.status;
			await waitFor(async () => (await status(running.id)) === "running");
			await waitFor(async () => (await status(sleeping.id)) === "waiting");
			await waitFor(async () => (await status(waiting.id)) === "waiting");
			expect(await status(queued.id)).toBe("queued");

			const admin = new Redis(TEST_REDIS_URL);
			try {
				await admin.flushall();
				expect(await admin.dbsize()).toBe(0);
			} finally {
				admin.disconnect();
			}

			const token = (
				await ns.pool.query<{ id: string }>(
					`SELECT id FROM "${ns.schema}".waitpoints WHERE run_id = $1`,
					[waiting.id]
				)
			).rows[0];
			if (!token) throw new Error("token not found");
			expect(await engine.tokens.complete(token.id, "approved")).toEqual({ completed: true });
			releaseRunning = true;

			const done = async (id: string) => {
				await waitFor(async () => (await status(id)) === "completed", { timeoutMs: 10_000 });
				return (await engine?.runs.get(id))?.output;
			};
			expect(await done(queued.id)).toBe("queued-done");
			expect(await done(sleeping.id)).toBe("slept");
			expect(await done(waiting.id)).toBe("approved");
			expect(await done(running.id)).toBe("ran");
		},
		30_000
	);

	(enabled ? it : it.skip)(
		"a debounced run keeps collapsing triggers and runs once after Redis is wiped (decision 24)",
		async () => {
			const seen: number[] = [];
			const task = defineTask({
				id: `debounced-${shortId()}`,
				queue: { name: `q${shortId()}` },
				run: async (payload: { n: number }) => {
					seen.push(payload.n);
					return payload.n;
				},
			});
			const debounced = createEngine({
				postgres: ns.databaseUrl,
				redis: ns.redisUrl,
				schema: ns.schema,
				prefix: ns.prefix,
				tasks: [task],
				shutdownTimeoutMs: 1_000,
				reconciler: { intervalMs: 200, graceMs: 200, batchSize: 50 },
				logger: silent,
			});
			try {
				await debounced.start();
				const opts = { debounce: { key: "course:1", delay: 1_000 } };
				const first = await debounced.trigger(task, { n: 1 }, opts);
				await debounced.trigger(task, { n: 2 }, opts);

				const admin = new Redis(TEST_REDIS_URL);
				try {
					await admin.flushall();
				} finally {
					admin.disconnect();
				}

				// The pending run and its key live in Postgres: this still collapses into it.
				const third = await debounced.trigger(task, { n: 3 }, opts);
				expect(third.id).toBe(first.id);
				const admin2 = new Redis(TEST_REDIS_URL);
				try {
					// Wipe again, so the only job left for the run is the reconciler's re-enqueue.
					await admin2.flushall();
				} finally {
					admin2.disconnect();
				}

				await waitFor(async () => (await debounced.runs.get(first.id))?.status === "completed", {
					timeoutMs: 10_000,
				});
				expect(seen).toEqual([3]);
				const runs = await debounced.runs.list({ taskId: task.id, limit: 10 });
				expect(runs.items).toHaveLength(1);
			} finally {
				await debounced.stop();
			}
		},
		30_000
	);
});
