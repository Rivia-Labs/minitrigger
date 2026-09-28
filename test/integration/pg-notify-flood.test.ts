/**
 * Review 5, item A5: the wake channel is untrusted (any role with CONNECT can NOTIFY on it). A
 * flood of NOTIFYs, each in its own transaction, must not turn into one claim per notification
 * (times the queues, for a payload no queue matches): passes triggered by `wake()` are spaced by a
 * minimum interval, and the engine keeps processing normal runs.
 */
import { randomUUID } from "node:crypto";
import { Client } from "pg";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { migrate } from "../../src/migrations/migrate.js";
import { channelName } from "../../src/pg/listener.js";
import { defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const QUEUES = 3;
const FLOOD = 3_000;

let engine: Engine | undefined;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
});

afterEach(async () => {
	vi.restoreAllMocks();
	await engine?.stop();
	engine = undefined;
});

describe("NOTIFY flood on the wake channel (A5)", () => {
	it("bounds the claims it causes and keeps processing runs", async () => {
		const suffix = randomUUID().slice(0, 8);
		const tasks = Array.from({ length: QUEUES }, (_, i) =>
			defineTask({
				id: `flood-${i}-${suffix}`,
				queue: { name: `flood${i}_${suffix}`, concurrency: 2 },
				run: async () => "ok",
			})
		);
		engine = createEngine({
			transport: "postgres",
			postgres: ns.databaseUrl,
			schema: ns.schema,
			tasks,
			pool: { max: 20 },
			// Long poll: every claim below comes from a wake-up, not the timer.
			polling: { intervalMs: 30_000, jitterMs: 0 },
			shutdownTimeoutMs: 1_000,
			logger: silent,
		});
		await engine.start();
		// Let the startup passes settle.
		await new Promise(resolve => setTimeout(resolve, 300));

		let claims = 0;
		const original = Client.prototype.query;
		vi.spyOn(Client.prototype, "query").mockImplementation(function (
			this: Client,
			...args: unknown[]
		) {
			if (typeof args[0] === "string" && args[0].includes("WITH picked")) claims++;
			return (original as (...a: unknown[]) => unknown).apply(this, args) as never;
		});

		const flooder = new Client({ connectionString: ns.databaseUrl });
		await flooder.connect();
		const channel = channelName("wake", ns.schema);
		const started = Date.now();
		try {
			// Separate transactions (autocommit): Postgres cannot fold them into one delivery.
			for (let i = 0; i < FLOOD; i++) {
				await (original as (...a: unknown[]) => Promise<unknown>).call(
					flooder,
					"SELECT pg_notify($1, $2)",
					[channel, "forged"]
				);
			}
		} finally {
			await flooder.end();
		}
		// Deliveries still queued in the listener, and the collapsed trailing passes.
		await new Promise(resolve => setTimeout(resolve, 300));
		const elapsedMs = Date.now() - started;

		// At most one wake-triggered pass per queue every 10 ms (the minimum interval is 10-20 ms),
		// plus slack for the passes in flight at the edges.
		const bound = QUEUES * (elapsedMs / 10 + 5);
		expect(claims, `claims=${claims} elapsedMs=${elapsedMs}`).toBeLessThanOrEqual(bound);

		// The engine still processes a normal run right away.
		const task = tasks[0];
		if (!task) throw new Error("no task");
		const { id } = await engine.trigger(task, undefined);
		await waitFor(async () => (await engine?.runs.get(id))?.status === "completed", {
			timeoutMs: 5_000,
		});
	});
});
