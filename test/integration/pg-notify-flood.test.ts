/**
 * Review 5, item A5: the wake channel is untrusted (any role with CONNECT can NOTIFY on it). A
 * flood of NOTIFYs, each in its own transaction, must not turn into one claim per notification
 * (times the queues, for a payload no queue matches): passes triggered by `wake()` are spaced by a
 * minimum interval, and the engine keeps processing normal runs.
 *
 * Review 6 L4: the main bound is a rate over a sliding window (claims in any {@link WINDOW_MS}),
 * which does not grow with the elapsed time: a slow CI host that stretches the flood leaves it
 * unchanged, while without the limiter the claims of one window are bounded only by claim latency.
 */
import { randomUUID } from "node:crypto";
import { Client } from "pg";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { migrate } from "../../src/migrations/migrate.js";
import { channelName } from "../../src/pg/listener.js";
import { MIN_WAKE_INTERVAL_MS, WAKE_BURST } from "../../src/pg/workers.js";
import { defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const QUEUES = 3;
const FLOOD = 3_000;
/** Sliding window of the rate bound. */
const WINDOW_MS = 150;

/** Largest number of timestamps (ascending) that fall within any `windowMs` span. */
function maxInWindow(times: readonly number[], windowMs: number): number {
	let best = 0;
	let start = 0;
	for (let end = 0; end < times.length; end++) {
		while ((times[end] ?? 0) - (times[start] ?? 0) >= windowMs) start++;
		best = Math.max(best, end - start + 1);
	}
	return best;
}

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

		const claimTimes: number[] = [];
		/** Wall time of each whole claim transaction (BEGIN .. COMMIT/ROLLBACK). */
		const claimLatencies: number[] = [];
		const openTx = new WeakMap<Client, { begin: number; claim: boolean }>();
		const original = Client.prototype.query;
		vi.spyOn(Client.prototype, "query").mockImplementation(function (
			this: Client,
			...args: unknown[]
		) {
			const text = typeof args[0] === "string" ? args[0] : "";
			if (text === "BEGIN") openTx.set(this, { begin: performance.now(), claim: false });
			if (text.includes("WITH picked")) {
				claimTimes.push(performance.now());
				const tx = openTx.get(this);
				if (tx) tx.claim = true;
			}
			const result = (original as (...a: unknown[]) => unknown).apply(this, args);
			if (text === "COMMIT" || text === "ROLLBACK") {
				const tx = openTx.get(this);
				openTx.delete(this);
				if (tx?.claim && result instanceof Promise) {
					result.then(
						() => claimLatencies.push(performance.now() - tx.begin),
						() => {}
					);
				}
			}
			return result as never;
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

		const claims = claimTimes.length;
		const densest = maxInWindow(claimTimes, WINDOW_MS);
		const detail = `claims=${claims} densest=${densest}/${WINDOW_MS}ms elapsedMs=${elapsedMs}`;
		// Per queue, fruitless wake-triggered passes: a burst of WAKE_BURST, then one every
		// MIN_WAKE_INTERVAL_MS; +2 per queue for a pass in flight at each edge of the window.
		const windowBound = QUEUES * (WAKE_BURST + Math.ceil(WINDOW_MS / MIN_WAKE_INTERVAL_MS) + 2);
		// Review 6 fix round 1: the window bound only tells the limiter apart from its absence when
		// an unlimited loop could exceed it, i.e. when one claim takes less than WINDOW_MS / (per
		// queue bound). On a host where a claim transaction is that slow, the unlimited rate is
		// about the bound itself: the check is skipped (the rest stays as a smoke test) and the
		// limiter's timing is pinned by the fake-clock test in test/unit/pg-workers.test.ts.
		const perQueueBound = windowBound / QUEUES;
		const sorted = [...claimLatencies].sort((a, b) => a - b);
		const claimLatencyMs = sorted[Math.floor(sorted.length / 2)] ?? 0;
		if (claimLatencyMs * perQueueBound <= WINDOW_MS) {
			expect(densest, `${detail} claimLatencyMs=${claimLatencyMs.toFixed(2)}`).toBeLessThanOrEqual(
				windowBound
			);
		} else {
			console.warn(
				`pg-notify-flood: median claim latency ${claimLatencyMs.toFixed(2)} ms is too slow for ` +
					`the ${WINDOW_MS} ms window bound to tell the limiter apart; bound not asserted (${detail})`
			);
		}
		// And the same rate over the whole flood.
		const totalBound = QUEUES * (WAKE_BURST + elapsedMs / MIN_WAKE_INTERVAL_MS + 5);
		expect(claims, detail).toBeLessThanOrEqual(totalBound);

		// The engine still processes a normal run right away.
		const task = tasks[0];
		if (!task) throw new Error("no task");
		const { id } = await engine.trigger(task, undefined);
		await waitFor(async () => (await engine?.runs.get(id))?.status === "completed", {
			timeoutMs: 5_000,
		});
	});
});
