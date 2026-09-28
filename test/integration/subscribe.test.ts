/**
 * Phase 7: `engine.runs.subscribe(runId)` (pub/sub + Postgres, mandatory order) from a
 * trigger-only engine.
 */
import { randomUUID } from "node:crypto";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, type Engine, type RunStatusUpdate } from "../../src/engine.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import {
	describeTransports,
	type EngineOverrides,
	engineConfigFor,
} from "../helpers/engine-factory.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

describeTransports("subscribe", transport => {
	const ns = useTestNamespace();
	const shortId = () => randomUUID().slice(0, 8);
	const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
	const engines: Engine[] = [];

	function makeEngine(tasks: AnyTask[], overrides: EngineOverrides = {}): Engine {
		const engine = createEngine(
			engineConfigFor(transport, ns, {
				tasks,
				pool: { max: 5 },
				heartbeatIntervalMs: 1_000,
				stallTimeoutMs: 10_000,
				shutdownTimeoutMs: 2_000,
				logger: silent,
				...overrides,
			})
		);
		engines.push(engine);
		return engine;
	}

	async function collect(iterable: AsyncIterable<RunStatusUpdate>): Promise<RunStatusUpdate[]> {
		const updates: RunStatusUpdate[] = [];
		for await (const update of iterable) updates.push(update);
		return updates;
	}

	function gate(): { open: () => void; wait: Promise<void> } {
		let open: () => void = () => {};
		const wait = new Promise<void>(resolve => {
			open = resolve;
		});
		return { open, wait };
	}

	beforeAll(async () => {
		const engine = createEngine(
			engineConfigFor(transport, ns, {
				tasks: [],
			})
		);
		await engine.migrate();
		await engine.stop();
	});

	afterEach(async () => {
		await Promise.all(engines.splice(0).map(e => e.stop()));
	});

	describe("runs.subscribe", () => {
		it("receives queued -> running -> waiting -> running -> completed across a sleep", async () => {
			const task = defineTask({
				id: `sleepy-${shortId()}`,
				queue: { name: `q${shortId()}` },
				run: async (_payload: undefined, ctx) => {
					await ctx.step.run("before", () => 1);
					await ctx.step.sleep("nap", 300);
					return "ok";
				},
			});
			const worker = makeEngine([task]);
			await worker.start();
			// Subscribing from an engine that never starts (decision 19).
			const client = makeEngine([task]);
			// Delayed, so the subscription's first read is deterministically `queued`.
			const handle = await client.trigger(task, undefined, { delay: 400 });
			const updates = await collect(client.runs.subscribe(handle.id));

			const full = ["queued", "running", "waiting", "running", "completed"];
			const statuses = updates.map(u => u.status);
			if (transport === "bullmq") {
				expect(statuses).toEqual(full);
			} else {
				// Postgres mode (decision 28(d), spec §3.7): a status NOTIFY carries only the run id,
				// deduplicated per 5 ms window, and the subscriber re-reads the row -- transitions a
				// few ms apart (e.g. the resumed `running` and `completed`) can collapse into the
				// latest one. Still ordered, starting from the first read and ending terminal.
				expect(statuses[0]).toBe("queued");
				expect(statuses.at(-1)).toBe("completed");
				let cursor = 0;
				for (const status of statuses) {
					const at = full.indexOf(status, cursor);
					expect(at, `${statuses.join(" -> ")} is not a subsequence`).toBeGreaterThanOrEqual(0);
					cursor = at + 1;
				}
			}
			for (let i = 1; i < updates.length; i++) {
				expect(updates[i]?.updatedAt.getTime()).toBeGreaterThanOrEqual(
					updates[i - 1]?.updatedAt.getTime() ?? 0
				);
			}
			expect(updates.every(u => u.runId === handle.id)).toBe(true);
			const run = await client.runs.get(handle.id);
			expect(updates.at(-1)?.updatedAt.getTime()).toBe(run?.updatedAt.getTime());
		});

		it("ends with the terminal status when the run finishes during the subscription", async () => {
			const release = gate();
			const started = gate();
			const task = defineTask({
				id: `gated-${shortId()}`,
				queue: { name: `q${shortId()}` },
				run: async () => {
					started.open();
					await release.wait;
					return 42;
				},
			});
			const worker = makeEngine([task]);
			await worker.start();
			const client = makeEngine([task]);
			const handle = await client.trigger(task);
			await started.wait;

			const received: RunStatusUpdate[] = [];
			const done = (async () => {
				for await (const update of client.runs.subscribe(handle.id)) received.push(update);
			})();
			await waitFor(() => received.length > 0, { timeoutMs: 5_000, intervalMs: 10 });
			expect(received[0]?.status).toBe("running");
			release.open();
			await done;
			expect(received.map(u => u.status)).toEqual(["running", "completed"]);
		});

		it("emits the terminal status and ends for an already finished run", async () => {
			const task = defineTask({
				id: `quick-${shortId()}`,
				queue: { name: `q${shortId()}` },
				run: async () => "fast",
			});
			const worker = makeEngine([task]);
			await worker.start();
			const handle = await worker.trigger(task);
			await waitFor(async () => (await worker.runs.get(handle.id))?.status === "completed", {
				timeoutMs: 10_000,
				intervalMs: 25,
			});
			const client = makeEngine([task]);
			const updates = await collect(client.runs.subscribe(handle.id));
			expect(updates.map(u => u.status)).toEqual(["completed"]);

			// Unknown runs end without emitting anything.
			expect(await collect(client.runs.subscribe(randomUUID()))).toEqual([]);
		});

		it("stops on break, on AbortSignal and on engine.stop()", async () => {
			const task = defineTask({
				id: `idle-${shortId()}`,
				queue: { name: `q${shortId()}` },
				run: async () => "never runs",
			});
			// Nobody executes: the run stays queued forever.
			const client = makeEngine([task]);
			const handle = await client.trigger(task);

			for await (const update of client.runs.subscribe(handle.id)) {
				expect(update.status).toBe("queued");
				break;
			}

			const controller = new AbortController();
			const aborted = collect(client.runs.subscribe(handle.id, { signal: controller.signal }));
			await new Promise(resolve => setTimeout(resolve, 100));
			controller.abort();
			expect((await aborted).map(u => u.status)).toEqual(["queued"]);

			const stopped = collect(client.runs.subscribe(handle.id));
			await new Promise(resolve => setTimeout(resolve, 100));
			await client.stop();
			expect((await stopped).map(u => u.status)).toEqual(["queued"]);
			expect(() => client.runs.subscribe(handle.id)).toThrow(/stopped/);
		});
	});
});
