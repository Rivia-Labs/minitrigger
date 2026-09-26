/**
 * Decision 27 and the batch run operations in test mode (`createTestEngine`): the same core code
 * as the real engine over the in-memory store/transport.
 */
import { afterEach, describe, expect, it } from "vitest";
import { ConfigError, defineTask, type EngineLogger } from "../../src/index.js";
import { createTestEngine, type TestEngine } from "../../src/testing/index.js";

const quiet: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const engines: TestEngine[] = [];

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

function engineWith(...tasks: Parameters<typeof createTestEngine>[0]["tasks"]): TestEngine {
	const engine = createTestEngine({ tasks, logger: quiet, clock: { start: 0 } });
	engines.push(engine);
	return engine;
}

const emails = defineTask({
	id: "send-email",
	queue: { name: "emails", concurrency: 3 },
	run: async (payload: { to: string }) => `sent to ${payload.to}`,
});

/** Set by the test that uses it: the task pauses its own queue right before sleeping. */
let pauseOwnQueue: () => Promise<unknown> = async () => {};
const napper = defineTask({
	id: "napper",
	queue: { name: "emails" },
	run: async (_p: undefined, ctx) => {
		await ctx.step.run("pause", async () => {
			await pauseOwnQueue();
			return null;
		});
		await ctx.step.sleep("nap", "1h");
		return "rested";
	},
});

const other = defineTask({
	id: "other",
	queue: { name: "other" },
	run: async () => "other done",
});

describe("test engine: queues", () => {
	it("a paused queue's runs are held by drain until resume; other queues keep running", async () => {
		const engine = engineWith(emails, other);
		await engine.queues.pause("emails");
		const held = await engine.trigger(emails, { to: "a@b.c" });
		const free = await engine.trigger(other);
		await engine.drain();
		expect((await engine.runs.get(held))?.status).toBe("queued");
		expect((await engine.runs.get(free))?.status).toBe("completed");

		const state = await engine.queues.resume("emails");
		expect(state).toMatchObject({ name: "emails", paused: false });
		expect(await engine.drive(held)).toMatchObject({
			status: "completed",
			output: "sent to a@b.c",
		});
	});

	it("a delayed job of a paused queue never moves the clock (no endless drain)", async () => {
		const engine = engineWith(emails, napper);
		pauseOwnQueue = () => engine.queues.pause("emails");
		const run = await engine.trigger(napper);
		// autoAdvance: the sleep's wake-up job is held, so drain returns instead of looping.
		await engine.drain();
		expect((await engine.runs.get(run))?.status).toBe("waiting");
		expect(engine.clock.now().getTime()).toBe(0);
		await engine.queues.resume("emails");
		await engine.drain();
		expect(await engine.drive(run)).toMatchObject({ status: "completed", output: "rested" });
	});

	it("list reports declared queues and persisted state; scale needs a worker", async () => {
		const engine = engineWith(emails, other);
		await engine.queues.pause("other");
		const listed = await engine.queues.list();
		expect(listed).toEqual([
			{
				name: "emails",
				paused: false,
				updatedAt: null,
				declared: true,
				declaredConcurrency: 3,
				concurrency: null,
				activeLocal: 0,
			},
			{
				name: "other",
				paused: true,
				updatedAt: new Date(0),
				declared: true,
				declaredConcurrency: 10,
				concurrency: null,
				activeLocal: 0,
			},
		]);
		await expect(engine.queues.scale("emails", 2)).rejects.toThrow(ConfigError);
		await expect(engine.queues.pause("missing")).rejects.toThrow(/unknown queue/);
	});
});

describe("test engine: batch operations", () => {
	it("cancelMany cancels matching runs, retryFailed replays failed ones once", async () => {
		let fail = true;
		const flaky = defineTask({
			id: "flaky",
			queue: { name: "flaky" },
			retry: { maxAttempts: 1 },
			run: async (payload: { n: number }) => {
				if (fail) throw new Error("boom");
				return payload.n;
			},
		});
		const engine = engineWith(flaky, other);
		await engine.queues.pause("other");
		const pending = [await engine.trigger(other), await engine.trigger(other)];
		const res = await engine.runs.cancelMany({ queue: "other", limit: 10 });
		expect(new Set(res.canceled)).toEqual(new Set(pending.map(p => p.id)));
		expect(res.hasMore).toBe(false);

		const failed = await engine.runInline(flaky, { n: 7 });
		expect(failed.status).toBe("failed");
		fail = false;
		const retry = await engine.runs.retryFailed({ taskId: "flaky", limit: 10 });
		expect(retry.retried).toHaveLength(1);
		expect(retry.retried[0]?.runId).toBe(failed.runId);
		const newRunId = retry.retried[0]?.newRunId ?? "";
		expect(await engine.drive(newRunId)).toMatchObject({ status: "completed", output: 7 });
		expect((await engine.runs.retryFailed({ taskId: "flaky", limit: 10 })).retried).toEqual([]);
	});

	// Item 7: same fix as the real engine (shared `createEngineCore`) -- queues.pause/resume/scale
	// and runs.cancelMany used to skip the stopped check that trigger/replay/retryFailed already had.
	it("queues.pause/resume/scale and runs.cancelMany reject after stop(), like other ops", async () => {
		const engine = engineWith(other);
		await engine.stop();
		await expect(engine.queues.pause("other")).rejects.toThrow(/stopped/);
		await expect(engine.queues.resume("other")).rejects.toThrow(/stopped/);
		await expect(engine.queues.scale("other", 1)).rejects.toThrow(/stopped/);
		await expect(engine.runs.cancelMany({ limit: 10 })).rejects.toThrow(/stopped/);
	});
});
