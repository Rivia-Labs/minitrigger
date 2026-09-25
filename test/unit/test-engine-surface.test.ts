/**
 * Phase 9.2: the rest of the test engine's surface (same as the real engine where meaningful) and
 * the in-memory transport's BullMQ-like semantics. No infra.
 */
import { afterEach, describe, expect, it } from "vitest";
import {
	ConfigError,
	defineTask,
	type EngineLogger,
	MinitriggerError,
	type RunEvent,
	type RunStatusUpdate,
} from "../../src/index.js";
import {
	createTestEngine,
	type TestEngine,
	type TestEngineConfig,
} from "../../src/testing/index.js";
import { createMemoryTransport } from "../../src/testing/memory-transport.js";

const logged: { level: string; message: string }[] = [];
const captureLogger: EngineLogger = {
	debug: () => {},
	info: (_f, message) => logged.push({ level: "info", message }),
	warn: (_f, message) => logged.push({ level: "warn", message }),
	error: (_f, message) => logged.push({ level: "error", message }),
};

const engines: TestEngine[] = [];
function testEngine(config: TestEngineConfig): TestEngine {
	const engine = createTestEngine({ logger: captureLogger, ...config });
	engines.push(engine);
	return engine;
}

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
	const errors = logged.filter(l => l.level === "error");
	logged.length = 0;
	// Nothing in these scenarios is an engine-level error (e.g. a failing `-system` job).
	expect(errors).toEqual([]);
});

const START = new Date("2026-03-01T10:00:00.000Z");

describe("test engine: public surface", () => {
	it("trigger only enqueues; drain runs it; runs.get/list/logs work", async () => {
		const hello = defineTask({
			id: "hello",
			run: async (name: string, ctx) => {
				ctx.logger.info("greeting", { name });
				return `hello ${name}`;
			},
		});
		const engine = testEngine({ tasks: [hello], clock: { start: START } });

		const handle = await engine.trigger(hello, "ada");
		expect((await engine.runs.get(handle))?.status).toBe("queued");

		await engine.drain();
		const run = await engine.runs.get(handle);
		expect(run).toMatchObject({ status: "completed", output: "hello ada", attempt: 1 });
		expect(run?.createdAt).toEqual(START);

		const logs = await engine.runs.logs(handle);
		expect(logs.items.map(l => [l.level, l.message, l.data])).toEqual([
			["info", "greeting", { name: "ada" }],
		]);
		const list = await engine.runs.list({ taskId: "hello" });
		expect(list.items.map(r => r.id)).toEqual([handle.id]);
	});

	it("emits engine.events and runs.subscribe updates", async () => {
		const napper = defineTask({
			id: "napper",
			run: async (_p: null, ctx) => {
				await ctx.step.sleep("nap", "10m");
				return 1;
			},
		});
		const engine = testEngine({ tasks: [napper] });
		const events: string[] = [];
		for (const name of [
			"run:queued",
			"run:started",
			"run:waiting",
			"run:completed",
		] as RunEvent["name"][]) {
			engine.events.on(name, e => {
				events.push(e.name);
			});
		}

		const handle = await engine.trigger(napper, null);
		const updates: RunStatusUpdate[] = [];
		const collecting = (async () => {
			for await (const update of engine.runs.subscribe(handle)) updates.push(update);
		})();
		await engine.drain();
		await collecting;

		expect(events).toEqual([
			"run:queued",
			"run:started",
			"run:waiting",
			"run:started",
			"run:completed",
		]);
		expect(updates.at(-1)?.status).toBe("completed");
	});

	it("idempotency keys return the existing run", async () => {
		const once = defineTask({ id: "once", run: async (_p: null) => "x" });
		const engine = testEngine({ tasks: [once] });

		const a = await engine.trigger(once, null, { idempotencyKey: "k1" });
		const b = await engine.trigger(once, null, { idempotencyKey: "k1" });
		expect(b.id).toBe(a.id);
	});

	it("delayed triggers auto-advance the clock", async () => {
		const later = defineTask({ id: "later", run: async (_p: null) => "done" });
		const engine = testEngine({ tasks: [later], clock: { start: START } });

		const result = await engine.runInline(later, null, { delay: "30m" });

		expect(result.status).toBe("completed");
		expect(engine.clock.now().getTime() - START.getTime()).toBe(30 * 60_000);
	});

	it("cancels a waiting run (and its pending children)", async () => {
		const child = defineTask({
			id: "slow-child",
			run: async (_p: null, ctx) => {
				await ctx.step.createToken("never");
				await ctx.step.waitForToken("never");
				return 1;
			},
		});
		const parent = defineTask({
			id: "cancel-parent",
			run: async (_p: null, ctx) => {
				await ctx.step.triggerAndWait("child", child, null);
				return 2;
			},
		});
		const engine = testEngine({ tasks: [child, parent] });

		const first = await engine.runInline(parent, null);
		expect(first.status).toBe("waiting");

		expect(await engine.runs.cancel(first.runId)).toBe("canceled");
		expect((await engine.drive(first.runId)).status).toBe("canceled");
		const [childRun] = (await engine.runs.list({ taskId: "slow-child" })).items;
		expect(childRun?.status).toBe("canceled");
	});

	it("a waitpoint expiry due before a child's sleep fires first (time order)", async () => {
		const sleepy = defineTask({
			id: "sleepy-child",
			run: async (_p: null, ctx) => {
				await ctx.step.sleep("long", "2h");
				return "late";
			},
		});
		const parent = defineTask({
			id: "impatient",
			run: async (_p: null, ctx) => {
				const r = await ctx.step.triggerAndWait("child", sleepy, null, { timeout: "1h" });
				return r.ok ? "child finished" : r.error.name;
			},
		});
		const engine = testEngine({ tasks: [sleepy, parent], clock: { start: START } });

		const result = await engine.runInline(parent, null);

		expect(result).toMatchObject({ status: "completed", output: "Timeout" });
		const [childRun] = (await engine.runs.list({ taskId: "sleepy-child" })).items;
		expect(childRun?.status).toBe("canceled");
		// The clock stopped at the timeout: the canceled child's sleep never moved it further.
		expect(engine.clock.now().getTime() - START.getTime()).toBe(3_600_000);
	});

	it("ctx.stream chunks can be read with runs.readStream", async () => {
		const streamer = defineTask({
			id: "streamer",
			run: async (_p: null, ctx) => {
				async function* tokens() {
					yield "a";
					yield { b: 1 };
				}
				await ctx.stream("out", tokens());
				return null;
			},
		});
		const engine = testEngine({ tasks: [streamer] });

		const result = await engine.runInline(streamer, null);
		const chunks: unknown[] = [];
		for await (const chunk of engine.runs.readStream(result.runId, "out")) chunks.push(chunk);

		expect(chunks).toEqual(["a", { b: 1 }]);
	});

	it("tickSchedule fires one occurrence of a declarative or API schedule", async () => {
		const report = defineTask({
			id: "report",
			schedule: { cron: "0 9 * * *", payload: { kind: "daily" } },
			run: async (p: { kind: string }) => p.kind,
		});
		const engine = testEngine({ tasks: [report], clock: { start: START } });

		await engine.tickSchedule("report");
		// The same occurrence again is a no-op (idempotency key per occurrence, decision 9).
		await engine.tickSchedule("report", { at: START });
		const runs = (await engine.runs.list({ taskId: "report" })).items;
		expect(runs.map(r => [r.status, r.output])).toEqual([["completed", "daily"]]);

		await engine.schedules.upsert({
			id: "weekly",
			task: report,
			cron: "0 9 * * 1",
			payload: { kind: "weekly" },
		});
		await engine.tickSchedule("weekly");
		const outputs = (await engine.runs.list({ taskId: "report" })).items.map(r => r.output);
		expect(outputs.sort()).toEqual(["daily", "weekly"]);
	});

	it("stops runaway runs with maxJobsPerDrain", async () => {
		const forever = defineTask({
			id: "forever",
			run: async (_p: null, ctx) => {
				for (let i = 0; ; i++) await ctx.step.sleep(`tick-${i}`, "1s");
			},
		});
		const engine = testEngine({ tasks: [forever], maxJobsPerDrain: 20 });

		await expect(engine.runInline(forever, null)).rejects.toThrow(/more than 20 jobs/);
	});

	it("rejects use after stop()", async () => {
		const noop = defineTask({ id: "noop", run: async (_p: null) => null });
		const engine = testEngine({ tasks: [noop] });
		await engine.stop();
		await expect(engine.trigger(noop, null)).rejects.toBeInstanceOf(MinitriggerError);
		await expect(engine.drain()).rejects.toBeInstanceOf(MinitriggerError);
	});

	it("validates tasks like createEngine", () => {
		const bad = defineTask({
			id: "bad",
			retry: { maxAttempts: 0 },
			run: async (_p: null) => null,
		});
		expect(() => createTestEngine({ tasks: [bad] })).toThrow(ConfigError);
	});
});

describe("memory transport", () => {
	function setup() {
		let t = 1_000;
		const transport = createMemoryTransport({
			prefix: "mt",
			queues: [{ name: "q", concurrency: 1 }],
			now: () => t,
		});
		return { transport, advance: (ms: number) => (t += ms) };
	}

	it("dedupes by jobId, including while the job is active, and never removes an active job", async () => {
		const { transport } = setup();
		await transport.enqueueRun({ queue: "q", runId: "r", seq: 1 });
		await transport.enqueueRun({ queue: "q", runId: "r", seq: 1 });
		expect(transport.jobs()).toHaveLength(1);

		const job = transport.takeDue();
		expect(job?.id).toBe("r-1");
		await transport.enqueueRun({ queue: "q", runId: "r", seq: 1 });
		await transport.removeRunJob("q", "r", 1);
		expect(transport.jobs()).toHaveLength(1);
		if (job) transport.finish(job);
		expect(transport.jobs()).toHaveLength(0);
	});

	it("delays against the injected clock, FIFO among equally due jobs", async () => {
		const { transport, advance } = setup();
		await transport.enqueueRun({ queue: "q", runId: "late", seq: 1, delayMs: 500 });
		await transport.enqueueRun({ queue: "q", runId: "a", seq: 1 });
		await transport.enqueueSystem("waitpoint-expire", { waitpointId: "w" }, { jobId: "wp-w" });
		expect(transport.nextDueAt("run")).toBe(1_000);

		expect(transport.takeDue()?.id).toBe("a-1");
		expect(transport.takeDue()?.id).toBe("wp-w");
		expect(transport.takeDue()).toBeUndefined();
		advance(500);
		expect(transport.takeDue()?.id).toBe("late-1");
	});

	it("accepts undeclared queues and rejects invalid names like the BullMQ transport", async () => {
		const { transport } = setup();
		await expect(
			transport.enqueueRun({ queue: "from-new-deploy", runId: "r", seq: 1 })
		).resolves.toBeUndefined();
		await expect(transport.enqueueRun({ queue: "system", runId: "r", seq: 2 })).rejects.toThrow(
			ConfigError
		);
	});
});
