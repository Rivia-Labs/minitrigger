/** Phase 7: `engine.events.on(...)` local run events. */
import { randomUUID } from "node:crypto";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine, type RunEvent } from "../../src/engine.js";
import { AbortTaskRunError } from "../../src/errors.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);
const errors: Array<{ fields: Record<string, unknown>; message: string }> = [];
const testLogger: EngineLogger = {
	debug() {},
	info() {},
	warn() {},
	error: (fields, message) => errors.push({ fields, message }),
};
const engines: Engine[] = [];

function makeEngine(tasks: AnyTask[], overrides: Partial<EngineConfig> = {}): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 5 },
		heartbeatIntervalMs: 1_000,
		stallTimeoutMs: 10_000,
		shutdownTimeoutMs: 2_000,
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

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
	errors.length = 0;
});

describe("engine.events", () => {
	it("fires queued/started/completed for a successful run; handler errors are logged", async () => {
		const task = defineTask({
			id: `ok-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async () => "fine",
		});
		const engine = makeEngine([task]);
		const seen: RunEvent[] = [];
		engine.events.on("run:queued", e => {
			seen.push(e);
		});
		engine.events.on("run:started", e => {
			seen.push(e);
		});
		engine.events.on("run:completed", e => {
			seen.push(e);
		});
		engine.events.on("run:completed", () => {
			throw new Error("sync handler boom");
		});
		engine.events.on("run:completed", async () => {
			throw new Error("async handler boom");
		});
		await engine.start();
		const handle = await engine.trigger(task);
		await waitFor(() => seen.some(e => e.name === "run:completed"), { timeoutMs: 10_000 });

		expect(seen.map(e => e.name)).toEqual(["run:queued", "run:started", "run:completed"]);
		expect(seen.map(e => e.status)).toEqual(["queued", "running", "completed"]);
		const completed = seen[2];
		expect(completed).toMatchObject({ runId: handle.id, taskId: task.id, attempt: 1, error: null });
		expect(completed?.updatedAt).toBeInstanceOf(Date);
		// Attempt telemetry (durationMs/startedAt): present on the terminal write, absent on `started`
		// (which doesn't end an attempt).
		expect(completed?.startedAt).toBeInstanceOf(Date);
		expect(typeof completed?.durationMs).toBe("number");
		expect(completed?.durationMs).toBeGreaterThanOrEqual(0);
		expect(seen[1]?.durationMs).toBeUndefined();
		expect(seen[1]?.startedAt).toBeUndefined();
		await waitFor(() => errors.length >= 2, { timeoutMs: 2_000 });
		expect(errors.map(e => e.message)).toEqual([
			"engine.events handler failed",
			"engine.events handler failed",
		]);
		expect((await engine.runs.get(handle.id))?.status).toBe("completed");
	});

	it("fires retrying and then failed, and unsubscribe stops delivery", async () => {
		const task = defineTask({
			id: `bad-${shortId()}`,
			queue: { name: `q${shortId()}` },
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 10, jitter: false } },
			run: async (_payload: undefined, ctx) => {
				throw new Error(`attempt ${ctx.run.attempt} failed`);
			},
		});
		const fatal = defineTask({
			id: `fatal-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async () => {
				throw new AbortTaskRunError("no retry");
			},
		});
		const engine = makeEngine([task, fatal]);
		const seen: RunEvent[] = [];
		engine.events.on("run:retrying", e => {
			seen.push(e);
		});
		engine.events.on("run:failed", e => {
			seen.push(e);
		});
		const started: RunEvent[] = [];
		const off = engine.events.on("run:started", e => {
			started.push(e);
		});
		off();
		await engine.start();
		const handle = await engine.trigger(task);
		await waitFor(() => seen.some(e => e.name === "run:failed"), { timeoutMs: 10_000 });
		expect(seen.map(e => [e.name, e.status])).toEqual([
			["run:retrying", "queued"],
			["run:failed", "failed"],
		]);
		expect(seen[0]?.error?.message).toBe("attempt 1 failed");
		expect(seen[1]).toMatchObject({ runId: handle.id, attempt: 2 });
		expect(seen[1]?.error?.message).toBe("attempt 2 failed");
		// run:retrying and run:failed both carry attempt telemetry.
		for (const e of [seen[0], seen[1]]) {
			expect(e?.startedAt).toBeInstanceOf(Date);
			expect(typeof e?.durationMs).toBe("number");
		}

		const fatalHandle = await engine.trigger(fatal);
		await waitFor(() => seen.some(e => e.runId === fatalHandle.id), { timeoutMs: 10_000 });
		expect(seen.at(-1)).toMatchObject({ name: "run:failed", runId: fatalHandle.id, attempt: 1 });
		expect(started).toEqual([]);

		expect(() => engine.events.on("run:unknown" as never, () => {})).toThrow(/unknown event/);
	});

	it("fires canceled and waiting where this process made the transition", async () => {
		const task = defineTask({
			id: `waits-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_payload: undefined, ctx) => {
				await ctx.step.waitForToken("approval");
			},
		});
		const engine = makeEngine([task]);
		const names: string[] = [];
		const seen: RunEvent[] = [];
		for (const name of ["run:waiting", "run:canceled"] as const) {
			engine.events.on(name, e => {
				names.push(`${e.name}:${e.status}`);
				seen.push(e);
			});
		}
		await engine.start();
		const handle = await engine.trigger(task);
		await waitFor(() => names.includes("run:waiting:waiting"), { timeoutMs: 10_000 });
		expect(await engine.runs.cancel(handle.id)).toBe("canceled");
		await waitFor(() => names.includes("run:canceled:canceled"), { timeoutMs: 5_000 });
		expect(names).toEqual(["run:waiting:waiting", "run:canceled:canceled"]);
		// run:waiting carries attempt telemetry (it suspended an in-flight attempt); run:canceled
		// isn't an attempt-ending write from the executor's point of view, so it doesn't.
		expect(seen[0]?.startedAt).toBeInstanceOf(Date);
		expect(typeof seen[0]?.durationMs).toBe("number");
		expect(seen[1]?.durationMs).toBeUndefined();
	});
});
