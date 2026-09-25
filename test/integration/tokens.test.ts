import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { escapeIdentifier } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { AbortTaskRunError, SerializationError } from "../../src/errors.js";
import { type AnyTask, defineTask, type TokenHandle } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const shortId = () => randomUUID().slice(0, 8);
const uniqueQueue = () => `q${shortId()}`;
const uniqueTaskId = (label: string) => `${label}-${shortId()}`;
const fastRetry = (maxAttempts: number) => ({
	maxAttempts,
	backoff: { type: "fixed" as const, baseMs: 10, jitter: false },
});

const logs: { level: string; message: string }[] = [];
const testLogger: EngineLogger = {
	debug: (_f, message) => logs.push({ level: "debug", message }),
	info: (_f, message) => logs.push({ level: "info", message }),
	warn: (_f, message) => logs.push({ level: "warn", message }),
	error: (_f, message) => logs.push({ level: "error", message }),
};

const engines: Engine[] = [];

async function startedEngine(tasks: AnyTask[], overrides: Partial<EngineConfig> = {}) {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 8 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 5_000,
		shutdownTimeoutMs: 2_000,
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	await engine.start();
	return engine;
}

const t = (table: string) => `${escapeIdentifier(ns.schema)}.${table}`;

async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 10_000) {
	await waitFor(async () => (await engine.runs.get(id))?.status === status, {
		timeoutMs,
		intervalMs: 20,
	});
	const run = await engine.runs.get(id);
	if (!run) throw new Error("unreachable");
	return run;
}

async function waitpointsOf(runId: string) {
	const { rows } = await ns.pool.query<{ id: string; step_name: string; status: string }>(
		`SELECT id, step_name, status FROM ${t("waitpoints")} WHERE run_id = $1 ORDER BY step_name`,
		[runId]
	);
	return rows;
}

async function stepsOf(runId: string) {
	const { rows } = await ns.pool.query<{ name: string; kind: string; output: unknown }>(
		`SELECT name, kind, output FROM ${t("steps")} WHERE run_id = $1 ORDER BY seq_index`,
		[runId]
	);
	return rows;
}

/** Waits until the run is `waiting` with a pending token named `name`; returns its id. */
async function pendingToken(engine: Engine, runId: string, name: string): Promise<string> {
	await waitForStatus(engine, runId, "waiting");
	const wp = (await waitpointsOf(runId)).find(w => w.step_name === name);
	if (!wp) throw new Error(`token ${name} not found`);
	expect(wp.status).toBe("pending");
	return wp.id;
}

let systemRedis: Redis;
let systemQueue: Queue;

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
	systemRedis = new Redis(ns.redisUrl);
	systemQueue = new Queue(`${ns.prefix}-system`, { connection: systemRedis, prefix: ns.prefix });
});

afterAll(async () => {
	await systemQueue.close();
	systemRedis.disconnect();
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
	logs.length = 0;
});

describe("tokens", () => {
	it("token completed: the waiting run resumes with the output", async () => {
		let executions = 0;
		const task = defineTask({
			id: uniqueTaskId("tok"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				executions++;
				const token = await ctx.step.createToken("approval");
				const result = await ctx.step.waitForToken<{ approved: boolean }>(token);
				return result.ok ? result.output : { approved: false };
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "approval");

		expect(await engine.tokens.complete(tokenId, { approved: true })).toEqual({ completed: true });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ approved: true });
		expect(executions).toBe(2);
		// Decision 10: the #wait step keeps only the reference.
		expect(await stepsOf(handle.id)).toEqual([
			{ name: "approval", kind: "token", output: { id: tokenId } },
			{ name: "approval#wait", kind: "wait", output: { waitpointId: tokenId } },
		]);
	});

	it("token expired: waitForToken returns { ok: false, error: Timeout }", async () => {
		const task = defineTask({
			id: uniqueTaskId("tok-exp"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				const token = await ctx.step.createToken("approval", { timeout: "300ms" });
				return ctx.step.waitForToken(token);
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toMatchObject({ ok: false, error: { name: "Timeout" } });
		expect((await waitpointsOf(handle.id))[0]?.status).toBe("timed_out");
		const tokenId = (await waitpointsOf(handle.id))[0]?.id ?? "";
		// The token's run already finished: `run_finished`, not a misleading `timed_out`.
		expect(await engine.tokens.complete(tokenId, 1)).toEqual({
			completed: false,
			reason: "run_finished",
		});
	});

	it("a token that timed out on its own (run still waiting on it) reports timed_out", async () => {
		const task = defineTask({
			id: uniqueTaskId("tok-own-timeout"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				const token = await ctx.step.createToken("t", { timeout: "300ms" });
				const result = await ctx.step.waitForToken(token);
				await ctx.step.sleep("hold", "1h");
				return result;
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		await waitFor(async () => (await waitpointsOf(handle.id))[0]?.status === "timed_out");
		await waitForStatus(engine, handle.id, "waiting");
		const tokenId = (await waitpointsOf(handle.id))[0]?.id ?? "";
		expect(await engine.tokens.complete(tokenId, 1)).toEqual({
			completed: false,
			reason: "timed_out",
		});
	});

	it("a waitForToken the task did not await suspends the run instead of completing it", async () => {
		let executions = 0;
		const task = defineTask({
			id: uniqueTaskId("tok-not-awaited"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				executions++;
				void ctx.step.waitForToken("t").catch(() => {});
				return "returned";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "t");
		expect(logs.some(l => l.level === "warn" && l.message.includes("was not awaited"))).toBe(true);
		expect(await engine.tokens.complete(tokenId, "ok")).toEqual({ completed: true });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toBe("returned");
		expect(executions).toBe(2);
	});

	it("completing a token twice: the second call is a no-op", async () => {
		let executions = 0;
		const task = defineTask({
			id: uniqueTaskId("tok-twice"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				executions++;
				return ctx.step.waitForToken<number>("t");
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "t");

		expect(await engine.tokens.complete(tokenId, 1)).toEqual({ completed: true });
		expect(await engine.tokens.complete(tokenId, 2)).toEqual({
			completed: false,
			reason: "completed",
		});
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ ok: true, output: 1 });
		await sleep(200);
		expect(executions).toBe(2);
		expect(await engine.tokens.complete(randomUUID(), 1)).toEqual({
			completed: false,
			reason: "not_found",
		});
		expect(await engine.tokens.complete("not-a-uuid", 1)).toEqual({
			completed: false,
			reason: "not_found",
		});
	});

	it("two tokens completed simultaneously (Promise.all): the run resumes exactly once", async () => {
		const executions = new Map<string, number>();
		const task = defineTask({
			id: uniqueTaskId("tok-two"),
			queue: { name: uniqueQueue(), concurrency: 10 },
			run: async (_p: { v: number }, ctx) => {
				executions.set(ctx.run.id, (executions.get(ctx.run.id) ?? 0) + 1);
				const [a, b] = await Promise.all([
					ctx.step.waitForToken<string>("a"),
					ctx.step.waitForToken<string>("b"),
				]);
				return [a, b];
			},
		});
		const engine = await startedEngine([task]);
		const handles = await Promise.all([1, 2, 3, 4, 5].map(v => engine.trigger(task, { v })));
		await Promise.all(
			handles.map(async h => {
				const a = await pendingToken(engine, h.id, "a");
				const b = await pendingToken(engine, h.id, "b");
				await Promise.all([engine.tokens.complete(a, "A"), engine.tokens.complete(b, "B")]);
			})
		);
		for (const h of handles) {
			const run = await waitForStatus(engine, h.id, "completed");
			expect(run.output).toEqual([
				{ ok: true, output: "A" },
				{ ok: true, output: "B" },
			]);
		}
		await sleep(300);
		for (const h of handles) expect(executions.get(h.id)).toBe(2);
	});

	it("an invalid token output is rejected and the token stays pending", async () => {
		const task = defineTask({
			id: uniqueTaskId("tok-invalid"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => ctx.step.waitForToken("t"),
		});
		const engine = await startedEngine([task], { limits: { maxPayloadBytes: 1024 } });
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "t");

		await expect(engine.tokens.complete(tokenId, { when: new Date() })).rejects.toBeInstanceOf(
			SerializationError
		);
		await expect(engine.tokens.complete(tokenId, "x".repeat(2_000))).rejects.toThrow(
			/exceeding the configured size limit/
		);
		await expect(engine.tokens.complete(tokenId, () => 1)).rejects.toBeInstanceOf(
			SerializationError
		);
		expect((await waitpointsOf(handle.id))[0]?.status).toBe("pending");
		expect((await engine.runs.get(handle.id))?.status).toBe("waiting");
		expect(await engine.tokens.complete(tokenId, "ok")).toEqual({ completed: true });
		await waitForStatus(engine, handle.id, "completed");
	});

	it("token completed while the run is still running: resumes without suspending", async () => {
		let executions = 0;
		let tokenFromRun: TokenHandle | undefined;
		let release!: () => void;
		const released = new Promise<void>(resolve => {
			release = resolve;
		});
		const task = defineTask({
			id: uniqueTaskId("tok-running"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				executions++;
				const token = await ctx.step.createToken("t", { timeout: "1h" });
				tokenFromRun = token;
				await ctx.step.run("work", async () => {
					await released;
					return 1;
				});
				return ctx.step.waitForToken<string>(token);
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		await waitFor(() => tokenFromRun !== undefined);
		expect((await engine.runs.get(handle.id))?.status).toBe("running");
		expect(await engine.tokens.complete(tokenFromRun?.id ?? "", "early")).toEqual({
			completed: true,
		});
		// The expire job was removed by the completion.
		expect(await systemQueue.getJob(`wp-${tokenFromRun?.id}-expire`)).toBeUndefined();
		release();
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ ok: true, output: "early" });
		expect(executions).toBe(1);
	});

	it("a token completed with a 1 MB output", async () => {
		const task = defineTask({
			id: uniqueTaskId("tok-big"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				const result = await ctx.step.waitForToken<string>("big");
				return result.ok ? result.output.length : -1;
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "big");
		const big = "x".repeat(1_000_000);
		expect(await engine.tokens.complete(tokenId, big)).toEqual({ completed: true });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toBe(1_000_000);
		// Read from waitpoints.output, never copied into the step (maxStepOutputBytes 256 KB).
		expect((await stepsOf(handle.id)).find(s => s.name === "big#wait")?.output).toEqual({
			waitpointId: tokenId,
		});
	});

	it("a replay that recreates a token with timeout does not schedule a second expire job", async () => {
		let executions = 0;
		const task = defineTask({
			id: uniqueTaskId("tok-replay"),
			queue: { name: uniqueQueue() },
			retry: fastRetry(3),
			run: async (_p: { v: number }, ctx) => {
				executions++;
				const token = await ctx.step.createToken("t", { timeout: "1h" });
				if (ctx.run.attempt < 3) throw new Error("force a replay");
				return ctx.step.waitForToken<number>(token);
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "t");
		expect(executions).toBe(3);
		expect(await waitpointsOf(handle.id)).toHaveLength(1);

		const delayed = await systemQueue.getDelayed();
		const expireJobs = delayed.filter(j => j.name === "waitpoint-expire");
		expect(expireJobs.map(j => j.id)).toEqual([`wp-${tokenId}-expire`]);
		expect(expireJobs[0]?.data).toEqual({ waitpointId: tokenId });

		await engine.tokens.complete(tokenId, 7);
		await waitForStatus(engine, handle.id, "completed");
		expect(await systemQueue.getJob(`wp-${tokenId}-expire`)).toBeUndefined();
	});

	it("waiting on a token + a 1 s sleep: completing the token early wakes the run once", async () => {
		let executions = 0;
		const task = defineTask({
			id: uniqueTaskId("tok-sleep"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				executions++;
				const [token] = await Promise.all([
					ctx.step.waitForToken<string>("t"),
					ctx.step.sleep("nap", "1s"),
				]);
				return token;
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "t");
		expect(await engine.tokens.complete(tokenId, "done")).toEqual({ completed: true });
		const { rows } = await ns.pool.query<{ wake_at: Date }>(
			`SELECT wake_at FROM ${t("steps")} WHERE run_id = $1 AND name = 'nap'`,
			[handle.id]
		);
		const wakeAt = rows[0]?.wake_at.getTime() ?? 0;
		// The resume is scheduled for the sleep's wake_at (resume rule), not right away.
		expect((await engine.runs.get(handle.id))?.runAfter?.getTime()).toBe(wakeAt);
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ ok: true, output: "done" });
		expect(run.finishedAt?.getTime() ?? 0).toBeGreaterThanOrEqual(wakeAt);
		await sleep(300);
		expect(executions).toBe(2);
	});

	it("a run that fails definitively times out its own pending tokens and drops their expire jobs", async () => {
		let tokenId = "";
		const task = defineTask({
			id: uniqueTaskId("tok-fail"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				const token = await ctx.step.createToken("t", { timeout: "1h" });
				tokenId = token.id;
				throw new AbortTaskRunError("give up");
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		await waitForStatus(engine, handle.id, "failed");
		expect((await waitpointsOf(handle.id))[0]?.status).toBe("timed_out");
		await waitFor(async () => (await systemQueue.getJob(`wp-${tokenId}-expire`)) === undefined);
		// The token's run already finished: `run_finished`, not a misleading `timed_out`.
		expect(await engine.tokens.complete(tokenId, 1)).toEqual({
			completed: false,
			reason: "run_finished",
		});
	});

	it("a token that timed out on its own (run still waiting on it) reports timed_out", async () => {
		const task = defineTask({
			id: uniqueTaskId("tok-own-timeout"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				const token = await ctx.step.createToken("t", { timeout: "300ms" });
				const result = await ctx.step.waitForToken(token);
				await ctx.step.sleep("hold", "1h");
				return result;
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		await waitFor(async () => (await waitpointsOf(handle.id))[0]?.status === "timed_out");
		await waitForStatus(engine, handle.id, "waiting");
		const tokenId = (await waitpointsOf(handle.id))[0]?.id ?? "";
		expect(await engine.tokens.complete(tokenId, 1)).toEqual({
			completed: false,
			reason: "timed_out",
		});
	});

	it("a waitForToken the task did not await suspends the run instead of completing it", async () => {
		let executions = 0;
		const task = defineTask({
			id: uniqueTaskId("tok-not-awaited"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				executions++;
				void ctx.step.waitForToken("t").catch(() => {});
				return "returned";
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "t");
		expect(logs.some(l => l.level === "warn" && l.message.includes("was not awaited"))).toBe(true);
		expect(await engine.tokens.complete(tokenId, "ok")).toEqual({ completed: true });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toBe("returned");
		expect(executions).toBe(2);
	});

	it("an expire job that fires before the DB expiry is re-delayed in place (same jobId)", async () => {
		const task = defineTask({
			id: uniqueTaskId("tok-early"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				const token = await ctx.step.createToken("t", { timeout: "5s" });
				return ctx.step.waitForToken(token);
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "t");
		const jobId = `wp-${tokenId}-expire`;
		// Simulate BullMQ's clock running ahead of the DB's: fire the expiry right now.
		await systemQueue.remove(jobId);
		await systemQueue.add("waitpoint-expire", { waitpointId: tokenId }, { jobId, attempts: 1 });
		await waitFor(
			async () => (await systemQueue.getJob(jobId)?.then(j => j?.getState())) === "delayed"
		);
		expect(logs.some(l => l.message.includes("fired early; re-delayed"))).toBe(true);
		expect((await waitpointsOf(handle.id))[0]?.status).toBe("pending");
		// ... and it still expires the token at the DB's expires_at.
		const run = await waitForStatus(engine, handle.id, "completed", 15_000);
		expect(run.output).toMatchObject({ ok: false, error: { name: "Timeout" } });
	});

	it("only awaited tokens block: the run wakes after a sleep with a token created but not awaited yet", async () => {
		let executions = 0;
		let reminders = 0;
		const task = defineTask({
			id: uniqueTaskId("tok-reminder"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				executions++;
				const token = await ctx.step.createToken("ok", { timeout: "2d" });
				await ctx.step.sleep("reminder", "1s");
				await ctx.step.run("remind", () => {
					reminders++;
					return "sent";
				});
				return ctx.step.waitForToken(token);
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		// Wakes up after the sleep without the token being completed, sends the reminder, then
		// suspends on the token.
		await waitFor(async () => reminders === 1, { timeoutMs: 10_000, intervalMs: 20 });
		const tokenId = await pendingToken(engine, handle.id, "ok");
		expect((await stepsOf(handle.id)).map(s => s.name)).toEqual([
			"ok",
			"reminder",
			"remind",
			"ok#wait",
		]);
		expect(executions).toBe(2);
		expect(await engine.tokens.complete(tokenId, { answer: "yes" })).toEqual({ completed: true });
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ ok: true, output: { answer: "yes" } });
		await sleep(300);
		expect(executions).toBe(3);
		expect(reminders).toBe(1);
	});

	it("completing a not-yet-awaited token while the run sleeps does not resume it before the sleep is due", async () => {
		let executions = 0;
		const task = defineTask({
			id: uniqueTaskId("tok-early-complete"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				executions++;
				const token = await ctx.step.createToken("ok");
				await ctx.step.sleep("nap", "2s");
				return ctx.step.waitForToken(token);
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		await waitForStatus(engine, handle.id, "waiting");
		const tokenId = (await waitpointsOf(handle.id))[0]?.id ?? "";
		const { rows } = await ns.pool.query<{ wake_at: Date }>(
			`SELECT wake_at FROM ${t("steps")} WHERE run_id = $1 AND name = 'nap'`,
			[handle.id]
		);
		const wakeAt = rows[0]?.wake_at.getTime() ?? 0;
		expect(await engine.tokens.complete(tokenId, "early")).toEqual({ completed: true });
		await sleep(700);
		expect(executions).toBe(1);
		expect((await engine.runs.get(handle.id))?.status).toBe("waiting");
		const run = await waitForStatus(engine, handle.id, "completed");
		expect(run.output).toEqual({ ok: true, output: "early" });
		expect(run.finishedAt?.getTime() ?? 0).toBeGreaterThanOrEqual(wakeAt);
		await sleep(300);
		expect(executions).toBe(2);
	});

	it("waitForToken(name) without createToken creates the token (steps name and name#wait)", async () => {
		const task = defineTask({
			id: uniqueTaskId("tok-name"),
			queue: { name: uniqueQueue() },
			run: async (_p: { v: number }, ctx) => {
				await ctx.step.run("before", () => 1);
				return ctx.step.waitForToken("t");
			},
		});
		const engine = await startedEngine([task]);
		const handle = await engine.trigger(task, { v: 1 });
		const tokenId = await pendingToken(engine, handle.id, "t");
		await engine.tokens.complete(tokenId, null);
		await waitForStatus(engine, handle.id, "completed");
		expect((await stepsOf(handle.id)).map(s => [s.name, s.kind])).toEqual([
			["before", "run"],
			["t", "token"],
			["t#wait", "wait"],
		]);
	});
});
