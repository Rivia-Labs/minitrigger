/**
 * Phase 9.2 acceptance: testing a user's task code with `createTestEngine`, no Redis and no
 * Postgres (runs in the `unit` project, without docker). Written the way a user would, importing
 * only the public entry point.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	ConfigError,
	createTestEngine,
	defineTask,
	type EngineLogger,
	TaskNotRegisteredError,
	type TestEngine,
	type TestEngineConfig,
} from "../../src/index.js";
import * as registryModule from "../../src/registry.js";

const warnings: string[] = [];
const quietLogger: EngineLogger = {
	debug: () => {},
	info: () => {},
	warn: (_fields, message) => warnings.push(message),
	error: (_fields, message) => warnings.push(message),
};

const engines: TestEngine[] = [];
function testEngine(config: TestEngineConfig): TestEngine {
	const engine = createTestEngine({ logger: quietLogger, ...config });
	engines.push(engine);
	return engine;
}

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
	warnings.length = 0;
});

const START = new Date("2026-01-01T00:00:00.000Z");
const HOUR = 3_600_000;
const DAY = 24 * HOUR;

describe("createTestEngine: steps and sleep", () => {
	it("runs a multi-step task with memoized steps across a sleep, instantly", async () => {
		const calls = { charge: 0, email: 0 };
		const checkout = defineTask({
			id: "checkout",
			run: async (payload: { orderId: string; amount: number }, ctx) => {
				const charge = await ctx.step.run("charge", async () => {
					calls.charge++;
					return { chargeId: `ch_${payload.orderId}`, amount: payload.amount };
				});
				await ctx.step.sleep("cool-off", "1d");
				await ctx.step.run("email", async () => {
					calls.email++;
				});
				return { chargeId: charge.chargeId };
			},
		});
		const engine = testEngine({ tasks: [checkout], clock: { start: START } });

		const result = await engine.runInline(checkout, { orderId: "o1", amount: 42 });

		expect(result).toMatchObject({
			status: "completed",
			output: { chargeId: "ch_o1" },
			error: null,
			attempt: 1,
		});
		// The sleep suspended the run and the replay did not re-run "charge".
		expect(calls).toEqual({ charge: 1, email: 1 });
		// The fake clock jumped a whole day; no real time was spent waiting.
		expect(engine.clock.now().getTime() - START.getTime()).toBe(DAY);
		const steps = await engine.runs.steps(result.runId);
		expect(steps.map(s => [s.name, s.kind, s.status])).toEqual([
			["charge", "run", "completed"],
			["cool-off", "sleep", "completed"],
			["email", "run", "completed"],
		]);
	});

	it("with autoAdvance: false, a sleeping run waits for clock.advance", async () => {
		const reminder = defineTask({
			id: "reminder",
			run: async (_payload: null, ctx) => {
				await ctx.step.sleep("wait", "2h");
				return "sent";
			},
		});
		const engine = testEngine({
			tasks: [reminder],
			clock: { start: START, autoAdvance: false },
		});

		const first = await engine.runInline(reminder, null);
		expect(first.status).toBe("waiting");

		await engine.clock.advance("1h");
		expect((await engine.drive(first.runId)).status).toBe("waiting");

		await engine.clock.advance("1h");
		expect(await engine.drive(first.runId)).toMatchObject({ status: "completed", output: "sent" });
	});
});

describe("createTestEngine: tokens", () => {
	const approval = defineTask({
		id: "approval",
		run: async (payload: { doc: string }, ctx) => {
			const token = await ctx.step.createToken("approve", { timeout: "3d" });
			await ctx.step.run("notify", async () => ({ tokenId: token.id, doc: payload.doc }));
			const decision = await ctx.step.waitForToken<{ approved: boolean }>(token);
			if (!decision.ok) return { status: "expired" as const };
			return { status: decision.output.approved ? ("approved" as const) : ("rejected" as const) };
		},
	});

	it("blocks on a token until the test completes it", async () => {
		const engine = testEngine({ tasks: [approval] });

		const first = await engine.runInline(approval, { doc: "contract.pdf" });
		expect(first.status).toBe("waiting");

		const [token] = await engine.tokens.pending(first.runId);
		expect(token?.name).toBe("approve");
		if (!token) throw new Error("no pending token");
		expect(await engine.tokens.complete(token.id, { approved: true })).toEqual({ completed: true });

		const final = await engine.drive(first.runId);
		expect(final).toMatchObject({ status: "completed", output: { status: "approved" } });
		expect(await engine.tokens.pending(first.runId)).toEqual([]);
	});

	it("times the token out when the clock passes its timeout", async () => {
		const engine = testEngine({ tasks: [approval], clock: { start: START } });

		const first = await engine.runInline(approval, { doc: "late.pdf" });
		// A token timeout alone never moves the clock: the run stays blocked.
		expect(first.status).toBe("waiting");
		expect(engine.clock.now()).toEqual(START);

		await engine.clock.advance("3d");
		expect(await engine.drive(first.runId)).toMatchObject({
			status: "completed",
			output: { status: "expired" },
		});
	});
});

describe("createTestEngine: subtasks", () => {
	it("runs triggerAndWait and batchTriggerAndWait children inline", async () => {
		const square = defineTask({
			id: "square",
			run: async (n: number, ctx) => {
				await ctx.step.sleep("think", "5m");
				return n * n;
			},
		});
		const parent = defineTask({
			id: "sum-of-squares",
			run: async (ns: number[], ctx) => {
				const first = await ctx.step.triggerAndWait("first", square, ns[0] ?? 0);
				const rest = await ctx.step.batchTriggerAndWait("rest", square, ns.slice(1));
				const values = [first, ...rest].map(r => (r.ok ? r.output : 0));
				return values.reduce((a, b) => a + b, 0);
			},
		});
		const engine = testEngine({ tasks: [square, parent] });

		const result = await engine.runInline(parent, [1, 2, 3]);

		expect(result).toMatchObject({ status: "completed", output: 14 });
		const children = await engine.runs.list({ taskId: "square" });
		expect(children.items).toHaveLength(3);
		expect(
			children.items.every(c => c.status === "completed" && c.parentRunId === result.runId)
		).toBe(true);
	});
});

describe("createTestEngine: retries", () => {
	it("retries with backoff under the fake clock", async () => {
		let attempts = 0;
		const flaky = defineTask({
			id: "flaky",
			retry: { maxAttempts: 3, backoff: { type: "exponential", baseMs: 60_000, jitter: false } },
			run: async (_payload: null, ctx) => {
				attempts++;
				if (ctx.run.attempt < 3) throw new Error(`boom #${ctx.run.attempt}`);
				return "ok";
			},
		});
		const engine = testEngine({ tasks: [flaky], clock: { start: START } });
		const retrying: number[] = [];
		engine.events.on("run:retrying", e => {
			retrying.push(e.attempt);
		});

		const result = await engine.runInline(flaky, null);

		expect(result).toMatchObject({ status: "completed", output: "ok", attempt: 3 });
		expect(attempts).toBe(3);
		// 1 min after attempt 1, 2 min after attempt 2 (exponential, no jitter).
		expect(engine.clock.now().getTime() - START.getTime()).toBe(3 * 60_000);
		expect(retrying).toEqual([2, 3]);
	});

	it("fails the run once attempts are exhausted", async () => {
		const broken = defineTask({
			id: "broken",
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 1_000, jitter: false } },
			run: async (_payload: null) => {
				throw new TypeError("always broken");
			},
		});
		const engine = testEngine({ tasks: [broken] });

		const result = await engine.runInline(broken, null);

		expect(result.status).toBe("failed");
		expect(result.attempt).toBe(2);
		expect(result.error).toMatchObject({ name: "TypeError", message: "always broken" });
	});
});

describe("createTestEngine: non-determinism", () => {
	it("surfaces a NonDeterminismError when the replay calls different steps", async () => {
		// Simulates a deploy that renamed a step while a run was suspended.
		let version = 1;
		const task = defineTask({
			id: "evolving",
			retry: { maxAttempts: 5 },
			run: async (_payload: null, ctx) => {
				await ctx.step.run(version === 1 ? "load" : "fetch", async () => 1);
				version++;
				await ctx.step.sleep("pause", "1m");
				return "done";
			},
		});
		const engine = testEngine({ tasks: [task] });

		const result = await engine.runInline(task, null);

		expect(result.status).toBe("failed");
		// A code bug: failed without retry.
		expect(result.attempt).toBe(1);
		expect(result.error).toMatchObject({ name: "NonDeterminismError", retryable: false });
		expect(result.error?.message).toContain("load");
	});
});

describe("createTestEngine: stop() (M1)", () => {
	it("resolves even if a task never settles", async () => {
		let started = false;
		const stuck = defineTask({
			id: "stuck",
			run: async (_payload: null) => {
				started = true;
				await new Promise(() => {});
			},
		});
		const engine = testEngine({ tasks: [stuck] });

		await engine.trigger(stuck, null);
		// Never awaited to completion: the drain loop gets stuck awaiting the task's own promise.
		void engine.drain();
		// Let the run actually acquire and start (real timers: past decision 20's `isShuttingDown`
		// check at acquire time, so `stop()` below has to deal with a genuinely in-flight execution,
		// not just hand back a not-yet-acquired job).
		while (!started) {
			await new Promise(resolve => setTimeout(resolve, 0));
		}

		await expect(engine.stop()).resolves.toBeUndefined();
	});
});

describe("createTestEngine: tickSchedule error handling (M2)", () => {
	it("throws a ConfigError for an unknown schedule id", async () => {
		const engine = testEngine({ tasks: [] });

		await expect(engine.tickSchedule("no-such-schedule")).rejects.toThrow(ConfigError);
	});

	it("rejects if the schedule-tick job it enqueued fails", async () => {
		// Simulates a rolling deploy: the schedule exists in the store, but this process doesn't
		// have the task registered when its tick is processed.
		const ghost = defineTask({
			id: "ghost",
			schedule: { cron: "0 9 * * *", payload: null },
			run: async (_payload: null) => "x",
		});
		const engine = testEngine({ tasks: [ghost] });
		const spy = vi.spyOn(registryModule, "getTask").mockImplementationOnce(() => {
			throw new TaskNotRegisteredError('task "ghost" is not registered in this engine');
		});

		try {
			await expect(engine.tickSchedule("ghost")).rejects.toBeInstanceOf(TaskNotRegisteredError);
		} finally {
			spy.mockRestore();
		}
	});
});

describe("createTestEngine: clock semantics with token+sleep (L1)", () => {
	it("autoAdvance fires a token's own timeout before a later sleep resolves, even though the run had not reached waitForToken yet", async () => {
		const task = defineTask({
			id: "token-then-sleep",
			run: async (_payload: null, ctx) => {
				const token = await ctx.step.createToken("approve", { timeout: "1h" });
				await ctx.step.sleep("wait", "2h");
				const decision = await ctx.step.waitForToken<{ approved: boolean }>(token);
				return decision.ok ? "approved" : decision.error.name;
			},
		});
		const engine = testEngine({ tasks: [task], clock: { start: START } });

		const result = await engine.runInline(task, null);

		// Documented behavior (README "Testing your tasks"): autoAdvance jumps to whichever delayed
		// job is due first across every kind, so the token's own 1h expiry fires, in time order,
		// before the 2h sleep resolves -- even though this run had not called waitForToken yet when
		// the clock crossed the 1h mark. Use `autoAdvance: false` to interleave (e.g. complete the
		// token) before the timeout instead.
		expect(result).toMatchObject({ status: "completed", output: "Timeout" });
		expect(engine.clock.now().getTime() - START.getTime()).toBe(2 * HOUR);
	});
});

describe("createTestEngine: declarative schedules sync lazily (L2)", () => {
	it("schedules.list() on a fresh engine returns declarative schedules without any other call", async () => {
		const report = defineTask({
			id: "report-l2",
			schedule: { cron: "0 9 * * *", payload: null },
			run: async (_payload: null) => "x",
		});
		const engine = testEngine({ tasks: [report] });

		const list = await engine.schedules.list();

		expect(list.map(s => s.id)).toEqual(["report-l2"]);
	});
});

describe("createTestEngine: concurrent drain() (L5)", () => {
	it("a drain() started while another is already in progress still reflects a token completed during the race", async () => {
		const approval = defineTask({
			id: "approval-l5",
			run: async (_payload: { doc: string }, ctx) => {
				const token = await ctx.step.createToken("approve", { timeout: "3d" });
				const decision = await ctx.step.waitForToken<{ approved: boolean }>(token);
				return decision.ok ? "approved" : "expired";
			},
		});
		const engine = testEngine({ tasks: [approval] });

		const first = await engine.runInline(approval, { doc: "contract.pdf" });
		expect(first.status).toBe("waiting");
		const [token] = await engine.tokens.pending(first.runId);
		if (!token) throw new Error("no pending token");

		// Two concurrent drain() calls, racing a tokens.complete() that enqueues the run's resume
		// after the first drain's own last `takeDue()` check but possibly before it settles.
		const p1 = engine.drain();
		const p2 = engine.drain();
		await engine.tokens.complete(token.id, { approved: true });
		await Promise.all([p1, p2]);

		// Without looping again after an in-progress drain (L5), either concurrent call could settle
		// before the resume job lands, leaving the run `waiting` even though nothing blocks it.
		expect((await engine.runs.get(first.runId))?.status).toBe("completed");
	});
});

describe("createTestEngine: additional coverage", () => {
	it("tokens.complete reports reason 'timed_out' for a token whose expiry already fired but whose run is not finished yet", async () => {
		const task = defineTask({
			id: "token-then-long-sleep",
			run: async (_payload: null, ctx) => {
				const token = await ctx.step.createToken("approve", { timeout: "1h" });
				await ctx.step.sleep("wait", "2h");
				const decision = await ctx.step.waitForToken<{ approved: boolean }>(token);
				return decision.ok ? "approved" : decision.error.name;
			},
		});
		const engine = testEngine({
			tasks: [task],
			clock: { start: START, autoAdvance: false },
		});

		const first = await engine.runInline(task, null);
		expect(first.status).toBe("waiting");
		const [token] = await engine.tokens.pending(first.runId);
		if (!token) throw new Error("no pending token");

		// Only the token's own 1h expiry job is due; the 2h sleep's wake job is not, so the run
		// stays suspended (not terminal) once the expiry runs.
		await engine.clock.advance("1h");
		expect((await engine.runs.get(first.runId))?.status).toBe("waiting");

		expect(await engine.tokens.complete(token.id, { approved: true })).toEqual({
			completed: false,
			reason: "timed_out",
		});
	});

	it("waitForToken(name) without a prior createToken still shows up in tokens.pending", async () => {
		const task = defineTask({
			id: "wait-by-name-only",
			run: async (_payload: null, ctx) => {
				const decision = await ctx.step.waitForToken<{ approved: boolean }>("approve");
				return decision.ok ? "approved" : "expired";
			},
		});
		const engine = testEngine({ tasks: [task] });

		const first = await engine.runInline(task, null);
		expect(first.status).toBe("waiting");

		const pending = await engine.tokens.pending(first.runId);
		expect(pending.map(t => t.name)).toEqual(["approve"]);
		const [pendingToken] = pending;
		if (!pendingToken) throw new Error("no pending token");

		await engine.tokens.complete(pendingToken.id, { approved: true });
		const final = await engine.drive(first.runId);
		expect(final).toMatchObject({ status: "completed", output: "approved" });
	});

	it("runs.replay() re-triggers the same task/payload as a fresh run", async () => {
		const echo = defineTask({
			id: "echo-replay",
			run: async (payload: { n: number }) => payload.n * 2,
		});
		const engine = testEngine({ tasks: [echo] });

		const first = await engine.runInline(echo, { n: 21 });
		expect(first).toMatchObject({ status: "completed", output: 42 });

		const replayed = await engine.runs.replay(first.runId);
		expect(replayed.id).not.toBe(first.runId);
		const second = await engine.drive(replayed);
		expect(second).toMatchObject({ status: "completed", output: 42 });
	});
});
