import { describe, expect, it } from "vitest";
import {
	ConfigError,
	DuplicateStepError,
	ExecutionLostError,
	NestedStepError,
	NonDeterminismError,
	SerializationError,
} from "../../src/errors.js";
import { resolveChildTask } from "../../src/executor/execute-run.js";
import { ExecutionState } from "../../src/executor/execution.js";
import {
	createStepContext,
	DEFERRED_OUTPUT_BATCH_SIZE,
	type StepWaitDeps,
	SuspendSignal,
} from "../../src/executor/step-context.js";
import { buildRegistry } from "../../src/registry.js";
import type {
	ExecutionSnapshot,
	Json,
	LoadedStep,
	RunRecord,
	StepStore,
} from "../../src/store/types.js";
import { type AnyTask, defineTask } from "../../src/task.js";

const NOW = new Date("2026-01-01T00:00:00.000Z");

function runRecord(): RunRecord {
	return {
		id: "00000000-0000-0000-0000-000000000001",
		taskId: "t",
		queue: "q",
		payload: null,
		status: "running",
		output: null,
		error: null,
		attempt: 1,
		maxAttempts: 3,
		executionId: "e",
		enqueueSeq: 1,
		heartbeatAt: NOW,
		runAfter: null,
		parentRunId: null,
		parentWaitpointId: null,
		parentStepName: null,
		batchIndex: null,
		startedAt: NOW,
		finishedAt: null,
		createdAt: NOW,
		updatedAt: NOW,
		progress: null,
	};
}

interface FakeStore extends StepStore {
	inserted: { name: string; seqIndex: number; output: Json | null; kind: string }[];
	sleeps: { name: string; seqIndex: number; durationMs: number }[];
	completedSleeps: string[];
	outputLoads: [number, number][];
	lose: boolean;
}

function fakeStore(saved: LoadedStep[] = [], outputs = new Map<number, Json>()): FakeStore {
	const store: FakeStore = {
		inserted: [],
		sleeps: [],
		completedSleeps: [],
		outputLoads: [],
		lose: false,
		loadForExecution: () => Promise.reject(new Error("not used")),
		async loadOutputs(_runId, from, to) {
			store.outputLoads.push([from, to]);
			return saved
				.filter(s => s.seqIndex >= from && s.seqIndex <= to)
				.map(s => ({
					name: s.name,
					seqIndex: s.seqIndex,
					output: outputs.get(s.seqIndex) ?? null,
				}));
		},
		async insertCompleted(_owner, step) {
			if (store.lose) return "lost";
			store.inserted.push(step);
			return "ok";
		},
		async insertSleep(_owner, step) {
			if (store.lose) return { kind: "lost" };
			store.sleeps.push(step);
			return { kind: "inserted", wakeAt: new Date(NOW.getTime() + step.durationMs), delayMs: 0 };
		},
		async completeSleep(_owner, name) {
			if (store.lose) return "lost";
			store.completedSleeps.push(name);
			return "ok";
		},
		insertWaitRef: () => Promise.reject(new Error("not used")),
		list: () => Promise.reject(new Error("not used")),
	};
	return store;
}

const notUsed = () => Promise.reject(new Error("not used"));
const unusedWaits: StepWaitDeps = {
	runs: { triggerChildren: notUsed, listChildResults: notUsed },
	waitpoints: { createToken: notUsed, getToken: notUsed },
	maxPayloadBytes: 1024,
	resolveTask: () => {
		throw new Error("not used");
	},
	enqueueRun: notUsed,
	scheduleExpire: notUsed,
};

function setup(
	saved: LoadedStep[] = [],
	opts: { deferred?: Map<number, Json>; now?: Date; waits?: Partial<StepWaitDeps> } = {}
) {
	const state = new ExecutionState(runRecord(), "exec-1");
	const store = fakeStore(saved, opts.deferred);
	const snapshot: ExecutionSnapshot = {
		now: opts.now ?? NOW,
		steps: saved,
		outputsDeferred: opts.deferred !== undefined,
		waitpoints: [],
	};
	const ctx = createStepContext(
		state,
		{ steps: store, maxStepOutputBytes: 1024, waits: { ...unusedWaits, ...opts.waits } },
		snapshot
	);
	return { state, store, ctx, step: ctx.api };
}

const done = (name: string, seqIndex: number, output: Json | null = null): LoadedStep => ({
	name,
	seqIndex,
	kind: "run",
	status: "completed",
	wakeAt: null,
	output,
});

describe("StepContext", () => {
	it("assigns seq_index at call time, not completion time", async () => {
		const { step, store } = setup();
		const slow = step.run("a", async () => {
			await new Promise(r => setTimeout(r, 20));
			return "A";
		});
		const fast = step.run("b", () => "B");
		await Promise.all([slow, fast]);
		expect(store.inserted.map(s => [s.name, s.seqIndex])).toEqual([
			["b", 1],
			["a", 0],
		]);
	});

	it("returns saved outputs without calling fn (cache hit)", async () => {
		const { step, store } = setup([done("a", 0, { x: 1 })]);
		let called = false;
		const value = await step.run("a", () => {
			called = true;
			return { x: 2 };
		});
		expect(value).toEqual({ x: 1 });
		expect(called).toBe(false);
		expect(store.inserted).toEqual([]);
	});

	it("detects a different name at a saved seq_index and a saved name at another index", async () => {
		const a = setup([done("a", 0), done("b", 1)]);
		await expect(a.step.run("b", () => 1)).rejects.toBeInstanceOf(NonDeterminismError);
		expect(a.state.fatalError).toBeInstanceOf(NonDeterminismError);

		const c = setup([done("x", 1)]);
		await c.step.run("new", () => 0);
		await expect(c.step.run("y", () => 1)).rejects.toThrow(/was "x"/);
		const d = setup([done("x", 0)]);
		await d.step.run("x", () => 1);
		await expect(d.step.run("y", () => 1)).resolves.toBe(1);
		const e = setup([done("x", 1)]);
		await expect(e.step.run("x", () => 1)).rejects.toThrow(/was step #1/);
	});

	it("detects a step kind change at the same name and index", async () => {
		const { step } = setup([done("a", 0)]);
		await expect(step.sleep("a", 10)).rejects.toBeInstanceOf(NonDeterminismError);
	});

	it("compares the stored step kind exactly (createToken swapped for step.run)", async () => {
		const token: LoadedStep = { ...done("x", 0, { id: "wp" }), kind: "token" };
		const { step, state } = setup([token]);
		let called = false;
		await expect(
			step.run("x", () => {
				called = true;
				return 1;
			})
		).rejects.toThrow(/is a run step now, but was a token step/);
		expect(state.fatalError).toBeInstanceOf(NonDeterminismError);
		expect(called).toBe(false);
	});

	it("rejects duplicate names (fatal)", async () => {
		const { step, state, store } = setup();
		await step.run("a", () => 1);
		await expect(step.run("a", () => 2)).rejects.toBeInstanceOf(DuplicateStepError);
		expect(state.fatalError).toBeInstanceOf(DuplicateStepError);
		expect(store.inserted).toEqual([{ name: "a", seqIndex: 0, output: 1, kind: "run" }]);
	});

	it("rejects invalid names as a fatal error without consuming a seq_index", async () => {
		const { step, state, store } = setup();
		await expect(step.run("bad#name", () => 1)).rejects.toThrow(/reserved/);
		expect(state.fatalError).toBeInstanceOf(ConfigError);
		expect(store.inserted).toEqual([]);
	});

	it("rejects a non-function fn as a fatal error without consuming a seq_index", async () => {
		const { step, state, store } = setup();
		// @ts-expect-error -- exercising the runtime check for non-TS callers.
		await expect(step.run("a", "not a function")).rejects.toThrow(/fn must be a function/);
		expect(state.fatalError).toBeInstanceOf(ConfigError);
		expect(store.inserted).toEqual([]);
	});

	it("rejects an invalid sleep duration as a fatal error without consuming a seq_index", async () => {
		const { step, state, store } = setup();
		// @ts-expect-error -- exercising the runtime check for non-TS callers.
		await expect(step.sleep("nap", "not-a-duration")).rejects.toThrow(/invalid duration/);
		expect(state.fatalError).toBeInstanceOf(ConfigError);
		expect(store.sleeps).toEqual([]);
	});

	it("rejects non-array payloads for batchTriggerAndWait as a fatal error", async () => {
		const { step, state } = setup();
		await expect(
			// @ts-expect-error -- exercising the runtime check for non-TS callers.
			step.batchTriggerAndWait("kids", {} as never, "not-an-array")
		).rejects.toThrow(/payloads must be an array/);
		expect(state.fatalError).toBeInstanceOf(ConfigError);
	});

	it("after a fatal invalid-argument error, later step calls rethrow it without running fn", async () => {
		const { step, state } = setup();
		// @ts-expect-error -- exercising the runtime check for non-TS callers.
		const error = await step.run("a", "not a function").catch((e: unknown) => e);
		expect(error).toBeInstanceOf(ConfigError);
		let called = false;
		await expect(
			step.run("b", () => {
				called = true;
				return 1;
			})
		).rejects.toBe(error);
		expect(called).toBe(false);
		expect(state.fatalError).toBe(error);
	});

	it("after a fatal error, later step calls rethrow it without running fn", async () => {
		const { step, state } = setup([done("a", 0)]);
		const error = await step.run("b", () => 1).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(NonDeterminismError);
		// User code caught it and keeps going: every later step.* rethrows the fatal error.
		let called = false;
		await expect(
			step.run("c", () => {
				called = true;
				return 1;
			})
		).rejects.toBe(error);
		await expect(step.sleep("d", 10)).rejects.toBe(error);
		expect(called).toBe(false);
		expect(state.fatalError).toBe(error);
	});

	it("rejects steps called inside a step.run fn (NestedStepError, fatal, nothing saved)", async () => {
		const { step, state, store } = setup();
		let innerCalled = false;
		const outer = step.run("outer", async () => {
			await Promise.resolve();
			await step.run("inner", () => {
				innerCalled = true;
				return 1;
			});
			return "outer";
		});
		await expect(outer).rejects.toBeInstanceOf(NestedStepError);
		await expect(outer).rejects.toThrow(
			'ctx.step.run("inner") called inside step.run("outer"): nested steps are not supported; ' +
				"call steps from the task body"
		);
		expect(state.fatalError).toBeInstanceOf(NestedStepError);
		expect(innerCalled).toBe(false);
		expect(store.inserted).toEqual([]);
	});

	it("a nested step caught inside the outer fn still fails the run and saves nothing", async () => {
		const { step, state, store } = setup();
		const outer = step.run("outer", async () => {
			await step.sleep("inner-nap", 10).catch(() => undefined);
			return "outer";
		});
		await expect(outer).rejects.toBeInstanceOf(NestedStepError);
		expect(state.fatalError).toBeInstanceOf(NestedStepError);
		expect(store.inserted).toEqual([]);
		expect(store.sleeps).toEqual([]);
	});

	it("concurrent top-level steps started while another step fn runs are not nested", async () => {
		const { step, store } = setup();
		let release!: () => void;
		const a = step.run(
			"a",
			() =>
				new Promise<string>(r => {
					release = () => r("A");
				})
		);
		const b = step.run("b", () => "B");
		release();
		await expect(Promise.all([a, b])).resolves.toEqual(["A", "B"]);
		expect(store.inserted.map(s => s.name).sort()).toEqual(["a", "b"]);
	});

	it("startInternal skips user-name validation but registers and tracks the step", async () => {
		const { ctx, state } = setup();
		let release!: () => void;
		const p = ctx.startInternal("wait", "approval#wait", async reg => {
			await new Promise<void>(r => {
				release = r;
			});
			return reg;
		});
		expect(state.inFlight.size).toBe(1);
		release();
		const reg = await p;
		expect(reg).toMatchObject({ kind: "wait", name: "approval#wait", seqIndex: 0 });
		expect(state.fatalError).toBeUndefined();
	});

	it("normalizes void to null and validates the output size", async () => {
		const { step, store } = setup();
		await expect(step.run("v", async () => {})).resolves.toBeNull();
		await expect(step.run("big", () => "x".repeat(2000))).rejects.toThrow(
			/output of step "big".*exceeding/
		);
		expect(store.inserted).toEqual([{ name: "v", seqIndex: 0, output: null, kind: "run" }]);
	});

	it("a lost insert throws ExecutionLostError and aborts the signal", async () => {
		const { step, store, state } = setup();
		store.lose = true;
		await expect(step.run("a", () => 1)).rejects.toBeInstanceOf(ExecutionLostError);
		expect(state.signal.aborted).toBe(true);
		expect(state.abortKind).toBe("lost");
		// Later steps are refused at the boundary, fn not called.
		let called = false;
		await expect(
			step.run("b", () => {
				called = true;
				return 1;
			})
		).rejects.toBeInstanceOf(ExecutionLostError);
		expect(called).toBe(false);
	});

	it("a first sleep records the step and suspends; later calls rethrow the same signal", async () => {
		const { step, store, state } = setup();
		let suspendedEvents = 0;
		state.onSuspend(() => suspendedEvents++);
		const error = await step.sleep("nap", "2s").catch((e: unknown) => e);
		expect(error).toBeInstanceOf(SuspendSignal);
		expect(state.suspended).toBe(true);
		expect(suspendedEvents).toBe(1);
		expect(store.sleeps).toEqual([{ name: "nap", seqIndex: 0, durationMs: 2000 }]);
		let called = false;
		await expect(
			step.run("after", () => {
				called = true;
				return 1;
			})
		).rejects.toBe(error);
		expect(called).toBe(false);
	});

	it("a replayed sleep completes when due by the DB clock and suspends otherwise", async () => {
		const due: LoadedStep = {
			name: "nap",
			seqIndex: 0,
			kind: "sleep",
			status: "waiting",
			wakeAt: new Date(NOW.getTime() - 1),
			output: null,
		};
		const a = setup([due]);
		await expect(a.step.sleep("nap", "1h")).resolves.toBeUndefined();
		expect(a.store.completedSleeps).toEqual(["nap"]);

		const future = { ...due, wakeAt: new Date(NOW.getTime() + 60_000) };
		const b = setup([future]);
		await expect(b.step.sleep("nap", "1h")).rejects.toBeInstanceOf(SuspendSignal);
		expect(b.store.completedSleeps).toEqual([]);
		expect(b.store.sleeps).toEqual([]);

		const c = setup([{ ...due, status: "completed" }]);
		await expect(c.step.sleep("nap", "1h")).resolves.toBeUndefined();
		expect(c.store.completedSleeps).toEqual([]);
	});

	it("loads deferred outputs lazily in aligned seq_index batches", async () => {
		const total = DEFERRED_OUTPUT_BATCH_SIZE * 2 + 5;
		const saved = Array.from({ length: total }, (_, i) => done(`s${i}`, i));
		const outputs = new Map<number, Json>(saved.map(s => [s.seqIndex, { i: s.seqIndex }]));
		const { step, store } = setup(saved, { deferred: outputs });

		// Concurrent calls in the first batch share one load.
		const first = await Promise.all([step.run("s0", () => -1), step.run("s1", () => -1)]);
		expect(first).toEqual([{ i: 0 }, { i: 1 }]);
		expect(store.outputLoads).toEqual([[0, DEFERRED_OUTPUT_BATCH_SIZE - 1]]);

		for (let i = 2; i < total; i++) {
			expect(await step.run(`s${i}`, () => -1)).toEqual({ i });
		}
		expect(store.outputLoads).toEqual([
			[0, DEFERRED_OUTPUT_BATCH_SIZE - 1],
			[DEFERRED_OUTPUT_BATCH_SIZE, 2 * DEFERRED_OUTPUT_BATCH_SIZE - 1],
			[2 * DEFERRED_OUTPUT_BATCH_SIZE, 3 * DEFERRED_OUTPUT_BATCH_SIZE - 1],
		]);
		expect(store.inserted).toEqual([]);
	});

	it("tracks steps as in flight until they settle", async () => {
		const { step, state } = setup();
		let release!: () => void;
		const p = step.run(
			"a",
			() =>
				new Promise<number>(r => {
					release = () => r(1);
				})
		);
		expect(state.inFlight.size).toBe(1);
		release();
		await p;
		await Promise.resolve();
		expect(state.inFlight.size).toBe(0);
	});
	describe("rejections of calls that are never awaited are marked handled", () => {
		/** Collects `unhandledRejection`s raised while `fn` runs (and a few ticks after). */
		async function unhandledDuring(fn: () => void): Promise<unknown[]> {
			const seen: unknown[] = [];
			const listener = (reason: unknown) => seen.push(reason);
			process.on("unhandledRejection", listener);
			try {
				fn();
				await new Promise(r => setTimeout(r, 20));
			} finally {
				process.off("unhandledRejection", listener);
			}
			return seen;
		}

		it("invalid name, after abort, after suspension, invalid token handle", async () => {
			const invalid = setup();
			expect(
				await unhandledDuring(() => {
					void invalid.step.run("bad#name", () => 1);
					void invalid.step.trigger("bad:name", {} as never, null as never);
				})
			).toEqual([]);

			const aborted = setup();
			aborted.state.abort("canceled");
			expect(
				await unhandledDuring(() => {
					void aborted.step.run("a", () => 1);
					void aborted.step.sleep("s", "1s");
					void aborted.step.waitForToken("tok");
				})
			).toEqual([]);

			const suspended = setup();
			await expect(suspended.step.sleep("s", "1s")).rejects.toBeInstanceOf(SuspendSignal);
			expect(
				await unhandledDuring(() => {
					void suspended.step.run("a", () => 1);
					void suspended.step.createToken("t");
				})
			).toEqual([]);

			const badHandle = setup();
			expect(
				await unhandledDuring(() => {
					void badHandle.step.waitForToken({ id: 1, name: "x" } as never);
					void badHandle.step.waitForToken({ id: "1", name: "x#y" });
				})
			).toEqual([]);
		});

		it("still rejects for callers that await", async () => {
			const { step, state } = setup();
			state.abort("canceled");
			await expect(step.run("a", () => 1)).rejects.toThrow(/canceled/);
			const other = setup();
			await expect(other.step.run("a:b", () => 1)).rejects.toBeInstanceOf(ConfigError);
		});
	});
	describe("child task and child payload arguments (fatal, no retry)", () => {
		const spec = { id: "child", queue: "default", maxAttempts: 3 };
		const child = { id: "child", options: {} } as unknown as AnyTask;

		it("a task the resolver rejects is a fatal error", async () => {
			const { step, state } = setup([], {
				waits: {
					resolveTask: () => {
						throw new ConfigError("malformed task");
					},
				},
			});
			await expect(step.trigger("t", child, 1)).rejects.toThrow(/malformed task/);
			expect(state.fatalError).toBeInstanceOf(ConfigError);
		});

		for (const [label, payload] of [
			["a Date", { at: new Date(0) }],
			["an oversized value", "x".repeat(2_000)],
		] as const) {
			it(`a child payload with ${label} is a fatal SerializationError (all three methods)`, async () => {
				for (const call of [
					(s: ReturnType<typeof setup>["step"]) => s.trigger("t", child, payload as never),
					(s: ReturnType<typeof setup>["step"]) => s.triggerAndWait("t", child, payload as never),
					(s: ReturnType<typeof setup>["step"]) =>
						s.batchTriggerAndWait("t", child, [1, payload] as never),
				]) {
					const { step, state } = setup([], { waits: { resolveTask: () => spec } });
					await expect(call(step)).rejects.toBeInstanceOf(SerializationError);
					expect(state.fatalError).toBeInstanceOf(SerializationError);
					// Later steps rethrow it: the code bug stops the run.
					await expect(step.run("next", () => 1)).rejects.toBe(state.fatalError);
				}
			});
		}
	});
});

describe("resolveChildTask", () => {
	const registered = defineTask({
		id: "known",
		queue: { name: "known-q" },
		retry: { maxAttempts: 7 },
		run: async (p: number) => p,
	});
	const registry = buildRegistry([registered]);

	it("uses the registered task when its id is registered in this process", () => {
		const stale = { id: "known", options: { queue: { name: "other-q" } } } as unknown as AnyTask;
		expect(resolveChildTask(registry, stale)).toEqual({
			id: "known",
			queue: "known-q",
			maxAttempts: 7,
		});
	});

	it("builds the spec from the task object itself when it is not registered here", () => {
		const remote = defineTask({
			id: "remote",
			queue: { name: "remote-q" },
			retry: { maxAttempts: 2 },
			run: async () => null,
		});
		expect(resolveChildTask(registry, remote)).toEqual({
			id: "remote",
			queue: "remote-q",
			maxAttempts: 2,
		});
		const bare = defineTask({ id: "bare", run: async () => null });
		expect(resolveChildTask(registry, bare)).toEqual({
			id: "bare",
			queue: "default",
			maxAttempts: 3,
		});
	});

	it("rejects a malformed task object or an invalid queue name with a ConfigError", () => {
		for (const bad of [
			null,
			undefined,
			"remote",
			{},
			{ id: 1, options: {} },
			{ id: "", options: {} },
			{ id: "remote" },
			{ id: "remote", options: null },
			{ id: "remote", options: { queue: { name: "Bad Queue" } } },
			{ id: "remote", options: { queue: { name: "system" } } },
			{ id: "remote", options: { queue: { name: 5 } } },
			{ id: "remote", options: { retry: { maxAttempts: 0 } } },
		]) {
			expect(() => resolveChildTask(registry, bad as unknown as AnyTask)).toThrow(ConfigError);
		}
	});
});
