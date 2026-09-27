import type { Job } from "bullmq";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { SystemJobDelayed } from "../../src/executor/system.js";
import {
	CLAIM_DRAIN_TIMEOUT_MS,
	createQueueWorker,
	createSystemWorker,
	MIN_IDLE_SLEEP_MS,
	type PgSystemJob,
	type QueueWorkerOptions,
	type SystemWorkerOptions,
} from "../../src/pg/workers.js";
import type {
	ClaimRunsInput,
	Json,
	RunRecord,
	SystemJobRecord,
	SystemJobStore,
} from "../../src/store/types.js";

const T0 = new Date("2026-01-01T00:00:00.000Z").getTime();

type Call = { level: string; fields: Record<string, unknown>; message: string };

function recordingLogger(): EngineLogger & { calls: Call[] } {
	const calls: Call[] = [];
	const make = (level: string) => (fields: Record<string, unknown>, message: string) => {
		calls.push({ level, fields, message });
	};
	return {
		calls,
		debug: make("debug"),
		info: make("info"),
		warn: make("warn"),
		error: make("error"),
	};
}

function run(id: string): RunRecord {
	return {
		id,
		taskId: "task-1",
		queue: "q",
		payload: null,
		status: "running",
		output: null,
		error: null,
		attempt: 1,
		maxAttempts: 3,
		executionId: "exec",
		enqueueSeq: 1,
		heartbeatAt: null,
		runAfter: new Date(T0),
		parentRunId: null,
		parentWaitpointId: null,
		parentStepName: null,
		batchIndex: null,
		startedAt: null,
		finishedAt: null,
		createdAt: new Date(T0),
		updatedAt: new Date(T0),
		progress: null,
		debounceKey: null,
		concurrencyKey: null,
	};
}

function deferred<T = void>() {
	let resolve!: (v: T) => void;
	let reject!: (e: unknown) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
}

/** A fake `RunClaimStore` whose claims pop from a backlog the test fills. */
function fakeClaim() {
	const backlog: RunRecord[] = [];
	const calls: ClaimRunsInput[] = [];
	let nextDue: number | null = null;
	let hold: Promise<void> | undefined;
	let failNext = 0;
	const claim = {
		claimRuns: vi.fn(async (input: ClaimRunsInput) => {
			calls.push(input);
			if (hold) await hold;
			if (failNext > 0) {
				failNext--;
				throw new Error("db down");
			}
			return backlog.splice(0, input.limit);
		}),
		nextDueInMs: vi.fn(async () => nextDue),
	};
	return {
		claim,
		backlog,
		calls,
		setNextDue(d: number | null) {
			nextDue = d;
		},
		holdClaims(p: Promise<void> | undefined) {
			hold = p;
		},
		failClaims(n: number) {
			failNext = n;
		},
	};
}

/** A `process` whose runs finish only when the test says so. */
function controlledProcess() {
	const running = new Map<string, ReturnType<typeof deferred<void>>>();
	const process = vi.fn((r: RunRecord) => {
		const d = deferred();
		running.set(r.id, d);
		return d.promise;
	});
	return {
		process,
		running,
		finish(id: string) {
			running.get(id)?.resolve();
		},
	};
}

const flush = () => vi.advanceTimersByTimeAsync(0);

function queueWorker(
	claim: QueueWorkerOptions["claim"],
	process: QueueWorkerOptions["process"],
	overrides: Partial<QueueWorkerOptions> = {}
) {
	const logger = recordingLogger();
	const worker = createQueueWorker({
		queue: "q",
		concurrency: 2,
		claim,
		process,
		polling: { intervalMs: 1000, jitterMs: 0 },
		logger,
		...overrides,
	});
	return { worker, logger };
}

beforeEach(() => {
	vi.useFakeTimers();
	vi.setSystemTime(T0);
});
afterEach(() => {
	vi.useRealTimers();
});

describe("queue worker", () => {
	it("claims up to the free slots and runs them concurrently", async () => {
		const c = fakeClaim();
		c.backlog.push(run("r1"), run("r2"), run("r3"));
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		worker.start();
		await flush();
		expect(c.calls).toEqual([expect.objectContaining({ queue: "q", limit: 2 })]);
		expect(p.process).toHaveBeenCalledTimes(2);
		expect(worker.inFlight).toBe(2);

		p.finish("r1");
		await flush();
		expect(c.calls[1]).toMatchObject({ queue: "q", limit: 1 });
		expect(p.process).toHaveBeenCalledTimes(3);
		expect(worker.inFlight).toBe(2);
		worker.stopClaiming();
	});

	it("after an empty claim sleeps the DB-computed delay (bounded by intervalMs)", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		const dues: Array<number | null> = [300, 5000, null];
		c.claim.nextDueInMs.mockImplementation(async () => dues.shift() ?? null);

		const expectNextClaimAfter = async (ms: number) => {
			const before = c.calls.length;
			await vi.advanceTimersByTimeAsync(ms - 1);
			expect(c.calls.length).toBe(before);
			await vi.advanceTimersByTimeAsync(1);
			expect(c.calls.length).toBe(before + 1);
		};
		worker.start();
		await flush();
		expect(c.calls).toHaveLength(1);
		await expectNextClaimAfter(300); // due soon: not the full interval
		await expectNextClaimAfter(1000); // due far away: bounded by intervalMs
		await expectNextClaimAfter(1000); // nothing due: intervalMs
		worker.stopClaiming();
	});

	it("uses only the DB-computed delay: an app clock far ahead of the DB changes nothing", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		// `now()` (the app clock) is an hour ahead; the DB says the next row is due in 300 ms.
		const { worker } = queueWorker(c.claim, p.process, { now: () => Date.now() + 3_600_000 });
		c.setNextDue(300);
		worker.start();
		await flush();
		expect(c.calls).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(299);
		expect(c.calls).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(1);
		expect(c.calls).toHaveLength(2);
		worker.stopClaiming();
	});

	it("never spins: a 0 ms due delay with jitter 0 still sleeps MIN_IDLE_SLEEP_MS", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		c.setNextDue(0);
		worker.start();
		await flush();
		expect(c.calls).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(MIN_IDLE_SLEEP_MS - 1);
		expect(c.calls).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(1);
		expect(c.calls).toHaveLength(2);
		// 100 ms later: at most one pass per MIN_IDLE_SLEEP_MS, not a hot loop.
		await vi.advanceTimersByTimeAsync(100);
		expect(c.calls.length).toBeLessThanOrEqual(2 + 100 / MIN_IDLE_SLEEP_MS);
		worker.stopClaiming();
	});

	it("adds a jitter in [0, jitterMs) to the sleep", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process, {
			polling: { intervalMs: 1000, jitterMs: 200 },
			random: () => 0.5,
		});
		worker.start();
		await flush();
		await vi.advanceTimersByTimeAsync(1099);
		expect(c.calls).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(1);
		expect(c.calls).toHaveLength(2);
		worker.stopClaiming();
	});

	it("wake() during a claim schedules exactly one extra pass", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		const gate = deferred();
		c.holdClaims(gate.promise);
		worker.start();
		await flush();
		expect(c.calls).toHaveLength(1);
		for (let i = 0; i < 5; i++) worker.wake();
		c.holdClaims(undefined);
		gate.resolve();
		await flush();
		expect(c.calls).toHaveLength(2);
		// ...and then it sleeps normally (no busy loop).
		await vi.advanceTimersByTimeAsync(999);
		expect(c.calls).toHaveLength(2);
		worker.stopClaiming();
	});

	it("wake() while sleeping claims right away and cancels the pending sleep", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		worker.start();
		await flush();
		worker.wake();
		await flush();
		expect(c.calls).toHaveLength(2);
		await vi.advanceTimersByTimeAsync(999);
		expect(c.calls).toHaveLength(2);
		await vi.advanceTimersByTimeAsync(1);
		expect(c.calls).toHaveLength(3);
		worker.stopClaiming();
	});

	it("wake() with no free slot does not query", async () => {
		const c = fakeClaim();
		c.backlog.push(run("r1"));
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process, { concurrency: 1 });
		worker.start();
		await flush();
		expect(worker.inFlight).toBe(1);
		const calls = c.claim.claimRuns.mock.calls.length;
		worker.wake();
		worker.wake();
		await flush();
		await vi.advanceTimersByTimeAsync(5000);
		expect(c.claim.claimRuns.mock.calls.length).toBe(calls);
		expect(c.claim.nextDueInMs).not.toHaveBeenCalled();
		worker.stopClaiming();
	});

	it("a forged/unknown wake never throws or stops the loop", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		c.failClaims(1);
		worker.start();
		expect(() => worker.wake()).not.toThrow();
		await flush();
		expect(c.calls).toHaveLength(1);
		// While backing off, a wake does not bypass the backoff (a NOTIFY storm cannot hammer a
		// failing database), and it never throws.
		expect(() => worker.wake()).not.toThrow();
		await flush();
		expect(c.calls).toHaveLength(1);
		await vi.advanceTimersByTimeAsync(500);
		expect(c.calls).toHaveLength(2);
		// Healthy again: the next wake claims normally.
		c.backlog.push(run("r1"));
		worker.wake();
		await flush();
		expect(p.process).toHaveBeenCalledWith(expect.objectContaining({ id: "r1" }));
		worker.stopClaiming();
	});

	it("claim errors back off (500 ms, 1 s, ... 30 s) and warn once per 10 s", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker, logger } = queueWorker(c.claim, p.process);
		c.failClaims(100);
		worker.start();
		await flush();
		const times: number[] = [Date.now() - T0];
		const expected = [500, 1000, 2000, 4000, 8000, 16000, 30000, 30000];
		for (const gap of expected) {
			const before = c.calls.length;
			await vi.advanceTimersByTimeAsync(gap - 1);
			expect(c.calls.length).toBe(before);
			await vi.advanceTimersByTimeAsync(1);
			expect(c.calls.length).toBe(before + 1);
			times.push(Date.now() - T0);
		}
		const warns = logger.calls.filter(l => l.level === "warn");
		// Claims at t = 0, .5, 1.5, 3.5, 7.5, 15.5, 31.5, 61.5, 91.5 s → warns at 0, 15.5, 31.5, 61.5, 91.5.
		expect(warns.length).toBe(5);
		expect(warns[0]?.fields).toMatchObject({ queue: "q", backoffMs: 500 });
		// Recovery resets the backoff.
		c.failClaims(0);
		await vi.advanceTimersByTimeAsync(30000);
		const before = c.calls.length;
		await vi.advanceTimersByTimeAsync(1000);
		expect(c.calls.length).toBe(before + 1);
		worker.stopClaiming();
	});

	it("stopClaiming() then drain() waits in-flight runs and returns the stragglers after timeout", async () => {
		const c = fakeClaim();
		c.backlog.push(run("r1"), run("r2"));
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		worker.start();
		await flush();
		worker.stopClaiming();
		worker.wake();
		const drained = worker.drain(1000);
		let result: RunRecord[] | undefined;
		void drained.then(r => {
			result = r;
		});
		p.finish("r1");
		await flush();
		expect(result).toBeUndefined();
		await vi.advanceTimersByTimeAsync(1000);
		expect(result?.map(r => r.id)).toEqual(["r2"]);
		expect(c.calls).toHaveLength(1); // nothing claimed after the stop signal

		p.finish("r2");
		await flush();
		expect(await worker.drain(1000)).toEqual([]);
	});

	it("drain() resolves as soon as every in-flight run ends", async () => {
		const c = fakeClaim();
		c.backlog.push(run("r1"));
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		worker.start();
		await flush();
		worker.stopClaiming();
		const drained = worker.drain(60_000);
		p.finish("r1");
		await flush();
		await expect(drained).resolves.toEqual([]);
		expect(vi.getTimerCount()).toBe(0);
	});

	it("drain() waits for a claim that was in flight when stopClaiming() ran and includes its runs", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		const gate = deferred();
		c.holdClaims(gate.promise);
		worker.start();
		await flush();
		worker.stopClaiming();
		let result: RunRecord[] | undefined;
		void worker.drain(1000).then(r => {
			result = r;
		});
		await vi.advanceTimersByTimeAsync(10);
		expect(result).toBeUndefined();

		c.backlog.push(run("r1"));
		gate.resolve();
		await flush();
		expect(p.process).toHaveBeenCalledWith(expect.objectContaining({ id: "r1" }));
		expect(result).toBeUndefined();
		p.finish("r1");
		await flush();
		expect(result).toEqual([]);
		expect(c.calls).toHaveLength(1);
		expect(c.claim.nextDueInMs).not.toHaveBeenCalled();
	});

	it("drain() keeps waiting for a claim in flight past its timeoutMs (bounded separately) and launches its runs", async () => {
		// A claim slower than `shutdownTimeoutMs` must not be abandoned: the runs it brings would be
		// launched after stop() resolved, outside the drain and the hand-back (spec §3.4).
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		const gate = deferred();
		c.holdClaims(gate.promise);
		worker.start();
		await flush();
		worker.stopClaiming();
		let result: RunRecord[] | undefined;
		void worker.drain(100).then(r => {
			result = r;
		});
		await vi.advanceTimersByTimeAsync(400);
		expect(result).toBeUndefined();

		c.backlog.push(run("r1"));
		gate.resolve();
		await flush();
		expect(p.process).toHaveBeenCalledWith(expect.objectContaining({ id: "r1" }));
		// The executions' own budget (100 ms) already ran out: r1 is returned as still in flight.
		expect(result?.map(r => r.id)).toEqual(["r1"]);
		p.finish("r1");
		await flush();
	});

	it("drain() gives up on a claim that never settles after the claim drain bound", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		c.holdClaims(new Promise<void>(() => {}));
		worker.start();
		await flush();
		worker.stopClaiming();
		let result: RunRecord[] | undefined;
		void worker.drain(100).then(r => {
			result = r;
		});
		await vi.advanceTimersByTimeAsync(4_900);
		expect(result).toBeUndefined();
		await vi.advanceTimersByTimeAsync(200);
		expect(result).toEqual([]);
	});

	it("drain() hands the claim in flight its deadline: remainingMs counts down the claim drain bound", async () => {
		const c = fakeClaim();
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process);
		const gate = deferred();
		c.holdClaims(gate.promise);
		worker.start();
		await flush();
		const input = c.calls[0];
		expect(input?.remainingMs).toBeTypeOf("function");
		// No deadline while the worker runs normally.
		expect(input?.remainingMs?.()).toBeUndefined();

		worker.stopClaiming();
		void worker.drain(100);
		expect(input?.remainingMs?.()).toBe(CLAIM_DRAIN_TIMEOUT_MS);
		await vi.advanceTimersByTimeAsync(1_000);
		expect(input?.remainingMs?.()).toBe(CLAIM_DRAIN_TIMEOUT_MS - 1_000);
		await vi.advanceTimersByTimeAsync(4_500);
		expect(input?.remainingMs?.()).toBeLessThan(0);
		gate.resolve();
		await flush();
	});

	it("a drain() budget longer than the claim bound is the claim's deadline too", async () => {
		const c = fakeClaim();
		const { worker } = queueWorker(c.claim, controlledProcess().process);
		const gate = deferred();
		c.holdClaims(gate.promise);
		worker.start();
		await flush();
		worker.stopClaiming();
		void worker.drain(20_000);
		expect(c.calls[0]?.remainingMs?.()).toBe(20_000);
		gate.resolve();
		await flush();
	});

	it("globalConcurrency is passed to claimRuns and onSlotFreed fires when a run ends", async () => {
		const c = fakeClaim();
		c.backlog.push(run("r1"));
		const p = controlledProcess();
		const onSlotFreed = vi.fn();
		const { worker } = queueWorker(c.claim, p.process, { globalConcurrency: 3, onSlotFreed });
		worker.start();
		await flush();
		expect(c.calls[0]).toMatchObject({ queue: "q", limit: 2, globalConcurrency: 3 });
		expect(onSlotFreed).not.toHaveBeenCalled();
		p.finish("r1");
		await flush();
		expect(onSlotFreed).toHaveBeenCalledTimes(1);
		worker.stopClaiming();
	});

	it("a process() rejection is logged and frees the slot", async () => {
		const c = fakeClaim();
		c.backlog.push(run("r1"));
		const boom = new Error("boom");
		const { worker, logger } = queueWorker(c.claim, async () => Promise.reject(boom), {
			concurrency: 1,
		});
		worker.start();
		await flush();
		expect(worker.inFlight).toBe(0);
		expect(logger.calls.find(l => l.level === "error")?.fields).toMatchObject({
			runId: "r1",
			err: boom,
		});
		worker.stopClaiming();
	});

	it("setConcurrency() raises the free slots and claims right away", async () => {
		const c = fakeClaim();
		c.backlog.push(run("r1"), run("r2"));
		const p = controlledProcess();
		const { worker } = queueWorker(c.claim, p.process, { concurrency: 1 });
		worker.start();
		await flush();
		expect(worker.inFlight).toBe(1);
		worker.setConcurrency(3);
		expect(worker.concurrency).toBe(3);
		await flush();
		expect(c.calls.at(-1)).toMatchObject({ queue: "q", limit: 2 });
		expect(worker.inFlight).toBe(2);
		worker.stopClaiming();
	});
});

// ---------------------------------------------------------------------------
// System worker
// ---------------------------------------------------------------------------

function sysRecord(overrides: Partial<SystemJobRecord> = {}): SystemJobRecord {
	return {
		jobId: "wp-w1-expire",
		name: "waitpoint-expire",
		data: { waitpointId: "w1" },
		runAfter: new Date(T0 - 5),
		attempts: 0,
		maxAttempts: 1,
		lockedBy: "lock-1",
		...overrides,
	};
}

function fakeSystemJobs(dbNow: Date) {
	const backlog: SystemJobRecord[] = [];
	const store = {
		claim: vi.fn(async (limit: number, _lockMs: number) => backlog.splice(0, limit)),
		nextDueInMs: vi.fn(async () => null),
		complete: vi.fn(async (_id: string, _lock: string) => true),
		delay: vi.fn(async (_id: string, _lock: string, _ms: number) => true),
		fail: vi.fn(
			async (_id: string, _lock: string, _ms: number): Promise<"retried" | "exhausted" | "lost"> =>
				"retried"
		),
		rescheduleTick: vi.fn(
			async (_id: string, _lock: string, _next: { runAt: Date; prevMillis: number }) => true
		),
		remove: vi.fn(async (_id: string, _o?: { evenIfLocked?: boolean }) => true),
		now: vi.fn(async () => dbNow),
		upsert: vi.fn(),
		upsertScheduleTick: vi.fn(),
		get: vi.fn(),
		listByName: vi.fn(),
	};
	return { store, backlog };
}

function systemWorker(
	systemJobs: unknown,
	process: SystemWorkerOptions["process"],
	overrides: Partial<SystemWorkerOptions> = {}
) {
	const logger = recordingLogger();
	const worker = createSystemWorker({
		concurrency: 2,
		systemJobs: systemJobs as SystemJobStore,
		lockMs: 30_000,
		process,
		backoffMs: attempt => Math.min(60_000, 1000 * 2 ** attempt),
		polling: { intervalMs: 1000, jitterMs: 0 },
		logger,
		...overrides,
	});
	return { worker, logger };
}

function tickRecord(overrides: Partial<SystemJobRecord> = {}, data: Record<string, unknown> = {}) {
	return sysRecord({
		jobId: "schedule-tick:daily",
		name: "schedule-tick",
		data: {
			scheduleId: "daily",
			cron: "* * * * *",
			schema: "mt",
			prevMillis: T0 - 60_000,
			...data,
		} as Json,
		maxAttempts: 5,
		...overrides,
	});
}

describe("system worker", () => {
	it("delivers a job object with id/name/data/opts.prevMillis/repeatJobKey/token and completes on success", async () => {
		const { store, backlog } = fakeSystemJobs(new Date(T0));
		backlog.push(sysRecord({ attempts: 2, maxAttempts: 3 }));
		const seen: PgSystemJob[] = [];
		const { worker } = systemWorker(store, async (_name, _data, job: Job) => {
			seen.push(job as unknown as PgSystemJob);
		});
		worker.start();
		await flush();
		expect(store.claim).toHaveBeenCalledWith(2, 30_000);
		const job = seen[0];
		expect(job).toMatchObject({
			id: "wp-w1-expire",
			name: "waitpoint-expire",
			data: { waitpointId: "w1" },
			timestamp: T0 - 5,
			token: "lock-1",
			attemptsMade: 2,
		});
		expect(job?.repeatJobKey).toBeUndefined();
		expect(job?.opts.prevMillis).toBeUndefined();
		expect(store.complete).toHaveBeenCalledWith("wp-w1-expire", "lock-1");
		worker.stopClaiming();
	});

	it("a schedule-tick job carries repeatJobKey and opts.prevMillis", async () => {
		const { store, backlog } = fakeSystemJobs(new Date(T0));
		backlog.push(tickRecord());
		const seen: PgSystemJob[] = [];
		const { worker } = systemWorker(store, async (_n, _d, job: Job) => {
			seen.push(job as unknown as PgSystemJob);
		});
		worker.start();
		await flush();
		expect(seen[0]?.repeatJobKey).toBe("daily");
		expect(seen[0]?.opts).toEqual({ prevMillis: T0 - 60_000, repeatJobKey: "daily" });
		worker.stopClaiming();
	});

	it("SystemJobDelayed after moveToDelayed re-delays in place; other errors fail with backoff; exhausted logs error", async () => {
		const { store, backlog } = fakeSystemJobs(new Date(T0));
		backlog.push(sysRecord({ jobId: "a" }));
		const { worker, logger } = systemWorker(store, async (_n, _d, job: Job) => {
			await job.moveToDelayed(Date.now() + 250, job.token);
			throw new SystemJobDelayed();
		});
		worker.start();
		await flush();
		expect(store.delay).toHaveBeenCalledWith("a", "lock-1", 250);
		expect(store.complete).not.toHaveBeenCalled();
		expect(store.fail).not.toHaveBeenCalled();
		expect(logger.calls.filter(l => l.level === "error" || l.level === "warn")).toEqual([]);
		worker.stopClaiming();

		// A plain error: fail with the backoff of the current attempt → warn.
		const s2 = fakeSystemJobs(new Date(T0));
		s2.backlog.push(sysRecord({ jobId: "b", attempts: 1, maxAttempts: 3 }));
		const boom = new Error("boom");
		const w2 = systemWorker(s2.store, async () => Promise.reject(boom));
		w2.worker.start();
		await flush();
		expect(s2.store.fail).toHaveBeenCalledWith("b", "lock-1", 2000);
		expect(w2.logger.calls.find(l => l.level === "warn")?.fields).toMatchObject({
			jobId: "b",
			err: boom,
		});
		w2.worker.stopClaiming();

		// Exhausted: error.
		const s3 = fakeSystemJobs(new Date(T0));
		s3.store.fail.mockResolvedValue("exhausted");
		s3.backlog.push(sysRecord({ jobId: "c" }));
		const w3 = systemWorker(s3.store, async () => Promise.reject(boom));
		w3.worker.start();
		await flush();
		expect(w3.logger.calls.find(l => l.level === "error")?.fields).toMatchObject({
			jobId: "c",
			err: boom,
		});
		w3.worker.stopClaiming();
	});

	it("schedule-tick: success and exhaustion both reschedule to the next occurrence, never complete/delete", async () => {
		// DB clock 12:00:30; cron every minute → next 12:01:00.
		const dbNow = new Date("2026-01-01T12:00:30.000Z");
		const next = new Date("2026-01-01T12:01:00.000Z");

		// Success.
		const s1 = fakeSystemJobs(dbNow);
		s1.backlog.push(tickRecord({}, { prevMillis: new Date("2026-01-01T12:00:00.000Z").getTime() }));
		const w1 = systemWorker(s1.store, async () => {});
		w1.worker.start();
		await flush();
		expect(s1.store.rescheduleTick).toHaveBeenCalledWith("schedule-tick:daily", "lock-1", {
			runAt: next,
			prevMillis: next.getTime(),
		});
		expect(s1.store.complete).not.toHaveBeenCalled();
		expect(s1.store.remove).not.toHaveBeenCalled();
		w1.worker.stopClaiming();

		// Failure on the last attempt: error + reschedule (fail() would delete the row).
		const s2 = fakeSystemJobs(dbNow);
		s2.backlog.push(tickRecord({ attempts: 4, maxAttempts: 5 }));
		const w2 = systemWorker(s2.store, async () => Promise.reject(new Error("no task")));
		w2.worker.start();
		await flush();
		expect(s2.store.fail).not.toHaveBeenCalled();
		expect(s2.store.rescheduleTick).toHaveBeenCalledWith("schedule-tick:daily", "lock-1", {
			runAt: next,
			prevMillis: next.getTime(),
		});
		expect(w2.logger.calls.some(l => l.level === "error")).toBe(true);
		w2.worker.stopClaiming();

		// Failure below the limit: fail with backoff, no reschedule.
		const s3 = fakeSystemJobs(dbNow);
		s3.backlog.push(tickRecord({ attempts: 3, maxAttempts: 5 }));
		const w3 = systemWorker(s3.store, async () => Promise.reject(new Error("no task")));
		w3.worker.start();
		await flush();
		expect(s3.store.fail).toHaveBeenCalledWith("schedule-tick:daily", "lock-1", 8000);
		expect(s3.store.rescheduleTick).not.toHaveBeenCalled();
		w3.worker.stopClaiming();
	});

	it("schedule-tick: the next occurrence is after max(db now, prevMillis)", async () => {
		// prevMillis ahead of the DB clock (clock skew): next is after prevMillis.
		const dbNow = new Date("2026-01-01T12:00:30.000Z");
		const s = fakeSystemJobs(dbNow);
		s.backlog.push(
			tickRecord(
				{},
				{ prevMillis: new Date("2026-01-01T12:02:00.000Z").getTime(), timezone: "UTC" }
			)
		);
		const w = systemWorker(s.store, async () => {});
		w.worker.start();
		await flush();
		const at = new Date("2026-01-01T12:03:00.000Z");
		expect(s.store.rescheduleTick).toHaveBeenCalledWith("schedule-tick:daily", "lock-1", {
			runAt: at,
			prevMillis: at.getTime(),
		});
		w.worker.stopClaiming();
	});

	it("schedule-tick with invalid row data is logged and removed, never processed", async () => {
		for (const data of [
			{ scheduleId: "daily", schema: "mt", prevMillis: 1 }, // no cron
			{ scheduleId: "daily", cron: "* * * * *", schema: "mt", prevMillis: "x" },
			{ scheduleId: "daily", cron: "* * * * *", schema: "mt", prevMillis: 1, timezone: 3 },
			{ cron: "* * * * *", schema: "mt", prevMillis: 1 },
			null,
		]) {
			const s = fakeSystemJobs(new Date(T0));
			s.backlog.push(
				sysRecord({ jobId: "schedule-tick:daily", name: "schedule-tick", data: data as Json })
			);
			const process = vi.fn(async () => {});
			const w = systemWorker(s.store, process);
			w.worker.start();
			await flush();
			expect(process).not.toHaveBeenCalled();
			expect(s.store.remove).toHaveBeenCalledWith("schedule-tick:daily", { evenIfLocked: true });
			expect(s.store.rescheduleTick).not.toHaveBeenCalled();
			expect(w.logger.calls.some(l => l.level === "error")).toBe(true);
			w.worker.stopClaiming();
		}
	});

	it("schedule-tick whose stored cron no longer parses is logged and removed", async () => {
		const s = fakeSystemJobs(new Date(T0));
		s.backlog.push(tickRecord({}, { cron: "not a cron" }));
		const w = systemWorker(s.store, async () => {});
		w.worker.start();
		await flush();
		expect(s.store.rescheduleTick).not.toHaveBeenCalled();
		expect(s.store.remove).toHaveBeenCalledWith("schedule-tick:daily", { evenIfLocked: true });
		expect(w.logger.calls.some(l => l.level === "error")).toBe(true);
		w.worker.stopClaiming();
	});

	it("a fenced write that does not match (lock lost) is logged at debug and ignored", async () => {
		const { store, backlog } = fakeSystemJobs(new Date(T0));
		store.complete.mockResolvedValue(false);
		store.fail.mockResolvedValue("lost");
		backlog.push(sysRecord({ jobId: "a" }), sysRecord({ jobId: "b" }));
		const { worker, logger } = systemWorker(store, async (_n, _d, job: Job) => {
			if (job.id === "b") throw new Error("boom");
		});
		worker.start();
		await flush();
		const debug = logger.calls.filter(l => l.level === "debug").map(l => l.fields.jobId);
		expect(debug.sort()).toEqual(["a", "b"]);
		expect(logger.calls.filter(l => l.level === "warn" || l.level === "error")).toEqual([]);
		worker.stopClaiming();
	});

	it("a bookkeeping write that throws is logged and does not stop the loop", async () => {
		const { store, backlog } = fakeSystemJobs(new Date(T0));
		store.complete.mockRejectedValueOnce(new Error("db down"));
		backlog.push(sysRecord({ jobId: "a" }));
		const { worker, logger } = systemWorker(store, async () => {});
		worker.start();
		await flush();
		expect(logger.calls.find(l => l.level === "warn")?.fields).toMatchObject({ jobId: "a" });
		backlog.push(sysRecord({ jobId: "b" }));
		worker.wake();
		await flush();
		expect(store.complete).toHaveBeenLastCalledWith("b", "lock-1");
		worker.stopClaiming();
	});
});
