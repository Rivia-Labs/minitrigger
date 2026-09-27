import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ConfigError, MinitriggerError } from "../../src/errors.js";
import { channelName } from "../../src/pg/listener.js";
import type { NotifyBatcher } from "../../src/pg/notify-batcher.js";
import { createPgTransport, type PgTransportOptions } from "../../src/pg/transport.js";
import type { SqlStore } from "../../src/store/sql/index.js";
import type {
	ClaimRunsInput,
	Json,
	QueueStateRecord,
	RunRecord,
	ScheduleTickData,
	SystemJobRecord,
	SystemJobStore,
} from "../../src/store/types.js";

const T0 = new Date("2026-01-01T00:00:00.000Z").getTime();
const SCHEMA = "s";

function run(id: string, overrides: Partial<RunRecord> = {}): RunRecord {
	return {
		id,
		taskId: "task-1",
		queue: "q1",
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
		...overrides,
	};
}

function recordingLogger() {
	const calls: Array<{ level: string; fields: Record<string, unknown>; message: string }> = [];
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

function fakeBatcher(): NotifyBatcher & { calls: Array<{ channel: string; payload: string }> } {
	const calls: Array<{ channel: string; payload: string }> = [];
	return {
		calls,
		notify: vi.fn((channel: string, payload: string) => {
			calls.push({ channel, payload });
		}),
		flush: vi.fn(async () => {}),
		close: vi.fn(async () => {}),
	};
}

/** A minimal, self-consistent `RunClaimStore` fake: claims pop off a backlog the test fills. */
function fakeClaim() {
	const backlog: RunRecord[] = [];
	const calls: ClaimRunsInput[] = [];
	let nextDue: number | null = null;
	return {
		claimRuns: vi.fn(async (input: ClaimRunsInput) => {
			calls.push(input);
			return backlog.splice(0, input.limit);
		}),
		nextDueInMs: vi.fn(async () => nextDue),
		backlog,
		calls,
		setNextDue(d: number | null) {
			nextDue = d;
		},
	};
}

/** Like {@link fakeClaim}, but with a separate backlog per queue name (for multi-queue tests). */
function fakeClaimPerQueue(backlogs: Record<string, RunRecord[]>) {
	const calls: ClaimRunsInput[] = [];
	return {
		claimRuns: vi.fn(async (input: ClaimRunsInput) => {
			calls.push(input);
			const backlog = backlogs[input.queue] ?? [];
			return backlog.splice(0, input.limit);
		}),
		nextDueInMs: vi.fn(async () => null),
		calls,
	};
}

/**
 * A self-consistent `SystemJobStore` fake: `upsertScheduleTick` really only rewrites the row (and
 * moves the pending tick) when `cron`/`timezone` changed, mirroring the real store's semantics
 * (`src/store/sql/system-jobs.ts`) instead of a mock that just trusts whatever the test asserts.
 */
function fakeSystemJobs(dbNow: Date) {
	const rows = new Map<string, SystemJobRecord>();
	const store: SystemJobStore = {
		upsert: vi.fn(async () => {}),
		upsertScheduleTick: vi.fn(
			async (input: {
				jobId: string;
				data: ScheduleTickData;
				runAt: Date;
			}): Promise<"created" | "updated" | "unchanged"> => {
				const existing = rows.get(input.jobId);
				if (!existing) {
					rows.set(input.jobId, {
						jobId: input.jobId,
						name: "schedule-tick",
						data: input.data as unknown as Json,
						runAfter: input.runAt,
						attempts: 0,
						maxAttempts: 5,
						lockedBy: null,
					});
					return "created";
				}
				const existingData = existing.data as unknown as ScheduleTickData;
				if (
					existingData.cron === input.data.cron &&
					existingData.timezone === input.data.timezone
				) {
					return "unchanged";
				}
				rows.set(input.jobId, {
					...existing,
					data: input.data as unknown as Json,
					runAfter: input.runAt,
					lockedBy: null,
				});
				return "updated";
			}
		),
		claim: vi.fn(async () => []),
		complete: vi.fn(async () => true),
		delay: vi.fn(async () => true),
		fail: vi.fn(async (): Promise<"retried" | "exhausted" | "lost"> => "retried"),
		rescheduleTick: vi.fn(async () => true),
		remove: vi.fn(async (jobId: string) => rows.delete(jobId)),
		get: vi.fn(async (jobId: string) => rows.get(jobId) ?? null),
		listByName: vi.fn(async (name: string) => [...rows.values()].filter(r => r.name === name)),
		nextDueInMs: vi.fn(async () => null),
		now: vi.fn(async () => dbNow),
	};
	return { store, rows };
}

function fakeQueues(initial: Record<string, boolean> = {}) {
	const state = new Map<string, QueueStateRecord>(
		Object.entries(initial).map(([queue, paused]) => [
			queue,
			{ queue, paused, updatedAt: new Date(T0) },
		])
	);
	return {
		get: vi.fn(async (queue: string) => state.get(queue) ?? null),
		setPaused: vi.fn(),
		list: vi.fn(async () => [...state.values()]),
	};
}

function fakeWake() {
	const handlers = new Set<(queue: string | undefined) => void>();
	return {
		wake: {
			subscribe: (h: (queue: string | undefined) => void) => {
				handlers.add(h);
				return () => handlers.delete(h);
			},
		},
		emit(queue: string | undefined) {
			for (const h of handlers) h(queue);
		},
		get size() {
			return handlers.size;
		},
	};
}

function makeOptions(overrides: Partial<PgTransportOptions> = {}): PgTransportOptions {
	const claim = fakeClaim();
	const systemJobs = fakeSystemJobs(new Date(T0));
	const queues = fakeQueues();
	const batcher = fakeBatcher();
	const logger = recordingLogger();
	const wake = fakeWake();
	const store = {
		claim,
		systemJobs: systemJobs.store,
		queues,
	} as unknown as SqlStore;
	return {
		store,
		batcher,
		schema: SCHEMA,
		queues: [
			{ name: "q1", concurrency: 1 },
			{ name: "q2", concurrency: 1 },
		],
		systemConcurrency: 2,
		stallTimeoutMs: 30_000,
		polling: { intervalMs: 1000, jitterMs: 0 },
		wake: wake.wake,
		logger,
		...overrides,
	};
}

beforeEach(() => {
	vi.useFakeTimers();
	vi.setSystemTime(T0);
});
afterEach(() => {
	vi.useRealTimers();
});

const flush = () => vi.advanceTimersByTimeAsync(0);

describe("createPgTransport", () => {
	it("enqueueRun notifies the wake channel with the queue name, delayed or not", async () => {
		const options = makeOptions();
		const transport = createPgTransport(options);
		const batcher = options.batcher as ReturnType<typeof fakeBatcher>;

		await transport.enqueueRun({ queue: "q1", runId: "r1", seq: 1 });
		await transport.enqueueRun({ queue: "q1", runId: "r1", seq: 2, delayMs: 5000 });

		expect(batcher.calls).toEqual([
			{ channel: channelName("wake", SCHEMA), payload: "q1" },
			{ channel: channelName("wake", SCHEMA), payload: "q1" },
		]);
	});

	it("removeRunJob and pauseQueue are no-ops, resumeQueue wakes the queue; isQueuePaused reads queue_state", async () => {
		const options = makeOptions();
		const queues = (options.store as unknown as { queues: ReturnType<typeof fakeQueues> }).queues;
		queues.get.mockImplementation(async (q: string) =>
			q === "q1" ? { queue: "q1", paused: true, updatedAt: new Date(T0) } : null
		);
		const transport = createPgTransport(options);

		await expect(transport.removeRunJob("q1", "r1", 1)).resolves.toBeUndefined();
		const batcher = options.batcher as ReturnType<typeof fakeBatcher>;
		await expect(transport.pauseQueue("q1")).resolves.toBeUndefined();
		expect(batcher.calls).toEqual([]);
		await expect(transport.resumeQueue("q1")).resolves.toBeUndefined();
		// A resumed queue is claimed right away, not at the next poll.
		expect(batcher.calls).toEqual([{ channel: channelName("wake", SCHEMA), payload: "q1" }]);
		await expect(transport.isQueuePaused("q1")).resolves.toBe(true);
		await expect(transport.isQueuePaused("q2")).resolves.toBe(false);
	});

	it("removeSystemJob swallows a store error (best-effort, never throws)", async () => {
		const options = makeOptions();
		const systemJobsStore = options.store.systemJobs;
		const boom = new Error("connection reset");
		(systemJobsStore.remove as ReturnType<typeof vi.fn>).mockRejectedValueOnce(boom);
		const transport = createPgTransport(options);

		await expect(transport.removeSystemJob("wp-w1-expire")).resolves.toBeUndefined();
		const logger = options.logger as ReturnType<typeof recordingLogger>;
		expect(logger.calls.find(c => c.fields.err === boom)).toMatchObject({
			fields: { jobId: "wp-w1-expire", err: boom },
		});

		// A subsequent call that doesn't throw still resolves normally.
		await expect(transport.removeSystemJob("wp-w2-expire")).resolves.toBeUndefined();
		expect(systemJobsStore.remove).toHaveBeenCalledWith("wp-w2-expire");
	});

	it("enqueueSystem upserts a system_jobs row with a generated jobId when none is given", async () => {
		const options = makeOptions();
		const transport = createPgTransport(options);
		const systemJobsStore = options.store.systemJobs;

		// Before startWorkers there is no worker to wake locally, but the upsert still happens
		// (never throws for lack of a running -system worker).
		await transport.enqueueSystem("retention", { window: 1 }, { jobId: "retention-1" });
		expect(systemJobsStore.upsert).toHaveBeenCalledWith({
			jobId: "retention-1",
			name: "retention",
			data: { window: 1 },
			maxAttempts: 1,
		});

		await transport.enqueueSystem("waitpoint-expire", { waitpointId: "w1" }, { delayMs: 250 });
		expect(systemJobsStore.upsert).toHaveBeenCalledWith(
			expect.objectContaining({ name: "waitpoint-expire", delayMs: 250, maxAttempts: 1 })
		);
		// No explicit jobId: a fresh one was generated (a UUID), not left undefined.
		const call = (systemJobsStore.upsert as ReturnType<typeof vi.fn>).mock.calls[1]?.[0] as {
			jobId: string;
		};
		expect(call.jobId).toMatch(/^[0-9a-f-]{36}$/);
	});

	it("enqueueSystem wakes the local -system worker once it is started, without waiting a poll interval", async () => {
		const claim = fakeClaim();
		const systemJobs = fakeSystemJobs(new Date(T0));
		const options = makeOptions({
			store: { claim, systemJobs: systemJobs.store, queues: fakeQueues() } as unknown as SqlStore,
			queues: [],
		});
		const transport = createPgTransport(options);
		await transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
		await flush();
		const before = systemJobs.store.claim as ReturnType<typeof vi.fn>;
		const callsBefore = before.mock.calls.length;

		await transport.enqueueSystem("retention", { window: 1 }, { jobId: "retention-1" });
		await flush();
		expect(before.mock.calls.length).toBeGreaterThan(callsBefore);

		await transport.closeWorkers({ timeoutMs: 100 });
	});

	it("upsertJobScheduler stores the next occurrence and getJobScheduler reads it back", async () => {
		const dbNow = new Date("2026-01-01T12:00:30.000Z");
		const systemJobs = fakeSystemJobs(dbNow);
		const options = makeOptions({
			store: {
				claim: fakeClaim(),
				systemJobs: systemJobs.store,
				queues: fakeQueues(),
			} as unknown as SqlStore,
		});
		const transport = createPgTransport(options);

		await transport.upsertJobScheduler({ id: "daily", pattern: "0 0 * * *", timezone: "UTC" });

		const info = await transport.getJobScheduler("daily");
		expect(info).toEqual({
			id: "daily",
			pattern: "0 0 * * *",
			timezone: "UTC",
			schema: SCHEMA,
		});

		const list = await transport.listJobSchedulers();
		expect(list).toEqual([info]);

		expect(await transport.getJobScheduler("missing")).toBeUndefined();
		expect(await transport.removeJobScheduler("daily")).toBe(true);
		expect(await transport.getJobScheduler("daily")).toBeUndefined();
		expect(await transport.removeJobScheduler("daily")).toBe(false);
	});

	it("re-upserting the same scheduler does not move the pending tick", async () => {
		const dbNow = new Date("2026-01-01T12:00:30.000Z");
		const systemJobs = fakeSystemJobs(dbNow);
		const options = makeOptions({
			store: {
				claim: fakeClaim(),
				systemJobs: systemJobs.store,
				queues: fakeQueues(),
			} as unknown as SqlStore,
		});
		const transport = createPgTransport(options);

		await transport.upsertJobScheduler({ id: "daily", pattern: "0 0 * * *", timezone: "UTC" });
		const first = await transport.getJobScheduler("daily");
		expect(systemJobs.store.upsertScheduleTick).toHaveBeenCalledTimes(1);

		// Re-upsert with the exact same pattern/timezone: the fake store (mirroring the real one)
		// reports "unchanged" and never moves the pending tick.
		await transport.upsertJobScheduler({ id: "daily", pattern: "0 0 * * *", timezone: "UTC" });
		const second = await transport.getJobScheduler("daily");
		expect(second).toEqual(first);
		expect(systemJobs.store.upsertScheduleTick).toHaveBeenCalledTimes(2);

		// A genuinely different pattern DOES move it.
		await transport.upsertJobScheduler({ id: "daily", pattern: "0 1 * * *", timezone: "UTC" });
		const third = await transport.getJobScheduler("daily");
		expect(third?.pattern).toBe("0 1 * * *");
	});

	it("rateLimit queues are refused at construction time", () => {
		const options = makeOptions({
			queues: [{ name: "q1", concurrency: 1, rateLimit: { max: 1, durationMs: 1000 } }],
		});
		expect(() => createPgTransport(options)).toThrow(ConfigError);
	});

	it("startWorkers creates one worker per selected queue and wakes by queue name or all", async () => {
		const claim = fakeClaim();
		claim.setNextDue(null); // nothing due: every worker sleeps a full poll interval after an empty claim
		let capturedHandler: ((q: string | undefined) => void) | undefined;
		const options = makeOptions({
			store: {
				claim,
				systemJobs: fakeSystemJobs(new Date(T0)).store,
				queues: fakeQueues(),
			} as unknown as SqlStore,
			wake: {
				subscribe(h) {
					capturedHandler = h;
					return () => {
						capturedHandler = undefined;
					};
				},
			},
		});

		const transport = createPgTransport(options);
		await transport.startWorkers({
			queues: ["q1"],
			processRun: async () => {},
			processSystem: async () => {},
		});
		await flush();

		// Only q1 was selected: q2 never claims.
		const q1Calls = claim.calls.filter(c => c.queue === "q1").length;
		const q2Calls = claim.calls.filter(c => c.queue === "q2").length;
		expect(q1Calls).toBe(1);
		expect(q2Calls).toBe(0);
		expect(transport.workerConcurrency("q1")).toBe(1);
		expect(transport.workerConcurrency("q2")).toBeUndefined();

		expect(capturedHandler).toBeDefined();

		// A hit against a started queue wakes just that one.
		capturedHandler?.("q1");
		await flush();
		expect(claim.calls.filter(c => c.queue === "q1").length).toBe(2);

		// An unknown/untrusted queue name wakes every queue this process runs.
		capturedHandler?.("some-other-queue");
		await flush();
		expect(claim.calls.filter(c => c.queue === "q1").length).toBe(3);

		// `undefined` (e.g. a fresh listener reconnect) wakes everyone too.
		capturedHandler?.(undefined);
		await flush();
		expect(claim.calls.filter(c => c.queue === "q1").length).toBe(4);

		await transport.closeWorkers({ timeoutMs: 100 });
	});

	it("onSlotFreed notifies the wake channel only for a queue with globalConcurrency", async () => {
		const claim = fakeClaimPerQueue({
			q1: [run("r-q1", { queue: "q1", enqueueSeq: 1 })],
			q2: [run("r-q2", { queue: "q2", enqueueSeq: 1 })],
		});
		const options = makeOptions({
			store: {
				claim,
				systemJobs: fakeSystemJobs(new Date(T0)).store,
				queues: fakeQueues(),
			} as unknown as SqlStore,
			queues: [
				{ name: "q1", concurrency: 1 },
				{ name: "q2", concurrency: 1, globalConcurrency: 3 },
			],
		});
		const batcher = options.batcher as ReturnType<typeof fakeBatcher>;
		const transport = createPgTransport(options);

		await transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
		// The claim, the (immediately-resolving) process, its `.finally()` and `onFinished` all
		// chain off already-settled promises: a couple of flushes drain the whole chain.
		await flush();
		await flush();

		const q1WakeCalls = batcher.calls.filter(c => c.payload === "q1").length;
		const q2WakeCalls = batcher.calls.filter(c => c.payload === "q2").length;
		expect(q1WakeCalls).toBe(0);
		expect(q2WakeCalls).toBe(1);

		await transport.closeWorkers({ timeoutMs: 100 });
	});

	it("startWorkers rejects an unknown queue name in the `queues` filter", async () => {
		const options = makeOptions();
		const transport = createPgTransport(options);
		await expect(
			transport.startWorkers({
				queues: ["nope"],
				processRun: async () => {},
				processSystem: async () => {},
			})
		).rejects.toThrow(ConfigError);
	});

	it("startWorkers refuses to run twice on the same transport", async () => {
		const options = makeOptions();
		const transport = createPgTransport(options);
		await transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
		await flush();
		await expect(
			transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} })
		).rejects.toThrow(ConfigError);
		await transport.closeWorkers({ timeoutMs: 100 });
	});

	it("closeWorkers before startWorkers still invokes beforeClose (with an empty stillActive)", async () => {
		const options = makeOptions();
		const transport = createPgTransport(options);
		const beforeClose = vi.fn(async () => {});

		const result = await transport.closeWorkers({ timeoutMs: 100, beforeClose });

		expect(beforeClose).toHaveBeenCalledTimes(1);
		expect(beforeClose).toHaveBeenCalledWith([]);
		expect(result).toEqual({ stillActive: [] });
	});

	it("startWorkers after closeWorkers rejects, even if workers were never started", async () => {
		const options = makeOptions();
		const transport = createPgTransport(options);

		await transport.closeWorkers({ timeoutMs: 100 });

		await expect(
			transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} })
		).rejects.toThrow(MinitriggerError);
	});

	it("startWorkers rejects after closeWorkers even when workers WERE started first", async () => {
		const options = makeOptions({ queues: [{ name: "q1", concurrency: 1 }] });
		const transport = createPgTransport(options);
		await transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
		await flush();
		await transport.closeWorkers({ timeoutMs: 100 });

		await expect(
			transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} })
		).rejects.toThrow(MinitriggerError);
	});

	it("closeWorkers stops claiming, drains and reports still-active runs to beforeClose", async () => {
		const claim = fakeClaim();
		claim.backlog.push(run("r1", { queue: "q1", enqueueSeq: 7 }));
		const options = makeOptions({
			store: {
				claim,
				systemJobs: fakeSystemJobs(new Date(T0)).store,
				queues: fakeQueues(),
			} as unknown as SqlStore,
			queues: [{ name: "q1", concurrency: 1 }],
		});

		let releaseProcess: (() => void) | undefined;
		const processRun = vi.fn(
			() =>
				new Promise<void>(resolve => {
					releaseProcess = resolve;
				})
		);

		const transport = createPgTransport(options);
		await transport.startWorkers({ processRun, processSystem: async () => {} });
		await flush();
		expect(processRun).toHaveBeenCalledTimes(1);

		const beforeClose = vi.fn(async () => {});
		const closePromise = transport.closeWorkers({ timeoutMs: 50, beforeClose });
		await vi.advanceTimersByTimeAsync(50);
		const result = await closePromise;

		expect(result.stillActive).toEqual([{ queue: "q1", jobId: "r1-7" }]);
		expect(beforeClose).toHaveBeenCalledWith([{ queue: "q1", jobId: "r1-7" }]);

		releaseProcess?.();
	});

	it("closeWorkers reports no still-active jobs once everything settles before the timeout", async () => {
		const options = makeOptions({ queues: [{ name: "q1", concurrency: 1 }] });
		const transport = createPgTransport(options);
		await transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
		await flush();
		const result = await transport.closeWorkers({ timeoutMs: 1000 });
		expect(result.stillActive).toEqual([]);
	});

	it("closeWorkers propagates beforeClose failures only as a warn, never throwing", async () => {
		const options = makeOptions({ queues: [{ name: "q1", concurrency: 1 }] });
		const transport = createPgTransport(options);
		await transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
		await flush();
		const boom = new Error("boom");
		await expect(
			transport.closeWorkers({
				timeoutMs: 100,
				beforeClose: async () => {
					throw boom;
				},
			})
		).resolves.toEqual({ stillActive: [] });
		const logger = options.logger as ReturnType<typeof recordingLogger>;
		expect(logger.calls.find(c => c.level === "warn")?.fields).toMatchObject({ err: boom });
	});

	it("checkRedis always resolves undefined", async () => {
		const transport = createPgTransport(makeOptions());
		await expect(transport.checkRedis()).resolves.toBeUndefined();
	});

	it("setWorkerConcurrency changes a started queue's worker and returns false for an unknown one", async () => {
		const options = makeOptions({ queues: [{ name: "q1", concurrency: 1 }] });
		const transport = createPgTransport(options);
		await transport.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
		await flush();
		expect(transport.setWorkerConcurrency("q1", 5)).toBe(true);
		expect(transport.workerConcurrency("q1")).toBe(5);
		expect(transport.setWorkerConcurrency("nope", 5)).toBe(false);
		await transport.closeWorkers({ timeoutMs: 100 });
	});
});
