import type { Job } from "bullmq";
import { describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import {
	ActiveExecutions,
	createSystemDispatcher,
	type SystemDeps,
} from "../../src/executor/index.js";
import { buildRegistry, type Registry } from "../../src/registry.js";
import type { CreateRunResult, RunRecord, ScheduleRecord, Store } from "../../src/store/types.js";
import { defineTask } from "../../src/task.js";

/**
 * Unit coverage for `executor/system.ts`'s `schedule-tick` dispatch (fix 2/3, opus review): every
 * failure -- including `processScheduleTick` propagating `TaskNotRegisteredError` -- must be logged
 * with `scheduleId`/`scheduledAtMs` and rethrown (not swallowed), so BullMQ's own `attempts: 5` +
 * backoff (the job scheduler's template opts, `src/queue/transport.ts`) actually gets a chance to
 * retry it.
 */

const NOW = new Date("2026-01-01T00:00:00.000Z");

function scheduleRecord(overrides: Partial<ScheduleRecord> = {}): ScheduleRecord {
	return {
		id: "daily",
		taskId: "task-1",
		cron: "0 9 * * *",
		timezone: null,
		payload: { v: 1 },
		source: "api",
		createdAt: NOW,
		updatedAt: NOW,
		...overrides,
	};
}

function runRecord(overrides: Partial<RunRecord> = {}): RunRecord {
	return {
		id: "00000000-0000-0000-0000-000000000001",
		taskId: "task-1",
		queue: "default",
		payload: null,
		status: "queued",
		output: null,
		error: null,
		attempt: 1,
		maxAttempts: 3,
		executionId: null,
		enqueueSeq: 1,
		heartbeatAt: null,
		runAfter: NOW,
		parentRunId: null,
		parentWaitpointId: null,
		parentStepName: null,
		batchIndex: null,
		startedAt: null,
		finishedAt: null,
		createdAt: NOW,
		updatedAt: NOW,
		...overrides,
	};
}

function silentLogger(): EngineLogger & {
	calls: Array<{ level: string; fields: unknown; message: string }>;
} {
	const calls: Array<{ level: string; fields: unknown; message: string }> = [];
	const make =
		(level: string) =>
		(fields: Record<string, unknown>, message: string): void => {
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

/** A fake BullMQ `Job` carrying exactly what `scheduleTick` reads off it. */
function fakeJob(
	overrides: { repeatJobKey?: string; prevMillis?: number; data?: unknown } = {}
): Job {
	return {
		id: "repeat:daily:1700000000000",
		repeatJobKey: overrides.repeatJobKey,
		opts: { prevMillis: overrides.prevMillis },
		timestamp: 1_699_999_999_000,
		data: overrides.data ?? {},
	} as unknown as Job;
}

const echoTask = defineTask({ id: "task-1", run: async (p: { v: number }) => p });

function baseDeps(overrides: Partial<SystemDeps> = {}): {
	deps: SystemDeps;
	logger: ReturnType<typeof silentLogger>;
	registry: Registry;
} {
	const logger = silentLogger();
	const registry = overrides.registry ?? buildRegistry([echoTask]);
	const store = {
		schedules: {
			get: async (id: string) => (id === "daily" ? scheduleRecord() : null),
		},
		runs: {
			create: async () =>
				({
					kind: "created",
					run: runRecord(),
					enqueue: {
						runId: runRecord().id,
						taskId: "task-1",
						queue: "default",
						seq: 1,
						delayMs: 0,
						status: "queued",
						updatedAt: NOW,
					},
				}) as CreateRunResult,
		},
	} as unknown as Store;
	const transport = {
		enqueueRun: vi.fn(async () => {}),
		removeRunJob: vi.fn(async () => {}),
		removeSystemJob: vi.fn(async () => {}),
		upsertJobScheduler: vi.fn(async () => {}),
		removeJobScheduler: vi.fn(async () => true),
		getJobScheduler: vi.fn(async () => ({
			id: "daily",
			pattern: "0 9 * * *",
			timezone: undefined,
		})),
	} as unknown as SystemDeps["transport"];

	const deps: SystemDeps = {
		config: { logger },
		store,
		transport,
		active: new ActiveExecutions(),
		registry,
		...overrides,
	};
	return { deps, logger, registry };
}

describe("createSystemDispatcher: schedule-tick", () => {
	it("resolves scheduleId from job.repeatJobKey and scheduledAtMs from job.opts.prevMillis", async () => {
		const { deps, logger } = baseDeps();
		const dispatch = createSystemDispatcher(deps);
		const job = fakeJob({ repeatJobKey: "daily", prevMillis: 1_700_000_000_000 });

		await dispatch("schedule-tick", { scheduleId: "daily" }, job);

		expect(logger.calls.some(c => c.level === "error")).toBe(false);
	});

	it("falls back to job.data.scheduleId when repeatJobKey is absent", async () => {
		const { deps } = baseDeps();
		const dispatch = createSystemDispatcher(deps);
		const job = fakeJob({ prevMillis: 1_700_000_000_000, data: { scheduleId: "daily" } });

		await expect(dispatch("schedule-tick", { scheduleId: "daily" }, job)).resolves.toBeUndefined();
	});

	it("logs scheduleId/scheduledAtMs and rethrows when the task isn't registered in this process (fix 2/3)", async () => {
		const emptyRegistry = buildRegistry([]);
		const { deps, logger } = baseDeps({ registry: emptyRegistry });
		const dispatch = createSystemDispatcher(deps);
		const job = fakeJob({ repeatJobKey: "daily", prevMillis: 1_700_000_000_000 });

		await expect(dispatch("schedule-tick", { scheduleId: "daily" }, job)).rejects.toThrow();

		const errorCall = logger.calls.find(c => c.level === "error");
		expect(errorCall).toBeDefined();
		expect(errorCall?.fields).toMatchObject({
			scheduleId: "daily",
			scheduledAtMs: 1_700_000_000_000,
		});
	});

	it("survives a retried job (same repeatJobKey/prevMillis) by processing the same tick again", async () => {
		const emptyRegistry = buildRegistry([]);
		const { deps } = baseDeps({ registry: emptyRegistry });
		const dispatch = createSystemDispatcher(deps);
		// Same job identity across "attempts" (BullMQ retries the same Job record; repeatJobKey and
		// opts.prevMillis are read straight off the Redis job hash, so they're unchanged on retry).
		const job = fakeJob({ repeatJobKey: "daily", prevMillis: 1_700_000_000_000 });

		await expect(dispatch("schedule-tick", { scheduleId: "daily" }, job)).rejects.toThrow();
		await expect(dispatch("schedule-tick", { scheduleId: "daily" }, job)).rejects.toThrow();
	});
});
