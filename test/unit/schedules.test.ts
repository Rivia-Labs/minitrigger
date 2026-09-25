import { describe, expect, it, vi } from "vitest";
import { ConfigError, SerializationError, TaskNotRegisteredError } from "../../src/errors.js";
import type {
	JobSchedulerInfo,
	Transport,
	UpsertJobSchedulerInput,
} from "../../src/queue/transport.js";
import { buildRegistry, type Registry } from "../../src/registry.js";
import {
	createReconcileSchedulesHook,
	createSchedulesApi,
	processScheduleTick,
	type SchedulesApiDeps,
	syncDeclarativeSchedules,
} from "../../src/schedules.js";
import type {
	CreateRunResult,
	NewSchedule,
	RunRecord,
	ScheduleRecord,
	Store,
	UpsertScheduleOptions,
	UpsertScheduleResult,
} from "../../src/store/types.js";
import { defineTask } from "../../src/task.js";

const NOW = new Date("2026-01-01T00:00:00.000Z");

function silentLogger() {
	const calls: { level: string; fields: unknown; message: string }[] = [];
	const make =
		(level: string) =>
		(fields: Record<string, unknown>, message: string): void => {
			calls.push({ level, fields, message });
		};
	return {
		logger: { debug: make("debug"), info: make("info"), warn: make("warn"), error: make("error") },
		calls,
	};
}

function scheduleRecord(overrides: Partial<ScheduleRecord> = {}): ScheduleRecord {
	return {
		id: "sched-1",
		taskId: "task-1",
		cron: "0 9 * * *",
		timezone: null,
		payload: null,
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
		queue: "q",
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
		progress: null,
		debounceKey: null,
		concurrencyKey: null,
		...overrides,
	};
}

/**
 * Minimal fake `Store.schedules` + `Store.runs` (only the methods this module calls). Backed by a
 * `Map<id, ScheduleRecord>` (not a single slot) so tests can seed/observe more than one schedule at
 * once (fix 4's "leaves the OTHER task's sync unaffected" cases need that).
 */
function fakeStore(
	overrides: Partial<{ schedule: ScheduleRecord | null; createResult: CreateRunResult }> = {}
) {
	const schedulesUpsertCalls: NewSchedule[] = [];
	const runsCreateCalls: unknown[] = [];
	const deleteDeclarativeExceptCalls: (readonly string[])[] = [];
	const seed = overrides.schedule === undefined ? scheduleRecord() : overrides.schedule;
	const byId = new Map<string, ScheduleRecord>(seed ? [[seed.id, seed]] : []);
	const createResult: CreateRunResult =
		overrides.createResult ??
		({
			kind: "created",
			run: runRecord(),
			enqueue: {
				runId: runRecord().id,
				taskId: "task-1",
				queue: "q",
				seq: 1,
				delayMs: 0,
				status: "queued",
				updatedAt: NOW,
			},
		} as CreateRunResult);

	const store = {
		schedules: {
			async upsert(
				input: NewSchedule,
				options: UpsertScheduleOptions = {}
			): Promise<UpsertScheduleResult> {
				schedulesUpsertCalls.push(input);
				const prev = byId.get(input.id) ?? null;
				if (options.requireSourceMatch && prev && prev.source !== input.source) {
					// Mirrors the real stores' guarded upsert (fix 4): write nothing, report the
					// conflict.
					return {
						schedule: prev,
						created: false,
						cronChanged: false,
						timezoneChanged: false,
						conflict: { existingSource: prev.source },
					};
				}
				const row: ScheduleRecord = {
					id: input.id,
					taskId: input.taskId,
					cron: input.cron,
					timezone: input.timezone,
					payload: input.payload,
					source: input.source,
					createdAt: NOW,
					updatedAt: NOW,
				};
				byId.set(input.id, row);
				return { schedule: row, created: !prev, cronChanged: true, timezoneChanged: true };
			},
			async delete(id: string): Promise<boolean> {
				return byId.delete(id);
			},
			async get(id: string): Promise<ScheduleRecord | null> {
				return byId.get(id) ?? null;
			},
			async list(): Promise<ScheduleRecord[]> {
				return [...byId.values()];
			},
			async listDeclarative(): Promise<ScheduleRecord[]> {
				return [...byId.values()].filter(s => s.source === "declarative");
			},
			async deleteDeclarativeExcept(ids: readonly string[]): Promise<number> {
				deleteDeclarativeExceptCalls.push(ids);
				let n = 0;
				for (const s of [...byId.values()]) {
					if (s.source === "declarative" && !ids.includes(s.id)) {
						byId.delete(s.id);
						n++;
					}
				}
				return n;
			},
		},
		runs: {
			async create(input: unknown): Promise<CreateRunResult> {
				runsCreateCalls.push(input);
				return createResult;
			},
		},
	} as unknown as Store;

	return { store, schedulesUpsertCalls, runsCreateCalls, deleteDeclarativeExceptCalls };
}

function fakeTransport(overrides: Partial<Transport> = {}) {
	const upsertCalls: UpsertJobSchedulerInput[] = [];
	const removeCalls: string[] = [];
	const enqueueCalls: unknown[] = [];
	const transport = {
		async enqueueRun(input: unknown) {
			enqueueCalls.push(input);
		},
		async removeSystemJob() {},
		async upsertJobScheduler(input: UpsertJobSchedulerInput) {
			upsertCalls.push(input);
		},
		async removeJobScheduler(id: string) {
			removeCalls.push(id);
			return true;
		},
		async getJobScheduler(): Promise<JobSchedulerInfo | undefined> {
			return undefined;
		},
		async listJobSchedulers(): Promise<JobSchedulerInfo[]> {
			return [];
		},
		...overrides,
	} as unknown as Transport;
	return { transport, upsertCalls, removeCalls, enqueueCalls };
}

function registryOf(...tasks: Parameters<typeof buildRegistry>[0]): Registry {
	return buildRegistry(tasks);
}

const echoTask = defineTask({ id: "task-1", run: async (p: { v: number }) => p });

describe("createSchedulesApi: validation", () => {
	function api(overrides: Partial<SchedulesApiDeps> = {}) {
		const { store } = fakeStore();
		const { transport } = fakeTransport();
		const { logger } = silentLogger();
		return createSchedulesApi({
			store,
			transport,
			registry: registryOf(echoTask),
			logger,
			maxPayloadBytes: 1024,
			...overrides,
		});
	}

	it("rejects an invalid schedule id", async () => {
		await expect(
			api().upsert({ id: "bad id!", task: echoTask, cron: "0 9 * * *", payload: { v: 1 } })
		).rejects.toThrow(ConfigError);
	});

	it("rejects a schedule id containing ':'", async () => {
		await expect(
			api().upsert({ id: "has:colon", task: echoTask, cron: "0 9 * * *", payload: { v: 1 } })
		).rejects.toThrow(ConfigError);
	});

	it("rejects a cron pattern with too few fields", async () => {
		await expect(
			api().upsert({ id: "ok", task: echoTask, cron: "0 9 *", payload: { v: 1 } })
		).rejects.toThrow(ConfigError);
	});

	it("rejects a cron pattern with an illegal character", async () => {
		await expect(
			api().upsert({ id: "ok", task: echoTask, cron: "0 9 * * $", payload: { v: 1 } })
		).rejects.toThrow(ConfigError);
	});

	it("accepts a 6-field cron pattern (seconds)", async () => {
		const { transport, upsertCalls } = fakeTransport();
		const { store } = fakeStore();
		const { logger } = silentLogger();
		const schedules = createSchedulesApi({
			store,
			transport,
			registry: registryOf(echoTask),
			logger,
			maxPayloadBytes: 1024,
		});
		const result = await schedules.upsert({
			id: "ok",
			task: echoTask,
			cron: "* * * * * *",
			payload: { v: 1 },
		});
		expect(result.cron).toBe("* * * * * *");
		expect(upsertCalls).toEqual([{ id: "ok", pattern: "* * * * * *" }]);
	});

	it("rejects an unrecognized IANA timezone", async () => {
		await expect(
			api().upsert({
				id: "ok",
				task: echoTask,
				cron: "0 9 * * *",
				timezone: "Not/AZone",
				payload: { v: 1 },
			})
		).rejects.toThrow(ConfigError);
	});

	it("accepts a recognized IANA timezone", async () => {
		const result = await api().upsert({
			id: "ok",
			task: echoTask,
			cron: "0 9 * * *",
			timezone: "America/Belem",
			payload: { v: 1 },
		});
		expect(result.timezone).toBe("America/Belem");
	});

	it("rejects a task the registry doesn't know", async () => {
		const other = defineTask({ id: "not-registered", run: async () => {} });
		await expect(api().upsert({ id: "ok", task: other, cron: "0 9 * * *" })).rejects.toThrow(
			TaskNotRegisteredError
		);
	});

	it("rejects a non-serializable / too-large payload", async () => {
		await expect(
			api({ maxPayloadBytes: 4 }).upsert({
				id: "ok",
				task: echoTask,
				cron: "0 9 * * *",
				payload: { v: 123_456_789 },
			})
		).rejects.toThrow(SerializationError);
	});

	it("defaults an omitted payload to null", async () => {
		const { store, schedulesUpsertCalls } = fakeStore();
		const { transport } = fakeTransport();
		const { logger } = silentLogger();
		const task = defineTask({ id: "task-1", run: async () => {} });
		await createSchedulesApi({
			store,
			transport,
			registry: registryOf(task),
			logger,
			maxPayloadBytes: 1024,
		}).upsert({ id: "ok", task, cron: "0 9 * * *" });
		expect(schedulesUpsertCalls[0]?.payload).toBeNull();
	});
});

describe("createSchedulesApi: upsert/delete/list wiring", () => {
	it("upsert writes postgres first, then syncs the BullMQ scheduler", async () => {
		const { store } = fakeStore();
		const { transport, upsertCalls } = fakeTransport();
		const { logger } = silentLogger();
		const schedules = createSchedulesApi({
			store,
			transport,
			registry: registryOf(echoTask),
			logger,
			maxPayloadBytes: 1024,
		});

		const result = await schedules.upsert({
			id: "daily",
			task: echoTask,
			cron: "0 9 * * *",
			timezone: "America/Belem",
			payload: { v: 42 },
		});

		expect(result).toMatchObject({
			id: "daily",
			taskId: "task-1",
			cron: "0 9 * * *",
			source: "api",
		});
		expect(upsertCalls).toEqual([{ id: "daily", pattern: "0 9 * * *", timezone: "America/Belem" }]);
	});

	it("upsert returns the postgres result even if the BullMQ sync fails (logged, not thrown)", async () => {
		const { store } = fakeStore();
		const { transport } = fakeTransport({
			upsertJobScheduler: vi.fn(async () => {
				throw new Error("redis is down");
			}),
		});
		const { logger, calls } = silentLogger();
		const schedules = createSchedulesApi({
			store,
			transport,
			registry: registryOf(echoTask),
			logger,
			maxPayloadBytes: 1024,
		});

		const result = await schedules.upsert({
			id: "daily",
			task: echoTask,
			cron: "0 9 * * *",
			payload: { v: 1 },
		});
		expect(result.id).toBe("daily");
		expect(calls.some(c => c.level === "warn")).toBe(true);
	});

	it("delete removes the BullMQ scheduler only after the postgres row is gone", async () => {
		const { store } = fakeStore({ schedule: scheduleRecord({ id: "daily" }) });
		const { transport, removeCalls } = fakeTransport();
		const { logger } = silentLogger();
		const schedules = createSchedulesApi({
			store,
			transport,
			registry: registryOf(echoTask),
			logger,
			maxPayloadBytes: 1024,
		});

		expect(await schedules.delete("daily")).toBe(true);
		expect(removeCalls).toEqual(["daily"]);
		expect(await schedules.delete("daily")).toBe(false); // Already gone: no second BullMQ call.
		expect(removeCalls).toEqual(["daily"]);
	});

	it("delete tolerates a BullMQ removal failure (logged, still reports the postgres deletion)", async () => {
		const { store } = fakeStore({ schedule: scheduleRecord({ id: "daily" }) });
		const { transport } = fakeTransport({
			removeJobScheduler: vi.fn(async () => {
				throw new Error("redis is down");
			}),
		});
		const { logger, calls } = silentLogger();
		const schedules = createSchedulesApi({
			store,
			transport,
			registry: registryOf(echoTask),
			logger,
			maxPayloadBytes: 1024,
		});

		expect(await schedules.delete("daily")).toBe(true);
		expect(calls.some(c => c.level === "warn")).toBe(true);
	});

	it("list maps every stored schedule to the public shape", async () => {
		const { store } = fakeStore({ schedule: scheduleRecord({ id: "daily" }) });
		const { transport } = fakeTransport();
		const { logger } = silentLogger();
		const schedules = createSchedulesApi({
			store,
			transport,
			registry: registryOf(echoTask),
			logger,
			maxPayloadBytes: 1024,
		});
		expect(await schedules.list()).toEqual([scheduleRecord({ id: "daily" })]);
	});

	it("fix 4: throws ConfigError when the id is already owned by a declarative schedule; never syncs BullMQ", async () => {
		const { store } = fakeStore({
			schedule: scheduleRecord({ id: "daily", source: "declarative" }),
		});
		const { transport, upsertCalls } = fakeTransport();
		const { logger } = silentLogger();
		const schedules = createSchedulesApi({
			store,
			transport,
			registry: registryOf(echoTask),
			logger,
			maxPayloadBytes: 1024,
		});

		await expect(
			schedules.upsert({ id: "daily", task: echoTask, cron: "0 9 * * *", payload: { v: 1 } })
		).rejects.toThrow(ConfigError);
		expect(upsertCalls).toEqual([]);
		// The postgres row is untouched (still "declarative", still its original cron).
		expect(await store.schedules.get("daily")).toMatchObject({ source: "declarative" });
	});
});

describe("syncDeclarativeSchedules", () => {
	it("upserts every registered task's declared schedule (postgres + BullMQ)", async () => {
		const declared = defineTask({
			id: "declared",
			schedule: { cron: "0 9 * * *", payload: { v: 1 } },
			run: async () => {},
		});
		const plain = defineTask({ id: "plain", run: async () => {} });
		const { store, schedulesUpsertCalls } = fakeStore({ schedule: null });
		const { transport, upsertCalls } = fakeTransport();
		const { logger } = silentLogger();

		await syncDeclarativeSchedules({
			store,
			transport,
			registry: registryOf(declared, plain),
			logger,
			maxPayloadBytes: 1024,
			pruneDeclarative: false,
		});

		expect(schedulesUpsertCalls).toEqual([
			{
				id: "declared",
				taskId: "declared",
				cron: "0 9 * * *",
				timezone: null,
				payload: { v: 1 },
				source: "declarative",
			},
		]);
		expect(upsertCalls).toEqual([{ id: "declared", pattern: "0 9 * * *" }]);
	});

	it("isolates one task's bad schedule (invalid cron) from the rest", async () => {
		const bad = defineTask({ id: "bad", schedule: { cron: "not a cron" }, run: async () => {} });
		const good = defineTask({
			id: "good",
			schedule: { cron: "0 9 * * *" },
			run: async () => {},
		});
		const { store, schedulesUpsertCalls } = fakeStore({ schedule: null });
		const { transport } = fakeTransport();
		const { logger, calls } = silentLogger();

		await syncDeclarativeSchedules({
			store,
			transport,
			registry: registryOf(bad, good),
			logger,
			maxPayloadBytes: 1024,
			pruneDeclarative: false,
		});

		expect(schedulesUpsertCalls.map(c => c.id)).toEqual(["good"]);
		expect(calls.some(c => c.level === "error")).toBe(true);
	});

	it("without pruneDeclarative, leaves a no-longer-declared declarative schedule alone", async () => {
		const { store, deleteDeclarativeExceptCalls } = fakeStore({
			schedule: scheduleRecord({ id: "removed-from-code", source: "declarative" }),
		});
		const { transport, removeCalls } = fakeTransport();
		const { logger } = silentLogger();

		await syncDeclarativeSchedules({
			store,
			transport,
			registry: registryOf(),
			logger,
			maxPayloadBytes: 1024,
			pruneDeclarative: false,
		});

		expect(deleteDeclarativeExceptCalls).toEqual([]);
		expect(removeCalls).toEqual([]);
	});

	it("with pruneDeclarative: true, removes (postgres + BullMQ) a no-longer-declared schedule", async () => {
		const { store } = fakeStore({
			schedule: scheduleRecord({ id: "removed-from-code", source: "declarative" }),
		});
		const { transport, removeCalls } = fakeTransport();
		const { logger } = silentLogger();

		await syncDeclarativeSchedules({
			store,
			transport,
			registry: registryOf(),
			logger,
			maxPayloadBytes: 1024,
			pruneDeclarative: true,
		});

		expect(removeCalls).toEqual(["removed-from-code"]);
		expect(await store.schedules.get("removed-from-code")).toBeNull();
	});

	it("pruneDeclarative never touches an 'api' schedule", async () => {
		const { store } = fakeStore({ schedule: scheduleRecord({ id: "api-one", source: "api" }) });
		const { transport, removeCalls } = fakeTransport();
		const { logger } = silentLogger();

		await syncDeclarativeSchedules({
			store,
			transport,
			registry: registryOf(),
			logger,
			maxPayloadBytes: 1024,
			pruneDeclarative: true,
		});

		expect(removeCalls).toEqual([]);
		expect(await store.schedules.get("api-one")).not.toBeNull();
	});

	it("fix 4: does not clobber an id already owned by an 'api' schedule; isolated (logged, skipped, other tasks unaffected)", async () => {
		const declared = defineTask({
			id: "daily",
			schedule: { cron: "0 9 * * *" },
			run: async () => {},
		});
		const other = defineTask({
			id: "other",
			schedule: { cron: "0 10 * * *" },
			run: async () => {},
		});
		const { store, schedulesUpsertCalls } = fakeStore({
			schedule: scheduleRecord({ id: "daily", source: "api", cron: "0 8 * * *" }),
		});
		const { transport, upsertCalls } = fakeTransport();
		const { logger, calls } = silentLogger();

		await syncDeclarativeSchedules({
			store,
			transport,
			registry: registryOf(declared, other),
			logger,
			maxPayloadBytes: 1024,
			pruneDeclarative: false,
		});

		// The conflicting id's row is untouched (still "api", still its original cron); no BullMQ
		// sync happened for it.
		expect(await store.schedules.get("daily")).toMatchObject({ source: "api", cron: "0 8 * * *" });
		expect(upsertCalls.some(c => c.id === "daily")).toBe(false);
		expect(
			calls.some(c => c.level === "error" && (c.fields as { taskId?: string }).taskId === "daily")
		).toBe(true);
		// `other`'s own sync still ran (the fake store only tracks one row, but the upsert call for
		// it was still attempted, proving the loop didn't stop).
		expect(schedulesUpsertCalls.some(c => c.id === "other")).toBe(true);
	});
});

describe("createReconcileSchedulesHook", () => {
	it("upserts a postgres schedule missing its BullMQ scheduler", async () => {
		const { store } = fakeStore({ schedule: scheduleRecord({ id: "daily", cron: "0 9 * * *" }) });
		const { transport, upsertCalls } = fakeTransport({ listJobSchedulers: async () => [] });
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger })();

		expect(upsertCalls).toEqual([{ id: "daily", pattern: "0 9 * * *" }]);
	});

	it("upserts a postgres schedule whose BullMQ scheduler drifted (cron/timezone)", async () => {
		const { store } = fakeStore({
			schedule: scheduleRecord({ id: "daily", cron: "0 9 * * *", timezone: "UTC" }),
		});
		const { transport, upsertCalls } = fakeTransport({
			listJobSchedulers: async () => [{ id: "daily", pattern: "0 10 * * *", timezone: "UTC" }],
		});
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger })();

		expect(upsertCalls).toEqual([{ id: "daily", pattern: "0 9 * * *", timezone: "UTC" }]);
	});

	it("leaves a matching pair alone", async () => {
		const { store } = fakeStore({
			schedule: scheduleRecord({ id: "daily", cron: "0 9 * * *", timezone: null }),
		});
		const { transport, upsertCalls, removeCalls } = fakeTransport({
			listJobSchedulers: async () => [{ id: "daily", pattern: "0 9 * * *", timezone: undefined }],
		});
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger })();

		expect(upsertCalls).toEqual([]);
		expect(removeCalls).toEqual([]);
	});

	it("removes an orphan BullMQ scheduler with no matching postgres row", async () => {
		const { store } = fakeStore({ schedule: null });
		const { transport, removeCalls } = fakeTransport({
			listJobSchedulers: async () => [{ id: "orphan", pattern: "0 9 * * *", timezone: undefined }],
		});
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger })();

		expect(removeCalls).toEqual(["orphan"]);
	});

	it("fix 1: reads BullMQ schedulers before postgres schedules (sequential, not Promise.all)", async () => {
		const trace: string[] = [];
		const { transport } = fakeTransport({
			listJobSchedulers: async () => {
				trace.push("schedulers");
				return [];
			},
		});
		const store = {
			schedules: {
				list: async () => {
					trace.push("schedules");
					return [];
				},
				get: async () => null,
			},
		} as unknown as Pick<Store, "schedules">;
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger })();

		expect(trace).toEqual(["schedulers", "schedules"]);
	});

	it("fix 1: re-checks store.schedules.get right before removing an apparent orphan; skips if it now exists", async () => {
		const { transport, removeCalls } = fakeTransport({
			listJobSchedulers: async () => [{ id: "daily", pattern: "0 9 * * *", timezone: undefined }],
		});
		// `list()` is stale (as if read before a concurrent upsert landed); `get()` is fresher and
		// says the row now exists -- the hook must trust the re-check, not the stale `list()`.
		const store = {
			schedules: {
				list: async () => [],
				get: async (id: string) => (id === "daily" ? scheduleRecord({ id: "daily" }) : null),
			},
		} as unknown as Pick<Store, "schedules">;
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger })();

		expect(removeCalls).toEqual([]);
	});

	it("fix 8: never removes a scheduler tagged with a different schema", async () => {
		const { store } = fakeStore({ schedule: null });
		const { transport, removeCalls } = fakeTransport({
			listJobSchedulers: async () => [
				{ id: "foreign", pattern: "0 9 * * *", timezone: undefined, schema: "other_schema" },
			],
		});
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger, schema: "mt_test" })();

		expect(removeCalls).toEqual([]);
	});

	it("fix 8: removes an orphan tagged with this engine's own schema", async () => {
		const { store } = fakeStore({ schedule: null });
		const { transport, removeCalls } = fakeTransport({
			listJobSchedulers: async () => [
				{ id: "orphan", pattern: "0 9 * * *", timezone: undefined, schema: "mt_test" },
			],
		});
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger, schema: "mt_test" })();

		expect(removeCalls).toEqual(["orphan"]);
	});

	it("fix 8: removes an untagged orphan even when this engine's schema is known (pre-fix scheduler)", async () => {
		const { store } = fakeStore({ schedule: null });
		const { transport, removeCalls } = fakeTransport({
			listJobSchedulers: async () => [{ id: "orphan", pattern: "0 9 * * *", timezone: undefined }],
		});
		const { logger } = silentLogger();

		await createReconcileSchedulesHook({ store, transport, logger, schema: "mt_test" })();

		expect(removeCalls).toEqual(["orphan"]);
	});
});

describe("processScheduleTick", () => {
	it("missing schedule: removes the BullMQ scheduler, creates no run", async () => {
		const { store, runsCreateCalls } = fakeStore({ schedule: null });
		const { transport, removeCalls } = fakeTransport();
		const { logger } = silentLogger();

		await processScheduleTick(
			{ store, transport, registry: registryOf(echoTask), logger },
			"gone",
			Date.now()
		);

		expect(removeCalls).toEqual(["gone"]);
		expect(runsCreateCalls).toEqual([]);
	});

	it("fix 1: re-checks before removing; falls through to normal processing if the schedule now exists", async () => {
		// The first `get()` says "gone"; a fresh re-check (right before the removal) says it now
		// exists (as if a concurrent upsert landed in between) -- the tick must process it normally
		// instead of deleting the scheduler for a schedule that does exist.
		let calls = 0;
		const schedule = scheduleRecord({ id: "daily", taskId: "task-1", payload: { v: 7 } });
		const store = {
			schedules: {
				get: async (id: string) => {
					calls++;
					return calls === 1 ? null : id === "daily" ? schedule : null;
				},
			},
			runs: {
				create: async () =>
					({
						kind: "created",
						run: runRecord(),
						enqueue: {
							runId: runRecord().id,
							taskId: "task-1",
							queue: "q",
							seq: 1,
							delayMs: 0,
							status: "queued",
							updatedAt: NOW,
						},
					}) as CreateRunResult,
			},
		} as unknown as Pick<Store, "schedules" | "runs">;
		const { transport, removeCalls, enqueueCalls } = fakeTransport({
			getJobScheduler: async () => ({ id: "daily", pattern: schedule.cron, timezone: undefined }),
		});
		const { logger } = silentLogger();

		await processScheduleTick(
			{ store, transport, registry: registryOf(echoTask), logger },
			"daily",
			1_700_000_000_000
		);

		expect(removeCalls).toEqual([]);
		expect(enqueueCalls).toHaveLength(1);
	});

	it("missing schedule: tolerates the BullMQ removal itself failing", async () => {
		const { store, runsCreateCalls } = fakeStore({ schedule: null });
		const { transport } = fakeTransport({
			removeJobScheduler: vi.fn(async () => {
				throw new Error("redis is down");
			}),
		});
		const { logger, calls } = silentLogger();

		await expect(
			processScheduleTick(
				{ store, transport, registry: registryOf(echoTask), logger },
				"gone",
				Date.now()
			)
		).resolves.toBeUndefined();
		expect(runsCreateCalls).toEqual([]);
		expect(calls.some(c => c.level === "warn")).toBe(true);
	});

	it("present schedule: creates the run from the postgres payload with the __schedule idempotency key", async () => {
		const schedule = scheduleRecord({ id: "daily", taskId: "task-1", payload: { v: 7 } });
		const { store, runsCreateCalls } = fakeStore({ schedule });
		const { transport, enqueueCalls } = fakeTransport({
			getJobScheduler: async () => ({ id: "daily", pattern: schedule.cron, timezone: undefined }),
		});
		const { logger } = silentLogger();
		const notify = { status: vi.fn(), progress: vi.fn(), canceled: vi.fn() };

		await processScheduleTick(
			{ store, transport, registry: registryOf(echoTask), logger, notify },
			"daily",
			1_700_000_000_000
		);

		expect(runsCreateCalls).toEqual([
			expect.objectContaining({
				taskId: "task-1",
				queue: "default",
				payload: { v: 7 },
				idempotency: { key: "__schedule:daily:1700000000000", ttlMs: null },
			}),
		]);
		expect(enqueueCalls).toHaveLength(1);
		expect(notify.status).toHaveBeenCalledTimes(1);
	});

	it("a duplicate tick (idempotency key already claimed) enqueues nothing and doesn't notify", async () => {
		const schedule = scheduleRecord({ id: "daily" });
		const existing: CreateRunResult = { kind: "existing", run: runRecord({ status: "queued" }) };
		const { store, runsCreateCalls } = fakeStore({ schedule, createResult: existing });
		const { transport, enqueueCalls } = fakeTransport({
			getJobScheduler: async () => ({ id: "daily", pattern: schedule.cron, timezone: undefined }),
		});
		const { logger } = silentLogger();
		const notify = { status: vi.fn(), progress: vi.fn(), canceled: vi.fn() };

		await processScheduleTick(
			{ store, transport, registry: registryOf(echoTask), logger, notify },
			"daily",
			1_700_000_000_000
		);

		expect(runsCreateCalls).toHaveLength(1);
		expect(enqueueCalls).toEqual([]);
		expect(notify.status).not.toHaveBeenCalled();
	});

	it("repairs a drifted BullMQ scheduler before creating the run", async () => {
		const schedule = scheduleRecord({ id: "daily", cron: "0 9 * * *" });
		const { store } = fakeStore({ schedule });
		const { transport, upsertCalls } = fakeTransport({
			getJobScheduler: async () => ({ id: "daily", pattern: "0 10 * * *", timezone: undefined }),
		});
		const { logger } = silentLogger();

		await processScheduleTick(
			{ store, transport, registry: registryOf(echoTask), logger },
			"daily",
			Date.now()
		);

		expect(upsertCalls).toEqual([{ id: "daily", pattern: "0 9 * * *" }]);
	});

	it("task not registered in this process: throws (creates no run) instead of silently dropping the tick, so BullMQ retries it", async () => {
		const schedule = scheduleRecord({ id: "daily", taskId: "unregistered-task" });
		const { store, runsCreateCalls } = fakeStore({ schedule });
		const { transport } = fakeTransport({
			getJobScheduler: async () => ({ id: "daily", pattern: schedule.cron, timezone: undefined }),
		});
		const { logger } = silentLogger();

		await expect(
			processScheduleTick(
				{ store, transport, registry: registryOf(echoTask), logger },
				"daily",
				Date.now()
			)
		).rejects.toThrow(TaskNotRegisteredError);

		expect(runsCreateCalls).toEqual([]);
	});
});
