import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { escapeIdentifier } from "pg";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { createNotifyBatcher, type NotifyBatcher } from "../../src/pg/notify-batcher.js";
import { createPgTransport } from "../../src/pg/transport.js";
import { createRedisConnections } from "../../src/queue/connections.js";
import { createTransport, type Transport } from "../../src/queue/transport.js";
import { buildRegistry } from "../../src/registry.js";
import { processScheduleTick } from "../../src/schedules.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import { defineTask } from "../../src/task.js";
import {
	describeTransports,
	type EngineOverrides,
	engineConfigFor,
} from "../helpers/engine-factory.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

describeTransports("schedules", transport => {
	const ns = useTestNamespace();

	const shortId = () => randomUUID().slice(0, 8);
	const uniqueQueue = () => `q${shortId()}`;
	const uniqueTaskId = (label: string) => `${label}-${shortId()}`;
	const uniqueScheduleId = (label: string) => `${label}-${shortId()}`;

	const logs: { level: string; message: string }[] = [];
	const testLogger: EngineLogger = {
		debug: (_f, message) => logs.push({ level: "debug", message }),
		info: (_f, message) => logs.push({ level: "info", message }),
		warn: (_f, message) => logs.push({ level: "warn", message }),
		error: (_f, message) => logs.push({ level: "error", message }),
	};

	const engines: Engine[] = [];
	/** Standalone `Transport`s (test (f)): created outside any engine, closed in `afterEach`. */
	const transports: Transport[] = [];

	async function startedEngine(
		tasks: Parameters<typeof buildRegistry>[0],
		overrides: EngineOverrides = {}
	) {
		const engine = createEngine(
			engineConfigFor(transport, ns, {
				tasks,
				pool: { max: 8 },
				heartbeatIntervalMs: 200,
				heartbeatTimeoutMs: 1_000,
				stallTimeoutMs: 5_000,
				shutdownTimeoutMs: 2_000,
				reconciler: { intervalMs: 1_000, graceMs: 500, batchSize: 50 },
				logger: testLogger,
				...overrides,
			})
		);
		engines.push(engine);
		await engine.migrate();
		await engine.start();
		return engine;
	}

	const t = (table: string) => `${escapeIdentifier(ns.schema)}.${table}`;

	/**
	 * A raw BullMQ `Queue` on the `-system` queue, for tests that need to read/write a job scheduler
	 * directly (bypassing `engine.schedules.*`) -- e.g. simulating drift or an orphan the way another
	 * process or a hand-edited Postgres row would produce it. Connection + queue closed in `afterEach`.
	 */
	let rawSystemRedis: Redis | undefined;
	let rawSystemQueue: Queue | undefined;
	function systemQueue(): Queue {
		if (!rawSystemQueue) {
			rawSystemRedis = new Redis(ns.redisUrl);
			rawSystemQueue = new Queue(`${ns.prefix}-system`, {
				connection: rawSystemRedis,
				prefix: ns.prefix,
			});
		}
		return rawSystemQueue;
	}

	const batchers: NotifyBatcher[] = [];

	/**
	 * A standalone transport of the mode under test, outside any engine (never started; closed in
	 * `afterEach`): the BullMQ one on the test prefix, or the Postgres one on the test schema.
	 */
	function standaloneTransport(queueName?: string): Transport {
		const queues = queueName ? [{ name: queueName, concurrency: 1 }] : [];
		let tr: Transport;
		if (transport === "bullmq") {
			tr = createTransport({
				prefix: ns.prefix,
				connections: createRedisConnections(ns.redisUrl),
				queues,
				schema: ns.schema,
			});
		} else {
			const batcher = createNotifyBatcher({ db: ns.pool, logger: testLogger });
			batchers.push(batcher);
			tr = createPgTransport({
				store: createSqlStore(ns.pool, ns.schema),
				batcher,
				schema: ns.schema,
				queues,
				systemConcurrency: 1,
				stallTimeoutMs: 5_000,
				polling: { intervalMs: 1_000, jitterMs: 0 },
				wake: { subscribe: () => () => {} },
				logger: testLogger,
			});
		}
		transports.push(tr);
		return tr;
	}

	/**
	 * Raw access to the job scheduler behind a schedule (a BullMQ job scheduler, or the
	 * `schedule-tick:<id>` row of `system_jobs` in Postgres mode, decision 8), bypassing
	 * `engine.schedules.*` -- e.g. to simulate drift or an orphan.
	 */
	let rawTransport: Transport | undefined;
	const rawSchedulers = {
		async get(id: string) {
			if (transport === "bullmq") {
				const s = await systemQueue().getJobScheduler(id);
				return s ? { pattern: s.pattern } : undefined;
			}
			rawTransport ??= standaloneTransport();
			const s = await rawTransport.getJobScheduler(id);
			return s ? { pattern: s.pattern } : undefined;
		},
		async upsert(id: string, pattern: string) {
			if (transport === "bullmq") {
				await systemQueue().upsertJobScheduler(
					id,
					{ pattern },
					{ name: "schedule-tick", data: { scheduleId: id } }
				);
				return;
			}
			rawTransport ??= standaloneTransport();
			await rawTransport.upsertJobScheduler({ id, pattern });
		},
	};

	afterEach(async () => {
		// Fix "(d)" (opus review, Phase 8 tests): delete every schedule this test created before
		// stopping the engines -- a `* * * * * *`-style schedule left behind would keep producing real
		// BullMQ ticks (job schedulers live in Redis independently of any engine process) that could
		// bleed into later tests sharing this file's namespace. Some tests stop an engine themselves
		// mid-test (its own pool is then closed), so try each still-tracked engine until one works
		// rather than assuming `engines[0]` is still alive; best-effort, never fails the test.
		for (const candidate of engines) {
			try {
				const remaining = await candidate.schedules.list();
				await Promise.all(remaining.map(s => candidate.schedules.delete(s.id)));
				break;
			} catch {
				// This engine is already stopped (or otherwise unusable) -- try the next one.
			}
		}
		await Promise.all(engines.splice(0).map(e => e.stop().catch(() => {})));
		rawTransport = undefined;
		await Promise.all(
			transports.splice(0).map(tr => tr.closeWorkers({ timeoutMs: 500 }).catch(() => {}))
		);
		await Promise.all(batchers.splice(0).map(b => b.close().catch(() => {})));
		if (rawSystemQueue) {
			const q = rawSystemQueue;
			rawSystemQueue = undefined;
			await q.close();
		}
		if (rawSystemRedis) {
			const r = rawSystemRedis;
			rawSystemRedis = undefined;
			r.disconnect();
		}
		logs.length = 0;
	});

	describe("engine.schedules (api)", () => {
		it("upsert writes postgres and BullMQ; a high-frequency tick creates a run with the given payload", async () => {
			const task = defineTask({
				id: uniqueTaskId("api-sched"),
				queue: { name: uniqueQueue() },
				run: async (payload: { v: number }) => payload.v,
			});
			const engine = await startedEngine([task]);
			const id = uniqueScheduleId("every-second");

			const created = await engine.schedules.upsert({
				id,
				task,
				cron: "* * * * * *", // Every second (6-field cron; api-notes.md §1.10, verified against the
				// installed bullmq/cron-parser directly).
				payload: { v: 7 },
			});
			expect(created).toMatchObject({ id, taskId: task.id, cron: "* * * * * *", source: "api" });

			await waitFor(
				async () => {
					const { items } = await engine.runs.list({ taskId: task.id, status: "completed" });
					return items.some(r => r.output === 7);
				},
				{ timeoutMs: 8_000, intervalMs: 100 }
			);

			const listed = await engine.schedules.list();
			expect(listed.find(s => s.id === id)).toBeDefined();
		});

		it("payload changed only in Postgres (BullMQ scheduler left untouched) is what the next tick uses", async () => {
			let lastPayload: number | undefined;
			const task = defineTask({
				id: uniqueTaskId("pg-payload"),
				queue: { name: uniqueQueue() },
				run: async (payload: { v: number }) => {
					lastPayload = payload.v;
					return payload.v;
				},
			});
			const engine = await startedEngine([task]);
			const id = uniqueScheduleId("changing");
			await engine.schedules.upsert({ id, task, cron: "* * * * * *", payload: { v: 1 } });

			await waitFor(async () => lastPayload === 1, { timeoutMs: 8_000, intervalMs: 100 });

			// Mutate ONLY the postgres row directly (never touches BullMQ's job scheduler): proves the
			// tick handler always re-reads the payload from postgres rather than trusting anything cached
			// on the BullMQ side (decision 8).
			await ns.pool.query(`UPDATE ${t("schedules")} SET payload = $2::jsonb WHERE id = $1`, [
				id,
				JSON.stringify({ v: 2 }),
			]);

			await waitFor(async () => lastPayload === 2, { timeoutMs: 8_000, intervalMs: 100 });
		});

		it("delete removes the schedule; a manually-fired tick for it afterwards creates no run, even if the BullMQ removal itself fails", async () => {
			const task = defineTask({
				id: uniqueTaskId("del"),
				queue: { name: uniqueQueue() },
				run: async () => {},
			});
			const engine = await startedEngine([task]);
			const id = uniqueScheduleId("to-delete");
			await engine.schedules.upsert({ id, task, cron: "0 3 * * *" }); // Not due for hours: no real tick races this test.

			expect(await engine.schedules.delete(id)).toBe(true);
			expect((await engine.schedules.list()).find(s => s.id === id)).toBeUndefined();
			expect(await engine.schedules.delete(id)).toBe(false); // Already gone.

			// Simulate a tick landing right after `delete()` -- possible if its own BullMQ removal had
			// failed (Postgres is authoritative either way, decision 8): the schedule is already gone
			// from Postgres, so `processScheduleTick` must create no run, regardless of what BullMQ says.
			const store = createSqlStore(ns.pool, ns.schema);
			const registry = buildRegistry([task]);
			const failingTransport: Pick<
				Transport,
				| "enqueueRun"
				| "removeSystemJob"
				| "upsertJobScheduler"
				| "removeJobScheduler"
				| "getJobScheduler"
			> = {
				enqueueRun: async () => {
					throw new Error("must not be called: no run should be created");
				},
				removeSystemJob: async () => {},
				upsertJobScheduler: async () => {},
				removeJobScheduler: async () => {
					throw new Error("simulated BullMQ removal failure");
				},
				getJobScheduler: async () => undefined,
			};
			await expect(
				processScheduleTick(
					{ store, transport: failingTransport, registry, logger: testLogger },
					id,
					Date.now()
				)
			).resolves.toBeUndefined();

			const { items } = await store.runs.list({ taskId: task.id, limit: 10 });
			expect(items).toEqual([]);
		});

		it("delete with a REAL BullMQ removeJobScheduler failure still produces zero runs afterwards (self-heals)", async () => {
			// Test (c) (opus review): unlike the test above (a hand-built `failingTransport`), this goes
			// through the real `engine.schedules.delete()` -> real `Queue.removeJobScheduler` path, and
			// proves the self-heal end to end with a REAL, still-firing `* * * * * *` BullMQ scheduler:
			// even though `removeJobScheduler` itself fails so the scheduler survives in Redis, every
			// real tick it keeps producing hits `processScheduleTick`'s "missing schedule" branch (the
			// postgres row IS gone) and creates no run.
			const task = defineTask({
				id: uniqueTaskId("del-real-fail"),
				queue: { name: uniqueQueue() },
				run: async () => {},
			});
			const engine = await startedEngine([task]);
			const id = uniqueScheduleId("fast-delete-fails");
			await engine.schedules.upsert({ id, task, cron: "* * * * * *" });
			await waitFor(async () => (await rawSchedulers.get(id)) !== undefined, {
				timeoutMs: 5_000,
				intervalMs: 50,
			});

			if (transport === "bullmq") {
				const spy = vi
					.spyOn(Queue.prototype, "removeJobScheduler")
					.mockRejectedValueOnce(new Error("simulated real BullMQ removeJobScheduler failure"));
				try {
					expect(await engine.schedules.delete(id)).toBe(true); // postgres row gone regardless
				} finally {
					spy.mockRestore();
				}
			} else {
				// Postgres mode: the same end state (schedule row gone, tick row left behind) is
				// produced by deleting the `schedules` row alone.
				await ns.pool.query(`DELETE FROM ${t("schedules")} WHERE id = $1`, [id]);
			}
			expect((await engine.schedules.list()).find(s => s.id === id)).toBeUndefined();

			// The scheduler is still alive in Redis (the mocked removal never actually ran): give it
			// >= 2s of real ticks. None must produce a run, and the self-heal (fix 1: re-checked right
			// before removing) should eventually remove the scheduler itself too.
			await new Promise(resolve => setTimeout(resolve, 2_200));

			const { items } = await engine.runs.list({ taskId: task.id, limit: 10 });
			expect(items).toEqual([]);
			await waitFor(async () => (await rawSchedulers.get(id)) === undefined, {
				timeoutMs: 5_000,
				intervalMs: 100,
			});
		});

		it("two processes racing the exact same tick (e.g. a stalled-job redelivery) create exactly one run", async () => {
			const task = defineTask({
				id: uniqueTaskId("race"),
				queue: { name: uniqueQueue() },
				run: async () => {},
			});
			const store = createSqlStore(ns.pool, ns.schema);
			await store.schedules.upsert({
				id: "race-sched",
				taskId: task.id,
				cron: "* * * * * *",
				timezone: null,
				payload: null,
				source: "api",
			});
			const registry = buildRegistry([task]);
			const noopTransport: Pick<
				Transport,
				| "enqueueRun"
				| "removeSystemJob"
				| "upsertJobScheduler"
				| "removeJobScheduler"
				| "getJobScheduler"
			> = {
				enqueueRun: async () => {},
				removeSystemJob: async () => {},
				upsertJobScheduler: async () => {},
				removeJobScheduler: async () => true,
				getJobScheduler: async () => ({
					id: "race-sched",
					pattern: "* * * * * *",
					timezone: undefined,
				}),
			};
			const deps = { store, transport: noopTransport, registry, logger: testLogger };
			const scheduledAtMs = Date.now();

			// Simulate two `-system` workers (in two different processes) both processing the SAME tick
			// (same scheduleId + scheduledAtMs), e.g. one redelivered after a stalled-job recovery.
			await Promise.all([
				processScheduleTick(deps, "race-sched", scheduledAtMs),
				processScheduleTick(deps, "race-sched", scheduledAtMs),
				processScheduleTick(deps, "race-sched", scheduledAtMs),
			]);

			const { items } = await store.runs.list({ taskId: task.id, limit: 10 });
			expect(items).toHaveLength(1);
		});

		it("two real engine processes ticking the same api schedule create at most one run per elapsed second", async () => {
			const task = defineTask({
				id: uniqueTaskId("two-proc"),
				queue: { name: uniqueQueue() },
				run: async () => {},
			});
			const engineA = await startedEngine([task]);
			const engineB = await startedEngine([task]);
			const id = uniqueScheduleId("shared");
			const start = Date.now();
			await engineA.schedules.upsert({ id, task, cron: "* * * * * *" });

			await waitFor(async () => (await engineA.runs.list({ taskId: task.id })).items.length >= 2, {
				timeoutMs: 8_000,
				intervalMs: 100,
			});
			// Give it a moment more so both processes have had a fair chance to race ticks.
			await new Promise(resolve => setTimeout(resolve, 1_500));

			const elapsedSeconds = Math.ceil((Date.now() - start) / 1_000);
			const { items } = await engineA.runs.list({ taskId: task.id, limit: 50 });
			// Each second produces at most one tick/run; a duplicate-run bug would push this well past
			// `elapsedSeconds`.
			expect(items.length).toBeLessThanOrEqual(elapsedSeconds + 1);

			// Strengthened assertions (opus review, test (a)): one idempotency key per run, every
			// `__schedule:${id}:${millis}` key's `millis` distinct and a multiple of 1000 (decision 9's
			// namespace, `* * * * * *`'s own second-granularity), and evidence that both engines (not
			// just A) actually processed at least one tick -- proving the "at most one run per second"
			// bound isn't just because engine B never got a chance to race.
			const { rows: keys } = await ns.pool.query<{ key: string }>(
				`SELECT key FROM ${t("idempotency_keys")} WHERE task_id = $1 AND key LIKE $2`,
				[task.id, `__schedule:${id}:%`]
			);
			expect(keys).toHaveLength(items.length);
			const millis = keys.map(k => Number(k.key.slice(`__schedule:${id}:`.length)));
			expect(new Set(millis).size).toBe(millis.length); // every millis distinct
			for (const ms of millis) expect(ms % 1_000).toBe(0);

			// "Both engines actually processed a tick" isn't directly observable here: a successful tick
			// logs nothing distinguishing which process handled it (no per-engine id in the shared test
			// logger), and both `-system` workers pull from the very same BullMQ queue, so which of the
			// two happens to win any given tick is inherently nondeterministic. `elapsedSeconds` worth of
			// distinct, second-aligned idempotency keys above, produced while BOTH engines were started
			// and running their `-system` worker concurrently, is the available indirect evidence that
			// this is really a two-process race and not just engine A ticking alone.
			void engineB;
		});
	});

	describe("defineTask({ schedule }): declarative sync", () => {
		it("is created in postgres + BullMQ on start() and produces runs on its own", async () => {
			let runs = 0;
			const task = defineTask({
				id: uniqueTaskId("declarative"),
				queue: { name: uniqueQueue() },
				schedule: { cron: "* * * * * *", payload: { greeting: "hi" } },
				run: async (payload: { greeting: string }) => {
					runs++;
					return payload.greeting;
				},
			});
			const engine = await startedEngine([task]);

			await waitFor(async () => runs >= 2, { timeoutMs: 8_000, intervalMs: 100 });

			const mine = (await engine.schedules.list()).find(s => s.id === task.id);
			expect(mine).toMatchObject({
				id: task.id,
				taskId: task.id,
				cron: "* * * * * *",
				payload: { greeting: "hi" },
				source: "declarative",
			});
		});

		it("removed from code: persists by default, is removed with schedules.pruneDeclarative: true", async () => {
			const taskId = uniqueTaskId("prune");
			const declaring = defineTask({
				id: taskId,
				queue: { name: uniqueQueue() },
				schedule: { cron: "0 3 * * *" }, // Not due soon: no real ticks interfere.
				run: async () => {},
			});
			const engine1 = await startedEngine([declaring]);
			await engine1.stop();

			// "code no longer declares the schedule": a fresh engine that doesn't register this task at
			// all. Default `pruneDeclarative: false` (decision 21): the row must be left alone.
			const engine2 = await startedEngine([]);
			const stillThere = (await engine2.schedules.list()).find(s => s.id === taskId);
			expect(stillThere).toBeDefined();
			expect(stillThere?.source).toBe("declarative");
			await engine2.stop();

			const engine3 = await startedEngine([], { schedules: { pruneDeclarative: true } });
			const gone = (await engine3.schedules.list()).find(s => s.id === taskId);
			expect(gone).toBeUndefined();
		});
	});

	describe("reconciler: schedule drift/orphan repair via the leader (real BullMQ, test (e))", () => {
		it("corrects a live scheduler drifted from postgres, and removes an orphan scheduler with no matching row", async () => {
			const task = defineTask({
				id: uniqueTaskId("recon-sched"),
				queue: { name: uniqueQueue() },
				run: async () => {},
			});
			// A single started engine is always its own leader; its reconciler loop (intervalMs: 1000,
			// `startedEngine`'s config above) is what must do the repair/removal below, with no direct
			// call into `createReconcileSchedulesHook` from this test.
			const engine = await startedEngine([task]);
			const id = uniqueScheduleId("drift-live");
			await engine.schedules.upsert({ id, task, cron: "0 9 * * *" }); // Not due soon: no real tick races this.
			await waitFor(async () => (await rawSchedulers.get(id))?.pattern === "0 9 * * *", {
				timeoutMs: 5_000,
				intervalMs: 100,
			});

			// Drift the LIVE BullMQ scheduler only (postgres untouched): as if a hand-edited scheduler,
			// or another process's upsert that raced with an in-flight postgres change. Decision 8:
			// postgres is authoritative, so the leader must correct the live scheduler back.
			await rawSchedulers.upsert(id, "0 10 * * *");
			await waitFor(async () => (await rawSchedulers.get(id))?.pattern === "0 9 * * *", {
				timeoutMs: 5_000,
				intervalMs: 100,
			});

			// Orphan: a real BullMQ scheduler with no matching postgres row at all.
			const orphanId = uniqueScheduleId("orphan-live");
			await rawSchedulers.upsert(orphanId, "0 9 * * *");
			await waitFor(async () => (await rawSchedulers.get(orphanId)) === undefined, {
				timeoutMs: 5_000,
				intervalMs: 100,
			});
		});
	});

	describe("schedule-tick: repairs a drifted BullMQ scheduler against a real scheduler (test (f))", () => {
		it("processScheduleTick itself repairs cron+timezone drift, independent of the reconciler", async () => {
			const queueName = uniqueQueue();
			const task = defineTask({
				id: uniqueTaskId("tick-repair"),
				queue: { name: queueName },
				run: async () => {},
			});
			const store = createSqlStore(ns.pool, ns.schema);
			const registry = buildRegistry([task]);
			const tr = standaloneTransport(queueName);

			const id = uniqueScheduleId("tick-repair");
			await store.schedules.upsert({
				id,
				taskId: task.id,
				cron: "0 9 * * *",
				timezone: null,
				payload: null,
				source: "api",
			});
			await tr.upsertJobScheduler({ id, pattern: "0 9 * * *" });

			// Drift only postgres (as a different process's write, or a hand edit, would): a new cron
			// AND a new timezone, neither reflected in the live BullMQ scheduler yet.
			await store.schedules.upsert({
				id,
				taskId: task.id,
				cron: "0 10 * * *",
				timezone: "America/Belem",
				payload: null,
				source: "api",
			});

			// One direct tick (no engine/worker involved) must repair the live scheduler in place before
			// doing anything else -- this is `processScheduleTick`'s own repair, not the reconciler's.
			await processScheduleTick(
				{ store, transport: tr, registry, logger: testLogger },
				id,
				Date.now()
			);

			const live = await tr.getJobScheduler(id);
			expect(live?.pattern).toBe("0 10 * * *");
			expect(live?.timezone).toBe("America/Belem");
		});
	});
});
