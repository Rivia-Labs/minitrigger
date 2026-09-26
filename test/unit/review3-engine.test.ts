/**
 * Third external review, engine items with no infra needed:
 * - 2: a raw TypeORM `dataSource.manager`/`DataSource` (no own `queryRunner`, no
 *   `startTransaction`) is recognized as an unwrapped ORM handle;
 * - 3: once fully stopped every method fails with the clean "stopped" error; while stopping,
 *   reads, runs.cancel and tokens.complete still work;
 * - 4: `syncQueueStates` never undoes a pause committed while it runs.
 */
import { afterEach, describe, expect, it } from "vitest";
import { resolveConfig } from "../../src/config.js";
import {
	createEngineCore,
	type EngineCore,
	type EngineCoreLifecycle,
	validateTransactionClient,
} from "../../src/engine.js";
import { MinitriggerError } from "../../src/errors.js";
import { defineTask, type EngineLogger } from "../../src/index.js";
import { buildRegistry } from "../../src/registry.js";
import { createMemoryStore } from "../../src/store/memory/index.js";
import type { Store } from "../../src/store/types.js";
import { createTestEngine, type TestEngine } from "../../src/testing/index.js";
import { createMemoryPubSubHub, createMemoryStreams } from "../../src/testing/memory-pubsub.js";
import { createMemoryTransport, type MemoryTransport } from "../../src/testing/memory-transport.js";

const quiet: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };

describe("item 2: unwrapped TypeORM handles without queryRunner/startTransaction", () => {
	// The real (non-transactional) `dataSource.manager` has neither an own `queryRunner` key nor
	// `startTransaction`; TypeORM tags its classes with `"@instanceof"`.
	const manager = {
		"@instanceof": Symbol.for("EntityManager"),
		query: async () => [],
		transaction: async () => {},
		getRepository: () => ({}),
	};
	const dataSource = {
		"@instanceof": Symbol.for("DataSource"),
		query: async () => [],
		transaction: async () => {},
		getRepository: () => ({}),
	};

	it.each([
		["EntityManager", manager],
		["DataSource", dataSource],
	])("%s points at @rivia-labs/minitrigger/typeorm", (_label, handle) => {
		expect(() => validateTransactionClient(handle)).toThrow(/@rivia-labs\/minitrigger\/typeorm/);
	});

	it("same shape without the tag (query + transaction + getRepository) is also recognized", () => {
		const untagged = { query: async () => [], transaction: async () => {}, getRepository() {} };
		expect(() => validateTransactionClient(untagged)).toThrow(/@rivia-labs\/minitrigger\/typeorm/);
	});
});

const task = defineTask({
	id: "review3-task",
	queue: { name: "review3" },
	run: async (_p: undefined, ctx) => {
		await ctx.step.createToken("t");
		return null;
	},
});

const engines: TestEngine[] = [];
afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

function buildCore(
	onIsPaused: (queue: string) => Promise<void> = async () => {},
	lifecycle: EngineCoreLifecycle = { isShuttingDown: () => false, assertNotStopped: () => {} }
): {
	core: EngineCore;
	store: Store;
	transport: MemoryTransport;
} {
	const resolved = resolveConfig({
		postgres: "memory://review3",
		redis: "memory://review3",
		tasks: [task],
		logger: quiet,
	});
	const registry = buildRegistry(resolved.tasks);
	const store = createMemoryStore({ now: () => Date.now() });
	const transport = createMemoryTransport({
		prefix: resolved.prefix,
		queues: Array.from(registry.queues.values()),
		now: () => Date.now(),
	});
	const isQueuePaused = transport.isQueuePaused.bind(transport);
	transport.isQueuePaused = async queue => {
		await onIsPaused(queue);
		return isQueuePaused(queue);
	};
	const hub = createMemoryPubSubHub();
	const streams = createMemoryStreams();
	const core = createEngineCore(
		resolved,
		registry,
		{
			store,
			transport,
			publisher: hub.publisher,
			createPubSub: hub.createPubSub,
			streams,
			readStream: (runId, name, signal) => streams.read(runId, name, signal),
		},
		lifecycle
	);
	return { core, store, transport };
}

describe("item 3: stopping vs stopped", () => {
	/** A core whose lifecycle is driven by the test: "stopping" first, then "closed". */
	function lifecycleCore() {
		const state = { stopping: false, closed: false };
		const stopped = () => new MinitriggerError("this engine was stopped; create a new one");
		const built = buildCore(undefined, {
			isShuttingDown: () => state.stopping,
			assertNotStopped: () => {
				if (state.stopping) throw stopped();
			},
			assertNotClosed: () => {
				if (state.closed) throw stopped();
			},
		});
		return { ...built, state };
	}

	it("while stopping, reads/cancel/tokens.complete work; trigger and schedule writes don't", async () => {
		const { core, state } = lifecycleCore();
		const handle = await core.trigger(task);
		state.stopping = true;
		expect((await core.runs.get(handle.id))?.id).toBe(handle.id);
		expect((await core.runs.list()).items).toHaveLength(1);
		expect((await core.runs.logs(handle.id)).items).toEqual([]);
		expect(await core.runs.steps(handle.id)).toEqual([]);
		expect(await core.queues.list()).toHaveLength(1);
		expect(await core.schedules.list()).toEqual([]);
		expect(await core.tokens.complete("00000000-0000-4000-8000-000000000000")).toEqual({
			completed: false,
			reason: "not_found",
		});
		expect(await core.runs.cancel(handle.id)).toBe("canceled");
		await expect(core.trigger(task)).rejects.toThrow(/stopped/);
		await expect(core.schedules.upsert({ id: "x", task, cron: "0 3 * * *" })).rejects.toThrow(
			/stopped/
		);
		await expect(core.schedules.delete("x")).rejects.toThrow(/stopped/);
	});

	it("once closed, every method throws the lifecycle error", async () => {
		const { core, state } = lifecycleCore();
		const handle = await core.trigger(task);
		state.stopping = true;
		state.closed = true;
		const id = handle.id;
		const calls: Array<[string, () => Promise<unknown>]> = [
			["runs.get", () => core.runs.get(id)],
			["runs.list", () => core.runs.list()],
			["runs.logs", () => core.runs.logs(id)],
			["runs.steps", () => core.runs.steps(id)],
			["runs.cancel", () => core.runs.cancel(id)],
			["tokens.complete", () => core.tokens.complete("00000000-0000-4000-8000-000000000000")],
			["queues.list", () => core.queues.list()],
			["schedules.list", () => core.schedules.list()],
			["schedules.delete", () => core.schedules.delete("x")],
			["schedules.upsert", () => core.schedules.upsert({ id: "x", task, cron: "0 3 * * *" })],
		];
		for (const [name, call] of calls) {
			await expect(call(), name).rejects.toThrow(/stopped/);
			await expect(call(), name).rejects.toBeInstanceOf(MinitriggerError);
		}
	});

	it("a test engine still answers reads after stop() (drive/runInline interrupted by stop)", async () => {
		const engine = createTestEngine({ tasks: [task], logger: quiet, clock: { start: 0 } });
		engines.push(engine);
		const handle = await engine.trigger(task);
		await engine.stop();
		expect((await engine.runs.get(handle.id))?.status).toBe("queued");
		await expect(engine.trigger(task)).rejects.toThrow(/stopped/);
	});
});

describe("item 4: syncQueueStates vs a concurrent pause", () => {
	it("a pause committed between the Postgres list and the Redis read is not resumed", async () => {
		let injected = false;
		let core!: EngineCore;
		const built = buildCore(async () => {
			if (injected) return;
			injected = true;
			// Another process's queues.pause(): Postgres first, then the Redis flag.
			await core.queues.pause("review3");
		});
		core = built.core;
		// Postgres says "not paused" when sync lists it.
		await built.store.queues.setPaused("review3", false);

		await core.syncQueueStates();

		expect(injected).toBe(true);
		expect((await built.store.queues.get("review3"))?.paused).toBe(true);
		expect(await built.transport.isQueuePaused("review3")).toBe(true);
	});
});
