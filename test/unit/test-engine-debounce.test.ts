/**
 * Debounce (decision 24) and concurrency keys (decision 25) in `createTestEngine`. Debounce works
 * as in production for triggers issued before the engine drains. The test drain runs one job at a
 * time, so two runs of a key never overlap on their own: the concurrency tests seed a `running`
 * row of the key directly in the memory store.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { ConfigError, defineTask, type EngineLogger } from "../../src/index.js";
import * as memoryStoreModule from "../../src/store/memory/index.js";
import type { Store } from "../../src/store/types.js";
import {
	createTestEngine,
	type TestEngine,
	type TestEngineConfig,
} from "../../src/testing/index.js";

const quietLogger: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const engines: TestEngine[] = [];
function testEngine(config: TestEngineConfig): TestEngine {
	const engine = createTestEngine({ logger: quietLogger, ...config });
	engines.push(engine);
	return engine;
}

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

describe("createTestEngine: debounce", () => {
	it("collapses triggers issued before a drain; payload last by default", async () => {
		const seen: number[] = [];
		const task = defineTask({
			id: "recalc",
			run: async (payload: { n: number }) => {
				seen.push(payload.n);
				return payload.n;
			},
		});
		const engine = testEngine({ tasks: [task] });
		const ids = new Set<string>();
		for (let n = 1; n <= 5; n++) {
			ids.add((await engine.trigger(task, { n }, { debounce: { key: "c", delay: "2s" } })).id);
		}
		expect(ids.size).toBe(1);
		const [id] = [...ids];
		const result = await engine.drive(id ?? "");
		expect(result).toMatchObject({ status: "completed", output: 5, attempt: 1 });
		expect(seen).toEqual([5]);
		expect((await engine.runs.list({ taskId: "recalc" })).items).toHaveLength(1);
	});

	it("payload first and maxDelay follow the store clock (trailing)", async () => {
		const task = defineTask({ id: "t", run: async (payload: { n: number }) => payload.n });
		const engine = testEngine({ tasks: [task], clock: { autoAdvance: false } });
		const first = { key: "k1", delay: "10s", payload: "first" } as const;
		const a = await engine.trigger(task, { n: 1 }, { debounce: first });
		const startA = engine.clock.now().getTime();
		await engine.clock.advance("6s");
		await engine.trigger(task, { n: 2 }, { debounce: first });
		// Trailing: the second trigger pushes run_after to (now + delay) from itself.
		expect((await engine.runs.get(a))?.runAfter?.getTime()).toBe(startA + 6_000 + 10_000);
		await engine.clock.advance("10s");
		expect(await engine.runs.get(a)).toMatchObject({ status: "completed", output: 1 });

		const capped = { key: "cap", delay: "10s", maxDelay: "15s" } as const;
		const b = await engine.trigger(task, { n: 3 }, { debounce: capped });
		const bStart = engine.clock.now().getTime();
		await engine.clock.advance("8s");
		await engine.trigger(task, { n: 4 }, { debounce: capped });
		expect((await engine.runs.get(b))?.runAfter?.getTime()).toBe(bStart + 15_000);
		await engine.clock.advance("7s");
		expect(await engine.runs.get(b)).toMatchObject({ status: "completed", output: 4 });
	});

	it("rejects debounce with idempotencyKey, and a concurrencyKey on a queue without one", async () => {
		const task = defineTask({ id: "t", run: async () => "ok" });
		const engine = testEngine({ tasks: [task] });
		await expect(
			engine.trigger(task, undefined, { debounce: { key: "k", delay: 10 }, idempotencyKey: "i" })
		).rejects.toBeInstanceOf(ConfigError);
		await expect(engine.trigger(task, undefined, { concurrencyKey: "k" })).rejects.toBeInstanceOf(
			ConfigError
		);
		await expect(
			engine.trigger(task, undefined, { debounce: { key: "", delay: 10 } })
		).rejects.toBeInstanceOf(ConfigError);
		await expect(
			engine.trigger(task, undefined, { debounce: { key: "k", delay: 0 } })
		).rejects.toBeInstanceOf(ConfigError);
	});
});

describe("createTestEngine: concurrency key", () => {
	it("a run over the limit waits (no attempt spent) until the key's running run finishes", async () => {
		const spy = vi.spyOn(memoryStoreModule, "createMemoryStore");
		const task = defineTask({
			id: "course",
			queue: { name: "classification", concurrencyKey: { limit: 1 } },
			run: async (payload: { n: number }) => payload.n,
		});
		const engine = testEngine({ tasks: [task], clock: { autoAdvance: false } });
		const store = spy.mock.results[0]?.value as Store | undefined;
		const hooks = store ? memoryStoreModule.memoryStoreTestHooks(store) : null;
		if (!hooks) throw new Error("no memory store hooks");

		// Seed a run of the key that is "running" elsewhere (another process in production).
		const busy = await engine.trigger(task, { n: 1 }, { concurrencyKey: "course:1", delay: "1h" });
		await hooks.patchRun(busy.id, { status: "running" });

		const other = await engine.trigger(task, { n: 2 }, { concurrencyKey: "course:2" });
		const limited = await engine.trigger(task, { n: 3 }, { concurrencyKey: "course:1" });
		await engine.drain();
		expect(await engine.runs.get(other)).toMatchObject({ status: "completed" });
		const waiting = await engine.runs.get(limited);
		expect(waiting).toMatchObject({ status: "queued", attempt: 1, concurrencyKey: "course:1" });

		await hooks.patchRun(busy.id, { status: "canceled" });
		await engine.clock.advance("2s");
		expect(await engine.runs.get(limited)).toMatchObject({
			status: "completed",
			output: 3,
			attempt: 1,
		});
	});
});
