/** Phase 7: `engine.runs.list` filters/pagination and `engine.runs.steps`. */
import { randomUUID } from "node:crypto";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { BullmqTransportConfig, CommonConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine, type Run } from "../../src/engine.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);
const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const engines: Engine[] = [];
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

function makeEngine(
	tasks: AnyTask[],
	overrides: Partial<CommonConfig> & Partial<BullmqTransportConfig> = {}
): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 5 },
		heartbeatIntervalMs: 1_000,
		stallTimeoutMs: 10_000,
		shutdownTimeoutMs: 2_000,
		logger: silent,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

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
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

describe("runs.list", () => {
	it("filters by task, status (single or array) and created range; pages by cursor", async () => {
		const task = defineTask({
			id: `listed-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async () => "unused",
		});
		const other = defineTask({
			id: `other-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async () => "unused",
		});
		// Trigger-only: runs stay queued unless canceled.
		const engine = makeEngine([task, other]);
		const a = await engine.trigger(task);
		await sleep(15);
		const b = await engine.trigger(task);
		await sleep(15);
		const c = await engine.trigger(task);
		await engine.trigger(other);
		expect(await engine.runs.cancel(b)).toBe("canceled");

		const ids = (runs: Run[]) => runs.map(r => r.id);
		const byTask = await engine.runs.list({ taskId: task.id });
		expect(ids(byTask.items)).toEqual([c.id, b.id, a.id]);
		expect(byTask.nextCursor).toBeNull();

		expect(ids((await engine.runs.list({ taskId: task.id, status: "canceled" })).items)).toEqual([
			b.id,
		]);
		expect(
			ids((await engine.runs.list({ taskId: task.id, status: ["queued", "canceled"] })).items)
		).toEqual([c.id, b.id, a.id]);
		expect(
			ids((await engine.runs.list({ taskId: task.id, status: ["completed", "failed"] })).items)
		).toEqual([]);

		const runB = await engine.runs.get(b);
		const createdB = runB?.createdAt ?? new Date(0);
		expect(ids((await engine.runs.list({ taskId: task.id, createdFrom: createdB })).items)).toEqual(
			[c.id, b.id]
		);
		expect(ids((await engine.runs.list({ taskId: task.id, createdTo: createdB })).items)).toEqual([
			a.id,
		]);
		expect(
			ids(
				(
					await engine.runs.list({
						taskId: task.id,
						createdFrom: createdB,
						createdTo: new Date(createdB.getTime() + 1),
					})
				).items
			)
		).toEqual([b.id]);

		const page1 = await engine.runs.list({ taskId: task.id, limit: 2 });
		expect(ids(page1.items)).toEqual([c.id, b.id]);
		expect(page1.nextCursor).not.toBeNull();
		const page2 = await engine.runs.list({
			taskId: task.id,
			limit: 2,
			...(page1.nextCursor ? { cursor: page1.nextCursor } : {}),
		});
		expect(ids(page2.items)).toEqual([a.id]);
		expect(page2.nextCursor).toBeNull();

		// Limit is clamped to 100.
		expect((await engine.runs.list({ limit: 1_000 })).items.length).toBeLessThanOrEqual(100);
	});
});

describe("runs.steps", () => {
	it("lists every step in seq order, exposing only references for waits and triggers", async () => {
		const child = defineTask({
			id: `child-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (payload: { n: number }) => payload.n * 2,
		});
		const parent = defineTask({
			id: `parent-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_payload: undefined, ctx) => {
				const one = await ctx.step.run("compute", () => ({ value: 1 }));
				await ctx.step.sleep("pause", 50);
				await ctx.step.trigger("fire", child, { n: 1 });
				const waited = await ctx.step.triggerAndWait("await-child", child, { n: 2 });
				const token = await ctx.step.createToken("approval", { timeout: "1h" });
				return { one, waited: waited.ok ? waited.output : null, token: token.id };
			},
		});
		const engine = makeEngine([parent, child]);
		await engine.start();
		const handle = await engine.trigger(parent);
		await waitFor(async () => (await engine.runs.get(handle.id))?.status === "completed", {
			timeoutMs: 15_000,
			intervalMs: 25,
		});

		const steps = await engine.runs.steps(handle.id);
		expect(steps.map(s => [s.seq, s.name, s.kind, s.status])).toEqual([
			[0, "compute", "run", "completed"],
			[1, "pause", "sleep", "completed"],
			[2, "fire", "trigger", "completed"],
			[3, "await-child", "trigger_wait", "waiting"],
			[4, "approval", "token", "completed"],
		]);
		expect(steps[0]?.output).toEqual({ value: 1 });
		expect(steps[1]?.output).toBeNull();
		expect(steps[1]?.wakeAt).toBeInstanceOf(Date);
		expect(steps[2]?.output).toEqual({ id: expect.any(String) });
		// The wait step only references its waitpoint; the child's output is not copied (decision 10).
		expect(steps[3]?.output).toEqual({ waitpointId: expect.any(String) });
		expect(steps[4]?.output).toEqual({ id: expect.any(String) });
		expect(steps.every(s => s.createdAt instanceof Date)).toBe(true);

		expect(await engine.runs.steps(randomUUID())).toEqual([]);
		expect(await engine.runs.steps("nope")).toEqual([]);
	});
});
