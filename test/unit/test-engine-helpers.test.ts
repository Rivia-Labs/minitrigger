/**
 * `assertTriggered`/`refuteTriggered`/`allTriggered` (testing subpath), inspired by
 * `@rivia-labs/oban-ts`'s `assertEnqueued`/`refuteEnqueued`/`allEnqueued`. Built on
 * `TestEngine#runs.list`, so a child triggered via `ctx.step.trigger*` is matched exactly like a
 * top-level `engine.trigger()`.
 */
import { afterEach, describe, expect, it } from "vitest";
import { defineTask, type EngineLogger } from "../../src/index.js";
import type { AnyTask } from "../../src/task.js";
import {
	AssertionError,
	allTriggered,
	assertTriggered,
	createTestEngine,
	refuteTriggered,
	type TestEngine,
} from "../../src/testing/index.js";

const quietLogger: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const engines: TestEngine[] = [];
function testEngine(...tasks: readonly AnyTask[]): TestEngine {
	const engine = createTestEngine({ logger: quietLogger, tasks });
	engines.push(engine);
	return engine;
}

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

const sendReceipt = defineTask({
	id: "send-receipt",
	queue: { name: "mailers", concurrencyKey: { limit: 2 } },
	run: async (_payload: { orderId: number }) => "sent" as const,
});

const notifyWarehouse = defineTask({
	id: "notify-warehouse",
	run: async (_payload: { orderId: number; items: string[] }) => "notified" as const,
});

const placeOrder = defineTask({
	id: "place-order",
	run: async (payload: { orderId: number }, ctx) => {
		await ctx.step.trigger("notify", notifyWarehouse, { orderId: payload.orderId, items: ["a"] });
		return "placed" as const;
	},
});

describe("allTriggered", () => {
	it("is empty before anything is triggered", async () => {
		const engine = testEngine(sendReceipt);
		expect(await allTriggered(engine)).toEqual([]);
	});

	it("lists triggered runs, matching task by object or id", async () => {
		const engine = testEngine(sendReceipt);
		await engine.trigger(sendReceipt, { orderId: 1 });
		await engine.trigger(sendReceipt, { orderId: 2 });

		const byObject = await allTriggered(engine, { task: sendReceipt });
		expect(byObject).toHaveLength(2);
		const byId = await allTriggered(engine, { task: "send-receipt" });
		expect(byId).toHaveLength(2);
		expect(byId.map(r => r.payload)).toEqual(
			expect.arrayContaining([{ orderId: 1 }, { orderId: 2 }])
		);
	});

	// Item 3: `runs.list` itself is newest-first (`ORDER BY created_at DESC, id DESC`), but
	// `allTriggered`/`assertTriggered` are documented (and more natural for a test to read) as
	// oldest-first -- the order triggers actually happened in.
	it("returns oldest-first, regardless of runs.list's own newest-first order", async () => {
		const engine = testEngine(sendReceipt);
		const first = await engine.trigger(sendReceipt, { orderId: 1 });
		await engine.clock.advance(1_000);
		const second = await engine.trigger(sendReceipt, { orderId: 2 });
		await engine.clock.advance(1_000);
		const third = await engine.trigger(sendReceipt, { orderId: 3 });

		const found = await allTriggered(engine, { task: sendReceipt });
		expect(found.map(r => r.runId)).toEqual([first.id, second.id, third.id]);
	});

	it("matches payload as a deep partial (object) but exact (array)", async () => {
		const engine = testEngine(notifyWarehouse);
		await engine.trigger(notifyWarehouse, { orderId: 1, items: ["a", "b"] });

		expect(await allTriggered(engine, { payload: { orderId: 1 } })).toHaveLength(1);
		expect(await allTriggered(engine, { payload: { items: ["a", "b"] } })).toHaveLength(1);
		// Extra key on the match that the payload doesn't have: no match.
		expect(await allTriggered(engine, { payload: { orderId: 1, missing: true } })).toHaveLength(0);
		// Arrays must match exactly, not as a subset.
		expect(await allTriggered(engine, { payload: { items: ["a"] } })).toHaveLength(0);
	});

	it("matches by status", async () => {
		const engine = testEngine(sendReceipt);
		const { id } = await engine.trigger(sendReceipt, { orderId: 1 });
		expect(await allTriggered(engine, { status: "queued" })).toHaveLength(1);
		await engine.drive(id);
		expect(await allTriggered(engine, { status: "queued" })).toHaveLength(0);
		expect(await allTriggered(engine, { status: ["completed", "failed"] })).toHaveLength(1);
	});

	it("matches by concurrencyKey", async () => {
		const engine = testEngine(sendReceipt);
		await engine.trigger(sendReceipt, { orderId: 1 }, { concurrencyKey: "cust-1" });
		await engine.trigger(sendReceipt, { orderId: 2 }, { concurrencyKey: "cust-2" });
		expect(await allTriggered(engine, { concurrencyKey: "cust-1" })).toHaveLength(1);
		expect((await allTriggered(engine, { concurrencyKey: "cust-1" }))[0]?.payload).toEqual({
			orderId: 1,
		});
	});

	it("sees a child run triggered via ctx.step.trigger*", async () => {
		const engine = testEngine(placeOrder, notifyWarehouse);
		const { id } = await engine.trigger(placeOrder, { orderId: 7 });
		await engine.drive(id);

		const children = await allTriggered(engine, { task: notifyWarehouse });
		expect(children).toHaveLength(1);
		expect(children[0]).toMatchObject({
			taskId: "notify-warehouse",
			payload: { orderId: 7, items: ["a"] },
		});
		expect(children[0]?.parentRunId).toBe(id);
	});
});

describe("assertTriggered / refuteTriggered", () => {
	it("assertTriggered resolves with the match and lets further assertions chain off it", async () => {
		const engine = testEngine(sendReceipt);
		await engine.trigger(sendReceipt, { orderId: 1 });
		const found = await assertTriggered(engine, { task: sendReceipt, payload: { orderId: 1 } });
		expect(found.taskId).toBe("send-receipt");
	});

	it("assertTriggered throws AssertionError listing what WAS triggered when nothing matches", async () => {
		const engine = testEngine(sendReceipt);
		await engine.trigger(sendReceipt, { orderId: 1 });
		await expect(
			assertTriggered(engine, { task: sendReceipt, payload: { orderId: 2 } })
		).rejects.toThrow(AssertionError);
		await expect(
			assertTriggered(engine, { task: sendReceipt, payload: { orderId: 2 } })
		).rejects.toThrow(/orderId["\s:]*1/);
	});

	it("assertTriggered reports when nothing at all was triggered for the task", async () => {
		const engine = testEngine(sendReceipt);
		await expect(assertTriggered(engine, { task: sendReceipt })).rejects.toThrow(
			/No runs were triggered for task "send-receipt"/
		);
	});

	it("refuteTriggered passes when nothing matches, throws when something does", async () => {
		const engine = testEngine(sendReceipt);
		await refuteTriggered(engine, { task: sendReceipt });
		await engine.trigger(sendReceipt, { orderId: 1 });
		await refuteTriggered(engine, { task: sendReceipt, payload: { orderId: 2 } });
		await expect(refuteTriggered(engine, { task: sendReceipt })).rejects.toThrow(AssertionError);
	});

	it("truncates a large payload instead of dumping it into the failure message", async () => {
		const bigPayload = defineTask({
			id: "big-payload",
			run: async (_payload: { blob: string }) => null,
		});
		const engine = testEngine(bigPayload);
		await engine.trigger(bigPayload, { blob: "x".repeat(5_000) });
		let message = "";
		try {
			await assertTriggered(engine, { task: bigPayload, payload: { blob: "y" } });
		} catch (error) {
			message = (error as Error).message;
		}
		expect(message.length).toBeLessThan(1_000);
		expect(message).toContain("…");
	});
});
