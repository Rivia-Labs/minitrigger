/**
 * Review 5 item B6: `runs.readStream` on a trigger-only engine (BullMQ), created before `stop()`
 * and first iterated after it, ends quietly -- as on `main` (0.3.x) -- instead of rejecting with
 * "this engine was stopped" because the lazily loaded backend was sealed by `stop()`.
 */
import { randomUUID } from "node:crypto";
import { beforeAll, describe, expect, it } from "vitest";
import { createEngine } from "../../src/engine.js";
import { defineTask } from "../../src/task.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
const quietLogger = { debug() {}, info() {}, warn() {}, error() {} };
const task = defineTask({ id: "rs-task", run: async () => "ok" });

function makeEngine() {
	return createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks: [task],
		logger: quietLogger,
	});
}

beforeAll(async () => {
	const engine = makeEngine();
	await engine.migrate();
	await engine.stop();
});

async function drain(iterable: AsyncIterable<unknown>): Promise<unknown[]> {
	const items: unknown[] = [];
	for await (const item of iterable) items.push(item);
	return items;
}

describe("review 5 B6: readStream iterated after stop() on a trigger-only engine", () => {
	it("backend never loaded: ends without items", async () => {
		const engine = makeEngine();
		const iterable = engine.runs.readStream(randomUUID(), "out");
		await engine.stop();
		await expect(drain(iterable)).resolves.toEqual([]);
	});

	it("backend loaded by a trigger: ends without items", async () => {
		const engine = makeEngine();
		const handle = await engine.trigger(task);
		const iterable = engine.runs.readStream(handle.id, "out");
		await engine.stop();
		await expect(drain(iterable)).resolves.toEqual([]);
	});
});
