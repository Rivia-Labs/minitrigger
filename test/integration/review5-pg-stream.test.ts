/**
 * Review 5 item B5: with `transport: "postgres"`, `ctx.stream` is unavailable (decision 28(f)).
 * That is a code bug, like an invalid step argument: the run fails at once, without retry, even
 * when the task catches the error -- not a retryable error that burns every attempt.
 */
import { randomUUID } from "node:crypto";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { createEngine, type Engine } from "../../src/engine.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const engines: Engine[] = [];
const quietLogger = { debug() {}, info() {}, warn() {}, error() {} };

function makeEngine(tasks: AnyTask[]): Engine {
	const engine = createEngine({
		transport: "postgres",
		postgres: ns.databaseUrl,
		schema: ns.schema,
		tasks,
		logger: quietLogger,
		shutdownTimeoutMs: 1_000,
	});
	engines.push(engine);
	return engine;
}

beforeAll(async () => {
	const engine = createEngine({
		transport: "postgres",
		postgres: ns.databaseUrl,
		schema: ns.schema,
		tasks: [],
	});
	await engine.migrate();
	await engine.stop();
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

async function* source() {
	yield "a";
}

async function finalRun(engine: Engine, id: string) {
	let run = await engine.runs.get(id);
	await waitFor(
		async () => {
			run = await engine.runs.get(id);
			return run?.status === "failed" || run?.status === "completed";
		},
		{ timeoutMs: 10_000, intervalMs: 25 }
	);
	return run;
}

describe("review 5 B5: ctx.stream on transport postgres is fatal", () => {
	it("an uncaught ctx.stream error fails the run on its first attempt", async () => {
		const task = defineTask({
			id: `stream-${randomUUID().slice(0, 8)}`,
			retry: { maxAttempts: 3, backoff: { type: "fixed", baseMs: 10 } },
			run: async (_payload: undefined, ctx) => {
				await ctx.stream("out", source());
				return "streamed";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		const run = await finalRun(engine, handle.id);
		expect(run?.status).toBe("failed");
		expect(run?.attempt).toBe(1);
		expect(run?.error).toMatchObject({ name: "ConfigError", retryable: false });
	});

	it("a caught ctx.stream error still fails the run, without retry", async () => {
		const task = defineTask({
			id: `stream-caught-${randomUUID().slice(0, 8)}`,
			retry: { maxAttempts: 3, backoff: { type: "fixed", baseMs: 10 } },
			run: async (_payload: undefined, ctx) => {
				try {
					await ctx.stream("out", source());
				} catch {
					// ignored by the task
				}
				return "done";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();
		const handle = await engine.trigger(task);
		const run = await finalRun(engine, handle.id);
		expect(run?.status).toBe("failed");
		expect(run?.attempt).toBe(1);
		expect(run?.error?.name).toBe("ConfigError");
	});
});
