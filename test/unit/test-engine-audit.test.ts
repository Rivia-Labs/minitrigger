/**
 * Audit follow-ups on the test engine: `retention.days` honored like the real engine (low d),
 * stream end markers with errors (parity with `src/streams.ts`), and `runs.*` argument checks
 * shared with the real engine (low a).
 */
import { afterEach, describe, expect, it } from "vitest";
import {
	ConfigError,
	createTestEngine,
	defineTask,
	type EngineLogger,
	MinitriggerError,
	type TestEngine,
	type TestEngineConfig,
} from "../../src/index.js";
import { createMemoryStreams } from "../../src/testing/memory-pubsub.js";

const quietLogger: EngineLogger = {
	debug: () => {},
	info: () => {},
	warn: () => {},
	error: () => {},
};

const engines: TestEngine[] = [];
function testEngine(config: TestEngineConfig): TestEngine {
	const engine = createTestEngine({ logger: quietLogger, ...config });
	engines.push(engine);
	return engine;
}

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

const echo = defineTask({ id: "echo", run: async (p: { v: number }) => p.v });

describe("retention.days (low d)", () => {
	it("accepts a TTL above 30 days when retention.days allows it, like the real engine", async () => {
		const engine = testEngine({
			tasks: [echo],
			retention: { days: 90 },
			idempotencyKeyTtl: "60d",
		});
		const handle = await engine.trigger(
			echo,
			{ v: 1 },
			{ idempotencyKey: "k", idempotencyKeyTtl: "80d" }
		);
		expect(handle.id).toBeTruthy();
	});

	it("still rejects a TTL above the configured retention.days", async () => {
		expect(() =>
			testEngine({ tasks: [echo], retention: { days: 10 }, idempotencyKeyTtl: "20d" })
		).toThrow(ConfigError);
		const engine = testEngine({ tasks: [echo], retention: { days: 10 } });
		await expect(
			engine.trigger(echo, { v: 1 }, { idempotencyKey: "k", idempotencyKeyTtl: "11d" })
		).rejects.toBeInstanceOf(ConfigError);
	});

	it("keeps the 30-day default when retention is omitted", () => {
		expect(() => testEngine({ tasks: [echo], idempotencyKeyTtl: "31d" })).toThrow(ConfigError);
	});
});

describe("memory streams: end marker with an error (parity with src/streams.ts)", () => {
	it("a source that throws ends the stream with an error; readers throw after the earlier chunks", async () => {
		const streams = createMemoryStreams();
		async function* source() {
			yield "a";
			throw new TypeError("boom");
		}
		await expect(streams.pipe("r", "s", source())).rejects.toThrow("boom");
		const chunks: unknown[] = [];
		const read = async () => {
			for await (const chunk of streams.read("r", "s", undefined)) chunks.push(chunk);
		};
		await expect(read()).rejects.toThrow(MinitriggerError);
		await expect(read()).rejects.toThrow(/TypeError: boom/);
		expect(chunks[0]).toBe("a");
	});

	it("an aborted pipe stops pulling from the source and ends with an error", async () => {
		const streams = createMemoryStreams();
		const controller = new AbortController();
		async function* hanging() {
			yield 1;
			yield 2;
			await new Promise(() => {}); // a source that ignores the abort
		}
		setTimeout(() => controller.abort(new Error("run finished")), 20);
		await expect(
			streams.pipe("r", "abort", hanging(), { signal: controller.signal })
		).rejects.toThrow("run finished");
		const chunks: unknown[] = [];
		await expect(
			(async () => {
				for await (const chunk of streams.read("r", "abort", undefined)) chunks.push(chunk);
			})()
		).rejects.toThrow(/run finished/);
		expect(chunks).toEqual([1, 2]);
	});

	it("runs.readStream of the test engine throws when the task's stream failed", async () => {
		const streamer = defineTask({
			id: "failing-streamer",
			retry: { maxAttempts: 1 },
			run: async (_p: null, ctx) => {
				async function* tokens() {
					yield "x";
					throw new Error("llm down");
				}
				await ctx.stream("out", tokens());
				return null;
			},
		});
		const engine = testEngine({ tasks: [streamer] });
		const result = await engine.runInline(streamer, null);
		expect(result.status).toBe("failed");
		const chunks: unknown[] = [];
		await expect(
			(async () => {
				for await (const chunk of engine.runs.readStream(result.runId, "out")) chunks.push(chunk);
			})()
		).rejects.toThrow(/llm down/);
		expect(chunks).toEqual(["x"]);
	});
});

describe("runs.* argument checks (low a)", () => {
	it("non-UUID ids and invalid limits/afterIds behave like the real engine", async () => {
		const engine = testEngine({ tasks: [echo] });
		expect(await engine.runs.get("nope")).toBeNull();
		expect(await engine.runs.cancel("nope")).toBe("not_found");
		await expect(engine.runs.replay("nope")).rejects.toThrow(/not found/);
		await expect(engine.runs.list({ limit: Number.NaN })).rejects.toBeInstanceOf(ConfigError);
		await expect(
			engine.runs.logs("00000000-0000-4000-8000-000000000000", { afterId: "x" })
		).rejects.toBeInstanceOf(ConfigError);
	});
});
