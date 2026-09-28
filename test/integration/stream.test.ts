/** Phase 7: `ctx.stream(name, source)` + `engine.runs.readStream(runId, name)`. */
import { randomUUID } from "node:crypto";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { BullmqTransportConfig, CommonConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
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

/** A fake LLM: yields tokens with a small delay between them. */
async function* fakeLlm(tokens: readonly string[], delayMs: number): AsyncGenerator<string> {
	for (const token of tokens) {
		await sleep(delayMs);
		yield token;
	}
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

describe("ctx.stream / runs.readStream", () => {
	const tokens = ["Hello", ",", " world", "!", " How", " can", " I", " help", "?"];

	it("a live reader receives LLM-like tokens as they are produced, then ends", async () => {
		const produced: number[] = [];
		const task = defineTask({
			id: `llm-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_payload: undefined, ctx) => {
				const source = (async function* () {
					for await (const token of fakeLlm(tokens, 30)) {
						produced.push(Date.now());
						yield token;
					}
				})();
				await ctx.stream("answer", source);
				await ctx.stream(
					"meta",
					(async function* () {
						yield { model: "fake", tokens: tokens.length };
					})()
				);
				return tokens.join("");
			},
		});
		const worker = makeEngine([task]);
		await worker.start();
		const client = makeEngine([task]);
		const handle = await client.trigger(task);

		// Started before the first token exists: it blocks, then receives them live.
		const received: Array<{ token: string; at: number }> = [];
		for await (const token of client.runs.readStream<string>(handle.id, "answer")) {
			received.push({ token, at: Date.now() });
		}
		expect(received.map(r => r.token)).toEqual(tokens);
		// Live: the first token arrived well before the last one was produced.
		expect(received[0]?.at ?? 0).toBeLessThan(produced.at(-1) ?? 0);

		await waitFor(async () => (await client.runs.get(handle.id))?.status === "completed", {
			timeoutMs: 10_000,
		});
		expect((await client.runs.get(handle.id))?.output).toBe(tokens.join(""));

		// A reader after completion gets the whole stream (structured chunks too) and ends.
		const replayed: string[] = [];
		for await (const token of client.runs.readStream<string>(handle.id, "answer")) {
			replayed.push(token);
		}
		expect(replayed).toEqual(tokens);
		const meta: unknown[] = [];
		for await (const chunk of client.runs.readStream(handle.id, "meta")) meta.push(chunk);
		expect(meta).toEqual([{ model: "fake", tokens: tokens.length }]);

		// The stream key expires once ended.
		const ttl = await ns.redis.ttl(`${ns.prefix}:stream:${handle.id}:answer`);
		expect(ttl).toBeGreaterThan(0);
	});

	it("a reader stops on AbortSignal or break", async () => {
		const client = makeEngine([]);
		const runId = randomUUID();
		// Nobody writes this stream: the reader would block forever.
		const controller = new AbortController();
		const reading = (async () => {
			const chunks: unknown[] = [];
			for await (const chunk of client.runs.readStream(runId, "nothing", {
				signal: controller.signal,
			})) {
				chunks.push(chunk);
			}
			return chunks;
		})();
		await sleep(100);
		controller.abort();
		expect(await reading).toEqual([]);

		// Break after the first chunk of a stream that is still open.
		await ns.redis.xadd(`${ns.prefix}:stream:${runId}:open`, "*", "data", JSON.stringify("a"));
		for await (const chunk of client.runs.readStream(runId, "open")) {
			expect(chunk).toBe("a");
			break;
		}
	});

	it("a reader without a signal ends cleanly when engine.stop() disconnects it", async () => {
		const client = makeEngine([]);
		const runId = randomUUID();
		await ns.redis.xadd(`${ns.prefix}:stream:${runId}:open`, "*", "data", JSON.stringify("a"));
		const chunks: unknown[] = [];
		const reading = (async () => {
			for await (const chunk of client.runs.readStream(runId, "open")) chunks.push(chunk);
		})();
		await waitFor(() => chunks.length === 1, { timeoutMs: 5_000 });
		// The reader is now blocked in XREAD BLOCK; stop() disconnects its connection.
		await client.stop();
		await expect(reading).resolves.toBeUndefined();
		expect(chunks).toEqual(["a"]);
	});

	it("a stream that ended with an error still throws to the reader", async () => {
		const client = makeEngine([]);
		const runId = randomUUID();
		const key = `${ns.prefix}:stream:${runId}:bad`;
		await ns.redis.xadd(key, "*", "data", JSON.stringify("a"));
		await ns.redis.xadd(key, "*", "end", "1", "error", "Error: boom");
		const chunks: unknown[] = [];
		await expect(
			(async () => {
				for await (const chunk of client.runs.readStream(runId, "bad")) chunks.push(chunk);
			})()
		).rejects.toThrow(/ended with an error: Error: boom/);
		expect(chunks).toEqual(["a"]);
	});
});
