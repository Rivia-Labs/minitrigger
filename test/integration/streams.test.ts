import type { Redis } from "ioredis";
import { afterEach, describe, expect, it } from "vitest";
import { createStreams } from "../../src/streams.js";
import type { Json } from "../../src/utils/serialization.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();

const readers: Redis[] = [];

/** A connection DEDICATED to one `XREAD BLOCK` read, never the namespace's shared `ns.redis`. */
function dedicatedReader(): Redis {
	const reader = ns.redis.duplicate({ maxRetriesPerRequest: null });
	readers.push(reader);
	return reader;
}

afterEach(() => {
	while (readers.length > 0) {
		readers.pop()?.disconnect();
	}
});

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

describe("createStreams()", () => {
	it("reads back every chunk a writer appended, in order, ending at the end marker", async () => {
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix });
		const w = streams.writer("run-1", "stdout");
		const chunks: (string | Json)[] = ["hello", "world", { n: 1 }, 42, true, null];
		for (const chunk of chunks) {
			await w.append(chunk);
		}
		await w.end();

		const reader = dedicatedReader();
		const received: (string | Json)[] = [];
		for await (const chunk of streams.read("run-1", "stdout", { redis: reader, blockMs: 200 })) {
			received.push(chunk);
		}

		expect(received).toEqual(chunks);
	});

	it("a concurrent reader receives live chunks and then observes the end", async () => {
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix });
		const reader = dedicatedReader();

		const received: (string | Json)[] = [];
		const readDone = (async () => {
			for await (const chunk of streams.read("run-2", "log", { redis: reader, blockMs: 1_000 })) {
				received.push(chunk);
			}
		})();

		// Best-effort: let the reader issue its first BLOCK before the writer starts, to exercise
		// the "live" delivery path. Not load-bearing for correctness -- XREAD from id 0 would pick
		// up everything either way.
		await delay(50);

		const w = streams.writer("run-2", "log");
		await w.append("first");
		await delay(50);
		await w.append("second");
		await w.end();

		await readDone;
		expect(received).toEqual(["first", "second"]);
	});

	it("keeps an EXPIRE on the stream key from the first append (a dead writer's key expires)", async () => {
		const ttlSeconds = 123;
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix, ttlSeconds });
		const w = streams.writer("run-3", "out");
		await w.append("x");

		const key = `${ns.prefix}:stream:run-3:out`;
		const beforeEnd = await ns.redis.ttl(key);
		expect(beforeEnd).toBeGreaterThan(0);
		expect(beforeEnd).toBeLessThanOrEqual(ttlSeconds);

		await w.end();
		const ttl = await ns.redis.ttl(key);
		expect(ttl).toBeGreaterThan(0);
		expect(ttl).toBeLessThanOrEqual(ttlSeconds);
	});

	it("aborting stops the reader without waiting for the full block window", async () => {
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix });
		const reader = dedicatedReader();
		const controller = new AbortController();

		const readAll = (async () => {
			const received: (string | Json)[] = [];
			for await (const chunk of streams.read("run-4", "out", {
				redis: reader,
				blockMs: 10_000,
				signal: controller.signal,
			})) {
				received.push(chunk);
			}
			return received;
		})();

		await delay(50);
		const start = Date.now();
		controller.abort();

		const received = await readAll;
		const elapsed = Date.now() - start;

		expect(received).toEqual([]);
		expect(elapsed).toBeLessThan(2_000); // well under the 10s block window
	});

	it("pipe() appends every item of an async source in order, then ends the stream", async () => {
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix });

		async function* source(): AsyncGenerator<string> {
			yield "a";
			yield "b";
			yield "c";
		}

		await streams.pipe("run-5", "piped", source());

		const reader = dedicatedReader();
		const received: (string | Json)[] = [];
		for await (const chunk of streams.read("run-5", "piped", { redis: reader, blockMs: 200 })) {
			received.push(chunk);
		}

		expect(received).toEqual(["a", "b", "c"]);
	});

	it("pipe() still ends the stream (with an error) and sets the TTL when the source throws", async () => {
		const ttlSeconds = 77;
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix, ttlSeconds });

		async function* source(): AsyncGenerator<string> {
			yield "a";
			throw new Error("upstream broke");
		}

		await expect(streams.pipe("run-6", "err", source())).rejects.toThrow("upstream broke");

		const key = `${ns.prefix}:stream:run-6:err`;
		const ttl = await ns.redis.ttl(key);
		expect(ttl).toBeGreaterThan(0);
		expect(ttl).toBeLessThanOrEqual(ttlSeconds);

		// The reader gets the chunks before the failure, then surfaces the error (no blocking).
		const reader = dedicatedReader();
		const received: (string | Json)[] = [];
		await expect(
			(async () => {
				for await (const chunk of streams.read("run-6", "err", { redis: reader, blockMs: 200 })) {
					received.push(chunk);
				}
			})()
		).rejects.toThrow(/stream "err" of run run-6 ended with an error: Error: upstream broke/);
		expect(received).toEqual(["a"]);

		// Wire format: the error rides on the usual end marker, so older readers just see an end.
		const entries = await ns.redis.xrange(key, "-", "+");
		expect(entries.at(-1)?.[1]).toEqual(["end", "1", "error", "Error: upstream broke"]);
	});

	it("pipe() stops a source that ignores the abort and ends the stream when the signal fires", async () => {
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix });
		let returned = false;
		const hanging: AsyncIterable<string> = {
			[Symbol.asyncIterator]() {
				let sent = false;
				return {
					next: () => {
						if (!sent) {
							sent = true;
							return Promise.resolve({ done: false, value: "first" });
						}
						return new Promise<IteratorResult<string>>(() => {}); // never settles
					},
					return: () => {
						returned = true;
						return Promise.resolve({ done: true, value: undefined });
					},
				};
			},
		};
		const controller = new AbortController();
		const piping = streams.pipe("run-7", "hang", hanging, { signal: controller.signal });
		await delay(50);
		controller.abort(new Error("run canceled"));
		await expect(piping).rejects.toThrow("run canceled");
		await delay(10);
		expect(returned).toBe(true);

		const reader = dedicatedReader();
		const received: (string | Json)[] = [];
		await expect(
			(async () => {
				for await (const chunk of streams.read("run-7", "hang", { redis: reader, blockMs: 200 })) {
					received.push(chunk);
				}
			})()
		).rejects.toThrow(/ended with an error: Error: run canceled/);
		expect(received).toEqual(["first"]);
		expect(await ns.redis.ttl(`${ns.prefix}:stream:run-7:hang`)).toBeGreaterThan(0);
	});

	it("an end marker without an error field ends the read normally (older writers)", async () => {
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix });
		const key = `${ns.prefix}:stream:run-8:old`;
		await ns.redis.xadd(key, "*", "data", JSON.stringify("x"));
		await ns.redis.xadd(key, "*", "end", "1");
		const received: (string | Json)[] = [];
		for await (const chunk of streams.read("run-8", "old", {
			redis: dedicatedReader(),
			blockMs: 200,
		})) {
			received.push(chunk);
		}
		expect(received).toEqual(["x"]);
	});
});
