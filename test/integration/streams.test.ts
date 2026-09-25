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

	it("sets an EXPIRE on the stream key only after end()", async () => {
		const ttlSeconds = 123;
		const streams = createStreams({ redis: ns.redis, prefix: ns.prefix, ttlSeconds });
		const w = streams.writer("run-3", "out");
		await w.append("x");

		const key = `${ns.prefix}:stream:run-3:out`;
		expect(await ns.redis.ttl(key)).toBe(-1); // key exists, no TTL yet

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
});
