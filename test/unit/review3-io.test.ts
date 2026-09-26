import type { Redis } from "ioredis";
import { describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createOutboxListener } from "../../src/outbox.js";
import { createStreams } from "../../src/streams.js";

/** A reader connection whose `XREAD` always rejects with `error` (LOADING, NOAUTH...). */
function rejectingReader(error: Error): Redis {
	return { xread: () => Promise.reject(error) } as unknown as Redis;
}

describe("streams.read with a signal (review 3 item 1)", () => {
	const streams = createStreams({ redis: {} as Redis, prefix: "mt_unit" });

	it("surfaces a Redis error instead of ending the stream normally", async () => {
		const error = new Error("LOADING Redis is loading the dataset in memory");
		const controller = new AbortController();
		const iterate = async () => {
			const seen: unknown[] = [];
			for await (const chunk of streams.read("run", "s", {
				redis: rejectingReader(error),
				signal: controller.signal,
			})) {
				seen.push(chunk);
			}
			return seen;
		};
		await expect(iterate()).rejects.toBe(error);
	});

	it("still ends normally when the rejection follows an abort (connection closed to stop)", async () => {
		const controller = new AbortController();
		let rejectRead: ((error: Error) => void) | undefined;
		const reader = {
			xread: () =>
				new Promise((_, reject) => {
					rejectRead = reject;
				}),
		} as unknown as Redis;
		const done = (async () => {
			for await (const _ of streams.read("run", "s", {
				redis: reader,
				signal: controller.signal,
			})) {
				// no chunks
			}
		})();
		await new Promise(r => setTimeout(r, 5));
		controller.abort();
		rejectRead?.(new Error("Connection is closed."));
		await expect(done).resolves.toBeUndefined();
	});

	it("without a signal a rejected XREAD rejects too", async () => {
		const error = new Error("NOAUTH Authentication required.");
		const iterate = async () => {
			for await (const _ of streams.read("run", "s", { redis: rejectingReader(error) })) {
				// no chunks
			}
		};
		await expect(iterate()).rejects.toBe(error);
	});
});

function recordingLogger() {
	const warns: string[] = [];
	const debugs: string[] = [];
	const logger: EngineLogger = {
		debug: (_f, m) => void debugs.push(m),
		info: () => {},
		warn: (_f, m) => void warns.push(m),
		error: () => {},
	};
	return { logger, warns, debugs };
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() > deadline) throw new Error("timed out waiting for condition");
		await new Promise(r => setTimeout(r, 5));
	}
}

describe("outbox listener (review 3 items 4 and 5)", () => {
	it("throttles connection and sweep warnings separately", async () => {
		const { logger, warns } = recordingLogger();
		const listener = createOutboxListener({
			clientConfig: { host: "127.0.0.1", port: 1 },
			channel: "mt_outbox_s_unit",
			logger,
			pollIntervalMs: 60_000,
			reconnectMinMs: 60_000,
			reconnectMaxMs: 60_000,
			dispatchBatch: async () => {
				throw new Error("claim failed");
			},
		});
		try {
			await listener.start();
			await waitFor(() => warns.some(m => m.includes("outbox sweep failed")));
			expect(warns.some(m => m.includes("could not LISTEN"))).toBe(true);
		} finally {
			await listener.close();
		}
	});

	it("passes a signal to dispatchBatch that close() aborts, and dispatches nothing after close", async () => {
		const { logger } = recordingLogger();
		const signals: AbortSignal[] = [];
		let release: (() => void) | undefined;
		let calls = 0;
		const listener = createOutboxListener({
			clientConfig: { host: "127.0.0.1", port: 1 },
			channel: "mt_outbox_s_unit",
			logger,
			pollIntervalMs: 60_000,
			reconnectMinMs: 60_000,
			reconnectMaxMs: 60_000,
			closeTimeoutMs: 50,
			dispatchBatch: async (signal?: AbortSignal) => {
				calls++;
				if (signal) signals.push(signal);
				await new Promise<void>(r => {
					release = r;
				});
				return true; // "more to claim"
			},
		});
		await listener.start();
		await waitFor(() => calls === 1);
		expect(signals[0]?.aborted).toBe(false);
		await listener.close();
		expect(signals[0]?.aborted).toBe(true);
		release?.();
		await new Promise(r => setTimeout(r, 20));
		expect(calls).toBe(1);
	});
});
