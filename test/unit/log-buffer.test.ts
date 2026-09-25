import { describe, expect, it, vi } from "vitest";
import { createLogBuffer } from "../../src/logs/buffer.js";
import type { NewLogEntry } from "../../src/store/types.js";

function fakeInsertBatch() {
	const calls: NewLogEntry[][] = [];
	const insertBatch = vi.fn(async (entries: NewLogEntry[]) => {
		calls.push(entries);
	});
	return { insertBatch, calls };
}

describe("createLogBuffer", () => {
	it("batches 10k lines into at most ceil(n/maxBuffered)+1 insertBatch calls, preserving order", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		const total = 10_000;
		for (let i = 0; i < total; i++) {
			logger.info(`line-${i}`);
			// Yield to the microtask queue periodically so scheduled flushes actually run and drain
			// close to `maxBuffered` (50) at a time -- exercising real batching, not just one giant
			// call that happens to also satisfy the bound below.
			if (i % 25 === 24) await Promise.resolve();
		}
		await logger.close();

		expect(insertBatch).toHaveBeenCalled();
		expect(calls.length).toBeLessThanOrEqual(Math.ceil(total / 50) + 1);

		const messages = calls.flat().map(entry => entry.message);
		expect(messages).toEqual(Array.from({ length: total }, (_, i) => `line-${i}`));
	});

	it("filters entries below minLevel from both buffering and the mirror", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const mirrorLevels: string[] = [];
		const mirror = {
			debug: () => mirrorLevels.push("debug"),
			info: () => mirrorLevels.push("info"),
			warn: () => mirrorLevels.push("warn"),
			error: () => mirrorLevels.push("error"),
		};
		const logger = createLogBuffer({
			runId: "run-1",
			insertBatch,
			maxLogEntryBytes: 1024,
			minLevel: "warn",
			mirror,
		});

		logger.debug("d");
		logger.info("i");
		logger.warn("w");
		logger.error("e");
		await logger.close();

		expect(mirrorLevels).toEqual(["warn", "error"]);
		expect(calls.flat().map(entry => entry.message)).toEqual(["w", "e"]);
	});

	it("passes { runId, ...data } to the mirror", async () => {
		const { insertBatch } = fakeInsertBatch();
		const seen: Array<{ fields: Record<string, unknown>; message: string }> = [];
		const mirror = {
			debug: () => {},
			info: (fields: Record<string, unknown>, message: string) => seen.push({ fields, message }),
			warn: () => {},
			error: () => {},
		};
		const logger = createLogBuffer({ runId: "run-9", insertBatch, maxLogEntryBytes: 1024, mirror });

		logger.info("hi", { a: 1 });
		await logger.close();

		expect(seen).toEqual([{ fields: { runId: "run-9", a: 1 }, message: "hi" }]);
	});

	it("truncates message and data to maxLogEntryBytes before buffering", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const maxLogEntryBytes = 40;
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes });

		const longMessage = "x".repeat(500);
		logger.info(longMessage, { blob: "y".repeat(500) });
		await logger.close();

		const [entry] = calls.flat();
		expect(entry).toBeDefined();
		expect(entry?.message).not.toBe(longMessage);
		expect(Buffer.byteLength(entry?.message ?? "", "utf8")).toBeLessThanOrEqual(maxLogEntryBytes);
		expect(entry?.data).toMatchObject({ __truncated: true });
	});

	it("flushes on the timer even when below maxBuffered", async () => {
		vi.useFakeTimers();
		try {
			const { insertBatch, calls } = fakeInsertBatch();
			const logger = createLogBuffer({
				runId: "run-1",
				insertBatch,
				maxLogEntryBytes: 1024,
				flushIntervalMs: 500,
			});

			logger.info("only one");
			expect(insertBatch).not.toHaveBeenCalled();

			await vi.advanceTimersByTimeAsync(500);

			expect(insertBatch).toHaveBeenCalledTimes(1);
			expect(calls[0]?.map(entry => entry.message)).toEqual(["only one"]);

			await logger.close();
		} finally {
			vi.useRealTimers();
		}
	});

	it("close() flushes remaining entries and disables further logging", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		logger.info("a");
		logger.info("b");
		expect(insertBatch).not.toHaveBeenCalled();

		await logger.close();
		expect(insertBatch).toHaveBeenCalledTimes(1);
		expect(calls[0]?.map(entry => entry.message)).toEqual(["a", "b"]);

		logger.info("after close, should be dropped");
		await logger.close(); // idempotent
		expect(insertBatch).toHaveBeenCalledTimes(1);
	});

	it("retries a failed flush once, then drops the batch and calls onFlushError", async () => {
		let attempts = 0;
		const insertBatch = vi.fn(async () => {
			attempts++;
			throw new Error("boom");
		});
		const onFlushError = vi.fn();
		const logger = createLogBuffer({
			runId: "run-1",
			insertBatch,
			maxLogEntryBytes: 1024,
			onFlushError,
		});

		logger.error("oops");
		await logger.close();

		expect(attempts).toBe(2);
		expect(onFlushError).toHaveBeenCalledTimes(1);
		const call = onFlushError.mock.calls[0];
		expect(call).toBeDefined();
		const [error, entries] = call as [unknown, NewLogEntry[]];
		expect((error as Error).message).toBe("boom");
		expect(entries.map(entry => entry.message)).toEqual(["oops"]);
	});

	it("never throws for a circular reference in data (dropped-in for [Circular])", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		const circular: Record<string, unknown> = { a: 1 };
		circular.self = circular;
		expect(() => logger.info("has cycle", circular as never)).not.toThrow();
		await logger.close();

		expect(insertBatch).toHaveBeenCalledTimes(1);
		const [entry] = calls.flat();
		expect(entry?.data).toEqual({ a: 1, self: "[Circular]" });
		// The stored data must round-trip through JSON (i.e. actually be storable as jsonb).
		expect(() => JSON.stringify(entry?.data)).not.toThrow();
	});

	it("never throws for a bigint in data (converted to its decimal string)", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		expect(() => logger.info("has bigint", { n: 10n } as never)).not.toThrow();
		await logger.close();

		const [entry] = calls.flat();
		expect(entry?.data).toEqual({ n: "10" });
	});

	it("drops function/symbol values from data instead of throwing", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		expect(() =>
			logger.info("has fn", { keep: 1, fn: () => 1, sym: Symbol("x") } as never)
		).not.toThrow();
		await logger.close();

		const [entry] = calls.flat();
		expect(entry?.data).toEqual({ keep: 1 });
	});

	it("a bare (top-level) circular reference or bigint never throws either", async () => {
		const { insertBatch } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		const circular: Record<string, unknown> = {};
		circular.self = circular;
		expect(() => logger.info("bare cycle", circular as never)).not.toThrow();
		expect(() => logger.info("bare bigint", 10n as never)).not.toThrow();
		await logger.close();
	});

	it("coerces a non-string message instead of throwing", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		expect(() => logger.info(42 as unknown as string)).not.toThrow();
		expect(() => logger.info({ oops: true } as unknown as string)).not.toThrow();
		await logger.close();

		const messages = calls.flat().map(entry => entry.message);
		expect(messages).toEqual(["42", "an object"]);
	});

	it("sanitizes a NUL byte / lone surrogate in the message so the whole batch doesn't fail", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		logger.info("a\u0000b\uD800c");
		await logger.close();

		const [entry] = calls.flat();
		expect(entry?.message.includes("\u0000")).toBe(false);
		expect(entry?.message.isWellFormed()).toBe(true);
	});

	it("sanitizes a NUL byte / lone surrogate in data (string leaves and keys)", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({ runId: "run-1", insertBatch, maxLogEntryBytes: 1024 });

		logger.info("msg", { "bad\u0000key": "val\uD800ue" });
		await logger.close();

		const [entry] = calls.flat();
		const data = entry?.data as Record<string, string>;
		const [key] = Object.keys(data);
		expect(key).toBeDefined();
		expect(key?.includes("\u0000")).toBe(false);
		const value = key !== undefined ? data[key] : undefined;
		expect(value?.isWellFormed()).toBe(true);
		expect(() => JSON.stringify(entry?.data)).not.toThrow();
	});

	it("flushes a single batch as soon as maxBuffered is reached", async () => {
		const { insertBatch, calls } = fakeInsertBatch();
		const logger = createLogBuffer({
			runId: "run-1",
			insertBatch,
			maxLogEntryBytes: 1024,
			maxBuffered: 3,
		});

		logger.info("1");
		logger.info("2");
		logger.info("3");
		await vi.waitFor(() => expect(insertBatch).toHaveBeenCalledTimes(1));

		expect(calls[0]?.map(entry => entry.message)).toEqual(["1", "2", "3"]);
		await logger.close();
	});
});
