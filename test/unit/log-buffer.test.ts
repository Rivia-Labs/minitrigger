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
