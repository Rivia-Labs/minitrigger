import { describe, expect, it, vi } from "vitest";
import { createRunProgress, type RunProgressContext } from "../../src/executor/progress.js";
import type { SetProgressResult } from "../../src/store/types.js";
import type { Json } from "../../src/utils/serialization.js";

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const quietLogger = { debug() {}, info() {}, warn() {}, error() {} };

function fakeSetProgress(script: readonly ("ok" | "lost" | Error)[] = []) {
	const calls: Json[] = [];
	let updatedAtCounter = 0;
	const queue = [...script];
	const setProgress = vi.fn(
		async (_runId: string, _executionId: string, value: Json): Promise<SetProgressResult> => {
			calls.push(value);
			const next = queue.shift() ?? "ok";
			if (next instanceof Error) throw next;
			if (next === "lost") return { kind: "lost" };
			updatedAtCounter++;
			return { kind: "ok", updatedAt: new Date(updatedAtCounter) };
		}
	);
	return { setProgress, calls };
}

function warnCollector() {
	const messages: string[] = [];
	const logger = {
		...quietLogger,
		warn: (_fields: Record<string, unknown>, message: string) => {
			messages.push(message);
		},
	};
	return { logger, messages };
}

function ctx(overrides: Partial<RunProgressContext> = {}): RunProgressContext {
	return {
		runId: "r1",
		executionId: "e1",
		taskId: "t1",
		ownershipGone: () => false,
		...overrides,
	};
}

describe("createRunProgress", () => {
	it("writes the first value promptly (leading), coalescing sets that arrive during the window", async () => {
		const { setProgress, calls } = fakeSetProgress();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger: quietLogger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{ intervalMs: 40 }
		);

		writer.set({ i: 0 });
		await sleep(5);
		// The leading write fired right away, without waiting for the window.
		expect(calls).toEqual([{ i: 0 }]);

		// Both arrive well before the trailing write is due: only the LATEST is ever written.
		writer.set({ i: 1 });
		writer.set({ i: 2 });
		await sleep(60);
		expect(calls).toEqual([{ i: 0 }, { i: 2 }]);

		await writer.close();
		expect(calls).toEqual([{ i: 0 }, { i: 2 }]); // close() after everything settled writes nothing more.
	});

	it("close() flushes a pending value instead of waiting for the trailing timer", async () => {
		const { setProgress, calls } = fakeSetProgress();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger: quietLogger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{ intervalMs: 10_000 }
		);

		writer.set({ a: 1 });
		await sleep(5); // let the leading write land
		writer.set({ a: 2 }); // would otherwise sit pending for ~10s

		await writer.close();
		expect(calls).toEqual([{ a: 1 }, { a: 2 }]);
	});

	it("close() is idempotent", async () => {
		const { setProgress, calls } = fakeSetProgress();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger: quietLogger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{ intervalMs: 10 }
		);
		writer.set({ a: 1 });
		await writer.close();
		await writer.close();
		await writer.close();
		expect(calls).toEqual([{ a: 1 }]);
	});

	it("a set() after close() is a no-op", async () => {
		const { setProgress, calls } = fakeSetProgress();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger: quietLogger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{ intervalMs: 10 }
		);
		await writer.close();
		writer.set({ a: 1 });
		await sleep(20);
		expect(calls).toEqual([]);
	});

	it("stops writing and warns once when ownership is gone", async () => {
		const { setProgress, calls } = fakeSetProgress();
		const { logger, messages } = warnCollector();
		let gone = false;
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger, heartbeatTimeoutMs: 5_000 } },
			ctx({ ownershipGone: () => gone }),
			{ intervalMs: 10 }
		);

		writer.set({ a: 1 });
		await sleep(15);
		expect(calls).toEqual([{ a: 1 }]);

		gone = true;
		writer.set({ a: 2 });
		writer.set({ a: 3 });
		await sleep(20);
		expect(calls).toEqual([{ a: 1 }]);
		expect(messages).toHaveLength(1);

		await writer.close();
		expect(calls).toEqual([{ a: 1 }]);
	});

	it("stops writing and warns once when the store reports the run is no longer owned", async () => {
		const { setProgress, calls } = fakeSetProgress(["lost"]);
		const { logger, messages } = warnCollector();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{
				intervalMs: 10,
			}
		);

		writer.set({ a: 1 });
		await sleep(15);
		expect(calls).toEqual([{ a: 1 }]);
		expect(messages).toHaveLength(1);

		writer.set({ a: 2 });
		await sleep(20);
		expect(calls).toEqual([{ a: 1 }]); // no further attempts
		expect(messages).toHaveLength(1); // still only one warning
	});

	it("retries a failed write once; a transient failure is recovered", async () => {
		const { setProgress, calls } = fakeSetProgress([new Error("transient")]);
		const { logger, messages } = warnCollector();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{
				intervalMs: 10,
			}
		);

		writer.set({ a: 1 });
		await sleep(15);
		// Original attempt + one retry, both for the same value; the retry succeeded.
		expect(calls).toEqual([{ a: 1 }, { a: 1 }]);
		expect(messages).toHaveLength(0);

		writer.set({ a: 2 });
		await sleep(20);
		expect(calls).toEqual([{ a: 1 }, { a: 1 }, { a: 2 }]);
	});

	it("drops only that value and warns when a write fails twice in a row, but keeps writing", async () => {
		const { setProgress, calls } = fakeSetProgress([new Error("boom"), new Error("boom again")]);
		const { logger, messages } = warnCollector();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{
				intervalMs: 10,
			}
		);

		writer.set({ a: 1 });
		await sleep(15);
		expect(calls).toHaveLength(2); // original + one retry, both failed; {a:1} is dropped
		expect(messages).toHaveLength(1);

		// A transient double failure must not disable the writer for the rest of the execution
		// (only losing ownership does): the next value still gets written normally.
		writer.set({ a: 2 });
		await sleep(20);
		expect(calls).toHaveLength(3);
		expect(calls[2]).toEqual({ a: 2 });
		expect(messages).toHaveLength(1); // no new warning: this write succeeded

		await writer.close();
		expect(calls).toHaveLength(3);
	});

	it("failure then later set + close: the last value is still written", async () => {
		const { setProgress, calls } = fakeSetProgress([new Error("boom"), new Error("boom again")]);
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger: quietLogger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{ intervalMs: 10_000 }
		);

		writer.set({ a: 1 });
		await sleep(15); // both attempts fail; {a:1} is dropped, but the writer keeps running
		expect(calls).toHaveLength(2);

		writer.set({ a: 2 }); // pending, well inside the (huge) coalescing window
		await writer.close();
		expect(calls).toHaveLength(3);
		expect(calls[2]).toEqual({ a: 2 });
	});

	it("notifies a successful write with the value and the store's updatedAt", async () => {
		const { setProgress } = fakeSetProgress();
		const notifyProgress = vi.fn();
		const writer = createRunProgress(
			{
				runs: { setProgress },
				config: { logger: quietLogger, heartbeatTimeoutMs: 5_000 },
				notify: { progress: notifyProgress },
			},
			ctx(),
			{ intervalMs: 10 }
		);

		writer.set({ step: 1 });
		await sleep(15);
		expect(notifyProgress).toHaveBeenCalledTimes(1);
		expect(notifyProgress).toHaveBeenCalledWith("r1", expect.any(Date), { step: 1 });
	});

	it("warns once when set() is called after the execution ended (close())", async () => {
		const { setProgress, calls } = fakeSetProgress();
		const { logger, messages } = warnCollector();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{ intervalMs: 10 }
		);
		await writer.close();
		writer.set({ a: 1 });
		writer.set({ a: 2 });
		await sleep(20);
		expect(calls).toEqual([]);
		expect(messages.filter(m => m.includes("after this execution ended"))).toHaveLength(1);
	});

	it("flushForHandBack() writes the pending value even though ownership is already marked gone", async () => {
		const { setProgress, calls } = fakeSetProgress();
		let gone = false;
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger: quietLogger, heartbeatTimeoutMs: 5_000 } },
			ctx({ ownershipGone: () => gone }),
			{ intervalMs: 10_000 }
		);
		writer.set({ a: 1 }); // leading write
		writer.set({ a: 2 }); // pending behind the window
		// What stop() does (decision 20): mark the execution handed back, then flush BEFORE the
		// release, while the store's ownership guard still accepts this execution's writes.
		gone = true;
		const flushed = writer.flushForHandBack();
		// The executor's own close() racing it must not drop the value.
		await Promise.all([flushed, writer.close()]);
		expect(calls).toEqual([{ a: 1 }, { a: 2 }]);
	});

	it("flushForHandBack() with nothing pending waits for the write in flight and writes nothing more", async () => {
		const { setProgress, calls } = fakeSetProgress();
		const writer = createRunProgress(
			{ runs: { setProgress }, config: { logger: quietLogger, heartbeatTimeoutMs: 5_000 } },
			ctx(),
			{ intervalMs: 10_000 }
		);
		writer.set({ a: 1 });
		await writer.flushForHandBack();
		expect(calls).toEqual([{ a: 1 }]);
		await writer.close();
		expect(calls).toEqual([{ a: 1 }]);
	});
});
