import { describe, expect, it } from "vitest";
import { ExecutionState } from "../../src/executor/execution.js";
import { createRunLogger } from "../../src/executor/logger.js";
import type { RunRecord } from "../../src/store/types.js";

function state(): ExecutionState {
	return new ExecutionState({ id: "r1", taskId: "t1" } as RunRecord, "e1");
}

/** Mirrors `ownershipGone` in `src/executor/execute-run.ts` (not exported). */
function ownershipGone(s: ExecutionState): boolean {
	return (
		s.handedBack ||
		s.abortKind === "lost" ||
		s.abortKind === "canceled" ||
		s.abortKind === "shutdown"
	);
}

describe("ExecutionState.abort()", () => {
	it("records the first abort kind and ignores a later one of a different reason", () => {
		const s = state();
		s.abort("timeout");
		s.abort("canceled");
		expect(s.abortKind).toBe("timeout");
	});

	// External review item 1: `settleBeforeFail` fires `abort("finished")` right before the
	// failure write. If a real cancel/lost/shutdown lands after that, `abortKind` must stop
	// reading "finished" forever, or `ownershipGone(state)` (read live by the run logger/progress
	// writer at flush/close time) never reflects that ownership is actually gone.
	it.each(["lost", "canceled", "shutdown"] as const)(
		"lets a later '%s' overwrite a prior 'finished' abort",
		kind => {
			const s = state();
			s.abort("finished");
			expect(s.abortKind).toBe("finished");
			s.abort(kind);
			expect(s.abortKind).toBe(kind);
			// The signal was already aborted by "finished" and is not re-aborted with a new reason.
			expect(s.signal.aborted).toBe(true);
		}
	);

	it("does not let 'finished' overwrite an ownership-lost kind (order matters)", () => {
		const s = state();
		s.abort("canceled");
		s.abort("finished");
		expect(s.abortKind).toBe("canceled");
	});

	it("only overwrites 'finished' once (a second late abort is ignored)", () => {
		const s = state();
		s.abort("finished");
		s.abort("canceled");
		s.abort("lost");
		expect(s.abortKind).toBe("canceled");
	});

	it("'finished' overwritten by 'canceled' makes ownershipGone(state) true", () => {
		const s = state();
		s.abort("finished");
		expect(ownershipGone(s)).toBe(false);
		s.abort("canceled");
		expect(ownershipGone(s)).toBe(true);
	});
});

describe("run logs after a late cancel following a 'finished' abort (external review item 1)", () => {
	it("drops log entries buffered after the late cancel instead of writing them", async () => {
		const s = state();
		const written: unknown[][] = [];
		const logger = createRunLogger(
			{
				logs: { insertBatch: async (entries: unknown[]) => void written.push(entries) } as never,
				config: {
					logger: { debug() {}, info() {}, warn() {}, error() {} },
					logging: { minLevel: "info", mirror: false },
					limits: { maxLogEntryBytes: 1024 },
				} as never,
			},
			{
				runId: s.runId,
				taskId: s.taskId,
				attempt: 1,
				ownershipGone: () => ownershipGone(s),
			}
		);

		// The bound in `settleBeforeFail` elapses: ctx.signal aborts with "finished" ahead of the
		// terminal write.
		s.abort("finished");
		logger.info("buffered before the real cancel arrives");
		// The real cancel now arrives, racing the terminal write.
		s.abort("canceled");

		await logger.close();

		expect(written).toEqual([]);
	});
});

describe("ExecutionState.handBack()", () => {
	it("marks the hand-back and aborts with 'shutdown' before the flushes start", async () => {
		const s = state();
		const seen: Array<{ handedBack: boolean; abortKind: string | undefined }> = [];
		s.onHandBack(async () => {
			seen.push({ handedBack: s.handedBack, abortKind: s.abortKind });
		});
		await s.handBack();
		expect(seen).toEqual([{ handedBack: true, abortKind: "shutdown" }]);
		expect(s.signal.aborted).toBe(true);
	});

	it("waits for every flush and never rejects, even if one throws or rejects", async () => {
		const s = state();
		let slowDone = false;
		s.onHandBack(() => {
			throw new Error("sync boom");
		});
		s.onHandBack(() => Promise.reject(new Error("async boom")));
		s.onHandBack(async () => {
			await new Promise(resolve => setTimeout(resolve, 20));
			slowDone = true;
		});
		await expect(s.handBack()).resolves.toBeUndefined();
		expect(slowDone).toBe(true);
	});
});
