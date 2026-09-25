/** Audit low item (c): a retention sweep stops between batches when its signal fires (stop()). */
import { describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createRetention } from "../../src/retention.js";
import type { Store } from "../../src/store/types.js";

const logger: EngineLogger = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

describe("createRetention: abort between batches", () => {
	it("stops after the batch in progress once the signal is aborted", async () => {
		const controller = new AbortController();
		let calls = 0;
		const store = {
			runs: {
				async deleteFinishedBatch(_olderThanMs: number, batchSize: number) {
					calls++;
					if (calls === 3) controller.abort();
					return batchSize; // always a full batch: without the signal it would loop to the cap
				},
			},
		} as unknown as Store;
		const retention = createRetention({ store, logger, days: 30, batchSize: 10 });
		const deleted = await retention.runOnce(controller.signal);
		expect(calls).toBe(3);
		expect(deleted).toBe(30);
	});

	it("does nothing with an already-aborted signal", async () => {
		let calls = 0;
		const store = {
			runs: {
				async deleteFinishedBatch() {
					calls++;
					return 0;
				},
			},
		} as unknown as Store;
		const retention = createRetention({ store, logger, days: 30, batchSize: 10 });
		expect(await retention.runOnce(AbortSignal.abort())).toBe(0);
		expect(calls).toBe(0);
	});
});
