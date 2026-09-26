/**
 * Review 3:
 * - item 3: reconciler case 1 (`listStaleQueued`) skips runs still flagged `outbox_pending`
 *   (the outbox sweep dispatches those; recovering them too emits a duplicate `run:queued`);
 * - item 6: an error thrown by `closeWorkers`' `beforeClose` is logged, and closing continues.
 */
import { afterEach, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createRedisConnections } from "../../src/queue/connections.js";
import { createTransport } from "../../src/queue/transport.js";
import { useTestNamespace } from "../helpers/namespace.js";
import { useRunFixtures } from "../helpers/run-fixtures.js";

const ns = useTestNamespace();
const f = useRunFixtures(ns);

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	await Promise.all(cleanups.splice(0).map(fn => fn().catch(() => {})));
});

describe("listStaleQueued and the outbox (review 3 item 3)", () => {
	it("skips a committed run still waiting for the outbox sweep", async () => {
		const pending = f.newRun();
		await f.runs.create({ ...pending, outbox: { channel: "mt_outbox_s_review3", notify: false } });
		const plain = f.newRun();
		await f.runs.create(plain);
		// Both committed long ago (e.g. the caller's transaction stayed open for a while: `now()` is
		// the transaction start, so `run_after` is already past the grace period at commit).
		await f.sql(`UPDATE ${f.t("runs")} SET run_after = now() - interval '1 hour'`);

		const page = await f.runs.listStaleQueued(1_000, 10);
		expect(page.items.map(i => i.runId)).toEqual([plain.id]);

		// Once claimed (the sweep enqueued it), a lost enqueue is again the reconciler's job.
		const claim = await f.runs.claimOutbox(10);
		expect(claim.claimed).toBe(1);
		const after = await f.runs.listStaleQueued(1_000, 10);
		expect(after.items.map(i => i.runId).sort()).toEqual([pending.id, plain.id].sort());
	});
});

describe("closeWorkers beforeClose failure (review 3 item 6)", () => {
	it("logs the error and still closes", async () => {
		const warns: Array<{ message: string; fields: Record<string, unknown> }> = [];
		const logger: EngineLogger = {
			debug: () => {},
			info: () => {},
			warn: (fields, message) => void warns.push({ message, fields }),
			error: () => {},
		};
		const t = createTransport({
			prefix: ns.prefix,
			connections: createRedisConnections(ns.redisUrl),
			queues: [{ name: "q", concurrency: 1 }],
			logger,
		});
		await t.startWorkers({ processRun: async () => {}, processSystem: async () => {} });
		const boom = new Error("hand-back failed");
		await expect(
			t.closeWorkers({
				timeoutMs: 500,
				beforeClose: async () => {
					throw boom;
				},
			})
		).resolves.toEqual({ stillActive: [] });
		const logged = warns.find(w => w.fields.err === boom);
		expect(logged?.message).toMatch(/beforeClose/);
	});
});
