import { Redis } from "ioredis";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createPubSub, type PubSub } from "../../src/pubsub.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

interface Side {
	pubsub: PubSub;
	publisher: Redis;
	subscriber: Redis;
}

const sides: Side[] = [];

/**
 * Builds one "process": its own publisher connection plus a connection DEDICATED to subscribing
 * (never shared with command traffic -- `docs/research/api-notes.md` §2.2), matching how
 * `RedisConnections.dedicated("subscriber")` would hand `createPubSub` a connection in real use.
 */
function createSide(): Side {
	const publisher = new Redis(ns.redisUrl);
	const subscriber = publisher.duplicate({ maxRetriesPerRequest: null });
	const pubsub = createPubSub({ publisher, subscriber, prefix: ns.prefix });
	const side = { pubsub, publisher, subscriber };
	sides.push(side);
	return side;
}

afterEach(async () => {
	while (sides.length > 0) {
		const side = sides.pop();
		if (!side) continue;
		await side.pubsub.close();
		side.publisher.disconnect();
		side.subscriber.disconnect();
	}
});

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * `subscriber.subscribe()` completes asynchronously (see `src/pubsub.ts`), so publishing before it
 * lands would just be dropped by Redis pub/sub (no queueing for a not-yet-subscribed channel).
 * Rather than republishing each test's real payload in a `waitFor` loop -- which risks the
 * subscription becoming live between two loop iterations and delivering the payload twice -- warm
 * up with disposable probe messages on a throwaway `onCancel` handler, then let each test publish
 * its real payload exactly once.
 */
async function waitUntilSubscribed(a: Side, b: Side): Promise<void> {
	let ready = false;
	const unsubscribe = b.pubsub.onCancel(() => {
		ready = true;
	});
	try {
		await waitFor(async () => {
			if (ready) return true;
			await a.pubsub.publishCancel(["__warmup__"]);
			return ready;
		});
	} finally {
		unsubscribe();
	}
}

describe("createPubSub()", () => {
	it("a cancel published by one process is received by another", async () => {
		const a = createSide();
		const b = createSide();
		await waitUntilSubscribed(a, b);

		const received: string[][] = [];
		b.pubsub.onCancel(runIds => received.push(runIds));

		await a.pubsub.publishCancel(["run-1", "run-2"]);
		await waitFor(() => received.length > 0);

		expect(received).toEqual([["run-1", "run-2"]]);
	});

	it("fans out run-status per runId: only the matching subscription is called", async () => {
		const a = createSide();
		const b = createSide();
		await waitUntilSubscribed(a, b);

		const forRun1: unknown[] = [];
		const forRun2: unknown[] = [];
		b.pubsub.subscribeStatus("run-1", msg => forRun1.push(msg));
		b.pubsub.subscribeStatus("run-2", msg => forRun2.push(msg));

		const updatedAt = new Date().toISOString();
		await a.pubsub.publishStatus({ runId: "run-1", status: "completed", updatedAt });
		await a.pubsub.publishStatus({ runId: "run-2", status: "failed", updatedAt });
		await waitFor(() => forRun1.length > 0 && forRun2.length > 0);

		expect(forRun1).toEqual([{ runId: "run-1", status: "completed", updatedAt }]);
		expect(forRun2).toEqual([{ runId: "run-2", status: "failed", updatedAt }]);
	});

	it("unsubscribe stops further delivery to that handler", async () => {
		const a = createSide();
		const b = createSide();
		await waitUntilSubscribed(a, b);

		let count = 0;
		const unsubscribe = b.pubsub.subscribeStatus("run-x", () => {
			count++;
		});

		const updatedAt = new Date().toISOString();
		await a.pubsub.publishStatus({ runId: "run-x", status: "queued", updatedAt });
		await waitFor(() => count > 0);

		unsubscribe();
		await a.pubsub.publishStatus({ runId: "run-x", status: "running", updatedAt });
		await delay(200); // give a stray delivery a chance to arrive before asserting it didn't

		expect(count).toBe(1);
	});

	it("close() is idempotent and detaches handlers from further delivery", async () => {
		const a = createSide();
		const b = createSide();
		await waitUntilSubscribed(a, b);

		let count = 0;
		b.pubsub.onCancel(() => {
			count++;
		});

		await a.pubsub.publishCancel(["r1"]);
		await waitFor(() => count > 0);

		await b.pubsub.close();
		await expect(b.pubsub.close()).resolves.toBeUndefined(); // idempotent, doesn't throw

		await a.pubsub.publishCancel(["r2"]);
		await delay(200);
		expect(count).toBe(1);
	});

	it("ignores a malformed message and reports it via logger.warn, without calling handlers", async () => {
		const warn = vi.fn();
		const publisher = new Redis(ns.redisUrl);
		const subscriber = publisher.duplicate({ maxRetriesPerRequest: null });
		const pubsub = createPubSub({ publisher, subscriber, prefix: ns.prefix, logger: { warn } });
		sides.push({ pubsub, publisher, subscriber });

		let received = 0;
		let ready = false;
		const unsubscribe = pubsub.onCancel(() => {
			ready = true;
			received++;
		});
		// Warm up with a well-formed message first (same reasoning as `waitUntilSubscribed`), then
		// unsubscribe before sending the malformed one so the warm-up delivery doesn't count.
		await waitFor(async () => {
			if (ready) return true;
			await publisher.publish(`${ns.prefix}:cancel`, JSON.stringify({ runIds: ["__warmup__"] }));
			return ready;
		});
		unsubscribe();
		received = 0;

		pubsub.onCancel(() => {
			received++;
		});
		await publisher.publish(`${ns.prefix}:cancel`, "not valid json");
		await waitFor(() => warn.mock.calls.length > 0);

		expect(received).toBe(0);
		expect(warn.mock.calls[0]?.[1]).toMatch(/malformed/i);
	});
});
