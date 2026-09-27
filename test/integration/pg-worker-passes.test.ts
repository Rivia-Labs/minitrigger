/**
 * Review 5, item A2: a claim loop must not wake up once per due run when it cannot claim any of
 * them (paused queue, `globalConcurrency` saturated). With a backlog spread over time (a run due
 * every 5 ms), both used to make ~200 passes/s per process.
 */
import { randomUUID } from "node:crypto";
import { escapeIdentifier } from "pg";
import { beforeAll, describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { migrate } from "../../src/migrations/migrate.js";
import { createQueueWorker } from "../../src/pg/workers.js";
import { createSqlStore, type SqlStore } from "../../src/store/sql/index.js";
import type { RunClaimStore } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let store: SqlStore;
let s: string;

const silent: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
const MEASURE_MS = 1_000;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	store = createSqlStore(ns.pool, ns.schema);
	s = escapeIdentifier(ns.schema);
});

/** A run of `queue` due every 5 ms over the next 10 s. */
async function spreadBacklog(queue: string): Promise<void> {
	await ns.pool.query(
		`INSERT INTO ${s}.runs (id, task_id, queue, payload, status, max_attempts, run_after)
		 SELECT gen_random_uuid(), 't', $1, 'null', 'queued', 1, now() + g * interval '5 milliseconds'
		 FROM generate_series(1, 2000) g`,
		[queue]
	);
}

/** Claim passes (claimRuns calls) of a worker over MEASURE_MS. */
async function passesPerSecond(queue: string, globalConcurrency?: number): Promise<number> {
	let claims = 0;
	const claim: RunClaimStore = {
		claimRuns: input => {
			claims++;
			return store.claim.claimRuns(input);
		},
		nextDueInMs: q => store.claim.nextDueInMs(q),
	};
	const worker = createQueueWorker({
		queue,
		concurrency: 5,
		...(globalConcurrency !== undefined ? { globalConcurrency } : {}),
		claim,
		process: async () => {},
		polling: { intervalMs: 2_000, jitterMs: 0 },
		logger: silent,
	});
	worker.start();
	await new Promise(resolve => setTimeout(resolve, MEASURE_MS));
	worker.stopClaiming();
	await worker.drain(1_000);
	return claims;
}

describe("claim loop passes with an unclaimable backlog (A2)", () => {
	it("a paused queue sleeps polling.intervalMs instead of waking per due run", async () => {
		const q = `paused_${randomUUID().slice(0, 8)}`;
		await spreadBacklog(q);
		await store.queues.setPaused(q, true);
		const passes = await passesPerSecond(q);
		expect(passes).toBeLessThanOrEqual(2);
	});

	it("a globalConcurrency-saturated queue sleeps polling.intervalMs", async () => {
		const q = `sat_${randomUUID().slice(0, 8)}`;
		await ns.pool.query(
			`INSERT INTO ${s}.runs (id, task_id, queue, payload, status, max_attempts, started_at, heartbeat_at, execution_id)
			 VALUES (gen_random_uuid(), 't', $1, 'null', 'running', 1, now(), now(), gen_random_uuid())`,
			[q]
		);
		await spreadBacklog(q);
		const passes = await passesPerSecond(q, 1);
		expect(passes).toBeLessThanOrEqual(2);
	});
});
