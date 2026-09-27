/**
 * Review 5, item A1: the run claim (and `nextDueInMs`) must read `idx_runs_claim` in order and
 * stop at the LIMIT, even with a large due backlog and `queue_state` populated. A pause check
 * correlated to the outer row (`q.queue = r.queue`) made the planner pick an anti join and sort
 * the whole due backlog before the LIMIT (137 ms/claim with 500k due runs; the claim's 1.5 s
 * statement timeout then stalls the queue with millions).
 */
import { Client, escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlStore, type SqlStore } from "../../src/store/sql/index.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let store: SqlStore;
let s: string;

const HOT = "hot";
const BACKLOG = 150_000;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	store = createSqlStore(ns.pool, ns.schema);
	s = escapeIdentifier(ns.schema);
	// A large due backlog in HOT spread over the past hour, plus other queues and statuses.
	await ns.pool.query(
		`INSERT INTO ${s}.runs (id, task_id, queue, payload, status, max_attempts, run_after, created_at)
		 SELECT gen_random_uuid(), 't', $1, 'null', 'queued', 1,
		        now() - interval '1 hour' + (g * interval '20 milliseconds'),
		        now() - interval '1 hour'
		 FROM generate_series(1, $2::int) g`,
		[HOT, BACKLOG]
	);
	await ns.pool.query(
		`INSERT INTO ${s}.runs (id, task_id, queue, payload, status, max_attempts, run_after, created_at)
		 SELECT gen_random_uuid(), 't', 'q' || (g % 20), 'null',
		        (ARRAY['queued','waiting','completed','failed'])[1 + g % 4], 1,
		        CASE WHEN g % 4 < 2 THEN now() - interval '10 minutes' ELSE NULL END,
		        now() - interval '2 hours'
		 FROM generate_series(1, 100000) g`
	);
	await ns.pool.query(
		`INSERT INTO ${s}.runs (id, task_id, queue, payload, status, max_attempts, started_at, heartbeat_at, execution_id)
		 SELECT gen_random_uuid(), 't', $1, 'null', 'running', 1, now(), now(), gen_random_uuid()
		 FROM generate_series(1, 50) g`,
		[HOT]
	);
	await ns.pool.query(
		`INSERT INTO ${s}.queue_state (queue, paused)
		 SELECT 'q' || g, g % 2 = 0 FROM generate_series(0, 19) g`
	);
	await ns.pool.query(`ANALYZE ${s}.runs`);
	await ns.pool.query(`ANALYZE ${s}.queue_state`);
});

afterEach(async () => {
	vi.restoreAllMocks();
	await ns.pool.query(`DELETE FROM ${s}.queue_state WHERE queue = $1`, [HOT]);
});

/** Runs `fn` and returns the first statement it sent (via any pooled client) containing `marker`. */
async function capture(
	marker: string,
	fn: () => Promise<unknown>
): Promise<{ text: string; values: unknown[] }> {
	const calls: Array<[string, unknown[]]> = [];
	const original = Client.prototype.query;
	vi.spyOn(Client.prototype, "query").mockImplementation(function (
		this: Client,
		...args: unknown[]
	) {
		if (typeof args[0] === "string") calls.push([args[0], (args[1] as unknown[]) ?? []]);
		return (original as (...a: unknown[]) => unknown).apply(this, args) as never;
	});
	await fn();
	vi.restoreAllMocks();
	const hit = calls.find(([text]) => text.includes(marker));
	if (!hit) throw new Error(`no statement containing ${marker}`);
	return { text: hit[0], values: hit[1] };
}

/** EXPLAIN (ANALYZE) inside a rolled-back transaction (the claim is an UPDATE). */
async function explainAnalyze(text: string, values: unknown[]): Promise<string> {
	const client = await ns.pool.connect();
	try {
		await client.query("BEGIN");
		const { rows } = await client.query<{ "QUERY PLAN": string }>(
			`EXPLAIN (ANALYZE, BUFFERS) ${text}`,
			values
		);
		return rows.map(r => r["QUERY PLAN"]).join("\n");
	} finally {
		await client.query("ROLLBACK");
		client.release();
	}
}

function executionMs(plan: string): number {
	const m = /Execution Time: ([\d.]+) ms/.exec(plan);
	if (!m?.[1]) throw new Error(`no execution time in plan:\n${plan}`);
	return Number(m[1]);
}

/** A full `Sort` node (an `Incremental Sort` over the index order is fine). */
function hasFullSort(plan: string): boolean {
	return plan.split("\n").some(line => /(^|->)\s*Sort\s+\(/.test(line));
}

describe("claim plan with a large due backlog (A1)", () => {
	it("claims through idx_runs_claim in order, without sorting the backlog", async () => {
		const { text, values } = await capture("WITH picked", () =>
			store.claim.claimRuns({ queue: HOT, limit: 20 }).then(async runs => {
				// Undo: this test only wants the statement.
				await ns.pool.query(
					`UPDATE ${s}.runs SET status = 'queued', execution_id = NULL WHERE id = ANY($1::uuid[])`,
					[runs.map(r => r.id)]
				);
			})
		);
		const plan = await explainAnalyze(text, values);
		expect(plan, plan).toContain("idx_runs_claim");
		expect(plan, plan).not.toMatch(/Anti Join/);
		expect(hasFullSort(plan), plan).toBe(false);
		expect(executionMs(plan), plan).toBeLessThan(50);
	});

	it("a paused queue with a backlog costs almost nothing per poll", async () => {
		await ns.pool.query(`INSERT INTO ${s}.queue_state (queue, paused) VALUES ($1, true)`, [HOT]);
		const { text, values } = await capture("WITH picked", async () => {
			expect(await store.claim.claimRuns({ queue: HOT, limit: 20 })).toEqual([]);
		});
		const plan = await explainAnalyze(text, values);
		expect(plan, plan).not.toMatch(/Anti Join/);
		expect(hasFullSort(plan), plan).toBe(false);
		expect(executionMs(plan), plan).toBeLessThan(10);
	});

	it("nextDueInMs of a queue with a large due backlog reads the index", async () => {
		const { text, values } = await capture("min(run_after)", () => store.claim.nextDueInMs(HOT));
		const plan = await explainAnalyze(text, values);
		expect(plan, plan).not.toMatch(/Anti Join/);
		expect(hasFullSort(plan), plan).toBe(false);
		expect(executionMs(plan), plan).toBeLessThan(10);
	});
});
