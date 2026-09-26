/**
 * Migration 008: `runs.cancelMany()` without `createdFrom` reads active runs through
 * `idx_runs_active_created` instead of walking the finished history in `idx_runs_created`.
 */
import { describe, expect, it, vi } from "vitest";
import { useTestNamespace } from "../helpers/namespace.js";
import { useRunFixtures } from "../helpers/run-fixtures.js";

const ns = useTestNamespace();
const f = useRunFixtures(ns);

describe("idx_runs_active_created (migration 008)", () => {
	it("serves the cancel candidates query, not the full created_at index", async () => {
		// A long finished history, older than the few active runs.
		await f.sql(
			`INSERT INTO ${f.t("runs")} (id, task_id, queue, payload, status, max_attempts, created_at, finished_at)
			 SELECT gen_random_uuid(), 'history', 'default', 'null', 'completed', 1,
			        now() - interval '30 days' + g * interval '1 second', now()
			 FROM generate_series(1, 20000) g`
		);
		const active = [f.newRun(), f.newRun(), f.newRun()];
		for (const run of active) await f.runs.create(run);
		await f.sql(`ANALYZE ${f.t("runs")}`);

		// Capture the exact statement the store runs, then EXPLAIN it with the same parameters.
		const spy = vi.spyOn(ns.pool, "query");
		const page = await f.runs.listBatchCandidates({ purpose: "cancel", limit: 100 });
		const call = spy.mock.calls.find(([text]) => String(text).includes("ORDER BY r.created_at"));
		spy.mockRestore();
		expect(page.items.map(r => r.id).sort()).toEqual(active.map(r => r.id).sort());
		expect(call).toBeDefined();
		const [text, values] = call as unknown as [string, unknown[]];

		const { rows } = await ns.pool.query<{ "QUERY PLAN": string }>(`EXPLAIN ${text}`, values);
		const plan = rows.map(row => row["QUERY PLAN"]).join("\n");
		expect(plan).toContain("idx_runs_active_created");
		expect(plan).not.toContain("idx_runs_created ");
	});
});
