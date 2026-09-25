import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createRetention } from "../../src/retention.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import { useTestNamespace } from "../helpers/namespace.js";
import { useRunFixtures } from "../helpers/run-fixtures.js";

const ns = useTestNamespace();
const f = useRunFixtures(ns);

function store() {
	return createSqlStore(ns.pool, ns.schema);
}

function makeLogger(): { logger: EngineLogger; logs: Array<{ level: string; fields: unknown }> } {
	const logs: Array<{ level: string; fields: unknown }> = [];
	const logger: EngineLogger = {
		debug: fields => logs.push({ level: "debug", fields }),
		info: fields => logs.push({ level: "info", fields }),
		warn: fields => logs.push({ level: "warn", fields }),
		error: fields => logs.push({ level: "error", fields }),
	};
	return { logger, logs };
}

/** Creates + completes a run, then backdates `finished_at` by `ageDays`. */
async function finishedRun(ageDays: number, overrides: Parameters<typeof f.newRun>[0] = {}) {
	const { run, owner } = await f.running(overrides);
	await f.runs.complete(owner, null);
	await f.sql(
		`UPDATE ${f.t("runs")} SET finished_at = now() - $2::float8 * interval '1 day' WHERE id = $1`,
		[run.id, ageDays]
	);
	return run.id;
}

describe("createRetention", () => {
	it("deletes finished runs older than `days`, in batches, until a short batch ends the loop", async () => {
		const total = 25;
		const batchSize = 10;
		const ids: string[] = [];
		for (let i = 0; i < total; i++) {
			ids.push(await finishedRun(60));
		}

		const s = store();
		const spy = vi.spyOn(s.runs, "deleteFinishedBatch");
		const { logger } = makeLogger();
		const retention = createRetention({ store: s, logger, days: 30, batchSize });

		const deleted = await retention.runOnce();

		expect(deleted).toBe(total);
		// ceil(25 / 10) = 3 batches: 10, 10, 5 (the last one, being < batchSize, ends the loop).
		expect(spy).toHaveBeenCalledTimes(3);
		for (const id of ids) {
			expect(await store().runs.get(id)).toBeNull();
		}
	});

	it("leaves runs younger than `days` alone", async () => {
		const youngId = await finishedRun(1);
		const s = store();
		const { logger } = makeLogger();
		const retention = createRetention({ store: s, logger, days: 30, batchSize: 100 });

		expect(await retention.runOnce()).toBe(0);
		expect(await store().runs.get(youngId)).not.toBeNull();
	});

	it("keeps a finished run whose parent is still unfinished", async () => {
		const parent = await f.running({ taskId: "parent" });
		const child = await f.running({ taskId: "child" });
		// Make `child` a `step.trigger` descendant of `parent` without going through triggerChildren
		// (which requires the parent to still be running/acquired) -- direct SQL is simpler here and
		// matches what other reconciler-adjacent tests do to force a specific graph shape.
		await f.sql(`UPDATE ${f.t("runs")} SET parent_run_id = $1 WHERE id = $2`, [
			parent.owner.runId,
			child.owner.runId,
		]);
		await f.runs.complete(child.owner, null);
		await f.sql(
			`UPDATE ${f.t("runs")} SET finished_at = now() - interval '60 days' WHERE id = $1`,
			[child.owner.runId]
		);
		// `parent` itself stays running (unfinished).

		const s = store();
		const { logger } = makeLogger();
		const retention = createRetention({ store: s, logger, days: 30, batchSize: 100 });

		expect(await retention.runOnce()).toBe(0);
		expect(await store().runs.get(child.owner.runId)).not.toBeNull();
	});

	it("a step.trigger child survives its deleted parent, with parent_run_id set NULL", async () => {
		const parent = await f.running({ taskId: "parent" });
		const waitpointId = randomUUID();
		const childId = randomUUID();
		const created = await f.runs.triggerChildren({
			parent: parent.owner,
			stepName: "child",
			seqIndex: 0,
			mode: "trigger",
			waitpointId,
			timeoutMs: null,
			children: [
				{
					id: childId,
					taskId: "child-task",
					queue: "default",
					payload: null,
					maxAttempts: 1,
					idempotencyKey: `__child:${parent.owner.runId}:child`,
					batchIndex: null,
				},
			],
		});
		if (created.kind !== "created") throw new Error("expected created");

		// Parent finishes and becomes old enough to be deleted.
		await f.runs.complete(parent.owner, null);
		await f.sql(
			`UPDATE ${f.t("runs")} SET finished_at = now() - interval '60 days' WHERE id = $1`,
			[parent.owner.runId]
		);
		// The child also finishes, but recently -- not old enough to be deleted itself.
		const childOwner = await f.acquire(childId);
		await f.runs.complete(childOwner, null);
		await f.sql(`UPDATE ${f.t("runs")} SET finished_at = now() - interval '1 hour' WHERE id = $1`, [
			childId,
		]);

		const s = store();
		const { logger } = makeLogger();
		const retention = createRetention({ store: s, logger, days: 30, batchSize: 100 });

		const deleted = await retention.runOnce();
		expect(deleted).toBe(1);

		expect(await store().runs.get(parent.owner.runId)).toBeNull();
		const child = await store().runs.get(childId);
		expect(child).not.toBeNull();
		expect(child?.parentRunId).toBeNull();
	});

	it("stops after maxIterations, warns, and can be resumed by a later call", async () => {
		const total = 12;
		for (let i = 0; i < total; i++) {
			await finishedRun(60);
		}

		const s = store();
		const { logger, logs } = makeLogger();
		const retention = createRetention({
			store: s,
			logger,
			days: 30,
			batchSize: 5,
			maxIterations: 2,
		});

		const deleted = await retention.runOnce();
		expect(deleted).toBe(10); // 2 iterations * batchSize 5, cap reached with a still-full batch
		expect(logs.some(l => l.level === "warn")).toBe(true);

		const remaining = await retention.runOnce();
		expect(remaining).toBe(2);
	});
});
