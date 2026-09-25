import { randomUUID } from "node:crypto";
import { beforeAll, describe, expect, it } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlContext, type SqlContext } from "../../src/store/sql/db.js";
import { createStepStore } from "../../src/store/sql/steps.js";
import type { Json, Ownership, StepStore } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";

// task 2.2b owns steps.ts/logs.ts/index.ts and these three test files only; runs.ts is owned by
// another agent working concurrently, so runs are inserted with raw SQL here rather than through
// the (still being written) RunStore, per the task brief.

const ns = useTestNamespace();
let ctx: SqlContext;
let steps: StepStore;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	ctx = createSqlContext(ns.schema);
	steps = createStepStore({ kind: "pool", pool: ns.pool }, ctx);
});

/** Inserts a `running` run directly (bypassing RunStore) and returns its ownership. */
async function insertRunningRun(): Promise<Ownership> {
	const runId = randomUUID();
	const executionId = randomUUID();
	await ns.pool.query(
		`INSERT INTO ${ctx.t("runs")}
		   (id, task_id, queue, payload, status, max_attempts, execution_id, enqueue_seq, run_after)
		 VALUES ($1, 'task-a', 'default', '{}'::jsonb, 'running', 3, $2, 1, now())`,
		[runId, executionId]
	);
	return { runId, executionId };
}

/** Flips a run's status so its execution no longer owns it (loses the ownership guard). */
async function loseOwnership(runId: string): Promise<void> {
	await ns.pool.query(
		`UPDATE ${ctx.t("runs")} SET status = 'canceled', execution_id = NULL WHERE id = $1`,
		[runId]
	);
}

async function dbNow(): Promise<Date> {
	const { rows } = await ns.pool.query<{ now: Date }>("SELECT now() AS now");
	const row = rows[0];
	if (!row) throw new Error("SELECT now() returned no rows");
	return row.now;
}

describe("StepStore.loadForExecution", () => {
	it("returns steps inline (not deferred) when under maxInlineBytes", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, { kind: "run", name: "a", seqIndex: 1, output: { v: 1 } });
		await steps.insertCompleted(owner, { kind: "run", name: "b", seqIndex: 2, output: { v: 2 } });

		const snapshot = await steps.loadForExecution(owner.runId, 16 * 1024 * 1024);

		expect(snapshot.outputsDeferred).toBe(false);
		expect(snapshot.steps.map(s => [s.name, s.output])).toEqual([
			["a", { v: 1 }],
			["b", { v: 2 }],
		]);
		expect(snapshot.now).toBeInstanceOf(Date);
	});

	it("defers outputs (all null) when the total size exceeds a tiny maxInlineBytes", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, {
			kind: "run",
			name: "a",
			seqIndex: 1,
			output: { v: "x".repeat(100) },
		});
		await steps.insertCompleted(owner, {
			kind: "run",
			name: "b",
			seqIndex: 2,
			output: { v: "y".repeat(100) },
		});

		const snapshot = await steps.loadForExecution(owner.runId, 1);

		expect(snapshot.outputsDeferred).toBe(true);
		expect(snapshot.steps).toHaveLength(2);
		for (const step of snapshot.steps) {
			expect(step.output).toBeNull();
		}
	});

	it("is not deferred and has no waitpoints for an empty run", async () => {
		const owner = await insertRunningRun();
		const snapshot = await steps.loadForExecution(owner.runId, 16 * 1024 * 1024);
		expect(snapshot.steps).toEqual([]);
		expect(snapshot.outputsDeferred).toBe(false);
		expect(snapshot.waitpoints).toEqual([]);
	});

	it("orders steps by seq_index regardless of insertion order", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, { kind: "run", name: "third", seqIndex: 3, output: null });
		await steps.insertCompleted(owner, { kind: "run", name: "first", seqIndex: 1, output: null });
		await steps.insertCompleted(owner, { kind: "run", name: "second", seqIndex: 2, output: null });

		const snapshot = await steps.loadForExecution(owner.runId, 16 * 1024 * 1024);
		expect(snapshot.steps.map(s => s.name)).toEqual(["first", "second", "third"]);
	});

	it("includes the run's waitpoints without outputs", async () => {
		const owner = await insertRunningRun();
		const waitpointId = randomUUID();
		await ns.pool.query(
			`INSERT INTO ${ctx.t("waitpoints")}
			   (id, run_id, step_name, kind, status, pending_count, output, expires_at)
			 VALUES ($1, $2, 'approve', 'token', 'pending', NULL, NULL, now() + interval '1 day')`,
			[waitpointId, owner.runId]
		);

		const snapshot = await steps.loadForExecution(owner.runId, 16 * 1024 * 1024);
		expect(snapshot.waitpoints).toEqual([
			{
				id: waitpointId,
				stepName: "approve",
				kind: "token",
				status: "pending",
				pendingCount: null,
				expiresAt: expect.any(Date),
			},
		]);
	});
});

describe("StepStore.loadOutputs", () => {
	it("returns only the requested seq_index range, ordered", async () => {
		const owner = await insertRunningRun();
		for (let i = 1; i <= 5; i++) {
			await steps.insertCompleted(owner, {
				kind: "run",
				name: `s${i}`,
				seqIndex: i,
				output: { i },
			});
		}

		const range = await steps.loadOutputs(owner.runId, 2, 4);
		expect(range).toEqual([
			{ name: "s2", seqIndex: 2, output: { i: 2 } },
			{ name: "s3", seqIndex: 3, output: { i: 3 } },
			{ name: "s4", seqIndex: 4, output: { i: 4 } },
		]);
	});

	it("returns an empty array outside the range", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, { kind: "run", name: "s1", seqIndex: 1, output: { i: 1 } });
		expect(await steps.loadOutputs(owner.runId, 10, 20)).toEqual([]);
	});
});

describe("StepStore.insertCompleted", () => {
	it("inserts a new step and reports ok", async () => {
		const owner = await insertRunningRun();
		const result = await steps.insertCompleted(owner, {
			kind: "run",
			name: "step-a",
			seqIndex: 1,
			output: { done: true },
		});
		expect(result).toBe("ok");

		const [record] = await steps.list(owner.runId);
		expect(record).toMatchObject({
			name: "step-a",
			seqIndex: 1,
			kind: "run",
			status: "completed",
			output: { done: true },
		});
	});

	it("is idempotent: a duplicate insert (ON CONFLICT DO NOTHING) still reports ok and does not overwrite", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, {
			kind: "run",
			name: "step-a",
			seqIndex: 1,
			output: { v: "first" },
		});

		const second = await steps.insertCompleted(owner, {
			kind: "run",
			name: "step-a",
			seqIndex: 1,
			output: { v: "second" },
		});
		expect(second).toBe("ok");

		const records = await steps.list(owner.runId);
		expect(records).toHaveLength(1);
		expect(records[0]?.output).toEqual({ v: "first" });
	});

	it("reports lost when the execution no longer owns the run", async () => {
		const owner = await insertRunningRun();
		await loseOwnership(owner.runId);

		const result = await steps.insertCompleted(owner, {
			kind: "run",
			name: "step-a",
			seqIndex: 1,
			output: null,
		});
		expect(result).toBe("lost");
		expect(await steps.list(owner.runId)).toEqual([]);
	});

	it("reports lost when executionId doesn't match, even if the run is running", async () => {
		const owner = await insertRunningRun();
		const wrongOwner: Ownership = { runId: owner.runId, executionId: randomUUID() };

		const result = await steps.insertCompleted(wrongOwner, {
			kind: "run",
			name: "step-a",
			seqIndex: 1,
			output: null,
		});
		expect(result).toBe("lost");
	});

	it("enforces UNIQUE(run_id, seq_index) at the schema level", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, { kind: "run", name: "step-a", seqIndex: 1, output: null });

		await expect(
			ns.pool.query(
				`INSERT INTO ${ctx.t("steps")} (run_id, name, seq_index, kind, status, output)
				 VALUES ($1, 'step-b', 1, 'run', 'completed', NULL)`,
				[owner.runId]
			)
		).rejects.toThrow(/duplicate key|unique/i);
	});
});

describe("StepStore.insertSleep / completeSleep", () => {
	it("computes wake_at and delayMs from the DB clock", async () => {
		const owner = await insertRunningRun();
		const before = await dbNow();
		const durationMs = 60_000;

		const result = await steps.insertSleep(owner, { name: "nap", seqIndex: 1, durationMs });
		expect(result.kind).toBe("inserted");
		if (result.kind !== "inserted") return;

		const after = await dbNow();
		expect(result.wakeAt.getTime()).toBeGreaterThanOrEqual(before.getTime() + durationMs - 1000);
		expect(result.wakeAt.getTime()).toBeLessThanOrEqual(after.getTime() + durationMs + 1000);
		// delayMs is computed from the same `now()` as wakeAt, so it should be very close to
		// durationMs (allow a little slack for query latency).
		expect(result.delayMs).toBeGreaterThan(durationMs - 2000);
		expect(result.delayMs).toBeLessThanOrEqual(durationMs);
	});

	it("reports lost when ownership is lost", async () => {
		const owner = await insertRunningRun();
		await loseOwnership(owner.runId);
		const result = await steps.insertSleep(owner, { name: "nap", seqIndex: 1, durationMs: 1000 });
		expect(result).toEqual({ kind: "lost" });
	});

	it("completeSleep is not_due while wake_at is in the future, then ok once past, driven by DB time", async () => {
		const owner = await insertRunningRun();
		await steps.insertSleep(owner, { name: "nap", seqIndex: 1, durationMs: 60 * 60_000 });

		expect(await steps.completeSleep(owner, "nap")).toBe("not_due");

		await ns.pool.query(
			`UPDATE ${ctx.t("steps")} SET wake_at = now() - interval '1 second' WHERE run_id = $1 AND name = $2`,
			[owner.runId, "nap"]
		);

		expect(await steps.completeSleep(owner, "nap")).toBe("ok");
		const [record] = await steps.list(owner.runId);
		expect(record?.status).toBe("completed");
	});

	it("completeSleep reports lost when ownership is lost", async () => {
		const owner = await insertRunningRun();
		await steps.insertSleep(owner, { name: "nap", seqIndex: 1, durationMs: 1 });
		await loseOwnership(owner.runId);
		expect(await steps.completeSleep(owner, "nap")).toBe("lost");
	});
});

describe("StepStore.insertWaitRef", () => {
	it("inserts a `name#wait` step referencing the waitpoint", async () => {
		const owner = await insertRunningRun();
		const waitpointId = randomUUID();
		const result = await steps.insertWaitRef(owner, {
			kind: "wait",
			name: "approval#wait",
			seqIndex: 2,
			waitpointId,
		});
		expect(result).toBe("ok");

		const [record] = await steps.list(owner.runId);
		expect(record).toMatchObject({
			name: "approval#wait",
			kind: "wait",
			status: "waiting",
			output: { waitpointId },
			wakeAt: null,
		});
	});

	it("reports lost when ownership is lost", async () => {
		const owner = await insertRunningRun();
		await loseOwnership(owner.runId);
		const result = await steps.insertWaitRef(owner, {
			kind: "wait",
			name: "approval#wait",
			seqIndex: 2,
			waitpointId: randomUUID(),
		});
		expect(result).toBe("lost");
	});
});

describe("StepStore.list", () => {
	it("returns every column, ordered by seq_index", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, { kind: "run", name: "b", seqIndex: 2, output: { v: 2 } });
		await steps.insertCompleted(owner, { kind: "run", name: "a", seqIndex: 1, output: { v: 1 } });

		const records = await steps.list(owner.runId);
		expect(records.map(r => r.name)).toEqual(["a", "b"]);
		for (const record of records) {
			expect(record.runId).toBe(owner.runId);
			expect(record.createdAt).toBeInstanceOf(Date);
		}
	});
});

describe("StepStore: jsonb outputs of every Json shape", () => {
	const shapes: [string, Json | null][] = [
		["a bare string", "hello"],
		["a string with quotes/backslashes", 'say "hi" \\ {x,y}'],
		["an array of scalars", [1, "two", false, null]],
		["an array of objects", [{ a: 1 }, { b: [2, 3] }]],
		["a nested object", { a: { b: { c: ["d", { e: null }] } } }],
		["a number", 42],
		["a boolean", false],
		["null", null],
	];

	for (const [label, output] of shapes) {
		it(`insertCompleted round-trips ${label}`, async () => {
			const owner = await insertRunningRun();
			expect(
				await steps.insertCompleted(owner, { kind: "run", name: "s", seqIndex: 1, output })
			).toBe("ok");
			const [record] = await steps.list(owner.runId);
			expect(record?.output).toEqual(output);
			const snapshot = await steps.loadForExecution(owner.runId, 16 * 1024 * 1024);
			expect(snapshot.steps[0]?.output).toEqual(output);
		});
	}
});

describe("StepStore: (run_id, seq_index) collisions under a different name", () => {
	it("insertCompleted throws instead of silently swallowing the collision", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, { kind: "run", name: "a", seqIndex: 1, output: 1 });
		await expect(
			steps.insertCompleted(owner, { kind: "run", name: "b", seqIndex: 1, output: 2 })
		).rejects.toThrow(/duplicate key|unique/i);
	});

	it("insertWaitRef: an existing step with the same name is ok (idempotent), not lost", async () => {
		const owner = await insertRunningRun();
		const waitpointId = randomUUID();
		const step = { kind: "wait" as const, name: "approval#wait", seqIndex: 2, waitpointId };
		expect(await steps.insertWaitRef(owner, step)).toBe("ok");
		expect(await steps.insertWaitRef(owner, step)).toBe("ok");
		expect(await steps.list(owner.runId)).toHaveLength(1);
	});

	it("insertWaitRef throws on a seq_index collision under a different name", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, { kind: "run", name: "a", seqIndex: 2, output: 1 });
		await expect(
			steps.insertWaitRef(owner, {
				kind: "wait",
				name: "approval#wait",
				seqIndex: 2,
				waitpointId: randomUUID(),
			})
		).rejects.toThrow(/duplicate key|unique/i);
	});
});

describe("StepStore: step kind", () => {
	it("writes the kind of each step and returns it from loadForExecution and list", async () => {
		const owner = await insertRunningRun();
		await steps.insertCompleted(owner, { kind: "run", name: "a", seqIndex: 0, output: 1 });
		await steps.insertSleep(owner, { name: "nap", seqIndex: 1, durationMs: 60_000 });
		await steps.insertWaitRef(owner, {
			kind: "wait",
			name: "approval#wait",
			seqIndex: 2,
			waitpointId: randomUUID(),
		});
		const expected = [
			["a", "run"],
			["nap", "sleep"],
			["approval#wait", "wait"],
		];
		const snapshot = await steps.loadForExecution(owner.runId, 16 * 1024 * 1024);
		expect(snapshot.steps.map(s => [s.name, s.kind])).toEqual(expected);
		expect((await steps.list(owner.runId)).map(s => [s.name, s.kind])).toEqual(expected);
	});

	it("the schema rejects an unknown kind", async () => {
		const owner = await insertRunningRun();
		await expect(
			ns.pool.query(
				`INSERT INTO ${ctx.t("steps")} (run_id, name, seq_index, kind, status)
				 VALUES ($1, 'x', 0, 'bogus', 'completed')`,
				[owner.runId]
			)
		).rejects.toThrow(/check constraint/i);
	});
});
