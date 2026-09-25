import { randomUUID } from "node:crypto";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlContext, type SqlContext } from "../../src/store/sql/db.js";
import { createLogStore } from "../../src/store/sql/logs.js";
import type { LogStore, NewLogEntry } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let ctx: SqlContext;
let logs: LogStore;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	ctx = createSqlContext(ns.schema);
	logs = createLogStore({ kind: "pool", pool: ns.pool }, ctx);
});

/** Inserts a bare run row (any status) so `run_logs.run_id`'s FK is satisfied. */
async function insertRun(): Promise<string> {
	const runId = randomUUID();
	await ns.pool.query(
		`INSERT INTO ${ctx.t("runs")} (id, task_id, queue, payload, max_attempts)
		 VALUES ($1, 'task-a', 'default', '{}'::jsonb, 3)`,
		[runId]
	);
	return runId;
}

function entry(runId: string, overrides: Partial<NewLogEntry> = {}): NewLogEntry {
	return { runId, level: "info", message: "hello", data: null, ...overrides };
}

describe("LogStore.insertBatch", () => {
	it("round-trips data (jsonb) of every Json shape, including null", async () => {
		const runId = await insertRun();
		const cases: NewLogEntry["data"][] = [
			null,
			{ a: 1, nested: { b: [1, 2, 3] } },
			[1, "two", false, null],
			"a bare string",
			'quotes " and \\ backslashes, {braces}, commas, NULL',
			"NULL",
			["a", "b"],
			[{ a: 1 }, { b: "x" }],
			{ s: 'nested "string"', arr: [[], {}] },
			42,
			true,
			false,
		];

		await logs.insertBatch(cases.map((data, i) => entry(runId, { message: `m${i}`, data })));

		const { rows: stored } = await ns.pool.query<{ message: string; data: unknown }>(
			`SELECT message, data FROM ${ctx.t("run_logs")} WHERE run_id = $1 ORDER BY id`,
			[runId]
		);
		expect(stored.map(r => r.data)).toEqual(cases);
	});

	it("defaults ts to the DB clock when omitted, and keeps an explicit ts", async () => {
		const runId = await insertRun();
		const explicitTs = new Date(Date.UTC(2020, 0, 1));
		await logs.insertBatch([
			entry(runId, { message: "no-ts" }),
			entry(runId, { message: "with-ts", ts: explicitTs }),
		]);

		const found = await logs.list(runId, { limit: 10 });
		const noTs = found.find(l => l.message === "no-ts");
		const withTs = found.find(l => l.message === "with-ts");
		expect(noTs?.ts.getTime()).toBeGreaterThan(Date.now() - 60_000);
		expect(withTs?.ts.toISOString()).toBe(explicitTs.toISOString());
	});

	it("does nothing (and issues no query) for an empty batch", async () => {
		const runId = await insertRun();
		const spy = vi.spyOn(ns.pool, "query");
		try {
			await logs.insertBatch([]);
			expect(spy).not.toHaveBeenCalled();
		} finally {
			spy.mockRestore();
		}
		expect(await logs.list(runId, { limit: 10 })).toEqual([]);
	});

	it.each([50, 500, 10_000])(
		"writes 10k entries in batches of %d using exactly that many INSERT statements",
		async batchSize => {
			const runId = await insertRun();
			const total = 10_000;
			const entries = Array.from({ length: total }, (_, i) => entry(runId, { message: `m${i}` }));

			const spy = vi.spyOn(ns.pool, "query");
			let expectedCalls = 0;
			try {
				for (let i = 0; i < entries.length; i += batchSize) {
					await logs.insertBatch(entries.slice(i, i + batchSize));
					expectedCalls += 1;
				}
				const insertCalls = spy.mock.calls.filter(([text]) => String(text).includes("INSERT INTO"));
				expect(insertCalls).toHaveLength(expectedCalls);
				expect(expectedCalls).toBe(Math.ceil(total / batchSize));
			} finally {
				spy.mockRestore();
			}

			const { rows } = await ns.pool.query<{ n: number }>(
				`SELECT count(*)::int AS n FROM ${ctx.t("run_logs")} WHERE run_id = $1`,
				[runId]
			);
			expect(rows[0]?.n).toBe(total);
		},
		30_000
	);
});

describe("LogStore.list", () => {
	it("orders by id ascending and supports afterId pagination (exclusive cursor)", async () => {
		const runId = await insertRun();
		await logs.insertBatch(
			Array.from({ length: 25 }, (_, i) => entry(runId, { message: `m${i}` }))
		);

		const firstPage = await logs.list(runId, { limit: 10 });
		expect(firstPage).toHaveLength(10);
		expect(firstPage.map(l => l.message)).toEqual(Array.from({ length: 10 }, (_, i) => `m${i}`));

		const lastOfFirst = firstPage[firstPage.length - 1];
		if (!lastOfFirst) throw new Error("expected a last entry");

		const secondPage = await logs.list(runId, { afterId: lastOfFirst.id, limit: 10 });
		expect(secondPage).toHaveLength(10);
		expect(secondPage.map(l => l.message)).toEqual(
			Array.from({ length: 10 }, (_, i) => `m${i + 10}`)
		);
		// Exclusive: the cursor entry itself never reappears.
		expect(secondPage.some(l => l.id === lastOfFirst.id)).toBe(false);

		const lastOfSecond = secondPage[secondPage.length - 1];
		if (!lastOfSecond) throw new Error("expected a last entry");
		const thirdPage = await logs.list(runId, { afterId: lastOfSecond.id, limit: 10 });
		expect(thirdPage).toHaveLength(5);
	});

	it("scopes to run_id", async () => {
		const runA = await insertRun();
		const runB = await insertRun();
		await logs.insertBatch([entry(runA, { message: "a" }), entry(runB, { message: "b" })]);

		expect((await logs.list(runA, { limit: 10 })).map(l => l.message)).toEqual(["a"]);
		expect((await logs.list(runB, { limit: 10 })).map(l => l.message)).toEqual(["b"]);
	});
});
