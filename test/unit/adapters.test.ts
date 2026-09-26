/**
 * ORM adapters (decision 26) with fake handles: argument validation, result normalization and the
 * statement each adapter hands to its ORM. Real ORMs: test/integration/adapters.test.ts.
 */
import { Client } from "pg";
import { describe, expect, it } from "vitest";
import { drizzleTx } from "../../src/adapters/drizzle.js";
import { knexTx } from "../../src/adapters/knex.js";
import { prismaTx } from "../../src/adapters/prisma.js";
import { typeormTx } from "../../src/adapters/typeorm.js";
import { validateTransactionClient } from "../../src/engine.js";
import { ConfigError } from "../../src/errors.js";
import { createSqlTxRunStore } from "../../src/store/sql/index.js";

describe("typeormTx", () => {
	const runner = (active = true, released = false) => {
		const calls: unknown[][] = [];
		return {
			calls,
			isTransactionActive: active,
			isReleased: released,
			startTransaction: async () => {},
			async query(...args: unknown[]) {
				calls.push(args);
				return { records: [{ id: "r" }], raw: [{ id: "r" }] };
			},
		};
	};

	it("runs on the transaction's QueryRunner with the structured result", async () => {
		const r = runner();
		const tx = typeormTx({ queryRunner: r });
		expect(await tx.query("SELECT $1", [1])).toEqual({ rows: [{ id: "r" }] });
		expect(r.calls).toEqual([["SELECT $1", [1], true]]);
	});

	it("rejects a manager without a transaction, an inactive runner, a non-postgres driver", () => {
		expect(() => typeormTx({ queryRunner: undefined })).toThrow(/not bound to a transaction/);
		expect(() => typeormTx(runner(false))).toThrow(/no active transaction/);
		expect(() => typeormTx(runner(true, true))).toThrow(ConfigError);
		expect(() =>
			typeormTx({ ...runner(), dataSource: { options: { type: "mysql" } } } as never)
		).toThrow(/"mysql"/);
		expect(() => typeormTx(42 as never)).toThrow(ConfigError);
		expect(() => typeormTx({} as never)).toThrow(ConfigError);
	});

	const runnerReturning = (result: unknown) => ({
		isTransactionActive: true,
		isReleased: false,
		startTransaction: async () => {},
		query: async () => result,
	});

	it("accepts a raw row array (a runner that ignores useStructuredResult)", async () => {
		const tx = typeormTx(runnerReturning([{ id: "r" }]) as never);
		expect(await tx.query("SELECT 1", [])).toEqual({ rows: [{ id: "r" }] });
	});

	it("throws ConfigError on an unknown result shape instead of treating it as zero rows", async () => {
		for (const result of [{ affected: 1 }, undefined, null, { records: "nope" }]) {
			const tx = typeormTx(runnerReturning(result) as never);
			await expect(tx.query("SELECT 1", [])).rejects.toThrow(ConfigError);
		}
	});
});

describe("knexTx", () => {
	const trx = (overrides: Record<string, unknown> = {}) => {
		const calls: unknown[][] = [];
		const fn = Object.assign(() => {}, {
			calls,
			isTransaction: true,
			isCompleted: () => false,
			client: { dialect: "postgresql" },
			raw: async (...args: unknown[]) => {
				calls.push(args);
				return { rows: [{ id: "r" }], rowCount: 1 };
			},
			...overrides,
		});
		return fn;
	};

	it("converts $n to escaped ? SQL (padded with spaces) with bindings per occurrence", async () => {
		const t = trx();
		expect(await knexTx(t).query("SELECT $1, $1 ? 'a'", ["v"])).toEqual({
			rows: [{ id: "r" }],
			rowCount: 1,
		});
		expect(t.calls).toEqual([["SELECT  ? ,  ?  \\? 'a'", ["v", "v"]]]);
	});

	it("rejects the knex instance, a completed transaction, other dialects", async () => {
		expect(() => knexTx(trx({ isTransaction: false }))).toThrow(/not a transaction/);
		expect(() => knexTx(trx({ isCompleted: () => true }))).toThrow(/already committed/);
		expect(() => knexTx(trx({ client: { dialect: "mysql" } }))).toThrow(/"mysql"/);
		expect(() => knexTx({} as never)).toThrow(ConfigError);
	});

	it("a key-renaming postProcessResponse (camelCase) keeps the run id the tx path relies on", async () => {
		// knex applies the user's postProcessResponse to trx.raw() too, so `task_id` comes back as
		// `taskId` and the store's row mapping loses it; `id` survives any case transform. The
		// engine's tx path must therefore only read `id` (and use the task it already has).
		const camelRow = { id: "11111111-1111-1111-1111-111111111111", taskId: "t", notified: "" };
		const t = trx({ raw: async () => ({ rows: [camelRow], rowCount: 1 }) });
		const store = createSqlTxRunStore(knexTx(t), "mt_camel");
		const result = await store.create({
			id: camelRow.id,
			taskId: "t",
			queue: "default",
			payload: null,
			maxAttempts: 1,
			delayMs: 0,
			outbox: { channel: "c" },
		});
		expect(result.kind).toBe("created");
		expect(result.run.id).toBe(camelRow.id);
	});
});

describe("drizzleTx", () => {
	/** A stand-in for drizzle's `sql` tag recording what it was built with. */
	const fakeSql = Object.assign(
		(strings: TemplateStringsArray, ...params: unknown[]) => ({ strings: [...strings], params }),
		{ param: (value: unknown) => ({ param: value }) }
	);

	/** Recognizes the adapter's own `pg_current_xact_id()` transaction-identity check query. */
	const isXactCheck = (query: { strings?: unknown[] }) =>
		Array.isArray(query.strings) &&
		typeof query.strings[0] === "string" &&
		query.strings[0].includes("pg_current_xact_id");

	it("builds the statement from the sql tag with sql.param values", async () => {
		const executed: unknown[] = [];
		const tx = {
			rollback: () => {},
			execute: async (query: { strings?: unknown[] }) => {
				executed.push(query);
				if (isXactCheck(query)) return [{ xid: "1", pid: 1 }];
				return [{ id: "r" }]; // postgres.js returns a row array
			},
		};
		expect(await drizzleTx(tx, fakeSql).query("SELECT $2, '$1', $1", ["a", undefined])).toEqual({
			rows: [{ id: "r" }],
		});
		// The wrap-time baseline check, this query's own check, then the query itself.
		expect(executed).toEqual([
			{
				strings: ["SELECT pg_current_xact_id()::text AS xid, pg_backend_pid() AS pid"],
				params: [],
			},
			{
				strings: ["SELECT pg_current_xact_id()::text AS xid, pg_backend_pid() AS pid"],
				params: [],
			},
			{ strings: ["SELECT ", ", '$1', ", ""], params: [{ param: null }, { param: "a" }] },
		]);
	});

	it("rejects the database object and a missing sql tag", () => {
		expect(() => drizzleTx({ execute: async () => [] } as never, fakeSql)).toThrow(
			/not a transaction/
		);
		expect(() =>
			drizzleTx({ execute: async () => [], rollback: () => {} }, undefined as never)
		).toThrow(/sql tag/);
	});

	it("throws once the underlying transaction's identity changes after wrap time", async () => {
		let identity = { xid: "10", pid: 111 };
		const tx = {
			rollback: () => {},
			execute: async (query: { strings?: unknown[] }) => {
				if (isXactCheck(query)) return [{ xid: identity.xid, pid: identity.pid }];
				return [{ id: "r" }];
			},
		};
		const wrapped = drizzleTx(tx, fakeSql);
		// Same identity as at wrap time: passes through.
		expect(await wrapped.query("SELECT 1", [])).toEqual({ rows: [{ id: "r" }] });
		// The transaction ended and the connection got reused elsewhere (a different xid/pid).
		identity = { xid: "99", pid: 222 };
		await expect(wrapped.query("SELECT 1", [])).rejects.toThrow(/used after its transaction ended/);
	});

	it("does not throw when the check query's result shape is unrecognized (degrades gracefully)", async () => {
		const tx = {
			rollback: () => {},
			execute: async (query: { strings?: unknown[] }) => {
				if (isXactCheck(query)) return [{ unexpected: true }];
				return [{ id: "r" }];
			},
		};
		await expect(drizzleTx(tx, fakeSql).query("SELECT 1", [])).resolves.toEqual({
			rows: [{ id: "r" }],
		});
	});
});

describe("prismaTx", () => {
	it("passes $n SQL and values through $queryRawUnsafe", async () => {
		const calls: unknown[][] = [];
		const tx = {
			$queryRawUnsafe: async (...args: unknown[]) => {
				calls.push(args);
				return [{ id: "r" }];
			},
		};
		expect(await prismaTx(tx).query("SELECT $1, $2", [1, undefined])).toEqual({
			rows: [{ id: "r" }],
		});
		expect(calls).toEqual([["SELECT $1, $2", 1, null]]);
	});

	it("rejects the PrismaClient itself and results that are not rows", async () => {
		const client = { $connect: async () => {}, $queryRawUnsafe: async () => [] };
		expect(() => prismaTx(client)).toThrow(/not an interactive transaction/);
		await expect(prismaTx({ $queryRawUnsafe: async () => 3 }).query("SELECT 1")).rejects.toThrow(
			/instead of rows/
		);
	});
});

describe("trigger options.tx with an unwrapped ORM handle", () => {
	it.each([
		["typeorm", { query: () => {}, queryRunner: undefined }],
		["typeorm", { query: () => {}, startTransaction: () => {} }],
		["knex", Object.assign(() => {}, { raw: () => {} })],
		["drizzle", { execute: () => {}, select: () => {}, query: {} }],
		["prisma", { $queryRawUnsafe: () => {} }],
	])("points at @rivia-labs/minitrigger/%s", (subpath, handle) => {
		expect(() => validateTransactionClient(handle)).toThrow(
			new RegExp(`@rivia-labs/minitrigger/${subpath}`)
		);
	});

	it("still accepts a node-postgres client and an adapter", () => {
		const client = new Client({ connectionString: "postgres://localhost/does-not-matter" });
		expect(validateTransactionClient(client)).toBe(client);
	});
});
