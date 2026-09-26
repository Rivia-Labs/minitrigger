/**
 * Decision 26, ORM adapters: `trigger(task, payload, { tx: <adapter>(handle) })` against the real
 * ORMs (TypeORM, knex, Drizzle node-postgres + postgres.js, Prisma with @prisma/adapter-pg) and a
 * real Postgres. One contract suite runs over every adapter.
 */
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { PrismaPg } from "@prisma/adapter-pg";
import { sql } from "drizzle-orm";
import { drizzle as drizzleNodePg } from "drizzle-orm/node-postgres";
import { drizzle as drizzlePostgresJs } from "drizzle-orm/postgres-js";
import knexFactory, { type Knex } from "knex";
import { escapeIdentifier, Pool } from "pg";
import postgres from "postgres";
import { DataSource } from "typeorm";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { drizzleTx } from "../../src/adapters/drizzle.js";
import { knexTx } from "../../src/adapters/knex.js";
import { prismaTx } from "../../src/adapters/prisma.js";
import type { TransactionClient } from "../../src/adapters/shared.js";
import { typeormTx } from "../../src/adapters/typeorm.js";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { ConfigError } from "../../src/errors.js";
import { migrate } from "../../src/migrations/migrate.js";
import { defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const t = (table: string) => `${escapeIdentifier(ns.schema)}.${table}`;
const root = join(dirname(fileURLToPath(import.meta.url)), "../..");

const logger: EngineLogger = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };

/** Thrown inside an ORM transaction callback to make the ORM roll it back. */
class Rollback extends Error {}

interface TxScope {
	/** The adapter-wrapped handle for `trigger(..., { tx })`. */
	readonly tx: TransactionClient;
	/** The raw ORM transaction handle (for the "pass it unwrapped" error case). */
	readonly raw: unknown;
	/** A business write through the ORM's own API, in the same transaction. */
	insertOrder(id: string): Promise<void>;
	/** `current_setting(name)` read through the ORM's own API, in the same transaction. */
	setting(name: string): Promise<string>;
}

interface AdapterCase {
	readonly name: string;
	setup(): Promise<void>;
	teardown(): Promise<void>;
	/** Runs `fn` in an ORM transaction: committed, or rolled back if `fn` throws. */
	inTx<T>(fn: (scope: TxScope) => Promise<T>): Promise<T>;
	/** Wraps a handle that is NOT in a transaction (must throw ConfigError). */
	wrapOutsideTx(): unknown;
	/** Wraps something that is not this ORM's handle at all (must throw ConfigError). */
	wrapWrongObject(): unknown;
	/** Uses a wrapper after its transaction ended, when the adapter can detect it. */
	useAfterEnd?(): Promise<unknown>;
}

// ---------------------------------------------------------------- TypeORM
let dataSource: DataSource;
const typeormCase: AdapterCase = {
	name: "typeorm",
	async setup() {
		dataSource = new DataSource({ type: "postgres", url: ns.databaseUrl, poolSize: 4 });
		await dataSource.initialize();
	},
	teardown: () => dataSource.destroy(),
	inTx: fn =>
		dataSource.transaction(manager =>
			fn({
				tx: typeormTx(manager),
				raw: manager,
				insertOrder: async id => {
					await manager.query(`INSERT INTO ${t("orders")} (id) VALUES ($1)`, [id]);
				},
				setting: async name =>
					(await manager.query("SELECT current_setting($1) AS v", [name]))[0].v as string,
			})
		),
	wrapOutsideTx: () => typeormTx(dataSource.manager),
	wrapWrongObject: () => typeormTx({} as never),
	async useAfterEnd() {
		const runner = dataSource.createQueryRunner();
		await runner.connect();
		await runner.startTransaction();
		const tx = typeormTx(runner);
		await runner.commitTransaction();
		await runner.release();
		return tx.query("SELECT 1", []);
	},
};

// ---------------------------------------------------------------- knex
let knex: Knex;
const knexCase: AdapterCase = {
	name: "knex",
	async setup() {
		knex = knexFactory({ client: "pg", connection: ns.databaseUrl, pool: { min: 0, max: 4 } });
	},
	teardown: () => knex.destroy(),
	inTx: fn =>
		knex.transaction(trx =>
			fn({
				tx: knexTx(trx),
				raw: trx,
				insertOrder: async id => {
					await trx.raw(`INSERT INTO ${t("orders")} (id) VALUES (?)`, [id]);
				},
				setting: async name =>
					(await trx.raw("SELECT current_setting(?) AS v", [name])).rows[0].v as string,
			})
		),
	wrapOutsideTx: () => knexTx(knex),
	wrapWrongObject: () => knexTx({ raw: 1 } as never),
	async useAfterEnd() {
		const trx = await knex.transaction();
		const tx = knexTx(trx);
		await trx.commit();
		return tx.query("SELECT 1", []);
	},
};

// ---------------------------------------------------------------- Drizzle (node-postgres)
let drizzlePool: Pool;
let drizzlePg: ReturnType<typeof drizzleNodePg>;
const drizzleNodePgCase: AdapterCase = {
	name: "drizzle (node-postgres)",
	async setup() {
		drizzlePool = new Pool({ connectionString: ns.databaseUrl, max: 4 });
		drizzlePg = drizzleNodePg({ client: drizzlePool });
	},
	teardown: () => drizzlePool.end(),
	inTx: fn =>
		drizzlePg.transaction(tx =>
			fn({
				tx: drizzleTx(tx, sql),
				raw: tx,
				insertOrder: async id => {
					await tx.execute(sql`INSERT INTO ${sql.raw(t("orders"))} (id) VALUES (${id})`);
				},
				setting: async name =>
					(await tx.execute(sql`SELECT current_setting(${name}) AS v`)).rows[0]?.v as string,
			})
		),
	wrapOutsideTx: () => drizzleTx(drizzlePg as never, sql),
	wrapWrongObject: () => drizzleTx({} as never, sql),
	async useAfterEnd() {
		let saved!: TransactionClient;
		await drizzlePg.transaction(async tx => {
			saved = drizzleTx(tx, sql);
		});
		return saved.query("SELECT 1", []);
	},
};

// ---------------------------------------------------------------- Drizzle (postgres.js)
let pgJs: postgres.Sql;
let drizzleJs: ReturnType<typeof drizzlePostgresJs>;
const drizzlePostgresJsCase: AdapterCase = {
	name: "drizzle (postgres.js)",
	async setup() {
		pgJs = postgres(ns.databaseUrl, { max: 4, onnotice: () => {} });
		drizzleJs = drizzlePostgresJs({ client: pgJs });
	},
	teardown: () => pgJs.end({ timeout: 5 }),
	inTx: fn =>
		drizzleJs.transaction(tx =>
			fn({
				tx: drizzleTx(tx, sql),
				raw: tx,
				insertOrder: async id => {
					await tx.execute(sql`INSERT INTO ${sql.raw(t("orders"))} (id) VALUES (${id})`);
				},
				setting: async name =>
					(await tx.execute(sql`SELECT current_setting(${name}) AS v`))[0]?.v as string,
			})
		),
	wrapOutsideTx: () => drizzleTx(drizzleJs as never, sql),
	wrapWrongObject: () =>
		drizzleTx({ execute: async () => [], rollback: () => {} }, undefined as never),
	async useAfterEnd() {
		let saved!: TransactionClient;
		await drizzleJs.transaction(async tx => {
			saved = drizzleTx(tx, sql);
		});
		return saved.query("SELECT 1", []);
	},
};

// ---------------------------------------------------------------- Prisma (7.x, @prisma/adapter-pg)
/** The slice of the generated PrismaClient this test uses (the client is generated at run time). */
interface PrismaLike {
	$transaction<T>(fn: (tx: PrismaTxLike) => Promise<T>): Promise<T>;
	$disconnect(): Promise<void>;
	$queryRawUnsafe(query: string, ...values: unknown[]): Promise<unknown>;
}
interface PrismaTxLike {
	$queryRawUnsafe(query: string, ...values: unknown[]): Promise<unknown>;
	$executeRawUnsafe(query: string, ...values: unknown[]): Promise<number>;
}
let prisma: PrismaLike;
const prismaCase: AdapterCase = {
	name: "prisma",
	async setup() {
		// `prisma generate` into the git-ignored dist-test/ (schema: test/fixtures/prisma). Prisma 7's
		// `prisma-client` generator needs no engine download, so this works offline.
		const out = join(root, "dist-test/prisma-client/client.ts");
		execFileSync(
			join(root, "node_modules/.bin/prisma"),
			["generate", "--schema", join(root, "test/fixtures/prisma/schema.prisma")],
			{ cwd: root, stdio: "pipe" }
		);
		if (!existsSync(out)) throw new Error(`prisma generate produced no ${out}`);
		const { PrismaClient } = (await import(pathToFileURL(out).href)) as {
			PrismaClient: new (options: { adapter: unknown }) => PrismaLike;
		};
		prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: ns.databaseUrl }) });
	},
	teardown: () => prisma.$disconnect(),
	inTx: fn =>
		prisma.$transaction(tx =>
			fn({
				tx: prismaTx(tx),
				raw: tx,
				insertOrder: async id => {
					await tx.$executeRawUnsafe(`INSERT INTO ${t("orders")} (id) VALUES ($1)`, id);
				},
				setting: async name =>
					(
						(await tx.$queryRawUnsafe("SELECT current_setting($1) AS v", name)) as { v: string }[]
					)[0]?.v as string,
			})
		),
	wrapOutsideTx: () => prismaTx(prisma),
	wrapWrongObject: () => prismaTx({ query: () => {} } as never),
};

const cases = [typeormCase, knexCase, drizzleNodePgCase, drizzlePostgresJsCase, prismaCase];

// ---------------------------------------------------------------- shared helpers
const engines: Engine[] = [];
let taskSeq = 0;
function recorder() {
	const executions: number[] = [];
	taskSeq++;
	const task = defineTask({
		id: `adapter-rec-${taskSeq}`,
		queue: { name: `adapterq${taskSeq}` },
		run: async (payload: { n: number; tags: string[]; note: string }) => {
			executions.push(payload.n);
			return payload;
		},
	});
	return { task, executions };
}

function makeEngine(tasks: ReturnType<typeof recorder>["task"][]) {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 6 },
		shutdownTimeoutMs: 1_000,
		logger,
	});
	engines.push(engine);
	return engine;
}

async function runRow(id: string) {
	const { rows } = await ns.pool.query<{ status: string; payload: unknown; output: unknown }>(
		`SELECT status, payload, output FROM ${t("runs")} WHERE id = $1`,
		[id]
	);
	return rows[0];
}

async function orderExists(id: string) {
	const { rowCount } = await ns.pool.query(`SELECT 1 FROM ${t("orders")} WHERE id = $1`, [id]);
	return rowCount === 1;
}

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	await ns.pool.query(`CREATE TABLE ${t("orders")} (id text PRIMARY KEY)`);
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

describe.each(cases)("$name adapter", adapter => {
	beforeAll(() => adapter.setup(), 60_000);
	afterAll(() => adapter.teardown());

	const payloadOf = (n: number) => ({ n, tags: ["a?", "b"], note: `it's $1 ? 'x' \\ ${n}` });

	it("commit: the run and the business write commit together; the run executes once", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();
		const order = `${adapter.name}-commit`;

		const handle = await adapter.inTx(async scope => {
			await scope.insertOrder(order);
			const h = await engine.trigger(task, payloadOf(1), { tx: scope.tx });
			expect(await runRow(h.id)).toBeUndefined(); // not visible before the commit
			return h;
		});

		expect(handle.taskId).toBe(task.id);
		await waitFor(async () => (await runRow(handle.id))?.status === "completed", {
			timeoutMs: 5_000,
			intervalMs: 20,
		});
		expect(executions).toEqual([1]);
		// jsonb round trip is exact (no double encoding by the ORM's parameter serializer).
		expect((await runRow(handle.id))?.payload).toEqual(payloadOf(1));
		expect(await orderExists(order)).toBe(true);
	});

	it("rollback: no run, no business write, nothing executes", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();
		const order = `${adapter.name}-rollback`;
		let runId: string | undefined;

		await expect(
			adapter.inTx(async scope => {
				await scope.insertOrder(order);
				runId = (await engine.trigger(task, payloadOf(2), { tx: scope.tx })).id;
				throw new Rollback("roll back");
			})
		).rejects.toBeInstanceOf(Rollback);

		expect(runId).toBeDefined();
		await new Promise(resolve => setTimeout(resolve, 300));
		expect(await runRow(runId as string)).toBeUndefined();
		expect(await orderExists(order)).toBe(false);
		expect(executions).toEqual([]);
	});

	it("idempotency key: a live key returns the existing run and the transaction stays usable", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();
		const key = `k-${adapter.name}`;

		const first = await adapter.inTx(scope =>
			engine.trigger(task, payloadOf(3), { tx: scope.tx, idempotencyKey: key })
		);
		const second = await adapter.inTx(async scope => {
			const h = await engine.trigger(task, payloadOf(4), { tx: scope.tx, idempotencyKey: key });
			// Still usable: no aborted transaction, no savepoint dance.
			await scope.insertOrder(`${adapter.name}-after-idem`);
			return h;
		});

		expect(second.id).toBe(first.id);
		expect((await runRow(first.id))?.payload).toEqual(payloadOf(3));
		expect(await orderExists(`${adapter.name}-after-idem`)).toBe(true);
		await waitFor(async () => (await runRow(first.id))?.status === "completed", {
			timeoutMs: 5_000,
			intervalMs: 20,
		});
		expect(executions).toEqual([3]);
	});

	it("many triggers in one transaction all commit and each executes once", async () => {
		const { task, executions } = recorder();
		const engine = makeEngine([task]);
		await engine.start();

		const handles = await adapter.inTx(async scope => {
			const out = [];
			for (let n = 0; n < 25; n++) {
				out.push(await engine.trigger(task, payloadOf(n), { tx: scope.tx }));
			}
			return out;
		});

		await waitFor(async () => executions.length === 25, { timeoutMs: 10_000, intervalMs: 20 });
		expect([...executions].sort((a, b) => a - b)).toEqual([...Array(25).keys()]);
		for (const h of handles) expect((await runRow(h.id))?.status).toBe("completed");
	});

	it("leaves the caller's session untouched", async () => {
		const { task } = recorder();
		const engine = makeEngine([task]);
		await adapter
			.inTx(async scope => {
				const before = [
					await scope.setting("search_path"),
					await scope.setting("statement_timeout"),
				];
				await engine.trigger(task, payloadOf(5), { tx: scope.tx, idempotencyKey: "session" });
				const after = [
					await scope.setting("search_path"),
					await scope.setting("statement_timeout"),
				];
				expect(after).toEqual(before);
				throw new Rollback();
			})
			.catch(error => {
				if (!(error instanceof Rollback)) throw error;
			});
	});

	it("ConfigError for a handle outside a transaction, a wrong object, or the raw handle", async () => {
		expect(() => adapter.wrapOutsideTx()).toThrow(ConfigError);
		expect(() => adapter.wrapWrongObject()).toThrow(ConfigError);
		if (adapter.useAfterEnd) await expect(adapter.useAfterEnd()).rejects.toThrow(ConfigError);

		const { task } = recorder();
		const engine = makeEngine([task]);
		await adapter
			.inTx(async scope => {
				await expect(
					engine.trigger(task, payloadOf(6), { tx: scope.raw as TransactionClient })
				).rejects.toThrow(/minitrigger\/(typeorm|knex|drizzle|prisma)/);
				throw new Rollback();
			})
			.catch(error => {
				if (!(error instanceof Rollback)) throw error;
			});
	});
});

// ---------------------------------------------------------------- drizzleTx: reused connection
// Reproduces the exact shape of the reported bug (drizzle-orm keeps `tx` queryable after commit,
// and node-postgres/postgres.js just release the pooled client back): with a pool of size 1, a
// SECOND transaction is forced onto the very same physical connection as the first, stale `tx`.
// Without the identity check in drizzleTx, `saved.query()` below would silently run as part of
// that second, unrelated transaction instead of throwing.
describe("drizzleTx: reused after its transaction ended (pool max 1)", () => {
	it("node-postgres: throws instead of running inside a later transaction on the reused connection", async () => {
		const pool = new Pool({ connectionString: ns.databaseUrl, max: 1 });
		const db = drizzleNodePg({ client: pool });
		try {
			let saved!: TransactionClient;
			await db.transaction(async tx => {
				saved = drizzleTx(tx, sql);
				await saved.query("SELECT 1", []);
			});
			await db.transaction(async () => {
				await expect(saved.query("SELECT 1", [])).rejects.toThrow(
					/used after its transaction ended/
				);
			});
		} finally {
			await pool.end();
		}
	});

	it("postgres.js: throws instead of running inside a later transaction on the reused connection", async () => {
		const client = postgres(ns.databaseUrl, { max: 1, onnotice: () => {} });
		const db = drizzlePostgresJs({ client });
		try {
			let saved!: TransactionClient;
			await db.transaction(async tx => {
				saved = drizzleTx(tx, sql);
				await saved.query("SELECT 1", []);
			});
			await db.transaction(async () => {
				await expect(saved.query("SELECT 1", [])).rejects.toThrow(
					/used after its transaction ended/
				);
			});
		} finally {
			await client.end({ timeout: 5 });
		}
	});
});
