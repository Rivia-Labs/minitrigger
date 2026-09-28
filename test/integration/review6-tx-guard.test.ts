/**
 * Review 6 item M1: `trigger(..., { tx })` must not need a pool connection. The mixed-mode guard
 * (review 5 B1) read `schema_meta` through the pool even on the `tx` path; with an injected pool
 * whose only connection is the caller's (in `BEGIN`), a cold engine's first `tx` trigger waited
 * for `connectionTimeoutMillis` (forever without one). The guard now reads the marker through the
 * caller's client, with statements that cannot abort the caller's transaction.
 *
 * Item L1: the lifecycle check is repeated after the (awaited) backend load / guard, so a stop()
 * that completes meanwhile fails the write with the engine's lifecycle error, not the raw
 * "Cannot use a pool after calling end".
 */
import { randomUUID } from "node:crypto";
import { escapeIdentifier, Pool, type PoolClient } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { EngineConfig } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { ConfigError, MinitriggerError } from "../../src/errors.js";
import { defineTask } from "../../src/task.js";
import { type TestNamespace, useTestNamespace } from "../helpers/namespace.js";

const nsMatch = useTestNamespace();
const nsForeign = useTestNamespace();
const nsNoMeta = useTestNamespace();
const nsL1 = useTestNamespace();
const nsPriv = useTestNamespace();

const engines: Engine[] = [];
const pools: Pool[] = [];
const quietLogger = { debug() {}, info() {}, warn() {}, error() {} };

afterEach(async () => {
	vi.restoreAllMocks();
	await Promise.all(engines.splice(0).map(e => e.stop().catch(() => undefined)));
	await Promise.all(pools.splice(0).map(p => p.end().catch(() => undefined)));
});

const t = (ns: TestNamespace, table: string) =>
	`${escapeIdentifier(ns.schema)}.${escapeIdentifier(table)}`;

beforeAll(async () => {
	for (const ns of [nsMatch, nsForeign, nsNoMeta, nsL1, nsPriv]) {
		const engine = createEngine({
			transport: "postgres",
			postgres: ns.databaseUrl,
			schema: ns.schema,
			tasks: [],
		});
		await engine.migrate();
		await engine.stop();
	}
	await nsNoMeta.pool.query(`DROP TABLE ${t(nsNoMeta, "schema_meta")}`);
});

const queue = `q${randomUUID().slice(0, 8)}`;
const task = defineTask({ id: "tx-guard-task", queue: { name: queue }, run: async () => "ok" });

type Transport = "bullmq" | "postgres";

function engineFor(transport: Transport, ns: TestNamespace, pool: Pool): Engine {
	const common = { schema: ns.schema, tasks: [task], logger: quietLogger, postgres: { pool } };
	const config: EngineConfig =
		transport === "bullmq"
			? { ...common, redis: ns.redisUrl, prefix: `${ns.prefix}${randomUUID().slice(0, 4)}` }
			: { ...common, transport: "postgres" };
	const engine = createEngine(config);
	engines.push(engine);
	return engine;
}

/** A one-connection pool whose only client the caller holds in `BEGIN`; counts later checkouts. */
async function saturatedPool(ns: TestNamespace) {
	const pool = new Pool({
		connectionString: ns.databaseUrl,
		max: 1,
		connectionTimeoutMillis: 3_000,
	});
	pool.on("error", () => {});
	pools.push(pool);
	const client: PoolClient = await pool.connect();
	await client.query("BEGIN");
	let checkouts = 0;
	const realConnect = pool.connect.bind(pool) as (...a: unknown[]) => unknown;
	(pool as unknown as { connect: (...a: unknown[]) => unknown }).connect = (...args: unknown[]) => {
		checkouts++;
		return realConnect(...args);
	};
	return { pool, client, checkouts: () => checkouts };
}

/** Rejects if `promise` takes longer than `ms` (the pool timeout is 3 s). */
function fast<T>(promise: Promise<T>, ms = 1_500): Promise<T> {
	return Promise.race([
		promise,
		new Promise<T>((_, reject) =>
			setTimeout(() => reject(new Error(`took more than ${ms} ms`)), ms)
		),
	]);
}

async function markTransport(ns: TestNamespace, mode: string): Promise<void> {
	await ns.pool.query(
		`INSERT INTO ${t(ns, "schema_meta")} (key, value) VALUES ('transport', $1)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
		[mode]
	);
}

async function runExists(ns: TestNamespace, id: string): Promise<boolean> {
	const { rows } = await ns.pool.query(`SELECT 1 FROM ${t(ns, "runs")} WHERE id = $1`, [id]);
	return rows.length === 1;
}

describe.each(["bullmq", "postgres"] as const)(
	"review 6 M1 (%s): tx trigger on a saturated pool",
	transport => {
		it("a cold engine's first tx trigger never takes a pool connection (marker matches)", async () => {
			await markTransport(nsMatch, transport);
			const { pool, client, checkouts } = await saturatedPool(nsMatch);
			const engine = engineFor(transport, nsMatch, pool);
			try {
				const { id } = await fast(engine.trigger(task, undefined, { tx: client }));
				// A second one: the memoized result, still no pool.
				const second = await fast(engine.trigger(task, undefined, { tx: client }));
				expect(checkouts()).toBe(0);
				await expect(client.query("SELECT 1 AS one")).resolves.toMatchObject({
					rows: [{ one: 1 }],
				});
				await client.query("COMMIT");
				expect(await runExists(nsMatch, id)).toBe(true);
				expect(await runExists(nsMatch, second.id)).toBe(true);
			} finally {
				client.release();
			}
		});

		it("a diverging marker is a ConfigError and leaves the caller's transaction usable", async () => {
			await markTransport(nsForeign, transport === "bullmq" ? "postgres" : "bullmq");
			const { pool, client, checkouts } = await saturatedPool(nsForeign);
			const engine = engineFor(transport, nsForeign, pool);
			try {
				const error = await fast(engine.trigger(task, undefined, { tx: client })).catch(
					(e: unknown) => e
				);
				expect(error).toBeInstanceOf(ConfigError);
				expect(checkouts()).toBe(0);
				// Not aborted: an aborted transaction would reject this with 25P02.
				await expect(client.query("SELECT 1 AS one")).resolves.toMatchObject({
					rows: [{ one: 1 }],
				});
				const commit = await client.query("COMMIT");
				// COMMIT of an aborted transaction silently reports ROLLBACK.
				expect(commit.command).toBe("COMMIT");
				// Memoized: refused again, without any query.
				await expect(engine.trigger(task, undefined)).rejects.toBeInstanceOf(ConfigError);
			} finally {
				client.release();
			}
		});

		it("no schema_meta table (not migrated to 009) is an absent key, transaction intact", async () => {
			const { pool, client, checkouts } = await saturatedPool(nsNoMeta);
			const engine = engineFor(transport, nsNoMeta, pool);
			try {
				const { id } = await fast(engine.trigger(task, undefined, { tx: client }));
				expect(checkouts()).toBe(0);
				await expect(client.query("SELECT 1 AS one")).resolves.toMatchObject({
					rows: [{ one: 1 }],
				});
				const commit = await client.query("COMMIT");
				expect(commit.command).toBe("COMMIT");
				expect(await runExists(nsNoMeta, id)).toBe(true);
			} finally {
				client.release();
			}
		});
	}
);

describe("review 6 L1: lifecycle re-checked after the awaited guard", () => {
	it("a stop() that completes during the guard read fails the write with the lifecycle error", async () => {
		const engine = createEngine({
			transport: "postgres",
			postgres: nsL1.databaseUrl,
			schema: nsL1.schema,
			tasks: [task],
			logger: quietLogger,
		});
		engines.push(engine);
		let release!: () => void;
		const gate = new Promise<void>(resolve => {
			release = resolve;
		});
		let reached!: () => void;
		const guardReached = new Promise<void>(resolve => {
			reached = resolve;
		});
		const realQuery = Pool.prototype.query;
		vi.spyOn(Pool.prototype, "query").mockImplementation(function (this: Pool, ...args: unknown[]) {
			const result = (realQuery as (...a: unknown[]) => Promise<unknown>).apply(this, args);
			if (typeof args[0] === "string" && args[0].includes("schema_meta")) {
				reached();
				return result.then(async value => {
					await gate;
					return value;
				}) as never;
			}
			return result as never;
		});
		const pending = engine.trigger(task, undefined).catch((e: unknown) => e);
		await guardReached;
		// Let the read itself finish (the client goes back), then stop the engine (ends its pool).
		await new Promise(resolve => setTimeout(resolve, 100));
		await engine.stop();
		release();
		const error = await pending;
		expect(error).toBeInstanceOf(MinitriggerError);
		expect((error as Error).message).toMatch(/stopped/);
		expect((error as Error).message).not.toMatch(/Cannot use a pool/);
	});
});

describe.each(["bullmq", "postgres"] as const)(
	"review 6 fix round 1 (%s): least-privilege tx role",
	transport => {
		const role = `mt_r6_${randomUUID().replaceAll("-", "").slice(0, 12)}`;
		const password = randomUUID();

		beforeAll(async () => {
			const s = escapeIdentifier(nsPriv.schema);
			const r = escapeIdentifier(role);
			await nsPriv.pool.query(`CREATE ROLE ${r} LOGIN PASSWORD '${password}'`);
			await nsPriv.pool.query(`GRANT USAGE ON SCHEMA ${s} TO ${r}`);
			// What the tx path needs (INSERT ... RETURNING), and nothing on schema_meta.
			await nsPriv.pool.query(
				`GRANT SELECT, INSERT, UPDATE ON ${t(nsPriv, "runs")}, ${t(nsPriv, "idempotency_keys")} TO ${r}`
			);
			await markTransport(nsPriv, transport);
		});

		afterAll(async () => {
			const r = escapeIdentifier(role);
			await nsPriv.pool.query(`DROP OWNED BY ${r}`).catch(() => undefined);
			await nsPriv.pool.query(`DROP ROLE IF EXISTS ${r}`).catch(() => undefined);
		});

		it("no SELECT on schema_meta: the tx trigger succeeds and the transaction stays usable", async () => {
			const url = new URL(nsPriv.databaseUrl);
			url.username = role;
			url.password = password;
			const pool = new Pool({ connectionString: url.toString(), max: 1 });
			pool.on("error", () => {});
			pools.push(pool);
			const client = await pool.connect();
			try {
				await client.query("BEGIN");
				// The role really cannot read the marker (checked in a savepoint, not to abort the tx).
				await client.query("SAVEPOINT probe");
				await expect(
					client.query(`SELECT value FROM ${t(nsPriv, "schema_meta")}`)
				).rejects.toMatchObject({ code: "42501" });
				await client.query("ROLLBACK TO SAVEPOINT probe");

				const engine = engineFor(transport, nsPriv, pool);
				const { id } = await fast(engine.trigger(task, undefined, { tx: client }));
				await expect(client.query("SELECT 1 AS one")).resolves.toMatchObject({
					rows: [{ one: 1 }],
				});
				const commit = await client.query("COMMIT");
				expect(commit.command).toBe("COMMIT");
				expect(await runExists(nsPriv, id)).toBe(true);
			} finally {
				client.release();
			}
		});
	}
);
