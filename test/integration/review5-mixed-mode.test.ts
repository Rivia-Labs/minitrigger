/**
 * Review 5 item B1: the mixed-transport guard (decision 28, `schema_meta.transport`) also covers a
 * trigger-only engine (one that never calls `start()`): before its first write that creates or
 * re-enqueues a run, the engine reads the schema's transport once and refuses another mode.
 */
import { randomUUID } from "node:crypto";
import { escapeIdentifier, Pool } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { EngineConfig } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { ConfigError } from "../../src/errors.js";
import { defineTask } from "../../src/task.js";
import { type TestNamespace, useTestNamespace } from "../helpers/namespace.js";

const nsPg = useTestNamespace();
const nsBull = useTestNamespace();
const nsEmpty = useTestNamespace();
/** Rolling upgrade: a 0.4 producer before any 0.4 engine ran migrate() (no `schema_meta` yet). */
const nsNoMeta = useTestNamespace();

const engines: Engine[] = [];
const pools: Pool[] = [];
const quietLogger = { debug() {}, info() {}, warn() {}, error() {} };

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop().catch(() => undefined)));
	await Promise.all(pools.splice(0).map(p => p.end().catch(() => undefined)));
});

const t = (ns: TestNamespace, table: string) =>
	`${escapeIdentifier(ns.schema)}.${escapeIdentifier(table)}`;

beforeAll(async () => {
	for (const ns of [nsPg, nsBull, nsEmpty, nsNoMeta]) {
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
	await nsPg.pool.query(
		`INSERT INTO ${t(nsPg, "schema_meta")} (key, value) VALUES ('transport', 'postgres')`
	);
	await nsBull.pool.query(
		`INSERT INTO ${t(nsBull, "schema_meta")} (key, value) VALUES ('transport', 'bullmq')`
	);
});

const queue = `q${randomUUID().slice(0, 8)}`;
const task = defineTask({ id: "mixed-mode-task", queue: { name: queue }, run: async () => "ok" });

function engineFor(transport: "bullmq" | "postgres", ns: TestNamespace, pool?: Pool): Engine {
	const common = { schema: ns.schema, tasks: [task], logger: quietLogger };
	const postgres = pool ? { pool } : ns.databaseUrl;
	const config: EngineConfig =
		transport === "bullmq"
			? { ...common, postgres, redis: ns.redisUrl, prefix: ns.prefix }
			: { ...common, postgres, transport: "postgres" };
	const engine = createEngine(config);
	engines.push(engine);
	return engine;
}

async function runCount(ns: TestNamespace): Promise<number> {
	const { rows } = await ns.pool.query<{ n: number }>(
		`SELECT count(*)::int AS n FROM ${t(ns, "runs")}`
	);
	return rows[0]?.n ?? 0;
}

describe("review 5 B1: mixed-mode guard on a trigger-only engine", () => {
	it("a BullMQ producer on a Postgres-mode schema is refused before any write", async () => {
		const engine = engineFor("bullmq", nsPg);
		const before = await runCount(nsPg);
		const error = await engine.trigger(task, undefined).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(ConfigError);
		expect((error as Error).message).toMatch(/transport "postgres"/);

		const client = await nsPg.pool.connect();
		try {
			await client.query("BEGIN");
			const txError = await engine
				.trigger(task, undefined, { tx: client })
				.catch((e: unknown) => e);
			expect(txError).toBeInstanceOf(ConfigError);
			// Review 6 L4: the refusal leaves the caller's transaction usable (an aborted one would
			// reject this with 25P02).
			await expect(client.query("SELECT 1 AS one")).resolves.toMatchObject({
				rows: [{ one: 1 }],
			});
			await client.query("ROLLBACK");
		} finally {
			client.release();
		}
		expect(await runCount(nsPg)).toBe(before);

		await expect(engine.tokens.complete(randomUUID(), 1)).rejects.toBeInstanceOf(ConfigError);
		await expect(engine.queues.pause(queue)).rejects.toBeInstanceOf(ConfigError);
		await expect(
			engine.schedules.upsert({ id: "s1", task, cron: "0 * * * *" })
		).rejects.toBeInstanceOf(ConfigError);
		await expect(engine.runs.retryFailed({ limit: 10 })).rejects.toBeInstanceOf(ConfigError);
	});

	it("a Postgres producer on a BullMQ-mode schema is refused before any write", async () => {
		const engine = engineFor("postgres", nsBull);
		const before = await runCount(nsBull);
		const error = await engine.trigger(task, undefined).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(ConfigError);
		expect((error as Error).message).toMatch(/transport "bullmq"/);
		expect(await runCount(nsBull)).toBe(before);
	});

	it("the matching mode writes, reading schema_meta only once per engine", async () => {
		const pool = new Pool({ connectionString: nsPg.databaseUrl, max: 5 });
		pools.push(pool);
		let metaReads = 0;
		const realQuery = pool.query.bind(pool) as (...args: unknown[]) => unknown;
		(pool as unknown as { query: (...args: unknown[]) => unknown }).query = (
			...args: unknown[]
		) => {
			const text = typeof args[0] === "string" ? args[0] : "";
			if (text.includes("schema_meta")) metaReads++;
			return realQuery(...args);
		};
		const engine = engineFor("postgres", nsPg, pool);
		for (let i = 0; i < 3; i++) await engine.trigger(task, undefined);
		expect(metaReads).toBe(1);
	});

	it("no key yet (no engine started): writes are allowed and nothing is recorded", async () => {
		const engine = engineFor("bullmq", nsEmpty);
		await engine.trigger(task, undefined);
		const { rows } = await nsEmpty.pool.query(`SELECT key FROM ${t(nsEmpty, "schema_meta")}`);
		expect(rows).toEqual([]);
		expect(await runCount(nsEmpty)).toBe(1);
	});

	it("review fix 1: no schema_meta table yet (not migrated to 009) is treated as an absent key", async () => {
		const engine = engineFor("bullmq", nsNoMeta);
		await engine.trigger(task, undefined);
		await engine.trigger(task, undefined);
		await expect(engine.tokens.complete(randomUUID(), 1)).resolves.toEqual({
			completed: false,
			reason: "not_found",
		});
		expect(await runCount(nsNoMeta)).toBe(2);
	});

	it("review 6 L4: runs.replay, queues.resume and schedules.delete are guarded too", async () => {
		const producer = engineFor("postgres", nsPg);
		const handle = await producer.trigger(task, undefined);
		const engine = engineFor("bullmq", nsPg);
		const before = await runCount(nsPg);
		// The mixed-mode refusal itself (mixedModeError), not some other ConfigError.
		const mixed = /used by engines with transport "postgres"; mixed transports/;
		for (const call of [
			() => engine.runs.replay(handle.id),
			() => engine.queues.resume(queue),
			() => engine.schedules.delete("s1"),
		]) {
			const error = await call().catch((e: unknown) => e);
			expect(error).toBeInstanceOf(ConfigError);
			expect((error as Error).message).toMatch(mixed);
		}
		expect(await runCount(nsPg)).toBe(before);
	});

	it("review fix 2: runs.cancel and runs.cancelMany are guarded too", async () => {
		const producer = engineFor("postgres", nsPg);
		const handle = await producer.trigger(task, undefined);
		const engine = engineFor("bullmq", nsPg);
		await expect(engine.runs.cancel(handle.id)).rejects.toBeInstanceOf(ConfigError);
		await expect(engine.runs.cancelMany({ limit: 10 })).rejects.toBeInstanceOf(ConfigError);
		expect((await producer.runs.get(handle.id))?.status).toBe("queued");
	});
});
