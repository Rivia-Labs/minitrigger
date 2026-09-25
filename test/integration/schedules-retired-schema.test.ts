/**
 * External review item (c): job schedulers left behind by a RETIRED schema (its prefix owner key
 * deleted, its schema dropped or its schedule row gone) used to fail every tick forever (5 `error`
 * logs per occurrence, nothing removed them). A tick or a reconciler pass that finds a scheduler
 * tagged with another schema now checks that schema in this database: gone (schema/table/row) ->
 * the scheduler is removed; still live -> kept and the tick keeps failing for the owner to pick up.
 */
import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { escapeIdentifier } from "pg";
import { afterEach, describe, expect, it } from "vitest";
import type { EngineConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine } from "../../src/engine.js";
import { migrate } from "../../src/migrations/migrate.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();
const shortId = () => randomUUID().slice(0, 8);
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

type LogRecord = { level: string; message: string; fields: Record<string, unknown> };
const logs: LogRecord[] = [];
const testLogger: EngineLogger = {
	debug: (fields, message) => logs.push({ level: "debug", message, fields }),
	info: (fields, message) => logs.push({ level: "info", message, fields }),
	warn: (fields, message) => logs.push({ level: "warn", message, fields }),
	error: (fields, message) => logs.push({ level: "error", message, fields }),
};

const engines: Engine[] = [];
const cleanups: Array<() => Promise<void>> = [];

function makeEngine(tasks: AnyTask[], overrides: Partial<EngineConfig> = {}): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 8 },
		shutdownTimeoutMs: 1_000,
		reconciler: { intervalMs: 60_000, graceMs: 30_000, batchSize: 50 },
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup().catch(() => {});
	await Promise.all(engines.splice(0).map(e => e.stop().catch(() => {})));
	await ns.redis.del(`${ns.prefix}:owner-schema`);
	logs.length = 0;
});

const task = defineTask({
	id: `retired-${shortId()}`,
	queue: { name: `q${shortId()}` },
	run: async (payload: { v: number }) => payload.v * 2,
});

let schemaCounter = 0;
async function foreignSchema(): Promise<string> {
	schemaCounter += 1;
	const schema = `${ns.schema.slice(0, 50)}_r${schemaCounter}`;
	await ns.pool.query(`CREATE SCHEMA IF NOT EXISTS ${escapeIdentifier(schema)}`);
	cleanups.push(async () => {
		await ns.pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(schema)} CASCADE`);
	});
	await migrate(ns.pool, schema);
	return schema;
}

/** A schedule created by an engine of `schema` (trigger-only: nothing started, nothing claimed). */
async function scheduleIn(schema: string, cron: string): Promise<string> {
	const engine = makeEngine([task], { schema });
	const id = `s-${shortId()}`;
	await engine.schedules.upsert({ id, task, cron, payload: { v: 1 } });
	// Stops a live schedule from ticking into the next tests (no-op once its schema is dropped).
	cleanups.push(async () => {
		await engine.schedules.delete(id);
	});
	return id;
}

async function schedulerExists(id: string): Promise<boolean> {
	const queue = new Queue(`${ns.prefix}-system`, { connection: ns.redis, prefix: ns.prefix });
	try {
		return (await queue.getJobScheduler(id)) !== undefined;
	} finally {
		await queue.close();
	}
}

async function runCount(schema: string): Promise<number> {
	const { rows } = await ns.pool.query<{ n: string }>(
		`SELECT count(*)::text AS n FROM ${escapeIdentifier(schema)}.runs`
	);
	return Number(rows[0]?.n);
}

const removedLog = (id: string) =>
	logs.filter(l => l.level === "warn" && l.fields.scheduleId === id && /retired/.test(l.message));
const errorLogs = (id: string) =>
	logs.filter(l => l.level === "error" && l.fields.scheduleId === id);

describe("ticks of a retired schema", () => {
	it("a dropped schema: its scheduler is removed at the first tick, no endless errors", async () => {
		const retired = await foreignSchema();
		const id = await scheduleIn(retired, "* * * * * *");
		await ns.pool.query(`DROP SCHEMA ${escapeIdentifier(retired)} CASCADE`);

		const engine = makeEngine([task]);
		await engine.migrate();
		await engine.start();

		await waitFor(async () => !(await schedulerExists(id)), { timeoutMs: 10_000 });
		// A couple more seconds of would-be ticks: nothing keeps failing.
		await sleep(2_500);
		expect(errorLogs(id)).toEqual([]);
		expect(removedLog(id)).toHaveLength(1);
		expect(await runCount(ns.schema)).toBe(0);
	}, 30_000);

	it("a schema that still exists but no longer has the row: the scheduler is removed", async () => {
		const retired = await foreignSchema();
		const id = await scheduleIn(retired, "* * * * * *");
		await ns.pool.query(`DELETE FROM ${escapeIdentifier(retired)}.schedules WHERE id = $1`, [id]);

		const engine = makeEngine([task]);
		await engine.migrate();
		await engine.start();

		await waitFor(async () => !(await schedulerExists(id)), { timeoutMs: 10_000 });
		expect(errorLogs(id)).toEqual([]);
		expect(await runCount(ns.schema)).toBe(0);
		expect(await runCount(retired)).toBe(0);
	}, 30_000);

	it("review item 3: the prefix owner key is missing, a retired schema's scheduler is kept (not removed)", async () => {
		const retired = await foreignSchema();
		const id = await scheduleIn(retired, "* * * * * *");
		await ns.pool.query(`DROP SCHEMA ${escapeIdentifier(retired)} CASCADE`);

		const engine = makeEngine([task]);
		await engine.migrate();
		await engine.start();
		// `start()` claims `${prefix}:owner-schema` for this engine's own schema (`claimPrefix`).
		// Deleting it afterwards simulates the key going missing while this engine is still up
		// (e.g. mid blue/green cutover) -- `isPrefixOwner()` can then no longer confirm exclusive
		// ownership of the prefix, so the foreign scheduler must be kept even though its schema
		// really is gone from THIS database.
		await ns.redis.del(`${ns.prefix}:owner-schema`);

		// Without a confirmed exclusive owner, `removeRetiredForeignScheduler` returns "live"
		// without even asking `foreignScheduleState` -- same as a genuinely live foreign schema, the
		// tick keeps refusing (logged `error`, retried by BullMQ) instead of removing anything.
		await waitFor(() => errorLogs(id).some(l => /another schema/.test(l.message)), {
			timeoutMs: 10_000,
		});
		await sleep(1_500);
		expect(await schedulerExists(id)).toBe(true);
		expect(removedLog(id)).toEqual([]);
	}, 30_000);

	it("a live foreign schema (row present): the scheduler is kept, nothing is created", async () => {
		const live = await foreignSchema();
		const id = await scheduleIn(live, "* * * * * *");

		const engine = makeEngine([task]);
		await engine.migrate();
		await engine.start();

		await waitFor(() => errorLogs(id).some(l => /another schema/.test(l.message)), {
			timeoutMs: 10_000,
		});
		expect(await schedulerExists(id)).toBe(true);
		expect(removedLog(id)).toEqual([]);
		expect(await runCount(ns.schema)).toBe(0);
		expect(await runCount(live)).toBe(0);
	}, 30_000);
});

describe("reconciler hook and foreign-tagged schedulers", () => {
	it("removes a retired schema's scheduler, keeps a live one's", async () => {
		// Far-future cron: no tick fires during the test, only the reconciler can act.
		const cron = "0 0 1 1 *";
		const retired = await foreignSchema();
		const retiredId = await scheduleIn(retired, cron);
		await ns.pool.query(`DROP SCHEMA ${escapeIdentifier(retired)} CASCADE`);
		const live = await foreignSchema();
		const liveId = await scheduleIn(live, cron);

		const engine = makeEngine([task], {
			reconciler: { intervalMs: 300, graceMs: 30_000, batchSize: 50 },
		});
		await engine.migrate();
		await engine.start();

		await waitFor(async () => !(await schedulerExists(retiredId)), { timeoutMs: 10_000 });
		// Several more reconciler passes: the live one survives them all.
		await sleep(1_500);
		expect(await schedulerExists(liveId)).toBe(true);
		expect(removedLog(retiredId)).toHaveLength(1);
		expect(removedLog(liveId)).toEqual([]);
	}, 30_000);
});
