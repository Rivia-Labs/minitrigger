/**
 * A backend terminated while the SQL store holds a checked-out pool client (inside
 * `withTransaction`/`atomic`) must reject the call, never crash the process: `pg` emits
 * "Connection terminated unexpectedly" as an `error` event on the client when its socket ends, and
 * pg-pool removes its own listener from a checked-out client (found by Task 12: a Postgres-mode
 * engine whose connections were terminated mid-execution raised uncaught exceptions).
 */
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { atomic } from "../../src/store/sql/db.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
const appName = `mt_drop_${randomUUID().slice(0, 8)}`;
let pool: Pool;

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

/** Terminates this file's own pool backends in `state` (never another file's connections). */
async function terminateOwn(state: string): Promise<void> {
	for (let i = 0; i < 100; i++) {
		const { rows } = await ns.pool.query<{ n: number }>(
			`SELECT count(pg_terminate_backend(pid))::int AS n FROM pg_stat_activity
			 WHERE application_name = $1 AND state = $2`,
			[appName, state]
		);
		if ((rows[0]?.n ?? 0) > 0) return;
		await sleep(20);
	}
	throw new Error(`no "${state}" backend of ${appName} to terminate`);
}

/** Records `uncaughtException`s while `fn` runs (plus a grace for the socket `end`). */
async function uncaughtDuring(fn: () => Promise<void>): Promise<unknown[]> {
	const caught: unknown[] = [];
	const onUncaught = (error: unknown) => caught.push(error);
	const previous = process.listeners("uncaughtException");
	process.removeAllListeners("uncaughtException");
	process.on("uncaughtException", onUncaught);
	try {
		await fn();
		await sleep(200);
	} finally {
		process.removeListener("uncaughtException", onUncaught);
		for (const listener of previous) process.on("uncaughtException", listener);
	}
	return caught;
}

beforeAll(() => {
	pool = new Pool({ connectionString: ns.databaseUrl, max: 2, application_name: appName });
	pool.on("error", () => {});
});

afterAll(async () => {
	await pool.end();
});

describe("SQL store: backend terminated while a pool client is checked out", () => {
	it("withTransaction (idle in transaction) rejects without an uncaught error", async () => {
		const store = createSqlStore(pool, ns.schema);
		const caught = await uncaughtDuring(async () => {
			let release!: () => void;
			const hold = new Promise<void>(resolve => {
				release = resolve;
			});
			const tx = store.withTransaction(() => hold);
			await terminateOwn("idle in transaction");
			await sleep(100); // the socket `end` reaches the checked-out client
			release();
			await expect(tx).rejects.toThrow();
		});
		expect(caught).toEqual([]);
		// The pool recovers: a fresh connection replaces the destroyed one.
		await expect(pool.query("SELECT 1 AS ok")).resolves.toMatchObject({ rows: [{ ok: 1 }] });
	});

	it("atomic (a query in flight) rejects without an uncaught error", async () => {
		const caught = await uncaughtDuring(async () => {
			const call = atomic({ kind: "pool", pool }, db => db.query("SELECT pg_sleep(5)"));
			await terminateOwn("active");
			await expect(call).rejects.toThrow();
		});
		expect(caught).toEqual([]);
		await expect(pool.query("SELECT 1 AS ok")).resolves.toMatchObject({ rows: [{ ok: 1 }] });
	});
});
