/**
 * Review 5, item A7: a claim (or `atomic`) that fails with a server-reported statement error
 * (e.g. 57014, statement timeout) whose ROLLBACK succeeded must return its client to the pool;
 * destroying it churned connections under contention. A failed ROLLBACK still destroys it.
 */
import { randomUUID } from "node:crypto";
import { escapeIdentifier, Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { atomic } from "../../src/store/sql/db.js";
import { createSqlStore, type SqlStore } from "../../src/store/sql/index.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let pool: Pool;
let store: SqlStore;
let s: string;
let removed = 0;
/** `remove` fires asynchronously after a destroyed client ends. */
const settle = () => new Promise(resolve => setTimeout(resolve, 100));

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	s = escapeIdentifier(ns.schema);
	pool = new Pool({ connectionString: ns.databaseUrl, max: 2 });
	pool.on("error", () => {});
	pool.on("remove", () => {
		removed++;
	});
	store = createSqlStore(pool, ns.schema);
});

afterAll(async () => {
	await pool.end();
});

describe("client reuse after a statement error (A7)", () => {
	it("a claim that hits its statement timeout returns the client to the pool", async () => {
		const q = `q_${randomUUID().slice(0, 8)}`;
		const holder = await ns.pool.connect();
		try {
			await holder.query("BEGIN");
			await holder.query(`LOCK TABLE ${s}.queue_state IN ACCESS EXCLUSIVE MODE`);
			const deadline = Date.now() + 200;
			const before = removed;
			await expect(
				store.claim.claimRuns({ queue: q, limit: 5, remainingMs: () => deadline - Date.now() })
			).rejects.toMatchObject({ code: "57014" });
			await settle();
			expect(removed - before).toBe(0);
			expect(pool.totalCount).toBeGreaterThan(0);
		} finally {
			await holder.query("ROLLBACK");
			holder.release();
		}
	});

	it("atomic() returns the client after a statement error it rolled back", async () => {
		await settle();
		const before = removed;
		await expect(
			atomic({ kind: "pool", pool }, async db => {
				await db.query("SET LOCAL statement_timeout = 20");
				await db.query("SELECT pg_sleep(1)");
			})
		).rejects.toMatchObject({ code: "57014" });
		await expect(
			atomic({ kind: "pool", pool }, db => db.query("SELECT 1/0"))
		).rejects.toMatchObject({ code: "22012" });
		await settle();
		expect(removed - before).toBe(0);
	});

	it("review 6 L4: a non-server error inside atomic() destroys the client", async () => {
		await settle();
		const before = removed;
		const bug = new Error("thrown by the callback, not by Postgres");
		await expect(
			atomic({ kind: "pool", pool }, async db => {
				await db.query("SELECT 1");
				throw bug;
			})
		).rejects.toBe(bug);
		await settle();
		expect(removed - before).toBe(1);
	});

	it("a connection error still destroys the client", async () => {
		await settle();
		const before = removed;
		await expect(
			atomic({ kind: "pool", pool }, async db => {
				await db.query("SELECT pg_terminate_backend(pg_backend_pid())");
			})
		).rejects.toBeDefined();
		await settle();
		expect(removed - before).toBe(1);
	});
});
