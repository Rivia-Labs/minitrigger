import { randomUUID } from "node:crypto";
import { beforeAll, describe, expect, it } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlContext, type SqlContext } from "../../src/store/sql/db.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import type { Ownership, Store } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let ctx: SqlContext;
let store: Store;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	ctx = createSqlContext(ns.schema);
	store = createSqlStore(ns.pool, ns.schema);
});

/** Inserts a `running` run directly (bypassing RunStore, owned by another concurrent task). */
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

describe("createSqlStore", () => {
	it("rejects an invalid schema name", () => {
		expect(() => createSqlStore(ns.pool, "not-valid!")).toThrow();
	});
});

describe("Store.withTransaction", () => {
	it("commits: writes made through tx are visible afterwards via the pool-bound store", async () => {
		const owner = await insertRunningRun();

		await store.withTransaction(async tx => {
			const result = await tx.steps.insertCompleted(owner, {
				name: "step-a",
				seqIndex: 1,
				output: { ok: true },
			});
			expect(result).toBe("ok");
		});

		const records = await store.steps.list(owner.runId);
		expect(records).toHaveLength(1);
		expect(records[0]).toMatchObject({ name: "step-a", output: { ok: true } });
	});

	it("rolls back: a write followed by a throw leaves nothing behind", async () => {
		const owner = await insertRunningRun();

		await expect(
			store.withTransaction(async tx => {
				await tx.steps.insertCompleted(owner, { name: "step-a", seqIndex: 1, output: null });
				throw new Error("boom");
			})
		).rejects.toThrow("boom");

		expect(await store.steps.list(owner.runId)).toEqual([]);
	});

	it("multiple writes inside one transaction are all-or-nothing", async () => {
		const owner = await insertRunningRun();

		await expect(
			store.withTransaction(async tx => {
				await tx.steps.insertCompleted(owner, { name: "a", seqIndex: 1, output: null });
				await tx.steps.insertCompleted(owner, { name: "b", seqIndex: 2, output: null });
				await tx.logs.insertBatch([
					{ runId: owner.runId, level: "info", message: "hi", data: null },
				]);
				throw new Error("boom");
			})
		).rejects.toThrow("boom");

		expect(await store.steps.list(owner.runId)).toEqual([]);
		expect(await store.logs.list(owner.runId, { limit: 10 })).toEqual([]);
	});

	it("propagates the callback's error unchanged", async () => {
		class CustomError extends Error {}
		await expect(
			store.withTransaction(async () => {
				throw new CustomError("custom failure");
			})
		).rejects.toBeInstanceOf(CustomError);
	});

	// pg-pool emits "remove" exactly when a client is taken out of the pool for good (via
	// `client.release(err)` -> `_remove`), and never for a plain successful release (which emits
	// only "release" and keeps the client idle). Watching for it is a deterministic way to prove
	// the client was destroyed, unlike comparing `totalCount`/`idleCount` snapshots -- those also
	// shift when `connect()` happens to reuse an already-idle client left over from an earlier
	// test, making a before/after comparison flaky.
	async function expectClientDestroyed(fn: () => Promise<unknown>): Promise<void> {
		// `client.end()` (called by pg-pool's `_remove`) closes the socket asynchronously, so
		// "remove" can fire a tick or two after `fn()`'s promise has already settled -- wait for
		// the event itself instead of polling a flag set synchronously after `await fn()`.
		const removed = new Promise<void>(resolve => {
			ns.pool.once("remove", () => resolve());
		});
		await fn();
		await Promise.race([
			removed,
			new Promise<never>((_resolve, reject) => {
				setTimeout(() => reject(new Error("expected the pool to remove a client")), 2000);
			}),
		]);
	}

	it("destroys the client on error instead of returning it to the pool", async () => {
		await expectClientDestroyed(() =>
			expect(
				store.withTransaction(async () => {
					throw new Error("boom");
				})
			).rejects.toThrow("boom")
		);

		// The pool itself must still be healthy afterwards.
		await expect(ns.pool.query("SELECT 1")).resolves.toBeDefined();
	});

	it("a query error inside the transaction (e.g. FK violation) also rolls back and destroys the client", async () => {
		await expectClientDestroyed(() =>
			expect(
				store.withTransaction(async tx => {
					// No matching run for this runId: violates run_logs' FK on run_id, exercising a
					// real Postgres-raised error rather than one thrown by our own code.
					await tx.logs.insertBatch([
						{ runId: randomUUID(), level: "info", message: "orphan", data: null },
					]);
				})
			).rejects.toThrow()
		);
	});
});
