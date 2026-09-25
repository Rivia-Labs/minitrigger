import { Client } from "pg";
import { describe, expect, it } from "vitest";
import { createLeaderElector } from "../../src/leader.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

/**
 * Finds the backend pid currently holding the leader lock `(hashtext(schema), 2)` (decision 16).
 * `objsubid = 2` is how Postgres marks the two-int form of an advisory lock in `pg_locks`
 * (`classid`/`objid` carry the two keys; the two-int overload is what both migrations and the
 * leader use -- see `src/migrations/migrate.ts` and `src/leader.ts`).
 */
async function getLeaderLockPid(schema: string): Promise<number | undefined> {
	const { rows } = await ns.pool.query<{ pid: number }>(
		`SELECT pid FROM pg_locks
		 WHERE locktype = 'advisory' AND classid = hashtext($1) AND objid = 2 AND objsubid = 2
		   AND granted = true`,
		[schema]
	);
	return rows[0]?.pid;
}

describe("createLeaderElector()", () => {
	it("only one of two electors becomes leader for the same schema", async () => {
		const a = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		const b = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		try {
			const aAcquired = await a.tryAcquire();
			const bAcquired = await b.tryAcquire();

			expect(aAcquired).toBe(true);
			expect(bAcquired).toBe(false);
			expect(a.isLeader()).toBe(true);
			expect(b.isLeader()).toBe(false);

			// b didn't win, so it shouldn't be holding an idle connection open either.
			expect(await b.tryAcquire()).toBe(false);
		} finally {
			await a.close();
			await b.close();
		}
	});

	it("release() lets the other elector acquire on its next try", async () => {
		const a = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		const b = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		try {
			expect(await a.tryAcquire()).toBe(true);
			expect(await b.tryAcquire()).toBe(false);

			await a.release();
			expect(a.isLeader()).toBe(false);

			expect(await b.tryAcquire()).toBe(true);
			expect(b.isLeader()).toBe(true);
		} finally {
			await a.close();
			await b.close();
		}
	});

	it("detects loss when the leader's backend is terminated, and can reacquire with a new client", async () => {
		let losses = 0;
		const elector = createLeaderElector({
			connectionString: ns.databaseUrl,
			schema: ns.schema,
			onLose: () => {
				losses += 1;
			},
		});
		try {
			expect(await elector.tryAcquire()).toBe(true);

			const pid = await getLeaderLockPid(ns.schema);
			expect(pid).toBeDefined();
			await ns.pool.query("SELECT pg_terminate_backend($1)", [pid]);

			// waitFor polls until its callback returns something truthy, so a plain `!isLeader()`
			// wouldn't work here: `false` (the condition we're waiting for) is itself falsy.
			await waitFor(() => (elector.isLeader() ? undefined : true), { timeoutMs: 5_000 });
			expect(losses).toBe(1);

			// Session-scoped advisory locks are released when the backend that held them is
			// terminated, so the lock is free again and a fresh client can reacquire it.
			expect(await elector.tryAcquire()).toBe(true);
			expect(elector.isLeader()).toBe(true);
		} finally {
			await elector.close();
		}
	});

	it("does not block a migrate()-style pg_advisory_xact_lock(hashtext(schema), 1) in a transaction", async () => {
		const elector = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		const migrator = new Client({ connectionString: ns.databaseUrl });
		await migrator.connect();
		try {
			expect(await elector.tryAcquire()).toBe(true);

			const raced = await Promise.race([
				migrator
					.query("BEGIN")
					.then(() => migrator.query("SELECT pg_advisory_xact_lock(hashtext($1), 1)", [ns.schema]))
					.then(() => migrator.query("COMMIT"))
					.then(() => ({ timedOut: false as const })),
				delay(5_000).then(() => ({ timedOut: true as const })),
			]);

			expect(raced.timedOut).toBe(false);
		} finally {
			await migrator.query("ROLLBACK").catch(() => {});
			await migrator.end();
			await elector.close();
		}
	});

	it("gives independent leadership to different schemas", async () => {
		const otherSchema = `${ns.schema}_other`;
		const a = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		const b = createLeaderElector({ connectionString: ns.databaseUrl, schema: otherSchema });
		try {
			expect(await a.tryAcquire()).toBe(true);
			expect(await b.tryAcquire()).toBe(true);
			expect(a.isLeader()).toBe(true);
			expect(b.isLeader()).toBe(true);
		} finally {
			await a.close();
			await b.close();
		}
	});

	it("close() is idempotent and releases the lock", async () => {
		const elector = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		expect(await elector.tryAcquire()).toBe(true);

		await elector.close();
		await elector.close(); // must not throw

		const other = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		try {
			expect(await other.tryAcquire()).toBe(true);
		} finally {
			await other.close();
		}
	});

	it("close() before ever acquiring is a no-op", async () => {
		const a = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		expect(await a.tryAcquire()).toBe(true);

		const b = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		await b.close();
		await b.close();

		expect(await b.tryAcquire()).toBe(false);
		await a.close();
	});

	it("supports clientFactory as an alternative to connectionString", async () => {
		const elector = createLeaderElector({
			clientFactory: () => new Client({ connectionString: ns.databaseUrl }),
			schema: ns.schema,
		});
		try {
			expect(await elector.tryAcquire()).toBe(true);
			expect(elector.isLeader()).toBe(true);
		} finally {
			await elector.close();
		}
	});
});

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}
