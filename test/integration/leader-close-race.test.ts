/**
 * External review item 2: `close()` racing an in-flight `tryAcquire()`. If `close()` runs while
 * the dedicated client is still connecting (or while the lock query is in flight), the attempt
 * must not end up holding the lock after `close()` returned: that lock would stay held for the
 * process lifetime (nobody releases it any more) and the open socket would keep the event loop
 * alive.
 */
import { Client } from "pg";
import { describe, expect, it } from "vitest";
import { createLeaderElector } from "../../src/leader.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();

function gate(): { promise: Promise<void>; open: () => void } {
	let open!: () => void;
	const promise = new Promise<void>(resolve => {
		open = resolve;
	});
	return { promise, open };
}

/**
 * A client whose `connect()` waits on `hold` before really connecting, or whose first `query()`
 * (the lock query) runs for real but only hands its result back after `hold`.
 */
function gatedClient(
	where: "connect" | "query",
	hold: Promise<void>,
	reached: () => void
): { client: Client; ended: () => boolean } {
	const client = new Client({ connectionString: ns.databaseUrl });
	let endedFlag = false;
	client.on("end", () => {
		endedFlag = true;
	});
	if (where === "connect") {
		const original = client.connect.bind(client);
		(client as unknown as { connect: () => Promise<void> }).connect = async () => {
			reached();
			await hold;
			await original();
		};
	} else {
		const original = client.query.bind(client) as (...args: unknown[]) => Promise<unknown>;
		let first = true;
		(client as unknown as { query: (...args: unknown[]) => Promise<unknown> }).query = async (
			...args: unknown[]
		) => {
			if (!first) return original(...args);
			first = false;
			// The lock query really runs (and may take the lock); only its result is held back.
			const result = await original(...args);
			reached();
			await hold;
			return result;
		};
	}
	return { client, ended: () => endedFlag };
}

async function lockHolderCount(schema: string): Promise<number> {
	const { rows } = await ns.pool.query<{ n: number }>(
		`SELECT count(*)::int AS n FROM pg_locks
		 WHERE locktype = 'advisory' AND classid = hashtext($1) AND objid = 2 AND objsubid = 2
		   AND granted = true`,
		[schema]
	);
	return rows[0]?.n ?? 0;
}

describe("leader close() racing an in-flight tryAcquire()", () => {
	for (const where of ["connect", "query"] as const) {
		it(`does not keep the lock nor the client when close() runs during ${where}`, async () => {
			const hold = gate();
			const reachedGate = gate();
			let made: ReturnType<typeof gatedClient> | undefined;
			const elector = createLeaderElector({
				clientFactory: () => {
					made = gatedClient(where, hold.promise, reachedGate.open);
					return made.client;
				},
				schema: ns.schema,
			});

			const attempt = elector.tryAcquire();
			await reachedGate.promise;
			await elector.close();
			hold.open();

			expect(await attempt).toBe(false);
			expect(elector.isLeader()).toBe(false);
			// The client this attempt built was ended, so no socket keeps the process alive.
			expect(made?.ended()).toBe(true);
			expect(await lockHolderCount(ns.schema)).toBe(0);

			const other = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
			try {
				expect(await other.tryAcquire()).toBe(true);
			} finally {
				await other.close();
			}
		});
	}

	// Nit (external review item 5): when `close()` ends the client concurrently with the lock
	// query, `tryAcquire()` used to also try its own unlock/end on that same, already-ending
	// client -- redundant (the session ending on its own already frees the session-scoped
	// advisory lock, docs/research on pg advisory locks) and it logged a spurious "failed to
	// unlock during release (client may already be dead)" warning every time this race happened.
	it("does not log a spurious unlock warning when close() ends the client during the lock query", async () => {
		const hold = gate();
		const reachedGate = gate();
		let made: ReturnType<typeof gatedClient> | undefined;
		const warnings: Array<{ message: string; err: unknown }> = [];
		const elector = createLeaderElector({
			clientFactory: () => {
				made = gatedClient("query", hold.promise, reachedGate.open);
				return made.client;
			},
			schema: ns.schema,
			logger: {
				warn: (fields, message) =>
					warnings.push({ message, err: (fields as { err?: unknown }).err }),
				error: () => {},
			},
		});

		const attempt = elector.tryAcquire();
		await reachedGate.promise;
		// The lock query already resolved for real (`gatedClient`'s "query" mode runs it before
		// holding back the result), so at this point the lock IS held -- exactly the case that used
		// to trigger the redundant unlock attempt below.
		expect(await lockHolderCount(ns.schema)).toBe(1);
		await elector.close();
		hold.open();

		expect(await attempt).toBe(false);
		expect(await lockHolderCount(ns.schema)).toBe(0);
		expect(warnings.filter(w => /unlock/.test(w.message))).toEqual([]);
	});
});
