import type { Pool } from "pg";
import { describe, expect, it } from "vitest";
import { assertPostgresReachable } from "../../src/backends/shared.js";
import { ConfigError, MinitriggerError } from "../../src/errors.js";

/** A pool whose `connect()` never settles, with the given counters. */
function stuckPool(counts: { max?: number; totalCount: number; waitingCount: number }): Pool {
	return {
		options: counts.max === undefined ? {} : { max: counts.max },
		totalCount: counts.totalCount,
		waitingCount: counts.waitingCount,
		connect: () => new Promise(() => {}),
	} as unknown as Pool;
}

async function failure(pool: Pool): Promise<Error> {
	const error = await assertPostgresReachable(pool, 20).then(
		() => undefined,
		(e: unknown) => e
	);
	if (!(error instanceof Error)) throw new Error("expected a rejection");
	return error;
}

describe("assertPostgresReachable: timeout wording", () => {
	it("a full pool (every slot taken) says the pool may be busy, not only unreachable", async () => {
		const error = await failure(stuckPool({ max: 2, totalCount: 2, waitingCount: 0 }));
		expect(error).toBeInstanceOf(MinitriggerError);
		expect(error).not.toBeInstanceOf(ConfigError);
		expect(error.message).toMatch(
			/could not get a connection within 20 ms: Postgres unreachable or every connection of the pool \(max 2\) stayed busy/
		);
	});

	it("queued checkouts also count as busy (default max 10)", async () => {
		const error = await failure(stuckPool({ totalCount: 3, waitingCount: 1 }));
		expect(error.message).toMatch(/every connection of the pool \(max 10\) stayed busy/);
	});

	it("a pool with free slots keeps the plain 'cannot reach Postgres within' wording", async () => {
		const error = await failure(stuckPool({ max: 8, totalCount: 1, waitingCount: 0 }));
		expect(error).toBeInstanceOf(MinitriggerError);
		expect(error.message).toMatch(/^cannot reach Postgres within 20 ms/);
	});
});
