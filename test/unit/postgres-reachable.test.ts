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

describe("assertPostgresReachable: driver error wording (review 6 L2)", () => {
	/** Node's happy-eyeballs connect: `localhost` -> ::1 and 127.0.0.1, an empty-message aggregate. */
	function aggregateRefused(): AggregateError {
		const error = new AggregateError(
			[
				Object.assign(new Error("connect ECONNREFUSED ::1:5432"), { code: "ECONNREFUSED" }),
				Object.assign(new Error("connect ECONNREFUSED 127.0.0.1:5432"), { code: "ECONNREFUSED" }),
			],
			""
		);
		return Object.assign(error, { code: "ECONNREFUSED" });
	}

	it("an AggregateError with an empty message names its code and first inner error", async () => {
		const cause = aggregateRefused();
		const pool = {
			options: {},
			totalCount: 0,
			waitingCount: 0,
			connect: () => Promise.reject(cause),
		} as unknown as Pool;
		const error = await assertPostgresReachable(pool, 1_000).then(
			() => undefined,
			(e: unknown) => e as Error
		);
		expect(error).toBeInstanceOf(MinitriggerError);
		expect(error?.message).not.toContain("Postgres ()");
		expect(error?.message).toMatch(
			/^cannot reach Postgres \(ECONNREFUSED: connect ECONNREFUSED ::1:5432\)/
		);
		expect((error as Error & { cause?: unknown }).cause).toBe(cause);
	});

	it("an ordinary error keeps its message", async () => {
		const pool = {
			options: {},
			totalCount: 0,
			waitingCount: 0,
			connect: () => Promise.reject(new Error('password authentication failed for user "x"')),
		} as unknown as Pool;
		const error = await assertPostgresReachable(pool, 1_000).catch((e: unknown) => e as Error);
		expect(error?.message).toMatch(/^cannot reach Postgres \(password authentication failed/);
	});
});
