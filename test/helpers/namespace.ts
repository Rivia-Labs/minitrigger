import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { escapeIdentifier, Pool } from "pg";
import { afterAll, beforeAll } from "vitest";
import { TEST_DATABASE_URL, TEST_REDIS_URL } from "./env.js";

// Must stay in sync with the pattern minitrigger validates schema/prefix identifiers against
// (decision 15 in docs/SPEC.md): `^[a-z_][a-z0-9_]{0,62}$`.
const NAMESPACE_PATTERN = /^[a-z_][a-z0-9_]{0,62}$/;

// The epoch-seconds timestamp embedded right after the `mt_test_` prefix lets a sweeper (see
// `global-setup.ts`) tell a namespace's *age* apart just from its name, so it can drop only
// schemas/keys left behind by a crashed *past* run instead of every `mt_test_*` name it sees --
// wiping those unconditionally would race a concurrently-starting vitest process that just
// created its own namespace (or another process's integration run still using theirs).
const NAMESPACE_TIMESTAMP_PATTERN = /^mt_test_(\d+)_/;

/** How old (in seconds) a namespace has to be, by its embedded timestamp, before a sweep drops it. */
export const STALE_NAMESPACE_MAX_AGE_SECONDS = 60 * 60;

/** Extracts the epoch-seconds timestamp embedded in a generated namespace/key name, if any. */
export function parseNamespaceEpochSeconds(name: string): number | undefined {
	const match = NAMESPACE_TIMESTAMP_PATTERN.exec(name);
	const raw = match?.[1];
	if (raw === undefined) return undefined;
	const epochSeconds = Number(raw);
	return Number.isFinite(epochSeconds) ? epochSeconds : undefined;
}

/**
 * True only when `name` carries an embedded timestamp *and* that timestamp is older than
 * {@link STALE_NAMESPACE_MAX_AGE_SECONDS}. A name whose age can't be determined is treated as
 * *not* stale (the safe default for a sweep): better to leave a leftover behind for a human to
 * clean up than to delete a namespace a concurrent process might still be using.
 */
export function isStaleNamespace(
	name: string,
	nowSeconds = Math.floor(Date.now() / 1000)
): boolean {
	const epochSeconds = parseNamespaceEpochSeconds(name);
	if (epochSeconds === undefined) return false;
	return nowSeconds - epochSeconds > STALE_NAMESPACE_MAX_AGE_SECONDS;
}

export interface TestNamespace {
	readonly schema: string;
	readonly prefix: string;
	readonly pool: Pool;
	readonly redis: Redis;
	readonly databaseUrl: string;
	readonly redisUrl: string;
}

export interface WaitForOptions {
	timeoutMs?: number;
	intervalMs?: number;
}

/**
 * Registers `beforeAll`/`afterAll` hooks that give the calling test file its own Postgres schema
 * and Redis key prefix, so files running in parallel never see each other's data. Call this once
 * at the top of a test file (module scope), then read the returned getters from inside tests.
 *
 * ```ts
 * const ns = useTestNamespace();
 *
 * it("does something", async () => {
 *   await ns.pool.query(`CREATE TABLE ${escapeIdentifier(ns.schema)}.items (id int)`);
 *   await ns.redis.set(`${ns.prefix}:marker`, "1");
 * });
 * ```
 */
export function useTestNamespace(): TestNamespace {
	// Embed the creation time (epoch seconds) right after the fixed prefix so a later sweep
	// (`global-setup.ts`'s `cleanupStaleSchemas`/`cleanupStaleNamespaceKeys`) can tell how old a
	// leftover namespace is from its name alone -- see `isStaleNamespace`.
	const epochSeconds = Math.floor(Date.now() / 1000);
	const random = randomUUID().replaceAll("-", "_");
	const schema = `mt_test_${epochSeconds}_${random}`;
	const prefix = schema;

	if (!NAMESPACE_PATTERN.test(schema)) {
		// Unreachable in practice (a UUID only yields hex digits and dashes, and epoch seconds
		// are plain digits), but guard the contract explicitly rather than let an invalid
		// identifier reach `escapeIdentifier`.
		throw new Error(
			`generated test namespace "${schema}" does not match ${NAMESPACE_PATTERN.source}`
		);
	}

	let pool: Pool | undefined;
	let redis: Redis | undefined;

	beforeAll(async () => {
		pool = new Pool({ connectionString: TEST_DATABASE_URL, max: 4 });
		// Without a listener, an error on an idle pooled client (e.g. the server restarting)
		// emits an unhandled "error" event on the Pool and crashes the process instead of
		// surfacing through whichever query/hook is actually in flight.
		pool.on("error", error => {
			console.error(`[test-namespace] postgres pool error for schema "${schema}":`, error);
		});
		await pool.query(`CREATE SCHEMA ${escapeIdentifier(schema)}`);

		// No `maxRetriesPerRequest: null`: that setting (meant for blocking commands like
		// `XREAD BLOCK`) makes ioredis queue commands indefinitely while it retries the
		// connection forever, which is the opposite of failing fast for this helper's own
		// short-lived client. A bounded `retryStrategy` plus an "error" listener (again, so a
		// connection failure doesn't crash the process via an unhandled "error" event) means a
		// broken Redis surfaces quickly through the `ping()` below instead of hanging.
		redis = new Redis(TEST_REDIS_URL, {
			retryStrategy: times => (times > 5 ? null : Math.min(times * 100, 2_000)),
		});
		redis.on("error", error => {
			console.error(`[test-namespace] redis client error for prefix "${prefix}":`, error);
		});
		await redis.ping();
	});

	afterAll(async () => {
		const errors: unknown[] = [];

		if (pool) {
			try {
				await pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(schema)} CASCADE`);
			} catch (error) {
				errors.push(error);
			}
		}

		if (redis) {
			try {
				await deleteKeysByPrefix(redis, prefix);
			} catch (error) {
				errors.push(error);
			}
			redis.disconnect();
		}

		if (pool) {
			try {
				await pool.end();
			} catch (error) {
				errors.push(error);
			}
		}

		if (errors.length > 0) {
			throw new AggregateError(errors, `failed to tear down test namespace "${schema}"`);
		}
	});

	return {
		get schema() {
			return schema;
		},
		get prefix() {
			return prefix;
		},
		get pool() {
			return requireReady(pool, "pool");
		},
		get redis() {
			return requireReady(redis, "redis");
		},
		get databaseUrl() {
			return TEST_DATABASE_URL;
		},
		get redisUrl() {
			return TEST_REDIS_URL;
		},
	};
}

function requireReady<T>(value: T | undefined, name: string): T {
	if (value === undefined) {
		throw new Error(
			`test namespace "${name}" is only available inside a test or a before/afterAll hook (it is created by beforeAll)`
		);
	}
	return value;
}

/** Deletes every Redis key starting with `prefix` using SCAN + UNLINK in batches. */
export async function deleteKeysByPrefix(
	redis: Redis,
	prefix: string,
	batchSize = 500
): Promise<void> {
	let cursor = "0";
	const pattern = `${prefix}*`;

	do {
		const [nextCursor, keys] = await redis.scan(cursor, "MATCH", pattern, "COUNT", 200);
		cursor = nextCursor;

		for (let i = 0; i < keys.length; i += batchSize) {
			const batch = keys.slice(i, i + batchSize);
			if (batch.length > 0) {
				await redis.unlink(...batch);
			}
		}
	} while (cursor !== "0");
}

/** Polls `fn` until it returns a truthy value, or throws once `timeoutMs` elapses. */
export async function waitFor<T>(
	fn: () => T | Promise<T>,
	options: WaitForOptions = {}
): Promise<T> {
	const timeoutMs = options.timeoutMs ?? 5_000;
	const intervalMs = options.intervalMs ?? 50;
	const deadline = Date.now() + timeoutMs;

	for (;;) {
		const result = await fn();
		if (result) {
			return result;
		}
		if (Date.now() >= deadline) {
			throw new Error(`waitFor: condition not met within ${timeoutMs}ms`);
		}
		await delay(intervalMs);
	}
}

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}
