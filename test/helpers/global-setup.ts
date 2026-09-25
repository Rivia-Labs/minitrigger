import { Redis } from "ioredis";
import { escapeIdentifier, Pool } from "pg";
import { TEST_DATABASE_URL, TEST_REDIS_URL } from "./env.js";
import { isStaleNamespace } from "./namespace.js";

const INFRA_HINT =
	"Run `pnpm infra:up` to start the local Postgres/Redis containers (see docker-compose.yml).";

// Every per-file namespace (test/helpers/namespace.ts) uses this prefix for both its Postgres
// schema name and its Redis key prefix, so a single pattern finds leftovers from either.
const STALE_NAMESPACE_PREFIX = "mt_test_";

/**
 * Vitest globalSetup for the `integration` project. Runs once, before any test file starts, in
 * its own process. It verifies Postgres and Redis are reachable (failing fast with a hint if
 * not) and sweeps `mt_test_*` schemas/keys left behind by a *crashed* previous run.
 *
 * The sweep only drops a namespace whose embedded timestamp (see `useTestNamespace`,
 * `isStaleNamespace`) is older than `STALE_NAMESPACE_MAX_AGE_SECONDS`: this global setup can
 * itself run concurrently with other `vitest` processes (e.g. `unit` and `integration` projects,
 * or two ad-hoc runs) that are creating their own namespaces at the same moment, and an
 * unconditional "drop every `mt_test_*` name" sweep would delete a schema/keys a concurrent
 * process just created and is about to use, rather than only truly-abandoned ones.
 */
export default async function setup(): Promise<void> {
	await verifyPostgres();
	await verifyRedis();
}

async function verifyPostgres(): Promise<void> {
	const pool = new Pool({
		connectionString: TEST_DATABASE_URL,
		max: 2,
		connectionTimeoutMillis: 5_000,
	});

	try {
		await pool.query("SELECT 1");
		await cleanupStaleSchemas(pool);
	} catch (error) {
		throw new Error(
			`Integration tests could not reach Postgres at ${TEST_DATABASE_URL}. ${INFRA_HINT}`,
			{
				cause: error,
			}
		);
	} finally {
		await pool.end().catch(() => {
			// Best-effort: nothing more we can do if the pool itself is broken.
		});
	}
}

async function cleanupStaleSchemas(pool: Pool): Promise<void> {
	const { rows } = await pool.query<{ nspname: string }>(
		`SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname LIKE 'mt\\_test\\_%' ESCAPE '\\'`
	);

	for (const { nspname } of rows) {
		if (!isStaleNamespace(nspname)) continue;
		await pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(nspname)} CASCADE`);
	}
}

async function verifyRedis(): Promise<void> {
	const redis = new Redis(TEST_REDIS_URL, {
		lazyConnect: true,
		connectTimeout: 5_000,
		maxRetriesPerRequest: 1,
		retryStrategy: () => null,
	});
	// Without a listener, a failed connection attempt emits an unhandled "error" event and
	// crashes the process before we get to report the friendlier message below.
	redis.on("error", () => {});

	try {
		await redis.connect();
		await redis.ping();
		await cleanupStaleNamespaceKeys(redis);
	} catch (error) {
		throw new Error(`Integration tests could not reach Redis at ${TEST_REDIS_URL}. ${INFRA_HINT}`, {
			cause: error,
		});
	} finally {
		redis.disconnect();
	}
}

/** Deletes only the `mt_test_*` keys whose embedded timestamp is older than the stale cutoff. */
async function cleanupStaleNamespaceKeys(redis: Redis, batchSize = 500): Promise<void> {
	let cursor = "0";
	const pattern = `${STALE_NAMESPACE_PREFIX}*`;
	const staleKeys: string[] = [];

	do {
		const [nextCursor, keys] = await redis.scan(cursor, "MATCH", pattern, "COUNT", 200);
		cursor = nextCursor;
		for (const key of keys) {
			if (isStaleNamespace(key)) staleKeys.push(key);
		}
	} while (cursor !== "0");

	for (let i = 0; i < staleKeys.length; i += batchSize) {
		const batch = staleKeys.slice(i, i + batchSize);
		if (batch.length > 0) {
			await redis.unlink(...batch);
		}
	}
}
