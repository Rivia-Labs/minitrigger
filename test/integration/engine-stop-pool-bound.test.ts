/**
 * External review item (b): `stop()` waited without limit in `pool.end()` for the pool the engine
 * created when a client was still checked out (a leader tick or an execution that survived the
 * bounded waits). `pg`'s `Pool.end()` only resolves once every client is released.
 */
import { Pool, type PoolClient } from "pg";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { createEngine, POOL_END_TIMEOUT_MS } from "../../src/engine.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();

type LogRecord = { level: string; message: string };
const logs: LogRecord[] = [];
const testLogger: EngineLogger = {
	debug: (_f, message) => logs.push({ level: "debug", message }),
	info: (_f, message) => logs.push({ level: "info", message }),
	warn: (_f, message) => logs.push({ level: "warn", message }),
	error: (_f, message) => logs.push({ level: "error", message }),
};

let leaked: PoolClient | undefined;

afterEach(() => {
	vi.restoreAllMocks();
	leaked?.release(true);
	leaked = undefined;
	logs.length = 0;
});

describe("stop() with a client of the engine's own pool never released", () => {
	it("resolves within the pool.end() bound and warns", async () => {
		// Captures the pool the engine creates (postgres given as a connection string).
		const pools: Pool[] = [];
		const originalConnect = Pool.prototype.connect;
		vi.spyOn(Pool.prototype, "connect").mockImplementation(function (this: Pool, ...args) {
			if (!pools.includes(this)) pools.push(this);
			return (originalConnect as (...a: unknown[]) => unknown).apply(this, args) as never;
		});

		const engine = createEngine({
			postgres: ns.databaseUrl,
			redis: ns.redisUrl,
			schema: ns.schema,
			prefix: ns.prefix,
			tasks: [],
			logger: testLogger,
		});
		await engine.migrate();
		const pool = pools[0];
		expect(pool).toBeDefined();
		// A checked-out client nobody releases (what a surviving tick/execution would hold).
		leaked = await (pool as Pool).connect();

		const t0 = Date.now();
		const outcome = await Promise.race([
			engine.stop().then(() => "stopped"),
			new Promise(resolve => setTimeout(() => resolve("hung"), POOL_END_TIMEOUT_MS + 5_000)),
		]);
		expect(outcome).toBe("stopped");
		expect(Date.now() - t0).toBeLessThan(POOL_END_TIMEOUT_MS + 2_000);
		expect(logs.some(l => l.level === "warn" && /pool/.test(l.message))).toBe(true);
	}, 30_000);
});
