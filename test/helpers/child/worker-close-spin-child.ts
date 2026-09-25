/**
 * Child process for `test/integration/worker-close-spin.test.ts`.
 *
 * BullMQ's `Worker.retryIfFailed` retries connection errors forever and skips its delay once the
 * worker is closing, so a job whose `moveToCompleted`/`moveToFailed` fails with a connection error
 * during/after `Worker.close()` spins in a zero-delay loop that starves the event loop (timers never
 * fire). That would hang a vitest worker for good (its own timeouts are timers too), so the
 * scenarios run here, in a plain `node` process the test can kill, and report over stdout:
 * one JSON line per result, `{"event":"probe",...}` after each `stop()` and `{"event":"done"}`.
 *
 * Run with `node --import ./test/helpers/child/register-ts.mjs <this file> <scenario>`, env
 * `SPIN_SCHEMA`, `SPIN_PREFIX`, `SPIN_DATABASE_URL`, `SPIN_REDIS_URL`, `SPIN_ITERATIONS`.
 */
import { randomUUID } from "node:crypto";
import net from "node:net";
import { escapeIdentifier, Pool } from "pg";
import type { EngineConfig, EngineLogger } from "../../../src/config.js";
import { createEngine } from "../../../src/engine.js";
import { type AnyTask, defineTask } from "../../../src/task.js";

const env = (name: string): string => {
	const value = process.env[name];
	if (!value) throw new Error(`missing env ${name}`);
	return value;
};
const schema = env("SPIN_SCHEMA");
const prefix = env("SPIN_PREFIX");
const databaseUrl = env("SPIN_DATABASE_URL");
const redisUrl = env("SPIN_REDIS_URL");
const iterations = Number(process.env["SPIN_ITERATIONS"] ?? "1");

const shortId = () => randomUUID().slice(0, 8);
const emit = (payload: Record<string, unknown>) => {
	process.stdout.write(`${JSON.stringify(payload)}\n`);
};

const silentLogger: EngineLogger = {
	debug: () => {},
	info: () => {},
	warn: () => {},
	error: () => {},
};

/**
 * Event-loop lag over `windowMs`: chained `setTimeout(0)`s, reporting the largest gap. A
 * microtask spin starves timers completely (this never resolves); a partial spin shows as lag.
 */
async function probe(windowMs: number): Promise<{ maxLagMs: number; ticks: number }> {
	const end = Date.now() + windowMs;
	let last = Date.now();
	let maxLagMs = 0;
	let ticks = 0;
	while (Date.now() < end) {
		await new Promise<void>(resolve => setTimeout(resolve, 0));
		const now = Date.now();
		maxLagMs = Math.max(maxLagMs, now - last);
		last = now;
		ticks++;
	}
	return { maxLagMs, ticks };
}

/** A TCP proxy to Redis that can be killed to simulate Redis going away. */
async function redisProxy(): Promise<{ url: string; kill(): Promise<void> }> {
	const target = new URL(redisUrl);
	const sockets = new Set<net.Socket>();
	const server = net.createServer(client => {
		const upstream = net.connect(Number(target.port || 6379), target.hostname);
		sockets.add(client);
		sockets.add(upstream);
		client.pipe(upstream).pipe(client);
		const drop = () => {
			client.destroy();
			upstream.destroy();
		};
		client.on("error", drop);
		upstream.on("error", drop);
		client.on("close", drop);
		upstream.on("close", drop);
	});
	await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
	const address = server.address() as net.AddressInfo;
	let killed = false;
	return {
		url: `redis://127.0.0.1:${address.port}${target.pathname}`,
		async kill() {
			if (killed) return;
			killed = true;
			const closed = new Promise<void>(resolve => server.close(() => resolve()));
			for (const s of sockets) s.destroy();
			await closed;
		},
	};
}

function baseConfig(tasks: AnyTask[], overrides: Partial<EngineConfig>): EngineConfig {
	return {
		postgres: databaseUrl,
		redis: redisUrl,
		schema,
		prefix,
		tasks,
		pool: { max: 8 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		stallTimeoutMs: 5_000,
		shutdownTimeoutMs: 500,
		reconciler: { intervalMs: 60_000, graceMs: 30_000, batchSize: 50 },
		logger: silentLogger,
		...overrides,
	};
}

async function runStatus(pool: Pool, runId: string): Promise<string | undefined> {
	const { rows } = await pool.query<{ status: string }>(
		`SELECT status FROM ${escapeIdentifier(schema)}.runs WHERE id = $1`,
		[runId]
	);
	return rows[0]?.status;
}

/**
 * (a) Redis goes away while a run is active, then `stop()`: the processor returns (aborted) while
 * the workers are closing and BullMQ's move to completed fails with a connection error.
 */
async function redisDown(checkPool: Pool, iteration: number): Promise<void> {
	const proxy = await redisProxy();
	let started: (() => void) | undefined;
	const running = new Promise<void>(resolve => {
		started = resolve;
	});
	const task = defineTask({
		id: `down-${shortId()}`,
		queue: { name: `q${shortId()}` },
		run: async (_p: null, ctx) => {
			started?.();
			await new Promise<void>(resolve => ctx.signal.addEventListener("abort", () => resolve()));
			return null;
		},
	});
	const engine = createEngine(baseConfig([task], { redis: proxy.url }));
	await engine.migrate();
	await engine.start();
	const handle = await engine.trigger(task, null);
	await running;
	await proxy.kill();
	const t0 = Date.now();
	await engine.stop();
	const stopMs = Date.now() - t0;
	const lag = await probe(500);
	emit({
		event: "probe",
		scenario: "redis-down",
		iteration,
		stopMs,
		...lag,
		status: await runStatus(checkPool, handle.id),
	});
}

/**
 * (b) Redis healthy; the execution is still busy (here: its step-snapshot query is held) past
 * `shutdownTimeoutMs` and the in-flight drain, so `stop()` closes every connection first and the
 * processor only returns ~1 s after `stop()` resolved: BullMQ's move then fails with
 * "Connection is closed".
 */
async function lateReturn(checkPool: Pool, iteration: number): Promise<void> {
	let releaseSnapshot: (() => void) | undefined;
	const snapshotHeld = new Promise<void>(resolve => {
		releaseSnapshot = resolve;
	});
	let snapshotReached: (() => void) | undefined;
	const reached = new Promise<void>(resolve => {
		snapshotReached = resolve;
	});
	const pool = new Pool({ connectionString: databaseUrl, max: 20 });
	pool.on("error", () => {});
	const original = pool.query.bind(pool) as (...args: unknown[]) => Promise<unknown>;
	(pool as unknown as { query: (...args: unknown[]) => Promise<unknown> }).query = async (
		...args: unknown[]
	) => {
		const first = args[0];
		const sql = typeof first === "string" ? first : ((first as { text?: string })?.text ?? "");
		if (sql.includes("outputs_deferred")) {
			snapshotReached?.();
			await snapshotHeld;
		}
		return original(...args);
	};

	const task = defineTask({
		id: `late-${shortId()}`,
		queue: { name: `q${shortId()}` },
		run: async (_p: null) => null,
	});
	const engine = createEngine(baseConfig([task], { postgres: { pool } }));
	await engine.migrate();
	await engine.start();
	const handle = await engine.trigger(task, null);
	await reached;
	const t0 = Date.now();
	await engine.stop();
	const stopMs = Date.now() - t0;
	// The processor returns now, after every Redis connection of the engine was closed.
	setTimeout(() => releaseSnapshot?.(), 1_000);
	const lag = await probe(2_500);
	emit({
		event: "probe",
		scenario: "late-return",
		iteration,
		stopMs,
		...lag,
		status: await runStatus(checkPool, handle.id),
	});
	await pool.end();
}

/**
 * (b') Redis healthy; the task ignores `ctx.signal` and only returns ~1 s after `stop()` resolved
 * (at-least-once, decision 20): the run must be back to `queued` and nothing may spin.
 */
async function ignoresSignal(checkPool: Pool, iteration: number): Promise<void> {
	let started: (() => void) | undefined;
	const running = new Promise<void>(resolve => {
		started = resolve;
	});
	let finish: (() => void) | undefined;
	const finished = new Promise<void>(resolve => {
		finish = resolve;
	});
	const task = defineTask({
		id: `ignore-${shortId()}`,
		queue: { name: `q${shortId()}` },
		run: async (_p: null) => {
			started?.();
			await finished;
			return null;
		},
	});
	const engine = createEngine(baseConfig([task], {}));
	await engine.migrate();
	await engine.start();
	const handle = await engine.trigger(task, null);
	await running;
	const t0 = Date.now();
	await engine.stop();
	const stopMs = Date.now() - t0;
	setTimeout(() => finish?.(), 1_000);
	const lag = await probe(2_500);
	emit({
		event: "probe",
		scenario: "ignores-signal",
		iteration,
		stopMs,
		...lag,
		status: await runStatus(checkPool, handle.id),
	});
}

const scenarios = {
	"redis-down": redisDown,
	"late-return": lateReturn,
	"ignores-signal": ignoresSignal,
} as const;

async function main(): Promise<void> {
	const name = process.argv[2] as keyof typeof scenarios;
	const scenario = scenarios[name];
	if (!scenario) throw new Error(`unknown scenario ${String(name)}`);
	const checkPool = new Pool({ connectionString: databaseUrl, max: 2 });
	checkPool.on("error", () => {});
	try {
		for (let i = 0; i < iterations; i++) await scenario(checkPool, i);
	} finally {
		await checkPool.end();
	}
	emit({ event: "done" });
	// Whatever a closed engine still holds (it should hold nothing) must not keep us alive.
	process.exit(0);
}

main().catch(error => {
	emit({ event: "error", message: error instanceof Error ? error.stack : String(error) });
	process.exit(1);
});
