/**
 * Review 5, item C1: `start()`/`stop()` with the database unreachable under `transport: "postgres"`
 * (the BullMQ-mode equivalent, with Redis unreachable, lives in engine-lifecycle-audit). In this
 * mode Postgres is the only dependency: `start()` has to fail within a bound (a
 * `MinitriggerError`, never hanging), `stop()` has to resolve within a bound, and an engine whose
 * `start()` failed has to be startable again once the database is back.
 *
 * The engine reaches Postgres through a TCP proxy the test controls: `blackhole` accepts and never
 * answers (the worst case: only `connectionTimeoutMillis` bounds a connect), `refuse` drops every
 * connection at once, `up` forwards to the test database.
 */
import { randomUUID } from "node:crypto";
import net from "node:net";
import pg from "pg";
import { afterEach, describe, expect, it } from "vitest";
import { createEngine, type Engine } from "../../src/engine.js";
import { ConfigError, MinitriggerError } from "../../src/errors.js";
import { defineTask } from "../../src/task.js";
import {
	describeTransports,
	type EngineOverrides,
	engineConfigFor,
} from "../helpers/engine-factory.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

type ProxyMode = "up" | "blackhole" | "refuse" | "hold";

interface PgProxy {
	readonly url: string;
	setMode(mode: ProxyMode): void;
	/** Forwards every connection accepted in `hold` mode (a connect that answers late). */
	releaseHeld(): void;
	/** Destroys every open connection (both sides), whatever the mode. */
	dropAll(): void;
	close(): Promise<void>;
}

async function pgProxy(databaseUrl: string, initial: ProxyMode): Promise<PgProxy> {
	const target = new URL(databaseUrl);
	const sockets = new Set<net.Socket>();
	let mode: ProxyMode = initial;
	const held: net.Socket[] = [];
	const forward = (client: net.Socket) => {
		const upstream = net.connect(Number(target.port || 5432), target.hostname);
		track(upstream);
		client.pipe(upstream).pipe(client);
		client.on("close", () => upstream.destroy());
		upstream.on("close", () => client.destroy());
	};
	const track = (socket: net.Socket) => {
		sockets.add(socket);
		socket.on("close", () => sockets.delete(socket));
		socket.on("error", () => socket.destroy());
	};
	const server = net.createServer(client => {
		track(client);
		if (mode === "refuse") {
			client.destroy();
			return;
		}
		if (mode === "blackhole") return;
		if (mode === "hold") {
			held.push(client);
			return;
		}
		forward(client);
	});
	await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
	const { port } = server.address() as net.AddressInfo;
	const url = new URL(databaseUrl);
	url.hostname = "127.0.0.1";
	url.port = String(port);
	let closed = false;
	return {
		url: url.toString(),
		setMode(next) {
			mode = next;
		},
		releaseHeld() {
			for (const client of held.splice(0)) if (!client.destroyed) forward(client);
		},
		dropAll() {
			for (const socket of sockets) socket.destroy();
		},
		async close() {
			if (closed) return;
			closed = true;
			const done = new Promise<void>(resolve => server.close(() => resolve()));
			for (const socket of sockets) socket.destroy();
			await done;
		},
	};
}

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

/** Races `promise` against `limitMs`: "hung" when it did not settle in time. */
async function settleWithin<T>(
	promise: Promise<T>,
	limitMs: number
): Promise<
	{ kind: "resolved"; value: T } | { kind: "rejected"; error: unknown } | { kind: "hung" }
> {
	return Promise.race([
		promise.then(
			value => ({ kind: "resolved" as const, value }),
			(error: unknown) => ({ kind: "rejected" as const, error })
		),
		sleep(limitMs).then(() => ({ kind: "hung" as const })),
	]);
}

describe("transport postgres with the database unreachable (review 5, C1)", () => {
	const ns = useTestNamespace();
	const engines: Engine[] = [];
	const proxies: PgProxy[] = [];
	const shortId = () => randomUUID().slice(0, 8);

	afterEach(async () => {
		for (const proxy of proxies) proxy.setMode("up");
		await Promise.all(engines.splice(0).map(e => e.stop().catch(() => {})));
		await Promise.all(proxies.splice(0).map(p => p.close()));
	});

	function makeTask() {
		return defineTask({
			id: `down-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (payload: { v: number }) => payload.v * 2,
		});
	}

	async function setup(initial: ProxyMode, overrides: EngineOverrides = {}) {
		const task = makeTask();
		// Migrated straight against the database, so only the engine's own connections go through
		// the proxy.
		const migrator = createEngine(engineConfigFor("postgres", ns, { tasks: [task] }));
		await migrator.migrate();
		await migrator.stop();
		const proxy = await pgProxy(ns.databaseUrl, initial);
		proxies.push(proxy);
		const engine = createEngine(
			engineConfigFor("postgres", ns, {
				postgres: proxy.url,
				tasks: [task],
				pool: { max: 8, connectionTimeoutMs: 1_000 },
				heartbeatIntervalMs: 200,
				heartbeatTimeoutMs: 1_000,
				stallTimeoutMs: 5_000,
				shutdownTimeoutMs: 1_000,
				reconciler: { intervalMs: 60_000, graceMs: 30_000, batchSize: 50 },
				...overrides,
			})
		);
		engines.push(engine);
		return { task, proxy, engine };
	}

	for (const mode of ["blackhole", "refuse"] as const) {
		it(`start() fails within a bound (${mode}), stop() resolves, and the engine can start again`, async () => {
			const { task, proxy, engine } = await setup(mode);

			const t0 = Date.now();
			const outcome = await settleWithin(engine.start(), 20_000);
			const startMs = Date.now() - t0;
			expect(outcome.kind).toBe("rejected");
			if (outcome.kind === "rejected") {
				expect(outcome.error).toBeInstanceOf(MinitriggerError);
				// Decision 28(h): transient infrastructure, never ConfigError (a MinitriggerError subclass).
				expect(outcome.error).not.toBeInstanceOf(ConfigError);
			}
			expect(startMs).toBeLessThan(15_000);

			// Back up: the same engine starts (nothing half-started blocks it) and runs a task.
			proxy.setMode("up");
			const restarted = await settleWithin(engine.start(), 20_000);
			expect(restarted.kind).toBe("resolved");
			const handle = await engine.trigger(task, { v: 21 });
			await waitFor(async () => (await engine.runs.get(handle.id))?.status === "completed", {
				timeoutMs: 10_000,
			});
			expect((await engine.runs.get(handle.id))?.output).toBe(42);

			proxy.setMode(mode);
			proxy.dropAll();
			const t1 = Date.now();
			const stopped = await settleWithin(engine.stop(), 20_000);
			expect(stopped.kind).toBe("resolved");
			expect(Date.now() - t1).toBeLessThan(15_000);
		}, 60_000);

		it(`stop() after a failed start() resolves within a bound (${mode})`, async () => {
			const { engine } = await setup(mode);
			const outcome = await settleWithin(engine.start(), 20_000);
			expect(outcome.kind).toBe("rejected");
			const t0 = Date.now();
			const stopped = await settleWithin(engine.stop(), 20_000);
			expect(stopped.kind).toBe("resolved");
			expect(Date.now() - t0).toBeLessThan(15_000);
		}, 60_000);
	}

	it("stop() resolves within a bound when the database goes away under a running engine", async () => {
		let started: (() => void) | undefined;
		const running = new Promise<void>(resolve => {
			started = resolve;
		});
		const long = defineTask({
			id: `long-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (_p: null, ctx) => {
				started?.();
				await new Promise<void>(resolve => ctx.signal.addEventListener("abort", () => resolve()));
				return null;
			},
		});
		const { proxy, engine } = await setup("up", { tasks: [long] });
		await engine.start();
		const handle = await engine.trigger(long, null);
		await running;

		proxy.setMode("blackhole");
		proxy.dropAll();
		const t0 = Date.now();
		const stopped = await settleWithin(engine.stop(), 20_000);
		expect(stopped.kind).toBe("resolved");
		expect(Date.now() - t0).toBeLessThan(15_000);

		// The hand-back could not reach the database: the run stays `running` until the stall
		// recovery of a live engine gives it back (at-least-once, decision 20).
		const { rows } = await ns.pool.query<{ status: string }>(
			`SELECT status FROM "${ns.schema}".runs WHERE id = $1`,
			[handle.id]
		);
		expect(rows[0]?.status).toBe("running");
	}, 60_000);
});

/**
 * Leftovers items 2 and 3 (post-merge of the Postgres transport): in BOTH transports, `start()`
 * with Postgres unreachable fails with a `MinitriggerError` (transient infrastructure, the driver
 * error as `cause`), never the raw `pg` error nor `ConfigError`; and the check is bounded by
 * `pool.connectionTimeoutMs` even for an injected pool without `connectionTimeoutMillis` (a host
 * that never answers would otherwise hang `start()` until the OS TCP timeout).
 */
describeTransports("start() with Postgres unreachable (leftovers 2/3)", transport => {
	const ns = useTestNamespace();
	const engines: Engine[] = [];
	const proxies: PgProxy[] = [];
	const pools: pg.Pool[] = [];
	const shortId = () => randomUUID().slice(0, 8);

	afterEach(async () => {
		await Promise.all(engines.splice(0).map(e => e.stop().catch(() => {})));
		await Promise.all(proxies.splice(0).map(p => p.close()));
		await Promise.all(pools.splice(0).map(p => p.end().catch(() => {})));
	});

	async function setup(initial: ProxyMode, injectPool: boolean) {
		const task = defineTask({
			id: `down-${shortId()}`,
			queue: { name: `q${shortId()}` },
			run: async (payload: { v: number }) => payload.v * 2,
		});
		const migrator = createEngine(engineConfigFor(transport, ns, { tasks: [task] }));
		await migrator.migrate();
		await migrator.stop();
		const proxy = await pgProxy(ns.databaseUrl, initial);
		proxies.push(proxy);
		let postgres: string | { pool: pg.Pool } = proxy.url;
		if (injectPool) {
			// Deliberately no `connectionTimeoutMillis`: only the engine's own timer bounds it.
			const pool = new pg.Pool({ connectionString: proxy.url, max: 8 });
			pool.on("error", () => {});
			pools.push(pool);
			postgres = { pool };
		}
		const engine = createEngine(
			engineConfigFor(transport, ns, {
				postgres,
				tasks: [task],
				pool: { max: 8, connectionTimeoutMs: 1_000 },
				shutdownTimeoutMs: 1_000,
				reconciler: { intervalMs: 60_000, graceMs: 30_000, batchSize: 50 },
			})
		);
		engines.push(engine);
		return { task, proxy, engine, pool: pools[pools.length - 1] };
	}

	it("refused connections: MinitriggerError with the driver error as cause, then starts again", async () => {
		const { task, proxy, engine } = await setup("refuse", false);
		const outcome = await settleWithin(engine.start(), 20_000);
		expect(outcome.kind).toBe("rejected");
		if (outcome.kind !== "rejected") return;
		expect(outcome.error).toBeInstanceOf(MinitriggerError);
		expect(outcome.error).not.toBeInstanceOf(ConfigError);
		expect((outcome.error as Error).message).toMatch(/cannot reach Postgres/);
		expect((outcome.error as Error).cause).toBeInstanceOf(Error);

		proxy.setMode("up");
		expect((await settleWithin(engine.start(), 20_000)).kind).toBe("resolved");
		const handle = await engine.trigger(task, { v: 21 });
		await waitFor(async () => (await engine.runs.get(handle.id))?.status === "completed", {
			timeoutMs: 10_000,
		});
	}, 60_000);

	it("injected pool without connectionTimeoutMillis, host never answers: bounded MinitriggerError", async () => {
		const { task, proxy, engine } = await setup("blackhole", true);
		const t0 = Date.now();
		const outcome = await settleWithin(engine.start(), 20_000);
		expect(outcome.kind).toBe("rejected");
		expect(Date.now() - t0).toBeLessThan(5_000);
		if (outcome.kind !== "rejected") return;
		expect(outcome.error).toBeInstanceOf(MinitriggerError);
		expect(outcome.error).not.toBeInstanceOf(ConfigError);
		expect((outcome.error as Error).message).toMatch(/cannot reach Postgres within 1000 ms/);

		// Restartable: a hung connect left behind does not block the next start().
		proxy.setMode("up");
		expect((await settleWithin(engine.start(), 20_000)).kind).toBe("resolved");
		const handle = await engine.trigger(task, { v: 21 });
		await waitFor(async () => (await engine.runs.get(handle.id))?.status === "completed", {
			timeoutMs: 10_000,
		});
	}, 60_000);

	it("a connect that answers after the bound is discarded, leaving nothing in the pool", async () => {
		const { proxy, engine, pool } = await setup("hold", true);
		if (pool === undefined) throw new Error("no pool");
		const outcome = await settleWithin(engine.start(), 20_000);
		expect(outcome.kind).toBe("rejected");
		expect(pool.totalCount).toBeGreaterThan(0);

		// The late connection completes: the probe's client is destroyed, never kept in the pool.
		proxy.setMode("up");
		proxy.releaseHeld();
		await waitFor(() => pool.totalCount === 0, { timeoutMs: 5_000 });
		expect(pool.idleCount).toBe(0);
	}, 60_000);
});
