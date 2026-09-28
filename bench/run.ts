/**
 * `pnpm bench` — SPEC.md §6 / CLAUDE.md "Fase 10" benchmark.
 *
 * Triggers 1,000 runs of a 3-step task against the docker infra (`pnpm infra:up`), on a fresh
 * Postgres schema (and, in `bullmq` mode, a fresh BullMQ prefix) so it never collides with real
 * data, and measures:
 *
 *   1. End-to-end throughput: wall-clock time from firing 1,000 `trigger()` calls to the last one
 *      completing, as runs/s.
 *   2. Sleep resume latency: for a task that does `step.sleep(...)` then one more step, the gap
 *      between the run becoming due (`runAfter`, Postgres time, read right after it suspends) and
 *      it reporting `completed` (also Postgres time, from the `run:completed` event) — i.e. how
 *      long the transport's delayed wake-up plus reacquiring the run plus the trailing step take.
 *   3. Token resume latency: for a task suspended on `waitForToken`, the gap between this
 *      process's own `engine.tokens.complete()` call returning and the `run:completed` event
 *      firing — both read from this process's clock, since the call and the event are both local.
 *
 * `--transport postgres|bullmq` (default `bullmq`, spec §3.10/decision 28) picks which transport
 * config to build: `bullmq` config unchanged (`redis`/`prefix`); `postgres` config carries neither
 * (`transport: "postgres"`, `polling` left at its default) per the design doc's Task 10d ruling.
 * Cleanup also branches: `postgres` mode never touches Redis/BullMQ (no queues were ever created),
 * so it only drops the schema.
 *
 * Prints a summary table, then cleans up: drops the schema and, in `bullmq` mode, obliterates
 * every BullMQ queue it created (including `${prefix}-system`), then deletes the
 * `${prefix}:owner-schema` marker and any other key left under the prefix.
 *
 * Imports from `../dist/index.js` (the **built** package entry), not `../src`: running this file
 * straight off `.ts` sources works (Node >=23.6 strips erasable TS syntax with no flag/deps), but
 * every internal import in `src/` uses a `.js` specifier per the NodeNext convention (see
 * CLAUDE.md) — plain `node` does not remap a literal `foo.js` specifier to an adjacent `foo.ts`
 * file (only extension-less/`.ts` specifiers get that treatment), so importing `../src/index.js`
 * would fail to resolve. The built `dist/` output has real `.js` files matching every specifier,
 * so it works with zero extra tooling. The `bench` script runs `pnpm build` first.
 */
import { randomUUID } from "node:crypto";
import * as os from "node:os";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { escapeIdentifier, Pool } from "pg";
import type { Engine, RunEvent, Task, TaskContext } from "../dist/index.js";
import { createEngine, defineTask } from "../dist/index.js";

type TransportKind = "bullmq" | "postgres";

function parseTransport(argv: readonly string[]): TransportKind {
	const flagIndex = argv.indexOf("--transport");
	const value = flagIndex === -1 ? undefined : argv[flagIndex + 1];
	if (value === undefined || value === "bullmq") return "bullmq";
	if (value === "postgres") return "postgres";
	throw new Error(`--transport must be "bullmq" or "postgres", got ${JSON.stringify(value)}`);
}

const transport = parseTransport(process.argv.slice(2));

const PG_URL =
	process.env["BENCH_DATABASE_URL"] ??
	"postgres://minitrigger:minitrigger@localhost:54329/minitrigger";
const REDIS_URL = process.env["BENCH_REDIS_URL"] ?? "redis://localhost:63799";

const THROUGHPUT_RUNS = 1_000;
const SLEEP_RUNS = 30;
const TOKEN_RUNS = 30;
const SLEEP_DURATION = "1s" as const;
const PHASE_TIMEOUT_MS = 180_000;

// A fresh schema/prefix per invocation (decision 15 identifier pattern: `^[a-z_][a-z0-9_]{0,62}$`)
// so a benchmark run never collides with real data or a concurrent benchmark run, and cleanup
// only ever has to worry about the namespace this run itself created.
const namespace = `mt_bench_${Date.now().toString(36)}${randomUUID().replace(/-/g, "").slice(0, 8)}`;

async function main(): Promise<void> {
	const throughputTask = defineTask({
		id: "bench.throughput",
		queue: { name: "bench-throughput", concurrency: 20 },
		run: async (payload: { n: number }, ctx: TaskContext) => {
			const a = await ctx.step.run("a", () => payload.n + 1);
			const b = await ctx.step.run("b", () => a + 1);
			const c = await ctx.step.run("c", () => b + 1);
			return c;
		},
	});

	const sleepTask = defineTask({
		id: "bench.sleep",
		queue: { name: "bench-sleep", concurrency: 10 },
		run: async (_payload: undefined, ctx: TaskContext) => {
			await ctx.step.sleep("wait", SLEEP_DURATION);
			await ctx.step.run("after", () => "done");
		},
	});

	const tokenTask = defineTask({
		id: "bench.token",
		queue: { name: "bench-token", concurrency: 10 },
		run: async (_payload: undefined, ctx: TaskContext) => {
			const token = await ctx.step.createToken("approve", { timeout: "30s" });
			await ctx.step.waitForToken(token);
		},
	});

	const engine: Engine =
		transport === "postgres"
			? createEngine({
					transport: "postgres",
					postgres: PG_URL,
					schema: namespace,
					tasks: [throughputTask, sleepTask, tokenTask],
					pool: { max: 60 },
					systemConcurrency: 5,
				})
			: createEngine({
					postgres: PG_URL,
					redis: REDIS_URL,
					schema: namespace,
					prefix: namespace,
					tasks: [throughputTask, sleepTask, tokenTask],
					pool: { max: 60 },
					systemConcurrency: 5,
				});

	const results: {
		throughput?: { runs: number; totalMs: number; runsPerSec: number };
		sleepLatencyMs?: number[];
		tokenLatencyMs?: number[];
	} = {};

	try {
		await engine.migrate();
		await engine.start();

		results.throughput = await benchThroughput(engine, throughputTask);
		results.sleepLatencyMs = await benchSleepLatency(engine, sleepTask);
		results.tokenLatencyMs = await benchTokenLatency(engine, tokenTask);
	} finally {
		await engine.stop().catch(err => {
			console.error("[bench] engine.stop() failed", err);
		});
		await cleanup(["bench-throughput", "bench-sleep", "bench-token"]);
	}

	printReport(results);
}

// ---------------------------------------------------------------------------
// Phase 1: end-to-end throughput
// ---------------------------------------------------------------------------

async function benchThroughput(
	engine: Engine,
	task: Task<"bench.throughput", { n: number }, number>
): Promise<{ runs: number; totalMs: number; runsPerSec: number }> {
	let remaining = THROUGHPUT_RUNS;
	let settle!: () => void;
	let fail!: (err: unknown) => void;
	const done = new Promise<void>((resolve, reject) => {
		settle = resolve;
		fail = reject;
	});

	const offCompleted = engine.events.on("run:completed", () => {
		remaining -= 1;
		if (remaining <= 0) settle();
	});
	const offFailed = engine.events.on("run:failed", event => {
		fail(new Error(`bench.throughput run ${event.runId} failed: ${JSON.stringify(event.error)}`));
	});

	try {
		const start = performance.now();
		await Promise.all(
			Array.from({ length: THROUGHPUT_RUNS }, (_, i) => engine.trigger(task, { n: i }))
		);
		await withTimeout(done, PHASE_TIMEOUT_MS, "throughput phase");
		const totalMs = performance.now() - start;
		return {
			runs: THROUGHPUT_RUNS,
			totalMs,
			runsPerSec: THROUGHPUT_RUNS / (totalMs / 1000),
		};
	} finally {
		offCompleted();
		offFailed();
	}
}

// ---------------------------------------------------------------------------
// Phase 2: sleep resume latency (Postgres time both ends)
// ---------------------------------------------------------------------------

async function benchSleepLatency(
	engine: Engine,
	task: Task<"bench.sleep", undefined, void>
): Promise<number[]> {
	const wakeAt = new Map<string, Date>();
	const latencies: number[] = [];
	let remaining = SLEEP_RUNS;
	let settle!: () => void;
	const done = new Promise<void>(resolve => {
		settle = resolve;
	});

	const offWaiting = engine.events.on("run:waiting", (event: RunEvent) => {
		// Fire-and-forget: reads `runAfter` (the wake time) right after the run recorded it. A
		// handful of runs is small enough that this extra `runs.get` per suspend does not affect
		// the sleep-phase timing (it isn't part of what's measured).
		void engine.runs.get(event.runId).then(run => {
			if (run?.runAfter) wakeAt.set(event.runId, run.runAfter);
		});
	});
	const offCompleted = engine.events.on("run:completed", (event: RunEvent) => {
		const wake = wakeAt.get(event.runId);
		if (wake) latencies.push(event.updatedAt.getTime() - wake.getTime());
		remaining -= 1;
		if (remaining <= 0) settle();
	});

	try {
		await Promise.all(Array.from({ length: SLEEP_RUNS }, () => engine.trigger(task)));
		await withTimeout(done, PHASE_TIMEOUT_MS, "sleep-latency phase");
		return latencies;
	} finally {
		offWaiting();
		offCompleted();
	}
}

// ---------------------------------------------------------------------------
// Phase 3: token resume latency (this process's clock both ends)
// ---------------------------------------------------------------------------

async function benchTokenLatency(
	engine: Engine,
	task: Task<"bench.token", undefined, void>
): Promise<number[]> {
	const latencies: number[] = [];

	// Sequential on purpose: each run's clock starts at the `tokens.complete()` call this script
	// makes, so runs are completed one at a time rather than let concurrent completions blur
	// which `run:completed` event belongs to which `complete()` call.
	for (let i = 0; i < TOKEN_RUNS; i++) {
		const handle = await engine.trigger(task);

		const waiting = new Promise<void>(resolve => {
			const off = engine.events.on("run:waiting", event => {
				if (event.runId === handle.id) {
					off();
					resolve();
				}
			});
		});
		await withTimeout(waiting, PHASE_TIMEOUT_MS, "token-latency phase (waiting)");

		const steps = await engine.runs.steps(handle);
		const waitStep = steps.find(s => s.kind === "wait");
		const tokenId = (waitStep?.output as { waitpointId?: string } | null)?.waitpointId;
		if (!tokenId) throw new Error(`bench.token run ${handle.id}: no waitpoint id on its wait step`);

		const completed = new Promise<Date>(resolve => {
			const off = engine.events.on("run:completed", event => {
				if (event.runId === handle.id) {
					off();
					resolve(event.updatedAt);
				}
			});
		});

		const callStart = performance.now();
		const outcome = await engine.tokens.complete(tokenId);
		if (!outcome.completed) {
			throw new Error(
				`bench.token run ${handle.id}: tokens.complete() did not complete (${outcome.reason})`
			);
		}
		await withTimeout(completed, PHASE_TIMEOUT_MS, "token-latency phase (completed)");
		latencies.push(performance.now() - callStart);
	}

	return latencies;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
	return new Promise((resolve, reject) => {
		const timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
		promise.then(
			value => {
				clearTimeout(timer);
				resolve(value);
			},
			err => {
				clearTimeout(timer);
				reject(err);
			}
		);
	});
}

function percentile(sortedAsc: number[], p: number): number {
	if (sortedAsc.length === 0) return Number.NaN;
	const idx = Math.min(sortedAsc.length - 1, Math.ceil((p / 100) * sortedAsc.length) - 1);
	return sortedAsc[Math.max(0, idx)] ?? Number.NaN;
}

function fmtMs(ms: number): string {
	return Number.isFinite(ms) ? `${ms.toFixed(1)} ms` : "n/a";
}

function printTable(headers: string[], rows: string[][]): void {
	const widths = headers.map((h, i) => Math.max(h.length, ...rows.map(r => (r[i] ?? "").length)));
	const line = (cells: string[]) => cells.map((c, i) => c.padEnd(widths[i] ?? 0)).join("  ");
	console.log(line(headers));
	console.log(widths.map(w => "-".repeat(w)).join("  "));
	for (const row of rows) console.log(line(row));
}

function printReport(results: {
	throughput?: { runs: number; totalMs: number; runsPerSec: number };
	sleepLatencyMs?: number[];
	tokenLatencyMs?: number[];
}): void {
	const cpus = os.cpus();
	console.log("\n=== minitrigger bench ===");
	console.log(`date: ${new Date().toISOString()}`);
	console.log(`node: ${process.version}  platform: ${os.platform()}/${os.arch()}`);
	console.log(`cpu: ${cpus[0]?.model ?? "unknown"} x${cpus.length}`);
	console.log(`mem: ${(os.totalmem() / 1024 ** 3).toFixed(1)} GiB`);
	console.log(`transport: ${transport}`);
	console.log(`namespace: ${namespace}\n`);

	if (results.throughput) {
		const { runs, totalMs, runsPerSec } = results.throughput;
		printTable(
			["Phase", "Runs", "Total time", "Throughput"],
			[
				[
					"3-step throughput",
					String(runs),
					`${(totalMs / 1000).toFixed(2)} s`,
					`${runsPerSec.toFixed(1)} runs/s`,
				],
			]
		);
		console.log();
	}

	const latencyRows: string[][] = [];
	for (const [name, values] of [
		["sleep resume (due -> completed)", results.sleepLatencyMs],
		["token resume (complete() -> completed)", results.tokenLatencyMs],
	] as const) {
		if (!values || values.length === 0) continue;
		const sorted = [...values].sort((a, b) => a - b);
		latencyRows.push([
			name,
			String(sorted.length),
			fmtMs(percentile(sorted, 50)),
			fmtMs(percentile(sorted, 95)),
			fmtMs(percentile(sorted, 99)),
			fmtMs(sorted[sorted.length - 1] ?? Number.NaN),
		]);
	}
	if (latencyRows.length > 0) {
		printTable(["Metric", "N", "p50", "p95", "p99", "max"], latencyRows);
	}
	console.log();
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

async function cleanup(taskQueueNames: string[]): Promise<void> {
	const pool = new Pool({ connectionString: PG_URL, max: 2 });
	try {
		await pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(namespace)} CASCADE`);
	} catch (err) {
		console.error("[bench] failed to drop schema", err);
	} finally {
		await pool.end().catch(() => {});
	}

	// `transport: "postgres"` never touched Redis/BullMQ (decision 28: no queues, no prefix), so
	// there is nothing to obliterate — the schema drop above is the whole cleanup.
	if (transport === "postgres") return;

	const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
	try {
		// `${prefix}-system` (decision 8) plus every task queue this bench created.
		const queueNames = [...taskQueueNames, `${namespace}-system`];
		for (const name of queueNames) {
			const queue = new Queue(name, { connection: redis, prefix: namespace });
			try {
				await queue.obliterate({ force: true });
			} catch (err) {
				console.error(`[bench] failed to obliterate queue "${name}"`, err);
			} finally {
				await queue.close();
			}
		}
		// Not a queue key: the prefix-owner marker `claimPrefix()` writes on start() (no TTL), then
		// anything else left under this run's unique prefix.
		try {
			await redis.del(`${namespace}:owner-schema`);
			let cursor = "0";
			do {
				const [next, keys] = await redis.scan(cursor, "MATCH", `${namespace}:*`, "COUNT", 500);
				cursor = next;
				if (keys.length > 0) await redis.del(...keys);
			} while (cursor !== "0");
		} catch (err) {
			console.error(`[bench] failed to delete the keys under prefix "${namespace}"`, err);
		}
	} finally {
		redis.disconnect();
	}
}

main()
	.then(() => process.exit(0))
	.catch(err => {
		console.error("[bench] failed:", err);
		process.exit(1);
	});
