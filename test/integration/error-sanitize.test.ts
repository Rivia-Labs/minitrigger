/**
 * Audit items 2 & 12 (external audit, see CLAUDE.md notes for this session): a NUL byte or lone
 * surrogate in a thrown error's message/stack -- or in `ctx.logger` message/data -- used to reach
 * Postgres unsanitized. Postgres `jsonb` rejects both, so `runs.fail`/`runs.retry`/`insertBatch`
 * would throw, the run would stay `running` with a stopped heartbeat until the stall timeout, and
 * the *real* error was replaced by a synthetic stall error every attempt. Separately, a non-Error
 * thrown value (`throw response`, `cause: axiosLikeError`) used to have its *entire* object
 * (headers, tokens, ...) dumped into `runs.error` via `JSON.stringify`, violating decision 12
 * ("never serialize the whole error object or arbitrary properties").
 *
 * These are real, end-to-end regression tests against the docker Postgres/Redis (not just the
 * `formatError`/`sanitizeLogData` unit tests in `test/unit/`): they prove the *engine* no longer
 * stalls or leaks, not just that the sanitizing functions behave in isolation.
 */
import { escapeIdentifier } from "pg";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import type { BullmqTransportConfig, CommonConfig, EngineLogger } from "../../src/config.js";
import { createEngine, type Engine, type RunLogPage } from "../../src/engine.js";
import { type AnyTask, defineTask } from "../../src/task.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

const noop = () => {};
const testLogger: EngineLogger = { debug: noop, info: noop, warn: noop, error: noop };

const engines: Engine[] = [];

function makeEngine(
	tasks: AnyTask[],
	overrides: Partial<CommonConfig> & Partial<BullmqTransportConfig> = {}
): Engine {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks,
		pool: { max: 5 },
		heartbeatIntervalMs: 200,
		heartbeatTimeoutMs: 1_000,
		// Short but well above the time a healthy retry/fail should take: if sanitization
		// regresses and the write throws, the run would only reach `failed` via this stall path,
		// which the tests below assert did NOT happen (by bounding the wait well under it).
		stallTimeoutMs: 6_000,
		shutdownTimeoutMs: 2_000,
		logger: testLogger,
		...overrides,
	});
	engines.push(engine);
	return engine;
}

async function waitForStatus(engine: Engine, id: string, status: string, timeoutMs = 3_000) {
	await waitFor(async () => (await engine.runs.get(id))?.status === status, {
		timeoutMs,
		intervalMs: 25,
	});
	const run = await engine.runs.get(id);
	if (!run) throw new Error("unreachable");
	return run;
}

async function runRow(id: string) {
	const { rows } = await ns.pool.query<{ status: string; heartbeat_at: Date | null }>(
		`SELECT status, heartbeat_at FROM ${escapeIdentifier(ns.schema)}.runs WHERE id = $1`,
		[id]
	);
	const row = rows[0];
	if (!row) throw new Error(`run ${id} not found`);
	return row;
}

beforeAll(async () => {
	const engine = createEngine({
		postgres: ns.databaseUrl,
		redis: ns.redisUrl,
		schema: ns.schema,
		prefix: ns.prefix,
		tasks: [],
	});
	await engine.migrate();
	await engine.stop();
});

afterEach(async () => {
	await Promise.all(engines.splice(0).map(e => e.stop()));
});

describe("error sanitization at the Postgres boundary (audit item 2)", () => {
	it("a NUL byte + lone surrogate in the error message fails the run fast, not via stall", async () => {
		const task = defineTask({
			id: `nul-byte-${Date.now()}`,
			queue: { name: `q-${Date.now()}` },
			retry: { maxAttempts: 1 },
			run: async () => {
				throw new Error("a\u0000b\uD800");
			},
		});
		const engine = makeEngine([task]);
		await engine.start();

		const handle = await engine.trigger(task);
		// A tight timeout well under `stallTimeoutMs`: if the write to `runs.fail` threw (the
		// pre-fix bug), the run would stay `running` until the stall recovery kicks in instead,
		// and this would time out.
		const run = await waitForStatus(engine, handle.id, "failed", 2_000);

		expect(run.error?.message.includes("\u0000")).toBe(false);
		expect(run.error?.message.isWellFormed()).toBe(true);
		// The NUL byte becomes U+FFFD, and the lone surrogate is also replaced with U+FFFD by
		// `toWellFormed()`.
		expect(run.error?.message).toBe("a�b�");

		// The heartbeat was never left dangling waiting for a stall sweep.
		const row = await runRow(handle.id);
		expect(row.status).toBe("failed");
	});

	it("retries normally (not via stall) when the thrown error has bad characters", async () => {
		let calls = 0;
		const task = defineTask({
			id: `nul-byte-retry-${Date.now()}`,
			queue: { name: `q-${Date.now()}` },
			retry: { maxAttempts: 2, backoff: { type: "fixed", baseMs: 20, jitter: false } },
			run: async () => {
				calls++;
				if (calls === 1) throw new Error("first\u0000attempt\uD800");
				return "ok";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();

		const handle = await engine.trigger(task);
		const run = await waitForStatus(engine, handle.id, "completed", 2_000);
		expect(run.output).toBe("ok");
		expect(calls).toBe(2);
	});

	it("does not dump an axios-like non-Error's arbitrary properties (decision 12)", async () => {
		const task = defineTask({
			id: `axios-like-${Date.now()}`,
			queue: { name: `q-${Date.now()}` },
			retry: { maxAttempts: 1 },
			run: async () => {
				throw {
					name: "AxiosError",
					message: "Request failed with status code 401",
					config: { headers: { Authorization: "Bearer super-secret-token" } },
					response: { headers: { "set-cookie": "session=super-secret-session" } },
				};
			},
		});
		const engine = makeEngine([task]);
		await engine.start();

		const handle = await engine.trigger(task);
		const run = await waitForStatus(engine, handle.id, "failed", 2_000);

		expect(run.error).toEqual({
			name: "AxiosError",
			message: "Request failed with status code 401",
			retryable: true,
		});
		const dumped = JSON.stringify(run.error);
		expect(dumped).not.toContain("super-secret");
		expect(dumped).not.toContain("Authorization");
	});

	it("triggerAndWait surfaces the child's error to the parent already sanitized", async () => {
		const child = defineTask({
			id: `child-bad-error-${Date.now()}`,
			queue: { name: `q-${Date.now()}` },
			retry: { maxAttempts: 1 },
			run: async () => {
				throw {
					name: "AxiosError",
					message: "child failed\u0000\uD800",
					config: { headers: { Authorization: "Bearer leak-me-not" } },
				};
			},
		});
		const parent = defineTask({
			id: `parent-waits-${Date.now()}`,
			queue: { name: `q-${Date.now()}` },
			run: async (_p, ctx) => {
				const result = await ctx.step.triggerAndWait("c", child);
				return result.ok ? { ok: true } : { ok: false, error: result.error };
			},
		});
		const engine = makeEngine([parent, child]);
		await engine.start();

		const handle = await engine.trigger(parent);
		const run = await waitForStatus(engine, handle.id, "completed", 4_000);

		const output = run.output as { ok: boolean; error?: { name: string; message: string } };
		expect(output.ok).toBe(false);
		expect(output.error?.name).toBe("AxiosError");
		expect(output.error?.message.includes("\u0000")).toBe(false);
		expect(output.error?.message.isWellFormed()).toBe(true);
		const dumped = JSON.stringify(output);
		expect(dumped).not.toContain("leak-me-not");
		expect(dumped).not.toContain("Authorization");
	});

	it("ctx.logger with a NUL byte / lone surrogate doesn't drop the batch", async () => {
		const task = defineTask({
			id: `bad-log-${Date.now()}`,
			queue: { name: `q-${Date.now()}` },
			run: async (_p, ctx) => {
				ctx.logger.info("line one\u0000\uD800", { note: "val\u0000ue\uD800" });
				ctx.logger.info("line two");
				return "done";
			},
		});
		const engine = makeEngine([task]);
		await engine.start();

		const handle = await engine.trigger(task);
		await waitForStatus(engine, handle.id, "completed", 3_000);

		const page = (await waitFor(
			async () => {
				const p = await engine.runs.logs(handle.id, { limit: 10 });
				return p.items.length >= 2 ? p : undefined;
			},
			{ timeoutMs: 3_000, intervalMs: 50 }
		)) as RunLogPage;
		const { items } = page;

		expect(items).toHaveLength(2);
		expect(items[0]?.message.includes("\u0000")).toBe(false);
		expect(items[0]?.message.isWellFormed()).toBe(true);
		expect(items[1]?.message).toBe("line two");
	});
});
