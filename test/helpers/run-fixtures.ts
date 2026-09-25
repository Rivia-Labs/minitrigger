import { randomUUID } from "node:crypto";
import { beforeAll, beforeEach } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlContext, type SqlContext, type SqlExecutor } from "../../src/store/sql/db.js";
import { createIdempotencyStore } from "../../src/store/sql/idempotency.js";
import { createRunStore } from "../../src/store/sql/runs.js";
import { createWaitpointStore } from "../../src/store/sql/waitpoints.js";
import type {
	IdempotencyStore,
	NewRun,
	Ownership,
	RunRecord,
	RunStore,
	WaitpointStore,
} from "../../src/store/types.js";
import type { TestNamespace } from "./namespace.js";

export interface RunFixtures {
	readonly ctx: SqlContext;
	readonly exec: SqlExecutor;
	readonly runs: RunStore;
	readonly waitpoints: WaitpointStore;
	readonly idempotency: IdempotencyStore;
	/** `pool.query` against the test schema (use `t("runs")` for table names). */
	sql<R = Record<string, unknown>>(text: string, values?: unknown[]): Promise<R[]>;
	t(table: Parameters<SqlContext["t"]>[0]): string;
	newRun(overrides?: Partial<NewRun>): NewRun;
	/** Creates a run and acquires it; returns the running record and its ownership. */
	running(overrides?: Partial<NewRun>): Promise<{ run: RunRecord; owner: Ownership }>;
	/** Acquires an existing run at its current seq. */
	acquire(runId: string): Promise<Ownership>;
}

/**
 * Migrates the namespace once and builds pool-bound SQL stores; truncates all tables before each
 * test so reconciler-style (schema-wide) queries see only the current test's rows.
 */
export function useRunFixtures(ns: TestNamespace): RunFixtures {
	let ctx: SqlContext | undefined;
	let exec: SqlExecutor | undefined;
	let runs: RunStore | undefined;
	let waitpoints: WaitpointStore | undefined;
	let idempotency: IdempotencyStore | undefined;

	beforeAll(async () => {
		await migrate(ns.pool, ns.schema);
		ctx = createSqlContext(ns.schema);
		exec = { kind: "pool", pool: ns.pool };
		runs = createRunStore(exec, ctx);
		waitpoints = createWaitpointStore(exec, ctx);
		idempotency = createIdempotencyStore(exec, ctx);
	});

	beforeEach(async () => {
		const c = need(ctx);
		await ns.pool.query(
			`TRUNCATE ${c.t("runs")}, ${c.t("idempotency_keys")}, ${c.t("steps")}, ${c.t("waitpoints")}, ${c.t("run_logs")} CASCADE`
		);
	});

	const fixtures: RunFixtures = {
		get ctx() {
			return need(ctx);
		},
		get exec() {
			return need(exec);
		},
		get runs() {
			return need(runs);
		},
		get waitpoints() {
			return need(waitpoints);
		},
		get idempotency() {
			return need(idempotency);
		},
		async sql<R>(text: string, values?: unknown[]) {
			const res = await ns.pool.query(text, values);
			return res.rows as R[];
		},
		t(table) {
			return need(ctx).t(table);
		},
		newRun(overrides = {}) {
			return {
				id: randomUUID(),
				taskId: "task-a",
				queue: "default",
				payload: { hello: "world" },
				maxAttempts: 3,
				delayMs: 0,
				...overrides,
			};
		},
		async running(overrides = {}) {
			const created = await fixtures.runs.create(fixtures.newRun(overrides));
			if (created.kind !== "created") throw new Error("expected a new run");
			const executionId = randomUUID();
			const run = await fixtures.runs.acquire(created.run.id, executionId, created.enqueue.seq);
			if (!run) throw new Error("acquire failed");
			return { run, owner: { runId: run.id, executionId } };
		},
		async acquire(runId) {
			const current = await fixtures.runs.get(runId);
			if (!current) throw new Error(`run ${runId} not found`);
			const executionId = randomUUID();
			const run = await fixtures.runs.acquire(runId, executionId, current.enqueueSeq);
			if (!run) throw new Error(`acquire of ${runId} failed (status ${current.status})`);
			return { runId, executionId };
		},
	};
	return fixtures;
}

function need<T>(value: T | undefined): T {
	if (value === undefined) throw new Error("run fixtures are only available inside tests");
	return value;
}
