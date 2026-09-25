import { beforeAll, beforeEach } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlContext, type SqlContext } from "../../src/store/sql/db.js";
import { createSqlStore } from "../../src/store/sql/index.js";
import type { Store } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";
import { defineDebounceConcurrencyContractSuite } from "../helpers/store-contract-debounce-suite.js";
import { defineStoreContractSuite, type StoreHarness } from "../helpers/store-contract-suite.js";

// The shared store contract suite against Postgres (the reference behavior the in-memory store of
// test/unit/memory-store.test.ts must match).

const ns = useTestNamespace();
let ctx: SqlContext;
let store: Store;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	ctx = createSqlContext(ns.schema);
	store = createSqlStore(ns.pool, ns.schema);
});

beforeEach(async () => {
	await ns.pool.query(
		`TRUNCATE ${ctx.t("runs")}, ${ctx.t("idempotency_keys")}, ${ctx.t("steps")},
		   ${ctx.t("waitpoints")}, ${ctx.t("run_logs")}, ${ctx.t("schedules")} CASCADE`
	);
});

/** "Advancing the clock" = shifting every stored timestamp back by `ms`. */
const SHIFTS: [Parameters<SqlContext["t"]>[0], string[]][] = [
	[
		"runs",
		[
			"heartbeat_at",
			"run_after",
			"started_at",
			"finished_at",
			"created_at",
			"updated_at",
			"debounce_deadline",
		],
	],
	["steps", ["wake_at", "created_at"]],
	["waitpoints", ["expires_at", "created_at", "resolved_at"]],
	["idempotency_keys", ["expires_at"]],
	["run_logs", ["ts"]],
	["schedules", ["created_at", "updated_at"]],
];

const harness: StoreHarness = {
	get store() {
		return store;
	},
	async advance(ms) {
		for (const [table, cols] of SHIFTS) {
			const set = cols.map(c => `${c} = ${c} - $1::float8 * interval '1 millisecond'`).join(", ");
			await ns.pool.query(`UPDATE ${ctx.t(table)} SET ${set}`, [ms]);
		}
	},
	async now() {
		const { rows } = await ns.pool.query<{ now: Date }>("SELECT now() AS now");
		const row = rows[0];
		if (!row) throw new Error("no now()");
		return row.now;
	},
	async patchRun(runId, patch) {
		const sets: string[] = [];
		const values: unknown[] = [runId];
		if (patch.status !== undefined) {
			values.push(patch.status);
			sets.push(`status = $${values.length}`);
		}
		if (patch.executionId === null) sets.push("execution_id = NULL");
		if (patch.runAfter === null) sets.push("run_after = NULL");
		if (sets.length === 0) return;
		await ns.pool.query(`UPDATE ${ctx.t("runs")} SET ${sets.join(", ")} WHERE id = $1`, values);
	},
};

defineStoreContractSuite(() => harness);
defineDebounceConcurrencyContractSuite(() => harness);
