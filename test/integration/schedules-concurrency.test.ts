/**
 * Audit low item (b): `requireSourceMatch` must hold under two concurrent upserts of a NEW id.
 * Both `SELECT ... FOR UPDATE` find no row (nothing to lock); the second INSERT then waits on the
 * first one's uncommitted row and, with `ON CONFLICT DO UPDATE`, used to overwrite it after the
 * commit without ever checking its source.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlContext, type SqlContext } from "../../src/store/sql/db.js";
import { createScheduleStore } from "../../src/store/sql/schedules.js";
import type { ScheduleStore } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let ctx: SqlContext;
let schedules: ScheduleStore;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	ctx = createSqlContext(ns.schema);
	schedules = createScheduleStore({ kind: "pool", pool: ns.pool }, ctx);
});

describe("ScheduleStore.upsert: concurrent creation of the same id", () => {
	it("a declarative upsert racing an uncommitted api insert reports a conflict, not an overwrite", async () => {
		const other = await ns.pool.connect();
		try {
			await other.query("BEGIN");
			await other.query(
				`INSERT INTO ${ctx.t("schedules")} (id, task_id, cron, timezone, payload, source)
				 VALUES ('race', 'api-task', '0 1 * * *', NULL, NULL, 'api')`
			);
			const racing = schedules.upsert(
				{
					id: "race",
					taskId: "decl-task",
					cron: "0 2 * * *",
					timezone: null,
					payload: null,
					source: "declarative",
				},
				{ requireSourceMatch: true }
			);
			// Let the upsert reach its INSERT and block on the uncommitted row.
			await new Promise(resolve => setTimeout(resolve, 200));
			await other.query("COMMIT");

			const result = await racing;
			expect(result.conflict).toEqual({ existingSource: "api" });
			expect(result.created).toBe(false);
			expect(result.schedule).toMatchObject({ taskId: "api-task", source: "api" });
			expect(await schedules.get("race")).toMatchObject({
				taskId: "api-task",
				cron: "0 1 * * *",
				source: "api",
			});
		} finally {
			await other.query("ROLLBACK").catch(() => {});
			other.release();
		}
	});

	it("without requireSourceMatch the racing upsert updates the row and reports created: false", async () => {
		const other = await ns.pool.connect();
		try {
			await other.query("BEGIN");
			await other.query(
				`INSERT INTO ${ctx.t("schedules")} (id, task_id, cron, timezone, payload, source)
				 VALUES ('race2', 'api-task', '0 1 * * *', NULL, NULL, 'api')`
			);
			const racing = schedules.upsert({
				id: "race2",
				taskId: "api-task",
				cron: "0 3 * * *",
				timezone: null,
				payload: null,
				source: "api",
			});
			await new Promise(resolve => setTimeout(resolve, 200));
			await other.query("COMMIT");

			const result = await racing;
			expect(result).toMatchObject({ created: false, cronChanged: true, timezoneChanged: false });
			expect((await schedules.get("race2"))?.cron).toBe("0 3 * * *");
		} finally {
			await other.query("ROLLBACK").catch(() => {});
			other.release();
		}
	});
});
