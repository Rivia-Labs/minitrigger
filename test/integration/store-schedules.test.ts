import { Client, escapeIdentifier } from "pg";
import { beforeAll, describe, expect, it } from "vitest";
import initSql from "../../src/migrations/001_init.sql";
import { MIGRATIONS } from "../../src/migrations/index.js";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlContext, type SqlContext } from "../../src/store/sql/db.js";
import { createScheduleStore } from "../../src/store/sql/schedules.js";
import type { NewSchedule, ScheduleStore } from "../../src/store/types.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let ctx: SqlContext;
let schedules: ScheduleStore;

beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	ctx = createSqlContext(ns.schema);
	schedules = createScheduleStore({ kind: "pool", pool: ns.pool }, ctx);
});

function input(overrides: Partial<NewSchedule> = {}): NewSchedule {
	return {
		id: "daily",
		taskId: "task-a",
		cron: "0 9 * * *",
		timezone: "America/Belem",
		payload: { url: "https://example.com" },
		source: "api",
		...overrides,
	};
}

describe("migrate() applies 001 through 006", () => {
	it("applies every migration to a fresh schema", async () => {
		const fresh = `${ns.schema}_fresh`;
		await ns.pool.query(`CREATE SCHEMA ${escapeIdentifier(fresh)}`);
		try {
			const result = await migrate(ns.pool, fresh);
			expect(result.applied).toEqual(MIGRATIONS.map(m => m.version));
			expect(result.applied).toEqual([1, 2, 3, 4, 5, 6]);

			const { rows } = await ns.pool.query<{ tablename: string }>(
				"SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename = 'schedules'",
				[fresh]
			);
			expect(rows).toHaveLength(1);
		} finally {
			await ns.pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(fresh)} CASCADE`);
		}
	});

	it("adds only migrations 2 to 6 to a schema already at version 1", async () => {
		const legacy = `${ns.schema}_legacy`;
		await ns.pool.query(`CREATE SCHEMA ${escapeIdentifier(legacy)}`);
		const client = new Client({ connectionString: ns.databaseUrl });
		await client.connect();
		try {
			// Hand-roll the state migrate() would have left behind before migration 2 existed,
			// without touching the pool's own connections (so their search_path stays untouched).
			await client.query(`SET search_path TO ${escapeIdentifier(legacy)}`);
			await client.query(
				`CREATE TABLE schema_migrations (
					version int PRIMARY KEY,
					name text NOT NULL,
					applied_at timestamptz NOT NULL DEFAULT now()
				)`
			);
			await client.query(initSql);
			await client.query("INSERT INTO schema_migrations (version, name) VALUES (1, 'init')");

			const result = await migrate(ns.pool, legacy);
			expect(result.applied).toEqual([2, 3, 4, 5, 6]);

			const { rows } = await ns.pool.query<{ tablename: string }>(
				"SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename = 'schedules'",
				[legacy]
			);
			expect(rows).toHaveLength(1);

			const { rows: versions } = await ns.pool.query<{ version: number }>(
				`SELECT version FROM ${escapeIdentifier(legacy)}.schema_migrations ORDER BY version`
			);
			expect(versions.map(r => r.version)).toEqual([1, 2, 3, 4, 5, 6]);
		} finally {
			await client.end();
			await ns.pool.query(`DROP SCHEMA IF EXISTS ${escapeIdentifier(legacy)} CASCADE`);
		}
	});
});

describe("ScheduleStore.upsert", () => {
	it("creates a new schedule with created and both *Changed flags true", async () => {
		const result = await schedules.upsert(input({ id: "create-1" }));
		expect(result.created).toBe(true);
		expect(result.cronChanged).toBe(true);
		expect(result.timezoneChanged).toBe(true);
		expect(result.schedule).toMatchObject({
			id: "create-1",
			taskId: "task-a",
			cron: "0 9 * * *",
			timezone: "America/Belem",
			payload: { url: "https://example.com" },
			source: "api",
		});
		expect(result.schedule.createdAt).toBeInstanceOf(Date);
		expect(result.schedule.updatedAt).toBeInstanceOf(Date);
	});

	it("re-upserting identical cron/timezone reports no change", async () => {
		await schedules.upsert(input({ id: "same" }));
		const result = await schedules.upsert(input({ id: "same" }));
		expect(result.created).toBe(false);
		expect(result.cronChanged).toBe(false);
		expect(result.timezoneChanged).toBe(false);
	});

	it("changing only cron reports cronChanged but not timezoneChanged", async () => {
		await schedules.upsert(input({ id: "cron-change" }));
		const result = await schedules.upsert(input({ id: "cron-change", cron: "0 10 * * *" }));
		expect(result.created).toBe(false);
		expect(result.cronChanged).toBe(true);
		expect(result.timezoneChanged).toBe(false);
		expect(result.schedule.cron).toBe("0 10 * * *");
	});

	it("changing only timezone reports timezoneChanged but not cronChanged", async () => {
		await schedules.upsert(input({ id: "tz-change" }));
		const result = await schedules.upsert(input({ id: "tz-change", timezone: "UTC" }));
		expect(result.created).toBe(false);
		expect(result.cronChanged).toBe(false);
		expect(result.timezoneChanged).toBe(true);
		expect(result.schedule.timezone).toBe("UTC");
	});

	it("null timezone round-trips and null->value / value->null both count as a change", async () => {
		const created = await schedules.upsert(input({ id: "tz-null", timezone: null }));
		expect(created.schedule.timezone).toBeNull();
		const toValue = await schedules.upsert(input({ id: "tz-null", timezone: "UTC" }));
		expect(toValue.timezoneChanged).toBe(true);
		const toNull = await schedules.upsert(input({ id: "tz-null", timezone: null }));
		expect(toNull.timezoneChanged).toBe(true);
		expect(toNull.schedule.timezone).toBeNull();
	});

	it("upsert overwrites taskId, payload and source too", async () => {
		await schedules.upsert(input({ id: "overwrite", source: "declarative" }));
		const result = await schedules.upsert(
			input({ id: "overwrite", taskId: "task-b", payload: { changed: true }, source: "api" })
		);
		expect(result.schedule.taskId).toBe("task-b");
		expect(result.schedule.payload).toEqual({ changed: true });
		expect(result.schedule.source).toBe("api");
	});

	it.each<[string, NewSchedule["payload"]]>([
		["null", null],
		["string", "a bare string"],
		["array", [1, "two", false, null]],
		["nested object", { a: 1, nested: { b: [1, 2, 3] } }],
	])("round-trips a %s payload", async (_name, payload) => {
		const id = `payload-${_name.replace(/\s+/g, "-")}`;
		const result = await schedules.upsert(input({ id, payload }));
		expect(result.schedule.payload).toEqual(payload);
		const fetched = await schedules.get(id);
		expect(fetched?.payload).toEqual(payload);
	});

	it.each(["", "has:colon", "has space", "a".repeat(129)])(
		"rejects an invalid schedule id %j",
		async invalidId => {
			await expect(schedules.upsert(input({ id: invalidId }))).rejects.toThrow(
				`schedule id "${invalidId}" is invalid`
			);
		}
	);

	it("accepts the boundary-valid id shapes (letters, digits, _, ., -; up to 128 chars)", async () => {
		const maxLenId = "a".repeat(128);
		await expect(schedules.upsert(input({ id: "a.b-c_d9" }))).resolves.toMatchObject({
			created: true,
		});
		await expect(schedules.upsert(input({ id: maxLenId }))).resolves.toMatchObject({
			created: true,
		});
	});
});

describe("ScheduleStore.delete / get", () => {
	it("deletes an existing schedule and returns true; returns false for a missing one", async () => {
		await schedules.upsert(input({ id: "to-delete" }));
		expect(await schedules.delete("to-delete")).toBe(true);
		expect(await schedules.get("to-delete")).toBeNull();
		expect(await schedules.delete("to-delete")).toBe(false);
		expect(await schedules.delete("never-existed")).toBe(false);
	});

	it("get() returns null for an unknown id", async () => {
		expect(await schedules.get("does-not-exist")).toBeNull();
	});
});

describe("ScheduleStore.list / listDeclarative", () => {
	it("filters by source and orders by id", async () => {
		await schedules.upsert(input({ id: "list-api-2", source: "api" }));
		await schedules.upsert(input({ id: "list-api-1", source: "api" }));
		await schedules.upsert(input({ id: "list-decl-1", source: "declarative" }));

		const apiOnly = await schedules.list({ source: "api" });
		const apiIds = apiOnly.map(s => s.id).filter(id => id.startsWith("list-api"));
		expect(apiIds).toEqual(["list-api-1", "list-api-2"]);
		expect(apiOnly.every(s => s.source === "api")).toBe(true);

		const declOnly = await schedules.listDeclarative();
		expect(declOnly.some(s => s.id === "list-decl-1")).toBe(true);
		expect(declOnly.every(s => s.source === "declarative")).toBe(true);
	});

	it("with no filter returns every source", async () => {
		await schedules.upsert(input({ id: "list-all-api", source: "api" }));
		await schedules.upsert(input({ id: "list-all-decl", source: "declarative" }));
		const all = await schedules.list();
		const ids = all.map(s => s.id);
		expect(ids).toContain("list-all-api");
		expect(ids).toContain("list-all-decl");
	});
});

describe("ScheduleStore.deleteDeclarativeExcept", () => {
	it("prunes declarative schedules not in the kept set, leaving api schedules untouched", async () => {
		await schedules.upsert(input({ id: "prune-decl-keep", source: "declarative" }));
		await schedules.upsert(input({ id: "prune-decl-drop", source: "declarative" }));
		await schedules.upsert(input({ id: "prune-api-untouched", source: "api" }));

		const deleted = await schedules.deleteDeclarativeExcept(["prune-decl-keep"]);
		expect(deleted).toBeGreaterThanOrEqual(1);

		expect(await schedules.get("prune-decl-keep")).not.toBeNull();
		expect(await schedules.get("prune-decl-drop")).toBeNull();
		expect(await schedules.get("prune-api-untouched")).not.toBeNull();
	});

	it("an empty keep list deletes every declarative schedule but no api schedule", async () => {
		await schedules.upsert(input({ id: "prune-empty-decl", source: "declarative" }));
		await schedules.upsert(input({ id: "prune-empty-api", source: "api" }));

		await schedules.deleteDeclarativeExcept([]);

		expect(await schedules.get("prune-empty-decl")).toBeNull();
		expect(await schedules.get("prune-empty-api")).not.toBeNull();
		expect(await schedules.listDeclarative()).toEqual([]);
	});
});
