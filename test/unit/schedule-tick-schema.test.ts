/** Audit item 8: a schedule tick of another schema (shared prefix) never creates or removes anything. */
import { describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import type { JobSchedulerInfo, Transport } from "../../src/queue/transport.js";
import { buildRegistry } from "../../src/registry.js";
import {
	assertScheduleTickSchema,
	processScheduleTick,
	ScheduleTickSchemaMismatchError,
} from "../../src/schedules.js";
import type { Store } from "../../src/store/types.js";
import { defineTask } from "../../src/task.js";

const logger = { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };
const task = defineTask({ id: "task-1", run: async (p: { v: number }) => p });

describe("assertScheduleTickSchema", () => {
	it("passes this schema's ticks and untagged (pre-tag) ticks", () => {
		expect(() =>
			assertScheduleTickSchema({ scheduleId: "s", schema: "a" }, "a", "s")
		).not.toThrow();
		expect(() => assertScheduleTickSchema({ scheduleId: "s" }, "a", "s")).not.toThrow();
		expect(() => assertScheduleTickSchema(null, "a", "s")).not.toThrow();
	});

	it("throws a ConfigError for another schema's tick", () => {
		const call = () => assertScheduleTickSchema({ scheduleId: "s", schema: "b" }, "a", "s");
		expect(call).toThrow(ScheduleTickSchemaMismatchError);
		expect(call).toThrow(ConfigError);
		expect(call).toThrow(/another schema/);
	});
});

describe("processScheduleTick with deps.schema", () => {
	function deps(schedulerSchema: string | undefined) {
		const removed: string[] = [];
		const created: unknown[] = [];
		const store = {
			schedules: { get: async () => null },
			runs: {
				create: async (input: unknown) => {
					created.push(input);
					throw new Error("must not create");
				},
			},
		} as unknown as Pick<Store, "schedules" | "runs">;
		const transport = {
			async enqueueRun() {},
			async removeSystemJob() {},
			async upsertJobScheduler() {},
			async removeJobScheduler(id: string) {
				removed.push(id);
				return true;
			},
			async getJobScheduler(id: string): Promise<JobSchedulerInfo | undefined> {
				return {
					id,
					pattern: "* * * * *",
					...(schedulerSchema !== undefined ? { schema: schedulerSchema } : {}),
				} as JobSchedulerInfo;
			},
		} as unknown as Transport;
		return { store, transport, removed, created };
	}

	it("never removes a scheduler tagged with another schema when the row is missing here", async () => {
		const d = deps("other");
		await expect(
			processScheduleTick(
				{
					store: d.store,
					transport: d.transport,
					registry: buildRegistry([task]),
					logger,
					schema: "mine",
				},
				"s1",
				Date.now()
			)
		).rejects.toBeInstanceOf(ScheduleTickSchemaMismatchError);
		expect(d.removed).toEqual([]);
		expect(d.created).toEqual([]);
	});

	it("still self-heals its own (or an untagged) orphan scheduler", async () => {
		for (const tag of ["mine", undefined]) {
			const d = deps(tag);
			await processScheduleTick(
				{
					store: d.store,
					transport: d.transport,
					registry: buildRegistry([task]),
					logger,
					schema: "mine",
				},
				"s1",
				Date.now()
			);
			expect(d.removed).toEqual(["s1"]);
		}
	});
});
