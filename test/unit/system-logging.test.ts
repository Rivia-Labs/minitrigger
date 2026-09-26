/**
 * Audit item (d): failures of the `waitpoint-expire` and `retention` system jobs are logged
 * (then rethrown, so BullMQ records the failure) instead of vanishing with `removeOnFail`.
 */
import type { Job } from "bullmq";
import { describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import {
	ActiveExecutions,
	createSystemDispatcher,
	type SystemDeps,
} from "../../src/executor/index.js";
import { SystemJobDelayed } from "../../src/executor/system.js";
import { buildRegistry } from "../../src/registry.js";
import type { Store } from "../../src/store/types.js";

type Call = { level: string; fields: Record<string, unknown>; message: string };

function recordingLogger(): EngineLogger & { calls: Call[] } {
	const calls: Call[] = [];
	const make = (level: string) => (fields: Record<string, unknown>, message: string) => {
		calls.push({ level, fields, message });
	};
	return {
		calls,
		debug: make("debug"),
		info: make("info"),
		warn: make("warn"),
		error: make("error"),
	};
}

function deps(store: unknown, extra: Partial<SystemDeps> = {}) {
	const logger = recordingLogger();
	const d: SystemDeps = {
		config: { logger },
		store: store as Store,
		transport: {
			enqueueRun: vi.fn(async () => {}),
			removeRunJob: vi.fn(async () => {}),
			removeSystemJob: vi.fn(async () => {}),
			upsertJobScheduler: vi.fn(async () => {}),
			removeJobScheduler: vi.fn(async () => true),
			getJobScheduler: vi.fn(async () => undefined),
		},
		active: new ActiveExecutions(),
		registry: buildRegistry([]),
		...extra,
	};
	return { d, logger };
}

const job = (data: unknown, extra: Record<string, unknown> = {}) =>
	({ id: "wp-x-expire", data, opts: {}, token: "tok", ...extra }) as unknown as Job;

describe("system jobs: failures are logged", () => {
	it("waitpoint-expire: a store failure is logged with the waitpointId and rethrown", async () => {
		const boom = new Error("db down");
		const { d, logger } = deps({ waitpoints: { expire: async () => Promise.reject(boom) } });
		const dispatch = createSystemDispatcher(d);

		await expect(dispatch("waitpoint-expire", { waitpointId: "w1" }, job({}))).rejects.toBe(boom);

		const error = logger.calls.find(c => c.level === "error");
		expect(error?.fields).toMatchObject({ waitpointId: "w1", err: boom });
		expect(error?.message).toMatch(/waitpoint-expire/);
	});

	it("waitpoint-expire: the not_due re-delay (SystemJobDelayed) is not logged as a failure", async () => {
		const { d, logger } = deps({
			waitpoints: { expire: async () => ({ kind: "not_due", delayMs: 10 }) },
		});
		const dispatch = createSystemDispatcher(d);
		const moveToDelayed = vi.fn(async () => {});

		await expect(
			dispatch("waitpoint-expire", { waitpointId: "w1" }, job({}, { moveToDelayed }))
		).rejects.toBeInstanceOf(SystemJobDelayed);

		expect(logger.calls.some(c => c.level === "error")).toBe(false);
	});

	it("retention: a sweep failure is logged and rethrown", async () => {
		const boom = new Error("sweep broke");
		const { d, logger } = deps(
			{},
			{
				runRetention: async () => Promise.reject(boom),
			}
		);
		const dispatch = createSystemDispatcher(d);

		await expect(dispatch("retention", {}, job({}))).rejects.toBe(boom);

		const error = logger.calls.find(c => c.level === "error");
		expect(error?.fields).toMatchObject({ err: boom });
		expect(error?.message).toMatch(/retention/);
	});
});

describe("system jobs: schedule-tick gets the engine's schema", () => {
	it("never removes a scheduler tagged with another schema when this schema has no row", async () => {
		const { d } = deps({ schedules: { get: async () => null } });
		const removeJobScheduler = vi.fn(async () => true);
		const dispatch = createSystemDispatcher({
			...d,
			config: { ...d.config, schema: "mine" },
			transport: {
				...d.transport,
				removeJobScheduler,
				getJobScheduler: vi.fn(async () => ({
					id: "daily",
					pattern: "* * * * *",
					timezone: undefined,
					schema: "theirs",
				})),
			},
		});

		await expect(
			dispatch("schedule-tick", { scheduleId: "daily" }, job({}, { repeatJobKey: "daily" }))
		).rejects.toThrow(/another schema/);
		expect(removeJobScheduler).not.toHaveBeenCalled();
	});
});
