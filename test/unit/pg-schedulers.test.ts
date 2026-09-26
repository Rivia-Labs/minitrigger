import { describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import { nextOccurrence, scheduleIdOfJob, scheduleTickJobId } from "../../src/pg/schedulers.js";

describe("nextOccurrence", () => {
	it("computes the next occurrence after a given instant in a timezone", () => {
		const after = new Date("2026-03-08T06:30:00Z"); // 01:30 America/New_York, DST-change day
		const next = nextOccurrence("0 3 * * *", "America/New_York", after);
		expect(next.toISOString()).toBe("2026-03-08T07:00:00.000Z"); // 03:00 EDT
	});

	it("is strictly after `after`", () => {
		const after = new Date("2026-01-01T00:00:00Z");
		expect(nextOccurrence("* * * * *", undefined, after).getTime()).toBe(after.getTime() + 60_000);
	});

	it("rejects invalid patterns with ConfigError", () => {
		expect(() => nextOccurrence("99 * * * *", undefined, new Date())).toThrow(ConfigError);
		expect(() => nextOccurrence("* * * * *", "Not/AZone", new Date())).toThrow(ConfigError);
	});
});

describe("scheduleTickJobId / scheduleIdOfJob", () => {
	it("job id round-trips", () => {
		expect(scheduleIdOfJob(scheduleTickJobId("daily"))).toBe("daily");
		expect(scheduleIdOfJob("wp-x-expire")).toBeUndefined();
	});
});
