/**
 * `runs.subscribe`'s ordering rule (decision 23), extracted as a pure helper (`isNewerRunUpdate`
 * in `src/engine.ts`) so it can be exercised without a real Postgres/Redis pub/sub round trip.
 */
import { describe, expect, it } from "vitest";
import { isNewerRunUpdate } from "../../src/engine.js";
import type { RunStatus } from "../../src/store/types.js";
import type { Json } from "../../src/utils/serialization.js";

function last(overrides: Partial<{ status: RunStatus; updatedAt: Date; progress: Json | null }>) {
	return {
		status: "running" as RunStatus,
		updatedAt: new Date("2026-01-01T00:00:00.000Z"),
		progress: null,
		...overrides,
	};
}

function candidate(
	overrides: Partial<{ status: RunStatus; updatedAt: Date; progress: Json | undefined }>
) {
	return {
		status: "running" as RunStatus,
		updatedAt: new Date("2026-01-01T00:00:00.000Z"),
		progress: undefined,
		...overrides,
	};
}

describe("isNewerRunUpdate", () => {
	it("a strictly later updatedAt is always newer", () => {
		expect(
			isNewerRunUpdate(
				last({ updatedAt: new Date("2026-01-01T00:00:00.000Z") }),
				candidate({ updatedAt: new Date("2026-01-01T00:00:00.001Z") })
			)
		).toBe(true);
	});

	it("a strictly earlier updatedAt is never newer", () => {
		expect(
			isNewerRunUpdate(
				last({ updatedAt: new Date("2026-01-01T00:00:00.001Z") }),
				candidate({ updatedAt: new Date("2026-01-01T00:00:00.000Z") })
			)
		).toBe(false);
	});

	it("same millisecond, same status, same progress: not newer", () => {
		expect(
			isNewerRunUpdate(
				last({ status: "running", progress: { a: 1, b: 2 } }),
				candidate({ status: "running", progress: { a: 1, b: 2 } })
			)
		).toBe(false);
	});

	it("same millisecond, different status: newer", () => {
		expect(isNewerRunUpdate(last({ status: "running" }), candidate({ status: "waiting" }))).toBe(
			true
		);
	});

	// (a) progress equality must be structural, not JSON-text based: jsonb/the memory store
	// reorder object keys, so two writes of an equal value under a different key order must not
	// look like a change.
	it("(a) same millisecond, same status, progress differs only by key order: not newer", () => {
		expect(
			isNewerRunUpdate(
				last({ status: "running", progress: { a: 1, b: 2, c: { d: 3, e: 4 } } }),
				candidate({ status: "running", progress: { c: { e: 4, d: 3 }, b: 2, a: 1 } })
			)
		).toBe(false);
	});

	it("(a) same millisecond, same status, progress differs in value: newer", () => {
		expect(
			isNewerRunUpdate(
				last({ status: "running", progress: { a: 1, b: 2 } }),
				candidate({ status: "running", progress: { a: 1, b: 3 } })
			)
		).toBe(true);
	});

	it("(a) key-order-insensitive equality also applies inside arrays/nested objects", () => {
		expect(
			isNewerRunUpdate(
				last({ status: "running", progress: [{ x: 1, y: 2 }, { z: 3 }] }),
				candidate({ status: "running", progress: [{ y: 2, x: 1 }, { z: 3 }] })
			)
		).toBe(false);
	});

	// (b) a same-millisecond "running" message carrying a progress field, arriving after an
	// already-emitted non-running status, is the earlier progress write showing up late
	// (pub/sub reordering) -- never a real transition back to running.
	it("(b) drops a same-ms running+progress message following a non-running last status", () => {
		expect(
			isNewerRunUpdate(
				last({ status: "waiting", progress: { step: 1 } }),
				candidate({ status: "running", progress: { step: 2 } })
			)
		).toBe(false);
	});

	it("(b) a genuine re-acquire's running message (no progress field) at the same ms still counts", () => {
		expect(
			isNewerRunUpdate(
				last({ status: "waiting", progress: { step: 1 } }),
				candidate({ status: "running", progress: undefined })
			)
		).toBe(true);
	});

	it("(b) the rule only applies at the same millisecond: a later running+progress message still counts", () => {
		expect(
			isNewerRunUpdate(
				last({
					status: "waiting",
					progress: { step: 1 },
					updatedAt: new Date("2026-01-01T00:00:00.000Z"),
				}),
				candidate({
					status: "running",
					progress: { step: 2 },
					updatedAt: new Date("2026-01-01T00:00:00.001Z"),
				})
			)
		).toBe(true);
	});

	it("(b) does not suppress a running+progress message when the last status was already running", () => {
		expect(
			isNewerRunUpdate(
				last({ status: "running", progress: { step: 1 } }),
				candidate({ status: "running", progress: { step: 2 } })
			)
		).toBe(true);
	});
});
