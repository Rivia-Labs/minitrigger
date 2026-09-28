/** Review 5 item B1: memoization rules of `createSchemaTransportGuard`. */
import { describe, expect, it } from "vitest";
import {
	createSchemaTransportGuard,
	SCHEMA_TRANSPORT_UNSET_RECHECK_MS,
} from "../../src/backends/shared.js";
import { ConfigError } from "../../src/errors.js";

function fakeMeta(values: Array<string | null | Error>) {
	let reads = 0;
	const meta = {
		get: async () => {
			const value = values[Math.min(reads, values.length - 1)];
			reads++;
			if (value instanceof Error) throw value;
			return value ?? null;
		},
		setIfAbsent: async () => {
			throw new Error("the guard never writes");
		},
	};
	return { store: { meta }, reads: () => reads };
}

const resolved = { schema: "s", transport: "bullmq" as const };

describe("createSchemaTransportGuard", () => {
	it("a match is read once", async () => {
		const f = fakeMeta(["bullmq"]);
		const guard = createSchemaTransportGuard(f.store, resolved);
		await Promise.all([guard(), guard(), guard()]);
		await guard();
		expect(f.reads()).toBe(1);
	});

	it("a mismatch is read once and always throws ConfigError", async () => {
		const f = fakeMeta(["postgres"]);
		const guard = createSchemaTransportGuard(f.store, resolved);
		await expect(guard()).rejects.toBeInstanceOf(ConfigError);
		await expect(guard()).rejects.toThrow(/transport "postgres"/);
		expect(f.reads()).toBe(1);
	});

	it("an absent key allows the write and is re-read after the recheck window", async () => {
		let clock = 0;
		const f = fakeMeta([null, "postgres"]);
		const guard = createSchemaTransportGuard(f.store, resolved, () => clock);
		await guard();
		await guard();
		expect(f.reads()).toBe(1);
		clock += SCHEMA_TRANSPORT_UNSET_RECHECK_MS;
		await expect(guard()).rejects.toBeInstanceOf(ConfigError);
		expect(f.reads()).toBe(2);
	});

	it("a failed read is not memoized", async () => {
		const f = fakeMeta([new Error("boom"), "bullmq"]);
		const guard = createSchemaTransportGuard(f.store, resolved);
		await expect(guard()).rejects.toThrow("boom");
		await guard();
		await guard();
		expect(f.reads()).toBe(2);
	});
});
