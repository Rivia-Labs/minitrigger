import { describe, expect, it } from "vitest";
import { ConfigError, MinitriggerError } from "../../src/errors.js";
import { assertRedisReachable } from "../../src/queue/redis-check.js";

describe("assertRedisReachable", () => {
	it("resolves when PING answers", async () => {
		await expect(assertRedisReachable({ ping: async () => "PONG" })).resolves.toBeUndefined();
	});

	it("throws a MinitriggerError (not a ConfigError) when PING never answers", async () => {
		const hanging = { ping: () => new Promise<never>(() => {}) };
		const error = await assertRedisReachable(hanging, 50).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(MinitriggerError);
		expect(error).not.toBeInstanceOf(ConfigError);
		expect((error as Error).message).toMatch(/no reply to PING within 50 ms/);
	});

	it("throws a MinitriggerError with the connection error as cause when PING fails", async () => {
		const cause = new Error("ECONNREFUSED");
		const failing = { ping: async () => Promise.reject(cause) };
		const error = await assertRedisReachable(failing, 1_000).catch((e: unknown) => e);
		expect(error).toBeInstanceOf(MinitriggerError);
		expect(error).not.toBeInstanceOf(ConfigError);
		expect((error as Error).message).toMatch(/cannot reach Redis \(ECONNREFUSED\)/);
		expect((error as Error).cause).toBe(cause);
	});
});
