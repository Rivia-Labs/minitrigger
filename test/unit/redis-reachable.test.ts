import { describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import { assertRedisReachable } from "../../src/queue/redis-check.js";

describe("assertRedisReachable", () => {
	it("resolves when PING answers", async () => {
		await expect(assertRedisReachable({ ping: async () => "PONG" })).resolves.toBeUndefined();
	});

	it("throws a ConfigError when PING never answers", async () => {
		const hanging = { ping: () => new Promise<never>(() => {}) };
		await expect(assertRedisReachable(hanging, 50)).rejects.toThrow(ConfigError);
		await expect(assertRedisReachable(hanging, 50)).rejects.toThrow(
			/no reply to PING within 50 ms/
		);
	});

	it("throws a ConfigError with the connection error when PING fails", async () => {
		const failing = { ping: async () => Promise.reject(new Error("ECONNREFUSED")) };
		await expect(assertRedisReachable(failing, 1_000)).rejects.toThrow(/ECONNREFUSED/);
	});
});
