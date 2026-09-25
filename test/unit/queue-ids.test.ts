import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import { runJobId, systemQueueName, waitpointExpireJobId } from "../../src/queue/ids.js";
import { checkRedisEvictionPolicy, type RedisConfigClient } from "../../src/queue/redis-check.js";

describe("runJobId", () => {
	it("builds the runId-seq jobId", () => {
		const runId = randomUUID();
		expect(runJobId(runId, 1)).toBe(`${runId}-1`);
		expect(runJobId(runId, 42)).toBe(`${runId}-42`);
	});

	it("rejects a negative or non-integer seq", () => {
		const runId = randomUUID();
		expect(() => runJobId(runId, -1)).toThrow(ConfigError);
		expect(() => runJobId(runId, 1.5)).toThrow(ConfigError);
	});

	it("rejects a runId containing ':' (would produce a jobId BullMQ rejects)", () => {
		expect(() => runJobId("run:with:colon", 1)).toThrow(ConfigError);
		expect(() => runJobId("run:with:colon", 1)).toThrow(/must not contain ":"/);
	});

	it("rejects a purely numeric jobId", () => {
		// A runId that is itself a bare number and a seq that, once joined, is still all digits
		// -- BullMQ throws "Custom Id cannot be integers" for this shape.
		expect(() => runJobId("123", 456)).not.toThrow(); // "123-456" is not purely numeric
		expect(() => runJobId("", 123)).toThrow(ConfigError); // "-123" -> parseInt("-123",10) === -123
	});
});

describe("waitpointExpireJobId", () => {
	it("builds the wp-id-expire jobId", () => {
		const id = randomUUID();
		expect(waitpointExpireJobId(id)).toBe(`wp-${id}-expire`);
	});

	it("rejects a waitpoint id containing ':'", () => {
		expect(() => waitpointExpireJobId("has:colon")).toThrow(ConfigError);
	});
});

describe("systemQueueName", () => {
	it("builds the prefix-system queue name", () => {
		expect(systemQueueName("minitrigger")).toBe("minitrigger-system");
		expect(systemQueueName("app_1")).toBe("app_1-system");
	});
});

describe("checkRedisEvictionPolicy", () => {
	function fakeClient(reply: unknown): RedisConfigClient {
		return {
			config: async () => reply,
		};
	}

	it("resolves without a warning when the policy is noeviction (flat RESP2 array reply)", async () => {
		await expect(
			checkRedisEvictionPolicy(fakeClient(["maxmemory-policy", "noeviction"]))
		).resolves.toBeUndefined();
	});

	it("resolves without a warning for a RESP3 map-like reply", async () => {
		await expect(
			checkRedisEvictionPolicy(fakeClient({ "maxmemory-policy": "noeviction" }))
		).resolves.toBeUndefined();
	});

	it("throws ConfigError when the policy is anything other than noeviction", async () => {
		await expect(
			checkRedisEvictionPolicy(fakeClient(["maxmemory-policy", "allkeys-lru"]))
		).rejects.toThrow(ConfigError);
		await expect(
			checkRedisEvictionPolicy(fakeClient(["maxmemory-policy", "allkeys-lru"]))
		).rejects.toThrow(/noeviction/);
	});

	it("returns a warning string (does not throw) when CONFIG errors, e.g. a managed Redis", async () => {
		const client: RedisConfigClient = {
			config: async () => {
				throw new Error("ERR unknown command 'CONFIG'");
			},
		};
		const warning = await checkRedisEvictionPolicy(client);
		expect(warning).toMatch(/could not read Redis/);
		expect(warning).toMatch(/unknown command 'CONFIG'/);
	});

	// Fix 10 (opus review): an empty/unexpected reply is the check being *unable to answer*
	// (decision 22), not a wrong answer -- it must warn, not throw.
	it.each<[string, unknown]>([
		["an empty array", []],
		["null", null],
		["an empty object", {}],
		["a bare string", "noeviction"],
	])("returns a warning (does not throw) for %s reply", async (_name, reply) => {
		const warning = await checkRedisEvictionPolicy(fakeClient(reply));
		expect(warning).toBeDefined();
		expect(warning).toMatch(/unexpected/);
	});
});
