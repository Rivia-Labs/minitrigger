/**
 * Review 5 item B3: `RedisLike` is structural (the public types never import `ioredis`), so a
 * `Cluster` or a hand-made object type-checks; the BullMQ backend checks the injected connection
 * at runtime instead of failing deep inside BullMQ.
 */
import { Cluster, Redis } from "ioredis";
import { afterEach, describe, expect, it } from "vitest";
import { ConfigError } from "../../src/errors.js";
import type { RedisLike } from "../../src/index.js";
import { createRedisConnections } from "../../src/queue/connections.js";

const opened: Array<{ disconnect(): void }> = [];
afterEach(() => {
	for (const c of opened.splice(0)) c.disconnect();
});

describe("review 5 B3: injected redis connection is checked at runtime", () => {
	it("an ioredis Cluster is a ConfigError", () => {
		const cluster = new Cluster([{ host: "127.0.0.1", port: 1 }], { lazyConnect: true });
		opened.push(cluster);
		expect(() => createRedisConnections({ connection: cluster as unknown as RedisLike })).toThrow(
			ConfigError
		);
		expect(() => createRedisConnections({ connection: cluster as unknown as RedisLike })).toThrow(
			/Cluster/
		);
	});

	it("a hand-made object with only the RedisLike methods is a ConfigError", () => {
		const fake: RedisLike = {
			duplicate: () => fake,
			publish: async () => 0,
			on: () => fake,
		};
		expect(() => createRedisConnections({ connection: fake })).toThrow(ConfigError);
	});

	it("an ioredis Redis instance is accepted", () => {
		const redis = new Redis({ lazyConnect: true, port: 1 });
		opened.push(redis);
		const connections = createRedisConnections({ connection: redis });
		expect(connections.shared()).toBe(redis);
	});
});
