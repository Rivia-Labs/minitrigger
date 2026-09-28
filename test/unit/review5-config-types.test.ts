/**
 * Review 5 item B3: `EngineConfig` became a union in 0.4 (decision 28), which breaks a few common
 * 0.3.x patterns. Each old pattern is pinned with `@ts-expect-error` (so this file documents the
 * break), next to its migration path through the exported `BullmqEngineConfig`/`CommonConfig`.
 * Checked by `pnpm typecheck` (the whole `test/` tree is type-checked).
 */
import type { Redis } from "ioredis";
import { describe, expect, expectTypeOf, it } from "vitest";
import type {
	BullmqEngineConfig,
	CommonConfig,
	EngineConfig,
	PostgresEngineConfig,
	RedisLike,
} from "../../src/index.js";
import { createEngine } from "../../src/index.js";

const redis = {} as Redis;

// 1. `interface X extends EngineConfig` (TS2312 on a union).
// @ts-expect-error -- a union cannot be extended
interface OldAppConfig extends EngineConfig {
	extra: string;
}
interface AppConfig extends BullmqEngineConfig {
	extra: string;
}

// 2. `Omit<EngineConfig, "tasks">` + spread (Omit over a union collapses the discriminant).
function oldSpread(base: Omit<EngineConfig, "tasks">) {
	// @ts-expect-error -- `transport` became "bullmq" | "postgres", matching neither arm
	return createEngine({ ...base, tasks: [] });
}
function newSpread(base: Omit<BullmqEngineConfig, "tasks">) {
	return createEngine({ ...base, tasks: [] });
}

// 3. `config.redis` possibly undefined on the union.
function oldRedis(cfg: EngineConfig): string | { connection: RedisLike } {
	// @ts-expect-error -- `redis` is optional (`never`) on the postgres arm
	return cfg.redis;
}
function newRedis(cfg: BullmqEngineConfig): string | { connection: RedisLike } {
	return cfg.redis;
}

// 4. `config.redis.connection` used as an ioredis `Redis`.
function oldConnection(cfg: BullmqEngineConfig): Redis | undefined {
	// @ts-expect-error -- RedisLike is structural, not the ioredis class
	return typeof cfg.redis === "object" ? cfg.redis.connection : undefined;
}
function newConnection(cfg: BullmqEngineConfig<Redis>): Redis | undefined {
	return typeof cfg.redis === "object" ? cfg.redis.connection : undefined;
}

// 5. `Partial<EngineConfig>` overrides spread into createEngine.
function oldOverrides(over: Partial<EngineConfig>) {
	// @ts-expect-error -- the spread widens `transport` to both arms
	return createEngine({ postgres: "postgres://x", redis: "redis://x", tasks: [], ...over });
}
function newOverrides(over: Partial<BullmqEngineConfig>) {
	return createEngine({ postgres: "postgres://x", redis: "redis://x", tasks: [], ...over });
}
const overrides: Partial<BullmqEngineConfig> = { prefix: "p", redis: "redis://x" };
// `Extract` is the equivalent form without the new alias.
const extracted: Extract<EngineConfig, { transport?: "bullmq" }> = {
	postgres: "postgres://x",
	redis: "redis://x",
	tasks: [],
};

describe("review 5 B3: public config types", () => {
	it("the arms are assignable to EngineConfig and to createEngine", () => {
		const bull: BullmqEngineConfig<Redis> = {
			postgres: "postgres://x",
			redis: { connection: redis },
			tasks: [],
		};
		const pg: PostgresEngineConfig = { postgres: "postgres://x", transport: "postgres", tasks: [] };
		expectTypeOf(bull).toExtend<EngineConfig>();
		expectTypeOf(pg).toExtend<EngineConfig>();
		expectTypeOf(extracted).toExtend<BullmqEngineConfig>();
		expectTypeOf<AppConfig>().toExtend<CommonConfig>();
		expectTypeOf<OldAppConfig>().not.toBeAny();
		expect([
			oldSpread,
			newSpread,
			oldRedis,
			newRedis,
			oldConnection,
			newConnection,
			oldOverrides,
			newOverrides,
		]).toHaveLength(8);
		expect(overrides.prefix).toBe("p");
	});
});
