import type { Redis } from "ioredis";
import { describe, it } from "vitest";
import type {
	BullmqTransportConfig,
	EngineConfig,
	PostgresTransportConfig,
	RedisLike,
} from "../../src/config.js";

/**
 * Compile-time only assertions for the `transport` discriminated union (spec §3.10, Postgres
 * transport plan task 2). As with `serializable-types.test.ts`/`task-types.test.ts`, the
 * interesting checks are the type errors `pnpm typecheck` would raise on a regression, and the
 * `// @ts-expect-error` cases, which fail typecheck if the constraint stops rejecting what it
 * should. Nothing here needs to run. The directive sits on the `const config: EngineConfig = {`
 * line itself (not the offending property below it): a type-mismatch error on an object literal
 * assigned to a declared type is reported at the assignment, not at the specific property.
 */

describe("EngineConfig: transport discriminated union", () => {
	it("bullmq (transport omitted, the default) accepts redis without a transport field", () => {
		const config: EngineConfig = { postgres: "postgres://x", tasks: [], redis: "redis://r" };
		void config;
	});

	it("transport: 'postgres' rejects redis", () => {
		// @ts-expect-error redis is not used with transport "postgres"
		const config: EngineConfig = {
			postgres: "postgres://x",
			tasks: [],
			transport: "postgres",
			redis: "redis://r",
		};
		void config;
	});

	it("transport: 'postgres' rejects prefix", () => {
		// @ts-expect-error prefix is not used with transport "postgres"
		const config: EngineConfig = {
			postgres: "postgres://x",
			tasks: [],
			transport: "postgres",
			prefix: "p",
		};
		void config;
	});

	it("transport: 'postgres' rejects outbox", () => {
		// @ts-expect-error outbox is not used with transport "postgres"
		const config: EngineConfig = {
			postgres: "postgres://x",
			tasks: [],
			transport: "postgres",
			outbox: {},
		};
		void config;
	});

	it("polling without transport: 'postgres' does not compile", () => {
		// @ts-expect-error polling is only used with transport "postgres"
		const config: EngineConfig = {
			postgres: "postgres://x",
			tasks: [],
			redis: "redis://r",
			polling: {},
		};
		void config;
	});

	it("an ioredis Redis instance is assignable to { connection: RedisLike }", () => {
		const redis = null as unknown as Redis;
		const wrapped: { connection: RedisLike } = { connection: redis };
		void wrapped;
	});

	it("BullmqTransportConfig/PostgresTransportConfig are exported and compose EngineConfig", () => {
		const bullmq: BullmqTransportConfig = { redis: "redis://r" };
		const postgres: PostgresTransportConfig = { transport: "postgres" };
		void bullmq;
		void postgres;
	});
});
