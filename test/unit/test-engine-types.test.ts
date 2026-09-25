import { describe, expectTypeOf, it } from "vitest";
import type { Engine, EngineClient } from "../../src/engine.js";
import { defineTask } from "../../src/task.js";
import {
	createTestEngine,
	type InlineRunResult,
	type TestEngine,
} from "../../src/testing/index.js";

/**
 * Compile-time only assertions for Phase 9.2's `EngineClient` (L4) and the result types of
 * `runInline`/`drive`. As with `serializable-types.test.ts`/`task-types.test.ts`, the interesting
 * checks are the type errors `pnpm typecheck` would raise on a regression, and the
 * `// @ts-expect-error` cases. Nothing here needs to run.
 */

describe("EngineClient: shared surface of Engine and TestEngine", () => {
	it("Engine is assignable to EngineClient", () => {
		expectTypeOf<Engine>().toMatchTypeOf<EngineClient>();
	});

	it("TestEngine is assignable to EngineClient", () => {
		expectTypeOf<TestEngine>().toMatchTypeOf<EngineClient>();
	});

	it("only exposes trigger/runs/tokens/schedules/events", () => {
		expectTypeOf<EngineClient>().toEqualTypeOf<
			Pick<Engine, "trigger" | "runs" | "tokens" | "schedules" | "events">
		>();
	});
});

describe("TestEngineConfig: only retention.days (audit low d)", () => {
	const typed = defineTask({
		id: "typed-result",
		run: async (_p: null) => ({ ok: true as const }),
	});

	it("accepts retention.days (TTL check like the real engine)", () => {
		createTestEngine({ tasks: [typed], retention: { days: 90 }, idempotencyKeyTtl: "60d" });
	});

	it("rejects the sweep options the test engine never uses", () => {
		createTestEngine({
			tasks: [typed],
			// @ts-expect-error the test engine runs no retention sweeps: no intervalMs/batchSize.
			retention: { days: 30, intervalMs: 1_000 },
		});
	});
});

describe("runInline/drive: result types", () => {
	const typed = defineTask({
		id: "typed-result-2",
		run: async (_p: null) => ({ ok: true as const }),
	});

	it("infers InlineRunResult<O> from the task's output", () => {
		const engine = createTestEngine({ tasks: [typed] });
		expectTypeOf(engine.runInline(typed, null)).resolves.toEqualTypeOf<
			InlineRunResult<{ ok: true }>
		>();
	});

	it("drive(run) infers O from a typed RunHandle", async () => {
		const engine = createTestEngine({ tasks: [typed] });
		const handle = await engine.trigger(typed, null);
		expectTypeOf(engine.drive(handle)).resolves.toEqualTypeOf<InlineRunResult<{ ok: true }>>();
	});
});
