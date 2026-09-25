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

describe("TestEngineConfig: no retention field (L4)", () => {
	const typed = defineTask({
		id: "typed-result",
		run: async (_p: null) => ({ ok: true as const }),
	});

	it("rejects a retention option", () => {
		createTestEngine({
			tasks: [typed],
			// @ts-expect-error retention isn't part of TestEngineConfig: the test engine's `-system`
			// worker never runs real retention sweeps (CLAUDE.md phase 9.2 notes). `days: 30` matches
			// `resolveConfig`'s own default, so this call itself does not also fail at runtime for
			// the unrelated reason of `idempotencyKeyTtl` exceeding retention.
			retention: { days: 30 },
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
