/**
 * `attachDefaultLogger` (opt-in `engine.events` -> `EngineLogger` bridge, `src/telemetry.ts`):
 * exercised against a minimal fake `Engine` (only `events`/`logger` are used) so this stays a fast
 * unit test instead of needing a real engine/infra.
 */
import { describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import type { Engine, EngineEvents, RunEvent, RunEventName } from "../../src/engine.js";
import { attachDefaultLogger } from "../../src/telemetry.js";

type Call = {
	level: "debug" | "info" | "warn" | "error";
	fields: Record<string, unknown>;
	message: string;
};

function fakeLogger(overrides: Partial<EngineLogger> = {}): {
	logger: EngineLogger;
	calls: Call[];
} {
	const calls: Call[] = [];
	const make =
		(level: Call["level"]) =>
		(fields: Record<string, unknown>, message: string): void => {
			calls.push({ level, fields, message });
		};
	const logger: EngineLogger = {
		debug: make("debug"),
		info: make("info"),
		warn: make("warn"),
		error: make("error"),
		...overrides,
	};
	return { logger, calls };
}

/** A fake `Engine`: `attachDefaultLogger` only ever touches `.events` and `.logger`. */
function fakeEngine(engineLogger: EngineLogger): {
	engine: Engine;
	emit: (event: RunEvent) => void;
} {
	const handlers = new Map<RunEventName, Set<(event: RunEvent) => void | Promise<void>>>();
	const events: EngineEvents = {
		on(name, handler) {
			let set = handlers.get(name);
			if (!set) {
				set = new Set();
				handlers.set(name, set);
			}
			set.add(handler);
			return () => {
				set?.delete(handler);
			};
		},
	};
	const engine = { events, logger: engineLogger } as unknown as Engine;
	const emit = (event: RunEvent) => {
		for (const handler of handlers.get(event.name) ?? []) void handler(event);
	};
	return { engine, emit };
}

function event(overrides: Partial<RunEvent> & Pick<RunEvent, "name">): RunEvent {
	return {
		runId: "11111111-1111-1111-1111-111111111111",
		taskId: "my-task",
		status: "completed",
		attempt: 1,
		error: null,
		updatedAt: new Date("2026-01-01T00:00:00.000Z"),
		...overrides,
	};
}

describe("attachDefaultLogger", () => {
	it("uses engine.logger by default", () => {
		const { logger, calls } = fakeLogger();
		const { engine, emit } = fakeEngine(logger);
		const off = attachDefaultLogger(engine);
		emit(event({ name: "run:started" }));
		expect(calls).toHaveLength(1);
		expect(calls[0]?.level).toBe("info");
		off();
	});

	it("prefers an explicitly passed logger over engine.logger", () => {
		const { logger: engineLogger, calls: engineCalls } = fakeLogger();
		const { logger: explicitLogger, calls: explicitCalls } = fakeLogger();
		const { engine, emit } = fakeEngine(engineLogger);
		const off = attachDefaultLogger(engine, explicitLogger);
		emit(event({ name: "run:started" }));
		expect(engineCalls).toHaveLength(0);
		expect(explicitCalls).toHaveLength(1);
		off();
	});

	it("logs run:started/completed at info, with durationMs on completed", () => {
		const { logger, calls } = fakeLogger();
		const { engine, emit } = fakeEngine(logger);
		const off = attachDefaultLogger(engine);

		emit(event({ name: "run:started" }));
		emit(event({ name: "run:completed", durationMs: 42.5, startedAt: new Date() }));

		expect(calls.map(c => c.level)).toEqual(["info", "info"]);
		expect(calls[0]?.fields).toEqual({ runId: expect.any(String), taskId: "my-task", attempt: 1 });
		expect(calls[1]?.fields).toMatchObject({ durationMs: 42.5 });
		off();
	});

	it("logs run:failed at error and run:retrying at warn, both with durationMs and a trimmed error", () => {
		const { logger, calls } = fakeLogger();
		const { engine, emit } = fakeEngine(logger);
		const off = attachDefaultLogger(engine);

		const fullError = {
			name: "Boom",
			message: "kaboom",
			stack: "Boom: kaboom\n    at secret/internal/path.ts:1:1",
			retryable: false,
		};
		emit(event({ name: "run:failed", error: fullError, durationMs: 10, status: "failed" }));
		emit(event({ name: "run:retrying", error: fullError, durationMs: 5, status: "queued" }));

		expect(calls.map(c => c.level)).toEqual(["error", "warn"]);
		for (const call of calls) {
			expect(call.fields.error).toEqual({ name: "Boom", message: "kaboom" });
			// Never the full SerializedError (no stack, no retryable) and never a payload/output field.
			expect(call.fields).not.toHaveProperty("output");
			expect(call.fields).not.toHaveProperty("payload");
			expect(JSON.stringify(call.fields)).not.toContain("secret/internal/path");
		}
		off();
	});

	it("logs run:canceled at warn without an error field", () => {
		const { logger, calls } = fakeLogger();
		const { engine, emit } = fakeEngine(logger);
		const off = attachDefaultLogger(engine);
		emit(event({ name: "run:canceled", status: "canceled" }));
		expect(calls).toHaveLength(1);
		expect(calls[0]?.level).toBe("warn");
		expect(calls[0]?.fields).not.toHaveProperty("error");
		off();
	});

	it("does not log run:queued or run:waiting", () => {
		const { logger, calls } = fakeLogger();
		const { engine, emit } = fakeEngine(logger);
		const off = attachDefaultLogger(engine);
		emit(event({ name: "run:queued" }));
		emit(event({ name: "run:waiting", status: "waiting" }));
		expect(calls).toHaveLength(0);
		off();
	});

	it("stops logging after unsubscribe (idempotent)", () => {
		const { logger, calls } = fakeLogger();
		const { engine, emit } = fakeEngine(logger);
		const off = attachDefaultLogger(engine);
		off();
		off(); // idempotent
		emit(event({ name: "run:started" }));
		expect(calls).toHaveLength(0);
	});

	it("catches a throwing logger call and reports it once via logger.error, never throwing", () => {
		const calls: Call[] = [];
		const logger: EngineLogger = {
			debug: () => {},
			info: () => {
				throw new Error("broken logger");
			},
			warn: () => {},
			error: (fields, message) => {
				calls.push({ level: "error", fields, message });
			},
		};
		const { engine, emit } = fakeEngine(logger);
		const off = attachDefaultLogger(engine);
		expect(() => emit(event({ name: "run:started" }))).not.toThrow();
		expect(calls).toHaveLength(1);
		expect(calls[0]?.fields).toMatchObject({ event: "run:started" });
		off();
	});
});
