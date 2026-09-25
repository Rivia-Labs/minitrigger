import { describe, expect, it } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { validateTransactionClient } from "../../src/engine.js";
import { ConfigError } from "../../src/errors.js";
import { createOutboxListener, outboxChannel } from "../../src/outbox.js";
import { defineTask } from "../../src/task.js";
import { createTestEngine } from "../../src/testing/test-engine.js";

describe("outboxChannel", () => {
	it("is readable when it fits and hashed (fixed length) when it does not", () => {
		expect(outboxChannel("minitrigger")).toBe("mt_outbox_s_minitrigger");
		const long = "a".repeat(63);
		const hashed = outboxChannel(long);
		expect(hashed).toMatch(/^mt_outbox_h_[0-9a-f]{48}$/);
		expect(Buffer.byteLength(hashed)).toBeLessThanOrEqual(63);
		expect(outboxChannel("b".repeat(63))).not.toBe(hashed);
		expect(Buffer.byteLength(outboxChannel("c".repeat(51)))).toBe(63);
	});

	it("rejects an invalid schema", () => {
		expect(() => outboxChannel("Bad-Schema")).toThrow(ConfigError);
	});
});

describe("validateTransactionClient", () => {
	it("accepts anything with query() except a pg.Pool", () => {
		const client = { query: async () => ({ rows: [] }) };
		expect(validateTransactionClient(client)).toBe(client);
		expect(() => validateTransactionClient(null)).toThrow(ConfigError);
		expect(() => validateTransactionClient({ query: 1 })).toThrow(ConfigError);
		expect(() =>
			validateTransactionClient({ query: async () => ({ rows: [] }), totalCount: 0, idleCount: 0 })
		).toThrow(/pg\.Pool/);
	});
});

describe("createTestEngine", () => {
	it("rejects trigger({ tx }) with an actionable ConfigError", async () => {
		const task = defineTask({ id: "t", run: async () => 1 });
		const engine = createTestEngine({ tasks: [task] });
		try {
			const tx = { query: async () => ({ rows: [] }) };
			await expect(engine.trigger(task, undefined, { tx })).rejects.toThrow(
				/not supported by this engine \(createTestEngine/
			);
		} finally {
			await engine.stop();
		}
	});
});

describe("createOutboxListener", () => {
	it("a synchronous throw while building the client warns and retries, never rejects", async () => {
		const warnings: string[] = [];
		const logger: EngineLogger = {
			debug: (_f, m) => void warnings.push(m),
			info: () => {},
			warn: (_f, m) => void warnings.push(m),
			error: (_f, m) => void warnings.push(m),
		};
		const unhandled: unknown[] = [];
		const onUnhandled = (reason: unknown) => void unhandled.push(reason);
		process.on("unhandledRejection", onUnhandled);
		let created = 0;
		const listener = createOutboxListener({
			clientConfig: { host: "127.0.0.1", port: 1 },
			channel: "mt_outbox_s_unit",
			logger,
			pollIntervalMs: 60_000,
			reconnectMinMs: 5,
			reconnectMaxMs: 10,
			dispatchBatch: async () => false,
			onClientCreated: () => {
				created++;
				throw new Error("boom from onClientCreated");
			},
		});
		try {
			await expect(listener.start()).resolves.toBeUndefined();
			// The reconnect timer path (`connect()` from a timer) hits the same throw.
			const deadline = Date.now() + 2_000;
			while (created < 3 && Date.now() < deadline) await new Promise(r => setTimeout(r, 10));
			expect(created).toBeGreaterThanOrEqual(3);
			expect(listener.isListening()).toBe(false);
			expect(warnings.some(m => m.includes("could not LISTEN"))).toBe(true);
			await new Promise(r => setTimeout(r, 20));
			expect(unhandled).toEqual([]);
		} finally {
			await listener.close();
			process.off("unhandledRejection", onUnhandled);
		}
	});
});
