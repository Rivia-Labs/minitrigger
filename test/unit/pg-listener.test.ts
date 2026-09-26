import { EventEmitter } from "node:events";
import type { Client, ClientConfig } from "pg";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { EngineLogger } from "../../src/config.js";
import { outboxChannel } from "../../src/outbox.js";
import { channelName, createPgListener } from "../../src/pg/listener.js";

const silent: EngineLogger = {
	debug: () => {},
	info: () => {},
	warn: () => {},
	error: () => {},
};

/**
 * Minimal double of `pg.Client`: an `EventEmitter` (so `.emit("notification"|"end"|"error", ...)`
 * drives the listener exactly like a real socket event) plus just enough of the client surface
 * (`connect`, `query`, `end`, `connection.stream.destroy`) for `createPgListener`. No such helper
 * existed in `test/unit/outbox.test.ts` at the time of this task (its only fake-client-shaped test
 * builds real `pg.Client` instances that fail to connect); this is a fresh, reusable double kept
 * local to this file since nothing else needs it yet.
 */
class FakeClient extends EventEmitter {
	queries: string[] = [];
	connection = { stream: { destroy: () => {} } };
	private readonly connectImpl: () => Promise<void>;
	private readonly endImpl: () => Promise<void>;

	constructor(options: { connect?: () => Promise<void>; end?: () => Promise<void> } = {}) {
		super();
		this.connectImpl = options.connect ?? (async () => {});
		this.endImpl = options.end ?? (async () => {});
	}

	connect(): Promise<void> {
		return this.connectImpl();
	}

	query(text: string): Promise<{ rows: unknown[] }> {
		this.queries.push(text);
		return Promise.resolve({ rows: [] });
	}

	end(): Promise<void> {
		return this.endImpl();
	}
}

function fakeClientFactory(
	makeOptions?: () => { connect?: () => Promise<void>; end?: () => Promise<void> }
): {
	factory: (config: ClientConfig) => Client;
	clients: FakeClient[];
} {
	const clients: FakeClient[] = [];
	const factory = (_config: ClientConfig) => {
		const client = new FakeClient(makeOptions?.());
		clients.push(client);
		return client as unknown as Client;
	};
	return { factory, clients };
}

describe("createPgListener", () => {
	afterEach(() => {
		vi.useRealTimers();
	});

	it("LISTENs every channel and dispatches notifications by channel", async () => {
		const { factory, clients } = fakeClientFactory();
		const seen: Array<[string, string]> = [];
		const listener = createPgListener({
			clientConfig: {},
			channels: ["a", "b"],
			logger: silent,
			onNotification: (c, p) => seen.push([c, p]),
			createClient: factory,
		});
		await listener.start();
		expect(clients[0]?.queries).toEqual(['LISTEN "a"', 'LISTEN "b"']);
		clients[0]?.emit("notification", { channel: "b", payload: "x" });
		expect(seen).toEqual([["b", "x"]]);
		await listener.close();
	});

	it("a throwing onNotification is logged and does not kill the client", async () => {
		const { factory, clients } = fakeClientFactory();
		const warnings: unknown[] = [];
		const logger: EngineLogger = {
			...silent,
			warn: (_fields, message) => void warnings.push(message),
		};
		const listener = createPgListener({
			clientConfig: {},
			channels: ["a"],
			logger,
			onNotification: () => {
				throw new Error("boom from onNotification");
			},
			createClient: factory,
		});
		await listener.start();
		clients[0]?.emit("notification", { channel: "a", payload: "x" });
		expect(warnings.length).toBeGreaterThan(0);
		expect(listener.isListening()).toBe(true);
		await listener.close();
	});

	it("reconnects with backoff after 'end' and re-LISTENs; onConnected fires each time", async () => {
		vi.useFakeTimers();
		const { factory, clients } = fakeClientFactory();
		let connectedCount = 0;
		const listener = createPgListener({
			clientConfig: {},
			channels: ["a"],
			logger: silent,
			onNotification: () => {},
			onConnected: () => {
				connectedCount++;
			},
			reconnectMinMs: 500,
			reconnectMaxMs: 500,
			createClient: factory,
		});
		await listener.start();
		expect(clients.length).toBe(1);
		expect(connectedCount).toBe(1);

		clients[0]?.emit("end");
		expect(listener.isListening()).toBe(false);

		// `base + jitter` (jitter up to `min(250, base)`, see `scheduleReconnect`): advance past the
		// worst case, not just `reconnectMinMs`.
		await vi.advanceTimersByTimeAsync(750);
		expect(clients.length).toBe(2);
		expect(clients[1]?.queries).toEqual(['LISTEN "a"']);
		expect(connectedCount).toBe(2);

		await listener.close();
	});

	it("no reconnect after close(); close() is bounded", async () => {
		const { factory, clients } = fakeClientFactory(() => ({
			end: () => new Promise<void>(() => {}), // never resolves
		}));
		const listener = createPgListener({
			clientConfig: {},
			channels: ["a"],
			logger: silent,
			onNotification: () => {},
			closeTimeoutMs: 30,
			createClient: factory,
		});
		await listener.start();
		expect(clients.length).toBe(1);

		const startedAt = Date.now();
		await listener.close();
		expect(Date.now() - startedAt).toBeLessThan(2_000);

		// Emitting "end" post-close never schedules a reconnect.
		clients[0]?.emit("end");
		await new Promise(resolve => setTimeout(resolve, 20));
		expect(clients.length).toBe(1);
	});

	it("channelName matches the outbox channel and hashes long schemas", () => {
		expect(channelName("outbox", "abc")).toBe(outboxChannel("abc"));
		expect(channelName("wake", "a".repeat(60))).toMatch(/^mt_wake_h_[0-9a-f]{48}$/);
	});
});
