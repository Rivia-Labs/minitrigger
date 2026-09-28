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

	it("subscribe() routes notifications of its channels to the handler until unsubscribed", async () => {
		const { factory, clients } = fakeClientFactory();
		const main: Array<[string, string]> = [];
		const listener = createPgListener({
			clientConfig: {},
			channels: ["wake", "status", "cancel"],
			logger: silent,
			onNotification: (c, p) => main.push([c, p]),
			createClient: factory,
		});
		const seen: Array<[string, string]> = [];
		const throwing = listener.subscribe(["status"], () => {
			throw new Error("boom");
		});
		const off = listener.subscribe(["status", "cancel"], (c, p) => seen.push([c, p]));
		await listener.start();
		clients[0]?.emit("notification", { channel: "status", payload: "1" });
		clients[0]?.emit("notification", { channel: "cancel", payload: "2" });
		clients[0]?.emit("notification", { channel: "wake", payload: "3" });
		expect(seen).toEqual([
			["status", "1"],
			["cancel", "2"],
		]);
		// The main callback sees everything, subscribers only their channels.
		expect(main).toHaveLength(3);
		off();
		throwing();
		clients[0]?.emit("notification", { channel: "status", payload: "4" });
		expect(seen).toHaveLength(2);
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

	it("reconnectNow() skips the backoff: connects right away and resets the attempt count", async () => {
		vi.useFakeTimers();
		let failures = 1;
		const { factory, clients } = fakeClientFactory(() => ({
			connect: async () => {
				if (failures > 0) {
					failures--;
					throw new Error("refused");
				}
			},
		}));
		const listener = createPgListener({
			clientConfig: {},
			channels: ["a"],
			logger: silent,
			reconnectMinMs: 30_000,
			reconnectMaxMs: 30_000,
			createClient: factory,
		});
		await listener.start(); // fails; the next attempt is 30 s away
		expect(listener.isListening()).toBe(false);
		expect(clients).toHaveLength(1);

		listener.reconnectNow();
		await vi.advanceTimersByTimeAsync(0);
		expect(clients).toHaveLength(2);
		expect(listener.isListening()).toBe(true);
		// The pending backoff timer was cleared: no third client later.
		await vi.advanceTimersByTimeAsync(60_000);
		expect(clients).toHaveLength(2);
		// Already listening (or connecting): a no-op.
		listener.reconnectNow();
		await vi.advanceTimersByTimeAsync(0);
		expect(clients).toHaveLength(2);
		await listener.close();
		listener.reconnectNow(); // after close(): never reconnects
		await vi.advanceTimersByTimeAsync(0);
		expect(clients).toHaveLength(2);
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
