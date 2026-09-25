/**
 * Audit low item (c): the leader's dedicated client must use TCP keepalive (so a half-open socket
 * is eventually detected and the advisory lock isn't "held" by a dead peer forever), and
 * `release()`/`close()` must be bounded: a socket that stopped answering can't make `stop()` or a
 * leadership handoff hang.
 */
import { Client } from "pg";
import { describe, expect, it } from "vitest";
import {
	createLeaderElector,
	LEADER_KEEPALIVE_INITIAL_DELAY_MS,
	leaderClientConfig,
} from "../../src/leader.js";
import { useTestNamespace, waitFor } from "../helpers/namespace.js";

const ns = useTestNamespace();

describe("leader client keepalive", () => {
	it("leaderClientConfig turns TCP keepalive on over any base config", () => {
		expect(leaderClientConfig({ connectionString: "postgres://x" })).toEqual({
			connectionString: "postgres://x",
			keepAlive: true,
			keepAliveInitialDelayMillis: LEADER_KEEPALIVE_INITIAL_DELAY_MS,
		});
		expect(leaderClientConfig({ keepAlive: false, host: "h" })).toMatchObject({
			host: "h",
			keepAlive: true,
		});
	});

	it("builds its own clients with keepalive (connectionString and clientConfig forms)", async () => {
		for (const config of [
			{ connectionString: ns.databaseUrl },
			{ clientConfig: { connectionString: ns.databaseUrl } },
		]) {
			const created: Client[] = [];
			const elector = createLeaderElector({
				...config,
				schema: ns.schema,
				onClientCreated: c => created.push(c),
			});
			try {
				expect(await elector.tryAcquire()).toBe(true);
				const internal = created[0]?.connection as unknown as { _keepAlive?: boolean };
				expect(internal._keepAlive).toBe(true);
			} finally {
				await elector.close();
			}
		}
	});
});

describe("leader release on an unresponsive socket", () => {
	it("close() returns within the release timeout and the lock is freed once the socket dies", async () => {
		let client: Client | undefined;
		const elector = createLeaderElector({
			clientFactory: () => {
				client = new Client({ connectionString: ns.databaseUrl });
				return client;
			},
			schema: ns.schema,
			releaseTimeoutMs: 300,
		});
		expect(await elector.tryAcquire()).toBe(true);
		// Simulate a half-open connection: nothing the server sends is read any more, so the
		// unlock query and end() never complete on their own.
		client?.connection.stream.pause();

		const started = Date.now();
		await elector.close();
		expect(Date.now() - started).toBeLessThan(2_000);
		expect(elector.isLeader()).toBe(false);

		// The socket was destroyed: the server ends the session and the lock becomes free.
		const other = createLeaderElector({ connectionString: ns.databaseUrl, schema: ns.schema });
		try {
			await waitFor(() => other.tryAcquire(), { timeoutMs: 5_000 });
			expect(other.isLeader()).toBe(true);
		} finally {
			await other.close();
		}
	});
});
