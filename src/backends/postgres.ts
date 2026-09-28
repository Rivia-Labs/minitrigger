/**
 * The Postgres-only backend (`transport: "postgres"`, decision 28; spec §3.1, §3.7, §3.8): the SQL
 * store, the Postgres transport (claim loops over `runs` and `system_jobs`), pub/sub by batched
 * `NOTIFY` and one dedicated `LISTEN` connection for the wake/status/cancel channels. No streams
 * (`ctx.stream`/`runs.readStream` are `ConfigError`). `createEngine` owns the lifecycle and calls
 * the hooks of {@link BackendRuntime}.
 *
 * Nothing here reaches `bullmq`/`ioredis` at runtime (only `import type`): this module must load
 * without either optional peer installed.
 * @internal
 */

import { randomUUID } from "node:crypto";
import type { ClientConfig } from "pg";
import type { EngineBackend } from "../engine.js";
import { checkEnvironment } from "../env-check.js";
import { ConfigError, MinitriggerError } from "../errors.js";
import { channelName, createPgListener } from "../pg/listener.js";
import { createNotifyBatcher } from "../pg/notify-batcher.js";
import { createPgPublisher, createPgPubSub } from "../pg/pubsub.js";
import { createPgTransport } from "../pg/transport.js";
import { createSqlTxRunStore } from "../store/sql/index.js";
import {
	assertSchemaTransport,
	dedicatedClientConfig,
	warnPoolSizing,
	warnScaledPoolSizing,
	withTimeout,
} from "./shared.js";
import type { BackendContext, BackendRuntime, BackendStartStep } from "./types.js";

/** Spec §3.8: bound on the `LISTEN` connection in `start()` (a timeout is a `MinitriggerError`). */
const LISTEN_READY_TIMEOUT_MS = 5_000;
/** Decision 22 amendment: how long `start()` waits for its own self-test `NOTIFY` to echo back. */
const NOTIFY_SELF_TEST_TIMEOUT_MS = 1_000;
/** `application_name` of the dedicated `LISTEN` connection (visible in `pg_stat_activity`). */
export const LISTENER_APPLICATION_NAME = "minitrigger-listener";

/** Builds the Postgres backend. @internal */
export function createPostgresBackend(ctx: BackendContext): BackendRuntime {
	const { resolved, registry, pool, store, logger } = ctx;
	const allQueues = Array.from(registry.queues.values());
	const wake = channelName("wake", resolved.schema);

	/** Wake consumers (the transport's claim loops): `queue` is a registered queue, or `undefined`. */
	const wakeHandlers = new Set<(queue: string | undefined) => void>();
	/** The `start()` self-test in progress: its unique payload and what to call when it echoes. */
	let selfTest: { token: string; echoed(): void } | undefined;
	/** Pending {@link waitListening} calls; resolved on connect, or ended by `dropSubscriber()`. */
	const listenWaiters = new Set<(listening: boolean) => void>();

	function wakeQueues(queue: string | undefined): void {
		for (const handler of Array.from(wakeHandlers)) {
			try {
				handler(queue);
			} catch (error) {
				logger.warn({ err: error, queue }, "wake handler threw; ignoring");
			}
		}
	}

	/**
	 * Wake channel only (status/cancel reach the pub/sub through `listener.subscribe`). The payload
	 * is an untrusted hint (decision 28(b)): a registered queue wakes just that queue; anything else
	 * (forged, empty, a queue this process does not know) wakes every queue.
	 */
	function onNotification(channel: string, payload: string): void {
		if (channel !== wake) return;
		if (selfTest !== undefined && payload === selfTest.token) {
			selfTest.echoed();
			return;
		}
		wakeQueues(registry.queues.has(payload) ? payload : undefined);
	}

	// One dedicated connection for every channel of the mode (decision 16 amendment): no `outbox`
	// channel, there is no outbox sweep in this mode (decision 28(j)).
	const listener = createPgListener({
		clientConfig: {
			...dedicatedClientConfig(resolved, pool),
			application_name: LISTENER_APPLICATION_NAME,
		},
		channels: (["wake", "status", "cancel"] as const).map(kind =>
			channelName(kind, resolved.schema)
		),
		logger,
		onNotification,
		onConnected: () => {
			for (const resolve of Array.from(listenWaiters)) resolve(true);
			listenWaiters.clear();
			// A (re)connect may have missed notifications: every queue recomputes what is due.
			wakeQueues(undefined);
		},
		connectTimeoutMs: LISTEN_READY_TIMEOUT_MS,
	});

	/** Starts the listener (idempotent; a failed connect only warns and retries in the background). */
	function startListener(): void {
		listener.start().catch((error: unknown) => {
			logger.warn({ err: error }, "pg listener failed to start");
		});
	}

	/** `true` once the listener is `LISTEN`ing, `false` on timeout or `dropSubscriber()`. */
	async function waitListening(timeoutMs: number): Promise<boolean> {
		if (listener.isListening()) return true;
		let settle: ((listening: boolean) => void) | undefined;
		const connected = new Promise<boolean>(resolve => {
			settle = resolve;
			listenWaiters.add(resolve);
		});
		try {
			let timer: ReturnType<typeof setTimeout> | undefined;
			const result = await Promise.race([
				connected,
				new Promise<boolean>(resolve => {
					timer = setTimeout(() => resolve(false), timeoutMs);
				}),
			]);
			clearTimeout(timer);
			return result;
		} finally {
			if (settle) listenWaiters.delete(settle);
		}
	}

	const batcher = createNotifyBatcher({ db: pool, logger });
	const transport = createPgTransport({
		store,
		batcher,
		schema: resolved.schema,
		queues: allQueues,
		systemConcurrency: resolved.systemConcurrency,
		stallTimeoutMs: resolved.stallTimeoutMs,
		polling: resolved.polling,
		wake: {
			subscribe: handler => {
				wakeHandlers.add(handler);
				return () => {
					wakeHandlers.delete(handler);
				};
			},
		},
		logger,
	});
	const publisher = createPgPublisher({ batcher, schema: resolved.schema });

	const backend: EngineBackend = {
		store,
		transport,
		// Publishing needs no started engine: a trigger-only engine (decision 19) still notifies.
		publisher,
		createPubSub() {
			// A trigger-only engine's `runs.subscribe` needs the listener too (started lazily).
			startListener();
			return createPgPubSub({
				publisher,
				store,
				schema: resolved.schema,
				// Review focus 6: the cancel re-read is scoped to runs executing in THIS process.
				isActiveLocally: ctx.isActiveLocally,
				subscribe: (channels, handler) => listener.subscribe(channels, handler),
				logger,
			});
		},
		// Decision 28(f): no `streams` -- `ctx.stream` fails the run without retry (the executor's
		// `streamTo`, review 5 item B5).
		readStream: () => {
			throw new ConfigError(
				'runs.readStream is not available with transport "postgres" (decision 28(f)); use ' +
					'transport "bullmq" for Redis Streams'
			);
		},
		onScaled: () =>
			warnScaledPoolSizing(ctx, allQueues, queue => transport.workerConcurrency(queue)),
		// Decision 28(j): the same insert, with the wake NOTIFY (payload: the queue) delivered on
		// commit and `outbox_pending = false` -- there is no outbox sweep in this mode, so a
		// pending row would never be claimed.
		createRunInTransaction: (tx, input) =>
			createSqlTxRunStore(tx, resolved.schema).create({
				...input,
				outbox: { channel: wake, notify: true, pending: false, payload: input.queue },
			}),
	};

	async function beforeStart(step: BackendStartStep): Promise<void> {
		// Decision 22 (amended): no Redis checks; Postgres version and pending migrations only.
		await checkEnvironment({ pool, schema: resolved.schema, logger, autoMigrate: false });
		step.assertNotAbandoned();
		// Decision 15 amendment: the schema's transport marker (mixed modes refused).
		await assertSchemaTransport(store, resolved);
		step.assertNotAbandoned();
	}

	/**
	 * Spec §3.8: the `LISTEN` connection (bounded, a timeout is a `MinitriggerError` -- transient
	 * infrastructure, never `ConfigError`), then the `NOTIFY` self-test (bounded, warn only), both
	 * before the workers. On a timeout the listener keeps reconnecting in the background (it may
	 * already serve `runs.subscribe` iterations of this engine); a later `start()` skips its
	 * reconnect backoff (`reconnectNow`) and waits for it again, and `stop()` closes it.
	 */
	async function awaitSubscriberReady(): Promise<void> {
		startListener();
		// A start() after a failed one: the listener may sit in its reconnect backoff (up to 30 s)
		// while this start() waits at most LISTEN_READY_TIMEOUT_MS, so try right away instead.
		if (!listener.isListening()) listener.reconnectNow();
		if (!(await waitListening(LISTEN_READY_TIMEOUT_MS))) {
			throw new MinitriggerError(
				`the Postgres LISTEN connection (wake/status/cancel notifications) was not ready within ` +
					`${LISTEN_READY_TIMEOUT_MS} ms: Postgres may refuse new connections ` +
					"(max_connections) or be unreachable; fix it, then call start() again"
			);
		}
		await notifySelfTest();
	}

	/**
	 * Decision 22 amendment: a `NOTIFY` on the wake channel must echo back to our own `LISTEN`
	 * within 1 s. Without it (a transaction-mode pooler in front of the dedicated connection, for
	 * instance), wake-ups only come from the poll: warn, never fail.
	 */
	async function notifySelfTest(): Promise<void> {
		const token = `self-test:${randomUUID()}`;
		let echoed!: () => void;
		const echo = new Promise<void>(resolve => {
			echoed = resolve;
		});
		selfTest = { token, echoed };
		try {
			const sent = pool.query("SELECT pg_notify($1::text, $2::text)", [wake, token]);
			if (
				await withTimeout(
					sent.then(() => echo),
					NOTIFY_SELF_TEST_TIMEOUT_MS
				)
			)
				return;
			logger.warn(
				{ channel: wake, timeoutMs: NOTIFY_SELF_TEST_TIMEOUT_MS },
				"NOTIFY self-test: notifications do not arrive (transaction-mode pooler in front of " +
					"the LISTEN connection?); wake-up latency falls back to the poll (polling.intervalMs)"
			);
		} catch (error) {
			logger.warn(
				{ err: error, channel: wake },
				"NOTIFY self-test could not run; wake-up latency may fall back to the poll " +
					"(polling.intervalMs)"
			);
		} finally {
			selfTest = undefined;
		}
	}

	return {
		backend,
		transport,
		beforeStart,
		warnPoolSizing: queueFilter => warnPoolSizing(ctx, allQueues, queueFilter),
		awaitSubscriberReady,
		// Ends a `start()` waiting for the LISTEN connection (a failed start, or stop() unblocking
		// it). The listener itself is shared with `runs.subscribe` and closed by `stop()` only.
		dropSubscriber() {
			for (const resolve of Array.from(listenWaiters)) resolve(false);
			listenWaiters.clear();
		},
		// No per-worker connections: the claim loops use the pool.
		onWorkersStartFailed: async () => {},
		// The listener and the self-test already ran before the workers (spec §3.8).
		afterWorkersStarted: () => {},
		// After the workers drained (the pg transport calls `beforeClose` last): no more wake-ups or
		// cancels are needed. Bounded by the listener's own close timeout (5 s).
		beforeClose: () => listener.close(),
		closeReaders() {
			// No streams in this mode.
		},
		// After the hand-back: the batcher flushes what is pending (the hand-back's wake NOTIFY,
		// last status publishes) before the engine ends the pool. Bounded (batcher 2 s, listener
		// 5 s, both idempotent).
		close: async () => {
			await Promise.all([batcher.close(), listener.close()]);
		},
		// The transport is the pool; nothing to skip waiting for.
		isReady: () => true,
		// `system_jobs` belongs to this schema alone: nothing else can own its schedule ticks.
		isPrefixOwner: async () => true,
		dedicatedClientConfig: (): ClientConfig => dedicatedClientConfig(resolved, pool),
	};
}
