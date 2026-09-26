/**
 * The Redis + BullMQ backend (`transport: "bullmq"`, the default), moved out of `createEngine`
 * (spec §3.1): Redis connections, the BullMQ transport, Redis Streams, Redis pub/sub, the Redis
 * prefix ownership check and the decision-26 outbox listener. `createEngine` owns the lifecycle
 * (step order, leader loop, hand-back, pool) and calls the hooks of {@link BackendRuntime}.
 *
 * This is the only module that reaches `bullmq`/`ioredis` (through `src/queue/*`, `pubsub.ts`,
 * `streams.ts`).
 * @internal
 */

import type { Redis } from "ioredis";
import type { ClientConfig } from "pg";
import { DEFAULTS } from "../config.js";
import type { EngineBackend } from "../engine.js";
import { checkEnvironment } from "../env-check.js";
import { ConfigError, MinitriggerError } from "../errors.js";
import { createOutboxListener, type OutboxListener, outboxChannel } from "../outbox.js";
import { createPubSub, createPubSubPublisher, type PubSub } from "../pubsub.js";
import {
	closeConnection,
	createRedisConnections,
	type RedisConnections,
} from "../queue/connections.js";
import { assertRedisReachable } from "../queue/redis-check.js";
import { createTransport, type Transport } from "../queue/transport.js";
import { createSqlTxRunStore } from "../store/sql/index.js";
import type { Json } from "../store/types.js";
import { createStreams } from "../streams.js";
import { dedicatedClientConfig, recommendedPoolMax, withTimeout } from "./shared.js";
import type { BackendContext, BackendRuntime, BackendStartStep } from "./types.js";

/** Review 3 item 1: bound on the pub/sub subscriber's SUBSCRIBE in start() (same as the PING). */
const SUBSCRIBE_READY_TIMEOUT_MS = 5_000;
/** At most one "outbox enqueue failed" warn per this window (Redis down: every poll fails). */
const OUTBOX_ENQUEUE_WARN_THROTTLE_MS = 10_000;
/** How many run ids that warn lists. */
const OUTBOX_ENQUEUE_WARN_MAX_IDS = 20;

/** Builds the BullMQ backend. Throws `ConfigError` without a `redis` config. @internal */
export function createBullmqBackend(ctx: BackendContext): BackendRuntime {
	const { resolved, registry, pool, ownsPool, store, logger } = ctx;
	if (resolved.redis === undefined) {
		throw new ConfigError('redis is required with transport "bullmq"');
	}
	const allQueues = Array.from(registry.queues.values());

	const connections = createRedisConnections(resolved.redis, {
		onError: (error, purpose) =>
			logger.debug({ err: error, purpose }, "redis connection error (ioredis reconnects)"),
	});
	/**
	 * Review 3 item 1: the workers' dedicated connections, so a start() that fails after creating
	 * some workers closes them at once (`closeWorkers` only runs on stop()).
	 */
	const workerConnections = new Set<Redis>();
	const transportConnections: RedisConnections = {
		shared: () => connections.shared(),
		dedicated: purpose => {
			const connection = connections.dedicated(purpose);
			if (purpose === "worker") workerConnections.add(connection);
			return connection;
		},
		closeAll: () => connections.closeAll(),
	};
	const transport: Transport = createTransport({
		prefix: resolved.prefix,
		connections: transportConnections,
		queues: Array.from(registry.queues.values()),
		systemConcurrency: resolved.systemConcurrency,
		schema: resolved.schema,
		logger,
	});

	const streams = createStreams({ redis: connections.shared(), prefix: resolved.prefix });
	/** Decision 26: the schema's NOTIFY channel and, once started, its listener. */
	const outbox: { channel: string; listener: OutboxListener | undefined } = {
		channel: outboxChannel(resolved.schema),
		listener: undefined,
	};
	/** The pub/sub subscriber connection, once created (disconnected directly by `stop()`). */
	let subscriberConnection: Redis | undefined;
	/**
	 * Dedicated connections of the open `runs.readStream` iterations, each with the controller that
	 * `stop()` aborts before disconnecting it, so the iteration ends cleanly (like
	 * `runs.subscribe`) instead of rejecting with "Connection is closed".
	 */
	const streamReaders = new Map<Redis, AbortController>();

	/** `runs.readStream`: one dedicated connection per reader (decision 14), closed at the end. */
	async function* readRunStream(
		runId: string,
		name: string,
		signal: AbortSignal | undefined
	): AsyncGenerator<string | Json, void, undefined> {
		// First iterated after stop() began: nothing would disconnect this reader any more.
		if (signal?.aborted || ctx.isShuttingDown()) return;
		// A duplicate (not `connections.dedicated`, which keeps every connection until stop()):
		// readers come and go, so each one is closed as soon as its iteration ends.
		const reader = connections.shared().duplicate({ maxRetriesPerRequest: null });
		reader.on("error", (error: unknown) =>
			logger.debug({ err: error, runId, name }, "stream reader connection error")
		);
		// Aborted by stop() only: a real end-with-error marker still throws out of `streams.read`.
		const stopController = new AbortController();
		streamReaders.set(reader, stopController);
		const readSignal = signal
			? AbortSignal.any([signal, stopController.signal])
			: stopController.signal;
		try {
			yield* streams.read(runId, name, { redis: reader, signal: readSignal });
		} finally {
			streamReaders.delete(reader);
			reader.disconnect();
		}
	}

	const backend: EngineBackend = {
		store,
		transport,
		// Publishing uses the shared connection, so a trigger-only engine (decision 19) still
		// publishes what it causes; only a started engine subscribes (dedicated connection).
		publisher: createPubSubPublisher({
			publisher: connections.shared(),
			prefix: resolved.prefix,
		}),
		createPubSub() {
			const subscriber = connections.dedicated("subscriber");
			// Its connection errors go to `logger.debug` (`createRedisConnections`' `onError`);
			// ioredis reconnects and re-subscribes on its own.
			subscriberConnection = subscriber;
			return createPubSub({
				publisher: connections.shared(),
				subscriber,
				prefix: resolved.prefix,
				logger,
			});
		},
		streams,
		readStream: readRunStream,
		onScaled: () => warnScaledPoolSizing(),
		createRunInTransaction: (tx, input) =>
			createSqlTxRunStore(tx, resolved.schema).create({
				...input,
				outbox: { channel: outbox.channel, notify: resolved.outbox.notify },
			}),
	};

	/**
	 * Review item 3: does THIS engine currently, exclusively own the Redis prefix? Bound to the
	 * same `${prefix}:owner-schema` key `claimPrefix()` writes (no TTL). `foreignScheduleState`
	 * only ever sees this engine's own database, so a foreign schema living in ANOTHER
	 * database (blue/green: old engine on database A, new one on database B, same prefix) always
	 * looks orphaned to it even while the old engine is still running -- this closes that gap: a
	 * foreign-tagged scheduler is only ever removed when this engine holds the prefix's owner key,
	 * i.e. nothing else currently claims it. A Redis error here is treated as "not the owner" (the
	 * safe default: keep the scheduler); logged once at `debug` to avoid repeating on every tick.
	 */
	let isPrefixOwnerErrorLogged = false;
	const isPrefixOwner = async (): Promise<boolean> => {
		try {
			const owner = await connections.shared().get(`${resolved.prefix}:owner-schema`);
			return owner === resolved.schema;
		} catch (error) {
			if (!isPrefixOwnerErrorLogged) {
				isPrefixOwnerErrorLogged = true;
				logger.debug(
					{ err: error, prefix: resolved.prefix },
					"could not check the Redis prefix owner key; treating a foreign-tagged scheduler as " +
						"still live (kept)"
				);
			}
			return false;
		}
	};

	async function beforeStart(step: BackendStartStep): Promise<void> {
		// Decision 22: a check that cannot run only warns; a wrong answer throws (Postgres
		// version, Redis eviction policy, pending migrations -- `autoMigrate: false` because
		// `start()` does not call `migrate()` itself; a pending migration must be applied via
		// `engine.migrate()` first, and the thrown `ConfigError` says so).
		// `checkEnvironment` itself calls `logger.warn` per warning; nothing else to do with the
		// returned report here.
		await assertRedisReachable(connections.shared());
		step.assertNotAbandoned();
		await checkEnvironment({
			pool,
			redis: connections.shared(),
			schema: resolved.schema,
			logger,
			autoMigrate: false,
		});
		step.assertNotAbandoned();
		await claimPrefix();
		step.assertNotAbandoned();
	}

	/**
	 * Audit item 8: a Redis `prefix` belongs to one schema. The `-system` queue (schedule ticks,
	 * waitpoint expirations, retention) and the task queues are keyed by prefix only, so a second
	 * schema on the same prefix would consume the first one's jobs. The first `start()` records its
	 * schema under `${prefix}:owner-schema` (no TTL); a start() with another schema fails. A Redis
	 * error here only warns (decision 22).
	 */
	async function claimPrefix(): Promise<void> {
		const key = `${resolved.prefix}:owner-schema`;
		let owner: string | null;
		try {
			const redis = connections.shared();
			const set = await redis.set(key, resolved.schema, "NX");
			owner = set === "OK" ? resolved.schema : await redis.get(key);
		} catch (error) {
			logger.warn(
				{ err: error, prefix: resolved.prefix },
				"could not check which schema owns this Redis prefix; skipping the check"
			);
			return;
		}
		if (owner !== null && owner !== resolved.schema) {
			throw new ConfigError(
				`Redis prefix "${resolved.prefix}" is already used by schema "${owner}", not ` +
					`"${resolved.schema}": each schema needs its own prefix (they share queues and ` +
					"schedule ticks otherwise). Use another prefix, or, if schema " +
					`"${owner}" was retired, delete the Redis key "${key}"`
			);
		}
	}

	/**
	 * Low item (b): warns when the pool is smaller than the README's sizing guide (total worker
	 * concurrency of the started queues + 5) and, for an injected pool, when it has no
	 * `connectionTimeoutMillis` (a heartbeat would then wait for a connection without limit instead
	 * of failing with a warn, decision 6). The engine-created pool is sized and bounded by default.
	 */
	function warnPoolSizing(queueFilter: readonly string[] | undefined): void {
		const started = queueFilter ? allQueues.filter(q => queueFilter.includes(q.name)) : allQueues;
		const recommended = recommendedPoolMax(started);
		const poolOptions = pool.options as { max?: number; connectionTimeoutMillis?: number };
		const max = ownsPool
			? (resolved.poolMax ?? recommendedPoolMax(allQueues))
			: (poolOptions.max ?? DEFAULTS.poolMax);
		if (max < recommended) {
			logger.warn(
				{ poolMax: max, recommended, injected: !ownsPool },
				`postgres pool.max (${max}) is below the total worker concurrency + ` +
					`${DEFAULTS.poolHeadroom} (${recommended}): runs will wait for connections and ` +
					"heartbeats may fail; raise pool.max or lower the queues' concurrency"
			);
		}
		if (!ownsPool && !poolOptions.connectionTimeoutMillis) {
			logger.warn(
				{},
				"the injected pg.Pool has no connectionTimeoutMillis: on a saturated pool, heartbeats " +
					"wait for a connection without limit instead of failing with a warning; set one " +
					"(e.g. 10000)"
			);
		}
	}

	/** `queues.scale()`: the same pool guide as {@link warnPoolSizing}, over the live worker concurrency. */
	function warnScaledPoolSizing(): void {
		const concurrency = allQueues.reduce(
			(sum, q) => sum + (transport.workerConcurrency(q.name) ?? 0),
			0
		);
		const recommended = Math.max(DEFAULTS.poolMax, concurrency + DEFAULTS.poolHeadroom);
		const poolOptions = pool.options as { max?: number };
		const max = ownsPool
			? (resolved.poolMax ?? recommendedPoolMax(allQueues))
			: (poolOptions.max ?? DEFAULTS.poolMax);
		if (max < recommended) {
			logger.warn(
				{ poolMax: max, recommended },
				`postgres pool.max (${max}) is below this process's worker concurrency + ` +
					`${DEFAULTS.poolHeadroom} (${recommended}) after queues.scale(): runs will wait for ` +
					"connections and heartbeats may fail"
			);
		}
	}

	async function awaitSubscriberReady(ps: PubSub): Promise<void> {
		// Review 3 item 1: bounded like the PING. The shared connection works, but a new one may
		// never become ready (Redis maxclients, a drop right after the PING); the SUBSCRIBE would
		// then wait forever (`maxRetriesPerRequest: null`), and start() with it.
		if (!(await withTimeout(ps.ready, SUBSCRIBE_READY_TIMEOUT_MS))) {
			// Review 4 item 2: transient infrastructure, not a configuration mistake: a
			// `MinitriggerError` (not `ConfigError`), so callers that treat `ConfigError` as
			// fatal still retry start().
			throw new MinitriggerError(
				`the Redis pub/sub subscriber did not become ready within ${SUBSCRIBE_READY_TIMEOUT_MS} ` +
					"ms, although PING on the shared connection worked: Redis may refuse new " +
					"connections (check maxclients / CLIENT LIST) or the network dropped; fix it, " +
					"then call start() again"
			);
		}
	}

	function dropSubscriber(): void {
		// Item 7: the subscriber is dedicated to this engine; dropped at once (a QUIT would wait in
		// ioredis' offline queue while Redis is unreachable).
		subscriberConnection?.disconnect();
		subscriberConnection = undefined;
	}

	async function closeWorkerConnections(): Promise<void> {
		const opened = Array.from(workerConnections);
		workerConnections.clear();
		await Promise.all(opened.map(connection => closeConnection(connection)));
	}

	/**
	 * Decision 26: claims committed `trigger(..., { tx })` runs, publishes `queued` (the transition
	 * the caller's `trigger` could not publish before its commit) and enqueues them. An enqueue lost
	 * here is recovered by the reconciler (decision 1): the flag is already cleared.
	 */
	async function dispatchOutboxBatch(signal?: AbortSignal): Promise<boolean> {
		// close() already began: claim nothing more (a claimed run would only be enqueued by us).
		if (signal?.aborted) return false;
		const claim = await store.runs.claimOutbox(resolved.outbox.batchSize);
		const failures: Array<{ runId: string; error: unknown }> = [];
		await Promise.all(
			claim.enqueue.map(async item => {
				ctx.notifyStatus({
					runId: item.runId,
					taskId: item.taskId,
					status: item.status,
					attempt: item.attempt,
					error: item.error,
					updatedAt: item.updatedAt,
					transition: "queued",
				});
				// Directly (not `enqueue()`, which warns per run): with Redis down, one warn per batch.
				try {
					await transport.enqueueRun({
						queue: item.queue,
						runId: item.runId,
						seq: item.seq,
						delayMs: item.delayMs,
					});
				} catch (error) {
					failures.push({ runId: item.runId, error });
				}
			})
		);
		if (failures.length > 0) warnOutboxEnqueueFailures(failures);
		return claim.claimed >= resolved.outbox.batchSize;
	}

	/** Last time {@link warnOutboxEnqueueFailures} logged, and the failures suppressed since. */
	let outboxEnqueueWarnAt = Number.NEGATIVE_INFINITY;
	let outboxEnqueueSuppressed = 0;
	/** One throttled warn per outbox batch whose enqueues failed (e.g. Redis down). */
	function warnOutboxEnqueueFailures(
		failures: ReadonlyArray<{ runId: string; error: unknown }>
	): void {
		const now = Date.now();
		if (now - outboxEnqueueWarnAt < OUTBOX_ENQUEUE_WARN_THROTTLE_MS) {
			outboxEnqueueSuppressed += failures.length;
			return;
		}
		outboxEnqueueWarnAt = now;
		const suppressed = outboxEnqueueSuppressed;
		outboxEnqueueSuppressed = 0;
		logger.warn(
			{
				failed: failures.length,
				err: failures[0]?.error,
				runIds: failures.slice(0, OUTBOX_ENQUEUE_WARN_MAX_IDS).map(f => f.runId),
				...(suppressed > 0 ? { suppressedSinceLastWarn: suppressed } : {}),
			},
			"outbox: enqueueing claimed runs failed; the reconciler re-enqueues them from Postgres"
		);
	}

	/**
	 * Decision 26: the LISTEN client is dedicated (LISTEN is session state; never a pooled
	 * connection, like the leader's, decision 16) and owned by the engine. Not awaited: its first
	 * connection is best-effort (a failure warns and retries; the poll still dispatches).
	 */
	function startOutboxListener(): void {
		if (ctx.isShuttingDown()) return;
		outbox.listener = createOutboxListener({
			clientConfig: dedicatedClientConfig(resolved, pool),
			channel: outbox.channel,
			logger,
			dispatchBatch: dispatchOutboxBatch,
			pollIntervalMs: resolved.outbox.pollIntervalMs,
			reconnectMaxMs: resolved.outbox.reconnectMaxMs,
		});
		// start() never rejects on connection problems (it warns and retries); anything else
		// unexpected is logged rather than left as an unhandled rejection.
		outbox.listener.start().catch(error => {
			logger.warn({ err: error }, "outbox listener failed to start");
		});
	}

	return {
		backend,
		transport,
		beforeStart,
		warnPoolSizing,
		awaitSubscriberReady,
		dropSubscriber,
		onWorkersStartFailed: closeWorkerConnections,
		afterWorkersStarted: startOutboxListener,
		// Decision 26: bounded; a run it did not dispatch stays flagged for the next engine's sweep.
		beforeClose: async () => {
			await outbox.listener?.close();
		},
		closeReaders() {
			for (const [reader, controller] of Array.from(streamReaders)) {
				controller.abort();
				reader.disconnect();
			}
		},
		// The Redis connections are closed by the transport's `closeWorkers`; nothing left here.
		close: async () => {},
		/** Whether the shared Redis connection is up. */
		isReady: () => connections.shared().status === "ready",
		isPrefixOwner,
		dedicatedClientConfig: (): ClientConfig => dedicatedClientConfig(resolved, pool),
	};
}
