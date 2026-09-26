/**
 * The seam between `createEngine` (lifecycle, leader loop, hand-back, core) and a transport
 * backend (spec §3.1): `src/backends/bullmq.ts` (Redis + BullMQ) today, `src/backends/postgres.ts`
 * next. Type-only: importing this module never loads `bullmq`/`ioredis`.
 * @internal
 */

import type { ClientConfig, Pool } from "pg";
import type { EngineLogger, ResolvedConfig } from "../config.js";
import type { EngineBackend } from "../engine.js";
import type { RunStatusInfo } from "../executor/index.js";
import type { PubSub } from "../pubsub.js";
import type { Transport } from "../queue/transport.js";
import type { Registry } from "../registry.js";
import type { SqlStore } from "../store/sql/index.js";

/** What `createEngine` hands a backend factory. @internal */
export interface BackendContext {
	resolved: ResolvedConfig;
	registry: Registry;
	pool: Pool;
	/** Whether the engine created `pool` (from a connection string) rather than received it. */
	ownsPool: boolean;
	store: SqlStore;
	logger: EngineLogger;
	/** True once `stop()` began. */
	isShuttingDown(): boolean;
	/**
	 * Runs executing in this process (`core.active`), for the cancel re-read of the Postgres
	 * pub/sub. Bound after `createEngineCore` runs: never call it from the factory itself.
	 */
	isActiveLocally(runId: string): boolean;
	/**
	 * The core's `notifyStatus` (publish + local event). Bound after `createEngineCore` runs:
	 * never call it from the factory itself.
	 */
	notifyStatus(info: RunStatusInfo): void;
}

/** Passed to {@link BackendRuntime.beforeStart}. @internal */
export interface BackendStartStep {
	/**
	 * Throws the lifecycle's `MinitriggerError` once `stop()` was called while `start()` was in
	 * progress (review 4 item 4); called between steps, exactly like `createEngine` does.
	 */
	assertNotAbandoned(): void;
}

/**
 * A transport backend, as `createEngine` drives it. The hooks are called at fixed points of
 * `start()`/`stop()`, whose step order is owned by `createEngine`:
 *
 * `start()`: `beforeStart` -> `warnPoolSizing` -> (outbox config warn, declarative schedules,
 * queue states) -> subscriber (`awaitSubscriberReady`; `dropSubscriber` if it fails) ->
 * workers (`onWorkersStartFailed` if they fail) -> `afterWorkersStarted` -> leader loop.
 *
 * `stop()` (inside the transport's `closeWorkers` `beforeClose`): leader loop stop in parallel
 * with `beforeClose` -> hand-back -> in-flight drain -> subscriptions ended -> `closeReaders` ->
 * pub/sub closed -> `dropSubscriber` -> notification drain (only if `isReady`); then, after
 * `closeWorkers`: `close` -> the engine-owned pool's bounded `end()`.
 * @internal
 */
export interface BackendRuntime {
	/** For `createEngineCore`. */
	backend: EngineBackend;
	transport: Transport;
	/**
	 * First steps of `start()`, before anything is written: reachability, environment checks,
	 * prefix/schema ownership. May throw (start() fails and can be retried).
	 */
	beforeStart(step: BackendStartStep): Promise<void>;
	/** Pool sizing warnings for the queues this process starts (`undefined`: all of them). */
	warnPoolSizing(queues: readonly string[] | undefined): void;
	/**
	 * Waits (bounded) for the subscriber created by `core.getPubSub()` to be ready; throws a
	 * `MinitriggerError` (transient infrastructure, never `ConfigError`) on timeout.
	 */
	awaitSubscriberReady(pubsub: PubSub): Promise<void>;
	/**
	 * Drops the subscriber's own connection at once, if one was created (a failed start(), a
	 * stop() unblocking a start() stuck on it, and stop()'s close sequence).
	 */
	dropSubscriber(): void;
	/** A failed `startWorkers`: closes the connections the partial workers were given. */
	onWorkersStartFailed(): Promise<void>;
	/**
	 * After the workers started, before the leader loop (BullMQ: the outbox listener, started
	 * without being awaited). May return nothing, so no extra microtask separates the two.
	 */
	afterWorkersStarted(): void | Promise<void>;
	/** The `beforeClose` step of `stop()`, in parallel with the leader loop stop; bounded. */
	beforeClose(): Promise<void>;
	/** Ends the open `runs.readStream` iterations (after `runs.subscribe` ones end). */
	closeReaders(): void;
	/** Last step of `stop()` before the pool: the backend's own remaining connections. */
	close(): Promise<void>;
	/** Whether the transport's connection is up (`stop()` skips waits on it when it isn't). */
	isReady(): boolean;
	/**
	 * Whether this engine exclusively owns the transport namespace, for removing a retired
	 * foreign schema's scheduler (review item 3). `false` on doubt (the safe default).
	 */
	isPrefixOwner(): Promise<boolean>;
	/** Config of the engine's dedicated `pg.Client`s (leader). */
	dedicatedClientConfig(): ClientConfig;
}

/** A backend factory. @internal */
export type BackendFactory = (ctx: BackendContext) => BackendRuntime;
