/**
 * A {@link BackendRuntime} whose real backend is loaded on first use (decision 14 amendment:
 * `bullmq`/`ioredis` are optional peers). `createEngine` stays synchronous and builds this
 * wrapper; the module behind it (`src/backends/bullmq.ts`, reached only through a dynamic
 * `import()`) is loaded and the backend built by the first call that needs it (`start()`,
 * `trigger()`, `runs.*`, `schedules.*`, ...), memoized.
 *
 * Before the load:
 * - the synchronous transport getters answer "no worker" (`workerConcurrency` -> `undefined`,
 *   `setWorkerConcurrency` -> `false`): there are no workers before `start()`;
 * - `stop()` never loads it: `closeWorkers` runs the `beforeClose` callback directly and seals the
 *   wrapper (a later load would open connections nothing closes), unless a load was already in
 *   progress, which is then awaited and closed normally.
 *
 * Nothing here imports `bullmq`/`ioredis` at runtime.
 * @internal
 */

import type { ClientConfig } from "pg";
import type { EngineBackend } from "../engine.js";
import { MinitriggerError } from "../errors.js";
import type { PubSub } from "../pubsub.js";
import type { Transport } from "../queue/transport.js";
import type { Json } from "../store/types.js";
import { dedicatedClientConfig } from "./shared.js";
import type { BackendContext, BackendFactory, BackendRuntime } from "./types.js";

/** Builds a runtime that loads `loadFactory()`'s backend on first use. @internal */
export function createLazyBackend(
	ctx: BackendContext,
	loadFactory: () => Promise<BackendFactory>
): BackendRuntime {
	let current: BackendRuntime | undefined;
	let loading: Promise<BackendRuntime> | undefined;
	/** Set by a `closeWorkers` that found nothing loaded: no load may start after it. */
	let sealed = false;

	function ensure(): Promise<BackendRuntime> {
		if (current) return Promise.resolve(current);
		if (loading) return loading;
		if (sealed) {
			return Promise.reject(new MinitriggerError("this engine was stopped; create a new one"));
		}
		const attempt = loadFactory().then(factory => {
			const runtime = factory(ctx);
			current = runtime;
			return runtime;
		});
		loading = attempt;
		// A failed load is not memoized: a later call tries again (and fails the same way if the
		// package is really missing).
		attempt.catch(() => {
			if (loading === attempt) loading = undefined;
		});
		return attempt;
	}

	const transport: Transport = {
		enqueueRun: async input => (await ensure()).transport.enqueueRun(input),
		removeRunJob: async (queue, runId, seq) =>
			(await ensure()).transport.removeRunJob(queue, runId, seq),
		enqueueSystem: async (name, data, opts) =>
			(await ensure()).transport.enqueueSystem(name, data, opts),
		removeSystemJob: async jobId => (await ensure()).transport.removeSystemJob(jobId),
		upsertJobScheduler: async input => (await ensure()).transport.upsertJobScheduler(input),
		removeJobScheduler: async id => (await ensure()).transport.removeJobScheduler(id),
		getJobScheduler: async id => (await ensure()).transport.getJobScheduler(id),
		listJobSchedulers: async () => (await ensure()).transport.listJobSchedulers(),
		startWorkers: async opts => (await ensure()).transport.startWorkers(opts),
		async closeWorkers(opts) {
			if (!current && loading) {
				// A load in progress (a trigger or start() racing stop()): close what it builds.
				await loading.catch(() => undefined);
			}
			if (!current) {
				sealed = true;
				// Same contract as the real transport: a failing callback is logged, closing continues.
				await opts.beforeClose?.([]).catch((error: unknown) => {
					ctx.logger.warn({ err: error }, "error before closing the workers");
				});
				return { stillActive: [] };
			}
			return current.transport.closeWorkers(opts);
		},
		checkRedis: async () => (await ensure()).transport.checkRedis(),
		pauseQueue: async queue => (await ensure()).transport.pauseQueue(queue),
		resumeQueue: async queue => (await ensure()).transport.resumeQueue(queue),
		isQueuePaused: async queue => (await ensure()).transport.isQueuePaused(queue),
		workerConcurrency: queue => current?.transport.workerConcurrency(queue),
		setWorkerConcurrency: (queue, concurrency) =>
			current?.transport.setWorkerConcurrency(queue, concurrency) ?? false,
	};

	async function* readStream(
		runId: string,
		name: string,
		signal: AbortSignal | undefined
	): AsyncGenerator<string | Json, void, undefined> {
		const runtime = await ensure();
		yield* runtime.backend.readStream(runId, name, signal);
	}

	const backend: EngineBackend = {
		store: ctx.store,
		transport,
		publisher: {
			publishStatus: async message => (await ensure()).backend.publisher.publishStatus(message),
			publishCancel: async runIds => (await ensure()).backend.publisher.publishCancel(runIds),
		},
		load: async () => {
			await ensure();
		},
		createPubSub(): PubSub {
			// Every caller awaits `load()` (runs.subscribe) or `beforeStart` (start()) first.
			if (!current) {
				throw new MinitriggerError("internal: pub/sub created before the backend was loaded");
			}
			return current.backend.createPubSub();
		},
		streams: {
			pipe: async (runId, name, source, options) =>
				(await ensure()).backend.streams.pipe(runId, name, source, options),
		},
		readStream,
		createRunInTransaction: async (tx, input) => {
			const create = (await ensure()).backend.createRunInTransaction;
			if (!create) throw new MinitriggerError("internal: backend without transactional trigger");
			return create(tx, input);
		},
		onScaled: () => current?.backend.onScaled?.(),
	};

	return {
		backend,
		transport,
		beforeStart: async step => {
			const runtime = await ensure();
			step.assertNotAbandoned();
			await runtime.beforeStart(step);
		},
		warnPoolSizing: queues => current?.warnPoolSizing(queues),
		awaitSubscriberReady: async ps => (await ensure()).awaitSubscriberReady(ps),
		dropSubscriber: () => current?.dropSubscriber(),
		onWorkersStartFailed: async () => current?.onWorkersStartFailed(),
		// Returns the backend's own result (possibly nothing, so no extra microtask separates the
		// workers from the leader loop); always loaded here (after `startWorkers`).
		afterWorkersStarted: () => current?.afterWorkersStarted(),
		beforeClose: async () => current?.beforeClose(),
		closeReaders: () => current?.closeReaders(),
		close: async () => current?.close(),
		isReady: () => current?.isReady() ?? false,
		// `false` on doubt (the safe default) -- never loads for this.
		isPrefixOwner: async () => (current ? current.isPrefixOwner() : false),
		dedicatedClientConfig: (): ClientConfig =>
			current ? current.dedicatedClientConfig() : dedicatedClientConfig(ctx.resolved, ctx.pool),
	};
}
