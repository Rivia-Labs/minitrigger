/**
 * Task 10c: `createEngine` loads `src/backends/bullmq.ts` with a dynamic `import()` (so neither the
 * main entry nor `/testing` loads `bullmq`/`ioredis`), and a missing optional peer is a
 * `ConfigError` with the install hint. The real proof without the packages installed is
 * `scripts/check-pack.mjs`.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BackendContext, BackendFactory, BackendRuntime } from "../../src/backends/types.js";
import type { EngineLogger } from "../../src/config.js";

const BULLMQ_BACKEND = "../../src/backends/bullmq.js";
const quiet: EngineLogger = { debug() {}, info() {}, warn() {}, error() {} };
/** Nothing listens there: no test below reaches a query. */
const POSTGRES = "postgres://127.0.0.1:1/none";

function missingPackage(name: string, code = "ERR_MODULE_NOT_FOUND"): Error {
	return Object.assign(
		new Error(`Cannot find package '${name}' imported from /app/node_modules/x/dist/chunk.js`),
		{ code }
	);
}

async function loadEngineModules() {
	const engine = await import("../../src/engine.js");
	const errors = await import("../../src/errors.js");
	return { ...engine, ...errors };
}

function bullmqConfig() {
	return {
		postgres: POSTGRES,
		redis: "redis://127.0.0.1:1",
		prefix: "mt_lazy",
		schema: "mt_lazy",
		tasks: [],
		logger: quiet,
	};
}

afterEach(() => {
	vi.doUnmock(BULLMQ_BACKEND);
	vi.resetModules();
});

describe("lazy BullMQ backend", () => {
	it.each([
		["ioredis", "ERR_MODULE_NOT_FOUND"],
		["bullmq", "ERR_MODULE_NOT_FOUND"],
		["ioredis", "MODULE_NOT_FOUND"],
	])("a missing %s (%s) is a ConfigError with the install hint", async (pkg, code) => {
		const failure = missingPackage(pkg, code);
		const factory = vi.fn(() => {
			throw failure;
		});
		vi.doMock(BULLMQ_BACKEND, factory);
		const { createEngine, ConfigError } = await loadEngineModules();

		const engine = createEngine(bullmqConfig());
		expect(factory).not.toHaveBeenCalled();

		const error = await engine.start().catch((e: unknown) => e);
		expect(error).toBeInstanceOf(ConfigError);
		expect((error as Error).message).toContain("pnpm add bullmq ioredis");
		expect((error as Error).message).toContain('transport: "postgres"');
		// The original load error is kept (possibly behind vitest's own wrapper).
		let cause: unknown = (error as Error).cause;
		while (cause instanceof Error && cause !== failure) cause = cause.cause;
		expect(cause).toBe(failure);

		// Engine APIs used without start() load the backend too, and fail the same way.
		const runId = "00000000-0000-4000-8000-000000000000";
		const next = (iterable: AsyncIterable<unknown>) => iterable[Symbol.asyncIterator]().next();
		await expect(next(engine.runs.subscribe(runId))).rejects.toBeInstanceOf(ConfigError);
		await expect(next(engine.runs.readStream(runId, "s"))).rejects.toThrow(
			/pnpm add bullmq ioredis/
		);
		await engine.stop();
	});

	it("rethrows any other load error as is", async () => {
		const failure = new TypeError("boom at module evaluation");
		vi.doMock(BULLMQ_BACKEND, () => {
			throw failure;
		});
		const { createEngine, ConfigError } = await loadEngineModules();
		const engine = createEngine(bullmqConfig());
		const error = await engine.start().catch((e: unknown) => e);
		expect(error).not.toBeInstanceOf(ConfigError);
		let found: unknown = error;
		while (found instanceof Error && found !== failure) found = found.cause;
		expect(found).toBe(failure);
		await engine.stop();
	});

	it("a module-not-found of another package is not reported as a missing peer", async () => {
		const failure = missingPackage("some-other-package");
		vi.doMock(BULLMQ_BACKEND, () => {
			throw failure;
		});
		const { createEngine, ConfigError } = await loadEngineModules();
		const engine = createEngine(bullmqConfig());
		const error = await engine.start().catch((e: unknown) => e);
		expect(error).not.toBeInstanceOf(ConfigError);
		await engine.stop();
	});

	it('transport: "postgres" never loads the BullMQ backend', async () => {
		const factory = vi.fn(() => {
			throw missingPackage("ioredis");
		});
		vi.doMock(BULLMQ_BACKEND, factory);
		const { createEngine } = await loadEngineModules();
		const engine = createEngine({
			transport: "postgres",
			postgres: POSTGRES,
			schema: "mt_lazy",
			tasks: [],
			logger: quiet,
		});
		await engine.stop();
		expect(factory).not.toHaveBeenCalled();
	});

	it("stop() of an engine that never used its backend does not load it", async () => {
		const factory = vi.fn(() => ({}));
		vi.doMock(BULLMQ_BACKEND, factory);
		const { createEngine } = await loadEngineModules();
		const engine = createEngine(bullmqConfig());
		// The synchronous transport getters answer "no worker here" without loading anything.
		await expect(engine.queues.scale("q", 2)).rejects.toThrow(/runs no worker for queue/);
		await engine.stop();
		expect(factory).not.toHaveBeenCalled();
	});

	it("loads the module and builds the backend once, and only when first needed", async () => {
		const beforeStart = vi.fn(async () => {
			throw new Error("stub beforeStart");
		});
		const createBullmqBackend = vi.fn(
			(): Partial<BackendRuntime> => ({
				beforeStart,
				backend: {} as BackendRuntime["backend"],
				transport: {
					closeWorkers: async ({ beforeClose }) => {
						await beforeClose?.([]);
						return { stillActive: [] };
					},
				} as Partial<BackendRuntime["transport"]> as BackendRuntime["transport"],
				beforeClose: async () => {},
				closeReaders: () => {},
				dropSubscriber: () => {},
				isReady: () => false,
				close: async () => {},
			})
		);
		const factory = vi.fn(() => ({ createBullmqBackend }));
		vi.doMock(BULLMQ_BACKEND, factory);
		const { createEngine } = await loadEngineModules();
		const engine = createEngine(bullmqConfig());
		expect(factory).not.toHaveBeenCalled();

		await expect(engine.start()).rejects.toThrow("stub beforeStart");
		await expect(engine.start()).rejects.toThrow("stub beforeStart");
		expect(factory).toHaveBeenCalledTimes(1);
		expect(createBullmqBackend).toHaveBeenCalledTimes(1);
		expect(beforeStart).toHaveBeenCalledTimes(2);
		await engine.stop();
	});
});

describe("createLazyBackend", () => {
	async function lazyWith(factory: () => Partial<BackendRuntime>) {
		const { createLazyBackend } = await import("../../src/backends/lazy.js");
		const loadFactory = vi.fn(async () => factory as unknown as BackendFactory);
		const ctx = { logger: quiet } as unknown as BackendContext;
		return { runtime: createLazyBackend(ctx, loadFactory), loadFactory };
	}

	it("closeWorkers with nothing loaded runs beforeClose and seals: no load after it", async () => {
		const { runtime, loadFactory } = await lazyWith(() => ({}));
		const beforeClose = vi.fn(async () => {});
		await expect(runtime.transport.closeWorkers({ timeoutMs: 10, beforeClose })).resolves.toEqual({
			stillActive: [],
		});
		expect(beforeClose).toHaveBeenCalledOnce();
		await expect(
			runtime.transport.enqueueRun({ queue: "q", runId: "r", seq: 1, delayMs: 0 })
		).rejects.toThrow(/stopped/);
		expect(loadFactory).not.toHaveBeenCalled();
		expect(runtime.isReady()).toBe(false);
		expect(runtime.transport.workerConcurrency("q")).toBeUndefined();
	});

	it("closeWorkers during a load awaits it and closes the loaded transport", async () => {
		const closeWorkers = vi.fn(async () => ({ stillActive: [] }));
		const enqueueRun = vi.fn(async () => {});
		const { runtime, loadFactory } = await lazyWith(() => ({
			transport: { closeWorkers, enqueueRun } as unknown as BackendRuntime["transport"],
		}));
		const enqueued = runtime.transport.enqueueRun({ queue: "q", runId: "r", seq: 1, delayMs: 0 });
		await runtime.transport.closeWorkers({ timeoutMs: 10 });
		await enqueued;
		expect(loadFactory).toHaveBeenCalledOnce();
		expect(enqueueRun).toHaveBeenCalledOnce();
		expect(closeWorkers).toHaveBeenCalledOnce();
	});
});
