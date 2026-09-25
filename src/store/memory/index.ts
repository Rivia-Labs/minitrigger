/**
 * In-memory {@link Store} for the test mode (Phase 9.1): same observable semantics as the SQL
 * store (docs/store-contract.md), no Postgres.
 *
 * - Every operation runs synchronously on the tables once it holds the store's async mutex, so it
 *   is atomic and all operations/transactions are serialized (a serializable database).
 * - Writes go through an undo journal: an operation that throws, or a `withTransaction` whose
 *   callback rejects, rolls back every write it made (ROLLBACK); `runs.create` inside a
 *   transaction rolls back only to its savepoint.
 * - `now()` is the injected clock, fixed for the duration of one operation or one
 *   `withTransaction` (Postgres `now()` is the transaction start time).
 * - Values are stored as `jsonb` would (JSON round trip) and deep-copied on the way out.
 */
import { AsyncLocalStorage } from "node:async_hooks";
import type { RunStatus, Store, StoreTx } from "../types.js";
import { createIdempotencyStore } from "./idempotency.js";
import { createLogStore } from "./logs.js";
import type { Exec } from "./resolve.js";
import { createRunStore } from "./runs.js";
import { createScheduleStore } from "./schedules.js";
import { MemDb, MemTx, Mutex } from "./state.js";
import { createStepStore } from "./steps.js";
import { createWaitpointStore } from "./waitpoints.js";

export interface MemoryStoreOptions {
	/** Clock in epoch milliseconds: the "database clock" of test mode. Default `Date.now`. */
	now?: () => number;
}

interface OpenTx {
	readonly store: object;
	open: boolean;
	aborted: boolean;
}

/** Tracks the transaction whose callback is running, to reject pool-bound use inside it. */
const currentTx = new AsyncLocalStorage<OpenTx>();

/** Test-only access to a memory store's internals (see {@link memoryStoreTestHooks}). */
const hooks = new WeakMap<Store, MemoryStoreTestHooks>();

export interface MemoryStoreTestHooks {
	/**
	 * Overwrites columns of a run row directly (like a raw SQL `UPDATE runs` in the SQL tests),
	 * serialized with the other operations. Times are epoch ms.
	 */
	patchRun(
		runId: string,
		patch: Partial<{
			status: RunStatus;
			executionId: string | null;
			runAfter: number | null;
			heartbeatAt: number | null;
			enqueueSeq: number;
		}>
	): Promise<void>;
}

/** Internals of a store built by {@link createMemoryStore}, for tests; null for other stores. */
export function memoryStoreTestHooks(store: Store): MemoryStoreTestHooks | null {
	return hooks.get(store) ?? null;
}

function buildRepos(exec: Exec) {
	return {
		runs: createRunStore(exec),
		steps: createStepStore(exec),
		waitpoints: createWaitpointStore(exec),
		idempotency: createIdempotencyStore(exec),
		logs: createLogStore(exec),
		schedules: createScheduleStore(exec),
	};
}

export function createMemoryStore(opts: MemoryStoreOptions = {}): Store {
	const clock = opts.now ?? Date.now;
	const db = new MemDb();
	const mutex = new Mutex();
	const identity = {};

	function assertNotInsideTx(): void {
		const tx = currentTx.getStore();
		if (tx?.open && tx.store === identity) {
			throw new Error(
				"memory store: the pool-bound store was used inside withTransaction (it would " +
					"deadlock waiting for the transaction); use the `tx` repositories"
			);
		}
	}

	const poolExec: Exec = {
		kind: "pool",
		async run(fn) {
			assertNotInsideTx();
			const release = await mutex.lock();
			const tx = new MemTx(db, clock());
			try {
				const result = fn(tx);
				tx.commit();
				return result;
			} catch (err) {
				tx.rollbackTo(0);
				throw err;
			} finally {
				release();
			}
		},
	};

	const store: Store = {
		...buildRepos(poolExec),

		async withTransaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T> {
			assertNotInsideTx();
			const release = await mutex.lock();
			const memTx = new MemTx(db, clock());
			const state: OpenTx = { store: identity, open: true, aborted: false };
			const txExec: Exec = {
				kind: "tx",
				async run(op) {
					if (!state.open) throw new Error("memory store: transaction already closed");
					if (state.aborted) {
						throw new Error(
							"current transaction is aborted, commands ignored until end of transaction block"
						);
					}
					try {
						return op(memTx);
					} catch (err) {
						// Like Postgres: a failed statement aborts the whole transaction.
						state.aborted = true;
						throw err;
					}
				},
			};
			try {
				const result = await currentTx.run(state, () => fn(buildRepos(txExec)));
				// COMMIT of an aborted transaction is a ROLLBACK in Postgres (no error raised).
				if (state.aborted) memTx.rollbackTo(0);
				else memTx.commit();
				return result;
			} catch (err) {
				memTx.rollbackTo(0);
				throw err;
			} finally {
				state.open = false;
				release();
			}
		},
	};

	hooks.set(store, {
		async patchRun(runId, patch) {
			await poolExec.run(tx => {
				const row = tx.db.runs.get(runId);
				if (!row) throw new Error(`patchRun: run ${runId} not found`);
				tx.set(tx.db.runs, runId, { ...row, ...patch });
			});
		},
	});

	return store;
}
