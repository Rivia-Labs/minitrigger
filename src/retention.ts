/**
 * Retention (Phase 6; `docs/SPEC.md` Phase 6, decision 10 for the parent rule). Deletes finished
 * runs older than `days`, in batches, via {@link Store.runs.deleteFinishedBatch}, which already
 * enforces "only a run with no parent or a finished parent" and does all its own locking
 * (`FOR UPDATE SKIP LOCKED`) and cascading deletes (steps/waitpoints/logs/keys; children's
 * `parent_*` set NULL). This module is just the batch loop: run on the leader (`src/leader.ts`),
 * as a `-system` job, per decision 8.
 */

import type { EngineLogger } from "./config.js";
import type { Store } from "./store/types.js";

const DAY_MS = 86_400_000;

/**
 * Hard cap on batches per {@link Retention.runOnce} call, so a huge backlog (e.g. after changing
 * `retention.days` down, or a long outage) can't loop inside a single tick forever. Hitting it
 * just means the remaining rows are picked up by the next tick.
 */
const DEFAULT_MAX_ITERATIONS = 10_000;

export interface RetentionOptions {
	store: Store;
	logger: EngineLogger;
	/** Runs finished more than this many days ago (and whose parent, if any, is finished) are deleted. */
	days: number;
	/** Rows deleted per `deleteFinishedBatch` call; a batch shorter than this ends the loop. */
	batchSize: number;
	/** Overrides {@link DEFAULT_MAX_ITERATIONS} (test-only knob). */
	maxIterations?: number;
}

export interface Retention {
	/** Loops `deleteFinishedBatch` until a batch is short (or the iteration cap is hit); returns the total deleted. */
	runOnce(): Promise<number>;
}

export function createRetention(options: RetentionOptions): Retention {
	const { store, logger, days, batchSize } = options;
	const maxIterations = options.maxIterations ?? DEFAULT_MAX_ITERATIONS;
	const olderThanMs = days * DAY_MS;

	async function runOnce(): Promise<number> {
		let total = 0;
		let iterations = 0;
		for (; iterations < maxIterations; iterations++) {
			const deleted = await store.runs.deleteFinishedBatch(olderThanMs, batchSize);
			total += deleted;
			if (deleted < batchSize) {
				return total;
			}
			// Yield between batches so a large backlog doesn't monopolize the event loop or hold the
			// pool's connections back-to-back without giving other work a turn.
			await yieldToEventLoop();
		}
		logger.warn(
			{ deleted: total, maxIterations },
			"retention: hit the per-run iteration cap with a full last batch; more rows likely remain " +
				"for the next run"
		);
		return total;
	}

	return { runOnce };
}

function yieldToEventLoop(): Promise<void> {
	return new Promise(resolve => setImmediate(resolve));
}
