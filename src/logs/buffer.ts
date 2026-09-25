/**
 * Batched, buffered run-log writer (Phase 7, `docs/SPEC.md` Phase 7 first bullet: "`ctx.logger`
 * com buffer e flush em lote via `unnest` (500 ms ou 50 entradas, e ao final)"). Standalone
 * building block: it only knows a `runId` and an `insertBatch` callback (typically
 * `store.logs.insertBatch`, itself a single `unnest`-based statement -- see
 * `src/store/sql/logs.ts`); `src/executor/logger.ts` builds one per execution as `ctx.logger`.
 *
 * Produces a `TaskLogger`-compatible object (`debug`/`info`/`warn`/`error(message, data?)`) that:
 *
 * - filters by `minLevel` before ever buffering or mirroring an entry (a filtered-out call costs
 *   nothing beyond the level comparison);
 * - truncates `data` with {@link truncateLogData} and caps `message` to `maxLogEntryBytes` (UTF-8
 *   bytes, via {@link truncateUtf8}) *before buffering*, so every row handed to `insertBatch` is
 *   bounded regardless of what the caller logged. The two caps are independent (`message` and
 *   `data` each get their own `maxLogEntryBytes` budget, not a shared one) -- simpler, and a
 *   pathological one doesn't starve the other's budget;
 * - batches: flushes every `flushIntervalMs` (default 500) OR as soon as `maxBuffered` (default
 *   50) entries have accumulated, one `insertBatch` call per `maxBatchSize` (default 500) entries -- never one
 *   query per log line (the Phase 7 acceptance criterion "10k logs sem uma query por linha");
 * - serializes flushes on a promise chain: a flush already running (or already queued) is always
 *   finished before the next one starts, so `insertBatch` never sees two concurrent calls for the
 *   same buffer and entries are always handed to it in the order they were logged. A flush that
 *   finds an empty buffer (e.g. a size-triggered flush that lost the race to an earlier one that
 *   already drained everything) is a no-op: it does not call `insertBatch` at all;
 * - on `insertBatch` failure, retries the SAME batch immediately, exactly once. If the retry also
 *   fails, the batch is dropped -- never re-queued, so a sustained outage can't grow the buffer
 *   without bound -- and `onFlushError` is invoked once with the final error and the dropped
 *   entries. `onFlushError` itself is never allowed to throw out of a flush;
 * - optionally mirrors every entry that passes the level filter to `mirror` (an `EngineLogger`-
 *   shaped sink: `(fields, message) => void`, called as `mirror[level](fields, message)`) with
 *   the *original, untruncated* `message`/`data` -- truncation is a storage concern, not a
 *   display one. `fields` is `{ runId, ...data }` when `data` is a plain object (the common
 *   case), or `{ runId, data }` when it is a primitive/array (spreading those wouldn't merge
 *   usefully). A throwing `mirror` never breaks logging or buffering;
 * - the flush timer is `unref()`d, so an idle buffer never keeps the process alive;
 * - `close()` flushes whatever remains, stops the timer, and makes every subsequent logger call
 *   (and a subsequent `flush()`) a no-op. `close()` itself is idempotent.
 */

import type { LogLevel, NewLogEntry } from "../store/types.js";
import type { TaskLogger } from "../task.js";
import { truncateUtf8 } from "../utils/error-format.js";
import { type Json, truncateLogData } from "../utils/serialization.js";

/** `EngineLogger`-shaped sink (see `src/config.ts`), duplicated here so this module stays standalone. */
export type LogBufferMirror = Record<
	LogLevel,
	(fields: Record<string, unknown>, message: string) => void
>;

export interface LogBufferOptions {
	runId: string;
	/** Typically `store.logs.insertBatch`. Called with a non-empty array; never called with `[]`. */
	insertBatch: (entries: NewLogEntry[]) => Promise<void>;
	/** Entries below this level are dropped before buffering or mirroring. Default `"debug"` (all). */
	minLevel?: LogLevel;
	maxLogEntryBytes: number;
	mirror?: LogBufferMirror;
	/** Default 500. */
	flushIntervalMs?: number;
	/** Default 50. */
	maxBuffered?: number;
	/**
	 * Most entries handed to one `insertBatch` call (default 500). A synchronous burst logged
	 * before any flush runs is split into several statements of at most this size, so a single
	 * `INSERT ... unnest` never carries an unbounded payload.
	 */
	maxBatchSize?: number;
	/** Called at most once per dropped batch, after one retry already failed. Never let it throw. */
	onFlushError?: (error: unknown, entries: readonly NewLogEntry[]) => void;
}

/** `ctx.logger` implementation: `TaskLogger` plus explicit `flush`/`close`. */
export interface LogBuffer extends TaskLogger {
	/** Flushes the current buffer (one `insertBatch` per `maxBatchSize` entries, none if empty). Never throws. */
	flush(): Promise<void>;
	/** Flushes remaining entries, stops the timer, and disables further logging. Idempotent. */
	close(): Promise<void>;
}

const LEVEL_RANK: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

export function createLogBuffer(options: LogBufferOptions): LogBuffer {
	const {
		runId,
		insertBatch,
		minLevel = "debug",
		maxLogEntryBytes,
		mirror,
		flushIntervalMs = 500,
		maxBuffered = 50,
		maxBatchSize = 500,
		onFlushError,
	} = options;

	const minRank = LEVEL_RANK[minLevel];

	const buffer: NewLogEntry[] = [];
	// Every scheduled flush chains onto this promise, so at most one `doFlush` body runs at a
	// time and they run in the order they were scheduled. `doFlush` never rejects (it catches its
	// own errors), so this chain is never "poisoned" by a failed flush.
	let flushChain: Promise<void> = Promise.resolve();
	let closed = false;

	const timer = setInterval(() => {
		scheduleFlush();
	}, flushIntervalMs);
	timer.unref();

	function scheduleFlush(): Promise<void> {
		flushChain = flushChain.then(doFlush);
		return flushChain;
	}

	async function doFlush(): Promise<void> {
		while (buffer.length > 0) {
			await flushBatch(buffer.splice(0, maxBatchSize));
		}
	}

	async function flushBatch(batch: NewLogEntry[]): Promise<void> {
		try {
			await insertBatch(batch);
			return;
		} catch {
			// First attempt failed; fall through to the single retry below.
		}
		try {
			await insertBatch(batch);
		} catch (error) {
			safeOnFlushError(error, batch);
		}
	}

	function safeOnFlushError(error: unknown, entries: readonly NewLogEntry[]): void {
		if (!onFlushError) return;
		try {
			onFlushError(error, entries);
		} catch {
			// An onFlushError callback must never itself break the buffer.
		}
	}

	function mirrorFields(data: Json | undefined): Record<string, unknown> {
		if (data === undefined) return { runId };
		if (isPlainObject(data)) return { runId, ...data };
		return { runId, data };
	}

	function emit(level: LogLevel, message: string, data?: Json): void {
		if (closed) return;
		if (LEVEL_RANK[level] < minRank) return;

		if (mirror) {
			try {
				mirror[level](mirrorFields(data), message);
			} catch {
				// A broken mirror logger must never break buffering.
			}
		}

		const entry: NewLogEntry = {
			runId,
			level,
			message: truncateUtf8(message, maxLogEntryBytes),
			data: normalizeData(truncateLogData(data, maxLogEntryBytes)),
		};
		buffer.push(entry);
		if (buffer.length >= maxBuffered) {
			scheduleFlush();
		}
	}

	return {
		debug: (message, data) => emit("debug", message, data),
		info: (message, data) => emit("info", message, data),
		warn: (message, data) => emit("warn", message, data),
		error: (message, data) => emit("error", message, data),
		flush: () => scheduleFlush(),
		close: async () => {
			if (closed) return;
			closed = true;
			clearInterval(timer);
			await scheduleFlush();
		},
	};
}

function isPlainObject(value: Json): value is Record<string, Json | undefined> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** `NewLogEntry.data` is `Json | null` (no `undefined`); `truncateLogData` may return `undefined`. */
function normalizeData(value: unknown): NewLogEntry["data"] {
	return value === undefined ? null : (value as NewLogEntry["data"]);
}
