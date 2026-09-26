/**
 * `ctx.stream(name, source)` / `engine.runs.readStream(runId, name)` transport (Phase 7 last
 * bullet). Standalone building block: it only knows an `ioredis` connection and a `prefix`, and
 * does not know about the executor/engine, Postgres, or task context -- wiring it in is another
 * agent's job.
 *
 * Backed by Redis Streams (`docs/research/api-notes.md` §2.3), one stream per `(runId, name)` at
 * key `${prefix}:stream:${runId}:${name}`. Every entry has exactly one of two shapes:
 *
 * - a chunk: field `data` holding `JSON.stringify(chunk)` (`chunk` is `string | Json`, so this
 *   round-trips both a bare string and a structured value uniformly);
 * - the end marker (written by {@link StreamWriter.end}): field `end` (value `"1"`), no `data`
 *   field. When the stream ended because its source threw or the run was aborted, the marker
 *   also carries an `error` field (the error's `name: message`, truncated); readers that predate
 *   it simply see an end, newer ones ({@link Streams.read}) throw.
 *
 * Every append and the end marker also (re)set the key's `EXPIRE` to `ttlSeconds` (one pipelined
 * round trip): Redis runs with `noeviction`, so a stream whose writer died (process crash) must
 * still go away on its own instead of staying forever.
 *
 * `XADD ... MAXLEN ~ maxLen` (approximate trimming, decision-consistent with the reconciler/queue
 * modules preferring `~` over exact `MAXLEN` for O(1) amortized trimming) bounds how much of a
 * stream is kept; a reader that starts from `0` after trimming has already happened simply never
 * sees the trimmed-away entries (documented, same spirit as decision 21's "schedules don't
 * silently make something up" -- a truncated stream is a capacity trade-off, not a bug).
 *
 * `writer`/`pipe` use `redis` (a normal, shared, non-blocking connection: `XADD` is not a
 * blocking command). `read` uses `XREAD BLOCK`, which IS a blocking command, so per decision 14 /
 * `docs/research/api-notes.md` §2.2 it is never issued on a shared connection -- `read` requires
 * the caller to pass a connection dedicated to this one read (e.g.
 * `RedisConnections.dedicated("stream")`); this module never `duplicate()`s one itself and never
 * closes what it's given.
 */

import type { ChainableCommander, Redis } from "ioredis";
import { MinitriggerError } from "./errors.js";
import { validateSqlIdentifier } from "./utils/identifiers.js";
import type { Json } from "./utils/serialization.js";

export interface StreamsOptions {
	/** Non-blocking connection used for `XADD`/`EXPIRE` (`writer`/`pipe`). Safe to share. */
	redis: Redis;
	prefix: string;
	/** `XADD ... MAXLEN ~` cap. Default 10 000. */
	maxLen?: number;
	/**
	 * `EXPIRE` (re)set on the stream key by every append and by {@link StreamWriter.end}.
	 * Default 86 400 (1 day).
	 */
	ttlSeconds?: number;
}

export interface StreamWriter {
	/** One `XADD` (+ `EXPIRE`, pipelined) per call. */
	append(chunk: string | Json): Promise<void>;
	/**
	 * Writes the end marker (with an `error` field when `error` is given), then sets the key's
	 * `EXPIRE`. Readers stop at the first end marker.
	 */
	end(error?: unknown): Promise<void>;
}

export interface PipeOptions {
	/**
	 * Stops pulling from `source` as soon as it fires (the run was aborted: canceled, timed out,
	 * lost, finished...): the end marker is written with an error and `pipe` rejects with the
	 * signal's reason. `source.return()` is called without waiting for it.
	 */
	signal?: AbortSignal;
}

export interface ReadStreamOptions {
	/**
	 * `XREAD BLOCK` window per poll, in ms. The read loop re-polls after each timeout (subject to
	 * `signal`), so this only bounds how promptly an abort without a firing event is noticed and
	 * how often an idle read re-issues `XREAD`; it does not bound total read time. Default 5000.
	 */
	blockMs?: number;
	/** Stops reading as soon as it fires, without waiting for the current `XREAD BLOCK` to time out. */
	signal?: AbortSignal;
	/** A connection DEDICATED to this read (e.g. `RedisConnections.dedicated("stream")`). */
	redis: Redis;
}

export interface Streams {
	writer(runId: string, name: string): StreamWriter;
	/**
	 * Appends every item of `source` in order, then writes the end marker. The end marker and the
	 * key's `EXPIRE` are ALWAYS written, also when `source` throws or `options.signal` fires (then
	 * with an `error` field, and `pipe` rejects with that error), so readers never block forever.
	 */
	pipe(
		runId: string,
		name: string,
		source: AsyncIterable<string | Json>,
		options?: PipeOptions
	): Promise<void>;
	/**
	 * Reads from the start (`id 0`) until the end marker, an abort, or the caller stops iterating.
	 * Already-written chunks are yielded immediately; if the writer hasn't called `end()` yet, the
	 * generator keeps blocking/polling for new chunks ("concurrent reader receives live chunks
	 * then ends"). An end marker with an `error` field (the writer's source threw or its run was
	 * aborted) makes the generator throw a {@link MinitriggerError} after the chunks before it.
	 */
	read(runId: string, name: string, options: ReadStreamOptions): AsyncIterable<string | Json>;
}

const DEFAULT_MAX_LEN = 10_000;
const DEFAULT_TTL_SECONDS = 86_400;
const DEFAULT_BLOCK_MS = 5_000;
/** Max characters of the error text stored in an end marker. */
const MAX_END_ERROR_LENGTH = 1_000;

export function createStreams(options: StreamsOptions): Streams {
	const { redis } = options;
	const prefix = validateSqlIdentifier("prefix", options.prefix);
	const maxLen = options.maxLen ?? DEFAULT_MAX_LEN;
	const ttlSeconds = options.ttlSeconds ?? DEFAULT_TTL_SECONDS;

	function key(runId: string, name: string): string {
		return `${prefix}:stream:${runId}:${name}`;
	}

	function writer(runId: string, name: string): StreamWriter {
		const streamKey = key(runId, name);
		return {
			async append(chunk) {
				await exec(
					redis
						.pipeline()
						.xadd(streamKey, "MAXLEN", "~", maxLen, "*", "data", JSON.stringify(chunk))
						.expire(streamKey, ttlSeconds)
				);
			},
			async end(error?: unknown) {
				const fields = error === undefined ? ["end", "1"] : ["end", "1", "error", errorText(error)];
				await exec(
					redis
						.pipeline()
						.xadd(streamKey, "MAXLEN", "~", maxLen, "*", ...fields)
						.expire(streamKey, ttlSeconds)
				);
			},
		};
	}

	async function pipe(
		runId: string,
		name: string,
		source: AsyncIterable<string | Json>,
		pipeOptions?: PipeOptions
	): Promise<void> {
		const signal = pipeOptions?.signal;
		const w = writer(runId, name);
		const iterator = source[Symbol.asyncIterator]();
		try {
			while (true) {
				signal?.throwIfAborted();
				const next = await nextOrAbort(iterator, signal);
				if (next.done === true) break;
				await w.append(next.value);
			}
		} catch (error) {
			// Stop the source (it may ignore this or hang: never awaited), end the stream with the
			// error so readers stop, and still report the original error to the caller.
			if (typeof iterator.return === "function") {
				Promise.resolve()
					.then(() => iterator.return?.())
					.catch(() => {});
			}
			await w.end(error).catch(() => {});
			throw error;
		}
		await w.end();
	}

	async function* read(
		runId: string,
		name: string,
		readOptions: ReadStreamOptions
	): AsyncGenerator<string | Json, void, void> {
		const { redis: reader, signal } = readOptions;
		const blockMs = readOptions.blockMs ?? DEFAULT_BLOCK_MS;
		const streamKey = key(runId, name);
		let lastId = "0";

		while (!(signal?.aborted ?? false)) {
			const outcome = await raceAbort(
				reader.xread("BLOCK", blockMs, "STREAMS", streamKey, lastId),
				signal
			);
			if (outcome.aborted) return;
			const result = outcome.value;
			if (result === null) continue; // block window elapsed with no new entries; poll again

			const streamEntries = result[0];
			if (streamEntries === undefined) continue;
			const [, entries] = streamEntries;

			for (const [id, fields] of entries) {
				lastId = id;
				const parsed = parseEntry(fields);
				if (parsed.kind === "end") {
					if (parsed.error === undefined) return;
					throw new MinitriggerError(
						`stream "${name}" of run ${runId} ended with an error: ${parsed.error}`
					);
				}
				if (parsed.kind === "data") yield parsed.value;
				// "skip": an entry that's neither shape this module writes; ignore defensively.
			}
		}
	}

	return { writer, pipe, read };
}

type ParsedEntry =
	| { kind: "end"; error: string | undefined }
	| { kind: "data"; value: string | Json }
	| { kind: "skip" };

function parseEntry(fields: string[]): ParsedEntry {
	const map = new Map<string, string>();
	for (let i = 0; i + 1 < fields.length; i += 2) {
		const field = fields[i];
		const value = fields[i + 1];
		if (field !== undefined && value !== undefined) map.set(field, value);
	}
	if (map.has("end")) return { kind: "end", error: map.get("error") };
	const data = map.get("data");
	if (data === undefined) return { kind: "skip" };
	try {
		return { kind: "data", value: JSON.parse(data) as string | Json };
	} catch {
		return { kind: "skip" };
	}
}

/** Runs a pipeline and rethrows the first command error (a pipeline never rejects on its own). */
async function exec(pipeline: ChainableCommander): Promise<void> {
	const results = (await pipeline.exec()) ?? [];
	for (const [error] of results) {
		if (error) throw error;
	}
}

/** `name: message` of what ended a stream, bounded. */
function errorText(error: unknown): string {
	const text =
		error instanceof Error || error instanceof DOMException
			? `${error.name}: ${error.message}`
			: String(error);
	return text.length > MAX_END_ERROR_LENGTH ? `${text.slice(0, MAX_END_ERROR_LENGTH)}…` : text;
}

/**
 * `iterator.next()` raced against `signal`: an abort rejects right away with the signal's reason,
 * without waiting for a source that ignores it.
 */
function nextOrAbort<T>(
	iterator: AsyncIterator<T>,
	signal: AbortSignal | undefined
): Promise<IteratorResult<T>> {
	const next = iterator.next();
	if (!signal) return next;
	return new Promise((resolve, reject) => {
		const onAbort = (): void => reject(signal.reason);
		signal.addEventListener("abort", onAbort, { once: true });
		next.then(
			result => {
				signal.removeEventListener("abort", onAbort);
				resolve(result);
			},
			(error: unknown) => {
				signal.removeEventListener("abort", onAbort);
				reject(error);
			}
		);
	});
}

type AbortRace<T> = { aborted: true } | { aborted: false; value: T };

/**
 * Races `promise` against `signal` firing. On abort, resolves immediately with `{ aborted: true }`
 * without waiting for `promise` to settle -- `promise` (the in-flight `XREAD BLOCK`) is left to
 * resolve on its own later, on a connection the caller owns and is expected to discard/close
 * after an abort, so nothing awaits or depends on its eventual result. A rejection is propagated
 * unless the signal had already fired by then.
 */
function raceAbort<T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<AbortRace<T>> {
	if (!signal) return promise.then(value => ({ aborted: false, value }));
	if (signal.aborted) return Promise.resolve({ aborted: true });

	return new Promise((resolve, reject) => {
		const onAbort = (): void => resolve({ aborted: true });
		signal.addEventListener("abort", onAbort, { once: true });
		promise.then(
			value => {
				signal.removeEventListener("abort", onAbort);
				resolve({ aborted: false, value });
			},
			(error: unknown) => {
				signal.removeEventListener("abort", onAbort);
				// A rejection AFTER the abort (the caller closed its dedicated connection to stop
				// the read) is part of that abort: end quietly. Any other rejection (LOADING after
				// a Redis restart, NOAUTH, ...) is a real error the reader must see; ending the
				// stream normally would silently drop the chunks it never read.
				if (signal.aborted) resolve({ aborted: true });
				else reject(error);
			}
		);
	});
}
