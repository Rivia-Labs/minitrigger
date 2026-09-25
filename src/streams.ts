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
 * - the end marker (written once, by {@link StreamWriter.end}): field `end` (value `"1"`), no
 *   `data` field. `end()` also sets the key's `EXPIRE` to `ttlSeconds` so a finished stream's
 *   entries don't outlive `retention.days` worth of Postgres state for no reason.
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

import type { Redis } from "ioredis";
import { validateSqlIdentifier } from "./utils/identifiers.js";
import type { Json } from "./utils/serialization.js";

export interface StreamsOptions {
	/** Non-blocking connection used for `XADD`/`EXPIRE` (`writer`/`pipe`). Safe to share. */
	redis: Redis;
	prefix: string;
	/** `XADD ... MAXLEN ~` cap. Default 10 000. */
	maxLen?: number;
	/** `EXPIRE` set on the stream key once {@link StreamWriter.end} runs. Default 86 400 (1 day). */
	ttlSeconds?: number;
}

export interface StreamWriter {
	/** One `XADD` per call. */
	append(chunk: string | Json): Promise<void>;
	/** Writes the end marker, then sets the key's `EXPIRE`. Idempotent to call more than once. */
	end(): Promise<void>;
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
	/** Appends every item of `source` in order, then calls `end()`. */
	pipe(runId: string, name: string, source: AsyncIterable<string | Json>): Promise<void>;
	/**
	 * Reads from the start (`id 0`) until the end marker, an abort, or the caller stops iterating.
	 * Already-written chunks are yielded immediately; if the writer hasn't called `end()` yet, the
	 * generator keeps blocking/polling for new chunks ("concurrent reader receives live chunks
	 * then ends").
	 */
	read(runId: string, name: string, options: ReadStreamOptions): AsyncIterable<string | Json>;
}

const DEFAULT_MAX_LEN = 10_000;
const DEFAULT_TTL_SECONDS = 86_400;
const DEFAULT_BLOCK_MS = 5_000;

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
				await redis.xadd(streamKey, "MAXLEN", "~", maxLen, "*", "data", JSON.stringify(chunk));
			},
			async end() {
				await redis.xadd(streamKey, "MAXLEN", "~", maxLen, "*", "end", "1");
				await redis.expire(streamKey, ttlSeconds);
			},
		};
	}

	async function pipe(
		runId: string,
		name: string,
		source: AsyncIterable<string | Json>
	): Promise<void> {
		const w = writer(runId, name);
		for await (const chunk of source) {
			await w.append(chunk);
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
				if (parsed.kind === "end") return;
				if (parsed.kind === "data") yield parsed.value;
				// "skip": an entry that's neither shape this module writes; ignore defensively.
			}
		}
	}

	return { writer, pipe, read };
}

type ParsedEntry = { kind: "end" } | { kind: "data"; value: string | Json } | { kind: "skip" };

function parseEntry(fields: string[]): ParsedEntry {
	const map = new Map<string, string>();
	for (let i = 0; i + 1 < fields.length; i += 2) {
		const field = fields[i];
		const value = fields[i + 1];
		if (field !== undefined && value !== undefined) map.set(field, value);
	}
	if (map.has("end")) return { kind: "end" };
	const data = map.get("data");
	if (data === undefined) return { kind: "skip" };
	try {
		return { kind: "data", value: JSON.parse(data) as string | Json };
	} catch {
		return { kind: "skip" };
	}
}

type AbortRace<T> = { aborted: true } | { aborted: false; value: T };

/**
 * Races `promise` against `signal` firing. On abort, resolves immediately with `{ aborted: true }`
 * without waiting for `promise` to settle -- `promise` (the in-flight `XREAD BLOCK`) is left to
 * resolve on its own later, on a connection the caller owns and is expected to discard/close
 * after an abort, so nothing awaits or depends on its eventual result.
 */
function raceAbort<T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<AbortRace<T>> {
	if (!signal) return promise.then(value => ({ aborted: false, value }));
	if (signal.aborted) return Promise.resolve({ aborted: true });

	return new Promise(resolve => {
		const onAbort = (): void => resolve({ aborted: true });
		signal.addEventListener("abort", onAbort, { once: true });
		promise.then(
			value => {
				signal.removeEventListener("abort", onAbort);
				resolve({ aborted: false, value });
			},
			() => {
				// A rejected XREAD (e.g. the caller's dedicated connection was closed to force an
				// abort) is treated the same as an abort: stop reading rather than throwing out of
				// the generator.
				signal.removeEventListener("abort", onAbort);
				resolve({ aborted: true });
			}
		);
	});
}
