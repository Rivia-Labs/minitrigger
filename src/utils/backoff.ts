import { ConfigError } from "../errors.js";

export type BackoffOptions = {
	type: "exponential" | "fixed";
	baseMs: number;
	maxMs?: number;
	jitter?: boolean;
};

export type RetryOptions = {
	maxAttempts: number;
	backoff?: BackoffOptions;
};

export const DEFAULT_RETRY: RetryOptions = {
	maxAttempts: 3,
	backoff: { type: "exponential", baseMs: 1_000, maxMs: 60_000 },
};

/**
 * Cap applied when `opts.maxMs` isn't specified. Without *some* cap, exponential backoff with a
 * large `attempt` overflows to `Infinity` (`baseMs * 2^(attempt - 1)`), and `baseMs: 0` combined
 * with a large `attempt` produces `NaN` (`0 * Infinity`). Both are now caught by the finiteness
 * guard below and fall back to this cap. One hour is a generous default: callers that want a
 * tighter ceiling should pass `maxMs` explicitly (as `DEFAULT_RETRY` does, at 60s).
 */
export const DEFAULT_MAX_BACKOFF_MS = 3_600_000;

/**
 * Computes the delay (in ms) before the next attempt, given the attempt that
 * just failed (1-based).
 *
 * - `exponential`: `baseMs * 2^(attempt - 1)`, capped at `maxMs` (default {@link DEFAULT_MAX_BACKOFF_MS}).
 * - `fixed`: always `baseMs`, capped at `maxMs` (default {@link DEFAULT_MAX_BACKOFF_MS}).
 *
 * `jitter` defaults to `true` and applies "equal jitter" (half fixed + half
 * random), so the delay never collapses to 0 the way full jitter can.
 *
 * The result is always a finite, non-negative integer: a non-finite raw/capped delay (from
 * `baseMs * 2^(attempt - 1)` overflowing, or `0 * Infinity` producing `NaN`) falls back to the
 * cap instead of propagating `NaN`/`Infinity`.
 */
export function computeBackoffMs(
	opts: BackoffOptions,
	attempt: number,
	random: () => number = Math.random
): number {
	if (!Number.isInteger(attempt) || attempt < 1) {
		throw new ConfigError(`invalid backoff attempt: ${attempt} (must be a positive integer)`);
	}
	if (!Number.isFinite(opts.baseMs) || opts.baseMs < 0) {
		throw new ConfigError(`invalid backoff baseMs: ${opts.baseMs} (must be finite and >= 0)`);
	}
	if (opts.maxMs !== undefined && (!Number.isFinite(opts.maxMs) || opts.maxMs < 0)) {
		throw new ConfigError(`invalid backoff maxMs: ${opts.maxMs} (must be finite and >= 0)`);
	}

	const maxMs = opts.maxMs ?? DEFAULT_MAX_BACKOFF_MS;
	const rawMs = opts.type === "exponential" ? opts.baseMs * 2 ** (attempt - 1) : opts.baseMs;
	const cappedMs = Number.isFinite(rawMs) ? Math.min(rawMs, maxMs) : maxMs;

	const jitter = opts.jitter ?? true;
	const delayMs = jitter ? cappedMs / 2 + random() * (cappedMs / 2) : cappedMs;

	return Math.max(0, Math.round(delayMs));
}
