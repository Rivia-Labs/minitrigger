/**
 * `$n` placeholder handling for ORM adapters whose raw-query API does not take Postgres-style
 * placeholders (knex: `?`; drizzle: a SQL object with interleaved params). Values are never
 * interpolated into the SQL text: they only move from `values[n - 1]` to the ORM's own
 * parameter slot, so the ORM (and node-postgres under it) still sends them as bind parameters.
 */

/** SQL text split around its `$n` placeholders: `segments.length === indexes.length + 1`. */
export interface SplitSql {
	readonly segments: string[];
	/** 1-based parameter number of each placeholder, in text order (repeats kept). */
	readonly indexes: number[];
}

const isDigit = (c: string | undefined) => c !== undefined && c >= "0" && c <= "9";
const isIdentChar = (c: string | undefined) => c !== undefined && /[A-Za-z0-9_$\u0080-￿]/.test(c);
const DOLLAR_TAG = /^\$([A-Za-z_\u0080-￿][A-Za-z0-9_\u0080-￿]*)?\$/;

/**
 * Bounded memoization of {@link splitPlaceholders}: an ORM adapter re-parses the SAME handful of
 * SQL statements (the tx create path, decision 26) on every `trigger(..., { tx })` call, so a
 * small cache turns that back into a one-time cost per distinct statement. Plain insertion-order
 * eviction (not full LRU) is enough for the bounded, small set of statements actually in play; the
 * cached `SplitSql` is never mutated by either caller (`toKnexRaw`, `drizzleTx`), so sharing the
 * same arrays across calls is safe.
 */
const PLACEHOLDER_CACHE_MAX = 64;
const placeholderCache = new Map<string, SplitSql>();

/**
 * Finds the `$n` placeholders of `text` the way the Postgres lexer does: `$n` inside a quoted
 * literal (`'...'`, `E'...'`), a quoted identifier, a comment (nested block comments included)
 * or a dollar-quoted string is text, not a placeholder; neither is `$n` glued to an identifier
 * (`a$1`). Throws on an unterminated literal/comment instead of guessing (never cached).
 */
export function splitPlaceholders(text: string): SplitSql {
	const cached = placeholderCache.get(text);
	if (cached) return cached;
	const result = computeSplitPlaceholders(text);
	if (placeholderCache.size >= PLACEHOLDER_CACHE_MAX) {
		const oldest = placeholderCache.keys().next().value;
		if (oldest !== undefined) placeholderCache.delete(oldest);
	}
	placeholderCache.set(text, result);
	return result;
}

function computeSplitPlaceholders(text: string): SplitSql {
	const segments: string[] = [];
	const indexes: number[] = [];
	let segmentStart = 0;
	let i = 0;
	const n = text.length;
	const unterminated = (what: string): never => {
		throw new Error(`unterminated ${what} in SQL passed to a minitrigger ORM adapter`);
	};

	while (i < n) {
		const c = text[i];
		const next = text[i + 1];
		if (c === "'") {
			// E'...' strings (the E is right before the quote) allow backslash escapes.
			const escapes =
				i > 0 && (text[i - 1] === "E" || text[i - 1] === "e") && !isIdentChar(text[i - 2]);
			i++;
			for (;;) {
				if (i >= n) unterminated("string literal");
				const d = text[i];
				if (escapes && d === "\\") {
					i += 2;
				} else if (d === "'") {
					if (text[i + 1] === "'") i += 2;
					else break;
				} else {
					i++;
				}
			}
			i++;
		} else if (c === '"') {
			i++;
			for (;;) {
				if (i >= n) unterminated("quoted identifier");
				if (text[i] === '"') {
					if (text[i + 1] === '"') i += 2;
					else break;
				} else {
					i++;
				}
			}
			i++;
		} else if (c === "-" && next === "-") {
			while (i < n && text[i] !== "\n") i++;
		} else if (c === "/" && next === "*") {
			let depth = 1;
			i += 2;
			while (depth > 0) {
				if (i >= n) unterminated("block comment");
				if (text[i] === "/" && text[i + 1] === "*") {
					depth++;
					i += 2;
				} else if (text[i] === "*" && text[i + 1] === "/") {
					depth--;
					i += 2;
				} else {
					i++;
				}
			}
		} else if (c === "$" && isDigit(next) && !isIdentChar(text[i - 1])) {
			let j = i + 1;
			while (isDigit(text[j])) j++;
			segments.push(text.slice(segmentStart, i));
			indexes.push(Number(text.slice(i + 1, j)));
			i = j;
			segmentStart = j;
		} else if (c === "$" && !isIdentChar(text[i - 1])) {
			const tag = DOLLAR_TAG.exec(text.slice(i))?.[0];
			if (tag === undefined) {
				i++;
			} else {
				const end = text.indexOf(tag, i + tag.length);
				if (end < 0) unterminated("dollar-quoted string");
				i = end + tag.length;
			}
		} else if (isIdentChar(c)) {
			// Skip the whole identifier/keyword so a `$` inside it (`a$1`) is never a placeholder.
			while (i < n && isIdentChar(text[i])) i++;
		} else {
			i++;
		}
	}
	segments.push(text.slice(segmentStart));
	return { segments, indexes };
}

/** `values[index - 1]`, or a clear error when the SQL references a parameter it was not given. */
export function valueAt(values: readonly unknown[], index: number): unknown {
	if (index < 1 || index > values.length) {
		throw new Error(
			`SQL passed to a minitrigger ORM adapter references $${index}, but ${values.length} value(s) were given`
		);
	}
	const value = values[index - 1];
	// node-postgres sends `undefined` as NULL; knex rejects it and drizzle drops it silently.
	return value === undefined ? null : value;
}

/**
 * Knex treats every `?` in raw SQL as a positional binding (`??` as an identifier), even inside
 * string literals and jsonb operators (`?`, `?|`, `?&`); `\?` is its escape for a literal `?`.
 *
 * Throws on a segment containing a backslash: a segment ending in one, glued directly to a `?`
 * knex would otherwise inject for the next `$n` (or that a preceding segment's own escaped `?`
 * happened to end with), turns into a `\?`/`\\?` sequence that knex's escaping does not parse the
 * way the surrounding SQL intends -- e.g. a real placeholder silently swallowed as a literal `?`.
 * {@link toKnexRaw} additionally joins segments with `" ? "` (padded with spaces) rather than a
 * bare `?`, so an injected placeholder is never adjacent to a segment boundary either; this throw
 * is the remaining, simpler guard for a backslash appearing INSIDE a segment. Minitrigger's own
 * SQL never contains one; raw SQL with a literal backslash (an escape-string literal, a regex
 * operator, ...) is not supported through `knexTx`.
 */
export function escapeKnexSql(segment: string): string {
	if (segment.includes("\\")) {
		throw new Error(
			"SQL passed to a minitrigger ORM adapter contains a backslash, which knex's raw `?` " +
				"placeholder escaping cannot handle reliably (a literal ? next to it could be " +
				"misparsed as an escape, or vice versa); knexTx does not support backslashes in raw SQL"
		);
	}
	return segment.replaceAll("?", "\\?");
}

/** `$n` SQL → knex `raw(sql, bindings)`: `?` per placeholder, one binding per occurrence. */
export function toKnexRaw(
	text: string,
	values: readonly unknown[] = []
): { sql: string; bindings: unknown[] } {
	const { segments, indexes } = splitPlaceholders(text);
	const bindings = indexes.map(index => valueAt(values, index));
	// Padded with spaces (not a bare "?"): extra whitespace around a bind parameter is always
	// syntactically inert in SQL, and it keeps an injected placeholder from ever sitting directly
	// against a segment's own escaped `?` (or a stray backslash `escapeKnexSql` didn't reject).
	return { sql: segments.map(escapeKnexSql).join(" ? "), bindings };
}
