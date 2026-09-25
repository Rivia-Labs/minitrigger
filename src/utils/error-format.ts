import { types as nodeUtilTypes } from "node:util";

export type SerializedError = {
	name: string;
	message: string;
	stack?: string;
	cause?: SerializedError;
	retryable: boolean;
};

const MAX_STACK_BYTES = 4096;
const MAX_MESSAGE_BYTES = 4096;
const MAX_CAUSE_DEPTH = 3;

/**
 * Converts an unknown thrown value into the `{ name, message, stack?, cause?, retryable }`
 * shape stored in `runs.error` (decision 12). Never copies arbitrary properties: only
 * `name`, `message`, `stack` and `cause` are read off `Error` instances, and a non-`Error`
 * thrown value (or `cause`) only ever contributes its own `message`/`name`/`stack`/`cause`
 * *if they're strings* -- never the rest of its (possibly sensitive: headers, tokens, ...)
 * properties (e.g. `throw response`, or `cause: someAxiosLikeError` that isn't itself an
 * `Error`). Every string that ends up in the result (`name`, `message`, `stack`) is run
 * through {@link sanitizeString}: Postgres `jsonb` rejects both a NUL byte and a lone
 * (unpaired) surrogate, and `err.message`/`err.stack` are attacker/user-controlled.
 */
export function formatError(err: unknown, retryable: boolean): SerializedError {
	return formatErrorAtDepth(err, retryable, 0);
}

function formatErrorAtDepth(err: unknown, retryable: boolean, depth: number): SerializedError {
	// `instanceof Error` alone misses errors thrown across a realm boundary (e.g. `node:vm`
	// contexts, or errors from a native addon/worker): they don't share this realm's `Error`
	// prototype, so `instanceof` returns `false` even though they're genuine `Error` objects.
	// `util.types.isNativeError` recognizes those too.
	if (err instanceof Error || nodeUtilTypes.isNativeError(err)) {
		const formatted: SerializedError = {
			name: sanitizeAndTruncate(err.name.length > 0 ? err.name : "Error", MAX_MESSAGE_BYTES),
			message: sanitizeAndTruncate(err.message, MAX_MESSAGE_BYTES),
			retryable,
		};
		if (err.stack !== undefined) {
			formatted.stack = sanitizeAndTruncate(err.stack, MAX_STACK_BYTES);
		}
		if (depth < MAX_CAUSE_DEPTH && err.cause !== undefined) {
			formatted.cause = formatErrorAtDepth(err.cause, retryable, depth + 1);
		}
		return formatted;
	}

	if (typeof err === "string") {
		return { name: "Error", message: sanitizeAndTruncate(err, MAX_MESSAGE_BYTES), retryable };
	}

	return formatNonError(err, retryable, depth);
}

/**
 * Non-`Error`, non-`string` thrown value (or `cause`): `throw response`, `throw { code: 42 }`,
 * `cause: someAxiosLikeErrorThatIsNotAnErrorInstance`, ... Only reads `message`/`name`/`stack`/
 * `cause` off it -- and only when they're themselves strings (`cause` handled by recursing) --
 * never the rest of its properties, however many or however sensitive.
 */
function formatNonError(err: unknown, retryable: boolean, depth: number): SerializedError {
	const record =
		typeof err === "object" && err !== null ? (err as Record<string, unknown>) : undefined;
	const rawMessage = typeof record?.["message"] === "string" ? record["message"] : undefined;
	const rawName =
		typeof record?.["name"] === "string" && record["name"].length > 0 ? record["name"] : undefined;
	const rawStack = typeof record?.["stack"] === "string" ? record["stack"] : undefined;

	const formatted: SerializedError = {
		name: sanitizeAndTruncate(rawName ?? "Error", MAX_MESSAGE_BYTES),
		message: sanitizeAndTruncate(rawMessage ?? describeNonErrorType(err), MAX_MESSAGE_BYTES),
		retryable,
	};
	if (rawStack !== undefined) {
		formatted.stack = sanitizeAndTruncate(rawStack, MAX_STACK_BYTES);
	}
	if (record !== undefined && depth < MAX_CAUSE_DEPTH && record["cause"] !== undefined) {
		formatted.cause = formatErrorAtDepth(record["cause"], retryable, depth + 1);
	}
	return formatted;
}

/**
 * Bounded, safe description of a non-`Error` thrown value that has no string `message` of its
 * own: a type label only, never the value's own (possibly large, possibly sensitive) content --
 * this is the fallback `describeNonError` used to reach for `JSON.stringify(value)`, which
 * happily dumped every property of an arbitrary thrown object (decision 12 forbids that).
 */
function describeNonErrorType(value: unknown): string {
	if (value === null) return "null";
	if (value === undefined) return "undefined";
	if (typeof value === "bigint" || typeof value === "boolean" || typeof value === "number") {
		return String(value);
	}
	if (typeof value === "symbol") return "a symbol was thrown";
	if (typeof value === "function") return "a function was thrown";
	if (Array.isArray(value)) return "an array was thrown";
	const ctor = (value as { constructor?: { name?: string } }).constructor;
	const label = ctor?.name !== undefined && ctor.name.length > 0 ? ctor.name : "object";
	return `a non-Error value of type ${label} was thrown`;
}

/** Sanitizes for Postgres `jsonb`, then truncates to `maxBytes` (see {@link sanitizeString}). */
function sanitizeAndTruncate(text: string, maxBytes: number): string {
	return truncateUtf8(sanitizeString(text), maxBytes);
}

/**
 * Describes an arbitrary value for a user-facing error message ("got ${describeOffendingValue(x)}").
 * Primitives are shown directly (strings quoted and UTF-8-truncated, so a huge or binary string
 * can never blow up the message or dump megabytes into a log); anything else (object, array,
 * function, ...) collapses to a short type description instead of `String(value)`, which for a
 * plain object silently produces the useless `"[object Object]"` and for an array or class
 * instance could interpolate arbitrary nested content -- including, for something like an
 * accidentally-passed connection object, values the message must never echo.
 */
export function describeOffendingValue(value: unknown, maxStringBytes = 200): string {
	if (value === null) return "null";
	if (value === undefined) return "undefined";
	if (typeof value === "string") return JSON.stringify(truncateUtf8(value, maxStringBytes));
	if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") {
		return String(value);
	}
	if (Array.isArray(value)) return "an array";
	if (typeof value === "function") return "a function";
	if (typeof value === "symbol") return "a symbol";
	return "an object";
}

/**
 * Postgres `jsonb` (and `text`) rejects two things a JS string can freely contain: a NUL byte
 * (`\u0000`) anywhere, and a lone/unpaired UTF-16 surrogate (a string that isn't "well-formed"
 * Unicode). Both are trivially reachable from user/attacker-controlled input -- an error
 * `message`/`stack`, or `ctx.logger` `message`/`data` -- and `JSON.stringify` happily encodes
 * both without complaint, so the *insert* is what fails, not the serialization. Used at the
 * boundary right before a string is handed to `formatError` or `ctx.logger.*`'s buffer: replace
 * a NUL byte with U+FFFD (REPLACEMENT CHARACTER) and any lone surrogate the same way (via
 * `String.prototype.toWellFormed()`), so the write itself can never fail on invalid content --
 * silently mangling a handful of bytes in an already-adversarial/malformed message is a much
 * better failure mode than losing the entire error (and stalling the run for up to
 * `stallTimeoutMs`) or dropping an entire batch of up to 500 log lines.
 */
export function sanitizeString(text: string): string {
	const withoutNul = text.includes("\u0000") ? text.replaceAll("\u0000", "�") : text;
	return withoutNul.isWellFormed() ? withoutNul : withoutNul.toWellFormed();
}

/**
 * Truncates `text` to at most `maxBytes` UTF-8 bytes, in O(n): a single `Buffer` encode/cut/
 * decode, rather than the previous approach of shrinking a JS-string slice one UTF-16 code unit
 * at a time and re-measuring its UTF-8 byte length on every step (O(n^2) for a long string with
 * a tight budget).
 *
 * Cutting mid-buffer can land inside a multi-byte UTF-8 sequence -- including, for an astral
 * character (outside the BMP), one whose decoded form is a UTF-16 *surrogate pair*, where
 * slicing off only one half would leave an unpaired/lone surrogate (invalid UTF-16). `Buffer`'s
 * UTF-8 decoder replaces an incomplete trailing sequence with a single U+FFFD (REPLACEMENT
 * CHARACTER) instead of emitting a broken character, so we only need to strip that synthesized
 * placeholder afterwards -- and only when it *was* synthesized by the cut, not when the original
 * text genuinely ended in U+FFFD.
 */
export function truncateUtf8(text: string, maxBytes: number): string {
	if (maxBytes <= 0) return "";

	const full = Buffer.from(text, "utf8");
	if (full.length <= maxBytes) return text;

	const result = full.subarray(0, maxBytes).toString("utf8");
	if (result.endsWith("�") && text.slice(0, result.length) !== result) {
		return result.slice(0, -1);
	}
	return result;
}
