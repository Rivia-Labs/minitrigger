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
 * `name`, `message`, `stack` and `cause` are read off `Error` instances.
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
			name: err.name.length > 0 ? err.name : "Error",
			message: truncateUtf8(err.message, MAX_MESSAGE_BYTES),
			retryable,
		};
		if (err.stack !== undefined) {
			formatted.stack = truncateUtf8(err.stack, MAX_STACK_BYTES);
		}
		if (depth < MAX_CAUSE_DEPTH && err.cause !== undefined) {
			formatted.cause = formatErrorAtDepth(err.cause, retryable, depth + 1);
		}
		return formatted;
	}

	if (typeof err === "string") {
		return { name: "Error", message: truncateUtf8(err, MAX_MESSAGE_BYTES), retryable };
	}

	return {
		name: "Error",
		message: truncateUtf8(describeNonError(err), MAX_MESSAGE_BYTES),
		retryable,
	};
}

function describeNonError(value: unknown): string {
	if (value === undefined) return "undefined";
	if (typeof value === "bigint" || typeof value === "symbol" || typeof value === "function") {
		return String(value);
	}
	try {
		const json = JSON.stringify(value);
		return json ?? String(value);
	} catch {
		return String(value);
	}
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
