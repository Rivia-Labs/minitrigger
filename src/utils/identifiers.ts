import { ConfigError } from "../errors.js";
import { truncateUtf8 } from "./error-format.js";

const SQL_IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]{0,62}$/;
const QUEUE_NAME_PATTERN = /^[a-z0-9_-]{1,64}$/;
const TASK_ID_PATTERN = /^[a-zA-Z0-9_.-]{1,128}$/;

const RESERVED_QUEUE_NAME = "system";
const STEP_NAME_MAX_LENGTH = 256;
const IDEMPOTENCY_KEY_MAX_LENGTH = 512;

/** Max bytes of a user-supplied identifier echoed back in an error message (avoids dumping a huge string into a log). */
const MAX_ECHOED_VALUE_BYTES = 200;

/** Truncates `value` (UTF-8 safe) for inclusion in an error message, marking it with "…" when cut. */
function echo(value: string): string {
	const truncated = truncateUtf8(value, MAX_ECHOED_VALUE_BYTES);
	return truncated.length < value.length ? `${truncated}…` : truncated;
}

/**
 * Audit item low-a: `jsonb`/`text` both reject a NUL (`\u0000`), and `text` silently re-encodes a
 * lone UTF-16 surrogate (one not part of a valid high/low pair, e.g. from a mistranslated
 * emoji/CJK codepoint) as U+FFFD when it's written (Postgres encodes strings as UTF-8, which has
 * no way to represent an unpaired surrogate; see docs/store-contract.md, "Postgres value
 * semantics"). Neither is safe in an identifier that's compared/looked-up later:
 * - a `steps`/`idempotency_keys`/`runs` NUL would only surface as the raw pg error (SQLSTATE
 *   `22021`/`22P05`) AFTER the user's step function already ran, instead of failing fast;
 * - a lone surrogate in a step name silently becomes a *different* string (U+FFFD) once
 *   persisted; the next replay's snapshot reads back the U+FFFD version and compares it against
 *   the ORIGINAL (still lone-surrogate) name the user's code computes again, which never matches
 *   -> `NonDeterminismError`, with no clue that the root cause is an unpaired surrogate.
 * Returns a reason string (`"NUL"` or `"an unpaired UTF-16 surrogate"`) when `value` contains
 * either, else `null`.
 */
function findUnsafeCharacter(value: string): string | null {
	for (let i = 0; i < value.length; i++) {
		const code = value.charCodeAt(i);
		if (code === 0) return "NUL";
		if (code >= 0xd800 && code <= 0xdbff) {
			// High surrogate: valid only when immediately followed by a low surrogate.
			const next = value.charCodeAt(i + 1);
			if (next >= 0xdc00 && next <= 0xdfff) {
				i++; // Skip the paired low surrogate; it's not "lone".
				continue;
			}
			return "an unpaired UTF-16 surrogate";
		}
		if (code >= 0xdc00 && code <= 0xdfff) {
			// Low surrogate not preceded by a high one (loop above always consumes a valid pair).
			return "an unpaired UTF-16 surrogate";
		}
	}
	return null;
}

/**
 * Throws a `ConfigError` naming `kind`/the offending value when it contains NUL or a lone UTF-16
 * surrogate (see {@link findUnsafeCharacter}); a no-op otherwise. Shared by every identifier
 * validator below whose value ends up in a Postgres `text` column looked up again later.
 */
function assertNoUnsafeCharacter(kind: string, value: string): void {
	const problem = findUnsafeCharacter(value);
	if (problem !== null) {
		throw new ConfigError(
			`${kind} "${echo(value)}" is invalid: contains ${problem}, which Postgres either ` +
				"rejects or silently rewrites (U+FFFD) -- remove it from the value"
		);
	}
}

/** Validates a Postgres schema name or BullMQ prefix (decision 15). */
export function validateSqlIdentifier(kind: "schema" | "prefix", value: string): string {
	if (!SQL_IDENTIFIER_PATTERN.test(value)) {
		throw new ConfigError(
			`${kind} "${echo(value)}" is invalid: must be 1-63 characters, start with a lowercase ` +
				"letter or underscore, and contain only lowercase letters, digits and underscores " +
				`(pattern ${SQL_IDENTIFIER_PATTERN.source})`
		);
	}
	return value;
}

/** Validates a user-declared queue name; the `system` name is reserved for the internal queue. */
export function validateQueueName(value: string): string {
	if (!QUEUE_NAME_PATTERN.test(value)) {
		throw new ConfigError(
			`queue name "${echo(value)}" is invalid: must be 1-64 characters using only lowercase ` +
				`letters, digits, "_" and "-" (pattern ${QUEUE_NAME_PATTERN.source})`
		);
	}
	if (value === RESERVED_QUEUE_NAME) {
		throw new ConfigError(
			'queue name "system" is reserved for the internal queue (named "<prefix>-system"); ' +
				'choose a different name, e.g. { queue: { name: "my-queue" } }'
		);
	}
	return value;
}

/**
 * Validates a `step.*` name (decision 11): non-empty, bounded length, and `#` and `:` are
 * reserved (`#` for internal step names such as `${name}#wait`; `:` separates the components of
 * the internal `__child:${runId}:${name}[:${i}]` idempotency keys, decision 9, so a name with `:`
 * could collide with another step's batch child key).
 */
export function validateStepName(value: string): string {
	if (value.length === 0) {
		throw new ConfigError(
			"step name is invalid: must not be empty; pass a non-empty name to step.*(name, ...)"
		);
	}
	if (value.length > STEP_NAME_MAX_LENGTH) {
		throw new ConfigError(
			`step name "${echo(value)}" is invalid: must be at most ${STEP_NAME_MAX_LENGTH} ` +
				`characters, got ${value.length}`
		);
	}
	assertNoUnsafeCharacter("step name", value);
	if (value.includes("#")) {
		throw new ConfigError(
			`step name "${echo(value)}" is invalid: "#" is reserved for internal step names ` +
				'(e.g. "name#wait"); remove it from your step name'
		);
	}
	if (value.includes(":")) {
		throw new ConfigError(
			`step name "${echo(value)}" is invalid: ":" is reserved (it separates the parts of ` +
				'internal child-run keys); use "-" or "/" instead, e.g. "fetch-1" or "fetch/1"'
		);
	}
	return value;
}

/** Validates an idempotency key (decision 9): non-empty, bounded length, and `__` prefix is reserved. */
export function validateIdempotencyKey(value: string): string {
	if (value.length === 0) {
		throw new ConfigError(
			"idempotency key is invalid: must not be empty; pass a non-empty " +
				"options.idempotencyKey to trigger()"
		);
	}
	if (value.length > IDEMPOTENCY_KEY_MAX_LENGTH) {
		throw new ConfigError(
			`idempotency key "${echo(value)}" is invalid: must be at most ` +
				`${IDEMPOTENCY_KEY_MAX_LENGTH} characters, got ${value.length}`
		);
	}
	assertNoUnsafeCharacter("idempotency key", value);
	if (value.startsWith("__")) {
		throw new ConfigError(
			`idempotency key "${echo(value)}" is invalid: keys starting with "__" are reserved ` +
				'for internal use; choose a key that doesn\'t start with "__"'
		);
	}
	return value;
}

/**
 * Validates a user-supplied debounce key (decision 24) or concurrency key (decision 25) like an
 * idempotency key: a non-empty string of at most 512 characters, `__` prefix reserved.
 */
export function validateTriggerKey(
	kind: "debounce key" | "concurrency key",
	option: string,
	value: unknown
): string {
	if (typeof value !== "string" || value.length === 0) {
		throw new ConfigError(
			`${kind} is invalid: must be a non-empty string; pass a non-empty ${option} to trigger()`
		);
	}
	if (value.length > IDEMPOTENCY_KEY_MAX_LENGTH) {
		throw new ConfigError(
			`${kind} "${echo(value)}" is invalid: must be at most ` +
				`${IDEMPOTENCY_KEY_MAX_LENGTH} characters, got ${value.length}`
		);
	}
	assertNoUnsafeCharacter(kind, value);
	if (value.startsWith("__")) {
		throw new ConfigError(
			`${kind} "${echo(value)}" is invalid: keys starting with "__" are reserved ` +
				'for internal use; choose a key that doesn\'t start with "__"'
		);
	}
	return value;
}

/** Validates a run/task id used as (part of) a BullMQ job id: no `:`, bounded length. */
export function validateTaskId(value: string): string {
	if (!TASK_ID_PATTERN.test(value)) {
		throw new ConfigError(
			`task id "${echo(value)}" is invalid: must be 1-128 characters using only letters, ` +
				`digits, ".", "_" and "-" (no ":"), pattern ${TASK_ID_PATTERN.source}`
		);
	}
	return value;
}
