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

/** Validates a `step.*` name (decision 11): non-empty, bounded length, and `#` is reserved. */
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
	if (value.includes("#")) {
		throw new ConfigError(
			`step name "${echo(value)}" is invalid: "#" is reserved for internal step names ` +
				'(e.g. "name#wait"); remove it from your step name'
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
	if (value.startsWith("__")) {
		throw new ConfigError(
			`idempotency key "${echo(value)}" is invalid: keys starting with "__" are reserved ` +
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
