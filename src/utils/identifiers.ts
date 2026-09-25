import { ConfigError } from "../errors.js";

const SQL_IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]{0,62}$/;
const QUEUE_NAME_PATTERN = /^[a-z0-9_-]{1,64}$/;
const TASK_ID_PATTERN = /^[a-zA-Z0-9_.-]{1,128}$/;

const RESERVED_QUEUE_NAME = "system";
const STEP_NAME_MAX_LENGTH = 256;
const IDEMPOTENCY_KEY_MAX_LENGTH = 512;

/** Validates a Postgres schema name or BullMQ prefix (decision 15). */
export function validateSqlIdentifier(kind: "schema" | "prefix", value: string): string {
	if (!SQL_IDENTIFIER_PATTERN.test(value)) {
		throw new ConfigError(
			`${kind} "${value}" is invalid: must match ${SQL_IDENTIFIER_PATTERN.source}`
		);
	}
	return value;
}

/** Validates a user-declared queue name; the `system` name is reserved for the internal queue. */
export function validateQueueName(value: string): string {
	if (!QUEUE_NAME_PATTERN.test(value)) {
		throw new ConfigError(
			`queue name "${value}" is invalid: must match ${QUEUE_NAME_PATTERN.source}`
		);
	}
	if (value === RESERVED_QUEUE_NAME) {
		throw new ConfigError(
			`queue name "system" is reserved: the internal system queue is named "\${prefix}-system"`
		);
	}
	return value;
}

/** Validates a `step.*` name (decision 11): non-empty, bounded length, and `#` is reserved. */
export function validateStepName(value: string): string {
	if (value.length === 0) {
		throw new ConfigError("step name is invalid: must not be empty");
	}
	if (value.length > STEP_NAME_MAX_LENGTH) {
		throw new ConfigError(
			`step name "${value}" is invalid: must be at most ${STEP_NAME_MAX_LENGTH} characters`
		);
	}
	if (value.includes("#")) {
		throw new ConfigError(`step name "${value}" is invalid: "#" is reserved`);
	}
	return value;
}

/** Validates an idempotency key (decision 9): non-empty, bounded length, and `__` prefix is reserved. */
export function validateIdempotencyKey(value: string): string {
	if (value.length === 0) {
		throw new ConfigError("idempotency key is invalid: must not be empty");
	}
	if (value.length > IDEMPOTENCY_KEY_MAX_LENGTH) {
		throw new ConfigError(
			`idempotency key "${value}" is invalid: must be at most ${IDEMPOTENCY_KEY_MAX_LENGTH} characters`
		);
	}
	if (value.startsWith("__")) {
		throw new ConfigError(
			`idempotency key "${value}" is invalid: keys starting with "__" are reserved for internal use`
		);
	}
	return value;
}

/** Validates a run/task id used as (part of) a BullMQ job id: no `:`, bounded length. */
export function validateTaskId(value: string): string {
	if (!TASK_ID_PATTERN.test(value)) {
		throw new ConfigError(`task id "${value}" is invalid: must match ${TASK_ID_PATTERN.source}`);
	}
	return value;
}
