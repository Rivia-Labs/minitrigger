import { SerializationError } from "../errors.js";
import { truncateUtf8 } from "./error-format.js";

/** A plain JSON value, for typing things read back from the database at runtime. */
export type Json = JsonPrimitive | JsonObject | readonly Json[];
export type JsonPrimitive = string | number | boolean | null;
export interface JsonObject {
	[key: string]: Json | undefined;
}

/**
 * Branded "error leaf" produced by {@link Serializable} in place of a type that
 * isn't JSON-serializable. It carries the reason as a string literal so it
 * shows up directly in the compiler's error message.
 */
export type SerializableError<Message extends string> = {
	readonly __serializableError: Message;
};

type JsonPrimitiveOrUndefined = string | number | boolean | null | undefined;

/**
 * Compile-time structural check: `Serializable<T>` mirrors `T`, except that any
 * leaf which isn't JSON-serializable (`Date`, `Map`, `Set`, `RegExp`, functions,
 * `bigint`, `symbol`, class instances with methods, ...) is replaced by a
 * {@link SerializableError} branded type carrying a readable reason.
 *
 * Works with both `interface` and `type` declarations (it walks `keyof T`
 * rather than requiring `T extends Json`, which would reject interfaces), and
 * distributes over unions, so `Serializable<A | B>` is `Serializable<A> | Serializable<B>`.
 *
 * Intended usage as a parameter constraint, so a caller passing a non-serializable
 * value gets a compile error at the call site:
 *
 * ```ts
 * function trigger<T>(payload: T & Serializable<T>): void { ... }
 * trigger({ when: new Date() }); // ts error: Date is not JSON-serializable
 * ```
 *
 * or, when `T` is already fixed/inferred elsewhere:
 *
 * ```ts
 * function assertPayload<T>(payload: Serializable<T>): void { ... }
 * ```
 */
export type Serializable<T> = unknown extends T
	? // `T` is `unknown` (or `any`): nothing to check structurally, and there's nothing more
		// specific we could narrow it to.
		T
	: T extends void
		? // `T` is `void` (a task/step `run` that returns nothing, e.g. `async () => {}`), which
			// used to fall through to the recursive object/array branches below (or the final
			// error leaf) instead of resolving to itself. Bare (not `[T] extends [void]`): TS
			// treats `void` in a bracketed/parenthesized position as a type annotation, which
			// Biome's `noConfusingVoidType` flags, but not as the bare operand of a conditional
			// type's `extends` clause -- and a bare check is fine here, since distributing this
			// specific branch over a union `T` (e.g. `Serializable<string | Date>`) still lets
			// each member fall through to the same per-member `Json`/structural checks below.
			T
		: [T] extends [Json]
			? // Short-circuit once `T` already fits the (self-referential) `Json` shape, instead of
				// recursing into it: recursing into `Json`'s own `JsonObject`/array branches is what
				// triggered "type instantiation is excessively deep" (TS2589) for `Serializable<Json>`
				// and for fields like `{ data: Json[] }`.
				T
			: SerializableInner<T>;

/**
 * The recursive structural walk behind {@link Serializable}. Split out so the guards above
 * (`unknown`, `void`, `Json`) run once at the top of every level of recursion -- nested calls
 * go through `Serializable<T[K]>`, not this helper directly, so a nested `Json`/`void`/`unknown`
 * field gets the same short-circuit treatment.
 */
type SerializableInner<T> = T extends JsonPrimitiveOrUndefined
	? T
	: T extends bigint
		? SerializableError<"bigint is not JSON-serializable; convert to a number or string">
		: T extends symbol
			? SerializableError<"symbol is not JSON-serializable">
			: T extends (...args: any[]) => unknown
				? SerializableError<"functions are not JSON-serializable">
				: T extends Date
					? SerializableError<"Date is not JSON-serializable; convert to ISO string">
					: T extends ReadonlyMap<unknown, unknown>
						? SerializableError<"Map is not JSON-serializable; convert to a plain object or array of entries">
						: T extends ReadonlySet<unknown>
							? SerializableError<"Set is not JSON-serializable; convert to an array">
							: T extends RegExp
								? SerializableError<"RegExp is not JSON-serializable; convert to a string">
								: T extends readonly unknown[]
									? { [K in keyof T]: Serializable<T[K]> }
									: T extends object
										? { [K in keyof T]: Serializable<T[K]> }
										: SerializableError<"value is not JSON-serializable">;

/**
 * Normalizes a task/step **output** before it's validated: a top-level `undefined` (the result
 * of a `run` typed/declared to return nothing, e.g. `async () => {}`, whose type is `void` per
 * the {@link Serializable} fix above) becomes `null`, matching the `runs.output`/`steps.output`
 * column, which stores `null` rather than the absence of a value.
 *
 * Callers: apply this to a task/step **output** right before {@link assertSerializable}, e.g.
 * `assertSerializable(normalizeOutput(result), { label: ..., maxBytes: maxStepOutputBytes })`.
 * Do **not** use it for payloads (`trigger()`'s `payload`, `tokens.complete()`'s `output`): those
 * call {@link assertSerializable} directly on the raw value, so a caller that forgets to pass a
 * payload still gets a clear error instead of it silently becoming `null`.
 */
export function normalizeOutput(value: unknown): unknown {
	return value === undefined ? null : value;
}

/**
 * Runtime counterpart of {@link Serializable}. Walks `value` once, throwing a
 * {@link SerializationError} with a precise path (`field \`a.b[0]\``) at the
 * first non-serializable leaf, circular reference, or top-level `undefined`.
 * `undefined` property values inside plain objects are allowed (JSON.stringify
 * drops them); `undefined` inside arrays is rejected, matching JSON.stringify's
 * own (surprising) behavior of turning it into `null` -- we'd rather fail loudly.
 * This function itself keeps rejecting a top-level `undefined` (it's meant for payloads and
 * for outputs *after* {@link normalizeOutput} has already run); it doesn't normalize on its own.
 *
 * On success, stringifies exactly once and returns the byte size, throwing if
 * it exceeds `opts.maxBytes`.
 */
export function assertSerializable(
	value: unknown,
	opts: { label: string; maxBytes: number }
): number {
	const problem = findSerializationProblem(value, "", new Set());
	if (problem !== undefined) {
		throw new SerializationError(`${opts.label} is not serializable: ${problem}`);
	}

	const json = JSON.stringify(value) ?? "null";
	const bytes = Buffer.byteLength(json, "utf8");
	if (bytes > opts.maxBytes) {
		throw new SerializationError(
			`${opts.label} is ${formatBytes(bytes)}, exceeding the configured size limit (${formatBytes(opts.maxBytes)})`
		);
	}
	return bytes;
}

function findSerializationProblem(
	value: unknown,
	path: string,
	ancestors: Set<object>
): string | undefined {
	if (value === null) return undefined;

	switch (typeof value) {
		case "string": {
			const problem = findStringProblem(value, () => describePath(path));
			if (problem !== undefined) return problem;
			return undefined;
		}
		case "boolean":
			return undefined;
		case "number":
			if (Number.isNaN(value))
				return `${describePath(path)} is NaN, which is not JSON-serializable`;
			if (!Number.isFinite(value))
				return `${describePath(path)} is Infinity, which is not JSON-serializable`;
			return undefined;
		case "undefined":
			return `${describePath(path)} is undefined`;
		case "bigint":
			return `${describePath(path)} is a bigint`;
		case "symbol":
			return `${describePath(path)} is a symbol`;
		case "function":
			return `${describePath(path)} is a function`;
		default:
			break;
	}

	const obj = value as object;

	if (obj instanceof Date) return `${describePath(path)} is a Date`;
	if (obj instanceof Map) return `${describePath(path)} is a Map`;
	if (obj instanceof Set) return `${describePath(path)} is a Set`;
	if (obj instanceof RegExp) return `${describePath(path)} is a RegExp`;

	if (ancestors.has(obj)) {
		return `${describePath(path)} contains a circular reference`;
	}

	if (Array.isArray(obj)) {
		ancestors.add(obj);
		for (let i = 0; i < obj.length; i++) {
			const problem = findSerializationProblem(obj[i], `${path}[${i}]`, ancestors);
			if (problem !== undefined) {
				ancestors.delete(obj);
				return problem;
			}
		}
		ancestors.delete(obj);
		return undefined;
	}

	const proto: object | null = Object.getPrototypeOf(obj) as object | null;
	if (proto !== null && proto !== Object.prototype) {
		const ctor = (obj as { constructor?: { name?: string } }).constructor;
		const label = ctor?.name !== undefined && ctor.name.length > 0 ? ctor.name : "class";
		return `${describePath(path)} is a ${label} instance, which is not JSON-serializable; use a plain object`;
	}

	ancestors.add(obj);
	for (const key of Object.keys(obj)) {
		const keyProblem = findStringProblem(key, () =>
			path === "" ? `object key \`${key}\`` : `object key \`${key}\` of field \`${path}\``
		);
		if (keyProblem !== undefined) {
			ancestors.delete(obj);
			return keyProblem;
		}

		const propValue = (obj as Record<string, unknown>)[key];
		if (propValue === undefined) continue; // dropped by JSON.stringify, allowed
		const fieldPath = path === "" ? key : `${path}.${key}`;
		const problem = findSerializationProblem(propValue, fieldPath, ancestors);
		if (problem !== undefined) {
			ancestors.delete(obj);
			return problem;
		}
	}
	ancestors.delete(obj);
	return undefined;
}

function describePath(path: string): string {
	return path === "" ? "value" : `field \`${path}\``;
}

/**
 * Postgres jsonb rejects both a NUL byte (`\u0000`) anywhere in a string and any string that
 * isn't "well-formed" Unicode (i.e. contains a lone/unpaired surrogate) -- `JSON.stringify`
 * happily encodes both, so the runtime walk has to catch them explicitly. Used for both string
 * *values* and object *keys* (Postgres jsonb applies the same rules to keys).
 */
function findStringProblem(value: string, describe: () => string): string | undefined {
	if (value.includes("\u0000")) {
		return `${describe()} contains a NUL byte (U+0000), which Postgres jsonb rejects`;
	}
	if (!value.isWellFormed()) {
		return `${describe()} is not well-formed Unicode (contains an unpaired surrogate), which Postgres jsonb rejects`;
	}
	return undefined;
}

function formatBytes(bytes: number): string {
	if (bytes < 1024) return `${bytes} B`;
	const units = ["KB", "MB", "GB", "TB"] as const;
	let value = bytes / 1024;
	let unitIndex = 0;
	while (value >= 1024 && unitIndex < units.length - 1) {
		value /= 1024;
		unitIndex += 1;
	}
	const unit = units[unitIndex] ?? "TB";
	return `${Math.round(value * 10) / 10} ${unit}`;
}

/**
 * For `maxLogEntryBytes`: returns `data` as-is if its JSON representation fits
 * within `maxBytes`, otherwise a `{ __truncated, preview, originalBytes }`
 * marker whose own JSON representation fits within `maxBytes`.
 *
 * The preview is cut with {@link truncateUtf8} (byte-based, never splits a UTF-16 surrogate
 * pair or UTF-8 codepoint), then -- because JSON-escaping quotes/backslashes/control characters
 * can make the *stringified* preview a few bytes larger than the raw cut -- shrunk by the
 * measured overflow in a small, bounded number of retries. That's O(n) plus a handful of
 * retries, rather than the previous approach of re-measuring the whole (possibly huge) string
 * while shaving it down one small step at a time, which degraded to O(n^2) for large payloads.
 */
export function truncateLogData(data: unknown, maxBytes: number): unknown {
	if (data === undefined) return data;

	let json: string;
	try {
		json = JSON.stringify(data) ?? "null";
	} catch {
		json = String(data);
	}

	const originalBytes = Buffer.byteLength(json, "utf8");
	if (originalBytes <= maxBytes) return data;

	const overheadBytes = Buffer.byteLength(
		JSON.stringify({ __truncated: true, preview: "", originalBytes }),
		"utf8"
	);
	let budget = Math.max(0, maxBytes - overheadBytes);

	for (;;) {
		const preview = budget > 0 ? truncateUtf8(json, budget) : "";
		const truncated = { __truncated: true as const, preview, originalBytes };
		const size = Buffer.byteLength(JSON.stringify(truncated), "utf8");
		if (size <= maxBytes || budget === 0) {
			return truncated;
		}
		budget = Math.max(0, budget - (size - maxBytes));
	}
}
