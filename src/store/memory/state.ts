/**
 * In-memory tables, undo journal and value helpers of the Phase 9 test-mode store. Every row is
 * immutable once stored: a write replaces the row in its `Map` through {@link MemTx.set}, which
 * records the inverse operation so a failing operation (or transaction) rolls back exactly like a
 * Postgres ROLLBACK. Times are stored as epoch milliseconds from the store's injected clock (the
 * "DB clock" of test mode, decision 17).
 */
import type {
	Json,
	LogLevel,
	RunStatus,
	ScheduleSource,
	SerializedError,
	StepKind,
	StepStatus,
	WaitpointKind,
	WaitpointStatus,
} from "../types.js";

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

export interface RunRow {
	readonly id: string;
	readonly taskId: string;
	readonly queue: string;
	readonly payload: Json;
	readonly status: RunStatus;
	readonly output: Json | null;
	readonly error: SerializedError | null;
	readonly attempt: number;
	readonly maxAttempts: number;
	readonly executionId: string | null;
	readonly enqueueSeq: number;
	readonly heartbeatAt: number | null;
	readonly runAfter: number | null;
	readonly parentRunId: string | null;
	readonly parentWaitpointId: string | null;
	readonly parentStepName: string | null;
	readonly batchIndex: number | null;
	readonly startedAt: number | null;
	readonly finishedAt: number | null;
	readonly createdAt: number;
	readonly updatedAt: number;
	/** `ctx.progress.set` (decision 23); `null` until the first `set`. */
	readonly progress: Json | null;
}

export interface StepRow {
	readonly runId: string;
	readonly name: string;
	readonly seqIndex: number;
	readonly kind: StepKind;
	readonly status: StepStatus;
	/** `null` for both SQL NULL (sleeps) and a JSON `null`; see {@link outputBytes}. */
	readonly output: Json | null;
	/** Approximation of `pg_column_size(output)`: JSON byte length, `null` for SQL NULL. */
	readonly outputBytes: number | null;
	readonly wakeAt: number | null;
	readonly createdAt: number;
}

export interface WaitpointRow {
	readonly id: string;
	readonly runId: string;
	readonly stepName: string;
	readonly kind: WaitpointKind;
	readonly status: WaitpointStatus;
	readonly pendingCount: number | null;
	readonly output: Json | null;
	readonly expiresAt: number | null;
	readonly createdAt: number;
	readonly resolvedAt: number | null;
	/**
	 * Set only by `timeOutWaitpointsOf` (every terminal run transition times out its own pending
	 * waitpoints in the same transaction): distinguishes that from a waitpoint that expired on its
	 * own (`waitpoints.ts#expire`), even when both happen to share the same frozen `tx.now` (the
	 * memory clock does not advance on its own between transactions, unlike Postgres' `now()`).
	 */
	readonly timedOutByRunExit?: boolean;
}

export interface KeyRow {
	readonly taskId: string;
	readonly key: string;
	readonly runId: string;
	readonly expiresAt: number | null;
}

export interface LogRow {
	readonly id: bigint;
	readonly runId: string;
	readonly level: LogLevel;
	readonly message: string;
	readonly data: Json | null;
	readonly ts: number;
}

export interface ScheduleRow {
	readonly id: string;
	readonly taskId: string;
	readonly cron: string;
	readonly timezone: string | null;
	readonly payload: Json | null;
	readonly source: ScheduleSource;
	readonly createdAt: number;
	readonly updatedAt: number;
}

/** All tables. `steps` is keyed by run id; each inner map is replaced (never mutated) on write. */
export class MemDb {
	readonly runs = new Map<string, RunRow>();
	readonly steps = new Map<string, ReadonlyMap<string, StepRow>>();
	readonly waitpoints = new Map<string, WaitpointRow>();
	/** Keyed by {@link keyOf}(taskId, key). */
	readonly keys = new Map<string, KeyRow>();
	readonly logs = new Map<bigint, LogRow>();
	readonly schedules = new Map<string, ScheduleRow>();
	/** `run_logs.id` bigserial: like a Postgres sequence, never rolled back. */
	logSeq = 0n;
}

export function keyOf(taskId: string, key: string): string {
	return `${taskId}\u0000${key}`;
}

// ---------------------------------------------------------------------------
// Transaction context (undo journal)
// ---------------------------------------------------------------------------

/**
 * One open "transaction": a fixed `now` (Postgres `now()` is the transaction start time) plus
 * the undo journal of every write made through it.
 */
export class MemTx {
	private readonly undo: (() => void)[] = [];

	constructor(
		readonly db: MemDb,
		readonly now: number
	) {}

	set<K, V>(map: Map<K, V>, key: K, value: V): void {
		if (map.has(key)) {
			const prev = map.get(key) as V;
			this.undo.push(() => map.set(key, prev));
		} else {
			this.undo.push(() => map.delete(key));
		}
		map.set(key, value);
	}

	delete<K, V>(map: Map<K, V>, key: K): boolean {
		if (!map.has(key)) return false;
		const prev = map.get(key) as V;
		this.undo.push(() => map.set(key, prev));
		map.delete(key);
		return true;
	}

	/** SAVEPOINT. */
	mark(): number {
		return this.undo.length;
	}

	/** ROLLBACK TO SAVEPOINT (or a full ROLLBACK with `mark = 0`). */
	rollbackTo(mark: number): void {
		while (this.undo.length > mark) this.undo.pop()?.();
	}

	/** COMMIT: forget the journal. */
	commit(): void {
		this.undo.length = 0;
	}
}

// ---------------------------------------------------------------------------
// Serialized access
// ---------------------------------------------------------------------------

/** FIFO async mutex: serializes operations and transactions like a serializable database. */
export class Mutex {
	private tail: Promise<void> = Promise.resolve();

	lock(): Promise<() => void> {
		let release!: () => void;
		const next = new Promise<void>(resolve => {
			release = resolve;
		});
		const acquired = this.tail.then(() => release);
		this.tail = this.tail.then(() => next);
		return acquired;
	}
}

// ---------------------------------------------------------------------------
// Value helpers (Postgres type semantics)
// ---------------------------------------------------------------------------

const UUID_PATTERN =
	/^\{?([0-9a-f]{8})-?([0-9a-f]{4})-?([0-9a-f]{4})-?([0-9a-f]{4})-?([0-9a-f]{12})\}?$/i;

/** Postgres `uuid` input: accepts the forms pg accepts, returns the canonical lowercase text. */
export function uuid(value: string): string {
	const m = typeof value === "string" ? UUID_PATTERN.exec(value) : null;
	const braces = typeof value === "string" && value.startsWith("{") !== value.endsWith("}");
	if (!m || braces) throw new Error(`invalid input syntax for type uuid: "${String(value)}"`);
	return `${m[1]}-${m[2]}-${m[3]}-${m[4]}-${m[5]}`.toLowerCase();
}

/** Nullable {@link uuid}. */
export function uuidOrNull(value: string | null): string | null {
	return value === null ? null : uuid(value);
}

/**
 * A value as `jsonb` stores it: JSON text round trip (drops `undefined` properties, turns
 * `undefined` into `null` like `jsonParam`, `-0` into `0`) and object keys in jsonb order
 * (shorter keys first, then bytewise). Rejects what Postgres' jsonb input rejects (audit item 2),
 * with the same SQLSTATE: U+0000 in a string or key (`22P05`, "unsupported Unicode escape
 * sequence") and a lone UTF-16 surrogate (`JSON.stringify` escapes it as `\udXXX`, which jsonb
 * refuses: `22P02`). An own `__proto__` key stays a plain data property, as `pg` returns it.
 */
export function toJsonb<T extends Json | SerializedError | null>(value: T | undefined): T {
	const text = JSON.stringify(value ?? null) as string | undefined;
	return normalizeKeys(text === undefined ? null : JSON.parse(text)) as T;
}

function checkJsonbString(value: string): void {
	if (value.includes("\u0000")) {
		throw pgError("22P05", "unsupported Unicode escape sequence");
	}
	if (!value.isWellFormed()) {
		throw pgError("22P02", "invalid input syntax for type json");
	}
}

function normalizeKeys(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(normalizeKeys);
	if (value !== null && typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>);
		entries.sort(([a], [b]) => {
			const la = Buffer.byteLength(a);
			const lb = Buffer.byteLength(b);
			if (la !== lb) return la - lb;
			return Buffer.compare(Buffer.from(a), Buffer.from(b));
		});
		for (const [k] of entries) checkJsonbString(k);
		// `Object.fromEntries` defines own data properties: `out[k] = v` with k === "__proto__"
		// would set the prototype instead and silently drop the key.
		return Object.fromEntries(entries.map(([k, v]) => [k, normalizeKeys(v)]));
	}
	if (typeof value === "string") checkJsonbString(value);
	if (typeof value === "number" && Object.is(value, -0)) return 0;
	return value;
}

/**
 * A value as a Postgres `text` column stores it: U+0000 is rejected (`22021`, "invalid byte
 * sequence for encoding UTF8: 0x00"); a lone surrogate can't be encoded as UTF-8, so `pg` sends
 * U+FFFD in its place (Node's `Buffer` encoding), which is what the row then holds.
 */
export function pgText(value: string): string {
	if (value.includes("\u0000")) {
		throw pgError("22021", 'invalid byte sequence for encoding "UTF8": 0x00');
	}
	return value.toWellFormed();
}

/** Nullable {@link pgText}. */
export function pgTextOrNull(value: string | null): string | null {
	return value === null ? null : pgText(value);
}

/** An error shaped like the `DatabaseError` pg raises (`code` = SQLSTATE). */
export function pgError(code: string, message: string): Error {
	const err = new Error(message);
	(err as Error & { code: string }).code = code;
	return err;
}

/** Approximation of `pg_column_size` for a jsonb value: its JSON byte length. */
export function jsonBytes(value: Json | null): number {
	return Buffer.byteLength(JSON.stringify(value));
}

/** Deep copy handed to callers (rows stay immutable inside the store). */
export function cloneJson<T>(value: T): T {
	return value === null || typeof value !== "object" ? value : structuredClone(value);
}

export function toDate(ms: number): Date;
export function toDate(ms: number | null): Date | null;
export function toDate(ms: number | null): Date | null {
	return ms === null ? null : new Date(ms);
}

/** `greatest(0, round(extract(epoch from (t - now())) * 1000))`. */
export function delayMs(at: number, now: number): number {
	return Math.max(0, Math.round(at - now));
}

/** `created_at` of runs is truncated to milliseconds (see CLAUDE.md). */
export function truncMs(ms: number): number {
	return Math.floor(ms);
}

/** SQL `LIMIT n` (negative is an error; fractional values are rounded like an int8 cast). */
export function sqlLimit(n: number): number {
	const v = Math.round(n);
	if (v < 0) throw new Error("LIMIT must not be negative");
	return v;
}

/** A unique-violation error shaped like the one pg raises. */
export function uniqueViolation(constraint: string): Error {
	const err = new Error(`duplicate key value violates unique constraint "${constraint}"`);
	(err as Error & { code: string }).code = "23505";
	return err;
}

/** A foreign-key-violation error shaped like the one pg raises. */
export function fkViolation(table: string, constraint: string): Error {
	const err = new Error(
		`insert or update on table "${table}" violates foreign key constraint "${constraint}"`
	);
	(err as Error & { code: string }).code = "23503";
	return err;
}
