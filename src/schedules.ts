/**
 * Schedules wired into the engine (Phase 8, `docs/SPEC.md` decisions 8, 9, 17, 21; `docs/HANDOFF.md`
 * "2. Fase 8"). `src/store/sql/schedules.ts` / `src/store/memory/schedules.ts` (already implemented)
 * own the `schedules` table; this module owns everything that touches BullMQ and the registry:
 *
 * - `createSchedulesApi`: `engine.schedules.upsert/delete/list` (Postgres first, then
 *   `upsertJobScheduler`/`removeJobScheduler` on the `-system` queue -- decision 8. A BullMQ
 *   failure after the Postgres write is only logged: Postgres is authoritative, and the tick
 *   handler below plus the reconciler hook self-heal any drift).
 * - `syncDeclarativeSchedules`: `defineTask({ schedule })` sync on `engine.start()` (decision 21).
 * - `createReconcileSchedulesHook`: bidirectional reconciliation for `reconciler.ts`'s
 *   `reconcileSchedules` hook.
 * - `processScheduleTick`: the `-system` worker's `schedule-tick` handler (decision 8/9). Postgres
 *   is read fresh on every tick (never trusts the BullMQ job's own copy); the run is created with
 *   idempotency key `__schedule:${id}:${scheduledAtMs}` and `ttlMs: null`, so two processes ticking
 *   the same occurrence create exactly one run (decision 9's `ON CONFLICT ... WHERE expires_at <
 *   now()` claim semantics, `RunStore.create`).
 *
 * Ticks missed while every process is down are not recovered (documented limitation, `README.md`):
 * BullMQ's job scheduler only ever keeps one pending "next iteration" job
 * (`docs/research/api-notes.md` §1.10).
 */

import { randomUUID } from "node:crypto";
import { escapeIdentifier, type Pool } from "pg";
import type { EngineLogger } from "./config.js";
import { ConfigError } from "./errors.js";
import { childTaskSpec, enqueue, type RunNotifier, taskQueueName } from "./executor/index.js";
import type { JobSchedulerInfo, Transport } from "./queue/transport.js";
import { getTask, type Registry } from "./registry.js";
import type { Json, ScheduleRecord, ScheduleSource, Store } from "./store/types.js";
import type { AnyTask, TriggerPayloadField } from "./task.js";
import { assertSerializable } from "./utils/serialization.js";

// ---------------------------------------------------------------------------
// Validation (Postgres/BullMQ are the deeper authorities; this only rejects the obviously wrong
// before a write -- see the comments on each function).
// ---------------------------------------------------------------------------

/**
 * Same shape as `SCHEDULE_ID_PATTERN` in `src/store/sql/schedules.ts` / `memory/schedules.ts`
 * (kept local there too, for the same reason: the schedule id is embedded verbatim in the
 * idempotency key `__schedule:${id}:${ts}` and used as-is as the BullMQ job scheduler id -- no
 * `:`, decision 7's spirit). Duplicated here (not imported from the store) so this module's error
 * message can fire before any store/BullMQ round trip.
 */
const SCHEDULE_ID_PATTERN = /^[a-zA-Z0-9_.-]{1,128}$/;

function validateScheduleId(id: unknown): string {
	if (typeof id !== "string" || !SCHEDULE_ID_PATTERN.test(id)) {
		throw new ConfigError(
			`schedule id ${JSON.stringify(id)} is invalid: must match ${SCHEDULE_ID_PATTERN.source}`
		);
	}
	return id;
}

/**
 * `cron-parser` (via BullMQ's `upsertJobScheduler`) is the actual authority on cron syntax --
 * minitrigger does not depend on it directly (it is a transitive dependency of `bullmq`, not
 * resolvable as a direct import under pnpm's strict `node_modules`, and adding it as a second,
 * separately-versioned cron parser would risk disagreeing with BullMQ's own). This only rejects
 * the obviously malformed (wrong shape, illegal characters) before touching Postgres or Redis, so
 * `schedules.upsert()`/`defineTask({ schedule })` sync fail fast and locally for a typo'd pattern;
 * a pattern that passes this but is still invalid cron surfaces as a `upsertJobScheduler` rejection
 * (logged, decision 8: Postgres already has the row, the reconciler keeps retrying the BullMQ side).
 */
const CRON_FIELD_PATTERN = /^[\d*/,\-A-Za-z?LW#]+$/;

function validateCronPattern(cron: unknown): string {
	if (typeof cron !== "string") {
		throw new ConfigError(`schedule cron must be a string, got ${JSON.stringify(cron)}`);
	}
	const trimmed = cron.trim();
	const fields = trimmed.length === 0 ? [] : trimmed.split(/\s+/);
	if (fields.length < 5 || fields.length > 7 || !fields.every(f => CRON_FIELD_PATTERN.test(f))) {
		throw new ConfigError(
			`schedule cron "${cron}" does not look like a valid cron pattern ` +
				"(expected 5-7 whitespace-separated fields)"
		);
	}
	return trimmed;
}

/** Uses `Intl` (built into Node) instead of a new dependency to reject an unrecognized IANA zone. */
function validateTimezone(timezone: unknown): string {
	if (typeof timezone !== "string" || timezone.length === 0) {
		throw new ConfigError(
			`schedule timezone must be a non-empty string, got ${JSON.stringify(timezone)}`
		);
	}
	try {
		new Intl.DateTimeFormat("en-US", { timeZone: timezone });
	} catch {
		throw new ConfigError(`schedule timezone "${timezone}" is not a recognized IANA timezone`);
	}
	return timezone;
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** Public view of a schedule (`engine.schedules.*`). */
export interface Schedule {
	id: string;
	taskId: string;
	cron: string;
	timezone: string | null;
	payload: Json | null;
	/** `"api"` (`engine.schedules.upsert`) or `"declarative"` (`defineTask({ schedule })`). */
	source: ScheduleSource;
	createdAt: Date;
	updatedAt: Date;
}

function toPublicSchedule(record: ScheduleRecord): Schedule {
	return {
		id: record.id,
		taskId: record.taskId,
		cron: record.cron,
		timezone: record.timezone,
		payload: record.payload,
		source: record.source,
		createdAt: record.createdAt,
		updatedAt: record.updatedAt,
	};
}

/**
 * Input of `engine.schedules.upsert()`. `payload` is guarded exactly like `TaskScheduleOptions`
 * (`src/task.ts`, fix 5): optional for a task that takes none, required and `Serializable`-checked
 * otherwise (`TriggerPayloadField`).
 */
export type ScheduleUpsertInput<T extends AnyTask = AnyTask> = {
	id: string;
	task: T;
	cron: string;
	timezone?: string;
} & TriggerPayloadField<T>;

export interface EngineSchedules {
	/**
	 * Writes Postgres first (source of truth, decision 8), then syncs the BullMQ job scheduler on
	 * the `-system` queue. A BullMQ failure here is only logged -- the row already exists, so the
	 * next reconciler tick (or the next time this schedule is upserted) repairs it.
	 */
	upsert<T extends AnyTask>(input: ScheduleUpsertInput<T>): Promise<Schedule>;
	/**
	 * Deletes the Postgres row first, then removes the BullMQ job scheduler. `false` if no schedule
	 * had this id. If the BullMQ removal fails, the schedule's own tick handler self-heals on its
	 * next fire (schedule missing in Postgres -> the scheduler is removed there, no run created).
	 */
	delete(id: string): Promise<boolean>;
	list(): Promise<Schedule[]>;
}

// ---------------------------------------------------------------------------
// engine.schedules.*
// ---------------------------------------------------------------------------

export interface SchedulesApiDeps {
	store: Pick<Store, "schedules">;
	transport: Pick<Transport, "upsertJobScheduler" | "removeJobScheduler">;
	registry: Registry;
	logger: EngineLogger;
	maxPayloadBytes: number;
}

export function createSchedulesApi(deps: SchedulesApiDeps): EngineSchedules {
	const { store, transport, registry, logger, maxPayloadBytes } = deps;

	async function upsert<T extends AnyTask>(input: ScheduleUpsertInput<T>): Promise<Schedule> {
		if (input === null || typeof input !== "object") {
			throw new ConfigError("schedules.upsert: input must be an object");
		}
		const id = validateScheduleId(input.id);
		const cron = validateCronPattern(input.cron);
		const timezone = input.timezone === undefined ? undefined : validateTimezone(input.timezone);
		const task = getTask(registry, input.task);
		const payload = (input.payload === undefined ? null : input.payload) as Json;
		assertSerializable(payload, {
			label: `payload of schedule "${id}"`,
			maxBytes: maxPayloadBytes,
		});

		const result = await store.schedules.upsert(
			{
				id,
				taskId: task.id,
				cron,
				timezone: timezone ?? null,
				payload,
				source: "api",
			},
			{ requireSourceMatch: true }
		);
		if (result.conflict) {
			// Fix 4: never let `engine.schedules.upsert()` (source "api") silently take over an id
			// already owned by `defineTask({ schedule })` (source "declarative") -- actionable, since
			// this is always a caller mistake (an id collision), not something that self-heals.
			throw new ConfigError(
				`schedules.upsert: schedule "${id}" is already declared by defineTask({ schedule }) ` +
					`(source "declarative"); engine.schedules.upsert() (source "api") cannot take over ` +
					"its id. Rename this schedule, or remove the `schedule` option from that task's " +
					"defineTask() if it should be managed through the API instead."
			);
		}
		await syncScheduler(transport, logger, { id, cron, timezone });
		return toPublicSchedule(result.schedule);
	}

	async function del(id: string): Promise<boolean> {
		if (typeof id !== "string") return false;
		const deleted = await store.schedules.delete(id);
		if (deleted) {
			try {
				await transport.removeJobScheduler(id);
			} catch (error) {
				logger.warn(
					{ err: error, scheduleId: id },
					"schedules.delete: the postgres row was removed but the BullMQ job scheduler removal " +
						"failed; the next tick self-heals (schedule missing in postgres -> scheduler removed, " +
						"no run created)"
				);
			}
		}
		return deleted;
	}

	async function list(): Promise<Schedule[]> {
		const rows = await store.schedules.list();
		return rows.map(toPublicSchedule);
	}

	return { upsert, delete: del, list };
}

/** Shared by `engine.schedules.upsert`/`syncDeclarativeSchedules`: Postgres already committed. */
async function syncScheduler(
	transport: Pick<Transport, "upsertJobScheduler">,
	logger: EngineLogger,
	input: { id: string; cron: string; timezone: string | undefined }
): Promise<void> {
	try {
		await transport.upsertJobScheduler({
			id: input.id,
			pattern: input.cron,
			...(input.timezone !== undefined ? { timezone: input.timezone } : {}),
		});
	} catch (error) {
		logger.warn(
			{ err: error, scheduleId: input.id },
			"failed to sync the BullMQ job scheduler after writing the schedule to Postgres " +
				"(decision 8: Postgres is authoritative; the reconciler will repair this)"
		);
	}
}

/** True if the live BullMQ scheduler's pattern/timezone disagree with (or are missing for) Postgres. */
function schedulerDiverged(
	schedule: { cron: string; timezone: string | null },
	current: JobSchedulerInfo | undefined
): boolean {
	if (!current) return true;
	if (current.pattern !== schedule.cron) return true;
	return (current.timezone ?? undefined) !== (schedule.timezone ?? undefined);
}

// ---------------------------------------------------------------------------
// Declarative sync (`defineTask({ schedule })`, decision 21) -- engine.start()
// ---------------------------------------------------------------------------

export interface DeclarativeSyncDeps {
	store: Pick<Store, "schedules">;
	transport: Pick<Transport, "upsertJobScheduler" | "removeJobScheduler">;
	registry: Registry;
	logger: EngineLogger;
	maxPayloadBytes: number;
	/** `config.schedules.pruneDeclarative` (default false, decision 21: never delete by default). */
	pruneDeclarative: boolean;
}

/**
 * Creates/updates the Postgres row (`source: "declarative"`, id = the task's own id -- a task has
 * at most one declarative schedule) and the BullMQ scheduler for every registered task with a
 * `schedule` option. With `pruneDeclarative: true`, also removes (Postgres row + BullMQ scheduler)
 * every declarative schedule that is no longer declared by any registered task; without it, a
 * schedule removed from code is left alone (decision 21).
 *
 * One task's bad `schedule` (invalid cron/timezone/payload) is isolated (logged, that task's
 * schedule left as-is) and never stops the others or `pruneDeclarative` from running.
 */
export async function syncDeclarativeSchedules(deps: DeclarativeSyncDeps): Promise<void> {
	const { store, transport, registry, logger, maxPayloadBytes, pruneDeclarative } = deps;
	const declaredIds: string[] = [];

	for (const task of registry.tasks.values()) {
		const declared = task.options.schedule;
		if (!declared) continue;
		declaredIds.push(task.id);
		try {
			const cron = validateCronPattern(declared.cron);
			const timezone =
				declared.timezone === undefined ? undefined : validateTimezone(declared.timezone);
			const payload = (declared.payload === undefined ? null : declared.payload) as Json;
			assertSerializable(payload, {
				label: `schedule payload of task "${task.id}"`,
				maxBytes: maxPayloadBytes,
			});
			const result = await store.schedules.upsert(
				{
					id: task.id,
					taskId: task.id,
					cron,
					timezone: timezone ?? null,
					payload,
					source: "declarative",
				},
				{ requireSourceMatch: true }
			);
			if (result.conflict) {
				// Fix 4: never let the declarative sync clobber an id already owned by an
				// `engine.schedules.upsert()` (source "api") call with the same id (the task's own
				// id) -- isolated like any other bad `schedule`, below.
				throw new Error(
					`schedule id "${task.id}" (this task's own id) is already an "api" schedule ` +
						"(created via engine.schedules.upsert()); defineTask({ schedule }) cannot take " +
						"it over. Rename the api schedule, or remove the `schedule` option from this " +
						"task's defineTask()."
				);
			}
			await syncScheduler(transport, logger, { id: task.id, cron, timezone });
		} catch (error) {
			logger.error(
				{ err: error, taskId: task.id },
				"defineTask({ schedule }) sync failed for this task; its schedule was left as-is"
			);
		}
	}

	if (!pruneDeclarative) return;

	const declaredSet = new Set(declaredIds);
	const existing = await store.schedules.listDeclarative();
	const stale = existing.filter(s => !declaredSet.has(s.id));
	if (stale.length === 0) return;

	await store.schedules.deleteDeclarativeExcept(declaredIds);
	for (const s of stale) {
		try {
			await transport.removeJobScheduler(s.id);
		} catch (error) {
			logger.warn(
				{ err: error, scheduleId: s.id },
				"pruneDeclarative: the postgres row was removed but the BullMQ job scheduler removal " +
					"failed; the reconciler will remove the orphaned scheduler"
			);
		}
	}
}

// ---------------------------------------------------------------------------
// Reconciler hook (bidirectional, `reconciler.ts`'s `reconcileSchedules`)
// ---------------------------------------------------------------------------

export interface ReconcileSchedulesDeps {
	store: Pick<Store, "schedules">;
	transport: Pick<
		Transport,
		"upsertJobScheduler" | "removeJobScheduler" | "listJobSchedulers" | "getJobScheduler"
	>;
	logger: EngineLogger;
	/**
	 * This engine's own Postgres schema (fix 8): a BullMQ scheduler tagged (via
	 * {@link JobSchedulerInfo.schema}, set on the job template's `data` by `upsertJobScheduler`) with
	 * a *different* schema belongs to another engine sharing this Redis `prefix` -- it is never
	 * treated as an orphan, even if it has no matching row in THIS schema's `schedules` table.
	 * Omitted (e.g. by tests that don't care) => every untagged/foreign-looking scheduler is treated
	 * as this hook's own, matching the pre-fix behavior. `prefix` should be unique per schema
	 * (documented in `README.md`); this tag is a safety net, not a substitute for that.
	 */
	schema?: string;
	/**
	 * Review item (c), with `schema`: a scheduler tagged with another schema is removed when that
	 * schema no longer owns it (retired: schema, table or row gone), via
	 * {@link removeRetiredForeignScheduler}. Omitted => foreign-tagged schedulers are always kept.
	 */
	foreignScheduleState?: RetiredSchedulerDeps["foreignScheduleState"];
	/** Review item 3, forwarded to {@link removeRetiredForeignScheduler}. */
	isPrefixOwner?: RetiredSchedulerDeps["isPrefixOwner"];
}

/**
 * Postgres is authoritative (decision 8): a schedule row with a missing or drifted BullMQ
 * scheduler is (re)upserted; a BullMQ scheduler with no matching Postgres row (e.g. a `delete()`
 * whose BullMQ removal failed, or a schedule deleted directly in Postgres) is removed. Isolated
 * per item like the rest of the reconciler (`reconciler.ts`'s `runCase` catches whatever this
 * throws), but a bad row here only affects that one item, not the whole hook.
 *
 * Reads BullMQ schedulers FIRST, then Postgres (sequentially, never `Promise.all`), and re-checks
 * `store.schedules.get(id)` immediately before removing an apparent orphan (fix 1): a schedule
 * created concurrently, between either read and the removal, must never be deleted just because it
 * raced this hook's own two reads. See `CLAUDE.md`, Phase 8 notes, for the race this closes.
 */
export function createReconcileSchedulesHook(deps: ReconcileSchedulesDeps): () => Promise<void> {
	const { store, transport, logger, schema, foreignScheduleState: stateOf, isPrefixOwner } = deps;

	return async function reconcileSchedules(): Promise<void> {
		// Order matters (fix 1): read the BullMQ side first, so a schedule created by a concurrent
		// `upsert()` after this read (but before the Postgres read below) shows up in `schedules`
		// without ever looking orphaned below (it just isn't in `schedulerById` yet, which the first
		// loop treats as "missing scheduler", not "missing row").
		const schedulers = await transport.listJobSchedulers();
		const schedules = await store.schedules.list();
		const schedulerById = new Map(schedulers.map(s => [s.id, s]));
		const scheduleIds = new Set(schedules.map(s => s.id));

		for (const schedule of schedules) {
			if (!schedulerDiverged(schedule, schedulerById.get(schedule.id))) continue;
			try {
				await transport.upsertJobScheduler({
					id: schedule.id,
					pattern: schedule.cron,
					...(schedule.timezone ? { timezone: schedule.timezone } : {}),
				});
			} catch (error) {
				logger.warn(
					{ err: error, scheduleId: schedule.id },
					"reconciler: failed to upsert a missing/drifted BullMQ job scheduler"
				);
			}
		}

		for (const scheduler of schedulers) {
			if (scheduleIds.has(scheduler.id)) continue;
			// Fix 8: a scheduler tagged with a different schema belongs to another engine sharing
			// this Redis prefix; never delete it, no matter what THIS schema's `schedules` table says.
			if (schema !== undefined && scheduler.schema !== undefined && scheduler.schema !== schema) {
				// Review item (c): unless that schema was retired (nothing else would ever remove it).
				if (!stateOf) continue;
				try {
					await removeRetiredForeignScheduler(
						{
							transport,
							logger,
							foreignScheduleState: stateOf,
							...(isPrefixOwner ? { isPrefixOwner } : {}),
						},
						scheduler.id,
						scheduler.schema
					);
				} catch (error) {
					logger.warn(
						{ err: error, scheduleId: scheduler.id, schema: scheduler.schema },
						"reconciler: could not check whether another schema still owns this job scheduler; kept"
					);
				}
				continue;
			}
			try {
				// Re-check right before removing (fix 1): closes the window between the `list()`
				// above and this point, during which a concurrent `upsert()` may have created the row.
				if ((await store.schedules.get(scheduler.id)) !== null) continue;
				await transport.removeJobScheduler(scheduler.id);
			} catch (error) {
				logger.warn(
					{ err: error, scheduleId: scheduler.id },
					"reconciler: failed to remove an orphaned BullMQ job scheduler (no matching postgres row)"
				);
			}
		}
	};
}

// ---------------------------------------------------------------------------
// `schedule-tick` handler (`-system` worker, decisions 8, 9, 17)
// ---------------------------------------------------------------------------

/**
 * Thrown for a `schedule-tick` job whose schema tag (the job scheduler template's
 * `data.schema`, see `Transport.upsertJobScheduler`) is not this engine's schema: the tick belongs
 * to another engine sharing this Redis `prefix` (a misconfiguration, refused by `start()`'s prefix
 * ownership check). The tick must neither create a run here nor remove the scheduler (its row
 * lives in the other schema); throwing lets BullMQ retry it (`attempts: 5`), possibly on a worker
 * of the engine that owns it.
 */
export class ScheduleTickSchemaMismatchError extends ConfigError {
	override name = "ScheduleTickSchemaMismatchError";
	constructor(scheduleId: string, tickSchema: string, schema: string) {
		super(
			`schedule tick "${scheduleId}" belongs to another schema ("${tickSchema}"), not this ` +
				`engine's ("${schema}"): two schemas share one Redis prefix. Give each schema its own ` +
				"prefix; nothing was created or removed for this tick"
		);
	}
}

/**
 * Audit item 8: rejects a `schedule-tick` job tagged with another schema (see
 * {@link ScheduleTickSchemaMismatchError}). An untagged tick (scheduler created before the tag
 * existed) passes. Called by the engine before dispatching the tick.
 */
export function assertScheduleTickSchema(data: unknown, schema: string, scheduleId: string): void {
	const tag = foreignScheduleTickSchema(data, schema);
	if (tag !== undefined) throw new ScheduleTickSchemaMismatchError(scheduleId, tag, schema);
}

/** The schema tag of a `schedule-tick` job's data when it is not `schema`, else `undefined`. */
export function foreignScheduleTickSchema(data: unknown, schema: string): string | undefined {
	const tag =
		data !== null && typeof data === "object" ? (data as { schema?: unknown }).schema : undefined;
	return typeof tag === "string" && tag !== schema ? tag : undefined;
}

/**
 * Whether another schema still owns schedule `scheduleId`, checked in THIS engine's database
 * (review item c): `"orphaned"` when the schema, its `schedules` table or the row is gone (a
 * retired schema whose job schedulers were left in Redis); `"live"` otherwise. The schema name
 * comes from Redis, so it only ever reaches SQL as a bound parameter (`format('%I')`) or through
 * `escapeIdentifier`. Any other error (e.g. no `USAGE` on that schema, a table that isn't ours)
 * propagates: the caller must then treat the scheduler as live.
 */
export async function foreignScheduleState(
	pool: Pick<Pool, "query">,
	foreignSchema: string,
	scheduleId: string
): Promise<"live" | "orphaned"> {
	const { rows } = await pool.query<{ exists: boolean }>(
		"SELECT to_regclass(format('%I.schedules', $1::text)) IS NOT NULL AS exists",
		[foreignSchema]
	);
	if (rows[0]?.exists !== true) return "orphaned";
	try {
		const found = await pool.query(
			`SELECT 1 FROM ${escapeIdentifier(foreignSchema)}.schedules WHERE id = $1`,
			[scheduleId]
		);
		return (found.rowCount ?? 0) > 0 ? "live" : "orphaned";
	} catch (error) {
		// Dropped between the two queries: undefined_table / invalid_schema_name.
		const code = (error as { code?: unknown }).code;
		if (code === "42P01" || code === "3F000") return "orphaned";
		throw error;
	}
}

export type RetiredSchedulerOutcome =
	/** The foreign schema still owns the schedule: keep its scheduler. */
	| "live"
	/** Orphaned, and its scheduler was removed now (logged once at `warn`). */
	| "removed"
	/** Orphaned, but the scheduler is already gone or now carries another tag: nothing to do. */
	| "stale";

export interface RetiredSchedulerDeps {
	transport: Pick<Transport, "getJobScheduler" | "removeJobScheduler">;
	logger: EngineLogger;
	/** {@link foreignScheduleState} bound to the engine's pool. */
	foreignScheduleState: (foreignSchema: string, scheduleId: string) => Promise<"live" | "orphaned">;
	/**
	 * External review item 3: {@link foreignScheduleState} only ever sees THIS engine's database --
	 * a foreign schema living in ANOTHER database (a blue/green deploy: old engine on database A,
	 * new engine on database B, same Redis prefix) always looks "orphaned" to it, even while the
	 * old engine is still running and actively re-creating that scheduler. Bound to
	 * `GET ${prefix}:owner-schema` (the same key `claimPrefix()` writes on `start()`, no TTL, never
	 * expires on its own): `true` only when it equals THIS engine's own schema, meaning this engine
	 * currently -- and exclusively, per that key's NX write -- owns the prefix, so any foreign tag
	 * left in Redis can only be a leftover from a schema that no longer claims it. `false` (key
	 * missing, or held by yet another schema) keeps the pre-fix behavior: never remove. Omitted =>
	 * always treated as `true` (tests that don't exercise this guard).
	 */
	isPrefixOwner?: () => Promise<boolean>;
}

/**
 * Review item (c): removes the job scheduler `scheduleId` tagged with `foreignSchema` when that
 * schema no longer owns it ({@link foreignScheduleState}) AND this engine is the confirmed,
 * exclusive owner of the Redis prefix ({@link RetiredSchedulerDeps.isPrefixOwner}, review item 3).
 * Before removing, re-reads the scheduler and only removes it if it still carries `foreignSchema`'s
 * tag (a scheduler re-created by this or another live schema under the same id is never touched).
 * Errors propagate (caller: keep it).
 */
export async function removeRetiredForeignScheduler(
	deps: RetiredSchedulerDeps,
	scheduleId: string,
	foreignSchema: string
): Promise<RetiredSchedulerOutcome> {
	if (deps.isPrefixOwner !== undefined && !(await deps.isPrefixOwner())) return "live";
	if ((await deps.foreignScheduleState(foreignSchema, scheduleId)) === "live") return "live";
	const current = await deps.transport.getJobScheduler(scheduleId);
	if (current?.schema !== foreignSchema) return "stale";
	if (!(await deps.transport.removeJobScheduler(scheduleId))) return "stale";
	deps.logger.warn(
		{ scheduleId, schema: foreignSchema },
		"removed the BullMQ job scheduler of a retired schema (its schema, schedules table or row " +
			"no longer exists in this database)"
	);
	return "removed";
}

export interface ProcessScheduleTickDeps {
	store: Pick<Store, "schedules" | "runs">;
	transport: Pick<
		Transport,
		| "enqueueRun"
		| "removeSystemJob"
		| "upsertJobScheduler"
		| "removeJobScheduler"
		| "getJobScheduler"
	>;
	registry: Registry;
	logger: EngineLogger;
	/** Best-effort; publishes the created run's status like `engine.trigger` does. */
	notify?: RunNotifier;
	/**
	 * This engine's schema (audit item 8). When set, a scheduler tagged with another schema is
	 * never removed by the "no row in postgres" self-heal: the tick throws
	 * {@link ScheduleTickSchemaMismatchError} instead.
	 */
	schema?: string;
}

/**
 * One `schedule-tick`: `scheduleId` is the schedule's own id (from `job.opts.repeatJobKey`, with
 * `job.data.scheduleId` as a fallback -- both point at the same value, decision 8's spirit) and
 * `scheduledAtMs` is the tick's logical scheduled time (`job.opts.prevMillis`, the DB-independent
 * clock BullMQ itself used to compute this occurrence -- decision 17 doesn't apply to it, it is not
 * a Postgres/step timestamp, just this tick's identity).
 *
 * - Schedule missing in Postgres: remove the BullMQ scheduler, create no run (self-heal of a
 *   `delete()` whose BullMQ removal failed, or a row deleted directly). Re-checks `store.schedules.get`
 *   immediately before removing (fix 1: a concurrent `upsert()` -- api or declarative sync -- may
 *   have created this id between the first read and here; if so, the scheduler is kept and the tick
 *   falls through to the normal "present" path below instead of losing it).
 * - Present: repair the BullMQ scheduler in place if its cron/timezone drifted from Postgres, then
 *   create the run with `payload` and `task` always read fresh from Postgres (decision 8) and
 *   idempotency key `__schedule:${scheduleId}:${scheduledAtMs}`, `ttlMs: null` (decision 9's claim
 *   semantics: whichever process's tick gets there first creates the run; every other tick for the
 *   same occurrence -- redelivery, or a second process -- becomes a no-op `"existing"`).
 * - Task not registered in this process (e.g. a rolling deploy): throws instead of silently
 *   dropping the tick (fix 2/3). BullMQ's own job scheduler template gives `schedule-tick` jobs
 *   `attempts: 5` with exponential backoff specifically so this is safe: a process that DOES have
 *   the task can pick up the retry (it is the same job, redelivered -- not a new tick), and the
 *   idempotency key (`__schedule:${scheduleId}:${scheduledAtMs}`) makes a retry that lands on a
 *   process that already succeeded a no-op. The caller (`executor/system.ts#scheduleTick`) logs
 *   every failure (this one included) with `scheduleId`/`scheduledAtMs` before letting it propagate.
 */
export async function processScheduleTick(
	deps: ProcessScheduleTickDeps,
	scheduleId: string,
	scheduledAtMs: number
): Promise<void> {
	const { store, transport, registry, logger, notify } = deps;

	let schedule = await store.schedules.get(scheduleId);
	if (!schedule) {
		// Re-check right before removing (fix 1): a concurrent upsert may have created this id
		// between the read above and now.
		schedule = await store.schedules.get(scheduleId);
	}
	if (!schedule) {
		if (deps.schema !== undefined) {
			// The row may live in another schema sharing this prefix: never remove its scheduler.
			const scheduler = await transport.getJobScheduler(scheduleId);
			if (scheduler?.schema !== undefined && scheduler.schema !== deps.schema) {
				throw new ScheduleTickSchemaMismatchError(scheduleId, scheduler.schema, deps.schema);
			}
		}
		try {
			await transport.removeJobScheduler(scheduleId);
		} catch (error) {
			logger.warn(
				{ err: error, scheduleId },
				"schedule tick: the schedule no longer exists in postgres but removing the BullMQ " +
					"job scheduler failed; a later tick or the reconciler will retry"
			);
		}
		logger.debug({ scheduleId }, "schedule tick: no matching schedule in postgres; no run created");
		return;
	}

	try {
		const current = await transport.getJobScheduler(scheduleId);
		if (schedulerDiverged(schedule, current)) {
			await transport.upsertJobScheduler({
				id: scheduleId,
				pattern: schedule.cron,
				...(schedule.timezone ? { timezone: schedule.timezone } : {}),
			});
		}
	} catch (error) {
		logger.warn(
			{ err: error, scheduleId },
			"schedule tick: failed to verify/repair the BullMQ job scheduler; the reconciler will retry"
		);
	}

	// Fix 2/3: a process that doesn't have `schedule.taskId` registered (e.g. mid rolling-deploy)
	// must not silently drop this tick -- `getTask` throwing `TaskNotRegisteredError` here propagates
	// out of `processScheduleTick`, failing the BullMQ job so its `attempts: 5` exponential backoff
	// retries it (same job, same `scheduledAtMs`/idempotency key); a different process (or this one,
	// once the deploy finishes) may then pick it up. `executor/system.ts#scheduleTick` logs this
	// (and every other failure) with `scheduleId`/`scheduledAtMs` before rethrowing.
	const task: AnyTask = getTask(registry, schedule.taskId);

	const result = await store.runs.create({
		id: randomUUID(),
		taskId: task.id,
		queue: taskQueueName(task),
		payload: schedule.payload,
		maxAttempts: childTaskSpec(task).maxAttempts,
		delayMs: 0,
		idempotency: { key: `__schedule:${scheduleId}:${scheduledAtMs}`, ttlMs: null },
	});
	// Decision 9: only a newly created run is enqueued; a duplicate tick (redelivery, or another
	// process racing the same occurrence) returns the run that already claimed the key, no-op.
	if (result.kind !== "created") return;

	notify?.status({
		runId: result.run.id,
		taskId: result.run.taskId,
		status: result.run.status,
		attempt: result.run.attempt,
		error: result.run.error,
		updatedAt: result.run.updatedAt,
	});
	await enqueue({ transport, config: { logger } }, result.enqueue);
}
