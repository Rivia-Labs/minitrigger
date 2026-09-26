import { MinitriggerError } from "../../errors.js";
import type {
	AcquireWithLimitResult,
	CanceledRun,
	CancelRunResult,
	ChildResult,
	CreateRunResult,
	EnqueueRequest,
	Json,
	NewRun,
	NewRunDebounce,
	Ownership,
	RecheckConcurrencyLimitResult,
	RunStatus,
	RunStore,
	SerializedError,
	StaleCursor,
	StalePage,
	StepKind,
	TerminalRunStatus,
	TriggerChildrenInput,
	TriggerChildrenResult,
	WaitpointResolution,
} from "../types.js";
import {
	concurrencyLimitedDelayMs,
	MAX_BATCH_LIMIT,
	MAX_LIST_LIMIT,
	retryIdempotencyKey,
} from "../types.js";
import { atomic, type Db, direct, type SqlContext, type SqlExecutor } from "./db.js";
import {
	applyResumeRule,
	blockingWaitpointExistsSql,
	cancelDescendants,
	canceledError,
	DELAY_MS_SQL,
	ENQUEUE_RETURNING,
	type EnqueueRow,
	isTerminal,
	jsonParam,
	lockParentOf,
	mapEnqueue,
	mapRun,
	markCanceled,
	nowPlusMs,
	RUN_COLUMNS,
	type RunRow,
	resolveChildWaitpoint,
	TERMINAL_SQL,
	timeOutWaitpointsOf,
} from "./resolve.js";

/**
 * `created_at` is written with millisecond precision so that a {@link RunCursor} built from a JS
 * `Date` (ms precision) round-trips exactly; with microseconds, rows sharing the cursor's
 * millisecond would be skipped by the `(created_at, id) < ($c, $id)` comparison.
 */
const CREATED_AT_SQL = "date_trunc('milliseconds', now())";

/** `steps.kind` of the step written by `triggerChildren`, per mode. */
const STEP_KIND_BY_MODE: Record<TriggerChildrenInput["mode"], StepKind> = {
	trigger: "trigger",
	wait: "trigger_wait",
	batch: "batch_wait",
};

/** Ownership predicate for writes to the run row itself (decision 5). */
const OWNED = "id = $1 AND execution_id = $2 AND status = 'running'";

type CreateOnce = CreateRunResult | "retry";

/** How many times a debounced create re-runs after losing the insert race (decision 24). */
const DEBOUNCE_CREATE_ATTEMPTS = 5;

function enqueueOfRow(row: RunRow & { delay_ms: number }): EnqueueRequest {
	return {
		runId: row.id,
		taskId: row.task_id,
		queue: row.queue,
		seq: row.enqueue_seq,
		delayMs: row.delay_ms,
		status: row.status,
		updatedAt: row.updated_at,
	};
}

export function createRunStore(exec: SqlExecutor, ctx: SqlContext): RunStore {
	const runs = ctx.t("runs");

	/**
	 * One create attempt, a single autocommit-safe statement (no BEGIN, no SAVEPOINT): the caller's
	 * transaction (decision 26) never gets a subtransaction per trigger, so hundreds of keyed
	 * triggers in one transaction cannot overflow the backend's subxid cache (suboverflow makes
	 * every snapshot on the cluster, replicas included, fall back to pg_subtrans lookups).
	 *
	 * With an idempotency key, decision 9's claim (`ON CONFLICT ... WHERE expires_at < now()
	 * RETURNING`, never `xmax`) runs in a CTE and the run row is inserted only `WHERE EXISTS` a
	 * claimed row. The key row references the run inserted by the sibling CTE: both CTEs share one
	 * snapshot, but the (non-deferrable) FK check is an after-trigger fired at the end of the
	 * statement, when the run row exists (verified on Postgres 16). A live key inserts nothing (and
	 * queues no NOTIFY: `pg_notify` is in the run insert's RETURNING); its owner is then read by a
	 * second statement, which sees a concurrent claimer that committed meanwhile.
	 */
	async function createOnce(db: Db, input: NewRun): Promise<CreateOnce> {
		const idem = input.idempotency;
		// Decision 26: with `outbox`, the row is flagged for the post-commit claim (poll sweep) and,
		// unless `outbox.notify === false` (config `outbox.notify`), the same statement also queues a
		// NOTIFY (delivered only on commit; the payload is constant, so Postgres collapses every
		// trigger of one transaction into a single notification). `notify: false` skips the NOTIFY
		// (and the commit-time lock it takes) -- dispatch then relies entirely on the poll sweep.
		const outbox = input.outbox;
		const sendNotify = outbox !== undefined && outbox.notify !== false;
		const params: unknown[] = [
			input.id,
			input.taskId,
			input.queue,
			jsonParam(input.payload),
			input.maxAttempts,
			input.delayMs,
			input.concurrencyKey ?? null,
			outbox !== undefined,
		];
		// `::text`: pg_notify returns `void`, which some ORM raw-query paths cannot deserialize
		// (Prisma: "Failed to deserialize column of type 'void'"); the column is never read.
		const notify = sendNotify
			? `, pg_notify($${params.push(outbox.channel)}::text, '')::text AS notified`
			: "";
		const insertRun = (source: string) =>
			`INSERT INTO ${runs} (id, task_id, queue, payload, status, attempt, max_attempts,
			   enqueue_seq, run_after, created_at, updated_at, concurrency_key, outbox_pending)
			 ${source}
			 RETURNING ${RUN_COLUMNS}, ${DELAY_MS_SQL} AS delay_ms${notify}`;
		const values = `$1::uuid, $2::text, $3::text, $4::jsonb, 'queued', 1, $5::int, 1,
			   ${nowPlusMs("$6")}, ${CREATED_AT_SQL}, now(), $7::text, $8::boolean`;
		let sql: string;
		if (idem) {
			const key = params.push(idem.key);
			const ttl = params.push(idem.ttlMs);
			// Explicit casts on every parameter: an ORM adapter's raw-query path (decision 26, e.g.
			// Prisma's `$queryRawUnsafe`) can send all bind values with a declared type of `text`
			// instead of leaving them untyped for Postgres to infer from context. Postgres has no
			// implicit/assignment cast from `text` to `uuid` (unlike an untyped literal), so `$1`
			// (run_id) landing in a `uuid` column without `::uuid` fails only through such an
			// adapter -- node-postgres itself sends parameters untyped and never hits this.
			sql = `WITH claim AS (
			     INSERT INTO ${ctx.t("idempotency_keys")} AS k (task_id, key, run_id, expires_at)
			     VALUES ($2::text, $${key}::text, $1::uuid,
			       CASE WHEN $${ttl}::float8 IS NULL THEN NULL ELSE ${nowPlusMs(`$${ttl}`)} END)
			     ON CONFLICT (task_id, key) DO UPDATE
			       SET run_id = EXCLUDED.run_id, expires_at = EXCLUDED.expires_at
			       WHERE k.expires_at < now()
			     RETURNING run_id
			   ), created AS (
			     ${insertRun(`SELECT ${values} WHERE EXISTS (SELECT 1 FROM claim)`)}
			   )
			   SELECT * FROM created`;
		} else {
			sql = insertRun(`VALUES (${values})`);
		}
		const { rows } = await db.query<RunRow & { delay_ms: number }>(sql, params);
		const row = rows[0];
		if (row) return { kind: "created", run: mapRun(row), enqueue: enqueueOfRow(row) };
		if (!idem) throw new Error("runs insert returned no row");
		// Live key: its owner. No row: it vanished since (retention deleted its run); retry.
		const { rows: owners } = await db.query<RunRow>(
			`SELECT ${RUN_COLUMNS} FROM ${runs}
			 WHERE id = (SELECT run_id FROM ${ctx.t("idempotency_keys")}
			             WHERE task_id = $1::text AND key = $2::text)`,
			[input.taskId, idem.key]
		);
		const owner = owners[0];
		return owner ? { kind: "existing", run: mapRun(owner) } : "retry";
	}

	/**
	 * Decision 24, one attempt inside the caller's transaction: collapse into the pending run of
	 * the key, or insert a new one. A unique violation of `idx_runs_debounce` (a concurrent
	 * trigger inserted first and committed) rolls back to the savepoint and reports `"retry"`: the
	 * next attempt's `FOR UPDATE` finds (and waits for) that run.
	 */
	async function createDebouncedOnce(
		db: Db,
		input: NewRun,
		debounce: NewRunDebounce
	): Promise<CreateOnce> {
		const { rows: found } = await db.query<{ id: string; enqueue_seq: number }>(
			`SELECT id, enqueue_seq FROM ${runs}
			 WHERE task_id = $1 AND debounce_key = $2 AND status = 'queued'
			 FOR UPDATE`,
			[input.taskId, debounce.key]
		);
		const existing = found[0];
		if (existing) {
			// Audit item 1: `enqueue_seq` (and thus `updated_at`'s worth of rescheduling) only
			// advances when the recomputed `run_after` actually differs from the locked row's
			// current one -- every `SET` expression here reads the row as it was BEFORE this
			// UPDATE, so `run_after` on the right of `IS DISTINCT FROM` is still the old value even
			// though the same statement also assigns it a new one. A collapse that lands on the
			// same `run_after` (e.g. already pinned at `debounce_deadline`) has nothing to
			// reschedule: bumping the seq then would needlessly fence the job already scheduled for
			// that unchanged `run_after` (it fences on the seq, not `run_after`).
			// Review item 4: never move run_after earlier than its current value while a
			// concurrency-limited attempt is backing off (`concurrency_waits > 0`) -- a `limited`
			// acquisition already pushed run_after into the future to space out retries; a debounce
			// collapse that pulled it back in would let the very next acquisition attempt through
			// before the backoff it's owed, skipping it entirely. `greatest` ignores NULL, so the
			// `CASE` below is a no-op (falls back to the plain `least(...)` collapse) whenever
			// `concurrency_waits = 0`.
			const { rows } = await db.query<RunRow & { delay_ms: number }>(
				`UPDATE ${runs}
				 SET run_after = greatest(
				       least(${nowPlusMs("$2")}, debounce_deadline),
				       CASE WHEN concurrency_waits > 0 THEN run_after END
				     ),
				     payload = CASE WHEN $3 THEN $4::jsonb ELSE payload END,
				     enqueue_seq = CASE
				       WHEN greatest(
				              least(${nowPlusMs("$2")}, debounce_deadline),
				              CASE WHEN concurrency_waits > 0 THEN run_after END
				            ) IS DISTINCT FROM run_after
				       THEN enqueue_seq + 1
				       ELSE enqueue_seq
				     END,
				     updated_at = now()
				 WHERE id = $1
				 RETURNING ${RUN_COLUMNS}, ${DELAY_MS_SQL} AS delay_ms`,
				[existing.id, debounce.delayMs, debounce.payload === "last", jsonParam(input.payload)]
			);
			const row = rows[0];
			if (!row) throw new Error("debounce: locked run disappeared");
			if (row.enqueue_seq === existing.enqueue_seq) {
				return { kind: "debounced", run: mapRun(row), enqueue: null };
			}
			return {
				kind: "debounced",
				run: mapRun(row),
				enqueue: enqueueOfRow(row),
				staleSeq: existing.enqueue_seq,
			};
		}
		await db.query("SAVEPOINT mt_debounce_run");
		try {
			const { rows } = await db.query<RunRow & { delay_ms: number }>(
				`INSERT INTO ${runs} (id, task_id, queue, payload, status, attempt, max_attempts,
				   enqueue_seq, run_after, created_at, updated_at, concurrency_key, debounce_key,
				   debounce_deadline)
				 VALUES ($1, $2, $3, $4::jsonb, 'queued', 1, $5, 1,
				   least(${nowPlusMs("$6")}, ${nowPlusMs("$9")}),
				   ${CREATED_AT_SQL}, now(), $7, $8, ${nowPlusMs("$9")})
				 RETURNING ${RUN_COLUMNS}, ${DELAY_MS_SQL} AS delay_ms`,
				[
					input.id,
					input.taskId,
					input.queue,
					jsonParam(input.payload),
					input.maxAttempts,
					debounce.delayMs,
					input.concurrencyKey ?? null,
					debounce.key,
					debounce.maxDelayMs,
				]
			);
			await db.query("RELEASE SAVEPOINT mt_debounce_run");
			const row = rows[0];
			if (!row) throw new Error("runs insert returned no row");
			return { kind: "created", run: mapRun(row), enqueue: enqueueOfRow(row) };
		} catch (error) {
			if ((error as { constraint?: string }).constraint !== "idx_runs_debounce") throw error;
			await db.query("ROLLBACK TO SAVEPOINT mt_debounce_run");
			await db.query("RELEASE SAVEPOINT mt_debounce_run");
			return "retry";
		}
	}

	/**
	 * Decision 5's acquisition statement (+ decision 24: the debounce key is released; decision 25:
	 * `concurrency_waits` resets to 0).
	 */
	async function acquireRow(db: Db, runId: string, executionId: string, seq: number) {
		const { rows } = await db.query<RunRow>(
			`UPDATE ${runs}
			 SET status = 'running', execution_id = $2, heartbeat_at = now(),
			     started_at = coalesce(started_at, now()), updated_at = now(), debounce_key = NULL,
			     concurrency_waits = 0
			 WHERE id = $1 AND status IN ('queued', 'waiting') AND enqueue_seq = $3
			 RETURNING ${RUN_COLUMNS}`,
			[runId, executionId, seq]
		);
		const row = rows[0];
		return row ? mapRun(row) : null;
	}

	async function getRun(db: Db, runId: string) {
		const { rows } = await db.query<RunRow>(`SELECT ${RUN_COLUMNS} FROM ${runs} WHERE id = $1`, [
			runId,
		]);
		const row = rows[0];
		return row ? mapRun(row) : null;
	}

	/** complete/fail: lock parent, guarded terminal update, child resolution. */
	async function finish(
		owner: Ownership,
		status: "completed" | "failed",
		output: Json | null,
		error: SerializedError | null
	): Promise<
		| { lost: true }
		| {
				lost: false;
				updatedAt: Date;
				parent: WaitpointResolution | null;
				expireJobsToRemove: string[];
		  }
	> {
		return atomic(exec, async db => {
			const parent = await lockParentOf(db, ctx, owner.runId);
			if (!parent) return { lost: true };
			const { rows } = await db.query<{
				parent_waitpoint_id: string | null;
				updated_at: Date;
			}>(
				`UPDATE ${runs}
				 SET status = $3, output = $4::jsonb, error = $5::jsonb, finished_at = now(),
				     run_after = NULL, execution_id = NULL, updated_at = now()
				 WHERE ${OWNED}
				 RETURNING parent_waitpoint_id, updated_at`,
				[
					owner.runId,
					owner.executionId,
					status,
					status === "completed" ? jsonParam(output) : null,
					error === null ? null : jsonParam(error),
				]
			);
			const row = rows[0];
			if (!row) return { lost: true };
			// Waitpoint rows last (lock order): the run's own pending waitpoints can never resolve
			// anything any more (mirror of cancel), then the parent's waitpoint.
			const expireJobsToRemove = await timeOutWaitpointsOf(db, ctx, [owner.runId]);
			const wp = row.parent_waitpoint_id;
			return {
				lost: false,
				updatedAt: row.updated_at,
				parent: wp ? await resolveChildWaitpoint(db, ctx, wp) : null,
				expireJobsToRemove,
			};
		});
	}

	/**
	 * Reconciler cases 1/2 (audit item 1): keyset page `ORDER BY run_after, id` after `after`.
	 * The cursor's `run_after` travels as UTC ISO text with microseconds (a JS `Date` would drop
	 * them and make the page loop or skip rows sharing a millisecond); the ISO form with `Z` parses
	 * back exactly whatever the session's DateStyle/TimeZone.
	 */
	async function listStale(
		predicate: string,
		graceMs: number,
		limit: number,
		after: StaleCursor | null,
		excludeQueues: readonly string[] = []
	): Promise<StalePage> {
		const params: unknown[] = [graceMs, limit];
		let keyset = "";
		if (after) {
			params.push(after.runAfter, after.id);
			keyset = `AND (r.run_after, r.id) > ($3::timestamptz, $4::uuid)`;
		}
		let excludeClause = "";
		if (excludeQueues.length > 0) {
			params.push(excludeQueues);
			excludeClause = `AND NOT (r.queue = ANY($${params.length}::text[]))`;
		}
		const { rows } = await direct(exec).query<EnqueueRow & { cursor_run_after: string }>(
			`SELECT r.id, r.task_id, r.queue, r.enqueue_seq, r.status, r.updated_at,
			   0::float8 AS delay_ms,
			   to_char(r.run_after AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
			     AS cursor_run_after
			 FROM ${runs} r
			 WHERE ${predicate}
			   AND r.run_after < now() - $1::float8 * interval '1 millisecond'
			   ${keyset}
			   ${excludeClause}
			 ORDER BY r.run_after, r.id LIMIT $2`,
			params
		);
		const last = rows[rows.length - 1];
		return {
			items: rows.map(mapEnqueue),
			nextCursor:
				rows.length >= limit && last ? { runAfter: last.cursor_run_after, id: last.id } : null,
		};
	}

	return {
		async create(input) {
			const debounce = input.debounce;
			if (debounce && input.outbox) {
				throw new Error("runs.create: debounce and outbox are mutually exclusive");
			}
			if (debounce) {
				if (input.idempotency) {
					throw new Error("runs.create: debounce and idempotency are mutually exclusive");
				}
				for (let i = 0; i < DEBOUNCE_CREATE_ATTEMPTS; i++) {
					const result = await atomic(exec, db => createDebouncedOnce(db, input, debounce));
					if (result !== "retry") return result;
				}
				throw new MinitriggerError(
					`trigger({ debounce: { key: ${JSON.stringify(debounce.key)} } }) of task ` +
						`"${input.taskId}" lost the insert race ${DEBOUNCE_CREATE_ATTEMPTS} times in a row: ` +
						"another process keeps winning the pending run of this key under very heavy " +
						"concurrent triggering. Retry the trigger() call, or trigger less frequently for " +
						"this key."
				);
			}
			for (let i = 0; i < 2; i++) {
				const result = await createOnce(direct(exec), input);
				if (result !== "retry") return result;
			}
			throw new Error(`run owning idempotency key of task ${input.taskId} kept vanishing`);
		},

		async claimOutbox(limit) {
			const { rows } = await direct(exec).query<
				EnqueueRow & { attempt: number; error: SerializedError | null }
			>(
				`WITH picked AS (
				   SELECT id FROM ${runs} WHERE outbox_pending
				   ORDER BY created_at LIMIT $1
				   FOR UPDATE SKIP LOCKED
				 )
				 UPDATE ${runs} r SET outbox_pending = false
				 FROM picked WHERE r.id = picked.id
				 RETURNING r.id, r.task_id, r.queue, r.enqueue_seq, r.status, r.updated_at, r.attempt,
				   r.error, greatest(0, round(extract(epoch from (r.run_after - now())) * 1000))::float8
				     AS delay_ms`,
				[limit]
			);
			return {
				claimed: rows.length,
				enqueue: rows
					.filter(row => row.status === "queued")
					.map(row => ({ ...mapEnqueue(row), attempt: row.attempt, error: row.error })),
			};
		},

		triggerChildren(input: TriggerChildrenInput): Promise<TriggerChildrenResult> {
			const n = input.children.length;
			if (input.mode === "batch" ? n < 1 : n !== 1) {
				throw new Error(`triggerChildren mode ${input.mode} got ${n} children`);
			}
			return atomic(exec, async db => {
				const guard = await db.query(`SELECT 1 FROM ${runs} WHERE ${OWNED} FOR SHARE`, [
					input.parent.runId,
					input.parent.executionId,
				]);
				if (guard.rowCount === 0) return { kind: "lost" };

				let waitpoint: {
					id: string;
					expiresAt: Date | null;
					expireDelayMs: number | null;
				} | null = null;
				if (input.mode !== "trigger") {
					const { rows } = await db.query<{ expires_at: Date | null; delay_ms: number | null }>(
						`INSERT INTO ${ctx.t("waitpoints")} (id, run_id, step_name, kind, status,
						   pending_count, expires_at)
						 VALUES ($1, $2, $3, $4, 'pending', $5,
						   CASE WHEN $6::float8 IS NULL THEN NULL ELSE ${nowPlusMs("$6")} END)
						 RETURNING expires_at,
						   CASE WHEN expires_at IS NULL THEN NULL
						     ELSE greatest(0, round(extract(epoch from (expires_at - now())) * 1000))
						   END::float8 AS delay_ms`,
						[
							input.waitpointId,
							input.parent.runId,
							input.stepName,
							input.mode === "batch" ? "batch" : "child_run",
							input.mode === "batch" ? n : null,
							input.timeoutMs,
						]
					);
					const row = rows[0];
					waitpoint = {
						id: input.waitpointId,
						expiresAt: row?.expires_at ?? null,
						expireDelayMs: row?.delay_ms ?? null,
					};
				}
				const parentWp = waitpoint ? waitpoint.id : null;

				const { rows: inserted } = await db.query<EnqueueRow>(
					`INSERT INTO ${runs} (id, task_id, queue, payload, status, attempt, max_attempts,
					   enqueue_seq, run_after, parent_run_id, parent_waitpoint_id, parent_step_name,
					   batch_index, created_at, updated_at)
					 SELECT c.id, c.task_id, c.queue, c.payload, 'queued', 1, c.max_attempts, 1,
					   ${nowPlusMs("$10")}, $6::uuid, $7::uuid, $8, c.batch_index, ${CREATED_AT_SQL}, now()
					 FROM unnest($1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::int[], $9::int[])
					   AS c(id, task_id, queue, payload, max_attempts, batch_index)
					 RETURNING ${ENQUEUE_RETURNING}`,
					[
						input.children.map(c => c.id),
						input.children.map(c => c.taskId),
						input.children.map(c => c.queue),
						input.children.map(c => jsonParam(c.payload)),
						input.children.map(c => c.maxAttempts),
						input.parent.runId,
						parentWp,
						input.stepName,
						input.children.map(c => c.batchIndex),
						input.delayMs ?? 0,
					]
				);
				const enqueueById = new Map(inserted.map(r => [r.id, mapEnqueue(r)]));
				// Internal keys never expire; any conflict is an invariant violation -> throws.
				await db.query(
					`INSERT INTO ${ctx.t("idempotency_keys")} (task_id, key, run_id, expires_at)
					 SELECT * FROM unnest($1::text[], $2::text[], $3::uuid[], $4::timestamptz[])`,
					[
						input.children.map(c => c.taskId),
						input.children.map(c => c.idempotencyKey),
						input.children.map(c => c.id),
						input.children.map(() => null),
					]
				);
				const first = input.children[0];
				await db.query(
					`INSERT INTO ${ctx.t("steps")} (run_id, name, seq_index, kind, status, output)
					 VALUES ($1, $2, $3, $6, $4, $5::jsonb)`,
					[
						input.parent.runId,
						input.stepName,
						input.seqIndex,
						input.mode === "trigger" ? "completed" : "waiting",
						input.mode === "trigger"
							? jsonParam({ id: first?.id ?? null })
							: jsonParam({ waitpointId: input.waitpointId }),
						STEP_KIND_BY_MODE[input.mode],
					]
				);
				const enqueue: EnqueueRequest[] = input.children.map(c => {
					const request = enqueueById.get(c.id);
					if (!request) throw new Error(`triggerChildren: child ${c.id} was not inserted`);
					return request;
				});
				return { kind: "created", enqueue, waitpoint };
			});
		},

		acquire(runId, executionId, seq) {
			return acquireRow(direct(exec), runId, executionId, seq);
		},

		acquireWithConcurrencyLimit(runId, executionId, seq, options) {
			return atomic(exec, async (db): Promise<AcquireWithLimitResult> => {
				const { rows: locked } = await db.query<{
					queue: string;
					concurrency_key: string | null;
					concurrency_waits: number;
				}>(
					`SELECT queue, concurrency_key, concurrency_waits FROM ${runs}
					 WHERE id = $1 AND status IN ('queued', 'waiting') AND enqueue_seq = $2
					 FOR UPDATE`,
					[runId, seq]
				);
				const target = locked[0];
				if (!target) return { kind: "stale" };
				if (target.concurrency_key !== null) {
					// Serializes every acquirer of this (schema, queue, key): the count below then sees
					// every `running` row committed by the previous holder (READ COMMITTED takes a new
					// snapshot per statement). Single-bigint key space, apart from decision 16's
					// two-int `(hashtext(schema), 1|2)` locks.
					await db.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
						`${ctx.schema}:${target.queue}:${target.concurrency_key}`,
					]);
					const { rows: counted } = await db.query<{ n: number }>(
						`SELECT count(*)::int AS n FROM ${runs}
						 WHERE queue = $1 AND concurrency_key = $2 AND status = 'running'`,
						[target.queue, target.concurrency_key]
					);
					if ((counted[0]?.n ?? 0) >= options.limit) {
						// Decision 25: growing backoff per consecutive `limited` result of this run,
						// computed from the PRE-increment `concurrency_waits` (time itself from the DB).
						const delayMs = concurrencyLimitedDelayMs(target.concurrency_waits);
						const { rows } = await db.query<EnqueueRow>(
							`UPDATE ${runs}
							 SET enqueue_seq = enqueue_seq + 1, run_after = ${nowPlusMs("$2")},
							     updated_at = now(), concurrency_waits = concurrency_waits + 1
							 WHERE id = $1
							 RETURNING ${ENQUEUE_RETURNING}`,
							[runId, delayMs]
						);
						const row = rows[0];
						if (!row) throw new Error("acquireWithConcurrencyLimit: locked run disappeared");
						return { kind: "limited", enqueue: mapEnqueue(row) };
					}
				}
				const run = await acquireRow(db, runId, executionId, seq);
				if (!run) throw new Error("acquireWithConcurrencyLimit: locked run was not acquired");
				return { kind: "acquired", run };
			});
		},

		recheckConcurrencyLimit(runId, executionId, options) {
			return atomic(exec, async (db): Promise<RecheckConcurrencyLimitResult> => {
				const { rows: locked } = await db.query<{
					queue: string;
					concurrency_key: string | null;
					concurrency_waits: number;
				}>(
					`SELECT queue, concurrency_key, concurrency_waits FROM ${runs}
					 WHERE ${OWNED} FOR UPDATE`,
					[runId, executionId]
				);
				const target = locked[0];
				if (!target) return { kind: "lost" };
				if (options.limit !== undefined && target.concurrency_key !== null) {
					// Same lock key as acquireWithConcurrencyLimit: claim re-checks and BullMQ-style
					// acquisitions of one key serialize against each other.
					await db.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
						`${ctx.schema}:${target.queue}:${target.concurrency_key}`,
					]);
					const { rows: counted } = await db.query<{ n: number }>(
						`SELECT count(*)::int AS n FROM ${runs}
						 WHERE queue = $1 AND concurrency_key = $2 AND status = 'running' AND id <> $3`,
						[target.queue, target.concurrency_key, runId]
					);
					if ((counted[0]?.n ?? 0) >= options.limit) {
						// Backoff from the PRE-increment waits; the claim never reset them, so it keeps
						// growing across claims (time from the DB, decision 17).
						const delayMs = concurrencyLimitedDelayMs(target.concurrency_waits);
						await db.query(
							`UPDATE ${runs}
							 SET status = 'queued', execution_id = NULL, run_after = ${nowPlusMs("$2")},
							     concurrency_waits = concurrency_waits + 1, updated_at = now()
							 WHERE id = $1`,
							[runId, delayMs]
						);
						return { kind: "limited" };
					}
				}
				const { rows } = await db.query<RunRow>(
					`UPDATE ${runs}
					 SET concurrency_waits = 0, started_at = coalesce(started_at, now())
					 WHERE id = $1
					 RETURNING ${RUN_COLUMNS}`,
					[runId]
				);
				const row = rows[0];
				if (!row) throw new Error("recheckConcurrencyLimit: locked run disappeared");
				return { kind: "ok", run: mapRun(row) };
			});
		},

		async heartbeat(runId, executionId, timeoutMs) {
			if (exec.kind === "tx") {
				throw new Error("heartbeat must not run inside a StoreTx (SET LOCAL would leak)");
			}
			const ms = Math.max(1, Math.floor(timeoutMs));
			if (!Number.isSafeInteger(ms)) throw new Error(`invalid heartbeat timeout: ${timeoutMs}`);
			return atomic(exec, async db => {
				// SET cannot take bind parameters; `ms` is a validated integer.
				await db.query(`SET LOCAL statement_timeout = ${ms}`);
				const { rowCount } = await db.query(
					`UPDATE ${runs} SET heartbeat_at = now() WHERE ${OWNED}`,
					[runId, executionId]
				);
				return rowCount === 0 ? "lost" : "ok";
			});
		},

		async complete(owner, output) {
			const r = await finish(owner, "completed", output, null);
			return r.lost
				? { kind: "lost" }
				: {
						kind: "completed",
						updatedAt: r.updatedAt,
						parent: r.parent,
						expireJobsToRemove: r.expireJobsToRemove,
					};
		},

		async fail(owner, error) {
			const r = await finish(owner, "failed", null, error);
			return r.lost
				? { kind: "lost" }
				: {
						kind: "failed",
						updatedAt: r.updatedAt,
						parent: r.parent,
						expireJobsToRemove: r.expireJobsToRemove,
					};
		},

		async retry(owner, error, backoffMs) {
			const { rows } = await direct(exec).query<EnqueueRow & { attempt: number }>(
				`UPDATE ${runs}
				 SET status = 'queued', attempt = attempt + 1, error = $3::jsonb, execution_id = NULL,
				     run_after = ${nowPlusMs("$4")}, enqueue_seq = enqueue_seq + 1, updated_at = now()
				 WHERE ${OWNED}
				 RETURNING attempt, ${ENQUEUE_RETURNING}`,
				[owner.runId, owner.executionId, jsonParam(error), backoffMs]
			);
			const row = rows[0];
			if (!row) return { kind: "lost" };
			return { kind: "retrying", attempt: row.attempt, enqueue: mapEnqueue(row) };
		},

		async release(owner) {
			const { rows } = await direct(exec).query<EnqueueRow>(
				`UPDATE ${runs}
				 SET status = 'queued', execution_id = NULL, run_after = now(),
				     enqueue_seq = enqueue_seq + 1, updated_at = now()
				 WHERE ${OWNED}
				 RETURNING ${ENQUEUE_RETURNING}`,
				[owner.runId, owner.executionId]
			);
			const row = rows[0];
			return row ? { kind: "released", enqueue: mapEnqueue(row) } : { kind: "lost" };
		},

		async setProgress(runId, executionId, value, timeoutMs) {
			if (exec.kind === "tx") {
				throw new Error("setProgress must not run inside a StoreTx (SET LOCAL would leak)");
			}
			const ms = Math.max(1, Math.floor(timeoutMs));
			if (!Number.isSafeInteger(ms)) throw new Error(`invalid setProgress timeout: ${timeoutMs}`);
			return atomic(exec, async db => {
				// SET cannot take bind parameters; `ms` is a validated integer.
				await db.query(`SET LOCAL statement_timeout = ${ms}`);
				const { rows } = await db.query<{ updated_at: Date }>(
					`UPDATE ${runs} SET progress = $3::jsonb, updated_at = now() WHERE ${OWNED}
					 RETURNING updated_at`,
					[runId, executionId, jsonParam(value)]
				);
				const row = rows[0];
				return row ? { kind: "ok", updatedAt: row.updated_at } : { kind: "lost" };
			});
		},

		suspend(owner) {
			return atomic(exec, async db => {
				const locked = await db.query(`SELECT 1 FROM ${runs} WHERE ${OWNED} FOR UPDATE`, [
					owner.runId,
					owner.executionId,
				]);
				if (locked.rowCount === 0) return { kind: "lost" };
				const { rows: state } = await db.query<{ has_pending: boolean; sleeping: boolean }>(
					`SELECT
					   ${blockingWaitpointExistsSql(ctx, "$1")} AS has_pending,
					   coalesce((SELECT max(wake_at) FROM ${ctx.t("steps")}
					             WHERE run_id = $1 AND status = 'waiting' AND wake_at IS NOT NULL)
					            > now(), false) AS sleeping`,
					[owner.runId]
				);
				const s = state[0];
				if (s?.has_pending) {
					// No enqueue, but still bump enqueue_seq so a stale redelivery of the job that
					// started this execution (`${id}-${oldSeq}`) cannot re-acquire the waiting run
					// (decision 5). The resolver's resume rule bumps it again before enqueuing.
					const { rows: upd } = await db.query<{ updated_at: Date }>(
						`UPDATE ${runs} SET status = 'waiting', run_after = NULL, execution_id = NULL,
						   enqueue_seq = enqueue_seq + 1, updated_at = now() WHERE id = $1
						 RETURNING updated_at`,
						[owner.runId]
					);
					const updatedAt = upd[0]?.updated_at;
					if (!updatedAt) throw new Error("suspend: locked run disappeared");
					return { kind: "waiting_on_waitpoints", status: "waiting", updatedAt };
				}
				const sleeping = s?.sleeping === true;
				const { rows } = await db.query<EnqueueRow>(
					`UPDATE ${runs} r
					 SET status = $2, execution_id = NULL, enqueue_seq = r.enqueue_seq + 1,
					     run_after = CASE WHEN $2 = 'waiting'
					       THEN (SELECT max(s.wake_at) FROM ${ctx.t("steps")} s
					             WHERE s.run_id = r.id AND s.status = 'waiting' AND s.wake_at IS NOT NULL)
					       ELSE now() END,
					     updated_at = now()
					 WHERE r.id = $1
					 RETURNING ${ENQUEUE_RETURNING}`,
					[owner.runId, sleeping ? "waiting" : "queued"]
				);
				const row = rows[0];
				if (!row) throw new Error("suspend: locked run disappeared");
				const enqueue = mapEnqueue(row);
				return sleeping ? { kind: "sleeping", enqueue } : { kind: "requeued", enqueue };
			});
		},

		cancel(runId, reason): Promise<CancelRunResult> {
			return atomic(exec, async db => {
				const parent = await lockParentOf(db, ctx, runId);
				if (!parent) return { kind: "not_found" };
				const { rows } = await db.query<{
					task_id: string;
					status: RunStatus;
					queue: string;
					enqueue_seq: number;
					attempt: number;
					parent_waitpoint_id: string | null;
				}>(
					`SELECT task_id, status, queue, enqueue_seq, attempt, parent_waitpoint_id FROM ${runs}
					 WHERE id = $1 FOR UPDATE`,
					[runId]
				);
				const root = rows[0];
				if (!root) return { kind: "not_found" };
				if (isTerminal(root.status)) {
					return { kind: "already_finished", status: root.status as TerminalRunStatus };
				}
				const rootUpdatedAt = await markCanceled(db, ctx, [runId], reason);
				const rootAt = rootUpdatedAt.get(runId);
				if (!rootAt) throw new Error(`cancel: run ${runId} was not updated`);
				const canceled: CanceledRun[] = [
					{
						runId,
						taskId: root.task_id,
						queue: root.queue,
						enqueueSeq: root.enqueue_seq,
						previousStatus: root.status as CanceledRun["previousStatus"],
						attempt: root.attempt,
						error: canceledError(reason),
						updatedAt: rootAt,
					},
				];
				canceled.push(...(await cancelDescendants(db, ctx, { parentRunIds: [runId] }, reason)));
				// Lock order: all runs above are locked; waitpoint rows come last.
				const expireJobsToRemove = await timeOutWaitpointsOf(
					db,
					ctx,
					canceled.map(c => c.runId)
				);
				const wp = root.parent_waitpoint_id;
				const parentRes = wp ? await resolveChildWaitpoint(db, ctx, wp) : null;
				return { kind: "canceled", canceled, parent: parentRes, expireJobsToRemove };
			});
		},

		get(runId) {
			return getRun(direct(exec), runId);
		},

		async list(filter) {
			const limit = Math.min(MAX_LIST_LIMIT, Math.max(1, Math.floor(filter.limit)));
			const where: string[] = [];
			const params: unknown[] = [];
			const add = (sql: (p: string) => string, value: unknown) => {
				params.push(value);
				where.push(sql(`$${params.length}`));
			};
			if (filter.taskId !== undefined) add(p => `task_id = ${p}`, filter.taskId);
			if (filter.status !== undefined) {
				const statuses = typeof filter.status === "string" ? [filter.status] : [...filter.status];
				add(p => `status = ANY(${p}::text[])`, statuses);
			}
			if (filter.createdFrom !== undefined) add(p => `created_at >= ${p}`, filter.createdFrom);
			if (filter.createdTo !== undefined) add(p => `created_at < ${p}`, filter.createdTo);
			if (filter.cursor !== undefined) {
				params.push(filter.cursor.createdAt, filter.cursor.id);
				where.push(`(created_at, id) < ($${params.length - 1}, $${params.length}::uuid)`);
			}
			params.push(limit);
			const { rows } = await direct(exec).query<RunRow>(
				`SELECT ${RUN_COLUMNS} FROM ${runs}
				 ${where.length > 0 ? `WHERE ${where.join(" AND ")}` : ""}
				 ORDER BY created_at DESC, id DESC
				 LIMIT $${params.length}`,
				params
			);
			const items = rows.map(mapRun);
			const last = items[items.length - 1];
			return {
				items,
				nextCursor:
					items.length === limit && last ? { createdAt: last.createdAt, id: last.id } : null,
			};
		},

		async listBatchCandidates(filter) {
			const limit = Math.min(MAX_BATCH_LIMIT, Math.max(1, Math.floor(filter.limit)));
			const where: string[] = [];
			const params: unknown[] = [];
			const add = (sql: (p: string) => string, value: unknown) => {
				params.push(value);
				where.push(sql(`$${params.length}`));
			};
			if (filter.purpose === "cancel") {
				where.push("r.status IN ('queued', 'running', 'waiting')");
			} else {
				// Decision 9's reserved namespace: a failed run already retried owns the key
				// `__retry:<id>` (see `retryIdempotencyKey`); PK lookup on `(task_id, key)`.
				where.push("r.status = 'failed'", "r.parent_run_id IS NULL");
				add(
					p => `NOT EXISTS (SELECT 1 FROM ${ctx.t("idempotency_keys")} k
					   WHERE k.task_id = r.task_id AND k.key = ${p}::text || r.id::text)`,
					retryIdempotencyKey("")
				);
			}
			if (filter.taskId !== undefined) add(p => `r.task_id = ${p}`, filter.taskId);
			if (filter.queue !== undefined) add(p => `r.queue = ${p}`, filter.queue);
			if (filter.createdFrom !== undefined) add(p => `r.created_at >= ${p}`, filter.createdFrom);
			if (filter.createdTo !== undefined) add(p => `r.created_at < ${p}`, filter.createdTo);
			if (filter.cursor !== undefined) {
				params.push(filter.cursor.createdAt, filter.cursor.id);
				where.push(`(r.created_at, r.id) > ($${params.length - 1}, $${params.length}::uuid)`);
			}
			// One extra row tells whether more match, without a count.
			params.push(limit + 1);
			const { rows } = await direct(exec).query<RunRow>(
				`SELECT ${RUN_COLUMNS} FROM ${runs} r
				 WHERE ${where.join(" AND ")}
				 ORDER BY r.created_at, r.id
				 LIMIT $${params.length}`,
				params
			);
			const items = rows.slice(0, limit).map(mapRun);
			const last = items[items.length - 1];
			return {
				items,
				nextCursor: rows.length > limit && last ? { createdAt: last.createdAt, id: last.id } : null,
			};
		},

		async listChildResults(waitpointId) {
			const { rows } = await direct(exec).query<{
				id: string;
				batch_index: number | null;
				status: RunStatus;
				output: Json | null;
				error: SerializedError | null;
			}>(
				`SELECT id, batch_index, status, output, error FROM ${runs}
				 WHERE parent_waitpoint_id = $1 ORDER BY batch_index`,
				[waitpointId]
			);
			return rows.map(
				(r): ChildResult => ({
					runId: r.id,
					batchIndex: r.batch_index,
					status: r.status,
					output: r.output,
					error: r.error,
				})
			);
		},

		listStaleQueued(graceMs, limit, after, excludeQueues) {
			// A committed `{ tx }` run not yet claimed by the outbox sweep (decision 26) is the
			// sweep's to dispatch: recovering it here too only duplicates the `run:queued` event.
			// Once claimed (flag cleared), a lost enqueue is the reconciler's again.
			return listStale(
				"r.status = 'queued' AND NOT r.outbox_pending",
				graceMs,
				limit,
				after ?? null,
				excludeQueues ?? []
			);
		},

		listStaleWaiting(graceMs, limit, after, excludeQueues) {
			return listStale(
				`r.status = 'waiting' AND NOT ${blockingWaitpointExistsSql(ctx, "r.id")}`,
				graceMs,
				limit,
				after ?? null,
				excludeQueues ?? []
			);
		},

		repairOrphanWaiting(limit) {
			return atomic(exec, async db => {
				const { rows } = await db.query<{ id: string }>(
					`SELECT r.id FROM ${runs} r
					 WHERE r.status = 'waiting' AND r.run_after IS NULL
					   AND NOT ${blockingWaitpointExistsSql(ctx, "r.id")}
					 ORDER BY r.id LIMIT $1
					 FOR UPDATE OF r SKIP LOCKED`,
					[limit]
				);
				const out: EnqueueRequest[] = [];
				for (const { id } of rows) {
					const enqueue = await applyResumeRule(db, ctx, id);
					if (enqueue) out.push(enqueue);
				}
				return out;
			});
		},

		async listStalled(stallTimeoutMs, limit) {
			const { rows } = await direct(exec).query<{ id: string }>(
				`SELECT id FROM ${runs}
				 WHERE status = 'running'
				   AND heartbeat_at < now() - $1::float8 * interval '1 millisecond'
				 ORDER BY heartbeat_at LIMIT $2`,
				[stallTimeoutMs, limit]
			);
			return rows.map(r => r.id);
		},

		recoverStalled(runId, stallTimeoutMs, error) {
			return atomic(exec, async db => {
				const parent = await lockParentOf(db, ctx, runId);
				if (!parent) return { kind: "not_stalled" };
				const { rows } = await db.query<{
					task_id: string;
					attempt: number;
					max_attempts: number;
					parent_waitpoint_id: string | null;
				}>(
					`SELECT task_id, attempt, max_attempts, parent_waitpoint_id FROM ${runs}
					 WHERE id = $1 AND status = 'running'
					   AND heartbeat_at < now() - $2::float8 * interval '1 millisecond'
					 FOR UPDATE`,
					[runId, stallTimeoutMs]
				);
				const run = rows[0];
				if (!run) return { kind: "not_stalled" };
				if (run.attempt < run.max_attempts) {
					const { rows: upd } = await db.query<EnqueueRow & { attempt: number }>(
						`UPDATE ${runs}
						 SET status = 'queued', attempt = attempt + 1, error = $2::jsonb,
						     execution_id = NULL, run_after = now(), enqueue_seq = enqueue_seq + 1,
						     updated_at = now()
						 WHERE id = $1
						 RETURNING attempt, ${ENQUEUE_RETURNING}`,
						[runId, jsonParam(error)]
					);
					const row = upd[0];
					if (!row) throw new Error("recoverStalled: locked run disappeared");
					return { kind: "requeued", attempt: row.attempt, enqueue: mapEnqueue(row) };
				}
				const { rows: upd } = await db.query<{ updated_at: Date }>(
					`UPDATE ${runs}
					 SET status = 'failed', error = $2::jsonb, execution_id = NULL, run_after = NULL,
					     finished_at = now(), updated_at = now()
					 WHERE id = $1
					 RETURNING updated_at`,
					[runId, jsonParam(error)]
				);
				const updatedAt = upd[0]?.updated_at;
				if (!updatedAt) throw new Error("recoverStalled: locked run disappeared");
				const expireJobsToRemove = await timeOutWaitpointsOf(db, ctx, [runId]);
				const wp = run.parent_waitpoint_id;
				return {
					kind: "failed",
					taskId: run.task_id,
					attempt: run.attempt,
					updatedAt,
					parent: wp ? await resolveChildWaitpoint(db, ctx, wp) : null,
					expireJobsToRemove,
				};
			});
		},

		async deleteFinishedBatch(olderThanMs, limit) {
			const { rowCount } = await direct(exec).query(
				`DELETE FROM ${runs} WHERE id IN (
				   SELECT r.id FROM ${runs} r
				   LEFT JOIN ${runs} p ON p.id = r.parent_run_id
				   WHERE r.status IN ${TERMINAL_SQL}
				     AND r.finished_at < now() - $1::float8 * interval '1 millisecond'
				     AND (p.id IS NULL OR p.status IN ${TERMINAL_SQL})
				   LIMIT $2
				   FOR UPDATE OF r SKIP LOCKED)`,
				[olderThanMs, limit]
			);
			return rowCount;
		},
	};
}
