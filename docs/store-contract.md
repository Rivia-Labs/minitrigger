# Store contract (`src/store/types.ts`)

Guide for the SQL repositories (task 2.2) and the in-memory store (task 9.1). `types.ts` is the
source of truth for signatures; this file says which SPEC decision each operation implements and
the SQL shape it must use. `$schema` is always `escapeIdentifier(schema)`.

## Global rules

- **Atomicity.** Every method is atomic. On `Store` (pool), a multi-statement method takes one
  client and runs `BEGIN ... COMMIT` (ROLLBACK + rethrow on error, always `release()`). On `StoreTx`
  it runs inside the caller's transaction. Only `runs.create` needs partial rollback: inside a
  `StoreTx` it uses `SAVEPOINT`/`ROLLBACK TO SAVEPOINT`. `withTransaction` does not nest.
- **Isolation** READ COMMITTED. Correctness comes from row locks.
- **Lock order** (no deadlocks between cancel, which goes top-down, and child completion, which
  goes bottom-up): parent run → run → descendants (level by level, `ORDER BY id`) → waitpoint
  rows. To lock a run's parent: read `parent_run_id` without a lock, `SELECT ... FOR UPDATE` the
  parent, then lock/update the run and re-validate.
- **Time** (dec. 17): every timestamp comes from `now()`; every returned `delayMs` is
  `greatest(0, round(extract(epoch from run_after - now()) * 1000))` computed in the same SQL.
  Intervals from ms: `now() + $n * interval '1 millisecond'`.
- **Ownership guard** (dec. 5): `EXISTS (SELECT 1 FROM runs WHERE id=$1 AND execution_id=$2 AND
  status='running' FOR SHARE)` for writes to other tables; for writes to the run row itself, the
  same predicate in the `UPDATE ... WHERE`. rowCount 0 → return `{ kind: "lost" }` / `"lost"`.
  Never throw for lost ownership.
- Every exit from `running` sets `execution_id = NULL`. Every `runs` write except `heartbeat` sets
  `updated_at = now()`.
- **Blocking waitpoint** (dec. 3, owner's decision: only awaited waitpoints block the resume): a
  `pending` waitpoint of the run that is either `child_run`/`batch` (always created together with
  their waiting step) or a `token` whose `${step_name}#wait` step (kind `wait`) exists, i.e. that
  `waitForToken` already awaited. A token created and not (yet) awaited never keeps a sleeping run
  from waking up. Single SQL source: `blockingWaitpointExistsSql` (`resolve.ts`), used by the
  resume rule, `suspend` and the reconciler's `listStaleWaiting`/`repairOrphanWaiting`.
- **Resume rule** (dec. 3), applied with the run locked `FOR UPDATE`: if `status='waiting'` and
  `NOT EXISTS (blocking waitpoint of run)` then `enqueue_seq = enqueue_seq + 1`, `run_after =
  greatest((SELECT max(wake_at) FROM steps WHERE run_id=$1 AND status='waiting' AND wake_at IS NOT
  NULL), now())`, status stays `waiting`, return `EnqueueRequest`. Otherwise return `null` (a
  `running` run is never enqueued; its suspension will see the resolved waitpoint).
- **Child resolution** (dec. 10), run inside every terminal transition of a run with
  `parent_waitpoint_id` (`complete`, `fail`, `cancel` (root only), `recoverStalled`→failed), with
  the parent already locked: `UPDATE waitpoints SET ... WHERE id=$wp AND status='pending'`:
  `child_run` → `status='completed'`; `batch` → `pending_count = pending_count - 1`, and
  `status='completed'` when it reaches 0. If no row was updated (already `timed_out`/`completed`),
  no-op → `parent: null`. When the waitpoint became `completed`, apply the resume rule to the
  parent and set `hasExpireJob = expires_at IS NOT NULL`. A batch decrement that doesn't reach 0
  returns a resolution with `resume: null, hasExpireJob: false`. Child outputs are NOT copied.
- **jsonb parameters** are always bound as JSON text (`jsonParam` = `JSON.stringify`, `::jsonb`;
  `jsonb[]` elements stringified one by one). Never pass a raw JS value: pg sends strings unquoted
  and turns arrays into PG array literals.
- **Descendants on failure.** A run that fails permanently (`fail`, `recoverStalled` → failed) does
  NOT cancel its descendants, exactly like `complete`: children started with `step.trigger` may
  outlive their parent. Only `cancel` and waitpoint expiry (`expire`) cancel children.
- **Own waitpoints on any terminal transition** (Phase 5). `complete`, `fail`, `recoverStalled` →
  failed and `cancel` set every `pending` waitpoint OWNED by the finished run(s) to `timed_out`
  (`resolved_at = now()`) in the same transaction, after the run update (waitpoint rows last), and
  return the ids that had `expires_at` in `expireJobsToRemove` (caller removes `wp-${id}-expire`).
  A later `completeToken` on them is `run_finished` (not `not_pending`/`timed_out`): the run
  status is read under the same `FOR UPDATE` lock.
- Callers enqueue/remove jobs only **after commit** (jobId `${runId}-${seq}`,
  `wp-${waitpointId}-expire`).

## RunStore

| Method | Decision | SQL shape |
|---|---|---|
| `create` | 9, 1 | `INSERT runs (status 'queued', attempt 1, enqueue_seq 1, run_after = now()+delay)`; if `idempotency`, `idempotency.claim` in the same tx. Not claimed → ROLLBACK (or to savepoint) and `get(existingRunId)` → `existing`. If that run is gone, retry once. |
| `triggerChildren` | 10, 11 | One tx: guard parent `FOR SHARE`; `INSERT waitpoints` (`child_run`: pending_count NULL; `batch`: pending_count = n; `expires_at = now()+timeout`); per child `INSERT runs ... RETURNING` (`queued`, `run_after=now()+delayMs` (default 0; `step.trigger({ delay })`), enqueue `delayMs` computed by the DB, parent_run_id, parent_waitpoint_id (NULL for `trigger`), parent_step_name, batch_index) + claim key with `ttlMs null`; `INSERT steps` (`trigger`: completed `{id}`, kind `trigger`; else waiting `{waitpointId}`, kind `trigger_wait`/`batch_wait`). Claim or step conflict → throw (rollback). Return enqueue per child + `expireDelayMs`. |
| `acquire` | 5 | Exactly the decision-5 UPDATE ... RETURNING *. |
| `heartbeat` | 6 | Own tx: `BEGIN; SET LOCAL statement_timeout = <int ms>; UPDATE runs SET heartbeat_at=now() WHERE id AND execution_id AND status='running'; COMMIT`. Never `SET` without `LOCAL`; do not touch `updated_at`. Not on `StoreTx`. |
| `complete` / `fail` | 5, 10 | Lock parent (if `parent_waitpoint_id`), then guarded `UPDATE runs SET status, output/error, finished_at=now(), run_after=NULL, execution_id=NULL`; then own pending waitpoints → `timed_out` (`expireJobsToRemove`); then child resolution. |
| `retry` | 4, 17 | Guarded `UPDATE ... status='queued', attempt=attempt+1, error, execution_id=NULL, run_after=now()+backoff, enqueue_seq=enqueue_seq+1 RETURNING` → enqueue. |
| `release` | 20 | Guarded `UPDATE ... status='queued', execution_id=NULL, run_after=now(), enqueue_seq+1`; attempt unchanged. |
| `suspend` | 3, 2 | `SELECT ... FROM runs WHERE id AND execution_id AND status='running' FOR UPDATE` (none → lost); then, in this tx: blocking waitpoint → `waiting, run_after NULL, seq+1` (no enqueue; the bump makes a stale redelivery of `${id}-${oldSeq}` a no-op in `acquire`); else `max(wake_at) > now()` over waiting sleeps → `waiting, seq+1, run_after=max`; else `queued, seq+1, run_after=now()`. Always `execution_id NULL`. |
| `cancel` | 5, 10, Ph6 | Lock root's parent, root, then iteratively `SELECT id FROM runs WHERE parent_run_id = ANY($ids) ORDER BY id FOR UPDATE` (walk through terminal nodes too); non-terminal → `canceled, execution_id NULL, run_after NULL, finished_at now(), error {name:'Canceled', message, retryable:false}`. Then (waitpoint rows last) `UPDATE waitpoints SET status='timed_out', resolved_at=now() WHERE run_id = ANY(canceled ids) AND status='pending' RETURNING id, expires_at IS NOT NULL` → `expireJobsToRemove` (ids with `expires_at`; caller removes `wp-${id}-expire`). Child resolution for the root only. Return root first. |
| `get`, `list` | Ph2/7 | `list`: `ORDER BY created_at DESC, id DESC`, cursor `(created_at, id) < ($c, $id)`, limit clamped to 100, optional task_id/status (`= ANY`)/created range. |
| `listChildResults` | 10 | `SELECT id, batch_index, status, output, error FROM runs WHERE parent_waitpoint_id=$1 ORDER BY batch_index`. |
| `listStaleQueued` | Ph6 #1 | `status='queued' AND run_after < now() - grace` → current seq, delay 0. Read-only. |
| `listStaleWaiting` | 3, Ph6 #2 | `status='waiting' AND run_after < now() - grace AND NOT EXISTS blocking wp` → same seq, delay 0. |
| `repairOrphanWaiting` | Ph6 #3 | tx: candidates `status='waiting' AND run_after IS NULL AND NOT EXISTS blocking wp ... FOR UPDATE SKIP LOCKED LIMIT n`, re-check, resume rule. |
| `listStalled` / `recoverStalled` | 6, 4 | List ids `status='running' AND heartbeat_at < now() - stall` (partial index). Per run tx: lock parent, lock run, re-check; `attempt < max_attempts` → `queued, attempt+1, error, run_after now(), seq+1`; else `failed` + finished_at + own pending waitpoints `timed_out` (`expireJobsToRemove`) + child resolution. |
| `deleteFinishedBatch` | Ph6, 10 | `DELETE FROM runs WHERE id IN (SELECT r.id FROM runs r LEFT JOIN runs p ON p.id = r.parent_run_id WHERE r.status IN terminal AND r.finished_at < now() - $age AND (p.id IS NULL OR p.status IN terminal) LIMIT n FOR UPDATE OF r SKIP LOCKED)`. |

`engine.runs.replay(id)` = `get` + `create` (new id, same task/payload, current queue and
maxAttempts from the task definition, **no** idempotency key). No dedicated store op.

## StepStore

| Method | Decision | SQL shape |
|---|---|---|
| `loadForExecution` | Ph4, 17 | The Phase-4 window query verbatim (`$2 = maxInlineBytes`; it also selects `kind`), plus `SELECT now()` and the run's waitpoints (`id, step_name, kind, status, pending_count, expires_at`, no output). When deferred, every `output` is null. |
| `loadOutputs` | Ph4 | `WHERE run_id=$1 AND seq_index BETWEEN $2 AND $3 ORDER BY seq_index`. |
| `insertCompleted` | 5, Ph4 | `INSERT INTO steps (..., kind, ...) SELECT ... WHERE <guard FOR SHARE> ON CONFLICT (run_id, name) DO NOTHING`, one CTE statement: guard failed → lost; name conflict → ok (not overwritten); `(run_id, seq_index)` collision under another name → throws. |
| `insertSleep` | Ph4, 17 | Same guard, `kind 'sleep', status 'waiting', wake_at = now()+ms RETURNING wake_at, delayMs`. |
| `completeSleep` | Ph4 | Guarded `UPDATE steps SET status='completed' WHERE run_id, name, status='waiting'`, returning the step; `not_due` if `wake_at > now()`. |
| `insertWaitRef` | 10, 11 | Guarded insert of `${name}#wait` with the given `kind` (`wait`), `waiting`, output `{waitpointId}`, `ON CONFLICT (run_id, name) DO NOTHING`; outcomes as `insertCompleted`. |
| `list` | Ph7 | All columns (incl. `kind`), `ORDER BY seq_index`. |

`steps.kind` (NOT NULL, CHECK) records which `ctx.step` method wrote the row and is compared
exactly on replay (decision 11): `run` (`insertCompleted`), `sleep` (`insertSleep`), `wait`
(`insertWaitRef`), `token` (`WaitpointStore.createToken`), and via `RunStore.triggerChildren` by
mode: `trigger` → `trigger`, `wait` → `trigger_wait`, `batch` → `batch_wait`.

Waitpoint-backed steps stay `waiting` forever; their state is the waitpoint's (found in the
snapshot by `step_name`: the step name for child/batch, the name without `#wait` for tokens).

## WaitpointStore

| Method | Decision | SQL shape |
|---|---|---|
| `createToken` | 10, 11 | One tx: guard; `INSERT waitpoints (kind 'token', status 'pending', expires_at) ON CONFLICT (run_id, step_name) DO NOTHING RETURNING` (conflict → return the existing token's id, `expireDelayMs` only while `pending`; a non-token waitpoint there throws); `INSERT steps (name, kind 'token', completed, {id}) ON CONFLICT (run_id, name) DO NOTHING`. |
| `completeToken` | 3, 12 | Read run_id; `SELECT ... FROM runs WHERE id FOR UPDATE`; `UPDATE waitpoints SET status='completed', output WHERE id AND kind='token' AND status='pending'`; resume rule. Not updated: `timed_out` token of a terminal run (completed/failed/canceled; status read by the `FOR UPDATE`) → `run_finished`; otherwise `not_pending` with the token status. |
| `expire` | 10, Ph6 #5 | Lock run FOR UPDATE; read the waitpoint (`status='pending' AND expires_at IS NOT NULL`, else noop); `expires_at > now()` → `not_due` with DB-computed `delayMs` (handler re-delays the job); else for child/batch cancel non-terminal children recursively (cancel's walk, `error.message = WAITPOINT_TIMEOUT_REASON` so the executor maps exactly those to `Timeout`; their parent-waitpoint resolution is a no-op; the caller removes their run jobs and aborts/publishes them), time out the canceled children's own pending waitpoints (→ `expireJobsToRemove`, as `cancel`), `UPDATE waitpoints SET status='timed_out', resolved_at=now()`; resume rule. |
| `listExpired` | Ph6 #5 | `status='pending' AND expires_at <= now()` (partial index). |
| `getToken` | 10 | `SELECT status, output, run_id FROM waitpoints WHERE id=$1`. |

## IdempotencyStore / LogStore

- `claim` (dec. 9): the decision-9 statement verbatim; no row → `SELECT run_id ... WHERE task_id,
  key` in the same tx. Never `xmax`. `find`: plain lookup.
- `logs.insertBatch` (Ph2/7): single `INSERT INTO run_logs (run_id, level, message, data, ts)
  SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::timestamptz[])` with
  `coalesce(ts, now())`. `list`: `WHERE run_id AND id > $after ORDER BY id LIMIT n`.

## ScheduleStore

| Method | Decision | SQL shape |
|---|---|---|
| `upsert` | 8, 21 | One tx: `SELECT cron, timezone FROM schedules WHERE id=$1 FOR UPDATE` (locks any existing row against a concurrent upsert of the same id), then `INSERT ... ON CONFLICT (id) DO UPDATE SET task_id, cron, timezone, payload, source, updated_at=now() RETURNING *`. `created` = no row found by the SELECT; `cronChanged`/`timezoneChanged` compare the new input against that row (both true when `created`). Never `xmax`. |
| `delete` | 21 | `DELETE FROM schedules WHERE id=$1`; `rowCount > 0`. |
| `get` | 8 | Plain lookup by `id`. |
| `list` | 8 | `[WHERE source=$1] ORDER BY id`; `listDeclarative()` = `list({ source: 'declarative' })`. |
| `deleteDeclarativeExcept` | 21 | `DELETE FROM schedules WHERE source='declarative' AND NOT (id = ANY($1::text[]))`; an empty `ids` deletes every declarative schedule (`id = ANY('{}')` is false for every row, so `NOT (...)` is true for all). `api` schedules are never touched. Returns the deleted count. |

`id` is validated (`^[a-zA-Z0-9_.-]{1,128}$`, no `:`) by `upsert` only -- the write path that
introduces new ids; `get`/`delete`/`list` accept any string (a non-matching id just isn't found).
Postgres is authoritative for `cron`/`timezone`/`payload` (decision 8): the `-system` tick handler
(engine-owned, not this store) always reads them fresh instead of trusting the BullMQ job
scheduler's own copy, and ticks create runs through `RunStore.create` with idempotency key
`__schedule:${id}:${ts}` and `ttlMs: null` -- that flow is the engine's, not `ScheduleStore`'s.

## Schema assumptions for 001_init.sql

`steps.run_id`, `waitpoints.run_id`, `run_logs.run_id` reference `runs(id) ON DELETE CASCADE`
(retention relies on it). `pending_count` is NULL for `token`/`child_run`.

## Schema assumptions for 002_schedules.sql

`schedules.task_id` is plain `text`, NOT a foreign key: tasks are registered in application code
(`defineTask`), not in a database table. `schedules.id` has no length/charset CHECK constraint --
`ScheduleStore.upsert` enforces the id shape in application code, matching how `runs.task_id` and
other id-shaped columns are validated outside the schema throughout this store.

## TODO

- Phase 7: consider an index `runs(created_at DESC, id DESC)` for `runs.list` without `taskId`
  (today only `runs(task_id, created_at DESC, id DESC)` exists).
