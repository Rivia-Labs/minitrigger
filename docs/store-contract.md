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
  Intervals from ms: `now() + $n * interval '1 millisecond'`. `run_after` is never NULL where
  `delayMs` is computed from it, so this shape is safe there. `expiresAt`/`expireDelayMs`
  (`triggerChildren`'s `waitpoint`, `WaitpointStore.createToken`) are different: `expires_at` CAN be
  NULL (no timeout), and `greatest(0, NULL)` is **0**, not NULL, in Postgres (`GREATEST`/`LEAST`
  ignore NULL arguments and only return NULL when every argument is NULL). Their SQL must be
  `CASE WHEN expires_at IS NULL THEN NULL ELSE greatest(0, round(extract(epoch from
(expires_at - now())) * 1000)) END` so a waitpoint without a timeout reports `expireDelayMs:
null`, never `0` (which would make the caller schedule a `wp-${id}-expire` job that fires
  immediately). The in-memory store mirrors this explicitly instead of computing `greatest`.
- **`EnqueueRequest`/`CanceledRun`/transition results carry `updatedAt` (and `status` where it
  isn't already implied by `kind`)** (Phase 7 perf): every `RunStore`/`WaitpointStore` method that
  changes a run's status returns that row's new `status`/`updated_at` from the same write, so
  callers (the engine's `RunNotifier`) never need an extra `runs.get` per transition. Concretely:
  `EnqueueRequest` (used by `create`'s `enqueue`, `triggerChildren`'s per-child `enqueue`, `retry`,
  `release`, `suspend`'s `sleeping`/`requeued`, `recoverStalled`'s `requeued`,
  `listStaleQueued`/`listStaleWaiting`/`repairOrphanWaiting`, and `WaitpointResolution.resume`) now
  has `status`/`updatedAt`; `CanceledRun` (one per canceled run in `CancelRunResult`, and per
  canceled child in `ExpireWaitpointResult`'s `canceledChildren`) also has `taskId`/`attempt`
  (unchanged by cancel) and `error` (always `{ name: "Canceled", message: reason, retryable:
false }` for that cancel call, computed once and reused for every row it cancels) alongside
  `updatedAt` (status is always `canceled`); `CompleteRunResult`/`FailRunResult` have a top-level
  `updatedAt` (`kind` is already the new status: `completed`/`failed`); `RecoverStalledResult`'s
  `failed` variant has `updatedAt` plus `taskId`/`attempt` (attempts were exhausted, so `attempt` is
  unchanged -- read once under the same lock as `max_attempts`); `SuspendResult`'s
  `waiting_on_waitpoints` variant has both `status: "waiting"` and `updatedAt` (its `kind` string is
  not a `RunStatus`, unlike `sleeping`/`requeued` whose status lives on `enqueue`). None of this
  needs a store change for `taskId`/`attempt` beyond what the same locked row already carries
  (`task_id`/`attempt` added to the `SELECT`/`RETURNING` lists that lacked them); `error` is built
  from the `reason`/error argument the caller already passed in, not re-read from the row.
  `RunNotifier.status` (`src/executor/notify.ts`) takes exactly this: a `RunStatusInfo` (`runId`,
  `taskId`, `status`, `attempt`, `error`, `updatedAt`) built from the store result plus whatever the
  caller's own `ExecutionState`/`RunRecord` already holds (unchanged fields), never from a fresh
  `runs.get`.
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
  A later `completeToken` on one of them is `run_finished` (not `not_pending`/`timed_out`)
  because `resolved_at = finished_at` exactly (same transaction, same `now()`/`tx.now`), checked
  under the run's `FOR UPDATE` lock. A token that expired on its own (`expire`, an earlier
  transaction) and whose run only finished later has `resolved_at != finished_at`: `completeToken`
  reports `not_pending`/`timed_out` instead, since a caller completing it late needs to know the
  token itself expired, not merely that the run is gone.
- **Postgres value semantics** (both stores): `jsonb` rejects U+0000 in a string or key (SQLSTATE
  `22P05`) and a lone UTF-16 surrogate (`22P02`, `JSON.stringify` escapes it as `\udXXX`); `text`
  rejects U+0000 (`22021`) and stores a lone surrogate as U+FFFD (pg encodes strings as UTF-8). An
  own `__proto__` key in a jsonb object stays a plain data property. The memory store raises errors
  with the same `code` (`pgError`) through `toJsonb`/`pgText`, so test mode fails where production
  fails; sanitizing user input is the callers' job (error-format/logger), not the store's.
- Callers enqueue/remove jobs only **after commit** (jobId `${runId}-${seq}`,
  `wp-${waitpointId}-expire`).

## RunStore

| Method                           | Decision   | SQL shape                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| -------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `create`                         | 9, 1       | `INSERT runs (status 'queued', attempt 1, enqueue_seq 1, run_after = now()+delay)`, one statement. If `idempotency`: one statement too — a CTE claims the key (decision 9's `ON CONFLICT ... WHERE expires_at < now() RETURNING`) and the run is inserted `WHERE EXISTS (claim)` (the key's FK to `runs` is checked at end of statement). Not claimed → nothing inserted; a second statement reads the owner run → `existing`. If that run is gone, retry once. No transaction or savepoint is opened. With `debounce` (dec. 24): see "Debounced create" below. `concurrency_key` is stored as given. |
| `triggerChildren`                | 10, 11     | One tx: guard parent `FOR SHARE`; `INSERT waitpoints` (`child_run`: pending_count NULL; `batch`: pending_count = n; `expires_at = now()+timeout`); per child `INSERT runs ... RETURNING` (`queued`, `run_after=now()+delayMs` (default 0; `step.trigger({ delay })`), enqueue `delayMs` computed by the DB, parent_run_id, parent_waitpoint_id (NULL for `trigger`), parent_step_name, batch_index) + claim key with `ttlMs null`; `INSERT steps` (`trigger`: completed `{id}`, kind `trigger`; else waiting `{waitpointId}`, kind `trigger_wait`/`batch_wait`). Claim or step conflict → throw (rollback). Return enqueue per child + `expireDelayMs`.             |
| `acquire`                        | 5, 24      | Exactly the decision-5 UPDATE ... RETURNING *, plus `debounce_key = NULL` (the key is released once the run leaves `queued`). |
| `acquireWithConcurrencyLimit`    | 25         | One tx: `SELECT queue, concurrency_key, concurrency_waits ... WHERE id AND status IN (queued, waiting) AND enqueue_seq FOR UPDATE` (none → `stale`); key NULL → the `acquire` UPDATE (also resets `concurrency_waits = 0`); else `pg_advisory_xact_lock(hashtextextended(schema \|\| ':' \|\| queue \|\| ':' \|\| key, 0))` (single-bigint key space, separate from dec. 16's two-int locks), `count(*)` of `running` with the same `(queue, concurrency_key)`; `>= limit` → `UPDATE ... concurrency_waits = concurrency_waits+1, enqueue_seq+1, run_after=now()+delay(waits), updated_at RETURNING` → `limited` (status/attempt unchanged), `delay(waits) = min(1000 * 2^waits, 30_000) + jitter` computed from the locked row's PRE-increment `concurrency_waits`; else the `acquire` UPDATE (resets `concurrency_waits = 0`) → `acquired`. Only `running` counts. Lock order: the run row, then the advisory lock; nothing else is locked while holding it (no deadlock with other acquirers or cancel). |
| `heartbeat`                      | 6          | Own tx: `BEGIN; SET LOCAL statement_timeout = <int ms>; UPDATE runs SET heartbeat_at=now() WHERE id AND execution_id AND status='running'; COMMIT`. Never `SET` without `LOCAL`; do not touch `updated_at`. Not on `StoreTx`.                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `complete` / `fail`              | 5, 10      | Lock parent (if `parent_waitpoint_id`), then guarded `UPDATE runs SET status, output/error, finished_at=now(), run_after=NULL, execution_id=NULL`; then own pending waitpoints → `timed_out` (`expireJobsToRemove`); then child resolution.                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `retry`                          | 4, 17      | Guarded `UPDATE ... status='queued', attempt=attempt+1, error, execution_id=NULL, run_after=now()+backoff, enqueue_seq=enqueue_seq+1 RETURNING` → enqueue.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `release`                        | 20         | Guarded `UPDATE ... status='queued', execution_id=NULL, run_after=now(), enqueue_seq+1`; attempt unchanged.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `setProgress`                    | 23         | Guarded (like `heartbeat`, but `updated_at` DOES change, unlike `heartbeat`): `UPDATE runs SET progress=$3::jsonb, updated_at=now() WHERE <ownership guard> RETURNING updated_at`. Overwrites the whole value (never merged); unaffected by any other transition, so it survives retry/resume untouched. |
| `suspend`                        | 3, 2       | `SELECT ... FROM runs WHERE id AND execution_id AND status='running' FOR UPDATE` (none → lost); then, in this tx: blocking waitpoint → `waiting, run_after NULL, seq+1` (no enqueue; the bump makes a stale redelivery of `${id}-${oldSeq}` a no-op in `acquire`); else `max(wake_at) > now()` over waiting sleeps → `waiting, seq+1, run_after=max`; else `queued, seq+1, run_after=now()`. Always `execution_id NULL`.                                                                                                                                                                                                                                            |
| `cancel`                         | 5, 10, Ph6 | Lock root's parent, root, then iteratively `SELECT id FROM runs WHERE parent_run_id = ANY($ids) ORDER BY id FOR UPDATE` (walk through terminal nodes too); non-terminal → `canceled, execution_id NULL, run_after NULL, finished_at now(), error {name:'Canceled', message, retryable:false}`. Then (waitpoint rows last) `UPDATE waitpoints SET status='timed_out', resolved_at=now() WHERE run_id = ANY(canceled ids) AND status='pending' RETURNING id, expires_at IS NOT NULL` → `expireJobsToRemove` (ids with `expires_at`; caller removes `wp-${id}-expire`). Child resolution for the root only. Return root first.                                         |
| `get`, `list`                    | Ph2/7      | `list`: `ORDER BY created_at DESC, id DESC`, cursor `(created_at, id) < ($c, $id)`, limit clamped to 100, optional task_id/status (`= ANY`)/created range.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `listChildResults`               | 10         | `SELECT id, batch_index, status, output, error FROM runs WHERE parent_waitpoint_id=$1 ORDER BY batch_index`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `listStaleQueued`                | Ph6 #1, 26, 27 | `status='queued' AND NOT outbox_pending AND run_after < now() - grace` → current seq, delay 0. Read-only. The `NOT outbox_pending` (decision 26) excludes a `trigger(..., { tx })` run not yet claimed by the outbox sweep -- it's that sweep's job to dispatch it, not the reconciler's (once the sweep clears the flag, a still-lost enqueue is the reconciler's again). Optional 4th param `excludeQueues?: readonly string[]` (decision 27): `AND NOT (queue = ANY($n::text[]))`, applied as a filter over the same `run_after`-ordered scan (not an index that skips those rows outright -- see `CLAUDE.md`'s perf note), so a paused queue's backlog is never re-enqueued by this case while it's paused. Keyset page `ORDER BY run_after, id LIMIT n` after the optional cursor (`(run_after, id) > ($c::timestamptz, $id)`); returns `{ items, nextCursor }`, `nextCursor` null on a short page. The cursor's `runAfter` is UTC ISO text with microseconds (`to_char(run_after AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`), never a JS `Date` (ms truncation would loop/skip). The reconciler keeps one cursor per case across ticks (bounded pages per tick, wrap-around on a short page) so stale runs whose jobs legitimately exist can't hide a lost enqueue behind them. |
| `listStaleWaiting`               | 3, Ph6 #2, 27 | `status='waiting' AND run_after < now() - grace AND NOT EXISTS blocking wp` → same seq, delay 0. Paginated like `listStaleQueued`, including the same optional `excludeQueues?: readonly string[]` 4th param and its "filter, not index skip" cost.                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `repairOrphanWaiting`            | Ph6 #3     | tx: candidates `status='waiting' AND run_after IS NULL AND NOT EXISTS blocking wp ... FOR UPDATE SKIP LOCKED LIMIT n`, re-check, resume rule.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `listStalled` / `recoverStalled` | 6, 4       | List ids `status='running' AND heartbeat_at < now() - stall` (partial index). Per run tx: lock parent, lock run, re-check; `attempt < max_attempts` → `queued, attempt+1, error, run_after now(), seq+1`; else `failed` + finished_at + own pending waitpoints `timed_out` (`expireJobsToRemove`) + child resolution.                                                                                                                                                                                                                                                                                                                                               |
| `deleteFinishedBatch`            | Ph6, 10    | `DELETE FROM runs WHERE id IN (SELECT r.id FROM runs r LEFT JOIN runs p ON p.id = r.parent_run_id WHERE r.status IN terminal AND r.finished_at < now() - $age AND (p.id IS NULL OR p.status IN terminal) LIMIT n FOR UPDATE OF r SKIP LOCKED)`.                                                                                                                                                                                                                                                                                                                                                                                                                     |

**Debounced create** (dec. 24; `NewRun.delayMs` ignored, `idempotency` together with `debounce`
throws). Loop (bounded; exhausted → `MinitriggerError`, not a plain `Error`), each attempt one tx:
`SELECT id, enqueue_seq FROM runs WHERE task_id AND debounce_key AND status='queued' FOR UPDATE`.
Found → `UPDATE runs SET run_after = least(now()+delay, debounce_deadline), payload = CASE last
THEN $new ELSE payload END, enqueue_seq = CASE WHEN least(now()+delay, debounce_deadline) IS
DISTINCT FROM run_after THEN enqueue_seq+1 ELSE enqueue_seq END, updated_at = now() RETURNING`
(the `enqueue_seq`/`run_after` comparison reads the row as it stood BEFORE this UPDATE, simultaneous-
assignment style, same statement) → `debounced` with either `{ enqueue, staleSeq: <the locked seq>
}` when the recomputed `run_after` actually moved (caller enqueues the new seq and removes
`${id}-${staleSeq}`) or `{ enqueue: null }` when it didn't (audit item 1: a collapse landing on the
SAME `run_after` as before -- e.g. every collapse once it's pinned at `debounce_deadline` -- has
nothing to reschedule; bumping the seq anyway would needlessly fence the job already correctly
scheduled for that unchanged `run_after`/seq before a worker gets to acquire it, which broke the
"a key that never goes quiet still runs periodically" promise under a trigger burst faster than
acquisition). `payload` is still applied either way, and always trailing; `concurrency_key`/
`debounce_deadline`/`max_attempts`/`concurrency_waits` of the locked run are never touched by the
collapse (only `run_after`, `enqueue_seq` per the above, and, per `payload`, the payload -- in
particular the collapse never resets `concurrency_waits`, which only `acquireWithConcurrencyLimit`'s
own acquire path does). Not found → `SAVEPOINT`; `INSERT ... debounce_key, debounce_deadline =
now()+maxDelay (NULL without), run_after = least(now()+delay, now()+maxDelay)`; a unique violation
of `idx_runs_debounce` → `ROLLBACK TO SAVEPOINT` and loop (the next `FOR UPDATE` waits for/finds the
winner). Never `xmax`. The memory store mirrors the same "bump `enqueue_seq` only when the
recomputed `run_after` differs from the locked row's current one" rule (comparing the two `number`s
directly, no `now()` round-trip needed) and scans for the queued row of the key under its mutex,
raising the same unique violation on a conflicting insert (never loops: no concurrent writer can
interleave under its mutex).

`engine.runs.replay(id)` = `get` + `create` (new id, same task/payload, current queue and
maxAttempts from the task definition, **no** idempotency key). No dedicated store op.

**`listBatchCandidates`** (`engine.runs.cancelMany`/`retryFailed`): read-only, one statement.
`SELECT RUN_COLUMNS FROM runs r WHERE <purpose> [AND task_id] [AND queue] [AND created_at >= from]
[AND created_at < to] [AND (created_at, id) > ($c, $id)] ORDER BY created_at, id LIMIT limit+1`
(ASCENDING, unlike `list`: parents come before their children, so a batch cancel reaches a parent
first and its children as descendants; the extra row only says whether `nextCursor` is set; `limit`
clamped to 1..`MAX_BATCH_LIMIT` = 1000). `purpose: "cancel"` → `status IN ('queued','running',
'waiting')`; `purpose: "retry"` → `status = 'failed' AND parent_run_id IS NULL AND NOT EXISTS
(idempotency_keys WHERE task_id = r.task_id AND key = '__retry:' || r.id)` (PK lookup). The engine
then acts on each row in its own short transaction: `cancel(id)` (a row that finished meanwhile is
`already_finished` and skipped), or `create` with idempotency key `retryIdempotencyKey(id)`
(`__retry:<failed run id>`, `ttlMs: null`, the reserved `__` namespace of decision 9): a failed run
is retried at most once, even by concurrent callers (`existing` = already retried). No transaction
spans the batch; no `FOR UPDATE SKIP LOCKED` is needed (per-run row locks inside `cancel`, and the
idempotency claim, make concurrent batches safe).

## StepStore

| Method             | Decision | SQL shape                                                                                                                                                                                                                                                 |
| ------------------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `loadForExecution` | Ph4, 17  | The Phase-4 window query verbatim (`$2 = maxInlineBytes`; it also selects `kind`), plus `SELECT now()` and the run's waitpoints (`id, step_name, kind, status, pending_count, expires_at`, no output). When deferred, every `output` is null.             |
| `loadOutputs`      | Ph4      | `WHERE run_id=$1 AND seq_index BETWEEN $2 AND $3 ORDER BY seq_index`.                                                                                                                                                                                     |
| `insertCompleted`  | 5, Ph4   | `INSERT INTO steps (..., kind, ...) SELECT ... WHERE <guard FOR SHARE> ON CONFLICT (run_id, name) DO NOTHING`, one CTE statement: guard failed → lost; name conflict → ok (not overwritten); `(run_id, seq_index)` collision under another name → throws. |
| `insertSleep`      | Ph4, 17  | Same guard, `kind 'sleep', status 'waiting', wake_at = now()+ms RETURNING wake_at, delayMs`.                                                                                                                                                              |
| `completeSleep`    | Ph4      | Guarded `UPDATE steps SET status='completed' WHERE run_id, name, status='waiting'`, returning the step; `not_due` if `wake_at > now()`.                                                                                                                   |
| `insertWaitRef`    | 10, 11   | Guarded insert of `${name}#wait` with the given `kind` (`wait`), `waiting`, output `{waitpointId}`, `ON CONFLICT (run_id, name) DO NOTHING`; outcomes as `insertCompleted`.                                                                               |
| `list`             | Ph7      | All columns (incl. `kind`), `ORDER BY seq_index`.                                                                                                                                                                                                         |

`steps.kind` (NOT NULL, CHECK) records which `ctx.step` method wrote the row and is compared
exactly on replay (decision 11): `run` (`insertCompleted`), `sleep` (`insertSleep`), `wait`
(`insertWaitRef`), `token` (`WaitpointStore.createToken`), and via `RunStore.triggerChildren` by
mode: `trigger` → `trigger`, `wait` → `trigger_wait`, `batch` → `batch_wait`.

Waitpoint-backed steps stay `waiting` forever; their state is the waitpoint's (found in the
snapshot by `step_name`: the step name for child/batch, the name without `#wait` for tokens).

## WaitpointStore

| Method          | Decision   | SQL shape                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| --------------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `createToken`   | 10, 11     | One tx: guard; `INSERT waitpoints (kind 'token', status 'pending', expires_at) ON CONFLICT (run_id, step_name) DO NOTHING RETURNING` (conflict → return the existing token's id, `expireDelayMs` only while `pending`; a non-token waitpoint there throws); `INSERT steps (name, kind 'token', completed, {id}) ON CONFLICT (run_id, name) DO NOTHING`.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `completeToken` | 3, 12      | Read run_id; `SELECT ... FROM runs WHERE id FOR UPDATE`; `UPDATE waitpoints SET status='completed', output WHERE id AND kind='token' AND status='pending'`; resume rule. Not updated: `timed_out` token whose run is terminal AND `resolved_at = finished_at` exactly (same transaction: the run's own terminal exit timed it out) → `run_finished`; otherwise `not_pending` with the token status (covers a token that expired on its own, in an earlier transaction, whose run only finished later).                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `expire`        | 10, Ph6 #5 | Lock run FOR UPDATE; read the waitpoint (`status='pending' AND expires_at IS NOT NULL`, else noop); `expires_at > now()` → `not_due` with DB-computed `delayMs` (handler re-delays the job); else for child/batch cancel non-terminal children recursively (cancel's walk seeded ONLY with the still non-terminal children: a child that already finished keeps its descendants, e.g. ones started by `step.trigger`; `error.message = WAITPOINT_TIMEOUT_REASON` so the executor maps exactly those to `Timeout`; their parent-waitpoint resolution is a no-op; the caller removes their run jobs and aborts/publishes them), time out the canceled children's own pending waitpoints (→ `expireJobsToRemove`, as `cancel`), `UPDATE waitpoints SET status='timed_out', resolved_at=now()`; resume rule. |
| `listExpired`   | Ph6 #5     | `status='pending' AND expires_at <= now()` (partial index).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `getToken`      | 10         | `SELECT status, output, run_id FROM waitpoints WHERE id=$1`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |

## IdempotencyStore / LogStore

- `claim` (dec. 9): the decision-9 statement verbatim; no row → `SELECT run_id ... WHERE task_id,
key` in the same tx. Never `xmax`. `find`: plain lookup.
- `logs.insertBatch` (Ph2/7): single `INSERT INTO run_logs (run_id, level, message, data, ts)
SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::jsonb[], $5::timestamptz[])` with
  `coalesce(ts, now())`. `list`: `WHERE run_id AND id > $after ORDER BY id LIMIT n`.

## ScheduleStore

| Method                    | Decision | SQL shape                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `upsert`                  | 8, 21    | One tx, loop: `SELECT ... FROM schedules WHERE id=$1 FOR UPDATE`. No row → `INSERT ... ON CONFLICT (id) DO NOTHING RETURNING *`: a row → `created` (all flags true); nothing (a concurrent upsert of the same NEW id committed first; a missing row can't be locked) → loop, the SELECT now locks the committed row. Row → `requireSourceMatch` guard (conflict, no write) else `UPDATE ... SET task_id, cron, timezone, payload, source, updated_at=now() RETURNING *`; `cronChanged`/`timezoneChanged` compare against the locked row. Never `xmax`. |
| `delete`                  | 21       | `DELETE FROM schedules WHERE id=$1`; `rowCount > 0`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `get`                     | 8        | Plain lookup by `id`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `list`                    | 8        | `[WHERE source=$1] ORDER BY id`; `listDeclarative()` = `list({ source: 'declarative' })`.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `deleteDeclarativeExcept` | 21       | `DELETE FROM schedules WHERE source='declarative' AND NOT (id = ANY($1::text[]))`; an empty `ids` deletes every declarative schedule (`id = ANY('{}')` is false for every row, so `NOT (...)` is true for all). `api` schedules are never touched. Returns the deleted count.                                                                                                                                                                                                                                                                          |

`id` is validated (`^[a-zA-Z0-9_.-]{1,128}$`, no `:`) by `upsert` only -- the write path that
introduces new ids; `get`/`delete`/`list` accept any string (a non-matching id just isn't found).
Postgres is authoritative for `cron`/`timezone`/`payload` (decision 8): the `-system` tick handler
(engine-owned, not this store) always reads them fresh instead of trusting the BullMQ job
scheduler's own copy, and ticks create runs through `RunStore.create` with idempotency key
`__schedule:${id}:${ts}` and `ttlMs: null` -- that flow is the engine's, not `ScheduleStore`'s.

## QueueStateStore (decision 27)

| Method      | SQL shape                                                                                                                                                                                                                                                                                              |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `setPaused` | One statement: `WITH prev AS (SELECT paused ... WHERE queue=$1) INSERT ... ON CONFLICT (queue) DO UPDATE SET paused, updated_at=now() RETURNING *, (SELECT paused FROM prev)`. Always writes; `changed` = `paused` differs from the previous value (missing row = not paused; approximate under concurrent writers, informational only). |
| `get`       | Plain lookup by `queue`.                                                                                                                                                                                                                                                                              |
| `list`      | `ORDER BY queue` (few rows: one per queue ever paused).                                                                                                                                                                                                                                                |

The engine writes the row first, then mirrors it into BullMQ's queue-level pause flag (Redis);
`start()` and the leader's reconciler re-apply every row to Redis (see SPEC decision 27). The store
never talks to Redis.

## Schema assumptions for 001_init.sql

`steps.run_id`, `waitpoints.run_id`, `run_logs.run_id` reference `runs(id) ON DELETE CASCADE`
(retention relies on it). `pending_count` is NULL for `token`/`child_run`.

## Schema assumptions for 002_schedules.sql

`schedules.task_id` is plain `text`, NOT a foreign key: tasks are registered in application code
(`defineTask`), not in a database table. `schedules.id` has no length/charset CHECK constraint --
`ScheduleStore.upsert` enforces the id shape in application code, matching how `runs.task_id` and
other id-shaped columns are validated outside the schema throughout this store.

## Schema assumptions for 003_runs_created_idx.sql

Adds `idx_runs_created ON runs (created_at DESC, id DESC)`, matching `RunStore.list`'s
`ORDER BY created_at DESC, id DESC` when called without `taskId` (see the `get`, `list` row above)
-- `idx_runs_task_created` (001_init.sql) only helps when `taskId` is part of the filter.

## Schema assumptions for 004_run_progress.sql

Adds `runs.progress jsonb` (decision 23), nullable, no default, NOT indexed -- `setProgress`'s
guarded `UPDATE` stays HOT on `fillfactor = 80` (001_init.sql), the same property `heartbeat`
relies on. `RUN_COLUMNS`/`RunRow`/`mapRun` (`store/sql/resolve.ts`) and the memory store's
`RunRow`/`mapRun` (`store/memory/{state,resolve}.ts`) all carry it; the memory store applies the
same jsonb value rules (`toJsonb`) as every other jsonb-shaped column.

## Schema assumptions for 005_debounce_concurrency.sql

Adds `runs.debounce_key text`, `runs.debounce_deadline timestamptz`, `runs.concurrency_key text`
and `runs.concurrency_waits int NOT NULL DEFAULT 0` (decisions 24, 25), the first three nullable.
`idx_runs_debounce` is UNIQUE on `(task_id, debounce_key) WHERE status = 'queued' AND debounce_key
IS NOT NULL` (at most one pending run per key, across processes); `idx_runs_concurrency_running` on
`(queue, concurrency_key) WHERE status = 'running' AND concurrency_key IS NOT NULL` serves the
acquisition count. `RUN_COLUMNS`/`RunRecord` carry `debounce_key`/`concurrency_key` (not the
deadline, not `concurrency_waits`: both are internal to `acquireWithConcurrencyLimit`/the
debounced-create path, read and written by dedicated queries, never through `RUN_COLUMNS`). The
SQL contract harness shifts `debounce_deadline` with the other timestamps when "advancing the
clock".

## Schema assumptions for 006_outbox.sql

Adds `runs.outbox_pending boolean NOT NULL DEFAULT false` and `idx_runs_outbox_pending ON runs
(created_at) WHERE outbox_pending` (decision 26). Not in `RUN_COLUMNS`/`RunRecord`: internal to the
outbox path. `RunStore.create` with `NewRun.outbox` inserts the row with the flag set and runs
`pg_notify($channel, '')` in the RETURNING list of the same `INSERT` (one round trip; with an
idempotency key, the run insert only happens when the key is claimed, so a live key inserts
nothing and queues no NOTIFY). It is used through `createSqlTxRunStore(client, schema)` (`store/sql/index.ts`), a
`RunStore` bound to the CALLER's client (`SqlExecutor` `{ kind: "tx", client: SqlQueryable }`):
`create` (without `debounce`) is a single statement there — never a `SAVEPOINT`, so any number of
triggers opens no subtransaction — and works on an autocommit client too. `outbox` + `debounce` throws. `RunStore.claimOutbox(limit)` is one statement: a CTE
`SELECT id ... WHERE outbox_pending ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED` feeding
`UPDATE ... SET outbox_pending = false` (no `updated_at` change), returning every cleared row's
count and the enqueue data (`delayMs` from `run_after - now()`, plus `attempt`/`error` for the
status notification) of those still `queued`. The memory store rejects `outbox` and its
`claimOutbox` always returns nothing.

## Schema assumptions for 007_queue_state.sql

Adds `queue_state(queue text PRIMARY KEY, paused boolean NOT NULL, updated_at timestamptz NOT NULL
DEFAULT now())` (decision 27). No FK (queues are declared in application code, like
`schedules.task_id`), no extra index. A queue without a row is not paused. The SQL contract
harness truncates it and shifts `updated_at` with the other timestamps.

## Schema assumptions for 008_runs_active_created_idx.sql

Adds `idx_runs_active_created ON runs (created_at, id) WHERE status IN ('queued', 'running',
'waiting')` for `listBatchCandidates({ purpose: "cancel" })`, whose `WHERE` repeats that exact
status list and orders by `(created_at, id)` ascending: keep both in sync, or the planner falls
back to walking `idx_runs_created` through the finished history. `IF NOT EXISTS`, so a copy
pre-built `CONCURRENTLY` under the same name is kept.

## Postgres transport (migration 009, decision 28)

SQL store only: `createSqlStore` returns `SqlStore extends Store` with three pool-bound
repositories that are not part of `StoreTx` and have no memory-store counterpart.

### RunClaimStore (`store.claim`, `src/store/sql/claim.ts`)

| Method      | SQL shape |
| ----------- | --------- |
| `claimRuns({ queue, limit })` | One statement, no explicit transaction: `WITH picked AS (SELECT r.id ... WHERE queue=$1 AND status IN ('queued','waiting') AND run_after <= now() AND NOT outbox_pending AND queue not paused ORDER BY run_after, created_at, id LIMIT $2 FOR UPDATE OF r SKIP LOCKED) UPDATE runs r SET status='running', execution_id=gen_random_uuid(), heartbeat_at=now(), started_at=<below>, debounce_key=NULL, updated_at=now() FROM picked RETURNING r.*` (columns qualified with `r.`; result re-sorted by `(run_after, created_at, id)`). `limit <= 0` → `[]` without a query. |
| `claimRuns({ ..., globalConcurrency })` | Short transaction: `pg_advisory_xact_lock(hashtextextended('<schema>:<queue>:global', 0))`, then `count(*)` of the queue's `running` rows (fresh READ COMMITTED snapshot), then the same claim with `LIMIT least(limit, globalConcurrency - running)`; nothing claimed when that is `<= 0`. Exact across processes. |
| `nextDueAt(queue)` | `min(run_after)` over the queue's `queued`/`waiting` rows with `run_after > now()`, or null. |

Mirrors `acquire` with two deliberate differences: `enqueue_seq` is unchanged (there is no job to
fence; `release`/`retry`/`suspend` still bump it) and `concurrency_waits` is NOT reset (the
decision 25 backoff grows from it; the concurrency-key re-check resets it on `ok`).
`started_at = CASE WHEN concurrency_key IS NULL THEN coalesce(started_at, now()) ELSE started_at
END`: a run with a concurrency key only "starts" after the re-check says `ok`. The returned
`runAfter` is the pre-claim value (the `UPDATE` does not touch it).

Invariant used by the claim (already true before this migration): `waiting` + `run_after` set =
ready to resume at `run_after`; `waiting` + `run_after NULL` = blocked on waitpoints, never
claimed. A `waiting` run whose only pending waitpoint is a token that is not awaited yet has
`run_after` set (decision 3) and is claimed.

Lock order: with `globalConcurrency`, the queue's advisory xact lock first, then row locks via
`SKIP LOCKED`. The claim never WAITS on a row lock, so it cannot form a cycle with the
parent -> run -> descendants -> waitpoints order; the advisory key is one bigint, outside the
`(hashtext(schema), 1|2)` space of decision 16 (like the decision 25 lock).

Indexes: `idx_runs_claim ON runs (queue, run_after) WHERE status IN ('queued','waiting')` and
`idx_runs_running_queue ON runs (queue) WHERE status = 'running'` (the `globalConcurrency` count),
both `IF NOT EXISTS` so they can be pre-built `CONCURRENTLY`.

### SystemJobStore (`store.systemJobs`, `src/store/sql/system-jobs.ts`)

`system_jobs (job_id PK, name, data jsonb, run_after, attempts, max_attempts, locked_until,
locked_by uuid, created_at)`, index `idx_system_jobs_due (run_after)`.

| Method | SQL shape |
| ------ | --------- |
| `upsert` | `INSERT ... VALUES (..., now() + delayMs, maxAttempts) ON CONFLICT (job_id) DO NOTHING` (BullMQ jobId semantics: re-add is a no-op). Defaults: `data {}`, `delayMs 0`, `maxAttempts 1`. |
| `upsertScheduleTick` | `INSERT (name 'schedule-tick', max_attempts 5) ON CONFLICT DO UPDATE SET data, run_after, attempts=0, locked_until/locked_by=NULL WHERE data->>'cron' or data->>'timezone' IS DISTINCT FROM EXCLUDED RETURNING (xmax = 0)`: `created` / `updated` / `unchanged` (no row). Same pattern is a no-op, so a restart never skips a due, unprocessed tick; a changed pattern releases the lock and the worker on the old tick loses its fencing. |
| `claim(limit, lockMs)` | `WITH picked AS (SELECT job_id WHERE run_after <= now() AND (locked_until IS NULL OR locked_until <= now()) ORDER BY run_after LIMIT $1 FOR UPDATE SKIP LOCKED) UPDATE ... SET locked_until = now() + lockMs, locked_by = gen_random_uuid()`. An expired lock is claimable again with a new token. |
| `complete` | `DELETE ... WHERE job_id=$1 AND locked_by=$2`. |
| `delay` | `run_after = now() + delayMs`, lock released; fenced. |
| `fail` | Two CTEs with disjoint predicates over one snapshot: `DELETE ... WHERE attempts + 1 >= max_attempts` (`exhausted`) and `UPDATE SET attempts+1, run_after = now() + backoffMs, lock released WHERE attempts + 1 < max_attempts` (`retried`); neither matched = `lost`. Never a DELETE and an UPDATE on the same row in one statement (the second would be silently ignored). |
| `rescheduleTick` | `run_after = $3, attempts = 0, data = jsonb_set(data, '{prevMillis}', ...)`, lock released; fenced. |
| `remove` | Without `evenIfLocked`: `AND (locked_until IS NULL OR locked_until <= now())` (never removes an active job, like BullMQ). `evenIfLocked` removes a scheduler row even mid-tick. |
| `get` / `listByName` | Plain reads (`listByName` ordered by `run_after, job_id`). |
| `nextDueAt` | `min(run_after) WHERE run_after > now()` (indexed); a lock that expires later is found by the regular poll. |
| `now` | `SELECT now()` (decision 17). |

Fencing: every write after a claim matches `job_id AND locked_by`. A token that is not a UUID
never matches (checked in JS, no query). A worker whose lock expired and was re-claimed by
another gets `false`/`"lost"` and must drop its result; handlers are idempotent (expiry re-checks
the waitpoint, retention is `SKIP LOCKED`, a repeated tick hits the `__schedule:id:ts` key).

### SchemaMetaStore (`store.meta`, `src/store/sql/schema-meta.ts`)

`schema_meta (key PK, value, updated_at)`. `get` is a plain lookup. `setIfAbsent`: `INSERT ...
ON CONFLICT (key) DO NOTHING RETURNING value` (waits for a concurrent inserter to commit); when
nothing was inserted, the effective value is re-read in a NEW statement (a read inside the same
statement would use the snapshot from before the concurrent commit). The existing value always
wins.
