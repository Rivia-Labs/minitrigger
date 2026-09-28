---
name: minitrigger-debug
description: Diagnose minitrigger (durable tasks/workflows on Postgres, plus BullMQ + Redis unless the project uses transport: "postgres") problems in a running project - runs stuck queued/running/waiting, jobs not running, run stuck, duplicate run, a run executed twice, NonDeterminismError or DuplicateStepError in production, TaskNotRegisteredError, schedule not firing, queue paused unexpectedly, outbox backlog growing, no leader/reconciler running. Use when the user reports that background runs are not progressing, are slow, duplicated, or failing.
allowed-tools: "Bash(psql *) Bash(docker compose exec *) Read Grep Glob"
---

# Debug minitrigger

Work from evidence: query Postgres and read the project's engine configuration before proposing a
fix. This skill is **read-only** — it never writes to the database. If a fix needs a write, say so
explicitly (preferably as an `engine.*` API call) and get the user's confirmation before it runs.

## 1. Snapshot

Run [scripts/diagnose.sql](scripts/diagnose.sql) against the project's real Postgres:

```bash
psql "$DATABASE_URL" -v schema=<configured schema> -f skills/minitrigger-debug/scripts/diagnose.sql
```

(Adjust the path to wherever the skill is installed; `schema` is the `createEngine({ schema })`
value, default `"minitrigger"`.) If the project's Postgres runs in a local Docker Compose setup
instead of a reachable `$DATABASE_URL`, use `docker compose exec`:

```bash
docker compose exec -T postgres psql -U <user> -d <db> -v schema=<schema> \
  -f skills/minitrigger-debug/scripts/diagnose.sql
```

**Never run write queries (`UPDATE`/`DELETE`/`INSERT`) against a project's real database without
explicit user confirmation** — this includes production, staging, or even a shared dev database.
The script itself only issues `SELECT`s.

Also read, in the project's own code:

- The `createEngine({...})` call: `schema`, `prefix`, `tasks`, `stallTimeoutMs`, `reconciler`
  (`intervalMs`/`graceMs`), `retention`, `outbox`, `schedules.pruneDeclarative`.
- Where `engine.start()`/`engine.stop()` are actually called, and from how many processes.
- Whether Redis has `maxmemory-policy noeviction` set (`redis-cli config get maxmemory-policy`) —
  anything else silently drops delayed-job state (sleeps, retry backoff, schedules).

## 2. Symptom → cause

**Run stuck `queued`** (diagnose.sql section 3)
- Lost enqueue: Postgres row exists, the Redis job was never created or was lost (process died
  between the two writes). Self-heals within `reconciler.graceMs + reconciler.intervalMs` — if it
  persists past that, no engine holds leadership (section 10) or the queue is paused (section 7).
- Queue paused (`queue_state.paused = true`, section 7): expected, not a bug — every process stops
  starting new runs of that queue until `engine.queues.resume(name)`.
- `run_after` in the future on purpose: a retry backoff, a debounce window, or a
  `trigger(..., { delay })` that hasn't elapsed yet — not stuck, just scheduled.
- Debounced run collapsed into a pending one (`debounce_key` set, section 8a): expected — it
  starts once the key has been quiet for `delay`, or at `debounce_deadline` if `maxDelay` was set.
- Concurrency-limited: `concurrency_waits > 0` (section 8b) — repeatedly found over the queue's
  `concurrencyKey.limit` at acquisition, backing off with a growing delay (~1s doubling to a 30s
  cap). Only `running` runs of the same key count; check who's currently `running` with that key.
- Outbox row (`outbox_pending = true`, section 6) not yet dispatched by any started engine — see
  "outbox backlog not dispatching" below.

**Run stuck `waiting`** (section 5)
- Expected if genuinely blocked: join to `waitpoints` (`status = 'pending'`) for what it's waiting
  on (`kind`: `token`/`child_run`/`batch`) and to `steps` (`kind` = `wait`/`trigger_wait`/
  `batch_wait`) for the step name. A `token` waitpoint needs `engine.tokens.complete(id, output)`
  called by whatever was supposed to signal it (webhook, human action) — check that path actually
  ran. A `child_run`/`batch` waitpoint resolves when the child run(s) finish; check the child's own
  status. `expires_at` past `now()` and still `pending` means the timeout job (`wp-*-expire`) also
  hasn't fired — check leadership (section 10), since expiration goes through the `-system` queue.

**Run stuck `running`** (stall, section 4)
- `heartbeat_at` older than `stallTimeoutMs` (default 60s): the reconciler treats it as stalled and
  recovers it (requeue with `attempt + 1`, or fail once `max_attempts` is exhausted) — but only if
  a leader is running (section 10).
- **Synchronous code blocking the event loop past `stallTimeoutMs` looks identical to a crashed
  process** from the reconciler's point of view, because the heartbeat runs on the same event loop
  as the task. Look for CPU-bound work not moved into a `worker_threads` worker.
- A crashed process's `running` run holds its `concurrencyKey` slot until stall recovery notices —
  a hung worker can block an entire key for up to `stallTimeoutMs`.

**Duplicate execution / a side effect ran twice**
- Expected under minitrigger's at-least-once guarantees, not necessarily a bug: (a) `engine.stop()`
  hands back a run still active past `shutdownTimeoutMs` **without** incrementing `attempt`, so the
  next attempt re-executes from the top; (b) a stalled run recovered by the reconciler is retried
  the same way. Either way, code **outside** a memoized `ctx.step.run`/`sleep`/etc. re-executes on
  every replay — if a side effect (an API call, a write) happened outside a step, it can run again.
  Fix: wrap the side effect in `ctx.step.run(name, fn)` so it's memoized, not re-executed on replay.
- Two runs for what should be one event: usually a missing `idempotencyKey` on `trigger()`.

**`TaskNotRegisteredError`** (section 9)
- Rolling-deploy symptom: the process that acquired the run (or the schedule tick) doesn't have
  that `task_id` in its registry — typically an old or new deploy briefly missing a task. Retryable
  for `engine.trigger`/worker acquisition; `step.trigger*` on an unregistered child task is fine by
  design (another process that does register it runs the child). Persisting past a deploy window
  means the task was removed from every process's registry while runs of it still exist.

**`NonDeterminismError` / `DuplicateStepError` / `NestedStepError`** (section 9)
- **Task code bugs, not infra.** Fatal — `attempt` does not increment, `status` goes straight to
  `failed`, no retry. `NonDeterminismError`: the step sequence on replay diverged from what was
  recorded (a step's name or `kind` at a given index doesn't match) — usually a deployed change
  that reordered/renamed/removed steps while a run was suspended mid-flight, or non-deterministic
  code (random values, `Date.now()`, concurrent branches calling steps after an `await`) outside
  `ctx.step.*`. `DuplicateStepError`: the same step name used twice in one execution.
  `NestedStepError`: `ctx.step.*` called from inside a `step.run` callback. Read the run's `steps`
  (`engine.runs.steps(runId)`) to see exactly where the recorded sequence and the code diverge.

**Schedule not firing**
- Compare `engine.schedules.list()` (from the running process) against section 11's Postgres rows
  — Postgres is the source of truth for `cron`/`timezone`/`payload`. If the row is correct but
  nothing fires, the BullMQ job scheduler may have drifted or gone missing; the reconciler
  self-heals this (re-upserts a missing/drifted scheduler, removes an orphaned one) **but only
  while a leader is running** — check section 10 first. Also check: is the schedule's `task_id`
  actually registered in whichever process is supposed to run it (a silent `TaskNotRegisteredError`
  on the tick retries 5x with backoff before giving up, per README)? Declarative schedules
  (`source = 'declarative'`) are synced once per `engine.start()` — a schedule added to code but
  not yet deployed anywhere won't appear in Postgres at all.
- **Missed ticks while every process was down are not backfilled** — this is expected behavior
  (README "Known limitations"), not a bug to chase.

**Queue paused unexpectedly** (section 7)
- Check `queue_state.paused` and `updated_at` — someone (or some script) called
  `engine.queues.pause(name)`. It's cross-process and survives restarts by design (Postgres is the
  source of truth, then mirrored into BullMQ). `engine.queues.resume(name)` undoes it.

**Outbox backlog growing** (section 6)
- `outbox_pending = true` rows should be near-zero and short-lived. A growing/aging backlog means
  no started engine is dispatching them: either nothing has called `engine.start()` against this
  schema, or the dedicated `LISTEN` connection is stuck. Check `pg_notification_queue_usage()` (a
  value approaching 1 means the cluster-wide notification queue is filling up because some listener
  stopped reading) and `pg_stat_activity` filtering `query LIKE 'LISTEN %mt_outbox_%'` for a
  listener that's connected but not consuming.

**No leader / reconciler not running** (section 10)
- No row in section 10 means no engine currently holds the leadership advisory lock for this
  schema — the reconciler isn't recovering lost enqueues, stalls, or overdue waitpoints, and
  retention isn't being scheduled. A brief gap right after a leader process dies is normal (another
  engine takes over within a few `reconciler.intervalMs` ticks); persistent absence across repeated
  runs of the script means **no started engine** (`engine.start()` called) is currently reachable
  for this schema at all.

## 3. Live, in-process diagnostics (complement to the SQL snapshot)

The SQL snapshot is a point-in-time view; these are live and only see what happens on the process
they're attached to.

```ts
// One-off or ad hoc: subscribe to this process's own transitions.
const off = engine.events.on("run:failed", event => {
  console.error(event.runId, event.taskId, event.attempt, event.error);
});
// names: run:queued, run:started, run:waiting, run:completed, run:failed, run:canceled, run:retrying

// Structured logging, opt-in: one line per transition, mirrored to engine.logger.
import { attachDefaultLogger } from "@rivia-labs/minitrigger";
const detach = attachDefaultLogger(engine); // or attachDefaultLogger(engine, pino())
// Security note: failure/retry lines only ever carry error.name/error.message -- never the full
// SerializedError (no stack, no cause) and never the run's payload/output.

// Per-queue state, this process only:
await engine.queues.list();
// [{ name, paused, updatedAt, declared, declaredConcurrency, concurrency, activeLocal }]
// `paused` is cross-process (from Postgres); `activeLocal` counts only THIS process's in-flight
// executions of the queue -- another process's runs aren't counted, so a low activeLocal here
// doesn't mean the queue is idle cluster-wide.
```

Both are process-local: `engine.events` only fires for transitions *this* process made (including
stall recoveries, if this process is the leader); `queues.list()`'s `activeLocal` is this process
only. Use the SQL snapshot for the cluster-wide picture, these for "what is *this* process seeing
right now."

## 4. Safe remediation

**Prefer the API over raw SQL** — it keeps BullMQ/Redis state (job existence, delays, queue pause
flags) consistent with Postgres; a raw `UPDATE`/`DELETE` on `runs`/`steps`/`waitpoints`/`schedules`/
`queue_state` does not, and can make things worse (e.g. deleting a `running` row while its BullMQ
job is still active, or clearing `run_after` without re-enqueuing).

```ts
await engine.runs.cancel(runId);
await engine.runs.cancelMany({ taskId, queue, createdFrom, createdTo, limit, cursor });
await engine.runs.retryFailed({ taskId, queue, createdFrom, createdTo, limit, cursor });
await engine.queues.pause(name);
await engine.queues.resume(name);
await engine.queues.scale(name, concurrency); // this process's worker only, not persisted
await engine.schedules.upsert({ id, task, cron, timezone, payload }); // re-syncs BullMQ too
await engine.schedules.delete(id);
```

**Confirm with the user before any write**, in any environment — including a raw SQL statement, if
the API genuinely can't do what's needed. State exactly what will change and why the API doesn't
cover it.

## 5. Report

State, in order:

1. **Evidence** — the relevant `diagnose.sql` section's output (or `engine.events`/`queues.list()`
   output), not a guess.
2. **Cause** — which symptom from section 2 above it matches, and why.
3. **Fix** — applied (if it was a safe, confirmed API call) or proposed (if it needs confirmation
   or touches production).
4. **Verification** — which query or API call to re-run afterward, and what a fixed state looks
   like (e.g. "section 3 no longer lists this run id", "`queues.list()` shows `paused: false`").
