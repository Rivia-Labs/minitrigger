-- minitrigger diagnostics (READ-ONLY). Usage:
--   psql "$DATABASE_URL" -v schema=minitrigger -f diagnose.sql
--
-- `schema` is the configured `createEngine({ schema })` value (default "minitrigger" if the
-- library's own default was never overridden). Every table reference below is scoped to it
-- explicitly (`:"schema".<table>`) rather than relying on `search_path`, since this script does
-- not know -- and should not change -- the caller's session state.
--
-- Optional overrides (defaults match the library's own config defaults, see README "Config
-- options"): pass with additional -v flags, e.g. -v stall_seconds=120.
--   grace_seconds  -- reconciler.graceMs default (30s): how long a queued/waiting run may sit
--                     past run_after before the reconciler is expected to have re-enqueued it.
--   stall_seconds  -- stallTimeoutMs default (60s): how old a running run's heartbeat may be
--                     before the reconciler treats it as stalled.
--   failure_window -- how far back to look for "recent failures by error name" (default 24 hours).
--   row_limit      -- max rows per listing section (default 50).
--
-- This script only ever SELECTs. If a symptom below needs a fix, use the engine API
-- (engine.runs.cancel/cancelMany/retryFailed, engine.queues.pause/resume) from your own code, or
-- get explicit confirmation before running any UPDATE/DELETE by hand -- see SKILL.md "Safe
-- remediation".

\set ON_ERROR_STOP on

\if :{?schema}
\else
	\set schema minitrigger
\endif
\if :{?grace_seconds}
\else
	\set grace_seconds 30
\endif
\if :{?stall_seconds}
\else
	\set stall_seconds 60
\endif
\if :{?failure_window}
\else
	\set failure_window '24 hours'
\endif
\if :{?row_limit}
\else
	\set row_limit 50
\endif

\echo '== schema =' :schema '(grace' :grace_seconds 's, stall' :stall_seconds 's, failures window' :failure_window ')'

\echo ''
\echo '== 1. applied migrations (schema_migrations, ordered by version)'
SELECT version, name, applied_at
FROM :"schema".schema_migrations
ORDER BY version;

\echo ''
\echo '== 2. runs by status and queue'
SELECT queue, status, count(*) AS runs,
	min(created_at) AS oldest_created_at,
	max(created_at) AS newest_created_at
FROM :"schema".runs
GROUP BY 1, 2
ORDER BY 1, 2;

\echo ''
\echo '== 3. stale queued/waiting runs (run_after older than :grace_seconds s -- lost-enqueue candidates)'
\echo '   a queued/waiting run whose run_after is this far in the past should have been picked up'
\echo '   by the reconciler already (reconciler.intervalMs + reconciler.graceMs); if it keeps'
\echo '   showing up here across repeated runs of this script, no leader is running the reconciler'
\echo '   (see section 10) or the run''s queue is paused (see section 7).'
SELECT id, task_id, queue, status, attempt, max_attempts, run_after,
	now() - run_after AS overdue_by, created_at
FROM :"schema".runs
WHERE status IN ('queued', 'waiting')
	AND run_after IS NOT NULL
	AND run_after < now() - make_interval(secs => :grace_seconds)
ORDER BY run_after
LIMIT :row_limit;

\echo ''
\echo '== 4. running runs with a stale heartbeat (stall candidates, heartbeat older than :stall_seconds s)'
\echo '   recovered by the reconciler (requeue with attempt+1, or fail once max_attempts is used up).'
\echo '   synchronous code blocking the event loop past stallTimeoutMs looks identical to a crashed'
\echo '   process -- check the run''s own logic before assuming a crash (section "How to read'
\echo '   engine.events" in SKILL.md can help distinguish live vs. actually-stalled).'
SELECT id, task_id, queue, attempt, max_attempts, execution_id, heartbeat_at,
	now() - heartbeat_at AS heartbeat_age, started_at
FROM :"schema".runs
WHERE status = 'running'
	AND heartbeat_at IS NOT NULL
	AND heartbeat_at < now() - make_interval(secs => :stall_seconds)
ORDER BY heartbeat_at
LIMIT :row_limit;

\echo ''
\echo '== 5. waiting runs joined to what they are blocked on (pending waitpoints)'
\echo '   kind: token (createToken/waitForToken), child_run (triggerAndWait), batch (batchTriggerAndWait).'
SELECT r.id AS run_id, r.task_id, r.queue, r.run_after, w.step_name, w.kind AS waitpoint_kind,
	w.pending_count, w.expires_at, w.created_at AS waitpoint_created_at
FROM :"schema".runs r
JOIN :"schema".waitpoints w ON w.run_id = r.id AND w.status = 'pending'
WHERE r.status = 'waiting'
ORDER BY w.created_at
LIMIT :row_limit;

\echo ''
\echo '== 6. outbox backlog (trigger(..., { tx }) rows committed but not yet dispatched)'
\echo '   should be near-zero and short-lived (dispatched within outbox.pollIntervalMs, default 2s,'
\echo '   or instantly via NOTIFY). A growing/aging backlog means no started engine is running'
\echo '   (nothing calling engine.start() against this schema) or its LISTEN connection is stuck --'
\echo '   see SKILL.md "outbox backlog not dispatching".'
SELECT id, task_id, queue, created_at, now() - created_at AS pending_for
FROM :"schema".runs
WHERE outbox_pending
ORDER BY created_at
LIMIT :row_limit;

\echo ''
\echo '== 7. paused queues (queue_state)'
\echo '   every process should stop starting new runs of a paused queue -- a run stuck queued in'
\echo '   one of these is expected, not a bug (see section 3 above for genuinely stuck runs).'
SELECT queue, paused, updated_at
FROM :"schema".queue_state
WHERE paused
ORDER BY queue;

\echo ''
\echo '== 8a. debounce key hotspots (grouped by task_id, debounce_key)'
SELECT task_id, debounce_key, count(*) AS pending_runs, min(run_after) AS earliest_run_after,
	max(debounce_deadline) AS max_deadline
FROM :"schema".runs
WHERE debounce_key IS NOT NULL
GROUP BY 1, 2
ORDER BY pending_runs DESC
LIMIT :row_limit;

\echo ''
\echo '== 8b. concurrency key hotspots (grouped by queue, concurrency_key) + wait distribution'
\echo '   concurrency_waits > 0 means that run has been repeatedly found over the limit and is'
\echo '   backing off with a growing delay (~1s doubling to a 30s cap, see README "Concurrency keys").'
SELECT queue, concurrency_key,
	count(*) FILTER (WHERE status = 'running') AS running_now,
	count(*) FILTER (WHERE status <> 'running') AS other_runs,
	max(concurrency_waits) AS max_concurrency_waits
FROM :"schema".runs
WHERE concurrency_key IS NOT NULL
GROUP BY 1, 2
ORDER BY max_concurrency_waits DESC, running_now DESC
LIMIT :row_limit;

\echo ''
\echo '== 9. recent failures by error name (last :failure_window)'
\echo '   NonDeterminismError/DuplicateStepError/NestedStepError = task code bugs, not infra: fatal,'
\echo '   no retry (attempt stops incrementing, status stays failed). TaskNotRegisteredError is'
\echo '   usually a rolling-deploy symptom (retryable) unless it keeps recurring.'
SELECT error->>'name' AS error_name, task_id, count(*) AS occurrences,
	max(finished_at) AS most_recent
FROM :"schema".runs
WHERE status = 'failed'
	AND finished_at > now() - :'failure_window'::interval
GROUP BY 1, 2
ORDER BY most_recent DESC
LIMIT :row_limit;

\echo ''
\echo '== 10. leader / reconciler advisory lock holder'
\echo '   pg_try_advisory_lock(hashtext(schema), 2) is held by whichever process is currently the'
\echo '   leader (runs the reconciler + schedules retention, src/leader.ts). classid is'
\echo '   hashtext(schema)::oid (an int4->oid wrap; hashtext can be negative), objid=2, objsubid=2'
\echo '   for this two-key advisory lock form -- verified against real Postgres 16 pg_locks output.'
\echo '   No rows = no engine currently holds leadership for this schema: the reconciler is not'
\echo '   running, so lost enqueues, stalls and overdue waitpoints are not being recovered, and'
\echo '   retention is not being scheduled. A gap of a few ticks (reconciler.intervalMs, default'
\echo '   15s) after a leader dies is normal -- persistent absence is not.'
SELECT l.pid, a.usename, a.client_addr, a.application_name, a.backend_start,
	now() - a.backend_start AS held_for, l.granted
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'advisory'
	AND l.classid = hashtext(:'schema')::oid
	AND l.objid = 2
	AND l.objsubid = 2;

\echo ''
\echo '== 11. schedules (api + declarative)'
\echo '   compare against engine.schedules.list() from the running process and, for drift in the'
\echo '   BullMQ side, see SKILL.md "schedule not firing" (the reconciler self-heals scheduler'
\echo '   drift, so persistent drift here usually means no leader -- cross-check section 10).'
SELECT id, task_id, cron, timezone, source, created_at, updated_at
FROM :"schema".schedules
ORDER BY source, id;

\echo ''
\echo '== done =='
