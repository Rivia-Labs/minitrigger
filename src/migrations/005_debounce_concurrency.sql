-- minitrigger migration 005: debounce and concurrency keys (docs/SPEC.md decisions 24 and 25).
--
-- Schema-agnostic like 001-004: every name below is UNQUALIFIED, resolved against the caller's
-- schema by the migration runner's `SET LOCAL search_path` (src/migrations/migrate.ts). Do not
-- qualify any identifier in this file with a schema name; do not run this file outside of that
-- runner.
--
-- Decision 24 (debounce): `debounce_key` is set by a debounced `trigger()` and cleared by
-- `acquire` (the key is released as soon as the run leaves `queued` for the first time, so a run
-- that goes back to `queued` -- retry, resume, graceful hand-back -- never absorbs a later
-- trigger). The unique partial index is what makes "at most one pending run per (task, key)"
-- hold across processes: two concurrent debounced triggers that both find no pending run race on
-- the INSERT, and the loser retries and collapses into the winner. `debounce_deadline` is the
-- `maxDelay` cap (NULL = no cap).
--
-- Decision 25 (concurrency key): `concurrency_key` is counted among `running` runs of the same
-- queue at acquisition (partial index: only running rows, usually a small set). `concurrency_waits`
-- counts consecutive `limited` acquisition attempts (reset to 0 whenever the run is acquired);
-- `acquireWithConcurrencyLimit` uses it to grow the retry delay (min(1s * 2^waits, 30s) + jitter)
-- instead of polling at a fixed interval.

ALTER TABLE runs
  ADD COLUMN debounce_key text,
  ADD COLUMN debounce_deadline timestamptz,
  ADD COLUMN concurrency_key text,
  ADD COLUMN concurrency_waits int NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX idx_runs_debounce ON runs (task_id, debounce_key)
  WHERE status = 'queued' AND debounce_key IS NOT NULL;

CREATE INDEX idx_runs_concurrency_running ON runs (queue, concurrency_key)
  WHERE status = 'running' AND concurrency_key IS NOT NULL;
