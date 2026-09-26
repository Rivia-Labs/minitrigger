-- minitrigger migration 008: index for runs.cancelMany() over active runs.
--
-- Schema-agnostic like the earlier migrations: every name below is UNQUALIFIED, resolved against
-- the caller's schema by the migration runner's `SET LOCAL search_path`
-- (src/migrations/migrate.ts). Do not qualify any identifier in this file with a schema name; do
-- not run this file outside of that runner.
--
-- `RunStore.listBatchCandidates({ purpose: "cancel" })` (src/store/sql/runs.ts) filters
-- `status IN ('queued', 'running', 'waiting')` and orders by `(created_at, id)` ascending. Without
-- `createdFrom`, the only usable index was `idx_runs_created` (003), walked from the oldest row
-- through the whole finished history before reaching an active run: measured 143 ms and ~1M rows
-- removed by filter for the first page on a table with 1M finished runs, versus 0.04 ms
-- (index-only) with this partial index. The index holds only active runs, so it stays small; a
-- status transition costs one more index write, while heartbeat/progress updates stay HOT (they
-- change neither `status` nor `created_at`).
--
-- `IF NOT EXISTS`: on a large `runs` table the index can be pre-built with `CREATE INDEX
-- CONCURRENTLY` under the same name before `engine.migrate()` (README "Upgrading to 0.3.0"); this
-- migration then only records the version.

CREATE INDEX IF NOT EXISTS idx_runs_active_created ON runs (created_at, id)
  WHERE status IN ('queued', 'running', 'waiting');
