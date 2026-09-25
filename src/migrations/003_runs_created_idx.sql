-- minitrigger migration 003: index for runs.list() without a taskId filter (docs/store-contract.md
-- TODO, Phase 7).
--
-- Schema-agnostic like 001_init.sql/002_schedules.sql: every name below is UNQUALIFIED, resolved
-- against the caller's schema by the migration runner's `SET LOCAL search_path`
-- (src/migrations/migrate.ts). Do not qualify any identifier in this file with a schema name; do
-- not run this file outside of that runner.
--
-- `idx_runs_task_created` (001_init.sql) covers `RunStore.list({ taskId })` (`WHERE task_id = $1
-- ORDER BY created_at DESC, id DESC`). A call without `taskId` uses the same `ORDER BY created_at
-- DESC, id DESC` (src/store/sql/runs.ts) but couldn't use that composite index (it doesn't lead
-- with `task_id`), so it fell back to a full sort of `runs`. `IF NOT EXISTS`: consistent with
-- every other index in this project being safe to re-run, even though `migrate()` never re-runs an
-- already-applied migration.

CREATE INDEX IF NOT EXISTS idx_runs_created ON runs (created_at DESC, id DESC);
