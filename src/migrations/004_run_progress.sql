-- minitrigger migration 004: run progress (docs/SPEC.md decision 23).
--
-- Schema-agnostic like 001_init.sql/002_schedules.sql/003_runs_created_idx.sql: every name below
-- is UNQUALIFIED, resolved against the caller's schema by the migration runner's `SET LOCAL
-- search_path` (src/migrations/migrate.ts). Do not qualify any identifier in this file with a
-- schema name; do not run this file outside of that runner.
--
-- One mutable value per run (`ctx.progress.set`), overwritten in place by its owning execution
-- (decision 5's ownership guard) and read back by `runs.get`/`runs.list`/`runs.subscribe`. Not
-- indexed, so `UPDATE runs SET progress = ..., updated_at = now() WHERE <ownership guard>` stays
-- HOT (`runs` was already created with `fillfactor = 80` in 001_init.sql for exactly this: cheap,
-- frequent single-row updates that don't touch any indexed column).

ALTER TABLE runs ADD COLUMN progress jsonb;
