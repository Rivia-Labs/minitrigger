-- minitrigger migration 009: tables and indexes for the Postgres-only transport (decision 28).
--
-- Schema-agnostic like the earlier migrations: every name below is UNQUALIFIED, resolved against
-- the caller's schema by the migration runner's `SET LOCAL search_path`
-- (src/migrations/migrate.ts). Do not qualify any identifier in this file with a schema name; do
-- not run this file outside of that runner.
--
-- `system_jobs` replaces the BullMQ `${prefix}-system` queue in the Postgres transport
-- (waitpoint-expire | retention | schedule-tick). A claimed job gets
-- `locked_until = now() + lockMs` and a fresh `locked_by` token; every later write on the row is
-- fenced by `job_id AND locked_by` (src/store/sql/system-jobs.ts).
--
-- `schema_meta` holds per-schema facts such as the transport in use (mixed modes are refused).
--
-- `idx_runs_claim` serves the run claim (src/store/sql/claim.ts) and `nextDueInMs`;
-- `idx_runs_running_queue` serves the `globalConcurrency` count. No `CONCURRENTLY` (decision 16,
-- like migrations 005/006/008); `IF NOT EXISTS` lets a large `runs` table pre-build both indexes
-- with `CREATE INDEX CONCURRENTLY` under the same names before `engine.migrate()`.

CREATE TABLE system_jobs (
  job_id       text PRIMARY KEY,
  name         text NOT NULL,
  data         jsonb NOT NULL DEFAULT '{}'::jsonb,
  run_after    timestamptz NOT NULL,
  attempts     int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 1,
  locked_until timestamptz,
  locked_by    uuid,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_system_jobs_due ON system_jobs (run_after);

CREATE TABLE schema_meta (
  key        text PRIMARY KEY,
  value      text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_runs_claim ON runs (queue, run_after)
  WHERE status IN ('queued', 'waiting');
CREATE INDEX IF NOT EXISTS idx_runs_running_queue ON runs (queue)
  WHERE status = 'running';
