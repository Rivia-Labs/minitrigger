-- minitrigger migration 006: transactional trigger / outbox (docs/SPEC.md decision 26).
--
-- Schema-agnostic like the earlier migrations: every name below is UNQUALIFIED, resolved against
-- the caller's schema by the migration runner's `SET LOCAL search_path`
-- (src/migrations/migrate.ts). Do not qualify any identifier in this file with a schema name; do
-- not run this file outside of that runner.
--
-- `trigger(task, payload, { tx })` inserts the run inside the CALLER's transaction and cannot
-- enqueue it (the job must neither run before the commit nor after a rollback), so the row is
-- written with `outbox_pending = true`. After the commit, a started engine claims it
-- (`UPDATE ... SET outbox_pending = false ... FOR UPDATE SKIP LOCKED`) and enqueues it. The partial
-- index only ever holds the committed-but-not-yet-dispatched rows (normally none), so the claim
-- sweep that every started engine runs on each NOTIFY and on its poll is an index-only probe of a
-- near-empty index. Updates that don't touch `outbox_pending` (heartbeat, progress) stay HOT.
--
-- Upgrading a large `runs` table: `CREATE INDEX` (without CONCURRENTLY, like migration 005) holds
-- a SHARE lock on `runs` while it scans the table; see README "Upgrading to 0.3.0" to pre-create
-- it CONCURRENTLY. The column itself is a metadata-only change (constant default, Postgres 11+).

ALTER TABLE runs ADD COLUMN outbox_pending boolean NOT NULL DEFAULT false;

CREATE INDEX idx_runs_outbox_pending ON runs (created_at) WHERE outbox_pending;
