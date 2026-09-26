-- minitrigger migration 007: persisted queue pause state (docs/SPEC.md decision 27).
--
-- Schema-agnostic like the earlier migrations: every name below is UNQUALIFIED, resolved against
-- the caller's schema by the migration runner's `SET LOCAL search_path`
-- (src/migrations/migrate.ts). Do not qualify any identifier in this file with a schema name; do
-- not run this file outside of that runner.
--
-- `engine.queues.pause(name)`/`resume(name)` write here first (Postgres is the source of truth,
-- decision 1); the state is then mirrored into BullMQ's queue-level pause flag in Redis, applied
-- again by every `engine.start()` before its workers fetch and re-synced by the leader's
-- reconciler tick (a FLUSHALL loses the Redis flag). A queue without a row is not paused. One row
-- per queue ever paused: a tiny table, no extra index.

CREATE TABLE queue_state (
  queue      text        PRIMARY KEY,
  paused     boolean     NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
