-- minitrigger migration 002: schedules (Phase 8, docs/SPEC.md decisions 8, 21).
--
-- Schema-agnostic like 001_init.sql: every name below is UNQUALIFIED, resolved against the
-- caller's schema by the migration runner's `SET LOCAL search_path` (src/migrations/migrate.ts).
-- Do not qualify any identifier in this file with a schema name; do not run this file outside of
-- that runner.
--
-- Postgres is authoritative for schedules (decision 8): `id` is the schedule id the caller
-- chooses (validated by the store as `^[a-zA-Z0-9_.-]{1,128}$`, no `:` -- it is embedded in the
-- idempotency key `__schedule:${id}:${ts}` and used as the BullMQ job scheduler id). `task_id` is
-- NOT a foreign key (tasks are registered in application code, not in the database). `source`
-- distinguishes schedules declared via `engine.schedules.upsert()` ('api') from ones synced from
-- `defineTask({ schedule })` ('declarative'); declarative removal is opt-in
-- (`pruneDeclarative: true`) via `ScheduleStore.deleteDeclarativeExcept`.

CREATE TABLE schedules (
	id text PRIMARY KEY,
	task_id text NOT NULL,
	cron text NOT NULL,
	timezone text,
	payload jsonb,
	source text NOT NULL CHECK (source IN ('api', 'declarative')),
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now()
);
