-- minitrigger migration 001: initial schema (Phase 2, docs/SPEC.md §5).
--
-- Schema-agnostic by design: every name below is UNQUALIFIED. The migration runner
-- (src/migrations/migrate.ts) is responsible for making these names resolve inside the
-- caller's configured schema, by running this file inside a transaction after:
--   1. taking `pg_advisory_xact_lock(hashtext(schema), 1)` (decision 16),
--   2. `CREATE SCHEMA IF NOT EXISTS <schema>`,
--   3. `SET LOCAL search_path TO <escaped schema>` -- LOCAL, never session-wide (decision 6's
--      spirit: a session-wide SET would leak into whatever other code later borrows this
--      connection from the pool, including user code if the pool was injected).
-- Do not qualify any identifier in this file with a schema name; do not run this file outside
-- of that runner.
--
-- `runs.attempt` semantics: 1-based *current attempt number* (not a "retries used so far"
-- counter). A fresh run starts at `attempt = 1` before its first execution; the engine
-- increments it only on a failed attempt or a stall recovered by the reconciler (decision 4),
-- never on a plain `waiting` resumption. A final failure does not increment it, so
-- `attempt <= max_attempts` always holds and `status = 'failed'` alone marks a permanent failure.

CREATE TABLE runs (
	id uuid PRIMARY KEY,
	task_id text NOT NULL,
	queue text NOT NULL,
	payload jsonb NOT NULL,
	status text NOT NULL DEFAULT 'queued'
		CHECK (status IN ('queued', 'running', 'waiting', 'completed', 'failed', 'canceled')),
	output jsonb,
	error jsonb,
	-- 1-based current attempt number; see file header.
	attempt int NOT NULL DEFAULT 1,
	max_attempts int NOT NULL,
	execution_id uuid,
	enqueue_seq int NOT NULL DEFAULT 1,
	heartbeat_at timestamptz,
	run_after timestamptz,
	parent_run_id uuid
		CONSTRAINT runs_parent_run_id_fkey REFERENCES runs (id) ON DELETE SET NULL,
	-- References `waitpoints(id)`, added below via ALTER TABLE once `waitpoints` exists: `runs`
	-- and `waitpoints` reference each other (waitpoints.run_id -> runs.id), so one of the two
	-- FKs has to be added after both tables exist.
	parent_waitpoint_id uuid,
	parent_step_name text,
	batch_index int,
	started_at timestamptz,
	finished_at timestamptz,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 80);

CREATE TABLE idempotency_keys (
	task_id text NOT NULL,
	key text NOT NULL,
	run_id uuid NOT NULL
		CONSTRAINT idempotency_keys_run_id_fkey REFERENCES runs (id) ON DELETE CASCADE,
	expires_at timestamptz,
	PRIMARY KEY (task_id, key)
);

CREATE TABLE steps (
	run_id uuid NOT NULL
		CONSTRAINT steps_run_id_fkey REFERENCES runs (id) ON DELETE CASCADE,
	name text NOT NULL,
	seq_index int NOT NULL,
	status text NOT NULL CHECK (status IN ('completed', 'waiting')),
	output jsonb,
	wake_at timestamptz,
	created_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (run_id, name),
	-- seq_index is assigned at call time (decision 3) and must be unique per run; also gives
	-- replay a cheap way to fetch the recorded step at a given position in order.
	CONSTRAINT steps_run_id_seq_index_key UNIQUE (run_id, seq_index)
);

CREATE TABLE waitpoints (
	id uuid PRIMARY KEY,
	run_id uuid NOT NULL
		CONSTRAINT waitpoints_run_id_fkey REFERENCES runs (id) ON DELETE CASCADE,
	step_name text NOT NULL,
	kind text NOT NULL CHECK (kind IN ('token', 'child_run', 'batch')),
	status text NOT NULL DEFAULT 'pending'
		CHECK (status IN ('pending', 'completed', 'timed_out')),
	pending_count int,
	output jsonb,
	expires_at timestamptz,
	created_at timestamptz NOT NULL DEFAULT now(),
	resolved_at timestamptz,
	CONSTRAINT waitpoints_run_id_step_name_key UNIQUE (run_id, step_name)
);

-- Completes the circular FK between `runs` and `waitpoints` (see comment on `runs` above).
ALTER TABLE runs
	ADD CONSTRAINT runs_parent_waitpoint_id_fkey
	FOREIGN KEY (parent_waitpoint_id) REFERENCES waitpoints (id) ON DELETE SET NULL;

CREATE TABLE run_logs (
	id bigserial PRIMARY KEY,
	run_id uuid NOT NULL
		CONSTRAINT run_logs_run_id_fkey REFERENCES runs (id) ON DELETE CASCADE,
	level text NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
	message text NOT NULL,
	data jsonb,
	ts timestamptz NOT NULL DEFAULT now()
);

-- Indexes (Phase 2 list, verbatim). Deliberately NO index on `heartbeat_at` or `updated_at`:
-- the heartbeat write must stay HOT and must not touch any index (decision 6).
CREATE INDEX idx_runs_status_run_after ON runs (status, run_after);
CREATE INDEX idx_runs_running ON runs (id) WHERE status = 'running';
CREATE INDEX idx_runs_finished_at ON runs (finished_at)
	WHERE status IN ('completed', 'failed', 'canceled');
CREATE INDEX idx_runs_task_created ON runs (task_id, created_at DESC, id DESC);
CREATE INDEX idx_runs_parent_run_id ON runs (parent_run_id);
CREATE INDEX idx_runs_parent_waitpoint_batch ON runs (parent_waitpoint_id, batch_index);
CREATE INDEX idx_waitpoints_pending ON waitpoints (run_id) WHERE status = 'pending';
CREATE INDEX idx_waitpoints_expires_pending ON waitpoints (expires_at) WHERE status = 'pending';
CREATE INDEX idx_run_logs_run_id ON run_logs (run_id, id);
CREATE INDEX idx_idempotency_keys_run_id ON idempotency_keys (run_id);
