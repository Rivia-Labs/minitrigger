# minitrigger

Durable tasks and workflows for Node.js, built on **BullMQ + Redis + Postgres**, installed as a
regular dependency in your own project. No platform, no deploy step, no dashboard by default, no
multi-tenancy — you point it at a Postgres and a Redis you already run, and define tasks in your
own codebase.

## What it is

- Tasks defined in code with `defineTask`, started with `trigger()`, executed by workers running
  inside your own process(es).
- Steps (`ctx.step.run`) that are memoized and replayed after a crash or a suspension, so a task
  function only re-does the work it hasn't already done.
- Durable waits (`step.sleep`, `step.waitForToken`) that don't occupy a worker slot while pending.
- Retries with backoff, per-queue concurrency and rate limiting.
- Subtasks (`step.trigger`, `step.triggerAndWait`, `step.batchTriggerAndWait`).
- Cron schedules, idempotency, cancellation, per-attempt timeouts, run logs and status queries.

**Postgres is the source of truth** for every run, step, waitpoint, idempotency key and log; BullMQ
is only used as transport (delivery, delay, concurrency, rate limiting). If Redis is wiped, no run
is lost — see [Operation](#operation).

### When to use it

- You have a personal or small-team project with tasks that must survive a crash or a redeploy
  mid-flight: scraping + LLM calls + human approval + notification, for example.
- You already run Postgres and Redis (or don't mind adding them) and want durability without
  standing up a separate workflow platform.
- You're comfortable with the task function being **re-executed from the top on every resume**
  (see [Replay with memoization](#replay-with-memoization)) — code outside `ctx.step.*` must be
  deterministic.

### Non-goals

- No hosted platform, no web dashboard by default (an optional dashboard is a possible future
  phase), no billing, no multi-tenancy, no authentication layer.
- No deployment of user code — you run your own process(es).
- No process-level checkpointing (no CRIU-style snapshotting); durability comes from replay +
  memoization, not from freezing the process.
- Postgres only; no other database backend is planned.

## Install

```bash
pnpm add minitrigger bullmq ioredis pg
pnpm add -D @types/pg   # optional, only needed if you use TypeScript
```

`bullmq`, `ioredis` and `pg` are **peer dependencies** — you control their exact versions.
Compatible ranges:

| Package | Range |
|---|---|
| `bullmq` | `^6.3.0` |
| `ioredis` | `^5.11.1` (not 6.x — BullMQ 6.3.x is tested against 5.11.1; the 6.x RESP3 default isn't handled by its adapter) |
| `pg` | `^8.13.0` |
| `@types/pg` | `^8.11.0` (optional peer — only needed because the public types reference `pg.Pool`) |

Requires **Node >= 24**.

<!-- TODO(phase 10): verify against implementation -->
Installing directly from GitHub (`pnpm add github:you/minitrigger`) is expected to work since the
package ships a `prepare` script that builds `dist`, but pnpm's handling of `prepare` scripts for
git dependencies needs to be validated before relying on it; a `.tgz` published to GitHub Releases
is the documented fallback.

## Infrastructure

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: minitrigger
      POSTGRES_PASSWORD: minitrigger
      POSTGRES_DB: minitrigger
    ports:
      - "5432:5432"
  redis:
    image: redis:7-alpine
    # BullMQ requires noeviction: it must never evict keys it's relying on for
    # delayed/active job state, or jobs silently disappear.
    command: ["redis-server", "--maxmemory-policy", "noeviction"]
    ports:
      - "6379:6379"
```

`noeviction` matters because BullMQ stores job state (including delayed jobs, i.e. everything
`step.sleep`/retry backoff/scheduled runs rely on) as ordinary Redis keys. Any other eviction
policy lets Redis silently drop that state under memory pressure; `minitrigger` checks this at
`engine.start()` and warns (or throws, if it can confirm the policy is wrong — see decision 22 in
`docs/SPEC.md`) if it isn't set.

## Quick start

```ts
import { createEngine, defineTask } from "minitrigger";

const sendWelcomeEmail = defineTask({
	id: "send-welcome-email",
	run: async (payload: { userId: string }) => {
		await sendEmail(payload.userId);
	},
});

const engine = createEngine({
	postgres: process.env.DATABASE_URL!,
	redis: process.env.REDIS_URL!,
	tasks: [sendWelcomeEmail],
});

await engine.migrate();
await engine.start();

const handle = await engine.trigger(sendWelcomeEmail, { userId: "u_123" });
await engine.runs.get(handle.id);

await engine.stop();
```

## Concepts

### Runs & statuses

Triggering a task creates a **run**: a row in Postgres with an id, a status, the payload, the
current attempt, and (once finished) an output or error. Statuses: `queued`, `running`, `waiting`
(suspended on a step, a sleep, a token, or a subtask), `completed`, `failed`, `canceled`.

### Replay with memoization

The task function is **re-executed from the beginning** on every resume (after a crash, a
suspension, or a retry). Steps that already completed return their saved output instead of
re-running; code that isn't wrapped in `ctx.step.*` runs again every time. This means:

- Anything non-deterministic (random values, current time, network calls, mutation of outside
  state) must happen **inside** a step.
- A step is identified by its **name**, in call order (`seq_index`). If the name recorded on
  replay doesn't match what was saved, the run fails with `NonDeterminismError`.

### Steps

- **`ctx.step.run(name, fn)`** — runs `fn` once, memoizes its (JSON-serializable) return value.
  Subsequent replays return the saved value without calling `fn` again.
- **`ctx.step.sleep(name, duration)`** — suspends the run until `duration` has elapsed, without
  occupying a worker slot. `duration` is a `Duration`: a number of milliseconds, or a string like
  `"5m"`, `"2d"`, `"1.5h"` (units: `ms`, `s`, `m`, `h`, `d`, `w`).
- **`ctx.step.createToken(name, { timeout? })`** / **`ctx.step.waitForToken(tokenOrName)`** — a
  durable "wait for an external signal" primitive. Create a token, hand its `id` to whatever will
  complete it (a webhook handler, a human clicking a link), then wait for it. Completed externally
  via `engine.tokens.complete(tokenId, output)`.
- **`ctx.step.trigger(name, task, payload, opts?)`** — starts a subtask without waiting for it.
- **`ctx.step.triggerAndWait(name, task, payload, { timeout? })`** — starts a subtask and suspends
  until it finishes (or times out).
- **`ctx.step.batchTriggerAndWait(name, task, payloads[], { timeout? })`** — starts many subtasks
  and suspends until all of them finish (or time out); the parent resumes exactly once.

`waitForToken`, `triggerAndWait`, and each item of `batchTriggerAndWait` return a `WaitResult<T>`
instead of throwing on failure/timeout:

```ts
type WaitResult<T> =
	| { ok: true; output: T }
	| { ok: false; error: SerializedError }; // error.name is "Timeout" or "Canceled" on those paths
```

so a subtask failing, timing out, or being canceled never throws into the parent — you always
check `.ok`.

### Idempotency

```ts
await engine.trigger(myTask, payload, { idempotencyKey: "invoice-123" });
```

The same `(task, key)` pair, while the key is alive, always returns the **same** run instead of
creating a new one. Keys default to a TTL of **7 days** (`idempotencyKeyTtl`), which must not
exceed `retention.days` (a key can't outlive the run it points to). Keys starting with `__` are
**reserved** for internal use (child-run and schedule bookkeeping) and are rejected if a user
supplies one. `engine.runs.replay(runId)` starts a **new** run of the same task/payload — it does
**not** reuse the original idempotency key.

### Queues

```ts
defineTask({
	id: "summarize",
	queue: { name: "llm", concurrency: 2, rateLimit: { max: 10, durationMs: 60_000 } },
	run: async (payload: { url: string }) => { /* ... */ },
});
```

Queue settings are shared per queue **name**: several tasks can reference the same queue, but only
one of them may declare `concurrency`/`globalConcurrency`/`rateLimit` for it — a second, differing
declaration for the same name fails engine creation with a clear error. A task with no `queue`
belongs to the `"default"` queue (concurrency 10 unless something else declares it).

### Retries & backoff

Retries are controlled by the engine, not by BullMQ (BullMQ jobs always use `attempts: 1`
internally). On failure, the engine increments `attempt` in Postgres and re-enqueues with a
backoff delay:

```ts
retry: {
	maxAttempts: 3,
	backoff: { type: "exponential", baseMs: 1_000, maxMs: 60_000, jitter: true }, // jitter defaults to true
}
```

`type` is `"exponential"` (`baseMs * 2^(attempt - 1)`, capped at `maxMs`) or `"fixed"` (`baseMs`,
capped at `maxMs`). Throwing `AbortTaskRunError` from inside a task (or a `step.run` callback)
fails the run permanently — **no retry is scheduled**, regardless of `maxAttempts`.

### `maxDurationMs`

```ts
defineTask({ id: "x", maxDurationMs: 5 * 60_000, run: async (_, ctx) => { /* ... */ } });
```

A per-**attempt** timeout, enforced via `ctx.signal`. The timer does **not** run while the run is
suspended (waiting on a sleep, token, or subtask) — only while an attempt is actively executing.

### Cancellation

`engine.runs.cancel(runId)` cancels the run and, **recursively**, its pending descendants (subtasks
started via `step.trigger`/`triggerAndWait`/`batchTriggerAndWait` that haven't finished). A
canceled run's `ctx.signal` fires; code that ignores the signal and keeps running can no longer
write anything (see [Operation](#operation) — ownership is checked on every step write).

<!-- TODO(phase 6): verify against implementation -->
Canceling a run currently aborts local executions of that process only; cross-process cancellation
(so a run being executed by a *different* process also aborts promptly, via Redis pub/sub) is part
of the still-in-progress resilience work and should be re-verified once wired up. Until then,
canceling a run owned by another process still marks it `canceled` in Postgres and prevents any
further writes (ownership check), but that other process's `ctx.signal` may not fire until it next
touches the database.

### Serialization rules

Payloads, step outputs, token outputs, and task outputs must be plain JSON: no `Date`, `Map`,
`Set`, `RegExp`, functions, `bigint`, or class instances with methods. This is enforced both at
compile time (the `Serializable<T>` type, which works with both `interface` and `type`
declarations) and at runtime. Limits:

| What | Default limit |
|---|---|
| Trigger payload / token output / **task output** | `maxPayloadBytes` — 1 MB |
| `step.run` output | `maxStepOutputBytes` — 256 KB |
| One log entry's `data` | `maxLogEntryBytes` — 16 KB (truncated with a marker past this) |

A task/step function that returns `void` (or `undefined`) has that output stored as JSON `null`.

## Errors reference

| Error | When |
|---|---|
| `ConfigError` | Invalid engine/task configuration (durations, identifiers, queue conflicts, retry/backoff options, bad env). |
| `SerializationError` | A payload/output/log value failed the JSON-serializability check or exceeded its size limit. |
| `AbortTaskRunError` | Thrown by user code to fail the run permanently, with no retry. |
| `NonDeterminismError` | The step sequence observed on replay diverged from what was recorded (wrong name or kind at a given position). |
| `DuplicateStepError` | The same step name was used twice within one execution. |
| `NestedStepError` | `ctx.step.*` was called from inside a `step.run` callback (nested steps aren't supported). |
| `ExecutionLostError` | The executing process lost ownership of the run (canceled, timed out, stolen by the reconciler, handed back on shutdown) — thrown into in-flight step calls; no further writes happen. |
| `TaskNotRegisteredError` | A task id was referenced (e.g. via `step.trigger`) that isn't registered on this engine. |

All of the above extend `MinitriggerError`.

## API reference

### `createEngine(config)`

See the [config options table](#config-options) below for every field.

### Engine methods

```ts
interface Engine {
	migrate(): Promise<MigrateResult>;         // applies pending migrations; safe to call concurrently
	start(options?: { queues?: string[] }): Promise<void>; // starts workers; omit to start none
	stop(): Promise<void>;                      // graceful shutdown; idempotent
	trigger<T extends AnyTask>(task: T, payload?, options?: TriggerOptions): Promise<RunHandle<TaskOutput<T>>>;
	runs: {
		get<O>(run: string | RunHandle<O>): Promise<Run<O> | null>;
		list(options?: ListRunsOptions): Promise<RunList>;
		cancel(run: string | RunHandle<unknown>): Promise<"canceled" | "already_finished" | "not_found">;
		replay<O>(run: string | RunHandle<O>): Promise<RunHandle<O>>; // new run, same payload, no idempotency key
	};
	tokens: {
		complete(tokenId: string, output?: unknown): Promise<CompleteTokenOutcome>;
	};
}
```

`TriggerOptions`: `{ idempotencyKey?: string; idempotencyKeyTtl?: Duration; delay?: Duration }`.

An engine that never calls `start()` can still `trigger()` and query — no workers, no queue
consumption (decision 19 in `docs/SPEC.md`).

<!-- TODO(phase 7): verify against implementation -->
The SPEC's target surface for run introspection also includes `engine.runs.logs(runId)`,
`engine.runs.steps(runId)`, `engine.runs.subscribe(runId)` (an `AsyncIterable` of run updates via
Postgres + Redis pub/sub), and `engine.events.on(...)` (local event emitter). These are part of
the still-pending logs/events phase and aren't on `Engine` yet — re-check this section once that
lands.

```ts
// Target shape (not yet implemented):
await engine.runs.logs(runId);
await engine.runs.steps(runId);
for await (const update of engine.runs.subscribe(runId)) { /* ... */ }
engine.events.on("run:completed", handler);
```

<!-- TODO(phase 8): verify against implementation -->
Declarative and imperative cron schedules are also part of the target API but not yet exposed on
`Engine`:

```ts
// Target shape (not yet implemented):
await engine.schedules.upsert({
	id: "daily",
	task: myTask,
	cron: "0 9 * * *",
	timezone: "America/Belem",
	payload: { url: "..." },
});
await engine.schedules.delete("daily");
await engine.schedules.list();
```

`defineTask` already accepts a declarative `schedule: { cron, timezone, payload? }` option meant to
be synced by `engine.start()`; by default nothing declared this way is ever deleted automatically —
removing a schedule from code requires `schedules: { pruneDeclarative: true }` on the engine
config once that option exists.

### `ctx` (second argument of a task's `run`)

```ts
type TaskContext = {
	step: StepApi;
	logger: TaskLogger;               // debug/info/warn/error(message, data?)
	signal: AbortSignal;               // cancellation, maxDurationMs, lost ownership, or shutdown
	run: { id: string; taskId: string; attempt: number; createdAt: Date };
	stream(name: string, source: AsyncIterable<Json | string>): Promise<void>;
};
```

<!-- TODO(phase 7): verify against implementation -->
`ctx.logger` currently exists as a type surface only; batched, size-limited writes to a `run_logs`
table (flushed every 500 ms / 50 entries, or at run end) are part of the pending logs phase.
`ctx.stream` (publish an async sequence via Redis Streams, readable with
`engine.runs.readStream(runId, name)`) is likewise a type surface only until that phase lands.

### `StepApi`

```ts
interface StepApi {
	run<T>(name: string, fn: () => Promise<T> | T): Promise<T>;
	sleep(name: string, duration: Duration): Promise<void>;
	createToken(name: string, opts?: { timeout?: Duration }): Promise<{ id: string; name: string }>;
	waitForToken<T = Json>(tokenOrName: TokenHandle | string): Promise<WaitResult<T>>;
	trigger<T extends AnyTask>(name: string, task: T, payload?, opts?: { delay?: Duration }): Promise<RunHandle<TaskOutput<T>>>;
	triggerAndWait<T extends AnyTask>(name: string, task: T, payload?, opts?: { timeout?: Duration }): Promise<WaitResult<TaskOutput<T>>>;
	batchTriggerAndWait<T extends AnyTask>(name: string, task: T, payloads: TaskPayload<T>[], opts?: { timeout?: Duration }): Promise<WaitResult<TaskOutput<T>>[]>;
}
```

### Config options

| Option | Default | Notes |
|---|---|---|
| `postgres` | — | Connection string, or `{ pool }` with an existing `pg.Pool` (never closed by the engine). |
| `redis` | — | Connection string, or `{ connection }` with an existing `ioredis` instance (only used for non-blocking producers; workers/subscribers always get a `duplicate()`). |
| `schema` | `"minitrigger"` | Postgres schema. Must match `^[a-z_][a-z0-9_]{0,62}$`. |
| `prefix` | `"minitrigger"` | BullMQ key prefix. Same pattern as `schema`. |
| `tasks` | — | Array of tasks from `defineTask`. |
| `pool.max` | `10` | Only used when the engine creates its own pool (string `postgres`). See [pool sizing](#pool-sizing). |
| `heartbeatIntervalMs` | `10 000` | How often a running execution updates `heartbeat_at`. Must be smaller than `stallTimeoutMs`. |
| `heartbeatTimeoutMs` | `2 000` | `SET LOCAL statement_timeout` applied to each heartbeat query/transaction. |
| `stallTimeoutMs` | `60 000` | A `running` run whose heartbeat is older than this is considered stalled by the reconciler. |
| `shutdownTimeoutMs` | `30 000` | How long `stop()` waits for active runs before handing them back. |
| `idempotencyKeyTtl` | `"7d"` | Default TTL for a user idempotency key; must not exceed `retention.days`. |
| `retention.days` | `30` | Finished runs older than this are deleted (only once unparented or their parent is finished). |
| `limits.maxPayloadBytes` | `1 MB` | Trigger payloads, token outputs, task outputs. |
| `limits.maxStepOutputBytes` | `256 KB` | `step.run` outputs. |
| `limits.maxLogEntryBytes` | `16 KB` | One `ctx.logger` entry's `data`. |
| `limits.maxInlineStepOutputBytes` | `16 MB` | Threshold above which a run's step outputs are loaded lazily in batches instead of in the single startup query. |
| `systemConcurrency` | `5` | Concurrency of the internal `${prefix}-system` worker (ticks, expirations, retention). |
| `logger` | console, warn/error only | `{ debug, info, warn, error }`, each `(fields, message) => void` (pino- and console-call-compatible). |

`Duration` fields accept either a number of milliseconds or a string like `"5m"`, `"2d"`, `"1.5h"`
(`ms`, `s`, `m`, `h`, `d`, `w`).

## Operation

- **Postgres is the source of truth.** `trigger()` does two non-atomic writes: insert the run in
  Postgres, then enqueue it in Redis. If the process dies between the two, the run exists but was
  never enqueued. This is recovered by the **reconciler**, which periodically re-enqueues
  `queued`/`waiting` runs whose `run_after` is in the past — the recovery latency for a lost
  enqueue is bounded by the reconciler's interval, not indefinite.
- **Heartbeat and stall.** A running execution updates `heartbeat_at` every `heartbeatIntervalMs`.
  The reconciler treats a `running` run whose heartbeat is older than `stallTimeoutMs` as stalled
  and recovers it (requeue with an incremented attempt, or fail definitively once attempts are
  exhausted). Because the heartbeat runs on the same event loop as your task code, **synchronous
  code that blocks the event loop for longer than `stallTimeoutMs` will itself cause a stall** —
  move CPU-bound work inside a step to a `worker_threads` worker instead of running it inline.
- **Retention.** Finished runs (`completed`/`failed`/`canceled`) older than `retention.days` are
  deleted in batches, only once they have no parent or their parent has also finished.
- **Graceful shutdown.** `engine.stop()` stops accepting new jobs and waits for active runs up to
  `shutdownTimeoutMs`. A run that's still active past that timeout is handed back to `queued`
  (ownership cleared, **no attempt increment**) and re-enqueued — this is an **at-least-once**
  guarantee: if your task's own logic already had a side effect before the timeout, that side
  effect may run again on retry unless it's inside a memoized step.
- **Pool sizing.** If you inject your own `pg.Pool`, make sure `pool.max >= total worker
  concurrency + 5` (the engine's own bookkeeping queries need headroom beyond what your workers
  consume). A separate, dedicated pool for the library is recommended over sharing your
  application's pool.
- **Advisory locks.** Migrations use `pg_advisory_xact_lock` inside a transaction; the leader
  (reconciler/retention owner) uses `pg_try_advisory_lock` on a dedicated client kept outside the
  pool for as long as it holds the lock — never on a connection borrowed from a pool, since a
  pooled connection can be silently recycled while the lock still appears held.
- **Two engines, one process.** Two `createEngine()` instances with different `schema`/`prefix`
  are expected to coexist in the same process without interfering with each other (no global
  state).

<!-- TODO(phase 6): verify against implementation -->
`leader.ts`, `reconciler.ts`, and `retention.ts` exist in the codebase and implement the behavior
described above, but as of this writing they are **not yet wired into `engine.start()`/`stop()`**
— the reconciler loop, leader election, and retention sweeps described in this section are the
target behavior for the in-progress resilience phase, not yet something `createEngine` runs for
you automatically. Re-verify this section once that phase completes.

## Known limitations

- **Lost-enqueue recovery latency.** A `trigger()` whose Redis enqueue is lost is only picked back
  up when the reconciler next runs — there is a real (bounded, but non-zero) delay, not instant
  recovery.
- **Long synchronous code stalls the run.** See [Operation](#operation) — anything that blocks the
  event loop past `stallTimeoutMs` looks identical to a crashed process from the reconciler's point
  of view.
- **Missed cron ticks are not backfilled.** If every process is down when a scheduled tick would
  have fired, that iteration is simply skipped — it does not run late, and does not accumulate.
- **Shared connection pool risk.** Injecting your application's own `pg.Pool` means the library's
  queries compete with your application's for connections; undersizing `pool.max` can cause
  intermittent heartbeat/acquisition failures under load.
- **Concurrent async branches calling steps after an `await` can be non-deterministic.** Calling
  `ctx.step.*` from inside concurrent branches (e.g. `Promise.all(items.map(async i => { await
  ctx.step.run(a); await ctx.step.run(b); }))`) records steps in **completion** order, which can
  vary between the original run and a replay, risking `NonDeterminismError`. Prefer calling steps
  before any `await` inside a branch, or run them sequentially. See
  [Golden rules](#golden-rules).
- **Generic tasks need a cast.** `defineTask` infers `Payload`/`Output` from `run`; a genuinely
  generic task (`<T extends Json>`) can't have its type parameter inferred from `trigger()` alone
  and needs an explicit cast at the call site.
- **A task declaring `payload: unknown` is treated as having no payload.** `TaskPayload<T>` maps an
  inferred `unknown` payload type to "no payload" (so `trigger(task)` is callable without a second
  argument); this applies whether the payload was truly omitted or explicitly typed as `unknown`.
  Use a named type (even a loose one) if you need `trigger()` to require an argument.

## Golden rules

1. **Nothing non-deterministic outside a step.** Random values, `Date.now()`, network calls, env
   reads that can change — all of it belongs inside `ctx.step.run`, not in the bare task function
   body, which re-executes on every replay.
2. **Step names must be unique and stable within a run.** Don't derive a step name from something
   that can change between replays; when naming steps in a loop, include the index
   (`` `send-${i}` ``) so identical-looking iterations don't collide.
3. **Steps should be idempotent where possible.** A step's side effect might, in rare recovery
   paths, be attempted more than once before its completion is durably recorded.
4. **Keep step outputs small.** They're capped (`maxStepOutputBytes`, 256 KB by default) and are
   read back on every replay — store references (an id, a URL) rather than large blobs.
5. **Don't reorder or remove steps while runs are waiting.** A deployed change that shifts step
   order or names will break replay for any run suspended mid-flight (`NonDeterminismError`).
6. **Don't swallow exceptions around `step.*` calls.** Internally, suspension is signaled by
   throwing; a `try/catch` around a `step.sleep`/`step.waitForToken` call that catches everything
   won't break replay (suspension is authoritative), but catching and discarding a **real** step
   failure defeats retries — only catch what you intend to handle.
7. **No nested steps.** Calling `ctx.step.*` from inside a `step.run` callback throws
   `NestedStepError` — steps don't nest.
8. **Move CPU-heavy work off the main thread.** Long synchronous work inside a step blocks the
   heartbeat and can look like a stall to the reconciler; use `worker_threads` for CPU-bound work.
