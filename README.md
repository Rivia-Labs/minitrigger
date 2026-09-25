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

`minitrigger` isn't published to a registry. Install a `.tgz` from GitHub Releases (recommended) or
straight from GitHub (works, but needs one opt-in on your side). Either way, `bullmq`, `ioredis` and
`pg` are **peer dependencies** — never bundled — so install them alongside `minitrigger`.

### Recommended: `.tgz` from GitHub Releases

The repository (`igorabreu29/minitrigger`) is **private**, so release assets need GitHub
authentication. Download the tarball with the GitHub CLI (logged in with access to the repo) and add
it from disk:

```bash
gh release download v0.1.0 -R igorabreu29/minitrigger -p "minitrigger-*.tgz"
pnpm add ./minitrigger-0.1.0.tgz bullmq ioredis pg
pnpm add -D @types/pg   # optional, only needed if you use TypeScript
```

Commit the `.tgz` (e.g. under `vendor/`) or fetch it in CI with a token that can read the repo.

A release tarball already contains the built `dist/` (`pnpm pack`'s `files: ["dist"]`), so nothing
builds on install: no pnpm `allowBuilds` opt-in, and no fresh, possibly-differently-resolved
`ioredis`/`msgpackr-extract` pulled in transitively just to run a build script. It's also pinned and
reproducible — a fixed artifact per version, not "whatever `HEAD` currently contains." `pnpm
check:pack` (see [Benchmark](#benchmark)) exercises exactly this pack → install → import path in CI.

### Fallback: install straight from GitHub

Needs git access to the private repo (SSH key or a credential helper for HTTPS).

```bash
pnpm add github:igorabreu29/minitrigger#v0.1.0 bullmq ioredis pg
```

`dist/` is `.gitignore`d and only produced by minitrigger's own `prepare` script, and pnpm 10+
blocks a git dependency's lifecycle scripts by default — so a plain `pnpm add github:...` fails with
`ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED` until you opt in, in **your own** `pnpm-workspace.yaml`:

```yaml
# your project's pnpm-workspace.yaml
allowBuilds:
  minitrigger@github.com/igorabreu29/minitrigger#<resolved-commit>: true
  msgpackr-extract: true # ioredis's optional native accelerator; blocked by the same pnpm mechanism
```

The `minitrigger@...` key must match the exact resolved git spec pnpm prints in its own
`ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED` error (package name + normalized git URL + resolved commit) —
copy it from that error rather than guessing, and expect to update it whenever you bump to a newer
commit/tag. Full reproduction and reasoning: `docs/research/github-install.md`.

### Peer dependencies & requirements

| Package | Range |
|---|---|
| `bullmq` | `^6.3.0` |
| `ioredis` | `^5.11.1` (not 6.x — BullMQ 6.3.x is tested against 5.11.1; the 6.x RESP3 default isn't handled by its adapter) |
| `pg` | `^8.13.0` |
| `@types/pg` | `^8.11.0` (optional peer — only needed because the public types reference `pg.Pool`) |

Also requires **Node >= 24**, **Postgres >= 13** (checked by `checkEnvironment`/`engine.start()`),
and **Redis with `maxmemory-policy noeviction`** — see [Infrastructure](#infrastructure) below.

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
- Step names must not contain `#` or `:` (both reserved: `#` for internal step names such as
  `${name}#wait`, `:` for internal child-run keys); an invalid name fails the run without retry.

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
capped at `maxMs`). A task with no `retry` option defaults to `{ maxAttempts: 3, backoff: {
type: "exponential", baseMs: 1_000, maxMs: 60_000 } }`. Throwing `AbortTaskRunError` from inside a
task (or a `step.run` callback) fails the run permanently — **no retry is scheduled**, regardless
of `maxAttempts`.

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

Cancellation works across processes: the canceled run ids are published on Redis pub/sub
(`${prefix}:cancel`), and every started engine sharing the same `prefix` aborts its local
execution of those runs (`ctx.signal` fires with an `AbortError`). An engine that only triggers
(never called `start()`) can cancel too: it publishes, it just doesn't subscribe. Pub/sub is only
the fast path — if a message is lost, the run is still `canceled` in Postgres and the ownership
check rejects every further write of the old execution (its next heartbeat also aborts it).

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
| `TaskNotRegisteredError` | `engine.trigger` / a schedule referenced a task that isn't registered on this engine, or a worker acquired a run of a task it doesn't know (retryable, e.g. during a rolling deploy). `step.trigger*` accepts an unregistered child task: another process that registers it runs the child. |

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
		logs(run, options?: { afterId?: string | null; limit?: number }): Promise<RunLogPage>;
		steps(run): Promise<RunStep[]>;
		subscribe(run, options?: { signal?: AbortSignal }): AsyncIterable<RunStatusUpdate>;
		readStream<T = Json | string>(run, name: string, options?: { signal?: AbortSignal }): AsyncIterable<T>;
	};
	tokens: {
		complete(tokenId: string, output?: unknown): Promise<CompleteTokenOutcome>;
	};
	schedules: {
		upsert<T extends AnyTask>(input: ScheduleUpsertInput<T>): Promise<Schedule>;
		delete(id: string): Promise<boolean>;
		list(): Promise<Schedule[]>;
	};
	events: {
		on(event: RunEventName, handler: (event: RunEvent) => void | Promise<void>): () => void;
	};
}
```

`payload` is required unless the task's `run` takes none (then it's optional and must be omitted
or `undefined`) — the exact type is `PayloadArgs<TaskPayload<T>, [options?: TriggerOptions]>`, not
the simplified `payload?` shown above.

`TriggerOptions`: `{ idempotencyKey?: string; idempotencyKeyTtl?: Duration; delay?: Duration }`.

An engine that never calls `start()` can still `trigger()` and query — no workers, no queue
consumption (decision 19 in `docs/SPEC.md`).

### Run introspection, events and streams

```ts
// Logs written with ctx.logger, oldest first; page with afterId (null = from the start).
const page = await engine.runs.logs(runId, { limit: 100 });          // { items, nextAfterId }
await engine.runs.logs(runId, { afterId: page.nextAfterId });

// Steps in call order: { name, seq, kind, status, output, wakeAt, createdAt }.
// For waits (`wait`, `trigger_wait`, `batch_wait`) `output` is only `{ waitpointId }`;
// `trigger` and `token` steps hold `{ id }`.
await engine.runs.steps(runId);

// Filters: taskId, status (one or an array), createdFrom (inclusive) / createdTo (exclusive),
// cursor (`nextCursor` of the previous page), limit (default 20, max 100). Newest first.
await engine.runs.list({ status: ["failed", "canceled"], createdFrom: yesterday, limit: 50 });

// Status updates of one run, across processes (Postgres + Redis pub/sub).
for await (const update of engine.runs.subscribe(runId, { signal })) {
	console.log(update.status, update.updatedAt); // ends after completed/failed/canceled
}

// Local events of THIS process; returns an unsubscribe function.
const off = engine.events.on("run:completed", event => { /* { name, runId, taskId, status, attempt, error, updatedAt } */ });

// Chunks written by ctx.stream("answer", ...) in the run, live.
for await (const token of engine.runs.readStream<string>(runId, "answer")) process.stdout.write(token);
```

- `runs.subscribe` works on an engine that never called `start()` (it opens its own subscriber
  connection, closed by `stop()`). Order: it subscribes to pub/sub first, then reads the run from
  Postgres and emits that status, then emits only changes newer than the last one emitted, and
  ends right after a terminal status (at once if the run is already finished; without emitting
  anything if the run does not exist). `break`, `options.signal` and `engine.stop()` end it too.
  Pub/sub is at-most-once, so after 15 s without messages it re-reads the run from Postgres.
- `engine.events.on(name, handler)` — names: `run:queued`, `run:started`, `run:waiting`,
  `run:completed`, `run:failed`, `run:canceled`, `run:retrying`. Only transitions made by this
  process are emitted (the process that triggered, acquired, suspended, finished, retried or
  canceled the run; stall recoveries on the leader). Handlers run after the commit, off the run's
  critical path; a throwing or rejecting handler is logged with `logger.error`.
- `runs.readStream` reads from the start of the stream (so a late reader gets every chunk still
  kept) and ends at the writer's end marker. When the stream ended with an error (the `source`
  given to `ctx.stream` threw, or the attempt was aborted mid-stream), the iteration **throws** a
  `MinitriggerError` (with the original `Name: message`) after yielding the chunks written before
  it. Each reader uses its own Redis connection, closed when the iteration ends (or on `stop()`).
- Readers stop at the **first** end marker. If a run retries after a stream error, the next
  attempt's `ctx.stream` with the same name appends to the same stream key, after that marker:
  a reader starting from the beginning never sees the retry's chunks. Use a per-attempt name
  (e.g. `` `answer-${ctx.run.attempt}` ``) when a stream must survive retries.

### Schedules (cron)

```ts
// Imperative: source "api" in Postgres, key by any id you choose.
await engine.schedules.upsert({
	id: "daily",
	task: myTask,
	cron: "0 9 * * *",      // 5-7 whitespace-separated fields; a 6th "seconds" field is allowed
	timezone: "America/Belem", // optional IANA zone; omitted = BullMQ's default (UTC-based)
	payload: { url: "..." },   // required (typed as myTask's payload) unless myTask takes none
});
await engine.schedules.delete("daily"); // true if a schedule with this id existed
await engine.schedules.list();          // every schedule (api + declarative), by id

// Declarative: synced by every engine.start() (source "declarative" in Postgres, id = task id).
export const dailyDigest = defineTask({
	id: "daily-digest",
	schedule: { cron: "0 9 * * *", timezone: "America/Belem", payload: { url: "..." } },
	run: async (payload: { url: string }) => { /* ... */ },
});
```

Postgres is the source of truth for `cron`/`timezone`/`payload` (decision 8): `engine.schedules.*`
and the declarative sync write it first, then sync the BullMQ job scheduler on the internal
`${prefix}-system` queue; a BullMQ failure right after the Postgres write is only logged — the
leader's reconciler tick and the schedule's own next fire both self-heal it (a schedule missing its
BullMQ scheduler is re-upserted, an orphaned BullMQ scheduler with no matching Postgres row is
removed, a scheduler whose live cron/timezone drifted from Postgres is re-upserted). Every tick
reads `cron`/`timezone`/`payload` fresh from Postgres — never from the BullMQ job's own copy — and
triggers the task with idempotency key `` `__schedule:${id}:${scheduledAtMs}` `` (the tick's
logical scheduled time, not `Date.now()`), so two processes' `-system` workers racing the same tick
(or a BullMQ-level job redelivery) create exactly one run.

**Ticks missed while every process is down are not recovered.** A BullMQ job scheduler only ever
keeps one pending "next iteration" job; if no process is running to process it across several
intervals, only the single next occurrence after the time a process comes back gets scheduled —
every occurrence in between is silently skipped (`docs/research/api-notes.md` §1.10).

By default nothing declared with `defineTask({ schedule })` is ever deleted automatically, even
after removing it from code (decision 21) — set `schedules: { pruneDeclarative: true }` in the
engine config to also delete (Postgres row + BullMQ scheduler) a declarative schedule no task
declares anymore on the next `start()`. This never touches `"api"` schedules created with
`engine.schedules.upsert()`.

**`pruneDeclarative` footgun: it looks at every task registered on *this* engine.** If different
processes start engines with different (heterogeneous) task registries against the same schema —
e.g. a rolling deploy where the old and new versions briefly run side by side, or two services
intentionally sharing one schema but each only registering some of the tasks — a `start()` with
`pruneDeclarative: true` on a process whose registry doesn't include a given task will treat that
task's declarative schedule as "no longer declared" and delete it, even though another process
still declares (and needs) it. Prefer `pruneDeclarative: true` only on a single process/deploy that
is guaranteed to have the full, current set of scheduled tasks registered; otherwise leave it off
and delete stale schedules explicitly. A schedule deleted from Postgres this way still leaves its
BullMQ job scheduler behind if the removal races another process, but the reconciler cleans up that
kind of orphan on its own (no action needed).

**A schedule id can't switch source underneath itself.** `engine.schedules.upsert()` (source
`"api"`) and `defineTask({ schedule })` (source `"declarative"`, keyed by the task's own id) never
overwrite each other's row for the same id: an `upsert()` targeting an id already owned by a
declarative schedule throws `ConfigError` (rename the schedule, or drop the `schedule` option from
that task's `defineTask()` if it should be API-managed instead); the declarative sync, symmetrically,
skips (logs an error naming the task, leaves the existing `"api"` row untouched) instead of taking
over an id an `upsert()` call already owns — every other task's sync and `pruneDeclarative` still run.

**A tick whose task isn't registered in this process retries.** If `processScheduleTick` can't
resolve `schedule.taskId` in this process's registry (e.g. mid rolling-deploy), the `schedule-tick`
job fails instead of being silently dropped — its BullMQ job scheduler template gives it
`attempts: 5` with exponential backoff (the one exception to the "run jobs use `attempts: 1`"
rule: it's safe here because the `__schedule:${id}:${scheduledAtMs}` idempotency key makes a retry
that lands on a process that already succeeded a no-op), so another process (or this one, once the
deploy finishes) gets a chance to pick up the same tick before it's lost for good.

**`prefix` must be unique per schema.** Every Redis structure — the task queues, the internal
`${prefix}-system` queue (schedule ticks, waitpoint expirations, retention), pub/sub channels and
streams — is keyed by `prefix` alone, not by `schema`. Two engines with the same `prefix` and
different `schema`s would consume each other's jobs: a run job acquired against the wrong schema
is lost until the reconciler re-enqueues it, and a schedule tick processed by the other schema's
engine used to remove the schedule's BullMQ scheduler. Sharing a prefix is therefore refused: the
first `engine.start()` records its schema under the Redis key `${prefix}:owner-schema`, and a
`start()` with another schema on that prefix throws a `ConfigError` (delete that key only when the
schema that owned it was retired). Two further guards remain for deployments that bypass the check
(e.g. engines started before it existed): each job scheduler is tagged with the creating engine's
schema, the reconciler never deletes a scheduler tagged with another schema, and a `schedule-tick`
tagged with another schema is refused (logged as an error, retried by BullMQ, nothing created or
removed). They limit the damage; they don't make a shared prefix work.

### `ctx` (second argument of a task's `run`)

```ts
type TaskContext = {
	step: StepApi;
	logger: TaskLogger;               // debug/info/warn/error(message, data?)
	signal: AbortSignal;               // cancellation, maxDurationMs, lost ownership, shutdown, or the attempt ending
	run: { id: string; taskId: string; attempt: number; createdAt: Date };
	stream(name: string, source: AsyncIterable<Json | string>): Promise<void>;
};
```

`ctx.logger` entries at or above `logging.minLevel` (default `"info"`) are stored in `run_logs` in
batches — one `INSERT ... unnest` statement per batch, flushed every 500 ms, every 50 entries and
always before the run's completion/failure/suspension is written — and mirrored to the engine
`logger` with `runId`, `taskId` and `attempt` (`logging.mirror`, default `true`). `data` larger
than `limits.maxLogEntryBytes` is truncated with a marker (the mirror gets the original). Entries
still buffered when the execution lost the run (canceled, stalled, handed back on shutdown) are
dropped with a `warn`. A failed batch is retried once, then dropped with a `warn`: logs never fail
a run.

`ctx.signal` also aborts once the attempt is over (completed, failed, retry scheduled, released,
or any other exit of the executor; its reason is a `"finished"` abort), so work the task left
running (an un-awaited promise, a stream, a step still running when `maxDurationMs` cut the wait for in-flight steps short) stops
instead of duplicating side effects alongside the next attempt. A suspension aborts it too.

`ctx.stream(name, source)` appends every item of `source` (strings or JSON values) to a Redis
Stream of this run (`XADD MAXLEN ~ 10000`), then writes an end marker — always, also when `source`
throws or `ctx.signal` aborts (then the marker carries the error and `ctx.stream` rejects with it,
see `runs.readStream` above). Every append refreshes the key's expiry (1 day), so a stream whose
writer died still goes away. It is not a step: it is not memoized, so a replay streams again.

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
| `pool.max` | total task-queue `concurrency` + 5 (at least 10) | Only used when the engine creates its own pool (string `postgres`). `start()` warns when it is below that for the started queues. See [pool sizing](#pool-sizing). |
| `pool.connectionTimeoutMs` | `10 000` | `connectionTimeoutMillis` of the engine's own pool: a query waiting longer for a free connection fails (a failed heartbeat only warns). |
| `heartbeatIntervalMs` | `10 000` | How often a running execution updates `heartbeat_at`. Must be smaller than `stallTimeoutMs`. |
| `heartbeatTimeoutMs` | `2 000` | `SET LOCAL statement_timeout` applied to each heartbeat query/transaction. |
| `stallTimeoutMs` | `60 000` | A `running` run whose heartbeat is older than this is considered stalled by the reconciler. |
| `shutdownTimeoutMs` | `30 000` | How long `stop()` waits for active runs before handing them back. |
| `idempotencyKeyTtl` | `"7d"` | Default TTL for a user idempotency key; must not exceed `retention.days`. |
| `retention.days` | `30` | Finished runs older than this are deleted (only once unparented or their parent is finished). |
| `retention.intervalMs` | `3 600 000` | How often (per window) the leader schedules a retention sweep. |
| `retention.batchSize` | `1 000` | Runs deleted per statement during a sweep. |
| `schedules.pruneDeclarative` | `false` | On `start()`, also delete (Postgres + BullMQ) a `defineTask({ schedule })` schedule no registered task declares anymore. Never touches `engine.schedules.upsert()` (`"api"`) schedules. |
| `reconciler.intervalMs` | `15 000` | Leader-election + reconciler tick. Also the worst-case latency to recover a lost enqueue (plus `graceMs`). |
| `reconciler.graceMs` | `30 000` | How long a `queued`/`waiting` run may sit past its `run_after` before the reconciler re-enqueues it. |
| `reconciler.batchSize` | `100` | Rows handled per reconciler case per tick. |
| `limits.maxPayloadBytes` | `1 MB` | Trigger payloads, token outputs, task outputs. |
| `limits.maxStepOutputBytes` | `256 KB` | `step.run` outputs. |
| `limits.maxLogEntryBytes` | `16 KB` | One `ctx.logger` entry's `data`. |
| `logging.minLevel` | `"info"` | `ctx.logger` entries below this level are neither stored nor mirrored. |
| `logging.mirror` | `true` | Also send `ctx.logger` entries to `logger` (with `runId`, `taskId`, `attempt`). |
| `limits.maxInlineStepOutputBytes` | `16 MB` | Threshold above which a run's step outputs are loaded lazily in batches instead of in the single startup query. |
| `systemConcurrency` | `5` | Concurrency of the internal `${prefix}-system` worker (ticks, expirations, retention). |
| `logger` | console, warn/error only | `{ debug, info, warn, error }`, each `(fields, message) => void` (pino- and console-call-compatible). |

`Duration` fields accept either a number of milliseconds or a string like `"5m"`, `"2d"`, `"1.5h"`
(`ms`, `s`, `m`, `h`, `d`, `w`).

## Testing your tasks

`createTestEngine({ tasks })` runs your real task code with **no Redis and no Postgres**: an
in-memory store and queue, driven through the same executor as production (replay, memoization,
suspension, retries, non-determinism checks). Nothing runs in the background; the test drives it.

```ts
import { createTestEngine } from "minitrigger";
import { expect, it } from "vitest";
import { approval } from "./tasks.js";

it("approves a document", async () => {
	const engine = createTestEngine({ tasks: [approval] });

	// Triggers and runs until the run finishes or blocks. Sleeps and retry backoffs are instant.
	const first = await engine.runInline(approval, { doc: "contract.pdf" });
	expect(first.status).toBe("waiting"); // blocked on ctx.step.waitForToken

	const [token] = await engine.tokens.pending(first.runId);
	await engine.tokens.complete(token!.id, { approved: true });

	const final = await engine.drive(first.runId); // runs again until idle
	expect(final).toMatchObject({ status: "completed", output: { status: "approved" } });
	await engine.stop();
});
```

- A child triggered with `step.trigger*` whose task isn't in the test engine's `tasks` stays unprocessed: the test engine only runs registered tasks, so the child fails with a retryable `TaskNotRegisteredError`. Register every task your test exercises.
- `runInline(task, payload, options?)` returns `{ runId, status, output, error, attempt }`.
  Children of `triggerAndWait`/`batchTriggerAndWait` run inline too. A failed run is returned,
  not thrown: `error` is the stored error (e.g. `{ name: "NonDeterminismError", ... }`).
- `trigger` only enqueues; `drain()` runs everything due; `drive(run)` = `drain()` + the run's
  result.
- **Clock.** The engine has its own clock (`clock: { start }`, default: now), used as the database
  clock. With the default `autoAdvance: true`, when nothing is due but a run is delayed (a
  `sleep`, a retry backoff, `trigger({ delay })`), the clock jumps straight to the **earliest**
  delayed job of **any** kind, in time order — a token or `triggerAndWait` timeout included, even
  one no execution has reached `waitForToken`/`triggerAndWait` for yet. This means a run that
  first sleeps past a *shorter* timeout it already created (e.g.
  `createToken(..., { timeout: "1h" })` then `sleep("2h")` then `waitForToken`) will see that
  token time out at the 1h mark before the sleep resolves, exactly as it would with a real clock:
  the token's own expiry does not wait for anyone to start waiting on it. Use
  `autoAdvance: false` and `await engine.clock.advance(...)` to interleave instead (e.g. complete
  a token before its timeout even though a later step would have slept past it).
- `runs.*`, `tokens.complete`, `events.on`, `schedules.*` and `runs.subscribe`/`readStream` work
  as in `createEngine`. There is no `migrate`/`start`. Cron schedules don't fire on their own:
  `await engine.tickSchedule(id)` fires one occurrence (a `defineTask({ schedule })` uses the task
  id) — it syncs every `defineTask({ schedule })` into the store first (lazily, once; `schedules.*`,
  `drain`/`drive`/`runInline`/`clock.advance` and `trigger` also trigger that sync, so
  `schedules.list()` on a fresh engine already sees them), rejects with a `ConfigError` for an
  unknown schedule id, and rejects with whatever error the tick job itself failed with.
- `EngineClient` (`Pick<Engine, "trigger" | "runs" | "tokens" | "schedules" | "events">`) is the
  surface `Engine` and `TestEngine` share: use it to type a helper that works against either.
- `maxDurationMs`, heartbeats and log flushes still use real timers. A drain that processes more
  than `maxJobsPerDrain` jobs (default 10 000) throws, to catch runs that never settle. Of
  `retention`, only `days` is accepted (for the idempotency-key TTL check, exactly as in
  `createEngine`): the test engine never runs retention sweeps.

## Operation

- **Startup environment checks.** `engine.start()` runs `checkEnvironment()` before starting any
  worker: Postgres server version, Redis `maxmemory-policy`, and pending migrations against
  `schema` (with `autoMigrate: false` — call `engine.migrate()` yourself first). Per decision 22, a
  check whose command couldn't even run (e.g. `CONFIG GET` blocked by a managed Redis provider, or
  an unexpected/empty reply from it) only logs a `warn` and is included in the returned report; only
  a check that *does* get a definitive, wrong answer (Postgres too old, eviction policy isn't
  `noeviction`, migrations genuinely pending) throws `ConfigError` naming every problem found. The
  same function (`checkEnvironment`, exported from the package root along with its types) can be
  called standalone too, e.g. in a health check or before `migrate()` in a deploy script.
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
  The workers stop fetching jobs first, before `stop()` waits for the leader's reconciler tick; a
  retention sweep in progress stops between batches. `stop()` also finishes when Redis is
  unreachable: Redis connections that are not ready are dropped instead of sent a `QUIT` (which
  would wait for a reconnection that never comes).
- **Pool sizing.** The pool needs `max >= total worker concurrency + 5` (the engine's own
  bookkeeping queries need headroom beyond what your workers consume). A pool the engine creates
  gets that size by default (at least 10) and a 10 s `connectionTimeoutMillis`
  (`pool.connectionTimeoutMs`). If you inject your own `pg.Pool`, size it the same way and set a
  `connectionTimeoutMillis`: `start()` warns when it is smaller than the started queues need or
  has no acquisition timeout (a heartbeat would then wait for a connection without limit instead of
  failing with a warning). A separate, dedicated pool for the library is recommended over sharing
  your application's pool.
- **Advisory locks.** Migrations use `pg_advisory_xact_lock` inside a transaction; the leader
  (reconciler/retention owner) uses `pg_try_advisory_lock` on a dedicated client kept outside the
  pool for as long as it holds the lock — never on a connection borrowed from a pool, since a
  pooled connection can be silently recycled while the lock still appears held.
- **Two engines, one process.** Two `createEngine()` instances with different `schema`/`prefix`
  are expected to coexist in the same process without interfering with each other (no global
  state).

- **Leader, reconciler and retention.** Every started engine runs a timer every
  `reconciler.intervalMs` (15 s) that tries to take the leader lock; only the leader runs the
  reconciler (lost enqueues, stale waiting runs, orphan waiting runs — logged as a `warn` with the
  `runId` —, stalled runs, overdue waitpoints) and schedules retention. Ticks never overlap and
  `stop()` waits for the current one and releases the lock. If the leader's dedicated connection
  dies, another engine takes over within a few ticks. The leader holds one extra Postgres
  connection, outside the pool, while it leads. Retention runs as a `retention` job on the
  internal `-system` queue, at most once per `retention.intervalMs` window (the window comes from
  the database clock), deleting `retention.batchSize` runs per statement; any engine's `-system`
  worker may process it (a sweep is idempotent). `engine.migrate()` never waits for the leader
  (different advisory lock).
- **Run status events.** Every status change the engine makes is published (best-effort) on
  `${prefix}:run-status` with the row's `updated_at` (the feed for `engine.runs.subscribe`).
- **Redis can be wiped.** After a `FLUSHALL` (or any lost job), the reconciler re-enqueues every
  run from Postgres within about `reconciler.graceMs + reconciler.intervalMs`.

## Benchmark

`pnpm bench` (`bench/run.ts`) runs against a disposable Postgres schema/Redis prefix and measures
end-to-end throughput (1,000 runs of a 3-step task) and resume latency (sleep, token). Order of
magnitude on a recent laptop: ~1.3k-1.4k runs/s throughput, ~50-100 ms sleep-resume p50, ~3-5 ms
token-resume p50 — treat these as ballpark, not an SLA; see `CLAUDE.md` ("Benchmark") for the full
methodology, machine and result table. `pnpm check:pack` builds, `pnpm pack`s and installs the
resulting `.tgz` in a clean temp directory, confirming ESM `import`, CJS `require`, and a `tsc
--strict` check against the shipped `.d.ts` all work — the same path a [GitHub Releases
install](#install) exercises.

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
9. **Don't call steps after an `await` inside concurrent branches.** Steps are recorded in
   **completion** order, not call order, once a branch keeps awaiting and calling more steps (e.g.
   `Promise.all(items.map(async i => { await ctx.step.run(a); await ctx.step.run(b); }))`); that
   order can differ between the original run and a replay and risks `NonDeterminismError`. Call
   steps before any `await` in a branch, or run them sequentially instead.
10. **A `void`/`undefined` step or task output is stored as `null`.** Don't rely on getting back
    exactly `undefined` from a memoized `step.run` or from `runs.get(...).output` — it round-trips
    as JSON `null`.
11. **A genuinely generic task needs a cast at the call site.** `defineTask` infers `Payload`/
    `Output` from `run`; a task typed `<T extends Json>` can't have `T` inferred from `trigger()`
    alone.
12. **`stop()` is at-least-once, not exactly-once.** A run still active past `shutdownTimeoutMs` is
    handed back and retried without an attempt increment; a side effect that already happened
    outside a memoized step may run again unless it's idempotent.
13. **Missed cron ticks are not backfilled.** If every process is down when a scheduled tick would
    have fired, that occurrence is skipped for good — it doesn't run late and doesn't accumulate.
14. **A failed attempt waits for its steps still in flight.** When the task function throws while
    other steps are still running (e.g. one step of a `Promise.all` rejects), the engine waits for
    them before recording the failure, without aborting `ctx.signal`: a step that completes is
    recorded and the retry reuses its output instead of running it again. `maxDurationMs` still
    bounds that wait (the attempt then fails with the original error, and the unfinished steps
    re-run on the retry); a step that would suspend meanwhile is ignored — the error wins. A step
    whose function only stops when `ctx.signal` aborts (instead of on its own) will block the
    failure/retry until it finishes; set `maxDurationMs` if that step could run long, or it hangs
    the attempt indefinitely.
