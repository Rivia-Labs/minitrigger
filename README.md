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

Documentação em português: [docs/pt/](docs/pt/README.md)

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

`bullmq`, `ioredis` and `pg` are **peer dependencies** — never bundled — so install them alongside
`@rivia-labs/minitrigger`.

### Recommended: npm (or any npm-compatible registry client)

```bash
pnpm add @rivia-labs/minitrigger bullmq ioredis pg
pnpm add -D @types/pg   # optional, only needed if you use TypeScript
```

(`npm install`/`yarn add` work the same way — nothing pnpm-specific about the published package.)

### Alternative: `.tgz` from GitHub Releases

Useful for pinning to an exact artifact without going through the registry (e.g. vendoring it, or
installing from a private mirror). If the repository (`igorabreu29/minitrigger`) is private for
you, release assets need GitHub authentication; download the tarball with the GitHub CLI (logged in
with access to the repo) and add it from disk:

```bash
gh release download v0.2.0 -R igorabreu29/minitrigger -p "*.tgz"
pnpm add ./minitrigger-0.2.0.tgz bullmq ioredis pg
pnpm add -D @types/pg   # optional, only needed if you use TypeScript
```

(`v0.2.0`'s tarball is named `minitrigger-0.2.0.tgz` — unscoped, see the note below. From
`v0.3.0` on it's `rivia-labs-minitrigger-<version>.tgz`, and the install command uses
`@rivia-labs/minitrigger` as shown in the "Recommended: npm" section above.)

Commit the `.tgz` (e.g. under `vendor/`) or fetch it in CI with a token that can read the repo.

A release tarball already contains the built `dist/` (`pnpm pack`'s `files` field: `dist`,
`LICENSE`, plus the Claude Code plugin content — `.claude-plugin`, `skills`, `docs/pt` — that this
package also ships for people using it inside a Claude Code project; none of that plugin content
is imported or loaded by the library itself, and it is not counted in the load-time/bundle-size
notes below), so nothing builds on install: no pnpm `allowBuilds` opt-in, and no fresh,
possibly-differently-resolved `ioredis`/`msgpackr-extract` pulled in transitively just to run a
build script. It's also pinned and reproducible — a fixed artifact per version, not "whatever
`HEAD` currently contains." `pnpm check:pack` (see [Benchmark](#benchmark)) exercises exactly this
pack → install → import path in CI.

Releases up to and including `v0.2.0` were published under the unscoped name `minitrigger` (the
tarball and its `package.json` inside that tag say `minitrigger`, not `@rivia-labs/minitrigger`) —
that only matters if you pin to one of those older tags; from `v0.3.0` on, everything (npm, the
release tarball, the GitHub-install fallback below) uses `@rivia-labs/minitrigger`.

### Fallback: install straight from GitHub

Needs git access to the repo (SSH key or a credential helper for HTTPS) — only relevant while/if the
repository is private; a public repo needs no auth for this.

```bash
pnpm add github:igorabreu29/minitrigger#v0.2.0 bullmq ioredis pg
```

`dist/` is `.gitignore`d and only produced by minitrigger's own `prepare` script, and pnpm 10+
blocks a git dependency's lifecycle scripts by default — so a plain `pnpm add github:...` fails with
`ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED` until you opt in, in **your own** `pnpm-workspace.yaml`:

```yaml
# your project's pnpm-workspace.yaml
allowBuilds:
  minitrigger@github.com/igorabreu29/minitrigger#<resolved-commit>: true # tags up to v0.2.0
  # "@rivia-labs/minitrigger@github.com/igorabreu29/minitrigger#<resolved-commit>": true  # v0.3.0+
  msgpackr-extract: true # ioredis's optional native accelerator; blocked by the same pnpm mechanism
```

The `<package-name>@github.com/...` key must match the exact resolved git spec pnpm prints in its
own `ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED` error (package name + normalized git URL + resolved
commit) — copy it from that error rather than guessing (the package name in that key is whatever
`package.json` says **at the tag/commit you're installing**, per the note above), and expect to
update it whenever you bump to a newer commit/tag. Full reproduction and reasoning:
`docs/research/github-install.md`.

### Peer dependencies & requirements

| Package | Range |
|---|---|
| `bullmq` | `^6.3.0` |
| `ioredis` | `^5.11.1` (not 6.x — BullMQ 6.3.x is tested against 5.11.1; the 6.x RESP3 default isn't handled by its adapter) |
| `pg` | `^8.13.0` |
| `@types/pg` | `^8.11.0` (optional peer — only needed because the public types reference `pg.Pool`) |

Also requires **Node >= 22**, **Postgres >= 13** (checked by `checkEnvironment`/`engine.start()`),
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
import { createEngine, defineTask } from "@rivia-labs/minitrigger";

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

### Debounce

```ts
await engine.trigger(recalcClassification, { courseId }, {
	debounce: { key: `course:${courseId}`, delay: "2s" },
});
```

Triggers with the same `(task, debounce.key)` collapse into **one pending run** while that run is
still `queued` and has not started: the trigger returns the pending run's handle instead of
creating another (800 grade changes → one recalculation). Once the run starts, the key is released
and the next trigger creates a new pending run, which later triggers collapse into — at most one
pending run per key, and it always sees the newest data. The rule lives in Postgres (a unique
partial index), so it holds across processes and survives a Redis `FLUSHALL`.

- `delay` (required, > 0): the window. Always trailing — each trigger pushes the execution to
  `delay` after it, so the run executes once the key has been quiet for `delay`.
- `payload`: `"last"` (default) — a collapsed trigger replaces the pending payload; `"first"` —
  keeps the original.
- `maxDelay` (>= `delay`): caps how far a run can be postponed from its first trigger, so a key
  that never goes quiet still runs periodically.
- Keys are scoped by task, validated like idempotency keys (non-empty, at most 512 characters,
  `__` prefix reserved). `debounce` cannot be combined with `idempotencyKey` or `delay`.
- A collapsed trigger doesn't change the run's status, so it emits no event and no
  `runs.subscribe` update. A run that already started and went back to `queued` (retry, resume
  from a sleep, graceful hand-back) never absorbs triggers.
- A collapsed trigger also doesn't change the pending run's `concurrencyKey`, `maxDelay` deadline
  or `maxAttempts` — only its `payload` (per the `payload` option) and `run_after` move; those
  other fields are fixed by whichever trigger created the pending run (first trigger wins).

Debounce keeps one pending run per key; to also keep two runs of the key from executing at the
same time, add a concurrency key (below) — together they give "one pending + one running".

### Concurrency keys

```ts
defineTask({
	id: "recalc-classification",
	queue: { name: "classification", concurrency: 4, concurrencyKey: { limit: 1 } },
	run: async (payload: { courseId: string }) => { /* ... */ },
});
await engine.trigger(recalcClassification, { courseId }, {
	concurrencyKey: courseId,
	debounce: { key: courseId, delay: "2s" },
});
```

A queue that declares `concurrencyKey: { limit }` executes at most `limit` runs with the same
`trigger(..., { concurrencyKey })` at once, across every process (the queue's `concurrency` still
bounds the total per worker). The check happens at acquisition, in one Postgres transaction
serialized per key (an advisory lock): a run over the limit is not started — it goes back to the
queue **without spending an attempt**, and tries again.

- This is polling, not FIFO: a run that has been waiting the longest isn't necessarily the next to
  acquire the key once a slot frees up. Pair `concurrencyKey` with `debounce` when you want the
  "one pending + one running" pattern instead of a fair queue per key.
- Each time a run is found over the limit it retries with a growing backoff (roughly doubling,
  starting around a second, capped at 30 s, plus jitter); the count resets to 0 once the run
  acquires. A key that's busy for a long time means the polling cost per waiting run grows to that
  30 s ceiling, not a tight loop — concretely, once a waiting run's own backoff has climbed toward
  the ceiling, the LATENCY from the key's slot actually freeing up to that run acquiring it can
  itself be as long as ~30 s (its next scheduled retry), not just "about a second"; a fresh
  (never-yet-limited) run always tries again within about a second regardless.
- Only `running` runs count: a run waiting on a sleep, token or child does not hold a slot, so
  another run of the key may execute while it waits.
- Runs triggered without a `concurrencyKey` are not limited. Using `concurrencyKey` on a task whose
  queue doesn't declare one is a `ConfigError`. `limit` is shared by every task of the queue, like
  the other queue settings.
- Only `engine.trigger` takes a `concurrencyKey` for now (not `step.trigger*`); `runs.replay()`
  keeps the original run's key **only while the queue still declares `concurrencyKey`** — if it
  was removed from the queue's config since, the replayed run gets no key and runs unlimited (a
  `ConfigError` at trigger time, not at replay time, since the original run may be long gone).
  Children triggered via `step.trigger`/`step.triggerAndWait`/
  `step.batchTriggerAndWait` on a queue with `concurrencyKey` run **unlimited** — the limit only
  applies to a run whose own `concurrencyKey` was set by `engine.trigger`.
- A process that doesn't know the queue's `concurrencyKey` setting (e.g. an old deploy) acquires
  without the check; keep the setting identical across deploys. The limit is read from each
  worker's own registry, not shared centrally.
- A crashed process's `running` run keeps holding its key's slot until stall recovery
  (`stallTimeoutMs`) notices and requeues it — a hung worker can block a key for up to that long.

### Transactional trigger (outbox)

Pass your own transaction as `tx` and the run is inserted **inside it**: your business write and
the run commit — or roll back — together. No more "order saved but the email job never queued" (or
the reverse).

```ts
import { Pool } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL }); // same database as the engine

const client = await pool.connect();
try {
  await client.query("BEGIN");
  const { rows } = await client.query(
    "INSERT INTO orders (customer_id, total) VALUES ($1, $2) RETURNING id",
    [customerId, total],
  );
  await engine.trigger(sendReceipt, { orderId: rows[0].id }, {
    tx: client,
    idempotencyKey: `receipt-${rows[0].id}`, // optional; works inside the transaction too
  });
  await client.query("COMMIT"); // only now can the run execute
} catch (error) {
  await client.query("ROLLBACK"); // the run is gone too; nothing ever executes
  throw error;
} finally {
  client.release();
}
```

- `tx` is a node-postgres `PoolClient`/`Client` on which you already ran `BEGIN`, an ORM
  transaction wrapped by its [adapter](#with-an-orm-typeorm-knex-drizzle-prisma), or any object
  with the same `query(text, values)` shape bound to an open transaction. Passing the `Pool` itself is rejected: its queries would run outside your transaction.
- Nothing is enqueued before your `COMMIT`. The insert also queues a `NOTIFY` (delivered only on
  commit, **one per transaction** however many runs it triggers); every started engine `LISTEN`s
  on a schema-scoped channel over a dedicated connection and dispatches the committed runs within
  milliseconds. `trigger` returns the handle right away; the `run:queued` event / status is
  published by the engine that dispatches the run, after the commit.
- **If the notification is lost** (listener reconnecting, no started engine at commit time, a
  pooler that breaks `LISTEN`), each started engine also sweeps every **2 s** and whenever it
  (re)connects or starts, so the run is still dispatched; an enqueue lost after that is recovered
  by the reconciler (`reconciler.intervalMs` + `graceMs`, as for any trigger). A trigger-only
  engine never listens: some engine must be started for the run to execute, as always.
- **PgBouncer (transaction pooling)** breaks `LISTEN` (the session is not kept): runs are then
  dispatched by the 2 s sweep. Point the engine's `postgres` at a session-mode pool or directly
  at Postgres for millisecond dispatch. Your `tx` client itself can come from anywhere that keeps
  one transaction on one server connection.
- **Cost.** One extra `NOTIFY` per transaction; Postgres serializes the commits of transactions
  that notify (a database-wide lock taken at commit), so at very high rates of small transactions
  that each trigger, batch several triggers per transaction. Every `NOTIFY` wakes **every** started
  engine of the schema; each runs one claim sweep (bursts are coalesced, and `SKIP LOCKED` makes
  exactly one of them dispatch each run), so the cost per commit grows with the number of started
  engines, not with the number of runs.
- **Connections.** Each started engine holds its `LISTEN` connection outside the pool, in
  addition to the leader-election one — see [pool sizing](#operation).
- **A stuck listener can hold back every notifying commit.** Postgres keeps undelivered
  notifications in one cluster-wide queue (8 GB) until the slowest listening backend consumes
  them; a `LISTEN` session that stops reading (a frozen process whose socket stays open) pins it,
  and once it is full, `COMMIT`s that notify fail. The engine's listener uses TCP keepalive and
  reconnects on error, so a dead peer is dropped; for a hung-but-alive process, monitor
  `pg_notification_queue_usage()` and terminate the offending backend
  (`pg_stat_activity` where `query LIKE 'LISTEN %mt_outbox_%'`).
- With `idempotencyKey`, the key claim and the run insert are **one statement** (no `SAVEPOINT`):
  a key that belongs to a live run returns that run, inserts nothing and your transaction stays
  usable. No subtransaction is opened per trigger, so hundreds of keyed triggers in one
  transaction are fine (no subxid-cache overflow). The key row stays locked until your transaction
  ends, so a concurrent trigger with the same key waits for it. A client without `BEGIN` works too
  (each trigger then autocommits on its own — you just lose the atomicity with your writes).
- `delay` counts from the start of your transaction (Postgres `now()`). Not combinable with
  `debounce` (it would lock the shared pending run until your transaction ends) and not supported
  by `createTestEngine` (`ConfigError`). The engine only runs parameterized, schema-qualified
  statements on your client — never `SET`, `LISTEN` or other session state.
- **Tuning.** `outbox.pollIntervalMs` (default 2 000, minimum 100), `outbox.batchSize` (default
  500, rows per `claimOutbox` statement, maximum 10 000) and `outbox.reconnectMaxMs` (default
  30 000, the LISTEN client's reconnect backoff cap, **minimum 500** — the backoff itself starts at
  500 ms, so a lower cap would collapse it into a near-continuous reconnect loop while Postgres is
  down) are `createEngine` config, not per-call options — out-of-range values throw `ConfigError`
  at `createEngine()` time; see the [config table](#config-options). Set `outbox.notify: false` to
  skip the commit-time `pg_notify` call entirely (the run is still flagged `outbox_pending`):
  trades the millisecond dispatch latency for never taking the cluster-wide NOTIFY lock at commit,
  relying solely on the `pollIntervalMs` sweep — worth it for very high-rate, small transactions
  where the serialized-commit cost (above) matters more than dispatch latency.
- **Two-phase commit.** Postgres refuses `PREPARE TRANSACTION` for a transaction that has run
  `NOTIFY`/`pg_notify` — it fails with `ERROR: cannot PREPARE a transaction that has executed
  NOTIFY`. Since `trigger(..., { tx })` calls `pg_notify` on commit whenever `outbox.notify` is at
  its default (`true`), a transaction that also does `PREPARE TRANSACTION` (2PC across multiple
  resources) will fail if it triggered anything. Set `outbox.notify: false` for that engine when
  your application uses 2PC — dispatch then relies only on the `pollIntervalMs` poll (see above),
  which never runs `NOTIFY` on your transaction's connection.
- Upgrading from 0.2.x: migration `006_outbox` adds a column and a partial index; see
  [Upgrading to 0.3.0](#upgrading-to-030).

#### With an ORM (TypeORM, knex, Drizzle, Prisma)

Each ORM has a tiny adapter on its own subpath that turns the ORM's transaction handle into the
`tx` client. The adapters import nothing from the ORMs (no extra dependency, the ORM you don't use
is never loaded) and send the engine's SQL through the ORM's own raw-query API on **the
transaction's connection**, always with bind parameters. The run commits or rolls back with the
ORM's transaction, exactly as above; everything in the list above applies unchanged.

```ts
// TypeORM (0.3.x / 1.x, postgres driver)
import { typeormTx } from "@rivia-labs/minitrigger/typeorm";
await dataSource.transaction(async manager => {
  const order = await manager.save(Order, { customerId, total });
  await engine.trigger(sendReceipt, { orderId: order.id }, { tx: typeormTx(manager) });
});

// knex (pg client)
import { knexTx } from "@rivia-labs/minitrigger/knex";
await knex.transaction(async trx => {
  const [order] = await trx("orders").insert({ customer_id: customerId, total }).returning("id");
  await engine.trigger(sendReceipt, { orderId: order.id }, { tx: knexTx(trx) });
});

// Drizzle (node-postgres or postgres.js driver): also pass drizzle's `sql` tag
import { sql } from "drizzle-orm";
import { drizzleTx } from "@rivia-labs/minitrigger/drizzle";
await db.transaction(async tx => {
  const [order] = await tx.insert(orders).values({ customerId, total }).returning();
  await engine.trigger(sendReceipt, { orderId: order.id }, { tx: drizzleTx(tx, sql) });
});

// Prisma (interactive transaction, PostgreSQL)
import { prismaTx } from "@rivia-labs/minitrigger/prisma";
await prisma.$transaction(async tx => {
  const order = await tx.order.create({ data: { customerId, total } });
  await engine.trigger(sendReceipt, { orderId: order.id }, { tx: prismaTx(tx) });
});
```

| Subpath | Wraps | Sent through | Rejected with `ConfigError` |
|---|---|---|---|
| `/typeorm` → `typeormTx(managerOrQueryRunner)` | the `manager` of `dataSource.transaction(...)`, or a `QueryRunner` after `startTransaction()` | `queryRunner.query(sql, params, true)` | `dataSource.manager` (no transaction), a runner with no active transaction or released (checked on every query), a non-`postgres` driver |
| `/knex` → `knexTx(trx)` | the `trx` of `knex.transaction(...)` | `trx.raw(sql, bindings)`: each `$n` becomes `?` (one binding per occurrence), every literal `?` is escaped as `\?` | the `knex` instance itself, a completed transaction (checked on every query), a non-`pg` client |
| `/drizzle` → `drizzleTx(tx, sql)` | the `tx` of `db.transaction(...)` | `` tx.execute(sql`...`) `` built with your `sql` tag, each `$n` becoming `sql.param(value)` | the `db` itself, a missing `sql` tag |
| `/prisma` → `prismaTx(tx)` | the `tx` of `prisma.$transaction(async tx => ...)` | `tx.$queryRawUnsafe(sql, ...values)` (`$n` placeholders as-is) | the `PrismaClient` itself (each statement would autocommit) |

- The wrappers are explicit on purpose: passing a raw ORM handle as `tx` is rejected with a
  `ConfigError` that names the adapter to use (a TypeORM manager even has a `query()` that would
  otherwise be called with the wrong result shape).
- Tested against real Postgres with TypeORM 1.1, knex 3.3, drizzle-orm 0.45 (node-postgres and
  postgres.js), and Prisma 7.10 (`prisma-client` generator + `@prisma/adapter-pg`). Prisma: mind
  the interactive transaction's `timeout` (5 s by default) like for any other statement in it.
- Any other driver/ORM works the same way: pass an object with `query(text, values)` that runs the
  `$1..$n` statement on the transaction's connection and resolves to `{ rows }`.

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

#### Pausing, resuming and scaling queues

```ts
await engine.queues.pause("llm");   // every process stops starting new runs of "llm"
await engine.queues.resume("llm");
await engine.queues.list();         // [{ name, paused, updatedAt, declared, declaredConcurrency, concurrency, activeLocal }]
await engine.queues.scale("llm", 5); // THIS process's worker only, until it restarts
```

- `pause`/`resume` work across processes and survive restarts. The state is saved in Postgres
  first (table `queue_state`, the source of truth) and then applied as BullMQ's queue-level pause
  in Redis, which every worker of every process honors within one fetch. Any engine can call them,
  started or not (e.g. an admin script). Idempotent.
- **Pausing never aborts running executions**: they finish (or suspend) normally. Only new jobs
  of the queue stop being picked up; triggers still create runs (they wait as `queued`), and the
  queue's retries, sleeps and resumes wait too until `resume`.
- Every `start()` re-applies the saved state before its workers fetch, and the leader re-applies
  it on every reconciler tick. After a Redis `FLUSHALL` (which erases BullMQ's flag), a paused
  queue is paused again within `reconciler.intervalMs` (15 s by default); runs of it could start in
  that window.
- The leader's reconciler excludes paused queues from its lost-enqueue/stale-waiting scans, so a
  paused queue's backlog never delays finding genuinely lost jobs of *other* queues — but excluding
  those rows still costs the leader tick something: it's a filter applied while scanning
  candidates in `run_after` order, not an index that skips a paused queue's rows outright.
  Budget roughly 0.1 ms of extra per-tick work per 1 000 `queued`/`waiting` rows sitting in a
  paused queue; a queue paused with a very large backlog (say, millions of queued runs left
  piled up on purpose) makes every leader tick proportionally slower for as long as it stays
  paused and that backlog stays there.
- Only queues used by this engine's tasks (or with a saved state) can be paused; the internal
  `-system` queue cannot.
- `list()` returns this engine's queues plus every queue with a saved state. `paused` comes from
  Postgres; `concurrency` is this process's current worker concurrency (null when this process
  runs no worker for the queue); `activeLocal` counts the executions of the queue's runs in
  progress **in this process** (other processes' are not counted).
- `scale(name, n)` changes the concurrency of this process's worker at runtime (a decrease stops
  new fetches beyond `n` at once, running jobs finish; an increase applies at the next fetch). It
  is not saved and does not affect other processes: to change it everywhere, change `concurrency`
  in the task and deploy. Keep `pool.max` above the total concurrency + 5 (a warning is logged when
  it is not).

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

#### Bulk cancel and retry

```ts
let cursor;
do {
	const res = await engine.runs.cancelMany({ taskId: "import-csv", limit: 500, cursor });
	console.log(`canceled ${res.canceled.length}`);
	cursor = res.nextCursor ?? undefined;
} while (cursor);

const { retried, skipped, hasMore, nextCursor } = await engine.runs.retryFailed({
	queue: "emails",
	createdFrom: new Date(Date.now() - 24 * 3_600_000),
	limit: 100,
});
```

- Filter: `taskId?`, `queue?`, `createdFrom?` (inclusive), `createdTo?` (exclusive), `cursor?`, and a
  **required** `limit` (1 to 1 000 per call). Runs are walked oldest first; when more match,
  `hasMore` is true — pass `nextCursor` back as `cursor`.
- `cancelMany` cancels the matching `queued`/`running`/`waiting` runs, each exactly like
  `runs.cancel()` (pending descendants too, running executions aborted, events and `runs.subscribe`
  notified), each in its own short transaction: no long transaction, safe to run next to anything.
  `canceled` lists every run moved to `canceled`, descendants included. `failed` lists one entry
  (`{ runId, error: { name, message } }`) per matched run whose own cancel raised something other
  than the transparently-retried deadlock (a store/Redis hiccup): the call still cancels every
  other matched run rather than losing `canceled` entirely to the first error. To retry only those
  failures, call `cancelMany` again with the **same** `cursor` you used for *this* call (not
  `nextCursor` — that page's own candidates are still `queued`/`running`/`waiting`, since they
  failed to cancel, so they're matched again from the same starting point); passing no cursor at
  all (start over) also retries them, along with everything before that page.
- `retryFailed` retries the matching `failed` top-level runs like `runs.replay()`: a **new** run of
  the same task with the same payload (`attempt` starts over; the failed run is left as it was;
  its concurrency key is kept only while the queue still declares `concurrencyKey`, same as
  `runs.replay()` above). A run this call retried is **excluded from candidacy** on every future
  call (matched by its `__retry:<id>` idempotency key, decision 9): pagination with `nextCursor`
  is safe to continue even while other calls are retrying runs concurrently, and re-running the
  same filter from the start (e.g. a daily sweep) only ever retries each failed run once — you
  don't need to track which ones you already handled. `already_retried` in `skipped` (below) is
  therefore rare in practice: it only happens when two calls race for the *same* run in the small
  window between listing it as a candidate and creating its retry. Runs with a parent are left out
  (their failure was already delivered to the parent: retry the parent). `skipped` lists runs whose
  task this process does not register (`task_not_registered`), that a concurrent call retried first
  (`already_retried`), or whose stored payload no longer validates, e.g. `config.limits.maxPayloadBytes`
  was lowered since it was created (`invalid_payload` — retrying it would fail the same way on every
  future call too, so it's skipped rather than failing the whole batch).
- **On a large `runs` table, pass `createdFrom`** (e.g. "no older than 24h") to both calls: without
  it, the first page's query still has to scan forward from the oldest matching row, which for
  `cancelMany`/`retryFailed` means scanning through however much old, already-finished history
  exists before it reaches anything recent — measured ~143 ms for the first page's query alone
  against a `runs` table with 1M finished rows on unremarkable hardware. `cursor`-based pagination
  itself is fine (it seeks by index, doesn't re-scan); it's specifically a `createdFrom`-less first
  call on an old table that pays this cost every time.

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
| `SerializationError` | A payload/output/log/progress value failed the JSON-serializability check or exceeded its size limit. |
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
	start(options?: { queues?: string[] }): Promise<void>; // starts workers; omit `queues` to start every declared task queue (the `-system` worker always starts)
	stop(): Promise<void>;                      // graceful shutdown; idempotent
	trigger<T extends AnyTask>(task: T, payload?, options?: TriggerOptions): Promise<RunHandle<TaskOutput<T>>>;
	runs: {
		get<O>(run: string | RunHandle<O>): Promise<Run<O> | null>;
		list(options?: ListRunsOptions): Promise<RunList>;
		cancel(run: string | RunHandle<unknown>): Promise<"canceled" | "already_finished" | "not_found">;
		replay<O>(run: string | RunHandle<O>): Promise<RunHandle<O>>; // new run, same payload, no idempotency key
		cancelMany(filter: BatchRunsFilter): Promise<CancelManyResult>;   // see "Bulk cancel and retry"
		retryFailed(filter: BatchRunsFilter): Promise<RetryFailedResult>;
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
	queues: {                                   // see "Pausing, resuming and scaling queues"
		pause(name: string): Promise<QueueState>;
		resume(name: string): Promise<QueueState>;
		list(): Promise<QueueInfo[]>;
		scale(name: string, concurrency: number): Promise<void>;
	};
	logger: EngineLogger; // config.logger (default: warn/error to console); used by attachDefaultLogger()
}
```

`attachDefaultLogger(engine, logger?)` — see [Structured logging](#structured-logging-attachdefaultlogger)
above — subscribes an `EngineLogger` (given, or `engine.logger`) to `engine.events` and returns an
unsubscribe function. Opt-in; exported from the main entry, not a method of `Engine`.

`payload` is required unless the task's `run` takes none (then it's optional and must be omitted
or `undefined`) — the exact type is `PayloadArgs<TaskPayload<T>, [options?: TriggerOptions]>`, not
the simplified `payload?` shown above.

`TriggerOptions`: `{ idempotencyKey?: string; idempotencyKeyTtl?: Duration; delay?: Duration;
debounce?: { key: string; delay: Duration; payload?: "last" | "first"; maxDelay?: Duration };
concurrencyKey?: string }` (see [Debounce](#debounce) and [Concurrency keys](#concurrency-keys)).

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

// Status updates of one run, across processes (Postgres + Redis pub/sub). `progress` is the last
// known value written by ctx.progress.set (below); it is emitted again on its own, even when the
// status doesn't change.
for await (const update of engine.runs.subscribe(runId, { signal })) {
	console.log(update.status, update.progress, update.updatedAt); // ends after completed/failed/canceled
}

// Local events of THIS process; returns an unsubscribe function.
const off = engine.events.on("run:completed", event => { /* { name, runId, taskId, status, attempt, error, updatedAt, startedAt?, durationMs? } */ });

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
- `run:completed`/`run:failed`/`run:retrying`/`run:waiting` also carry `startedAt` (DB clock) and
  `durationMs` (monotonic clock, `performance.now()` — never persisted to Postgres, telemetry
  only). Both measure the **current execution**, not the whole attempt: since its most recent
  acquisition or resume, not summed across earlier suspensions (a run that slept, then resumed and
  suspended again waiting on a token, then finally completed reports only the last leg's duration).
  Absent on `run:queued`, `run:started` and `run:canceled`, which aren't timing one execution's end
  — this includes a suspension that requeues right away because everything it waited for was
  already resolved by the time the suspension transaction ran (a `run:queued` transition, not a
  `run:waiting` execution with something actually left pending).
- `runs.readStream` reads from the start of the stream (so a late reader gets every chunk still
  kept) and ends at the writer's end marker. When the stream ended with an error (the `source`
  given to `ctx.stream` threw, or the attempt was aborted mid-stream), the iteration **throws** a
  `MinitriggerError` (with the original `Name: message`) after yielding the chunks written before
  it. Each reader uses its own Redis connection, closed when the iteration ends (or on `stop()`,
  which ends the iteration cleanly, without an error, like `runs.subscribe`). A Redis error on
  that connection while reading (e.g. `LOADING` right after a Redis restart, `NOAUTH`) also
  **throws** out of the iteration, distinct from `stop()`'s clean end — it no longer silently ends
  the stream as if it had reached the writer's end marker, which would otherwise drop whatever
  chunks the reader hadn't gotten to yet without any indication something went wrong.
- Readers stop at the **first** end marker. If a run retries after a stream error, the next
  attempt's `ctx.stream` with the same name appends to the same stream key, after that marker:
  a reader starting from the beginning never sees the retry's chunks. Use a per-attempt name
  (e.g. `` `answer-${ctx.run.attempt}` ``) when a stream must survive retries.

#### Structured logging (`attachDefaultLogger`)

```ts
import { attachDefaultLogger } from "@rivia-labs/minitrigger";

const detach = attachDefaultLogger(engine); // uses engine's own config.logger
// attachDefaultLogger(engine, pino());     // or an explicit pino/console-compatible logger

// A TestEngine has no `.logger` of its own -- the second argument is required for it:
// attachDefaultLogger(testEngine, pino());

// later, to stop:
detach();
```

Opt-in only — neither `createEngine` nor `start()` calls it for you. `engine` can be any engine
exposing `events` — a real `Engine` or a `TestEngine` (the `EventSource` type) — but the `logger`
argument is only optional for an `Engine`, which carries its own `config.logger`; a `TestEngine`
has none, so it's required there (omitting it throws `ConfigError`). It subscribes to
`engine.events` and writes one structured line per run transition:

| Event | Level | Extra fields |
|---|---|---|
| `run:started` | `info` | — |
| `run:completed` | `info` | `durationMs` |
| `run:failed` | `error` | `durationMs`, `error: { name, message }` |
| `run:retrying` | `warn` | `durationMs`, `error: { name, message }` |
| `run:canceled` | `warn` | — |

Every line also carries `runId`, `taskId`, `attempt`. **Security by default:** a failure/retry line
never includes the full `SerializedError` (no `stack`, no chained `cause`) and never the run's
payload or output — only `error.name`/`error.message`. `run:queued`/`run:waiting` aren't logged
(noisier, less actionable by default); subscribe to them yourself via `engine.events.on` if needed.
Your own logger's level filter still applies (e.g. a pino logger configured at `warn` drops the
`info` lines) — with one exception: the **built-in default logger** (`config.logger` left unset)
normally drops `debug`/`info` entirely, which would make `run:started`/`run:completed` disappear
silently, defeating the point of attaching a logger; `attachDefaultLogger` special-cases it so its
`info` lines go to `console.info` (same `[minitrigger]` prefix as its `warn`/`error`) instead of
being dropped — every other logger is left exactly as configured.
A logging call that itself throws (a broken custom `logger`) is caught; only the *first* such
failure is reported through `logger.error` (a broken logger would otherwise report itself on every
single event) — later failures in the same `attachDefaultLogger()` call are dropped silently, and a
`logger.error` call that itself throws while reporting is swallowed too. Nothing is ever thrown
back into `engine.events`.

#### Progress

```ts
run: async (payload, ctx) => {
	for (let i = 0; i < items.length; i++) {
		await process(items[i]);
		await ctx.progress.set({ processed: i + 1, total: items.length });
	}
},
```

```ts
(await engine.runs.get(runId))?.progress; // { processed: 340, total: 800 } | null
```

`ctx.progress.set(value)` overwrites a single value per run (`runs.progress`; `null` until the
first call) — not a log, not a step: it isn't memoized and doesn't participate in non-determinism
detection, so calling it a different number of times across replays is fine. `value`'s type is
inferred per call (`set<T>(value: T & Serializable<T>)`, same as `step.run`/a payload), so a plain
interface or object type works directly, not just `Json`. It's validated like a payload
(`Serializable`, `limits.maxProgressBytes`, default 16 KB) and rejects with
`SerializationError` if it's too large — a normal error the task can catch, unlike an invalid
`ctx.step.*` argument. Writes are coalesced (at most one every 250 ms; only the latest value
survives a burst) and the last value set is flushed before the run's next completion, failure or
suspension is written, and before `engine.stop()` hands a still-running run back to the queue
(decision 20: the flush happens before the release, while this process still owns the run). A
value is dropped only when this execution has already lost ownership of the run (canceled, stall
recovered elsewhere, ...) — the same "drop with a warning" fallback `ctx.logger` uses when it loses
the run. A `ctx.progress.set` after the attempt ended (e.g. from code still running after a
rejected `Promise.all`) writes nothing and logs one warning. A write that fails is retried once; if the retry also fails, only that value is
dropped (with a warning) — a transient store hiccup doesn't disable progress for the rest of the
execution. It survives retries and resumes (the engine never resets it on its own).
`engine.runs.get`/`runs.list` read the latest value back; `runs.subscribe` emits `update.progress`
on each persisted change (best-effort over pub/sub, with a periodic re-read as a safety net; a
progress-only change is emitted on its own, not just as a side effect of a status transition).

Nit: on `engine.stop()`'s graceful hand-back, progress and logs are treated differently. The
pending progress value is flushed (as above — decision 20, before the release, while this process
still owns the run), but any log entries still buffered at that point are dropped with a `warn`
(see `ctx.logger` above): logs are a stream meant to be written promptly, not a single
"latest value" a resumed attempt still needs.

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
The exception is a **retired** schema: when a job scheduler (or tick) tagged with another schema
is met and that schema, its `schedules` table or the schedule's row no longer exists in the
engine's database, the scheduler is removed (one `warn`) instead of failing every tick forever.
The check only sees the engine's own database, so on its own it would wrongly call a schema
"retired" whenever it lives in a DIFFERENT database (e.g. a blue/green deploy: the old engine on
database A, the new one on database B, both sharing one Redis prefix) — it has no way to see that
the old engine's schema is still there, still ticking, just not in database B. A second, database-
independent guard closes that gap: removal only ever happens while THIS engine holds the prefix's
owner key (`${prefix}:owner-schema`) EXCLUSIVELY for its own schema — the same key `start()` writes
above. **Do not delete `${prefix}:owner-schema` while the schema that owns it might still be
running** (e.g. before an old engine has fully stopped during a blue/green cutover): as long as
that key is missing or points at some other schema, this engine treats every foreign-tagged
scheduler as live and never removes it, no matter what its own database says. Delete the key only
once the retired schema's engine is confirmed stopped, so the new engine's `claimPrefix()` can
take over it — that's also what makes the cross-database removal above safe to run at all.

### `ctx` (second argument of a task's `run`)

```ts
type TaskContext = {
	step: StepApi;
	logger: TaskLogger;               // debug/info/warn/error(message, data?)
	signal: AbortSignal;               // cancellation, maxDurationMs, lost ownership, shutdown, or the attempt ending
	run: { id: string; taskId: string; attempt: number; createdAt: Date };
	stream(name: string, source: AsyncIterable<Json | string>): Promise<void>;
	progress: { set<T>(value: T & Serializable<T>): Promise<void> };   // see "Progress" above
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
| `outbox.pollIntervalMs` | `2 000` | How often every started engine sweeps for committed `trigger(..., { tx })` runs even without a `NOTIFY`. Range 100–2³¹-1. |
| `outbox.batchSize` | `500` | Rows claimed per `claimOutbox` statement. Range 1–10 000. |
| `outbox.reconnectMaxMs` | `30 000` | Upper bound of the dedicated LISTEN client's reconnect backoff. Range 500–2³¹-1 (the backoff starts at 500 ms). |
| `outbox.notify` | `true` | `false` skips the commit-time `pg_notify` in `trigger(..., { tx })` (dispatch then relies only on `outbox.pollIntervalMs`). See [Transactional trigger](#transactional-trigger-outbox). |
| `limits.maxPayloadBytes` | `1 MB` | Trigger payloads, token outputs, task outputs. |
| `limits.maxStepOutputBytes` | `256 KB` | `step.run` outputs. |
| `limits.maxLogEntryBytes` | `16 KB` | One `ctx.logger` entry's `data`. |
| `limits.maxProgressBytes` | `16 KB` | `ctx.progress.set` value (`runs.progress`). |
| `logging.minLevel` | `"info"` | `ctx.logger` entries below this level are neither stored nor mirrored. |
| `logging.mirror` | `true` | Also send `ctx.logger` entries to `logger` (with `runId`, `taskId`, `attempt`). |
| `limits.maxInlineStepOutputBytes` | `16 MB` | Threshold above which a run's step outputs are loaded lazily in batches instead of in the single startup query. |
| `systemConcurrency` | `5` | Concurrency of the internal `${prefix}-system` worker (ticks, expirations, retention). |
| `logger` | console, warn/error only | `{ debug, info, warn, error }`, each `(fields, message) => void` (pino- and console-call-compatible). |

`Duration` fields accept either a number of milliseconds or a string like `"5m"`, `"2d"`, `"1.5h"`
(`ms`, `s`, `m`, `h`, `d`, `w`).

## Testing your tasks

`createTestEngine({ tasks })`, imported from the **`@rivia-labs/minitrigger/testing`** subpath (not
the main entry), runs your real task code with **no Redis and no Postgres**: an in-memory store and
queue, driven through the same executor as production (replay, memoization, suspension, retries,
non-determinism checks). Nothing runs in the background; the test drives it. The subpath keeps
`createTestEngine` and its in-memory backend out of the main entry's bundle/load time for consumers
who never test this way; error classes (`ConfigError`, `NonDeterminismError`, etc.) still come from
the main entry, and `instanceof` checks against them work the same for errors thrown by a task
running under `createTestEngine` — both entries share the same compiled chunk, not separate copies.

```ts
import { createTestEngine } from "@rivia-labs/minitrigger/testing";
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
- `EngineClient` (`Pick<Engine, "trigger" | "runs" | "tokens" | "schedules" | "events" |
  "queues">`) is the surface `Engine` and `TestEngine` share: use it to type a helper that works
  against either.
- `queues.pause`/`resume`/`list` work in memory: a paused queue's jobs are held (a drain never
  takes them and they never move the clock) until `resume`, which does not drain by itself (drive
  the engine again). `queues.scale` throws (no workers: one job at a time). `runs.cancelMany`/
  `retryFailed` work as in production.
- `debounce` works as in production for triggers issued before the next drain. `concurrencyKey`
  is checked too, but the test engine runs one job at a time, so two runs of a key never overlap on
  their own; a run limited by some other `running` run is re-delayed with the same growing backoff
  as production (~1 s, then ~2 s, ~4 s, ... up to 30 s), which `autoAdvance: true` keeps jumping to
  until `maxJobsPerDrain` — use `autoAdvance: false` in such a test.
- `maxDurationMs`, heartbeats and log flushes still use real timers. A drain that processes more
  than `maxJobsPerDrain` jobs (default 10 000) throws, to catch runs that never settle. Of
  `retention`, only `days` is accepted (for the idempotency-key TTL check, exactly as in
  `createEngine`): the test engine never runs retention sweeps.

### `assertTriggered` / `refuteTriggered` / `allTriggered`

Also from `@rivia-labs/minitrigger/testing`, inspired by `@rivia-labs/oban-ts`'s
`assertEnqueued`/`refuteEnqueued`/`allEnqueued`: assertions over which runs a `TestEngine` has
triggered, built entirely on `engine.runs.list()` — so they work for a child triggered by
`ctx.step.trigger*` exactly like a top-level `engine.trigger()`, and for a real `Engine` too (they
only need `runs`, via the shared `EngineClient` surface).

```ts
import { assertTriggered, createTestEngine, refuteTriggered } from "@rivia-labs/minitrigger/testing";
import { expect, it } from "vitest";
import { placeOrder, sendReceipt } from "./tasks.js";

it("places an order and triggers a receipt", async () => {
	const engine = createTestEngine({ tasks: [placeOrder, sendReceipt] });
	const { id } = await engine.trigger(placeOrder, { orderId: 1 });
	await engine.drive(id); // runs placeOrder, which does ctx.step.trigger("receipt", sendReceipt, ...)

	await assertTriggered(engine, { task: sendReceipt, payload: { orderId: 1 } });
	await refuteTriggered(engine, { task: sendReceipt, payload: { orderId: 2 } });
	await engine.stop();
});
```

Match criteria (all optional, combined with AND):

| Field | Matches when |
| --- | --- |
| `task` | the task (object or id) equals it |
| `payload` | the run's payload **contains** these keys with equal values (deep; a nested array must match exactly, not as a subset) |
| `status` | one status, or any of several |
| `concurrencyKey` | equals it |

`assertTriggered` resolves with the first match (oldest first), so you can chain further
assertions off it (`(await assertTriggered(engine, ...)).runId`); with no match, it throws an
`AssertionError` listing what WAS triggered (bounded, payloads truncated, never a full dump).
`allTriggered(engine, match?)` returns every match, oldest first.

### `runTask`: unit-testing one task's logic

`runTask(task, payload, options?)`, also from the testing subpath, runs a task's `run` function
directly — **no engine, no store, no queue** — for a fast unit test of its logic in isolation, the
way you'd unit test any other function:

```ts
import { runTask } from "@rivia-labs/minitrigger/testing";
import { expect, it } from "vitest";
import { approval } from "./tasks.js";

it("approves when the token resolves truthy", async () => {
	const result = await runTask(approval, { doc: "contract.pdf" }, {
		tokens: { approval: { approved: true } },
	});
	expect(result).toMatchObject({ output: { status: "approved" }, error: null });
});
```

- `ctx.step.run(name, fn)` calls `fn` immediately (no memoization/replay to check against) and
  records the call; `ctx.step.sleep` resolves instantly. Both are recorded in `result.steps`.
- `ctx.step.createToken`/`waitForToken` and `ctx.step.trigger`/`triggerAndWait`/
  `batchTriggerAndWait` have no engine to resolve them for real, so they need a test double —
  `options.tokens: { [name]: value }` (or a `() => value` function, called lazily) and
  `options.children: { [taskId]: (payload) => output }` respectively — or they **throw** an
  actionable error (not returned as `result.error`: it's a missing test double, not a task bug).
  A `createToken` that's never awaited needs no entry. A `children` stub that throws only fails
  its own `triggerAndWait`/`batchTriggerAndWait` result (`{ ok: false, error }`); `trigger`
  (fire-and-forget) propagates it like any step. Every call is also recorded in `result.triggered`.
- `ctx.logger` entries land in `result.logs`; `ctx.progress.set` values, in call order, in
  `result.progress`; `ctx.stream(name, ...)` items, per stream name, in `result.streams`.
  `ctx.signal` never fires unless you pass `options.signal`; `ctx.run.attempt` defaults to 1
  (override it to exercise attempt-dependent logic).
- Payload/output validation reuses the real `Serializable`/size checks
  (`config.limits`, overridable via `options.limits`) and step-name validation
  (`ctx.step.*`'s reserved `#`/`:` characters included), so a task that passes here behaves the
  same in production. An invalid `payload` argument throws (like `trigger()` would). An invalid
  **task** output (the value `run` itself returns) comes back as `result.error`, not thrown —
  same as a failed run in production, `retryable: false` (deterministic: retrying wouldn't help).
  An invalid **step** output (`ctx.step.run`'s return value) is different: like production, it's
  an ordinary `SerializationError` handed back to the task's own code, not fatal — uncaught, it
  becomes a normal `result.error` with `retryable: true` (the task could catch it, or a future
  attempt with smaller output could succeed).
- `runTask` never rejects for an error the task's own code threw (or an invalid task output): it
  comes back as `result.error`, exactly like `TestEngine#runInline`'s `error` — `retryable` is
  `false` for the same deterministic bugs production treats as fatal (bad step name, duplicate or
  nested step, invalid task output, ...), and otherwise `true` (this includes an invalid **step**
  output, per above) unless the task threw `AbortTaskRunError`. On a thrown error, `runTask` waits
  up to `options.settleTimeoutMs` (default: `stallTimeoutMs`'s default, 60 s) for steps still in
  flight before reporting it, mirroring production's wait bounded by `stallTimeoutMs`; a fatal
  error never waits, and a successful return always waits for every step.
- **What this does *not* test:** replay, memoization across attempts, suspension, non-determinism
  detection, real waitpoint timeouts, or anything else that depends on durability. Use
  `createTestEngine` for that; use `runTask` for the logic a task computes given its inputs.

## Upgrading to 0.3.0

- **Package renamed** to `@rivia-labs/minitrigger`: replace the `minitrigger` dependency and every
  `from "minitrigger"` import.
- **`createTestEngine` moved** to the `@rivia-labs/minitrigger/testing` subpath (`import {
  createTestEngine } from "@rivia-labs/minitrigger/testing"`); it is no longer exported from the
  main entry. `typesVersions` maps the subpath for `moduleResolution: "node10"` consumers.
- **Node.js >= 22** is required.
- **Migration `006_outbox`** (transactional trigger, decision 26) adds
  `runs.outbox_pending boolean NOT NULL DEFAULT false` (metadata-only, near-instant) and a partial
  index with a plain `CREATE INDEX`, which holds a `SHARE` lock on `runs` while it scans the table
  (same trade-off as 005 below). On a large `runs` table, either accept the pause in a
  maintenance window, or pre-build it `CONCURRENTLY` and mark version 6 applied — outside a
  transaction, against the target schema, before `engine.migrate()`:

  ```sql
  SET search_path TO "<schema>";
  SET lock_timeout = '5s';

  ALTER TABLE runs ADD COLUMN IF NOT EXISTS outbox_pending boolean NOT NULL DEFAULT false;

  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_outbox_pending
    ON runs (created_at) WHERE outbox_pending;

  -- Must return indisvalid = t; if f (interrupted build), DROP INDEX CONCURRENTLY and rebuild.
  SELECT indisvalid FROM pg_index
  WHERE indexrelid = '"<schema>".idx_runs_outbox_pending'::regclass;

  -- Only once indisvalid = t:
  INSERT INTO "<schema>".schema_migrations (version, name)
  VALUES (6, 'outbox')
  ON CONFLICT (version) DO NOTHING;
  ```

  Check the definitions against `src/migrations/006_outbox.sql` of the version you install. Coming
  from 0.1.x, apply the 0.2.0 steps below first.
- **Migration `007_queue_state`** (queue pause/resume, decision 27) only creates the small
  `queue_state` table: no lock on `runs`, nothing to prepare.
- **New APIs:** `engine.queues` (pause/resume/list/scale) and `engine.runs.cancelMany`/
  `retryFailed`; `EngineClient` now includes `queues`.
- **Transactional trigger (outbox), decision 26.** `trigger(task, payload, { tx })` inserts the run
  in your own transaction instead of enqueueing right away — see
  [Transactional trigger](#transactional-trigger-outbox). New `outbox.*` config
  (`pollIntervalMs`, `batchSize`, `reconnectMaxMs`, `notify`) — see the [config table](#config-options).
- **ORM adapters for the outbox:** `@rivia-labs/minitrigger/typeorm|knex|drizzle|prisma` subpaths
  (`typeormTx`, `knexTx`, `drizzleTx`, `prismaTx`) turn an ORM's own transaction handle into the
  `tx` client above — see [With an ORM](#with-an-orm-typeorm-knex-drizzle-prisma).
- **`attachDefaultLogger(engine, logger?)`**, opt-in structured logging over `engine.events` — see
  [Structured logging](#structured-logging-attachdefaultlogger).
- **Test helpers**, from the `/testing` subpath: `assertTriggered`/`refuteTriggered`/`allTriggered`
  (assert what an `Engine`/`TestEngine` triggered) and `runTask` (unit-test one task's `run`
  function directly, no engine/store/queue) — see
  [`assertTriggered` / `refuteTriggered` / `allTriggered`](#asserttriggered--refutetriggered--alltriggered)
  and [`runTask`: unit-testing one task's logic](#runtask-unit-testing-one-tasks-logic).
- **Reads keep working while `stop()` drains, writes don't.** During a graceful `stop()` (workers
  closing, active runs draining, connections being released), read-only calls — `runs.get`/`list`/
  `logs`/`steps`, `runs.cancel`, `tokens.complete`, `queues.list`, `schedules.list` — keep working
  right up until `stop()` finishes closing everything (its very last step, just before closing a
  pool this engine created); only after that do they throw `"this engine was stopped; create a new
  one"`. Calls that start new work — `trigger`, `runs.replay`/`subscribe`/`readStream`/`cancelMany`/
  `retryFailed`, `queues.pause`/`resume`/`scale`, `schedules.upsert`/`delete` — throw that same
  error as soon as `stop()` **begins**, not when it finishes, since starting new work while the
  engine is on its way out would fight the shutdown. A `TestEngine` never throws this on its own
  (it has no `stop()`); it stays usable for the lifetime of the object.

## Upgrading to 0.2.0

Migration `005_debounce_concurrency` (decisions 24/25: debounce and concurrency keys) adds two
indexes to `runs` — `idx_runs_debounce` (unique, partial) and `idx_runs_concurrency_running`
(partial) — with plain `CREATE INDEX`, not `CREATE INDEX CONCURRENTLY` (audit item low-d):
`engine.migrate()` runs every pending migration inside ONE transaction (`src/migrations/migrate.ts`,
so a failure rolls back everything instead of leaving a schema half-migrated), and
`CREATE INDEX CONCURRENTLY` cannot run inside a transaction block at all — Postgres rejects it
outright. A plain `CREATE INDEX` takes a `SHARE` lock on `runs` for as long as the index build
takes, which blocks every `INSERT`/`UPDATE`/`DELETE` on `runs` (including run acquisition,
completion and the heartbeat) — on a `runs` table with a lot of history, that can be a real,
user-visible pause.

Two ways to upgrade:

- **Accept the pause, in a maintenance window.** `pg_stat_user_tables`/`pg_relation_size('runs')`
  gives a sense of how long a `CREATE INDEX` (not `CONCURRENTLY`) might take on your table before
  running `engine.migrate()`; do it at low traffic, or take the engine offline for the duration.
- **Avoid it: pre-build both indexes `CONCURRENTLY`, then tell `migrate()` they're already done.**
  Migration 005's own `CREATE INDEX` statements have no `IF NOT EXISTS` (and can't be edited now
  that they've shipped — schema changes only land in new migrations, see `CLAUDE.md`), so simply
  pre-creating the indexes and then calling `engine.migrate()` as normal would fail on "relation
  already exists" and roll back the whole batch. Instead, mark version 5 as applied directly, so
  `migrate()`'s own bookkeeping (`schema_migrations`) skips re-running its SQL. Run this OUTSIDE a
  transaction (e.g. `psql` with autocommit, one statement at a time — not inside a `BEGIN`), against
  the target schema, BEFORE calling `engine.migrate()`:

  ```sql
  SET search_path TO "<schema>";

  -- Fail fast instead of queueing behind (and blocking) other transactions on `runs` while this
  -- session waits for its lock -- `ADD COLUMN` below takes a brief `ACCESS EXCLUSIVE` lock even
  -- though it doesn't rewrite the table (all four columns are nullable or have a constant
  -- default). Pick a timeout appropriate for your traffic; 0 disables it (the default, and NOT
  -- what you want here).
  SET lock_timeout = '5s';

  ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS debounce_key text,
    ADD COLUMN IF NOT EXISTS debounce_deadline timestamptz,
    ADD COLUMN IF NOT EXISTS concurrency_key text,
    ADD COLUMN IF NOT EXISTS concurrency_waits int NOT NULL DEFAULT 0;

  CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_debounce
    ON runs (task_id, debounce_key)
    WHERE status = 'queued' AND debounce_key IS NOT NULL;

  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_concurrency_running
    ON runs (queue, concurrency_key)
    WHERE status = 'running' AND concurrency_key IS NOT NULL;
  ```

  **Before marking version 5 applied, check both indexes actually built.** `CREATE INDEX
  CONCURRENTLY` can be interrupted (killed session, `psql` disconnect, crash) and leave an
  `INVALID` index behind instead of failing outright -- and because it's already named
  `idx_runs_debounce`/`idx_runs_concurrency_running`, a re-run of the block above (`IF NOT
  EXISTS`) would silently skip rebuilding it, since the index exists, just unusable (never
  scanned by the planner, still maintained on every write):

  ```sql
  SELECT indexrelid::regclass::text AS index_name, indisvalid
  FROM pg_index
  WHERE indexrelid IN (
    '"<schema>".idx_runs_debounce'::regclass,
    '"<schema>".idx_runs_concurrency_running'::regclass
  );
  ```

  If either row comes back `indisvalid = f`, drop and rebuild just that one (still outside a
  transaction, `CONCURRENTLY` on both statements) before continuing:

  ```sql
  DROP INDEX CONCURRENTLY idx_runs_debounce; -- or idx_runs_concurrency_running

  CREATE UNIQUE INDEX CONCURRENTLY idx_runs_debounce
    ON runs (task_id, debounce_key)
    WHERE status = 'queued' AND debounce_key IS NOT NULL;
  -- or, for the other one:
  CREATE INDEX CONCURRENTLY idx_runs_concurrency_running
    ON runs (queue, concurrency_key)
    WHERE status = 'running' AND concurrency_key IS NOT NULL;
  ```

  Re-run the `pg_index` check above until both rows show `indisvalid = t`, then, and only then,
  mark version 5 applied:

  ```sql
  INSERT INTO "<schema>".schema_migrations (version, name)
  VALUES (5, 'debounce_concurrency')
  ON CONFLICT (version) DO NOTHING;
  ```

  After this, `engine.migrate()` sees version 5 already recorded and does nothing for it (its
  `CREATE TABLE IF NOT EXISTS schema_migrations`/earlier migrations still run normally if they're
  also pending). Double-check `<schema>` and the column/index definitions above still match
  `src/migrations/005_debounce_concurrency.sql` in the version you're upgrading to before running
  this by hand.

If you're upgrading an existing v0.1.0 deployment (which already has migrations 001-003, including
003's own `idx_runs_created`, applied), 004 is the only other migration 0.2.0 adds on top of 005,
and it's metadata-only (`ALTER TABLE runs ADD COLUMN progress jsonb`, no index, near-instant) — this
concern is specific to 005's two new indexes.

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
- **`start()` bounds its Redis waits.** It first `PING`s the shared connection (5 s timeout) before
  doing anything else, so a fully unreachable Redis fails fast instead of hanging. It then opens
  (or reuses) the dedicated pub/sub subscriber connection and waits for its `SUBSCRIBE` to become
  ready — also bounded to 5 s. The shared connection working is not enough: a *new* connection
  (the subscriber) can still fail to become ready (Redis refusing new connections over
  `maxclients`, or dropped right after that first `PING`), which without this bound would hang
  `start()` forever (the subscriber's `maxRetriesPerRequest: null` retries indefinitely). Past 5 s,
  `start()` rejects with a `ConfigError` naming the likely cause (check `maxclients`/`CLIENT LIST`
  or the network) instead of hanging; fix it and call `start()` again.
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
  would wait for a reconnection that never comes). Closing the pool the engine created is bounded
  too (5 s): if a client is still checked out, `stop()` logs a `warn` and resolves, and the pool
  finishes closing once that client is released. An injected pool is never closed.
#### Pool sizing

The pool needs `max >= total worker concurrency + 5` (the engine's own
bookkeeping queries need headroom beyond what your workers consume). On top of the pool, every
**started** engine holds **two** dedicated Postgres connections outside it (leader election and the
outbox `LISTEN`), so budget `max_connections` for `processes × (pool.max + 2)`. A pool the engine creates
gets that size by default (at least 10) and a 10 s `connectionTimeoutMillis`
(`pool.connectionTimeoutMs`). If you inject your own `pg.Pool`, size it the same way and set a
`connectionTimeoutMillis`: `start()` warns when it is smaller than the started queues need or
has no acquisition timeout (a heartbeat would then wait for a connection without limit instead of
failing with a warning). A separate, dedicated pool for the library is recommended over sharing
your application's pool.

#### Redis connections per engine

Every non-blocking use (BullMQ `Queue` producers for every task queue plus `-system`, the outbox
publish, the streams writer, environment checks) shares the **one** connection you gave the engine
(a connection string, or your own injected `ioredis` instance) — no extra socket for those. Two
kinds of use need their own connection instead, because BullMQ/ioredis don't allow blocking
commands or pub/sub subscriber mode on a connection also used for regular commands:

- **One `duplicate()` per started worker** (one task queue worker per started queue, plus the
  always-on `-system` worker): this library creates that duplicate (tracked, closed by `stop()`).
  BullMQ then creates **one more** connection of its own on top of that, internally, for the
  blocking fetch (`BZPOPMIN`) each `Worker` needs — invisible to this library's own connection
  count, but a real socket to Redis, opened and closed by that `Worker` itself. So each started
  queue (including `-system`) costs **2** actual connections, not 1.
- **One pub/sub subscriber connection**, created lazily the first time it's needed (`start()`
  subscribing to cross-process cancellation, or the first `runs.subscribe()` on an engine that
  never called `start()`) and reused afterwards for every channel this engine subscribes to — never
  more than one, and only ever created once per engine.

So a started engine with `N` task queues (the default: every declared queue, unless `start({
queues })` narrows it) holds, once running: `1` (shared) + `2 × (N + 1)` (worker + BullMQ's own
blocking duplicate, per queue including `-system`) + `1` (subscriber) = `2N + 4` fixed Redis
connections. A trigger-only engine (never `start()`ed) holds just the shared connection, plus the
subscriber if it ever calls `runs.subscribe()`. On top of the fixed count, `runs.readStream()`
opens one more `duplicate()` **per currently-open reader**, closed when that iteration ends (or on
`stop()`) — not a fixed cost, but worth budgeting for if you expect many concurrent readers.

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

## Contributing & releasing

`pnpm typecheck && pnpm lint && pnpm test` must be green before any change lands; `pnpm test`
needs the docker infra (`pnpm infra:up`) and includes the isolated `FLUSHALL` project (local
Redis only). `prepublishOnly` deliberately runs only `pnpm typecheck && pnpm lint &&
pnpm test:unit` (no infra, never a `FLUSHALL` on whatever Redis the publishing machine points
at), and `prepare` builds `dist/`. So **run the full `pnpm test` against the local infra, plus
`pnpm check:pack`, before `pnpm publish`** — publishing does not do it for you.

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
14. **A failed attempt waits for its steps still in flight — for a bounded time.** When the task
    function throws while other steps are still running (e.g. one step of a `Promise.all`
    rejects), the engine waits for them before recording the failure, without aborting
    `ctx.signal`: a step that completes is recorded and the retry reuses its output instead of
    running it again. The wait is bounded by `stallTimeoutMs` (default 60 s), or by
    `maxDurationMs` if that ends sooner; past the bound `ctx.signal` aborts and the attempt fails
    (or retries) with the original error, and the unfinished steps re-run on the retry. So a
    pattern like `Promise.race([ctx.step.run("x", ...), deadline])` whose losing step never settles
    costs at most `stallTimeoutMs` of the worker slot, not forever. A step that would suspend
    meanwhile is ignored — the error wins. A step that ignores `ctx.signal` keeps running after the
    bound, alongside the retry: honor `ctx.signal` in long steps.
