---
name: minitrigger-setup
description: Install and configure minitrigger (durable tasks/workflows for Node.js, built on Postgres, with BullMQ + Redis by default or Postgres alone via transport: "postgres") in an existing TypeScript or JavaScript project. Use when the user wants to add minitrigger, durable background jobs, workflow execution with replay/retries, or the outbox-style trigger pattern to a project, including plain Node, NestJS, Express, Fastify or Next.js apps.
when_to_use: "add minitrigger", "set up durable background jobs", "durable tasks with bullmq and postgres", "configure minitrigger in nestjs/express/nextjs", "wire up engine.start()/stop()"
---

# Set up minitrigger in a project

Goal: the project can `trigger()` tasks (optionally from inside its own Postgres transactions) and
a long-running worker process executes them durably. Follow the steps in order; verify each one
before moving on.

## 1. Detect the project

Detect, do not ask, unless it is genuinely ambiguous:

- **Package manager**: lockfile (`pnpm-lock.yaml`, `yarn.lock`, `package-lock.json`,
  `bun.lockb`) — use the matching install command.
- **TypeScript vs JS**, and **ESM vs CJS** (`"type": "module"` in `package.json`, or a
  `tsconfig.json` with `"module": "NodeNext"`/`"ESNext"`).
- **Framework**: `@nestjs/core` → NestJS; `express`/`fastify` → a plain HTTP server; `next` →
  Next.js; none of these → plain Node.
- **Existing Postgres/Redis wiring**: env vars like `DATABASE_URL`/`REDIS_URL`, an existing `pg`
  or `ioredis` usage — reuse the same connection info where sensible, but see step 4 on why a
  dedicated pool/connection is usually the better default for minitrigger itself.
- **Node version**: minitrigger requires **Node >= 22**. If the project's `engines` field or CI
  pins an older Node, flag it before continuing.

## 2. Install

```bash
pnpm add @rivia-labs/minitrigger bullmq ioredis pg
pnpm add -D @types/pg   # optional, only if the project uses TypeScript
```

With `transport: "postgres"` (see step 3), skip `bullmq`/`ioredis` entirely:

```bash
pnpm add @rivia-labs/minitrigger pg
```

Use `npm install`/`yarn add`/`bun add` instead of `pnpm add` to match the detected package
manager — the package works the same way with any of them.

`bullmq`, `ioredis` and `pg` are **peer dependencies**, never bundled — install them explicitly.
`bullmq`/`ioredis` are **optional peers**: with `transport: "postgres"` the package never imports
either (a dynamic `import()` only reached by the BullMQ backend), so a `ConfigError` at first use
(not `createEngine()` itself) is the only symptom of installing `transport: "bullmq"` without
them. Respect these ranges (do not let the package manager silently resolve outside them):

| Package | Range | Why |
|---|---|---|
| `bullmq` | `^6.3.10` | Only for `transport: "bullmq"` (the default). |
| `ioredis` | `^5.11.1` | Only for `transport: "bullmq"`. **Not 6.x** — BullMQ 6.3.x is tested against ioredis 5.11.1; ioredis 6's RESP3 default isn't handled by BullMQ's adapter. |
| `pg` | `^8.13.0` | Always required, both transports. |
| `@types/pg` | `^8.11.0` | Optional peer — only needed because minitrigger's public types reference `pg.Pool`. |

## 3. Infrastructure

Requires **Postgres >= 13** always. **Redis is only needed with the default
`transport: "bullmq"`** — ask whether the project wants to avoid standing up Redis: with
`transport: "postgres"` (no `bullmq`/`ioredis` install, no `redis`/`prefix` config) the whole
engine runs on Postgres alone (queue, delay, wake-up and the internal system queue), at the cost
of `ctx.stream`/`runs.readStream`, `queue.rateLimit` and an approximate `globalConcurrency`
(exact instead, in this mode) not being available — see the package's "Postgres-only mode" README
section for the trade-offs and when to prefer it. Default to `transport: "bullmq"` unless the
project asks to skip Redis, already has none, or explicitly wants the Postgres-only mode.

With `transport: "bullmq"`, Redis needs `maxmemory-policy noeviction`: BullMQ stores job state —
including delayed jobs, which is everything `step.sleep`/retry backoff/scheduled runs rely on — as
ordinary Redis keys; any other eviction policy lets Redis silently drop that state under memory
pressure. `engine.start()` checks this automatically (see step 7) and warns or throws depending on
how certain it can be.

Minimal local infra, if the project doesn't already have one (omit the `redis` service entirely
with `transport: "postgres"`):

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
    command: ["redis-server", "--maxmemory-policy", "noeviction"]
    ports:
      - "6379:6379"
```

## 4. Create the engine module

One file exporting a single `createEngine({...})` instance, e.g. `src/jobs/engine.ts`:

```ts
import { createEngine } from "@rivia-labs/minitrigger";
import { tasks } from "./tasks/index.js";

export const engine = createEngine({
	postgres: process.env.DATABASE_URL!,
	redis: process.env.REDIS_URL!,
	schema: "myapp",   // must match ^[a-z_][a-z0-9_]{0,62}$
	prefix: "myapp",   // BullMQ key prefix, same pattern
	tasks,
});
```

With `transport: "postgres"`, drop `redis`/`prefix` (both are a `ConfigError` in this mode) —
NOTIFY channels derive from `schema` instead:

```ts
export const engine = createEngine({
	postgres: process.env.DATABASE_URL!,
	transport: "postgres",
	schema: "myapp",
	tasks,
});
```

- `schema`/`prefix` must be **unique per deployment**: don't default two unrelated apps to the
  same schema/prefix on a shared Postgres/Redis.
- `prefix` must additionally be **unique per Postgres schema**. Every Redis structure (task
  queues, the internal `${prefix}-system` queue, pub/sub channels, streams) is keyed by `prefix`
  alone, not by `schema` — two engines sharing a prefix with different schemas would consume each
  other's jobs. minitrigger refuses this: the first `engine.start()` records its schema under the
  Redis key `${prefix}:owner-schema`, and a `start()` from another schema on that prefix throws a
  `ConfigError`. Only delete that key once the schema that owned it has fully stopped.
- Import `tasks` from a dedicated registry module (an array of everything `defineTask` produced)
  rather than inlining them here — the `minitrigger-task` skill covers writing those tasks.

## 5. Migrations

```ts
await engine.migrate();
```

Migration files ship embedded in the package — nothing to generate or run by hand. Calling it at
process boot is fine for a small, single-instance app. For anything with multiple instances,
prefer a deliberate migration step (the project's own migration flow, or a one-off deploy-time
script) instead of every replica calling `migrate()` on every boot — not because it's unsafe
(`migrate()` is guarded by a Postgres advisory lock, so concurrent calls are serialized and race
harmlessly) but because it's redundant and couples a schema change to every process restart.

## 6. `start()`/`stop()` wiring and shutdown

```ts
await engine.migrate();
await engine.start();
```

Call `start()` only in the process(es) meant to run workers. An engine that never calls `start()`
can still `trigger()` and query (`runs.get`, `runs.list`, ...) — that's the right shape for a
trigger-only process, e.g. a web server that only enqueues work.

```ts
async function shutdown() {
	// stop accepting new HTTP requests first, if this process runs a server
	await server.close();
	await engine.stop(); // graceful: waits up to shutdownTimeoutMs (default 30s), then hands
	                      // back any still-active runs (at-least-once — see minitrigger-task)
	// close YOUR OWN pg.Pool / ioredis instance here, if you created your own for other purposes.
	// Never close a pool minitrigger created for you (from a connection-string `postgres`/`redis`
	// config) — it isn't yours to close. minitrigger also never closes a pool/connection YOU inject.
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
```

## 7. Environment checks

`engine.start()` runs `checkEnvironment()` automatically before starting any worker: Postgres
server version, Redis `maxmemory-policy`, and pending migrations (with `autoMigrate: false`, so a
pending migration throws `ConfigError` pointing at `engine.migrate()` instead of running it
inline). A check that got a definitive wrong answer throws; one whose command couldn't even run
(e.g. a managed Redis blocking `CONFIG GET`) only warns. `checkEnvironment` is also exported
standalone from the main entry, for a health check or a pre-deploy script.

## 8. Pool sizing

When minitrigger creates its own `pg.Pool` (a connection-string `postgres` config), `pool.max`
defaults to `total task-queue concurrency + 5` (minimum 10), with `pool.connectionTimeoutMs`
defaulting to `10 000`. If you inject your own `pg.Pool` instead, size it the same way and set
`connectionTimeoutMillis` — `start()` warns when the pool is too small for the started queues, or
has no acquisition timeout.

Every **started** engine (one that called `start()`) also holds **2 extra dedicated connections
outside the pool** (leader election + the outbox `LISTEN`) — budget
`max_connections >= processes × (pool.max + 2)`.

A dedicated pool for minitrigger is recommended over sharing the application's own pool: the
library's queries would otherwise compete with the app's for connections.

## 9. Framework-specific wiring

Read the reference matching the project's framework for exactly where to construct the engine,
call `migrate()`/`start()`, and wire the shutdown hook:

| Framework | Reference |
|---|---|
| NestJS | [references/nestjs.md](references/nestjs.md) |
| Express / Fastify | [references/express.md](references/express.md) |
| Next.js | [references/nextjs.md](references/nextjs.md) |
| plain `pg` / other Node | [references/pg.md](references/pg.md) |

The one hard constraint across all of them: minitrigger's workers need a **long-running Node
process**. A serverless/edge request handler (a Next.js API route, a Lambda) can `trigger()` —
never call `start()` there — but a separate, persistent worker process must call `start()`.

## 10. First run end to end

Define one trivial task, trigger it, and read the run back — this is the smoke test that setup
worked:

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
await engine.runs.get(handle.id); // should show status "completed" shortly after

await engine.stop();
```

Verify: `(await engine.runs.get(handle.id))?.status === "completed"`.

## Rules

- Don't call `engine.migrate()` on every process boot in a multi-instance production deployment
  without understanding the trade-off above (advisory-lock-serialized, so it races harmlessly, but
  a deliberate migration step is still the better default beyond a single-instance app).
- Task `id`s are persisted — steps and idempotency keys reference them. Pick stable, namespaced
  ids (`"billing.send-invoice"`), and don't rename a task while runs of it may still be in flight.
- `schema`/`prefix` are validated (`^[a-z_][a-z0-9_]{0,62}$`) — lowercase, underscores.
- Every process that runs a worker for a queue must register every task of that queue
  (`TaskNotRegisteredError` is retryable, so a rolling deploy self-heals within a few attempts, but
  don't rely on it as a long-term state).
- Next steps: `minitrigger-outbox` (move existing side-effect writes into the outbox pattern),
  `minitrigger-task` (write tasks correctly) and `minitrigger-testing` (test them).

## Final report to the user

Summarize: files created/changed (the engine module, any framework wiring files, shutdown hook),
env vars introduced (e.g. `DATABASE_URL`, `REDIS_URL`), the command to start the worker process,
and anything not verified in this environment (e.g. "confirm Postgres/Redis are actually
reachable at the configured URLs — this was not tested end to end here").
