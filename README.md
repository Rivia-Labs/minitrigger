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

**Postgres is the source of truth** for every run, step, waitpoint, idempotency key and log; by
default BullMQ is used as transport (delivery, delay, concurrency, rate limiting) — if Redis is
wiped, no run is lost, see [Operation](#operation). Since 0.4.0, `transport: "postgres"` runs the
whole engine on Postgres alone, no Redis at all — see [Postgres-only mode](#postgres-only-mode).

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
installing from a private mirror). The repository (`Rivia-Labs/minitrigger`) is public, so the
release assets need no authentication; download the tarball (the GitHub CLI is shown here, but a
plain browser or `curl` download from the release page works too) and add it from disk:

```bash
gh release download v0.4.1 -R Rivia-Labs/minitrigger -p "*.tgz"
pnpm add ./rivia-labs-minitrigger-0.4.1.tgz bullmq ioredis pg
pnpm add -D @types/pg   # optional, only needed if you use TypeScript
```

(From `v0.3.0` on, the tarball is named `rivia-labs-minitrigger-<version>.tgz` and its
`package.json` says `@rivia-labs/minitrigger`, matching the "Recommended: npm" section above. Only
the older `v0.2.0` tag — kept for reference, not recommended for new installs — used the unscoped
`minitrigger-0.2.0.tgz`/`minitrigger`, see the note below.)

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

Only `v0.2.0` and earlier were published under the unscoped name `minitrigger` (the tarball and its
`package.json` inside that tag say `minitrigger`, not `@rivia-labs/minitrigger`) — that's the old
API and only matters if you deliberately pin to one of those older tags; `v0.3.0` on is current and
everything (npm, the release tarball, the GitHub-install fallback below) uses
`@rivia-labs/minitrigger`.

### Fallback: install straight from GitHub

Needs git access to the repo (SSH key or a credential helper for HTTPS) — only relevant while/if the
repository is private; a public repo needs no auth for this.

```bash
pnpm add github:Rivia-Labs/minitrigger#v0.4.1 bullmq ioredis pg
```

`dist/` is `.gitignore`d and only produced by minitrigger's own `prepare` script, and pnpm 10+
blocks a git dependency's lifecycle scripts by default — so a plain `pnpm add github:...` fails with
`ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED` until you opt in, in **your own** `pnpm-workspace.yaml`:

```yaml
# your project's pnpm-workspace.yaml
allowBuilds:
  "@rivia-labs/minitrigger@github.com/Rivia-Labs/minitrigger#<resolved-commit>": true # v0.3.0+
  # minitrigger@github.com/Rivia-Labs/minitrigger#<resolved-commit>: true # only for tags up to v0.2.0
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
| `bullmq` | `^6.3.10` (optional peer — only needed with `transport: "bullmq"`, the default; see below) |
| `ioredis` | `^5.11.1` (not 6.x — BullMQ 6.3.x is tested against 5.11.1; the 6.x RESP3 default isn't handled by its adapter; optional peer, same condition as `bullmq`) |
| `pg` | `^8.13.0` |
| `@types/pg` | `^8.11.0` (optional peer — only needed because the public types reference `pg.Pool`) |

**`bullmq`/`ioredis` are optional peers**, needed only with `transport: "bullmq"` (the default) —
skip them entirely for `transport: "postgres"` (see [Postgres-only mode](#postgres-only-mode)). The
package never imports either at module load; with `transport: "bullmq"` and either missing, the
first operation that needs it (not `createEngine()` itself) throws `ConfigError` telling you to
install `bullmq` and `ioredis`.

Also requires **Node >= 22**, **Postgres >= 13** (checked by `checkEnvironment`/`engine.start()`),
and, with `transport: "bullmq"`, **Redis with `maxmemory-policy noeviction`** — see
[Infrastructure](#infrastructure) below.

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
`docs/SPEC.md`) if it isn't set. None of this applies to `transport: "postgres"` — Redis isn't part
of the picture at all in that mode, see [Postgres-only mode](#postgres-only-mode).

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

#### Example: an approval workflow

A run that loads data, asks a human for approval through a link, waits up to three days for the
answer (without holding a worker), then pays after a cool-off period. Every side effect is inside a
step, so a crash or a retry at any point replays the finished steps from Postgres instead of
re-running them. Steps are at-least-once, though: one interrupted mid-way (a crash after the email
went out but before the step's result was saved) runs again, so make the side effects themselves
idempotent where a duplicate matters (e.g. the payment provider's idempotency key, as in
[Retries & backoff](#retries--backoff)).

```ts
import { AbortTaskRunError, defineTask } from "@rivia-labs/minitrigger";

type Decision = { approved: boolean; note: string };

export const approveExpense = defineTask({
	id: "approve-expense",
	retry: { maxAttempts: 5, backoff: { type: "exponential", baseMs: 2_000, maxMs: 60_000 } },
	run: async (payload: { expenseId: string; approver: string }, ctx) => {
		const expense = await ctx.step.run("load", () => db.expenses.find(payload.expenseId));
		if (!expense) throw new AbortTaskRunError(`expense ${payload.expenseId} not found`); // no retry

		const token = await ctx.step.createToken("approval", { timeout: "3d" });
		await ctx.step.run("email-approver", () =>
			mailer.send(payload.approver, `https://app.example.com/approve?token=${token.id}`),
		);

		const decision = await ctx.step.waitForToken<Decision>(token); // suspends: no worker slot used
		if (!decision.ok) {
			// decision.error.name === "Timeout": nobody answered within 3 days
			await ctx.step.run("escalate", () => mailer.send("finance@example.com", `stuck: ${expense.id}`));
			return { status: "escalated" as const };
		}
		if (!decision.output.approved) return { status: "rejected" as const, note: decision.output.note };

		await ctx.step.sleep("cool-off", "1h"); // durable: survives restarts and deploys
		const payment = await ctx.step.run("pay", () => payments.pay(expense.id, expense.amount));
		ctx.logger.info("expense paid", { paymentId: payment.id });
		return { status: "paid" as const, paymentId: payment.id };
	},
});
```

The link's handler completes the token from any process (an engine that never called `start()`
is enough):

```ts
// e.g. POST /approve?token=...
async function handleApproval(tokenId: string, approved: boolean, note: string) {
	const outcome = await engine.tokens.complete(tokenId, { approved, note });
	if (!outcome.completed) {
		// "not_found" | "completed" (already answered) | "timed_out" | "run_finished"
		return { status: 409, body: { reason: outcome.reason } };
	}
	return { status: 204 };
}
```

`tokens.complete` only records the output and makes the run due; a worker picks it up right away.
Completing twice is harmless (`reason: "completed"`), so the handler can be retried by the client.

#### Example: subtasks

```ts
import { AbortTaskRunError, defineTask } from "@rivia-labs/minitrigger";

export const resizeImage = defineTask({
	id: "resize-image",
	queue: { name: "images", concurrency: 8 },
	run: async (payload: { url: string; width: number }) => ({ url: await resize(payload.url, payload.width) }),
});

export const notifyFollowers = defineTask({
	id: "notify-followers",
	run: async (payload: { albumId: string }) => { /* ... */ },
});

export const publishAlbum = defineTask({
	id: "publish-album",
	run: async (payload: { albumId: string; photos: string[] }, ctx) => {
		// Fan out: one child run per photo; the parent suspends and resumes exactly once.
		const thumbs = await ctx.step.batchTriggerAndWait(
			"thumbnails",
			resizeImage,
			payload.photos.map(url => ({ url, width: 320 })),
			{ timeout: "30m" },
		);
		// One WaitResult per payload, in the same order. A failed/timed-out child never throws here.
		const ok = thumbs.flatMap(r => (r.ok ? [r.output.url] : []));
		const failed = thumbs.filter(r => !r.ok).length;
		if (failed > 0) ctx.logger.warn("some thumbnails failed", { failed });

		const cover = await ctx.step.triggerAndWait("cover", resizeImage, {
			url: payload.photos[0] ?? "",
			width: 1200,
		});
		// A replay returns the same memoized failure, so retrying the parent can't help: abort.
		if (!cover.ok) throw new AbortTaskRunError(`cover failed: ${cover.error.message}`);

		// Fire and forget: the child runs on its own, the parent doesn't wait for it.
		await ctx.step.trigger("notify", notifyFollowers, { albumId: payload.albumId });
		return { thumbnails: ok, cover: cover.output.url };
	},
});
```

Each child is a normal run (visible in `runs.list()` with `parentRunId` set, retried by its own
`retry` policy); the parent only sees its final result. Canceling the parent cancels its pending
children.

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

A typical use is a webhook that the provider may deliver more than once: key the run by the
provider's event id and every redelivery gets the same run back.

```ts
async function onPaymentWebhook(event: { id: string; invoiceId: string }) {
	const run = await engine.trigger(
		fulfillInvoice,
		{ invoiceId: event.invoiceId },
		{ idempotencyKey: `stripe-${event.id}`, idempotencyKeyTtl: "30d" }, // TTL <= retention.days
	);
	return run.id; // same id for every delivery of this event while the key lives
}
```

### Debounce

```ts
await engine.trigger(recalcClassification, { courseId }, {
	debounce: { key: `course:${courseId}`, delay: "2s" },
});
```

With every option: reindex a product 5 s after its last edit, with the newest payload, but never
later than one minute after the first edit of a burst:

```ts
await engine.trigger(
	reindexProduct,
	{ productId, editedBy: userId },
	{ debounce: { key: `product:${productId}`, delay: "5s", maxDelay: "1m", payload: "last" } },
);
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
  from a sleep, graceful hand-back) never absorbs triggers. With `transport: "postgres"` only,
  neither does a still-pending run whose own `run_after` has already come due (its
  `delay`/`maxDelay` deadline was reached but no worker has claimed it yet): the next trigger for
  that key releases its debounce key instead (the due run's payload/`run_after`/attempt count are
  untouched — it still runs exactly as it would have) and opens a **new** pending run for the key.
  Without this, a key triggered faster than a worker could claim it would keep the row locked by
  collapses and starve it of its `SKIP LOCKED` claim for as long as the burst lasted (see
  [Postgres-only mode](#postgres-only-mode)). With `transport: "bullmq"` (as in 0.3.x) a due
  pending run keeps absorbing triggers until a worker acquires it.
- A collapsed trigger also doesn't change the pending run's `concurrencyKey`, `maxDelay` deadline
  or `maxAttempts` — only its `payload` (per the `payload` option) and `run_after` move; those
  other fields are fixed by whichever trigger created the pending run (first trigger wins).

Debounce keeps one pending run per key (with `transport: "postgres"`, a pending run already due
and not yet claimed can briefly coexist with the next window's pending run, see above); to also
keep two runs of the key from executing at the same time, add a concurrency key (below) —
together they give "one pending + one running".

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

A per-tenant limit works the same way: every tenant gets at most two syncs at once against a
third-party API, while the queue as a whole still runs up to 20 in this process, so one busy
tenant can't take every slot.

```ts
export const syncCrm = defineTask({
	id: "sync-crm",
	queue: { name: "crm-sync", concurrency: 20, concurrencyKey: { limit: 2 } },
	run: async (payload: { tenantId: string; since: string }) => { /* ... */ },
});

await engine.trigger(syncCrm, { tenantId, since }, { concurrencyKey: `tenant-${tenantId}` });
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
  addition to the leader-election one — see [Pool sizing](#pool-sizing).
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
- **The mixed-transport guard is read through this same `tx` client**, never the pool, so it can't
  compete for a connection your app might be holding all of (see the note in
  [Pool sizing](#pool-sizing)): `to_regclass` first (safe even before migration 009 has run), then
  `schema_meta.transport` itself only if the caller's role can read it. A mismatch throws
  `ConfigError` and leaves your transaction usable — you decide whether to commit or roll back the
  rest of it. **If the `tx` role has no `SELECT` on `<schema>.schema_meta`, this check is skipped
  silently** (treated the same as no engine having recorded a transport yet: the write goes
  through, and the marker is re-read after 30 s) instead of aborting your transaction with a
  permission error — grant `SELECT` on `<schema>.schema_meta` to whatever role runs `tx` triggers
  if you want the guard active on this path.
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
- `drizzleTx` does **one extra round trip per `query()` call** (plus one at wrap time): drizzle-orm
  0.45.x exposes no reliable "this transaction ended" flag, so the adapter instead compares
  Postgres's own transaction identity (`pg_current_xact_id()` + `pg_backend_pid()`) on every call
  and throws `ConfigError` on a mismatch (using the wrapper after its `db.transaction(...)`
  callback returned would otherwise silently run inside whatever unrelated transaction later
  checks out the same recycled connection).
- `knexTx` **rejects raw SQL containing a literal backslash** — not just the `?`/`\?` escaping
  above, but any backslash at all in the text passed through it (an escape-string literal, a regex
  operator, ...) — since knex's own `?`-escaping can't reliably tell an injected placeholder from a
  backslash-adjacent one apart in that case. Minitrigger's own SQL never contains one; this only
  matters if you send other raw SQL through the same wrapped `tx`.

The examples below assume one engine for the whole app (triggering only, or started — the `tx`
path is the same) and a task like:

```ts
export const sendReceipt = defineTask({
	id: "send-receipt",
	run: async (payload: { orderId: string }) => { /* ... */ },
});
```

In every one of them, if the transaction rolls back (an error thrown inside the callback, an
explicit rollback, a failed `COMMIT`), **the run never existed**: no row, no job, no event — there
is nothing to clean up. The engine must use the same database as the ORM (any connection; the
run row is written through your transaction, not the engine's pool).

##### TypeORM

With `dataSource.transaction(...)` (commits when the callback resolves, rolls back when it throws):

```ts
import { typeormTx } from "@rivia-labs/minitrigger/typeorm";

const order = await dataSource.transaction(async manager => {
	const order = await manager.save(Order, { customerId, total });
	await engine.trigger(sendReceipt, { orderId: order.id }, {
		tx: typeormTx(manager),
		idempotencyKey: `receipt-${order.id}`, // claimed in the same transaction
	});
	if (total > creditLimit) throw new Error("over the limit"); // rolls back the order AND the run
	return order;
});
```

With a `QueryRunner`, when you drive the transaction yourself:

```ts
const runner = dataSource.createQueryRunner();
await runner.connect();
await runner.startTransaction();
try {
	const order = await runner.manager.save(Order, { customerId, total });
	await engine.trigger(sendReceipt, { orderId: order.id }, { tx: typeormTx(runner) });
	await runner.commitTransaction(); // only now can the run execute
} catch (error) {
	await runner.rollbackTransaction(); // the run is gone with the order
	throw error;
} finally {
	await runner.release();
}
```

`typeormTx(dataSource.manager)` (a manager outside any transaction) throws `ConfigError` right
away, and so does a wrapper used after `commitTransaction()`/`rollbackTransaction()`/`release()`:
the transaction state is checked on every query, so a run can never silently autocommit on its
own.

##### knex

```ts
import { knexTx } from "@rivia-labs/minitrigger/knex";

await knex.transaction(async trx => {
	const [order] = await trx("orders").insert({ customer_id: customerId, total }).returning("id");
	await engine.trigger(sendReceipt, { orderId: order.id }, { tx: knexTx(trx) });
});
```

Or with a transaction object you commit/roll back yourself:

```ts
const trx = await knex.transaction();
try {
	const [order] = await trx("orders").insert({ customer_id: customerId, total }).returning("id");
	await engine.trigger(sendReceipt, { orderId: order.id }, { tx: knexTx(trx) });
	await trx.commit();
} catch (error) {
	await trx.rollback();
	throw error;
}
```

`knexTx(knex)` (the instance, not a transaction), a transaction that already committed or rolled
back, and a non-`pg` client are `ConfigError`s. Remember the backslash limitation above if you
also send your own raw SQL through the wrapped `tx` (the knex query builder itself is unaffected:
it doesn't go through the wrapper).

##### Drizzle

Pass drizzle's own `sql` tag as the second argument (the adapter builds its statements with it,
so it never imports drizzle-orm itself). Both PostgreSQL drivers work:

```ts
import { sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-postgres";
// or: import { drizzle } from "drizzle-orm/postgres-js"; const db = drizzle({ client: postgres(url) });
import { Pool } from "pg";
import { drizzleTx } from "@rivia-labs/minitrigger/drizzle";

const db = drizzle({ client: new Pool({ connectionString: process.env.DATABASE_URL }) });

await db.transaction(async tx => {
	const [order] = await tx.insert(orders).values({ customerId, total }).returning();
	await engine.trigger(sendReceipt, { orderId: order!.id }, { tx: drizzleTx(tx, sql) });
	if (order!.total > creditLimit) tx.rollback(); // throws; the order and the run are rolled back
});
```

`drizzleTx(db, sql)` (the database, not a transaction) and a missing `sql` tag are
`ConfigError`s. Create the wrapper and `await` every `trigger` **inside** the callback: a wrapper
used after the callback returned is detected (see above) and throws instead of running in someone
else's transaction.

##### Prisma

Prisma 7 with the `prisma-client` generator and the `pg` driver adapter:

```ts
import { PrismaPg } from "@prisma/adapter-pg";
import { prismaTx } from "@rivia-labs/minitrigger/prisma";
import { PrismaClient } from "./generated/prisma/client.js"; // your generator's `output`

const prisma = new PrismaClient({
	adapter: new PrismaPg({ connectionString: process.env.DATABASE_URL! }),
});

const order = await prisma.$transaction(
	async tx => {
		const order = await tx.order.create({ data: { customerId, total } });
		await engine.trigger(sendReceipt, { orderId: order.id }, { tx: prismaTx(tx) });
		return order;
	},
	{ timeout: 10_000 }, // the interactive transaction's own limit (default 5 s) covers the trigger too
);
```

Only the **interactive** form works: `prisma.$transaction([...])` (the array/batch form) never
hands you a transaction client, so there is nothing to pass as `tx`. `prismaTx(prisma)` (the
`PrismaClient` itself, where each statement would autocommit) is a `ConfigError`.

##### Plain `pg`, and any other driver

A node-postgres `PoolClient`/`Client` with an open `BEGIN` is already a valid `tx` (see the first
example of this section). For any other driver, pass an object whose `query(text, values)` runs the
statement (`$1..$n` placeholders, values as bind parameters) **on the transaction's connection**
and resolves to `{ rows }`. For example, postgres.js without an ORM:

```ts
import postgres from "postgres";
import type { TransactionClient } from "@rivia-labs/minitrigger";

// postgres.js serializes a value bound to a json/jsonb parameter itself (JSON.stringify, picked
// from the type the server infers), but the engine already sends the payload as JSON text, so it
// would be stored as a jsonb *string*. Pass json/jsonb values through as-is, as drizzle-orm does
// for its own postgres.js client. This applies to every query on this client: use a dedicated
// one for triggers, or send JSON.stringify(value) yourself for your own json/jsonb parameters.
const sql = postgres(process.env.DATABASE_URL!);
sql.options.serializers[114] = sql.options.serializers[3802] = (x: unknown) => x; // json, jsonb

await sql.begin(async tx => {
	const [order] = await tx`
		INSERT INTO orders (customer_id, total) VALUES (${customerId}, ${total}) RETURNING id`;
	const client: TransactionClient = {
		query: async (text, values = []) => ({ rows: await tx.unsafe(text, values as never[]) }),
	};
	await engine.trigger(sendReceipt, { orderId: String(order!.id) }, { tx: client });
});
```

The engine only sends strings, numbers, booleans and `null` as values (the payload is sent as JSON
text with an explicit `::jsonb` cast), and the run insert casts every placeholder explicitly, so a
driver that types every bind parameter as `text` (like Prisma's raw queries) works too. A driver
that instead re-serializes each value according to the parameter type the server infers must have
that disabled for json/jsonb, as above — otherwise the payload is JSON-encoded twice. `drizzleTx`
(on either driver) and node-postgres need nothing extra.

##### Several triggers in one transaction

Trigger as many runs as you need in the same transaction — of the same task or different ones.
They commit together. With `transport: "bullmq"` and `outbox.notify` at its default, the whole
transaction sends a single `NOTIFY`; with `transport: "postgres"`, one per distinct queue of the
runs it triggered (Postgres collapses identical notifications within a transaction):

```ts
await dataSource.transaction(async manager => {
	const order = await manager.save(Order, { customerId, total, items });
	const tx = typeormTx(manager); // one wrapper per transaction is enough
	await engine.trigger(sendReceipt, { orderId: order.id }, { tx });
	await engine.trigger(reserveStock, { orderId: order.id }, { tx });
	for (const item of items) {
		await engine.trigger(notifyWarehouse, { orderId: order.id, sku: item.sku }, { tx });
	}
});
```

Issue them one after another (`await` each), not with `Promise.all`: they share one connection,
which runs one statement at a time anyway.

##### With `transport: "postgres"`

`trigger(..., { tx })` works the same in [Postgres-only mode](#postgres-only-mode), and the
adapters above are used unchanged (the same parameterized insert goes through them). The run row
itself is the queue there, so the insert in your transaction is all there is: on commit, a
`NOTIFY` wakes the queue's workers (the poll picks it up otherwise), and on rollback the row never
existed. There is no separate outbox sweep in that mode, which is why the `outbox.*` options are
rejected with `transport: "postgres"`.

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

`concurrency` limits how many runs of the queue execute at once **in this process**; `globalConcurrency`
is BullMQ's own queue-level limit, enforced across **every** process sharing the queue (Redis-side,
via `Queue.setGlobalConcurrency()`) — set it when several deployed processes share a queue and the
cap needs to hold across all of them, not per process.

```ts
// Two tasks share the "emails" queue; only one of them declares its settings.
export const sendWelcome = defineTask({
	id: "send-welcome",
	queue: { name: "emails", concurrency: 5, globalConcurrency: 20 }, // 5 per process, 20 in total
	run: async (payload: { userId: string }) => { /* ... */ },
});
export const sendDigest = defineTask({
	id: "send-digest",
	queue: { name: "emails" },
	run: async (payload: { userId: string; week: string }) => { /* ... */ },
});

// Split workloads across processes: this one only consumes "emails" (plus the internal `-system`
// queue, which always starts); an engine that never calls start() only triggers.
await engine.start({ queues: ["emails"] });
```

#### Pausing, resuming and scaling queues

```ts
await engine.queues.pause("llm");   // every process stops starting new runs of "llm"
await engine.queues.resume("llm");
await engine.queues.list();         // [{ name, paused, updatedAt, declared, declaredConcurrency, concurrency, activeLocal }]
await engine.queues.scale("llm", 5); // THIS process's worker only, until it restarts
```

For example, an admin script that holds outgoing email while the provider has an incident. It
never starts workers; pausing still applies to every running process at once:

```ts
const admin = createEngine({
	postgres: process.env.DATABASE_URL!,
	redis: process.env.REDIS_URL!,
	tasks: [sendWelcome, sendDigest],
});

const state = await admin.queues.pause("emails"); // { name: "emails", paused: true, updatedAt }
// ...later, once the provider is back:
await admin.queues.resume("emails");               // queued runs start draining right away
for (const q of await admin.queues.list()) console.log(q.name, q.paused ? "paused" : "running");
await admin.stop();
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

A common split: transient failures throw a normal error (retried with backoff), permanent ones throw
`AbortTaskRunError`. A retry replays the function, so steps that already succeeded are not redone:

```ts
import { AbortTaskRunError, defineTask } from "@rivia-labs/minitrigger";

export const chargeCard = defineTask({
	id: "charge-card",
	retry: { maxAttempts: 6, backoff: { type: "exponential", baseMs: 5_000, maxMs: 10 * 60_000 } },
	maxDurationMs: 60_000,
	run: async (payload: { invoiceId: string }, ctx) => {
		const invoice = await ctx.step.run("load-invoice", () => loadInvoice(payload.invoiceId));
		return ctx.step.run("charge", async () => {
			const res = await fetch("https://payments.example.com/charges", {
				method: "POST",
				headers: { "idempotency-key": invoice.id }, // the provider dedupes a retried call
				body: JSON.stringify({ amount: invoice.amount }),
				signal: ctx.signal, // aborted on cancel, maxDurationMs, shutdown
			});
			if (res.status >= 400 && res.status < 500 && res.status !== 408 && res.status !== 429) {
				throw new AbortTaskRunError(`charge rejected (${res.status})`); // retrying won't help
			}
			// 5xx, 408 (timeout), 429 (rate limited): retried with backoff
			if (!res.ok) throw new Error(`payments API ${res.status} (attempt ${ctx.run.attempt})`);
			return (await res.json()) as { chargeId: string };
		});
	},
});
```

A step's `fn` that throws is not memoized: the next attempt runs it again, so side effects inside
it should be idempotent on the other end (as the `idempotency-key` header above).

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

A nightly sweep that retries every failed `send-email` run of the last day, page by page (safe to
run twice: a run already retried is never a candidate again):

```ts
import type { BatchRunsFilter } from "@rivia-labs/minitrigger";

const filter: BatchRunsFilter = {
	taskId: "send-email",
	createdFrom: new Date(Date.now() - 24 * 3_600_000),
	limit: 200,
};
let page = await engine.runs.retryFailed(filter);
for (;;) {
	for (const { runId, newRunId } of page.retried) console.log(`retried ${runId} as ${newRunId}`);
	for (const { runId, reason } of page.skipped) console.warn(`skipped ${runId}: ${reason}`);
	if (!page.hasMore || !page.nextCursor) break;
	page = await engine.runs.retryFailed({ ...filter, cursor: page.nextCursor });
}
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
- **Cost on a large `runs` table.** `cancelMany` reads active runs through a partial index
  (`idx_runs_active_created`, migration 008), so its first page does not walk the finished history:
  ~0.04 ms against a table with 1M finished runs (it was ~143 ms before that index).
  `retryFailed` scans forward from the oldest failed run; failed runs are usually spread through the
  history, so this stays cheap (~0.5 ms in the same measurement), but `createdFrom` (e.g. "no older
  than 24h") still bounds the scan if your table holds a very long run of successes before the
  failures you care about. `cursor` pagination seeks by index and never re-scans.

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

All of the above extend `MinitriggerError`. A plain `MinitriggerError` (none of the subclasses)
from `start()` means transient infrastructure: Redis or Postgres unreachable, or the pub/sub
subscriber never became ready. Unlike a `ConfigError`, it is worth retrying `start()` once the
dependency is back.

## API reference

### Exports reference

Every named export of the main entry, `/testing` and the four ORM-adapter subpaths, one line each.
Names already covered by their own subsection above/below link there instead of repeating.

**Main entry (`@rivia-labs/minitrigger`):**

| Export | What it is |
|---|---|
| `createEngine(config)` | Builds an [`Engine`](#createengineconfig). |
| `Engine`, `EngineClient` | `Engine` is the full client (also `migrate`/`start`/`stop`); `EngineClient` is the `Pick<Engine, "trigger" \| "runs" \| "tokens" \| "schedules" \| "events" \| "queues">` subset both `Engine` and `TestEngine` satisfy. |
| `EngineConfig`, `EngineLimits`, `EngineLogger`, `EngineLogFn` | `createEngine`'s config shape (a discriminated union on `transport`, see [Postgres-only mode](#postgres-only-mode)); payload/output size limits ([config table](#config-options)); the pino/console-compatible logger shape and its per-level function type. |
| `BullmqTransportConfig`, `PostgresTransportConfig`, `RedisLike` | The transport-specific halves of the `EngineConfig` union (`transport: "bullmq"` \| `"postgres"`); `RedisLike` is the minimal structural shape of an injected `redis.connection` (no `ioredis` import in public types; checked to be an ioredis `Redis` at runtime). |
| `BullmqEngineConfig`, `PostgresEngineConfig`, `CommonConfig` | The two members of `EngineConfig` (`CommonConfig & BullmqTransportConfig`, `CommonConfig & PostgresTransportConfig`) and their shared fields; object types, for `extends`/`Omit`/`Partial` (see [Upgrading to 0.4.0](#upgrading-to-040)). `BullmqEngineConfig<Redis>` types `redis.connection` as ioredis `Redis`. |
| `StartOptions` | `engine.start(options)`'s options (e.g. `{ queues }` to start only some). |
| `TriggerOptions`, `DebounceOptions`, `TransactionClient` | `engine.trigger`'s third argument (idempotency, delay, [debounce](#debounce)/[concurrency keys](#concurrency-keys)/[`tx`](#transactional-trigger-outbox)); `DebounceOptions` is its `debounce` field; `TransactionClient` is the `tx` client shape. |
| `Run`, `RunList`, `ListRunsOptions`, `RunStep`, `RunLogEntry`, `RunLogPage`, `RunLogsOptions` | Shapes returned by `runs.get`/`list`/`steps`/`logs` — see [Run introspection](#run-introspection-events-and-streams). |
| `EngineRuns`, `EngineTokens`, `EngineQueues`, `EngineEvents`, `EngineSchedules` | The `runs`/`tokens`/`queues`/`events`/`schedules` sub-APIs of `Engine`/`EngineClient`. |
| `CancelRunOutcome`, `CancelManyResult`, `BatchRunsFilter`, `BatchRunError`, `RetryFailedResult`, `RetrySkipReason` | `runs.cancel`'s return; `runs.cancelMany`/`retryFailed`'s filter and results (a per-run failure, and why a failed run wasn't retried — e.g. its payload no longer fits `maxPayloadBytes`). |
| `CompleteTokenOutcome` | `tokens.complete(id, output)`'s return (`{ completed: boolean, reason?: ... }`). |
| `QueueInfo`, `QueueState` | `queues.list()`'s per-queue info; the persisted pause state behind it. |
| `RunEvent`, `RunEventName`, `RunEventHandler` | `engine.events.on(name, handler)`'s event shape (includes `durationMs`/`startedAt` on the transitions that carry them), event names and handler type. |
| `RunStatusUpdate`, `SubscribeOptions` | `runs.subscribe(run)`'s yielded value and its `{ signal? }` option. |
| `ReadStreamOptions` | `runs.readStream(run, name, options)`'s `{ signal? }` option. |
| `Schedule`, `ScheduleUpsertInput` | A row read from `schedules.list()`; the input shape for `schedules.upsert()`. |
| `attachDefaultLogger`, `EngineEventSource` | [Structured logging](#structured-logging-attachdefaultlogger); `EngineEventSource` is the minimal `{ events }` shape it accepts (any `Engine`/`TestEngine`). |
| `checkEnvironment`, `CheckEnvironmentOptions`, `EnvCheckReport`, `EnvCheckWarning`, `EnvCheckName`, `MIN_POSTGRES_VERSION_NUM` | Standalone version/eviction-policy/pending-migrations checks (decision 22) — `engine.start()` runs this itself with `autoMigrate: false`; call it directly for e.g. a readiness probe. `EnvCheckName` is `"postgres-version" \| "redis-eviction-policy" \| "pending-migrations"`; `MIN_POSTGRES_VERSION_NUM` is the floor it checks against (`130000`, i.e. 13.0). |
| `MigrateResult` | `engine.migrate()`'s return: `{ applied: number[] }`, the migration versions applied by that call. |
| `defineTask`, `Task`, `AnyTask`, `TaskOptions`, `TaskPayload<T>`, `TaskOutput<T>` | Declares a task; its type, an unconstrained version for generic helper signatures, its options shape, and helpers that extract a task's payload/output type. |
| `TaskContext`, `TaskLogger`, `TaskProgress`, `StepApi`, `StepTriggerOptions`, `TokenHandle`, `WaitResult<T>`, `RunHandle<O>` | The second argument of `run` (`ctx`) and its pieces — see [`ctx`](#ctx-second-argument-of-a-tasks-run) and [`StepApi`](#stepapi). `RunHandle<O>` is what `trigger`/`step.trigger` return (`{ id }`, typed by the task's output). |
| `TaskQueueOptions`, `TaskScheduleOptions` | `defineTask({ queue, schedule })`'s option shapes — see [Queues](#queues) and [Schedules](#schedules-cron). |
| `SerializedError` | The `{ name, message, stack?, cause? }` shape an error takes once persisted (e.g. `Run.error`, a `WaitResult`'s `error`). |
| `MinitriggerError`, `ConfigError`, `SerializationError`, `AbortTaskRunError`, `NonDeterminismError`, `DuplicateStepError`, `NestedStepError`, `ExecutionLostError`, `TaskNotRegisteredError` | See [Errors reference](#errors-reference) — all extend `MinitriggerError`. |
| `RunStatus`, `RunCursor`, `StepKind`, `StepStatus`, `LogLevel` | `Run.status`; the opaque cursor shape of `list()`'s `nextCursor`; a step's kind (`run`/`sleep`/`token`/`wait`/`trigger`/`trigger_wait`/`batch_wait`) and status; a log entry's level. |
| `Json`, `Serializable<T>` | A plain JSON value; the compile-time check used by `defineTask`/`trigger`/`ctx.progress.set`/etc. to reject a payload/output type that isn't JSON-serializable (see [Serialization rules](#serialization-rules)). |
| `Duration`, `parseDuration` | `number \| "5m"`-style duration type accepted by `delay`/`maxDurationMs`/etc., and the function that parses it. |
| `BackoffOptions`, `RetryOptions` | `defineTask({ retry })`'s backoff and retry option shapes — see [Retries & backoff](#retries--backoff). |

**`@rivia-labs/minitrigger/testing`** (see [Testing your tasks](#testing-your-tasks)):

| Export | What it is |
|---|---|
| `createTestEngine`, `TestEngine`, `TestEngineConfig` | An in-memory `Engine`-like client for tests, and its config. |
| `TestClock`, `TestClockOptions` | `testEngine.clock`'s API (`now()`, `advance()`) and its `{ start?, autoAdvance? }` config. |
| `InlineRunResult` | `runInline`/`drive`'s return: `{ runId, status, output, error, attempt }`. |
| `PendingToken`, `TestEngineTokens` | `tokens.pending(run)`'s item shape; the `tokens` sub-API's type (adds `pending` to `EngineTokens`). |
| `assertTriggered`, `refuteTriggered`, `allTriggered` | See [`assertTriggered` / `refuteTriggered` / `allTriggered`](#asserttriggered--refutetriggered--alltriggered). |
| `AssertionError` | Thrown by `assertTriggered`/`refuteTriggered` when the expectation fails (extends `MinitriggerError`). |
| `TriggeredEngine`, `TriggeredMatch`, `TriggeredRun` | The minimal `{ runs }` shape the helpers above accept; their `match` filter shape; a matched run's shape. |
| `runTask` | See [`runTask`: unit-testing one task's logic](#runtask-unit-testing-one-tasks-logic). |
| `RunTaskOptions`, `RunTaskResult`, `RunTaskStep`, `RunTaskLogEntry`, `RunTaskTriggered`, `ChildStub` | `runTask`'s options (incl. token/child stubs), its return shape, and the shapes of the steps/log lines/triggered children it recorded; `ChildStub` is one child-task stub. |

**ORM adapter subpaths** (`@rivia-labs/minitrigger/typeorm\|knex\|drizzle\|prisma`, see [With an ORM](#with-an-orm-typeorm-knex-drizzle-prisma)) — each exports its `*Tx` wrapper function, the `*TransactionLike`/`*SqlTag` structural type(s) it accepts, and re-exports `TransactionClient`:

| Subpath | Wrapper | Structural input type(s) |
|---|---|---|
| `/typeorm` | `typeormTx(manager \| queryRunner)` | `TypeormEntityManagerLike`, `TypeormQueryRunnerLike` |
| `/knex` | `knexTx(trx)` | `KnexTransactionLike` |
| `/drizzle` | `drizzleTx(tx, sql)` | `DrizzleTransactionLike`, `DrizzleSqlTag` |
| `/prisma` | `prismaTx(tx)` | `PrismaTransactionLike` |

None of the four import the ORM itself — the types above are structural, matched against whatever
transaction handle your ORM gives you.

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
concurrencyKey?: string; tx?: TransactionClient }` (see [Debounce](#debounce),
[Concurrency keys](#concurrency-keys) and [Transactional trigger](#transactional-trigger-outbox)).
`TransactionClient` is just `{ query(text, values?) }` — a node-postgres client with an open
`BEGIN` (never a `Pool`), or the return value of one of the ORM adapters
(`typeormTx`/`knexTx`/`drizzleTx`/`prismaTx` from `@rivia-labs/minitrigger/typeorm|knex|drizzle|prisma`
— see [With an ORM](#with-an-orm-typeorm-knex-drizzle-prisma)).

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

#### Example: metrics and alerts from events

```ts
engine.events.on("run:completed", event => {
	metrics.histogram("task_duration_ms", event.durationMs ?? 0, { task: event.taskId });
});
engine.events.on("run:retrying", event => {
	metrics.increment("task_retries", { task: event.taskId, error: event.error?.name ?? "unknown" });
});
engine.events.on("run:failed", async event => {
	// A rejected handler is logged with logger.error; it never affects the run.
	await alerts.send(`${event.taskId} ${event.runId} failed after ${event.attempt} attempts: ${event.error?.message}`);
});
```

Register handlers on every process that runs workers: events are local, so each process sees the
transitions it made itself.

#### Example: streaming an LLM answer

```ts
export const answer = defineTask({
	id: "answer",
	run: async (payload: { question: string }, ctx) => {
		let text = "";
		async function* tokens() {
			for await (const token of llm.stream(payload.question, { signal: ctx.signal })) {
				text += token;
				yield token;
			}
		}
		await ctx.stream("answer", tokens()); // live, for readers below
		return { text }; // the durable result, in runs.output
	},
});

// Reader (another process is fine): chunks as they arrive, then ends at the writer's end marker.
const { id } = await engine.trigger(answer, { question });
for await (const token of engine.runs.readStream<string>(id, "answer")) process.stdout.write(token);
```

`ctx.stream` needs `transport: "bullmq"` (Redis Streams). If the run can retry after a failed
stream, use a per-attempt name as explained above.

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
exposing `events` — a real `Engine` or a `TestEngine` (the `EngineEventSource` type) — but the `logger`
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

A Server-Sent Events endpoint that pushes status and progress to the browser until the run
finishes (plain `node:http`; the same loop works in any framework):

```ts
import { createServer } from "node:http";

createServer(async (req, res) => {
	const runId = new URL(req.url ?? "/", "http://localhost").searchParams.get("run");
	if (!runId) return void res.writeHead(400).end();

	res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
	const abort = new AbortController();
	req.on("close", () => abort.abort()); // browser went away: end the subscription

	// First the current state from Postgres, then every change; ends after a terminal status.
	for await (const { status, progress, updatedAt } of engine.runs.subscribe(runId, { signal: abort.signal })) {
		res.write(`data: ${JSON.stringify({ status, progress, updatedAt })}\n\n`);
	}
	res.end();
}).listen(3000);
```

A run id that doesn't exist ends the loop without emitting anything. The engine serving this
doesn't need to be started (it opens its own subscriber connection).

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
	cron: "0 9 * * *",      // 5 or 6 whitespace-separated fields (a leading seconds field is
	                        // optional; 7 fields are rejected since 0.4.0)
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

A task that takes no payload needs none in its schedule either:

```ts
export const purgeExpiredSessions = defineTask({
	id: "purge-expired-sessions",
	schedule: { cron: "*/15 * * * *" }, // every 15 minutes
	run: async () => { await db.sessions.deleteExpired(); },
});
```

Per-customer schedules managed from your app's settings screen: one `"api"` schedule per
customer, updated in place when the customer changes the time (`upsert` is keyed by `id`).
Schedule ids allow letters, digits, `_`, `.` and `-` (1–128 characters):

```ts
async function saveReportSettings(customer: { id: string; hour: number; timezone: string }) {
	await engine.schedules.upsert({
		id: `weekly-report.${customer.id}`,
		task: weeklyReport,                       // defineTask whose run takes { customerId: string }
		cron: `0 ${customer.hour} * * 1`,         // Mondays at the chosen hour...
		timezone: customer.timezone,              // ...in the customer's own timezone
		payload: { customerId: customer.id },
	});
}

async function disableReport(customerId: string) {
	await engine.schedules.delete(`weekly-report.${customerId}`);
}
```

Each tick creates an ordinary run (with the schedule's payload read fresh from Postgres), so
`runs.list({ taskId: "weekly-report" })`, events and retries work as for any other trigger.

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
| `transport` | `"bullmq"` | `"bullmq"` (Redis + BullMQ, unchanged) or `"postgres"` (no Redis at all, decision 28) — see [Postgres-only mode](#postgres-only-mode). A discriminated union: `redis`/`prefix`/`outbox` only exist with `"bullmq"`; `polling` only with `"postgres"`. |
| `redis` | — | **`transport: "bullmq"` only.** Connection string, or `{ connection }` with an existing `ioredis` instance (only used for non-blocking producers; workers/subscribers always get a `duplicate()`). Required with `"bullmq"`; a `ConfigError` if present with `"postgres"`. |
| `polling.intervalMs` | `1 000` | **`transport: "postgres"` only.** Cap on the adaptive poll fallback for a missed `NOTIFY` (wake, status, cancel). Minimum 100. |
| `polling.jitterMs` | `50` | **`transport: "postgres"` only.** Jitter added to the poll above, to spread out a `NOTIFY`-driven thundering herd. Minimum 0. |
| `schema` | `"minitrigger"` | Postgres schema. Must match `^[a-z_][a-z0-9_]{0,62}$`. |
| `prefix` | `"minitrigger"` | **`transport: "bullmq"` only.** BullMQ key prefix. Same pattern as `schema`. A `ConfigError` with `transport: "postgres"` (NOTIFY channels derive from `schema` instead). |
| `tasks` | — | Array of tasks from `defineTask`. |
| `pool.max` | total task-queue `concurrency` + 5 (at least 10; + `systemConcurrency` too with `transport: "postgres"`, whose `-system` worker runs against the pool) | Only used when the engine creates its own pool (string `postgres`). `start()` warns when it is below that for the started queues. See [pool sizing](#pool-sizing). |
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
| `outbox.pollIntervalMs` | `2 000` | **`transport: "bullmq"` only** (a `ConfigError` with `"postgres"` — there's no separate outbox to sweep, see [Postgres-only mode](#postgres-only-mode)). How often every started engine sweeps for committed `trigger(..., { tx })` runs even without a `NOTIFY`. Range 100–2³¹-1. |
| `outbox.batchSize` | `500` | **`transport: "bullmq"` only.** Rows claimed per `claimOutbox` statement. Range 1–10 000. |
| `outbox.reconnectMaxMs` | `30 000` | **`transport: "bullmq"` only.** Upper bound of the dedicated LISTEN client's reconnect backoff. Range 500–2³¹-1 (the backoff starts at 500 ms). |
| `outbox.notify` | `true` | **`transport: "bullmq"` only.** `false` skips the commit-time `pg_notify` in `trigger(..., { tx })` (dispatch then relies only on `outbox.pollIntervalMs`). See [Transactional trigger](#transactional-trigger-outbox). |
| `limits.maxPayloadBytes` | `1 MB` | Trigger payloads, token outputs, task outputs. |
| `limits.maxStepOutputBytes` | `256 KB` | `step.run` outputs. |
| `limits.maxLogEntryBytes` | `16 KB` | One `ctx.logger` entry's `data`. |
| `limits.maxProgressBytes` | `16 KB` | `ctx.progress.set` value (`runs.progress`). |
| `logging.minLevel` | `"info"` | `ctx.logger` entries below this level are neither stored nor mirrored. |
| `logging.mirror` | `true` | Also send `ctx.logger` entries to `logger` (with `runId`, `taskId`, `attempt`). |
| `limits.maxInlineStepOutputBytes` | `16 MB` | Threshold above which a run's step outputs are loaded lazily in batches instead of in the single startup query. |
| `systemConcurrency` | `5` | Concurrency of the internal `${prefix}-system` worker (ticks, expirations, retention). With `transport: "postgres"`, this worker claims rows of the `system_jobs` table against the same pool instead of a separate BullMQ queue — see [Postgres-only mode](#postgres-only-mode). |
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

#### Example: testing both outcomes of a timeout

Using the `approveExpense` task from [the approval workflow example](#example-an-approval-workflow)
(with `db`, `mailer` and `payments` mocked as usual in your test runner). With the default clock
the `sleep("cool-off", "1h")` is instant; with `autoAdvance: false` you move time yourself, which is
how you reach the token's 3-day timeout:

```ts
import { createTestEngine } from "@rivia-labs/minitrigger/testing";
import { expect, it } from "vitest";
import { approveExpense } from "./tasks.js";

it("pays an approved expense", async () => {
	const engine = createTestEngine({ tasks: [approveExpense] });
	const run = await engine.runInline(approveExpense, { expenseId: "e1", approver: "ana@example.com" });
	expect(run.status).toBe("waiting");

	const [token] = await engine.tokens.pending(run.runId); // [{ id, name: "approval" }]
	expect(await engine.tokens.complete(token!.id, { approved: true, note: "ok" })).toEqual({ completed: true });

	const final = await engine.drive(run.runId); // the 1h sleep is skipped by autoAdvance
	expect(final.status).toBe("completed");
	expect(final.output).toMatchObject({ status: "paid" });
	await engine.stop();
});

it("escalates when nobody answers in 3 days", async () => {
	const engine = createTestEngine({ tasks: [approveExpense], clock: { autoAdvance: false } });
	const run = await engine.runInline(approveExpense, { expenseId: "e1", approver: "ana@example.com" });

	await engine.clock.advance("3d"); // the token expires; the run resumes with { ok: false }
	const final = await engine.drive(run.runId);
	expect(final).toMatchObject({ status: "completed", output: { status: "escalated" } });

	const steps = await engine.runs.steps(run.runId);
	expect(steps.map(s => s.name)).toContain("escalate");
	await engine.stop();
});
```

`events.on` works here too, e.g. to assert that a run retried: collect `run:retrying` events into
an array before driving the engine.

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

```ts
const engine = createTestEngine({ tasks: [publishAlbum, resizeImage, notifyFollowers, syncCrm] });

// Every child of publishAlbum's batch was triggered, and linked to the parent. Runs created in
// the same millisecond have no guaranteed order, so compare them as a set.
const parent = await engine.runInline(publishAlbum, { albumId: "a1", photos: ["p1.jpg", "p2.jpg"] });
const thumbs = await allTriggered(engine, { task: resizeImage, payload: { width: 320 } });
expect(thumbs.map(r => r.payload)).toHaveLength(2);
expect(thumbs.map(r => r.payload)).toEqual(
	expect.arrayContaining([{ url: "p1.jpg", width: 320 }, { url: "p2.jpg", width: 320 }]),
);
expect(thumbs.every(r => r.parentRunId === parent.runId)).toBe(true);

// A per-tenant sync was queued with the right concurrency key, and nothing for another tenant.
await engine.trigger(syncCrm, { tenantId: "42", since: "2026-01-01" }, { concurrencyKey: "tenant-42" });
await assertTriggered(engine, { task: "sync-crm", concurrencyKey: "tenant-42", status: "queued" });
await refuteTriggered(engine, { task: "sync-crm", concurrencyKey: "tenant-7" });
```

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

Children are replaced by stubs keyed by the child task's id. A stub that throws becomes that
child's `{ ok: false, error }`, so failure handling is easy to exercise:

```ts
it("keeps the thumbnails that worked and notifies followers", async () => {
	const result = await runTask(publishAlbum, { albumId: "a1", photos: ["p1.jpg", "bad.jpg"] }, {
		children: {
			"resize-image": payload => {
				const { url, width } = payload as { url: string; width: number };
				if (url === "bad.jpg" && width === 320) throw new Error("corrupt image");
				return { url: `${url}@${width}` };
			},
			"notify-followers": () => null,
		},
	});

	expect(result.error).toBeNull();
	expect(result.output).toEqual({ thumbnails: ["p1.jpg@320"], cover: "p1.jpg@1200" });
	expect(result.logs).toContainEqual(expect.objectContaining({ level: "warn", data: { failed: 1 } }));
	expect(result.triggered.map(t => [t.mode, t.taskId])).toEqual([
		["batchTriggerAndWait", "resize-image"],
		["batchTriggerAndWait", "resize-image"],
		["triggerAndWait", "resize-image"],
		["trigger", "notify-followers"],
	]);
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

## Upgrading to 0.4.1

- **`bullmq` peer range is now `^6.3.10`** (was `^6.3.0`). BullMQ 6.3.10 fixes a bug in
  `Worker.retryIfFailed` (taskforcesh/bullmq#4831): a job finishing while or after its worker
  closed, with Redis down or its connections already closed, retried its Redis call in a
  zero-delay loop, pinning the CPU and keeping the process from exiting. minitrigger used to patch
  that private method; the patch is gone, so upgrade `bullmq` along with this release
  (`pnpm add bullmq@^6.3.10`). Nothing else changes, and `transport: "postgres"` doesn't use BullMQ.
- **`start()` with Redis unreachable rejects with a `MinitriggerError`** (was `ConfigError`), like
  "cannot reach Postgres": it is a transient outage, worth retrying. The `PING` error is its
  `cause` when there is one. Code that catches `ConfigError` around `start()` to detect a dead
  Redis should catch `MinitriggerError` (or match the "cannot reach Redis" message) instead.

## Upgrading to 0.4.0

- **New: `transport: "postgres"`** (decision 28) — opt-in; `transport: "bullmq"` stays the default
  and needs no changes at all. See [Postgres-only mode](#postgres-only-mode).
- **Migration `009_postgres_transport`** adds two new tables, always empty and instant to create
  (`system_jobs`, `schema_meta`), and two indexes on `runs`, both plain `CREATE INDEX IF NOT
  EXISTS` (decision 16's usual trade-off: a `SHARE` lock on `runs` for however long the build
  takes). They only matter once you run `transport: "postgres"` — a `"bullmq"` engine never claims
  through them — but `engine.migrate()` builds them for every schema regardless (schema is shared
  code, not per-transport). On a large `runs` table, pre-build the two indexes `CONCURRENTLY` under
  the same names first, outside a transaction, against the target schema, before
  `engine.migrate()`:

  ```sql
  SET search_path TO "<schema>";
  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_claim ON runs (queue, run_after)
    WHERE status IN ('queued', 'waiting');
  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_running_queue ON runs (queue)
    WHERE status = 'running';

  -- Must return indisvalid = t for both; if f (interrupted build), DROP INDEX CONCURRENTLY and
  -- rebuild that one before running engine.migrate() (its IF NOT EXISTS would otherwise keep an
  -- invalid index forever).
  SELECT indexrelid::regclass::text AS index_name, indisvalid FROM pg_index
  WHERE indexrelid IN (
    '"<schema>".idx_runs_claim'::regclass,
    '"<schema>".idx_runs_running_queue'::regclass
  );
  ```

  `engine.migrate()` then sees both indexes already exist and only creates `system_jobs`/
  `schema_meta` and records version 9 — no lock contention with your existing traffic either way,
  since those two tables start (and, for a `"bullmq"` deployment, stay) empty.
- **`bullmq` and `ioredis` are now optional peers.** Nothing changes for an existing
  `transport: "bullmq"` deployment — keep installing them exactly as before (see the
  [peer dependencies table](#peer-dependencies--requirements)). What changed under the hood: the
  main entry and `/testing` no longer statically import either package; the BullMQ backend loads
  lazily (a dynamic `import()`) the first time an engine actually needs it. If they aren't
  installed, that first use (not `createEngine()` itself) throws `ConfigError` telling you to
  install `bullmq` and `ioredis`. That includes `engine.schedules.upsert()`/`delete()`: the
  backend is loaded after the input is validated and before the Postgres write, so a missing peer
  is a `ConfigError` with nothing written (not a schedule row whose BullMQ scheduler silently
  never gets created).
- **Cron patterns are now validated by `cron-parser` itself**, not a hand-rolled regex, in both
  `defineTask({ schedule })` and `engine.schedules.upsert()`, in both transports. `cron-parser`
  recognizes 5 or 6 whitespace-separated fields (an optional leading seconds field); **a 7-field
  pattern the old regex let through now fails at validation time with `ConfigError`** instead of
  only failing later, silently, inside BullMQ's own scheduler (BullMQ uses the same library
  internally, so a 7-field pattern never actually ran on a schedule — this just surfaces the
  failure immediately, with a clear message, instead of a schedule that's silently never ticked).
  `cron-parser` is pinned to exactly the version BullMQ pins (`5.10.1`), so validation and the
  BullMQ scheduler always agree. Checked against `cron-parser` 5.10.1:
  - **Now `ConfigError` at `defineTask`/`schedules.upsert`** (the old regex let them through;
    BullMQ already rejected them, so none of them ever ran): 7 fields (`0 0 * * * * 2026`), `LW`
    and `15W` in day-of-month, a standalone `L` in day-of-week (`0 0 * * L`; `5L`, "last Friday",
    is fine), out-of-range values (`0 0 32 * *`, `0 24 * * *`), impossible dates (`0 0 30 2 *`).
  - **Now accepted** (the old regex rejected them): `@daily`, `@hourly`, `@weekly` and the other
    `@` shortcuts, and 3–4 field patterns. Careful with the latter: `cron-parser` fills them from
    the right (the given fields are the last ones: day-of-month/month/day-of-week), so `* * 5`
    means "every minute on Fridays", which is rarely what was meant — write all 5 fields.
  - **Still rejected:** an empty or whitespace-only pattern stays a `ConfigError`, as with the old
    regex. `cron-parser` alone would read `""` as "every minute", so the library rejects it before
    calling it.
- **`EngineConfig` is now a discriminated union** on `transport`
  (`{ transport?: "bullmq"; redis; prefix?; outbox? } | { transport: "postgres"; polling? }`), so
  the compiler now catches `redis`/`prefix`/`outbox` paired with `transport: "postgres"` (or
  `polling` without it) — these were plain optional fields before, checked only at runtime. If you
  build the config object incrementally with a spread (e.g. `{ ...base, transport: "postgres" }`),
  TypeScript widens a bare string literal assigned earlier back to `"bullmq" | "postgres"` unless
  you pin it — write `{ ...base, transport: "postgres" as const }` (or give `base` itself a
  narrower type) so the result still matches one union member exactly instead of failing to
  type-check as `EngineConfig`.

  Some 0.3.x patterns that treated `EngineConfig` as one object type no longer compile. Each has a
  one-word fix: use the new `BullmqEngineConfig` (= `CommonConfig & BullmqTransportConfig`, also
  exported, as is `PostgresEngineConfig`), or the equivalent
  `Extract<EngineConfig, { transport?: "bullmq" }>`:

  | 0.3.x code | Error in 0.4 | Write instead |
  |---|---|---|
  | `interface AppConfig extends EngineConfig` | TS2312 (can't extend a union) | `extends BullmqEngineConfig` |
  | `Omit<EngineConfig, "tasks">` then `createEngine({ ...base, tasks })` | TS2345/TS2379 | `Omit<BullmqEngineConfig, "tasks">` |
  | `Partial<EngineConfig>` overrides spread into `createEngine` | TS2379/TS2322 | `Partial<BullmqEngineConfig>` |
  | `config.redis` used as always present | TS18048 | type `config` as `BullmqEngineConfig` |
  | `config.redis.connection` used as an ioredis `Redis` | TS2740 (`RedisLike` is structural) | `BullmqEngineConfig<Redis>` |

  `redis.connection` is still meant to be an ioredis `Redis`: since `RedisLike` is only a
  structural type (so the public types never import `ioredis`), a `Cluster` or a hand-made object
  now type-checks, and is rejected at runtime with `ConfigError` when the BullMQ backend loads
  (first use), instead of failing deep inside BullMQ. A `Cluster` was never supported.
- **Redis connects lazily with `transport: "bullmq"`.** The engine no longer opens any Redis
  connection at `createEngine()` time — the first connection opens on first use (typically
  `engine.start()`, or an earlier `trigger()`/`runs.subscribe()` call on a trigger-only engine that
  never calls `start()`). A malformed `redis` connection string or an unreachable Redis now
  surfaces as an error from that first call, not from `createEngine()` itself. `runs.cancel()` and
  `tokens.complete()` are also a "first use": on a trigger-only engine they now load the backend
  (opening Redis) and read the mixed-transport guard (see below) on their very first call, even
  when the run/token id turns out not to resolve to anything — any syntactically valid UUID pays
  that cost before the `"not_found"` lookup runs.
- **`start()`'s Postgres reachability check can now say "pool busy" instead of "unreachable".**
  When the bounded check (see [Pool sizing](#pool-sizing) and
  [Postgres-only mode](#postgres-only-mode)) never got a connection at all because the pool was
  already at `max` or had checkouts queued, the `MinitriggerError` now says so explicitly instead
  of implying Postgres itself is down — see the wording note in both sections below.
- **Debounce with `transport: "postgres"`: a pending run already past its own due time no longer
  absorbs triggers** — see the note at the end of [Debounce](#debounce). `transport: "bullmq"` is
  unchanged from 0.3.x: a debounced run stays collapsible until a worker actually acquires it,
  however far past its own `delay`/`maxDelay` deadline that takes.
- **Mixed-transport guard, and moving a schema between transports** — see
  [Postgres-only mode](#postgres-only-mode). In short: `schema_meta` (new in this migration) now
  records which transport a schema is used with, and a `start()` with the other transport on the
  same schema is `ConfigError`. Engines that never call `start()` (trigger-only producers) only
  read the marker, and refuse their writes with the same `ConfigError` — including `runs.cancel`/
  `tokens.complete` on any well-formed id, even one that doesn't resolve to a run/token, and
  `trigger(..., { tx })`, whose read goes through your own transaction (grant `SELECT` on
  `<schema>.schema_meta` to the `tx` role, or that path's check is silently skipped instead of
  aborting your transaction on a permission error). Engines on 0.3.x and
  earlier never wrote that marker, so moving an existing schema to `transport: "postgres"` requires
  upgrading every engine on it to 0.4.0+ first (see the linked section for the full procedure).
- **`start()`'s reachability check can now name a busy pool instead of an unreachable database**,
  and a legitimate wake on `transport: "postgres"` can wait up to ~15 ms behind a `NOTIFY` flood —
  see the notes in [Pool sizing](#pool-sizing) and [Postgres-only mode](#postgres-only-mode).
- **Type-checking with `skipLibCheck: false` may need `@types/node`.** `pg`'s own `.d.ts` (already
  needed via the optional `@types/pg` peer) references Node core types; with `skipLibCheck: false`
  those get checked too, not just skimmed. `scripts/check-pack.mjs`'s own `skipLibCheck: false`
  check installs both `@types/pg` and `@types/node` for exactly this reason — do the same if your
  build sets `skipLibCheck: false` and you weren't already pulling `@types/node` in transitively.

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
- **Migration `008_runs_active_created_idx`** adds a partial index on `runs (created_at, id)`
  holding only `queued`/`running`/`waiting` runs (it serves `runs.cancelMany()`). It is a plain
  `CREATE INDEX IF NOT EXISTS`, so on a large table either accept the `SHARE` lock on `runs` while
  it builds, or pre-build it with the same name — outside a transaction, against the target schema,
  before `engine.migrate()` — and let the migration just record version 8:

  ```sql
  SET search_path TO "<schema>";
  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_active_created ON runs (created_at, id)
    WHERE status IN ('queued', 'running', 'waiting');

  -- Must return indisvalid = t; if f (interrupted build), DROP INDEX CONCURRENTLY and rebuild,
  -- otherwise the migration's IF NOT EXISTS would keep the invalid index.
  SELECT indisvalid FROM pg_index
  WHERE indexrelid = '"<schema>".idx_runs_active_created'::regclass;
  ```
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
  [Structured logging](#structured-logging-attachdefaultlogger). It reads `engine.logger` when no
  logger is passed, a new read-only field on `Engine` (`= config.logger`; not on `EngineClient`/
  `TestEngine`).
- **`durationMs`/`startedAt` on `engine.events`.** `run:completed`/`run:failed`/`run:retrying`/
  `run:waiting` now carry `startedAt` (DB clock) and `durationMs` (monotonic clock, never
  persisted) — see [`engine.events.on`](#run-introspection-events-and-streams). They measure the
  **current execution** since its last acquisition/resume, not the attempt summed across
  suspensions.
- **Test helpers**, from the `/testing` subpath: `assertTriggered`/`refuteTriggered`/`allTriggered`
  (assert what an `Engine`/`TestEngine` triggered) and `runTask` (unit-test one task's `run`
  function directly, no engine/store/queue) — see
  [`assertTriggered` / `refuteTriggered` / `allTriggered`](#asserttriggered--refutetriggered--alltriggered)
  and [`runTask`: unit-testing one task's logic](#runtask-unit-testing-one-tasks-logic).
- **Reads keep working while `stop()` drains, writes don't.** During a graceful `stop()` (workers
  closing, active runs draining, connections being released), reads plus the handful of calls a
  graceful drain still needs to finish cleanly — `runs.get`/`list`/`logs`/`steps`, `runs.cancel`,
  `tokens.complete`, `queues.list`, `schedules.list` — keep working right up until `stop()`
  finishes closing everything (its very last step, just before closing a pool this engine
  created); only after that do they throw `"this engine was stopped; create a new one"`. Calls
  that start new work — `trigger`, `runs.replay`/`subscribe`/`readStream`/`cancelMany`/
  `retryFailed`, `queues.pause`/`resume`/`scale`, `schedules.upsert`/`delete` — throw that same
  error as soon as `stop()` **begins**, not when it finishes, since starting new work while the
  engine is on its way out would fight the shutdown. `TestEngine` *does* have a `stop()`
  (`await testEngine.stop()`), and it enforces the same "begins vs. finishes" split for the
  write-side calls above (`trigger`, `cancelMany`, `retryFailed`, `queues.pause`/`resume`/`scale`,
  `schedules.upsert`/`delete` all throw right after `stop()` is called) — it just never enforces
  the second, "fully closed" half of the split for the read-plus-drain group, so those keep
  working for the lifetime of the object, even long after `stop()` resolved.

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
  doing anything else, so a fully unreachable Redis fails fast instead of hanging, with a
  `MinitriggerError` ("cannot reach Redis", the `PING` error as `cause` when there is one; before
  0.4.1 this was a `ConfigError`). It then opens
  (or reuses) the dedicated pub/sub subscriber connection and waits for its `SUBSCRIBE` to become
  ready — also bounded to 5 s. The shared connection working is not enough: a *new* connection
  (the subscriber) can still fail to become ready (Redis refusing new connections over
  `maxclients`, or dropped right after that first `PING`), which without this bound would hang
  `start()` forever (the subscriber's `maxRetriesPerRequest: null` retries indefinitely). Past 5 s,
  `start()` rejects with a `MinitriggerError` (transient infrastructure, not `ConfigError`) naming
  the likely cause (check `maxclients`/`CLIENT LIST` or the network) instead of hanging; fix it
  and call `start()` again. Right after the `PING`, Postgres gets the same treatment: an
  unreachable database rejects with a `MinitriggerError` ("cannot reach Postgres", the driver
  error as `cause`) within `pool.connectionTimeoutMs`, never the raw `pg` error. When the check
  never got a connection at all because the pool was already at `max` or had checkouts queued, the
  message says so instead ("...every connection of the pool (max N) stayed busy...") — the
  database itself may be fine; see [Pool sizing](#pool-sizing).
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
bookkeeping queries need headroom beyond what your workers consume), plus `systemConcurrency`
with `transport: "postgres"` — see the note below. On top of the pool, every **started**
engine holds **up to two** dedicated Postgres connections outside it (leader election, and the
`LISTEN` connection: the outbox's in BullMQ mode, the wake/status/cancel one in Postgres mode) —
fewer while either is still connecting or backing off after a dropped connection, and momentarily
fewer if one fails to (re)connect — so budget `max_connections` for `processes × (pool.max + 2)`. A pool the engine creates
gets that size by default (at least 10) and a 10 s `connectionTimeoutMillis`
(`pool.connectionTimeoutMs`). If you inject your own `pg.Pool`, size it the same way and set a
`connectionTimeoutMillis`: `start()` warns when it is smaller than the started queues need or
has no acquisition timeout (a heartbeat would then wait for a connection without limit instead of
failing with a warning). A separate, dedicated pool for the library is recommended over sharing
your application's pool.

The `+5` headroom above is a fixed constant, not a count of `systemConcurrency`'s own queries.
The recommended `pool.max` (the default of a pool the engine creates, and the threshold of
`start()`'s warning) is:

- `transport: "bullmq"`: `max(10, sum of task-queue concurrency + 5)`. It never adds
  `systemConcurrency` (default `5`): the `-system` jobs run on a BullMQ worker, and their
  Postgres queries share the `+5` headroom. If you raise `systemConcurrency` well above its
  default, size `pool.max` for that yourself; the warning won't account for it.
- `transport: "postgres"`: `max(10, sum of task-queue concurrency + systemConcurrency + 5)`. The
  `-system` worker claims and runs its jobs against this pool, so its concurrency is counted.

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

(Everything above this point in [Operation](#operation) is specific to `transport: "bullmq"`; read
on for the Postgres-only equivalents.)

## Postgres-only mode

```ts
import { createEngine, defineTask } from "@rivia-labs/minitrigger";

const engine = createEngine({
	postgres: process.env.DATABASE_URL!,
	transport: "postgres",
	tasks: [myTask],
});

await engine.migrate();
await engine.start();
```

Since 0.4.0, `transport: "postgres"` (decision 28 in `docs/SPEC.md`) runs the whole engine — queue,
delay, wake-up, the internal `-system` queue, and pub/sub — directly on Postgres: no Redis, no
BullMQ, and `bullmq`/`ioredis` don't need to be installed at all. `transport: "bullmq"` (the
default, entirely unchanged) is still the better fit once you already run Redis, or need Redis
Streams (`ctx.stream`) or a rate limit; reach for `"postgres"` when you'd rather not stand up Redis
in the first place, or already run enough Postgres capacity that a second moving part is pure
overhead. Outside what's listed below, the two transports are equivalent: same `defineTask`, same
`ctx.step.*`, same replay/memoization, same store contract, same `docs/store-contract.md`.

### When to use it

- You want durable tasks without adding Redis as an operational dependency.
- Your Postgres already has the connection headroom for another `pool.max` worth of connections per
  process (see [Connections per engine](#connections-per-engine) below) and can tolerate the
  extra claim/poll query traffic against `runs`.
- You don't need `ctx.stream`/`runs.readStream`, `queue.rateLimit`, or a Redis-backed
  `globalConcurrency` that's approximate under heavy racing (this transport's is exact instead, see
  below) — none of those exist in this mode.

### What's not available

| Field / call | Result |
|---|---|
| `redis`, `prefix` | `ConfigError` at `createEngine()` — no Redis in this mode; NOTIFY channels derive from `schema` instead. |
| `outbox.*` | `ConfigError` at `createEngine()` — `trigger(..., { tx })` already writes straight into the row the claim reads (decision 28(j)); there's no separate outbox to tune. |
| `queue.rateLimit` | `ConfigError` at `createEngine()` — not implemented for this transport (no SQL rate limiter yet). |
| `ctx.stream(name, source)` | `ConfigError` thrown to the task when called; like an invalid step argument, it fails the run without retry, even if the task catches it. |
| `runs.readStream(run, name)` | `ConfigError` thrown when called. |

### How it works

- **The queue is the `runs` table itself.** A `queued`/`waiting` row with `run_after <= now()` is a
  due job; a worker claims a batch with `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)` in
  one statement, writing the same fields the BullMQ mode's `acquire` does (`status = 'running'`, a
  fresh `execution_id`, `heartbeat_at`, `started_at` if unset). `trigger()` becomes one atomic
  write: nothing can be lost between "the run exists" and "the run is queued" (there's no separate
  enqueue step to drop). The reconciler's lost-enqueue cases (BullMQ mode's cases 1/2) don't run
  under this transport as a result — a run sitting past its `run_after` is ordinary backlog, not a
  sign anything was lost.
- **Wake-up is `NOTIFY`, with an adaptive poll as the safety net.** Every enqueue (insert, resume,
  retry, hand-back, stall recovery, a debounce collapse) also fires a `pg_notify` naming the queue;
  every started engine `LISTEN`s on one dedicated connection and wakes just that queue's claim loop
  (an unrecognized or forged payload wakes every registered queue instead — the payload is always a
  hint, never trusted, since any role with `CONNECT` can `NOTIFY` these channels). If the
  notification is lost (a reconnecting listener, a pooler in front of it that breaks `LISTEN`, no
  engine started yet), every worker that didn't fill its batch also polls: it reads the queue's
  earliest `run_after` and sleeps until then, capped at `polling.intervalMs` (default 1 000 ms,
  minimum 100) plus up to `polling.jitterMs` of jitter (default 50 ms, spreading out the thundering
  herd a burst of `NOTIFY`s would otherwise cause). So worst-case wake-up latency without `NOTIFY`
  is bounded by `polling.intervalMs`, never indefinite. Because the wake channel is untrusted input
  (any role with `CONNECT` can `NOTIFY` it), incoming wakes that turn up nothing to claim are
  rate-limited (a token bucket: a burst of 4, then one every 15 ms) so a `NOTIFY` flood can't spin
  the claim loop — a legitimate wake arriving while a flood is in progress can wait up to that same
  ~15 ms before its claim runs. Ordinary traffic (a real trigger, a token completing) is never
  throttled: only wake-triggered passes that claimed nothing count against the limit.
- **`globalConcurrency` is exact.** With a limit, the claim runs inside a short transaction that
  first takes a per-queue advisory lock, then counts the queue's `running` rows once — concurrent
  claims of the same queue serialize on that lock, so the `running` count across every process
  never exceeds the limit. No approximation, unlike relying on Redis-side counting under racing
  claims.
- **The internal `-system` queue is a table, not a BullMQ queue.** `system_jobs` (migration 009)
  holds waitpoint expirations (`wp-<id>-expire`), retention sweeps (`retention-<window>`) and cron
  ticks (`schedule-tick:<id>`), claimed the same way (`SKIP LOCKED`) with a fencing token
  (`locked_by`): a worker whose lock expired can never complete, delay or reschedule a row another
  worker has since reclaimed. The reconciler is not a job: the leader runs it inline on its own
  timer (`reconciler.intervalMs`), as in BullMQ mode.
- **Cron ticks never disappear on failure.** Instead of a BullMQ job scheduler, a schedule is one
  row (`schedule-tick:<id>`) that's *rewritten in place* to its next occurrence when the tick
  succeeds or exhausts its retries — never deleted and recreated — so there's no window where the
  schedule doesn't exist. Missed ticks (no process running) are still not backfilled, same as
  BullMQ mode; a tick that has to retry (backoff) delays its own following ticks, since there's
  never more than one in flight for a given schedule.

### Example: a full setup

A fuller setup than the one at the top of this section, with a shared `pg.Pool` and graceful
shutdown. Everything else — `defineTask`, steps, `trigger` (including `{ tx }` with the ORM
adapters), schedules, queues, events, `runs.subscribe` — is written exactly as in the examples
earlier in this README:

```ts
import { createEngine, type PostgresEngineConfig } from "@rivia-labs/minitrigger";
import { Pool } from "pg";

// Your own pool: size it for the queues' concurrency + systemConcurrency + 5, and set a timeout.
const pool = new Pool({
	connectionString: process.env.DATABASE_URL,
	max: 40,
	connectionTimeoutMillis: 10_000,
});

const config: PostgresEngineConfig = {
	transport: "postgres",
	postgres: { pool },                       // never closed by the engine
	schema: "jobs",
	tasks: [approveExpense, publishAlbum, resizeImage, notifyFollowers],
	polling: { intervalMs: 500, jitterMs: 50 }, // fallback when a NOTIFY is missed (default 1 000)
};
const engine = createEngine(config);

await engine.migrate();
await engine.start();

process.once("SIGTERM", async () => {
	await engine.stop(); // finishes or hands back running runs
	await pool.end();
});
```

### Latencies

- **Wake-up:** milliseconds via `NOTIFY` when it arrives; bounded by `polling.intervalMs` when it
  doesn't (see above).
- **`globalConcurrency`:** exact, with only the advisory lock's own (typically sub-millisecond)
  contention as extra latency.
- See [Benchmark](#benchmark) (and the fuller table in `CLAUDE.md`) for both transports measured
  side by side on the same machine: the Postgres transport met the design's acceptance bar
  (throughput within ±20% of BullMQ, sleep-resume p99 ≤ 150 ms) with zero extra tuning — in
  practice it came out ahead on both throughput and sleep-resume latency, since a `NOTIFY`-driven
  resume skips Redis's own delayed-job polling.

### Connections per engine

`pool.max`, when the engine creates its own pool, is derived as `max(10, sum of task-queue
concurrency + systemConcurrency + 5)` in this mode: the `-system` worker runs against the pool too
(there's no separate Redis-backed `-system` worker to hold its own connection), unlike `"bullmq"`
mode, where the formula doesn't add `systemConcurrency` (see [Pool sizing](#pool-sizing)). On top of
the pool, a **started** engine holds **up to two** dedicated connections outside it: the `LISTEN`
connection (wake/status/cancel notifications) and the leader-election client — fewer while either
is still connecting or backing off after a dropped connection. Size `max_connections` for
`processes × (pool.max + 2)`, the same shape as the BullMQ-mode outbox note in
[Transactional trigger](#transactional-trigger-outbox). If you inject your own `pg.Pool`, size it
the same way and set a `connectionTimeoutMillis` — `start()` warns otherwise, same as in BullMQ
mode.

Postgres is this mode's only dependency, so its reachability plays the role of Redis's `PING` in
BullMQ mode: with the database unreachable, `start()` rejects with a `MinitriggerError` ("cannot
reach Postgres", the driver error as `cause`) instead of hanging — within
`pool.connectionTimeoutMs` (default 10 s; for an injected pool, its own `connectionTimeoutMillis`
when set), whether the engine created the pool or you injected one, with or without
`connectionTimeoutMillis`: the check runs on its own timer, and a connection that only completes
after it is destroyed, never kept in the pool. Nothing is left running, and `start()` can be
called again once the database is back. BullMQ mode runs the same check right after the Redis
`PING`. An injected pool without `connectionTimeoutMillis` still keeps one slot held by
the abandoned connection attempt until the OS gives up on it (`pg-pool` cannot cancel a pending
connect), so a small injected pool can keep failing `start()` in the meantime: set
`connectionTimeoutMillis` on pools you inject. The error message distinguishes the two causes: a
pool that never handed out a connection at all because it was already at `max` or had checkouts
queued says so explicitly ("...every connection of the pool (max N) stayed busy...") instead of
implying Postgres itself is unreachable — check what else holds the pool's connections before
assuming the database is down. `stop()` resolves within its usual bounds with the database gone, whether `start()`
failed or the engine was running (a run it could not hand back stays `running` until another
engine's stall recovery).

### PgBouncer

A transaction-mode pooler breaks both dedicated connections this mode needs held open across
statements: the `LISTEN` connection (a transaction-mode connection doesn't keep `LISTEN` state
across transactions) and the leader's session-level advisory lock. `start()` runs a self-test right
after connecting — a `NOTIFY` on the wake channel that must echo back to its own `LISTEN` within
1 s — and only `warn`s if it doesn't ("notifications do not arrive... wake-up latency falls back to
the poll"), so a pooler in front of these two connections doesn't fail `start()`; it just silently
drops to poll-only wake-ups. Leader election breaks in a less obvious way: the session-level
advisory lock stays on whichever server connection ran `pg_try_advisory_lock`, and an elector that
won never checks the lock again. Another engine whose attempt the pooler routes to that same server
connection also gets the lock (two leaders at once); the pooler recycling that server connection
frees the lock while the first engine still believes it leads (another one can then win too); and
an unlock or a closed client never reaches that server connection, so the lock can stay leaked (no
leader until it is recycled). So expect duplicate or unstable leaders, and at times none (see
[No leader](#no-leader) below), not a clean "no leader". No self-test detects this: point the
engine's `postgres` at a session-mode pool, or directly at Postgres, for millisecond wake-ups and a
working leader election; ordinary claim/heartbeat/
completion queries through the main pool work fine behind a transaction-mode pooler either way
(they're all single statements).

### `NOTIFY` visibility

`pg_notify`/`LISTEN` only need `CONNECT` on the database — no extra grant for the application role.
That also means **any role with `CONNECT` can `LISTEN`** on this mode's channels (their names
derive from the schema name, so they are easy to guess). What such a role sees: the `runId`s on the
status and cancel channels, the **queue names** on the wake channel, and the **timing** of every
enqueue, status change and cancel — never payloads, outputs, or progress (every message carries
only a bare id or a queue name, and every subscriber re-reads Postgres before acting on one). If
run ids, queue names or activity patterns are sensitive in your setting, use a database dedicated
to this engine rather than one shared with other roles/teams.

The same role can also `NOTIFY` on those channels. Wake and status notifications are only hints
(a forged one costs a claim pass or a re-read; wake-triggered passes that claim nothing are
rate-limited), and a forged cancel never aborts a run that isn't `canceled` in Postgres. The
`start()` self-test is not authenticated either: its echo carries a random token, but a role that
sees that token on the wake channel can send it back. A forged echo can only hide the "notifications
do not arrive" warning; it never changes what the engine does.

### No leader

Without a leader (every engine's advisory-lock attempt losing, or no engine started at all), the
reconciler and retention sweeps don't run — but ordinary run execution, cron ticks (any `-system`
worker of any started engine claims `schedule-tick` rows; no leadership check gates it) and the
claim/wake/poll loops all keep going unaffected. The practical effect of no leader for a while: a
stalled run isn't recovered, a lost enqueue (rare in this mode to begin with, see above) isn't
reconciled, and finished runs pile up without being deleted — all bounded and self-healing once some
engine successfully becomes leader again.

### Mixed-mode guard, and moving a schema between transports

A schema only ever has one transport at a time: the first `start()` (either mode) records it in
`schema_meta` (`key = 'transport'`), and any later `start()` on the same schema with the *other*
transport gets `ConfigError` — a BullMQ engine and a Postgres engine sharing one schema would be two
independent sources of `-system` jobs (racing to run the same cron tick, expire the same waitpoint,
and so on).

An engine that never calls `start()` (a trigger-only producer) never writes the marker, but reads
it before its first write (`trigger`, including `{ tx }`; `runs.replay`/`cancel`/`cancelMany`/
`retryFailed`; `tokens.complete`; `queues.pause`/`resume`; `schedules.upsert`/`delete`) and
refuses the write with the same `ConfigError` when the schema belongs to the other transport. A
recorded mode is read once per engine. While no engine has recorded one yet (or the schema has no
`schema_meta` table, not migrated to 009 yet), writes are allowed and the marker is read again at
most every 30 s, so a producer can keep writing for up to 30 s after an engine of the other mode
first starts on that schema. `runs.cancel`/`tokens.complete` pay this read (and, with
`transport: "bullmq"`, load the backend — opening Redis on a trigger-only engine) on their very
first call even when the run/token id turns out not to resolve to anything: any well-formed UUID
reaches the guard before the `"not_found"` lookup runs.

With `{ tx }`, the read goes through your own transaction instead of the pool (two read-only
statements: `to_regclass`, then the marker itself) — see
[Transactional trigger](#transactional-trigger-outbox). **A `tx` role without `SELECT` on
`<schema>.schema_meta` gets no guard on that path**: the missing privilege is treated the same as
an absent marker (write allowed, re-read after 30 s) rather than raising a permission error that
would abort your transaction — grant `SELECT` on `<schema>.schema_meta` to whatever role runs
`tx` triggers to keep the check active there too.

**Engines on 0.3.x and earlier never write that marker** (it's new in migration 009). To move an
existing schema from `"bullmq"` to `transport: "postgres"`:

1. Upgrade every engine on that schema to 0.4.0+ first (they can keep running `transport: "bullmq"`
   meanwhile — upgrading the package version alone doesn't switch transports; an 0.4+ engine still
   defaulting to `"bullmq"` writes the marker as `"bullmq"` on its next `start()`, same as before).
2. Stop every engine on that schema.
3. Make sure `system_jobs` is empty and that BullMQ has no pending jobs left in the old
   `${prefix}-system` queue (waitpoint expirations, schedule ticks, a queued retention sweep) — the
   Postgres transport's `-system` never looks at BullMQ, so anything still sitting there is silently
   abandoned once you switch.
4. `UPDATE "<schema>".schema_meta SET value = 'postgres' WHERE key = 'transport';` (or delete that
   row and let the next `start()` with `transport: "postgres"` write it fresh).
5. Start the engines with `transport: "postgres"`.

There's no separate data migration for in-flight runs/waitpoints/schedules across the switch — they
were always Postgres rows (Postgres is the source of truth in both modes); only which side reads and
writes the `-system` jobs changes.

### `system_jobs` autovacuum

Rows are deleted as soon as a job completes (waitpoint expirations, retention sweeps) or exhausts
its retries — schedule ticks are the exception, rewritten in place (see above) — so this table
churns heavily relative to its size: expect a steady stream of dead tuples. Give it the same
attention you'd give any other high-churn table — a lower `autovacuum_vacuum_scale_factor` (a
per-table `ALTER TABLE system_jobs SET (autovacuum_vacuum_scale_factor = ...)` in the target schema)
keeps it from bloating between the database-wide autovacuum thresholds.

### Slow `-system` jobs (retention)

A claimed `system_jobs` row is locked for `stallTimeoutMs` (`locked_until`), and a running handler
never renews that lock. A handler that runs longer than `stallTimeoutMs` — in practice a large
retention sweep — can be claimed again by another `-system` worker and run twice in parallel. The
design accepts this: retention deletes with `SKIP LOCKED` (a second sweep only costs queries), a
waitpoint expiration re-checks the waitpoint before acting, a cron tick collides on its idempotency
key (`__schedule:<id>:<ts>`), and the `locked_by` fencing lets only the current owner complete or
reschedule the row. A sweep keeps deleting batches until the backlog is gone, so if sweeps
regularly run past `stallTimeoutMs`, run retention more often (`retention.intervalMs`, less to
delete per sweep) or raise `stallTimeoutMs`.

### Mode differences worth knowing

- **`trigger(..., { tx })` emits no `run:queued`.** In BullMQ mode, the dedicated outbox listener
  observes the caller's commit and publishes `queued` when it dispatches the run. In Postgres mode
  there's no separate outbox dispatcher — the insert already wrote the row a claim can pick up, and
  the engine never separately observes "the caller's transaction committed" as its own moment — so
  the first event/status a `{ tx }` trigger's run produces is `run:started`, once some worker claims
  it. Key off `run:started` (or poll `runs.get`) instead of `run:queued` for transactional triggers
  under this transport.
- **`runs.subscribe` may coalesce closely spaced intermediate statuses.** Status changes are grouped
  into batched `NOTIFY`s, and the subscriber re-reads the current row rather than replaying every
  individual message, so two statuses published within the same short window can collapse into only
  the later one being delivered. The **terminal** status (`completed`/`failed`/`canceled`) is always
  delivered. If you need every intermediate status, subscribe to `engine.events` in the same process
  instead — that's a direct in-process emit, no round trip through Postgres to coalesce against.
- **No streams, no rate limit** — see [What's not available](#whats-not-available) above.
- **Call `stop()` after a failed `start()`.** If `start()` throws (e.g. the `LISTEN` connection
  timed out), the listener client keeps trying to reconnect in the background regardless — call
  `await engine.stop()` to shut it down cleanly before discarding the engine, or before retrying
  `start()` on the same instance (also supported).
- **A retried `start()` reconnects the listener right away.** Calling `start()` again after a
  failed one skips the listener's reconnect backoff (0.5–30 s) and tries to connect at once; it
  still waits at most 5 s for `LISTEN` to be ready.

## Benchmark

`pnpm bench` (`bench/run.ts`) runs against a disposable Postgres schema/Redis prefix (or, with
`pnpm bench -- --transport postgres`, a disposable Postgres schema alone, no Redis) and measures
end-to-end throughput (1,000 runs of a 3-step task) and resume latency (sleep, token). Order of
magnitude on a recent laptop: ~1.3k-1.5k runs/s throughput, ~15-100 ms sleep-resume p50, ~3-10 ms
token-resume p50 for either transport — treat these as ballpark, not an SLA; see `CLAUDE.md`
("Benchmark") for the full methodology, machine and result tables (BullMQ and Postgres transports
measured side by side). `pnpm check:pack` builds, `pnpm pack`s and installs the resulting `.tgz` in
a clean temp directory, confirming ESM `import`, CJS `require`, a `tsc --strict` check against the
shipped `.d.ts`, and (against the docker Postgres, `pnpm infra:up` — no Redis needed for this part)
that `transport: "postgres"` works with neither `bullmq` nor `ioredis` installed all work — the
same path a [GitHub Releases install](#install) exercises.

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
`pnpm check:pack`, before `pnpm publish`** — publishing does not do it for you. `pnpm check:pack`
needs the docker **Postgres** (`pnpm infra:up`) for its `transport: "postgres"` smoke run — Redis is
deliberately *not* required (it proves the package works with neither `bullmq` nor `ioredis`
installed).

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
