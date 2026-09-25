# API research notes — minitrigger

Pinned versions researched: `bullmq@6.3.8`, `ioredis@5.11.1`, `pg@8.23.0`, `tsup@8.5.1`,
`vitest@5.0.1`, `typescript@6.0.3`.

Method: Context7 docs (`/taskforcesh/bullmq`, `/redis/ioredis`, `/brianc/node-postgres`,
`/egoist/tsup`, `/vitest-dev/vitest`) cross-checked against the actual published `.d.ts`/`.js`
sources, extracted via `npm pack <pkg>@<version>` into the scratchpad and read directly. Where
docs and typings disagreed, typings/source win and are called out below.

---

## 1. BullMQ 6 — what changed vs v5 (breaking)

BullMQ 6.3.8's `package.json` now declares:

```json
"peerDependencies": {
  "bullmq-otel": ">=2.0.0",
  "ioredis": ">=5.0.0",
  "pg": ">=8.0.0",
  "redis": ">=5.0.0"
}
```
all without `peerDependenciesMeta.optional` markers being visible from the tarball, but the
source (`dist/esm/classes/redis-connection.js`) explicitly lazy-loads `ioredis` only when
needed and documents that "users on another Redis driver (node-redis, Bun built-in, …) or on
the PostgreSQL backend never hit this path, so they never need `ioredis` installed" — i.e.
these are effectively optional peers even though the raw `peerDependencies` block doesn't show
`optional: true` in this tarball's `package.json` text (double-check `npm info bullmq peerDependenciesMeta`
if you need the literal flag before wiring install warnings).

### 1.1 New backend abstraction (the headline change)

BullMQ 6 introduced an `IQueueBackend` interface (`dist/esm/interfaces/queue-backend.d.ts`) that
`Queue`, `Worker`, `Job`, `FlowProducer` are now written against instead of talking to Redis
directly. Shipped backends:

- **Redis backend** (`RedisQueueBackend`, `dist/esm/classes/redis-queue-backend.d.ts`) — the
  default. Built on `RedisConnection`, which itself supports three underlying drivers via an
  adapter (`IRedisClient`): **ioredis** (`ioredis-client.d.ts`), **node-redis**
  (`node-redis-client.d.ts`), and a **Bun-native** client (`bun-redis-client.d.ts`). There's also
  a `RedisConnection.clientFactory` static hook to plug in a custom driver (e.g. valkey-glide).
- **PostgreSQL backend** (`dist/esm/postgres/postgres-queue-backend.d.ts`,
  `create-postgres-backend.d.ts`, `postgres-connection.d.ts`) — a from-scratch SQL implementation
  of the same `IQueueBackend` contract, selected via `setDefaultBackendFactory(...)` or by
  passing a `backendFactory` argument to `new Queue(name, opts, backendFactory)` /
  `new Worker(...)`.

This is why `pg` and `redis` (node-redis) showed up as new peer deps in v6 — they back
*alternative* BullMQ backends, not something minitrigger needs to install just to use BullMQ
with ioredis. **For minitrigger (ioredis + our own Postgres durable-state store), you do not
need the `pg` or `redis` peers at all** — only `ioredis` is required, and only if you don't pass
`RedisConnection.clientFactory` yourself. Don't confuse BullMQ's *own* optional Postgres queue
backend with minitrigger's separate use of Postgres for durability — they're unrelated features
that happen to share a peer dep name.

`Queue`/`Worker`/`FlowProducer` are generic over the backend type (`B extends IQueueBackend =
RedisQueueBackend`), so `new Queue('name', opts)` still gives you the Redis-backed class by
default with no code changes required from v5.

### 1.2 Other breaking changes (from BullMQ's own v5→v6 migration guide)

- **Minimum Node.js: 14.17.0** (`engines.node` in package.json — matches).
- **Removed** (replaced by Job Schedulers, see §6):
  - `Queue.add(name, data, { repeat })` / `Queue.addBulk(..., { repeat })`
  - `Queue.getRepeatableJobs()`
  - `Queue.removeRepeatable()` / `Queue.removeRepeatableByKey()`
  - the `Repeat` class entirely
  - Use `queue.upsertJobScheduler(...)`, `queue.getJobSchedulers()`, `queue.removeJobScheduler(...)` instead.
- **`Queue.resume()` is now `async`** — must be awaited (`await queue.resume()`); calling it
  fire-and-forget will race with jobs already being drained.
- **`debounce` job option removed** — replaced by `deduplication`. Source
  (`dist/cjs/classes/job.js`) throws at add-time if `debounce` is present on opts:
  `Error: 'Debounce option has been removed. Use deduplication option instead'`.
- **Custom `IQueueBackend` implementors**: method renames —
  `reprocessJob` → `retryFinishedJob`, `retryJobs` → `retryFinishedJobs`,
  `cleanJobsInSet` → `cleanJobsByState`, `isJobInList`/`isJobInZSet` → `isJobInState`.
  (Only relevant if you ever implement a custom backend; not needed for normal usage.)
- `QueueBaseOptions.blockingConnection` is now `@deprecated not in use anymore`.
- `KeyPrefixOptions.prefix` (the old `prefix` queue option) is now explicitly documented as
  **Redis-backend only** — other backends (e.g. Postgres) use a schema instead and ignore it.

### 1.3 Connection options — still an ioredis instance or a plain options object

`ConnectionOptions` (`dist/esm/interfaces/redis-options.d.ts`) is unchanged in shape:

```ts
export type ConnectionOptions = RedisOptions | ClusterOptions | IRedisClient | RedisConnectionClient;
```

So `connection:` still accepts either:
1. A plain options object (`{ host, port, password, db, ... }`) — BullMQ constructs its own
   ioredis client internally, OR
2. An already-constructed `ioredis.Redis` / `ioredis.Cluster` instance — BullMQ wraps it in a
   `Proxy` (`createIORedisClient`, `dist/esm/classes/ioredis-client.d.ts`) that:
   - adds a `runCommand` method for Lua-script dispatch by name,
   - adds structured-options variants of `hset`, `set`, `zrange`, `zrevrange`, `xadd`, `xread`,
     `xtrim`, `scan` (still backward compatible with native ioredis varargs calls),
   - wraps `pipeline()`/`multi()` results and `duplicate()` results in the same proxy.
   - **does not mutate** the original instance.

```ts
import { Queue, Worker } from 'bullmq';
import IORedis from 'ioredis';

const connection = new IORedis({
  host: 'localhost',
  port: 6379,
  maxRetriesPerRequest: null, // required, see 1.4
});

const queue = new Queue('emails', { connection });
const worker = new Worker('emails', processor, { connection });
```

Passing plain options (not an instance) is simpler for the `maxRetriesPerRequest` requirement —
see next.

### 1.4 `maxRetriesPerRequest: null` requirement

Source: `dist/cjs/classes/redis-connection.js`. Behavior differs depending on what you pass:

- **Plain options object** (`{ host, port, ... }`, not an ioredis instance): if the connection is
  used in blocking mode (Worker, or any blocking backend connection —
  `extraOptions.blocking !== false`), BullMQ **silently sets `maxRetriesPerRequest = null` for
  you** and prints:
  `console.error('BullMQ: WARNING! Your redis options maxRetriesPerRequest must be null and will be overridden by BullMQ.')`
  if you had set it to something else. You don't have to do anything, but you'll see the warning
  if you pass a non-null value.
- **Pre-built ioredis instance**: BullMQ does **not** mutate it. If `maxRetriesPerRequest` is not
  `null` on a blocking connection, it **throws**:
  `Error: 'BullMQ: Your redis options maxRetriesPerRequest must be null.'`
  — this is a hard failure, not a warning, when you hand it a live client. You must construct the
  ioredis instance with `maxRetriesPerRequest: null` yourself.

Gotcha: this means "pass an instance so we control lifecycle/pooling across Queue+Worker" (a very
common pattern to share one connection) **requires** you to set `maxRetriesPerRequest: null` at
instance-construction time, every time, including for non-blocking `Queue`-only usages that later
get reused by a `Worker`.

### 1.5 Custom `jobId` restrictions (verified in `dist/cjs/classes/job.js` around line 900)

```js
if (this.opts?.jobId) {
  if (`${parseInt(this.opts.jobId, 10)}` === this.opts.jobId) {
    throw new Error('Custom Id cannot be integers');
  }
  // legacy compat: colon allowed only if the id splits into exactly 3 segments
  // (old repeatable-job key shape); anything else with ':' throws.
  if (this.opts.jobId.includes(':') && this.opts.jobId.split(':').length !== 3) {
    throw new Error('Custom Id cannot contain :');
  }
}
```

- **Purely numeric strings are forbidden** (`"123"` throws `Custom Id cannot be integers`) —
  they'd collide with BullMQ's own auto-generated numeric ids. Use a prefix: `"job-123"`, `"#123"`.
- **Colon (`:`) is forbidden** *except* when the id has exactly 2 colons (3 segments) — a legacy
  carve-out kept only for backward compatibility with old repeatable-job key shapes
  (`repeat:<schedulerId>:<millis>`, see §6). Don't rely on this carve-out for your own ids; treat
  `:` as fully forbidden for anything you generate. Use `-` or `_` instead.
- Empty string is not special-cased here but an empty/falsy `jobId` just means "let BullMQ
  auto-generate one" (the `if (this.opts?.jobId)` guard skips falsy values).

### 1.6 Existing-jobId behavior (idempotent add)

From BullMQ's own docs (job-ids.md / throttle-jobs.md), confirmed in behavior:
- `queue.add(name, data, { jobId: 'x' })` when a job with id `x` **already exists** (in any
  active state: waiting/delayed/active/prioritized/waiting-children) → **the new job is silently
  ignored**; `add()` still resolves (returns a `Job` reflecting what's in Redis — practically the
  original job), no error is thrown.
- If the previous job with that id has already **completed or failed and was removed** (or
  `removeOnComplete`/`removeOnFail` evicted it), the id is free again and a new `add()` with the
  same `jobId` **creates a new job normally**. This is the classic gotcha: "idempotent add" only
  holds while the prior job instance's hash still exists in Redis — once GC'd, dedup resets.
- For minitrigger's idempotency needs (e.g. one job per workflow-step-attempt), pair `jobId` with
  `removeOnComplete: false`/a `KeepJobs` policy long enough to cover your at-least-once redelivery
  window, or track completion state yourself in Postgres rather than relying purely on BullMQ's
  jobId presence.

### 1.7 Worker options (from `dist/esm/interfaces/worker-options.d.ts`)

Relevant fields with exact defaults from the JSDoc:

| Option | Type | Default | Notes |
|---|---|---|---|
| `concurrency` | `number` | `1` | jobs processed in parallel per Worker instance |
| `limiter` | `{ max, duration }` | — | rate limiter (see §1.7.1) |
| `stalledInterval` | `number` (ms) | `30000` | how often the stalled-check runs |
| `maxStalledCount` | `number` | `1` | times a job may be recovered from stalled → wait before it's moved to `failed` |
| `lockDuration` | `number` (ms) | `30000` | how long a worker holds a job's lock before it's eligible to be considered stalled |
| `lockRenewTime` | `number` (ms) | `lockDuration / 2` | not recommended to change |
| `autorun` | `boolean` | `true` | if `false`, must call `worker.run()` manually |
| `drainDelay` | `number` (s) | `5` | long-poll duration when queue is empty |
| `skipStalledCheck` | `boolean` | `false` | **this worker** won't run stalled checks (other workers still can) |
| `skipLockRenewal` | `boolean` | `false` | lock will expire after `lockDuration` and (if stalled check isn't disabled) be moved back to wait |
| `maxStartedAttempts` | `number` | `undefined` | caps how many times a job may **start** processing (increments `attemptsStarted` every pickup, regardless of pass/fail); exceeding it fails the job with `UnrecoverableError` |
| `removeOnComplete` / `removeOnFail` | `KeepJobs` | keep | overridable per-job via job opts |
| `useWorkerThreads` | `boolean` | `false` | only with a file-based processor |
| `maximumRateLimitDelay` | `number` (ms) | `30000` | cap on idle time while rate-limited |

`limiter: { max, duration }` — `RateLimiterOptions`: max `max` jobs processed per `duration`
milliseconds, **per Worker instance** (not global across all workers on a queue — for that use
`Queue.setGlobalRateLimit`, §1.8).

#### 1.7.1 Minitrigger's stall design: `attempts: 1` + "redelivery is a no-op"

Given minitrigger's design (jobs use `attempts: 1`, and BullMQ-level stall detection is
superseded by Postgres-side stall tracking, with BullMQ redelivery treated as a no-op):

- **`maxStalledCount` does not prevent a job from ever reaching `failed`** — after
  `maxStalledCount` stall recoveries, BullMQ moves the job to `failed` regardless of `attempts`.
  With `attempts: 1`, a job that stalls even once and is *not* immediately redelivered — no,
  actually: stall recovery moves the job back to `wait` and it **will** be redelivered to a
  worker again, and this redelivery does **not** consume an `attempts` slot in the normal
  sense — `attemptsMade` is incremented on failure, not on stall-redelivery. So a stalled job can
  be redelivered `maxStalledCount` times before permanently failing, independent of `attempts:1`.
  To make stalls effectively "never fail the job" at the BullMQ layer (since your own Postgres
  layer owns real stall/redelivery semantics), set `maxStalledCount` to a very large number
  (e.g. `Number.MAX_SAFE_INTEGER` or a large practical constant) so BullMQ's stalled-checker
  essentially never gives up and just keeps recycling wait→active→(stall)→wait. Combine with:
  - `lockDuration` set to something comfortably larger than your expected worst-case processor
    runtime (so BullMQ's own stall detection rarely fires at all — you want *your* Postgres-side
    liveness/heartbeat tracking to be the source of truth, not BullMQ's).
  - Optionally `skipStalledCheck: true` on workers if you want **zero** BullMQ-level stall
    recovery and to handle 100% of redelivery yourselves via Postgres — but note the JSDoc
    caveat: "other workers could still perform stalled checks and move jobs back to wait for
    jobs being processed by this worker" — so `skipStalledCheck` must be set consistently on
    *every* worker for a queue if you want to fully disable it, or another worker's stalled
    checker will still recover your job.
  - `skipLockRenewal: true` is the more surgical knob if the goal is "let the lock expire and
    let *our* Postgres-side logic decide what happens next," while still leaving `stalledInterval`
    running as a safety net (large `maxStalledCount` means it just requeues instead of failing).

### 1.8 Queue global concurrency / rate limit (`dist/esm/classes/queue.d.ts`)

```ts
queue.setGlobalConcurrency(concurrency: number): Promise<number>
queue.removeGlobalConcurrency(): Promise<number>
queue.getGlobalConcurrency(): Promise<number | null>   // on QueueGetters, verified in queue-getters.d.ts
queue.setGlobalRateLimit(max: number, duration: number): Promise<number>
queue.removeGlobalRateLimit(): Promise<number>
```

`setGlobalConcurrency` caps total simultaneous *active* jobs across **all** workers consuming the
queue (not per-worker) — "setting this to 1 ensures no more than one job is processed at any
given time" across the whole queue. `setGlobalRateLimit` is the queue-wide equivalent of
per-worker `limiter`. There is no `getGlobalRateLimit` symmetric getter found in the typings —
only `getGlobalConcurrency` is exposed as a getter; rate limit state is read indirectly (e.g. via
`getRateLimitTtl` on the backend, not part of the public `Queue` surface).

### 1.9 Job options / removing delayed jobs / `getJob`

`DefaultJobOptions`/`BaseJobOptions` (`dist/esm/interfaces/base-job-options.d.ts`):
`delay`, `attempts`, `backoff`, `priority`, `lifo`, `removeOnComplete`, `removeOnFail`,
`keepLogs`, `stackTraceLimit`, `sizeLimit`, `jobId`, `parent`. `attempts` defaults to `1` (note:
JSDoc literally says `@defaultValue 1`, i.e. BullMQ's own default is already "no retries" unless
you opt in — consistent with minitrigger's `attempts: 1` design, meaning you can actually just
omit `attempts` entirely rather than set it explicitly, though being explicit is clearer).

Removing a delayed job:
```ts
await queue.remove(jobId, { removeChildren?: boolean }): Promise<number> // 1 = removed, 0 = locked/blocked
// or, if you already have the Job instance:
const job = await queue.getJob(jobId);
await job?.remove();
```
`Queue.getJob(jobId): Promise<Job | undefined>` — returns `undefined` if not found (does not
throw). Works for jobs in any state including `delayed`.

### 1.10 Job Schedulers

```ts
queue.upsertJobScheduler(
  jobSchedulerId: string,
  repeatOpts: Omit<RepeatOptions, 'key'>,          // { pattern? | every?, tz?, startDate?, endDate?, limit?, offset?, immediately? }
  jobTemplate?: { name?: string; data?: DataType; opts?: JobSchedulerTemplateOptions },
): Promise<Job>;

queue.removeJobScheduler(jobSchedulerId: string): Promise<boolean>;
queue.getJobScheduler(id: string): Promise<JobSchedulerJson | undefined>;
queue.getJobSchedulers(start?: number, end?: number, asc?: boolean): Promise<JobSchedulerJson[]>;
queue.getJobSchedulersCount(): Promise<number>;
```

`JobSchedulerTemplateOptions = Omit<JobsOptions, 'jobId' | 'repeat' | 'delay' | 'deduplication'>`
— you cannot set a custom `jobId`, `delay`, nested `repeat`, or `deduplication` on the template;
BullMQ derives these per iteration.

**How each scheduled tick gets a deterministic identity** (source:
`dist/cjs/classes/job-scheduler.js`, `getNextJobOpts` + `getSchedulerNextJobId`):

```js
getSchedulerNextJobId({ nextMillis, jobSchedulerId }) {
  return `repeat:${jobSchedulerId}:${nextMillis}`;
}
```

Every iteration's `Job.id` is **exactly** `repeat:<jobSchedulerId>:<nextMillis>` — this is
already a deterministic, per-tick idempotency key with no extra work needed. In addition, each
job produced by a scheduler carries on its `opts`:
- `job.repeatJobKey` — the scheduler id (`jobSchedulerId`). **Verified on bullmq 6.3.8: it is NOT on `job.opts` in the processor** (the `Job` constructor moves it to `this.repeatJobKey`); also put the id in the template `data` as a fallback.
- `opts.prevMillis` — the scheduled tick timestamp (same value baked into `job.id`)
- `job.timestamp` — wall-clock time the *iteration record* was created (not the same as the
  scheduled tick time; use `opts.prevMillis`/the `repeat:` id suffix for the tick's logical time,
  not `job.timestamp`).

So for minitrigger's own idempotency key per tick, prefer:
`` `${job.repeatJobKey}:${job.opts.prevMillis}` `` (equivalent to `job.id` minus the
`repeat:` prefix) rather than deriving anything from `job.timestamp`/`processedOn`.

**Missed iterations are not backfilled.** `JobScheduler.upsertJobScheduler`'s tick-advance logic
(`dist/cjs/classes/job-scheduler.js`):
```js
const prevMillis = opts.prevMillis || 0;
now = prevMillis < now ? now : prevMillis;
nextMillis = await this.repeatStrategy(now, repeatOpts, jobName); // cron-parser finds next fire *after* `now`
if (nextMillis < now) nextMillis = now;
```
It always computes the next occurrence relative to `Math.max(prevMillis, Date.now())`, i.e.
relative to *current real time*, not by walking forward one interval at a time from the last
tick. If your process/worker was down across several intervals of a cron/`every` schedule, on
resume there is exactly **one** delayed job scheduled for the next occurrence after "now" — all
missed occurrences in between are simply skipped, never produced. Only one delayed "next
iteration" job exists per scheduler at any time (the Lua script for `upsertJobScheduler`
explicitly removes the previous pending iteration from delayed/waiting/prioritized before
creating the new one when `override=true`, which `Queue.upsertJobScheduler` always passes).
Design implication: if minitrigger needs "catch up every missed tick" semantics, that must be
built on top (e.g. compute how many intervals were missed yourself and fan out extra jobs), not
assumed from BullMQ's scheduler.

### 1.11 `Worker.close(force?)` / `Queue.close()`

```ts
await worker.close();        // graceful: marks closing, stops picking up new jobs,
                              // waits for in-flight jobs to complete/fail; does not time out
await worker.close(true);    // force: closes immediately without waiting for active jobs
await queue.close();         // closes the queue's connection(s)
```
No built-in timeout on graceful `close()` — if you need a shutdown deadline, race it yourself:
`await Promise.race([worker.close(), delay(deadlineMs).then(() => worker.close(true))])` (or
similar), since an unbounded processor could otherwise hang shutdown forever.

`QueueEvents` is not required for minitrigger's design (per task framing) — it's an optional
separate class that consumes the queue's Redis event stream (`XREAD`) purely for pub/sub-style
job lifecycle notifications; `Queue`/`Worker` function fully without it.

### 1.12 Queue name / prefix

Source `dist/cjs/classes/queue-base.js`:
```js
if (!name) throw new Error('Queue name must be provided');
if (name.includes(':')) throw new Error('Queue name cannot contain :');
```
So queue names: **required, non-empty, must not contain `:`**. No other character restriction
enforced in code (alphanumerics, `-`, `_`, etc. are all fine — BullMQ just concatenates
`prefix:name:...` internally so `:` is reserved as the separator). `prefix` (default `bull`) is
Redis-backend-only per `KeyPrefixOptions` JSDoc (§1.2) — irrelevant if/when using another backend.

---

## 2. ioredis 5.x → be aware before considering 6.x

### 2.1 Recommended peer range: **pin to `ioredis` 5.x, do not allow 6.x yet**

```json
"peerDependencies": { "ioredis": ">=5.11.1 <6.0.0" }
```
or, more conservatively (matches exactly what BullMQ 6.3.8 itself tests against — its own
`devDependencies` pins `"ioredis": "5.11.1"` exactly for its default test suite):
```json
"peerDependencies": { "ioredis": "^5.11.1" }
```

Reasoning:
- BullMQ 6.3.8's own `peerDependencies` says `"ioredis": ">=5.0.0"` with **no upper bound**, so
  npm/pnpm will happily let `ioredis@6.0.0` satisfy it — but that's a peer-range oversight, not a
  tested compatibility claim. BullMQ's own test matrix pins ioredis to `5.11.1` exactly
  (`package.json` `devDependencies`).
- **ioredis 6.0.0 defaults to the RESP3 protocol** (`HELLO 3` handshake on connect). This is the
  single biggest risk for BullMQ compatibility: BullMQ's `ioredis-client.js` adapter (the
  wrapper described in §1.3) contains **no RESP3/`protocol` handling at all** — unlike its
  Bun-native client (`bun-redis-client.js`), which explicitly branches on "RESP3 returns a map,
  RESP2 returns flat array" for hash replies. The ioredis path assumes ioredis's v5 (RESP2) reply
  shapes for its Lua script results and raw command parsing.
- ioredis 6 also **requires Node.js ≥ 20** (`engines.node: ">=20.0.0"`, vs. 5.x's `>=12.22.0`) and
  bumps the supported Redis server floor from `2.6.12` to `6.2`.
- Other ioredis 6 changes per the official upgrade guide (`redis/ioredis` wiki
  "Upgrading-from-v5-to-v6"): a new `replyMapping` option (`"legacy"` default preserves v5 reply
  shapes, `"resp3"` returns native RESP3 types — maps as objects, doubles as numbers not strings,
  booleans as `true`/`false` instead of `0`/`1`), pub/sub no longer exclusively locks a RESP3
  connection into "subscriber mode" (regular commands remain usable while subscribed under
  RESP3; RESP2 connections keep the old exclusive subscriber-mode restriction), and Cluster/
  Sentinel option placement changed for RESP-related config (Cluster: under `redisOptions`;
  Sentinel: top-level).
- **Mitigation if you ever do want to try ioredis 6 with BullMQ**: explicitly pass
  `protocol: 2` in the ioredis options to force RESP2 (restores v5-compatible reply shapes) —
  this is the officially documented escape hatch for consumers who aren't ready for RESP3 — but
  this is unverified against BullMQ 6.3.8 specifically since BullMQ's CI doesn't test it. Until
  BullMQ's own peer range/tests bump to include ioredis 6, treat it as unsupported for
  minitrigger.

### 2.2 `duplicate()`, pub/sub subscriber mode

```ts
duplicate(override?: Partial<RedisOptions>): Redis;
```
Creates a new connection sharing connection options (optionally overridden), independent
socket/state. BullMQ itself uses `duplicate()` internally for dedicated blocking connections
(Worker's `waitForJob` primitive) — same pattern is safe/idiomatic for minitrigger's own uses
(e.g. a dedicated `SUBSCRIBE`/blocking client separate from the general-purpose command client).

Pub/sub subscriber mode (ioredis 5, RESP2 — what we're pinned to): once a connection calls
`subscribe()`/`psubscribe()`, it enters `"subscriber"` mode (`redis.mode` accessor:
`"normal" | "subscriber" | "monitor"`), and **only** `subscribe`, `psubscribe`, `unsubscribe`,
`punsubscribe`, `ping`, `quit` remain valid on that connection until the subscription set is
emptied again. **Always use a separate `duplicate()`d connection for pub/sub**, never share it
with regular command traffic.

### 2.3 Streams: `XADD ... MAXLEN ~`, `XREAD BLOCK`

```ts
await redis.xadd(streamKey, 'MAXLEN', '~', maxLen, '*', 'field1', 'value1');
// approximate trimming avoids O(N) exact trimming on every XADD
await redis.xread('BLOCK', timeoutMs, 'STREAMS', streamKey, lastId);
// timeoutMs = 0 blocks indefinitely; returns null on timeout, otherwise [[stream, [[id, fields[]]]]]
```
The structured-options `xadd`/`xread`/`xtrim` variants BullMQ's own client wrapper adds (§1.3)
are internal to BullMQ — plain ioredis usage from minitrigger code still uses the classic varargs
form shown above.

### 2.4 `CONFIG GET` when disabled, `status`, `quit` vs `disconnect`

- `status: 'wait' | 'reconnecting' | 'connecting' | 'connect' | 'ready' | 'close' | 'end'`
  (exact union from `built/Redis.d.ts`). Managed Redis providers (e.g. some cloud Redis) that
  disable `CONFIG` entirely surface a Redis `ERR unknown command 'CONFIG'` (or
  `NOAUTH`/`ERR This instance has cluster support disabled` style errors depending on provider) —
  ioredis does not special-case this; the promise simply rejects with whatever `ReplyError` the
  server returned. Don't rely on `CONFIG GET`/`SET` for feature-detection against managed Redis.
- `quit(): Promise<'OK'>` — graceful: waits for in-flight/queued commands to finish, then closes.
- `disconnect(reconnect?: boolean): void` — immediate: drops the connection right away,
  abandoning any queued/pending commands (they reject). `reconnect` (default false) controls
  whether ioredis should immediately try to reconnect afterward. Prefer `quit()` for graceful
  shutdown paths, `disconnect()` for hard-abort paths (e.g. a watchdog killing a wedged
  connection — this is exactly what BullMQ's own `resetBlockedConnection` helper does internally
  to recover a stuck blocking command).

---

## 3. pg 8.23.0

No bundled `.d.ts` — **`pg` ships no first-party TypeScript types**; you need `@types/pg` as a
devDependency for typings (confirmed: `find . -iname "*.d.ts"` in the packed tarball returns
nothing under `lib/`/`esm/`).

### 3.1 ESM import shape

```ts
// ESM: pg ships esm/index.mjs as a thin wrapper re-exporting named bindings
import pg, { Pool, Client, escapeIdentifier, escapeLiteral, types } from 'pg';
// or
import { Pool } from 'pg';
```
`escapeIdentifier`/`escapeLiteral` **are** available as named ESM exports (verified in
`esm/index.mjs`: `export const escapeIdentifier = pg.escapeIdentifier`). Both the default export
and named exports work; prefer named imports for clarity/tree-shaking intent (though pg's CJS
core means real tree-shaking won't happen). `pg`'s `exports` map:
```json
"exports": {
  ".": { "import": "./esm/index.mjs", "require": "./lib/index.js", "default": "./lib/index.js" }
}
```

### 3.2 Pool options

Defaults (from `lib/defaults.js`): `max: 10`, `idleTimeoutMillis: 30000`,
`connectionTimeoutMillis: 0` (no timeout by default — **you should always set this explicitly**
in production), `statement_timeout: false` (disabled), `query_timeout: false` (disabled).

```ts
const pool = new Pool({
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
  statement_timeout: 5000,   // ms — see 3.3
  query_timeout: 10000,      // ms — see 3.3
});
```

### 3.3 `statement_timeout` vs `query_timeout` — different enforcement layers

Verified in `lib/client.js`:

- **`statement_timeout`** (and `lock_timeout`, `idle_in_transaction_session_timeout`) are sent as
  **startup parameters** to Postgres (`getStartupConf()` → `SET statement_timeout = ...` at
  session start). This is **server-side enforcement**: Postgres itself cancels the statement and
  the client receives a real Postgres error (`57014 query_canceled`). It applies to *every*
  statement run on that connection/session unless overridden per-transaction
  (`SET LOCAL statement_timeout = ...`).
- **`query_timeout`** is a **client-side-only** JS `setTimeout` (`client.js` around the
  `readTimeout` logic): if the server hasn't replied within `query_timeout` ms, pg-node fires a
  local `Error('Query read timeout')` and resolves/rejects the query from the client's
  perspective — but **does not send a cancel request to Postgres**. The query keeps running
  server-side until it finishes or the session ends. If `pipeline: true` and the timed-out query
  was already sent (mid-flight), pg destroys the underlying socket to unblock the rest of the
  pipeline rather than leaving it wedged.
  - **Gotcha**: `query_timeout` alone does not stop server-side work — for actually bounding
    server-side query cost/time, `statement_timeout` is the one that matters. Use both together:
    `statement_timeout` as the real cutoff, `query_timeout` slightly larger as a client-side
    dead-client detector.

### 3.4 `pool.connect()` / `client.release(err)` / error events

```ts
const client = await pool.connect();
try {
  const res = await client.query('SELECT 1');
} catch (err) {
  client.release(err);   // passing an Error tells the pool to DESTROY this client,
                          // not return it to the idle pool — use whenever a query errors
                          // in a way that might have left the connection/session dirty
  throw err;
}
client.release();        // no-arg: return client to the pool for reuse
```
`pool.on('error', (err, client) => { ... })` — fires when an **idle** pooled client hits a
backend-level error (server restart, network partition). **You must attach this listener** — an
unhandled `error` event on the pool crashes the Node process, same as any other EventEmitter.
The offending client is auto-terminated/removed from the pool; it's passed to the handler only
for inspection.

For **dedicated clients** (a bare `new Client()`, not from a Pool — e.g. minitrigger's
`LISTEN/NOTIFY` connection), you must separately attach `client.on('error', ...)` — a `Client`
instance has no pool to auto-recover it, an unhandled error there will also crash the process,
and *you* are responsible for reconnecting.

Other pool events: `connect` (new backend connection established), `acquire` (client checked
out), `release` (client returned, `err` arg set if `release(err)` was called), `remove` (client
closed & evicted from pool).

### 3.5 Type parsing

- `timestamp`/`timestamptz` columns parse into JS `Date` objects on read (both types — the
  distinction is in how Postgres stores/interprets the value server-side, not in what pg-node
  returns). `date`/`timestamp` (no tz) are interpreted in `process.env.TZ`'s local time zone;
  microsecond precision in Postgres is truncated to millisecond precision in JS `Date`. Prefer
  `timestamptz` for anything minitrigger stores (avoids cross-timezone ambiguity across
  processes).
- `bigint`/`int8` (Postgres OID 20) is **returned as a `string`, not a `number`**, by pg's
  default type parsers — because a JS `number` can't safely represent the full int8 range. If you
  need it as a JS `bigint`, register a custom parser:
  `types.setTypeParser(20, BigInt)` (global) or `client.setTypeParser(20, BigInt)` (per-client,
  via `TypeOverrides` — confirmed in `lib/type-overrides.js`, which explicitly supports
  per-client override falling back to the global `types` parser).
- `json`/`jsonb` are auto-parsed into JS objects/arrays on read (no config needed).
- Register custom parsers globally via the `types` export:
  `import { types } from 'pg'; types.setTypeParser(oid, parseFn)`.

---

## 4. tsup 8.5.1

### 4.1 Dual ESM+CJS+dts config

```ts
// tsup.config.ts
import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  target: 'node24',
  clean: true,
  sourcemap: true,
  loader: {
    '.sql': 'text',   // embeds file contents as a string export — see 4.2
  },
});
```

`loader` is passed straight through to esbuild's `Loader` type
(`import { ... Loader } from 'esbuild'` in tsup's own `dist/index.d.ts`), which includes
`'text'` among its values (`'js' | 'jsx' | 'ts' | 'tsx' | 'css' | 'json' | 'text' | 'base64' |
'file' | 'dataurl' | 'binary' | 'copy' | 'default' | 'empty'`). `'text'` loads the file and
exposes its contents as a UTF-8 string default export — exactly what's wanted for embedding raw
`.sql` migration/query files as importable strings:
```ts
import upSql from './migrations/001_init.sql'; // upSql: string
```

`target` (default `'node14'` per `schema.json`) is forwarded verbatim to esbuild's `target`
option — `'node24'` is accepted as a string (esbuild does not hard-validate against a fixed
enum of Node versions; it uses the numeric portion to decide which JS syntax to preserve vs.
downlevel). Double check with a real build that esbuild's bundled feature-compat table has
explicit node24 data by the time you build in CI — if not, esbuild falls back to treating it as
"at least as new as the newest version it knows," which for an unbundled server package is not a
practical hazard (you're not shipping to older runtimes).

### 4.2 Dual-package `exports` map with separate `.d.ts`/`.d.cts`

tsup infers output extensions from your own `package.json`'s `"type"` field
(`defaultOutExtension`, verified in tsup's bundled source):
- if `package.json` has `"type": "module"` (ESM-first, which minitrigger should use): ESM output
  keeps `.js`/`.d.ts`, and the **`cjs` format output becomes `.cjs`/`.d.cts`**.
- if `"type"` is absent/`"commonjs"`: CJS output keeps `.js`/`.d.ts`, ESM output becomes
  `.mjs`/`.d.mts`.

For an ESM-first (`"type": "module"`) package built with `format: ['esm', 'cjs']`, you'd get
`dist/index.js` + `dist/index.d.ts` (ESM) and `dist/index.cjs` + `dist/index.d.cts` (CJS), and
the `package.json` `exports` map should be:

```json
{
  "type": "module",
  "main": "./dist/index.cjs",
  "module": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "import": { "types": "./dist/index.d.ts", "default": "./dist/index.js" },
      "require": { "types": "./dist/index.d.cts", "default": "./dist/index.cjs" }
    },
    "./package.json": "./package.json"
  }
}
```
Order matters within each conditions object: `"types"` **must come before** `"default"`
(TypeScript's `moduleResolution: "bundler"`/`"node16"`/`"nodenext"` resolves conditions in the
order listed, and expects the types condition to be checked first).

---

## 5. vitest 5.0.1

### 5.1 Prerequisites (breaking vs v3/v4)

- **Requires Node.js ≥ 22.12.0 and Vite ≥ 6.4.0** ("Running Vitest 5.0 on older versions of Vite
  or Node.js is not supported and may result in unexpected errors" — from the official migration
  guide). Confirmed by the packed tarball's own `engines`:
  `"node": "^22.12.0 || ^24.0.0 || >=26.0.0"`.
- Benchmarking API rewritten: `bench` is no longer a top-level import — it's now a test-context
  fixture destructured inside a `test()`. `bench.skip/only/todo` removed (use the surrounding
  `test.skip/only/todo`). `benchmark.reporters`/`outputFile`/`compare`/`outputJson` config and
  CLI flags removed — use `test.reporters`, `--reporter=json --outputFile=<path>`, and
  `writeResult`/`bench.from()` instead. `Vitest.mode` is now always `'test'` (no more
  `'benchmark'` mode).
- `coverage.thresholds.perFile` no longer auto-applies to glob-pattern thresholds — set `perFile`
  explicitly on each glob threshold entry that needs it.
- `coverage.include`/`coverage.exclude` now match relative-to-root paths without picomatch's
  `contains` option — patterns are stricter/more precise than v3/v4 (a pattern with no glob
  wildcard now matches a directory and everything inside it, rather than loosely substring
  matching absolute paths).
- jsdom/happy-dom: assignments to `globalThis`/`window` properties now propagate to the
  underlying DOM implementation (not relevant to minitrigger's Node-only test targets, but worth
  knowing if any browser-mode tests get added later).
- Browser context import path changed: `import { page } from 'vitest/browser'`
  (not `@vitest/browser/context` anymore) — again, only relevant if browser mode is ever used.

### 5.2 Config: `pool`, `fileParallelism`, `globalSetup`

```ts
// vitest.config.ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    pool: 'forks',             // default; 'threads' | 'forks' | 'vmThreads' | 'vmForks'
    fileParallelism: true,     // default true; false ⇒ forces maxWorkers: 1
    globalSetup: ['./test/global-setup.ts'],
  },
});
```

`pool` default is `'forks'` (a separate child process per worker — good isolation, heavier
startup than `'threads'`). For minitrigger's Postgres/Redis integration tests, `'forks'` is the
safer default (avoids any accidental shared-module state/connection leakage across test files
that `'threads'` could introduce), but `'threads'` is faster if your integration harness is
careful about not caching connections at module scope.

`fileParallelism: false` is exactly equivalent to `maxWorkers: 1` — use it (or the CLI flag
`--no-file-parallelism`) for a **whole test run** that must serialize across files. For
*selective* serialization (only some integration-test files must run one-at-a-time, e.g. ones
sharing one Postgres schema/Redis keyspace, while pure unit tests run in parallel), use
`test.projects` to split into two configs — one with `fileParallelism: false` matching only the
sequential file glob:

```ts
export default defineConfig({
  test: {
    projects: [
      { test: { name: 'unit', exclude: ['**/*.integration.test.ts'] } },
      { test: { name: 'integration', include: ['**/*.integration.test.ts'], fileParallelism: false } },
    ],
  },
});
```
(`test.workspace` / separate `vitest.workspace.ts` files were the v3-era mechanism; `test.projects`
inline in the root config, or an array of project config paths, is the current — v3-deprecated,
v4/v5-current — mechanism. Either still works in v5, but `projects` is the non-deprecated form
going forward.)

`globalSetup` runs **once** for the whole run (not per file, not per worker) — ideal for
spinning up/tearing down a shared Testcontainers Postgres/Redis instance once. Caveat found in
vitest's own source (`testConfig.ts`): if you use `test.projects` and a sub-project *extends* the
true root config, `globalSetup` is explicitly stripped from non-root project configs so it only
runs once for the whole run rather than once per project — but a project that is NOT extending
root (a genuinely separate/"container" project) does inherit/keep its own `globalSetup` and will
run it. Keep global infra setup (start Postgres/Redis containers) at the true root level, not
duplicated per project, unless you deliberately want isolated infra per project.

### 5.3 Typecheck mode / `expectTypeOf`

```json
{ "scripts": { "test:types": "vitest --typecheck" } }
```
```ts
import { expectTypeOf } from 'vitest';
expectTypeOf(myFn).returns.toEqualTypeOf<Promise<Job>>();
```
`--typecheck` runs `tsc`/type-level assertions (`.test-d.ts` files or `expectTypeOf`/`assertType`
calls inside regular test files) as part of the run; it's a separate concern from runtime
coverage and can be wired as its own CI script rather than bundled into the default `vitest run`.

---

## Summary — critical gotchas (read this if nothing else)

1. **BullMQ 6 didn't just bump a major version — it added a pluggable backend abstraction**
   (Redis via ioredis/node-redis/Bun-native, or a from-scratch Postgres backend). `pg` and
   `redis` in `peerDependencies` are for BullMQ's *own* optional Postgres backend, unrelated to
   minitrigger's separate Postgres durability layer. Default behavior (plain `new Queue(name,
   opts)`) is unchanged — still Redis via ioredis.
2. **`maxRetriesPerRequest: null` is enforced differently depending on what you pass to
   `connection`**: plain options object → BullMQ silently fixes it for blocking use (Worker) with
   a console warning; a pre-built ioredis instance → BullMQ **throws** if you didn't set it
   yourself.
3. **`jobId` restrictions are hard errors, not warnings**: purely-numeric strings throw `Custom Id
   cannot be integers`; any `:` (except a very narrow 3-segment legacy shape) throws `Custom Id
   cannot contain :`. Never use `:` in generated ids.
4. **Existing-jobId "idempotency" only lasts as long as the prior job's Redis hash exists** — once
   it's completed/failed and evicted (via `removeOnComplete`/`removeOnFail` or TTL), the same
   `jobId` can be reused for a brand-new job. Don't treat BullMQ jobId presence as a durable
   idempotency record — track it in Postgres if you need it past job retention.
5. **Job Schedulers don't backfill missed ticks.** Only one pending "next iteration" job exists
   per scheduler at a time; if the process was down across several intervals, only the single
   next occurrence *after* the time it's checked again gets scheduled — everything in between is
   silently skipped. But each tick's `job.id` is deterministic
   (`repeat:<jobSchedulerId>:<nextMillis>`), which doubles as your idempotency key.
6. **Recommended peer range: `"ioredis": "^5.11.1"` (or `>=5.11.1 <6.0.0`), not `>=5.0.0`.**
   BullMQ 6.3.8's own peerDependencies range technically allows ioredis 6, but BullMQ's test
   suite (and its ioredis-specific client adapter code) is pinned to and written against ioredis
   5's RESP2-shaped replies. ioredis 6 defaults to RESP3 and requires Node ≥ 20 — both unverified
   against BullMQ 6.3.8. Revisit once BullMQ officially widens/tests its ioredis-6 support.
7. **pg: `statement_timeout` (server-side, real cancel) and `query_timeout` (client-side only,
   does NOT cancel the running server-side query) are not interchangeable** — use both, with
   `statement_timeout` as the actual enforcement mechanism.
8. **pg has no bundled types** — add `@types/pg` as a devDependency.
9. **tsup's dual-package output extensions depend on your own `package.json`'s `"type"` field** —
   for an ESM-first (`"type": "module"`) package, the `cjs` format output is `.cjs`/`.d.cts`, and
   the `exports` map's `types` condition must be listed before `default` in each conditions
   object.
10. **Vitest 5 requires Node ≥ 22.12.0** — this must already be satisfied by minitrigger's Node
    24 target; no conflict, but it rules out testing on Node 20/22.0–22.11 runners.
