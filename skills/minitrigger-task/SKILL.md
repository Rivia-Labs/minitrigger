---
name: minitrigger-task
description: Write or review minitrigger tasks (defineTask, ctx.step.*, tokens, subtasks, debounce, concurrency keys, retries) for correctness — determinism under replay, idempotency, step naming, and the golden rules that keep a task safe to resume after a crash or suspension. Use when the user wants to create a new durable task, add steps/sleep/waitForToken/subtasks to one, or review an existing task for bugs before it ships.
when_to_use: "write a minitrigger task", "add a step to this task", "review this task for replay bugs", "why did NonDeterminismError happen", "use waitForToken/createToken", "debounce this trigger", "add a concurrency key"
---

# Write or review a minitrigger task

## `defineTask`

```ts
defineTask({
	id: string,                        // persisted; stable, namespaced (e.g. "billing.send-invoice")
	run: (payload, ctx) => Promise<Output> | Output,
	queue?: { name?: string; concurrency?: number; globalConcurrency?: number;
	          rateLimit?: { max: number; durationMs: number };
	          concurrencyKey?: { limit: number } },
	retry?: { maxAttempts: number; backoff: { type: "exponential" | "fixed"; baseMs: number; maxMs: number; jitter?: boolean } },
	maxDurationMs?: number,
	schedule?: { cron: string; timezone?: string; payload?: Payload }, // declarative cron, see below
});
```

A task whose `run` takes no payload argument makes `payload` **optional** in `trigger()` (and the
task is treated as having no payload if it's typed `unknown` too — `trigger(task)` then needs no
second argument). A genuinely generic task (`<T extends Json>`) can't have `T` inferred from
`trigger()` alone and needs a cast at the call site.

## Golden rules (from the README, follow all of them)

1. **Nothing non-deterministic outside a step.** Random values, `Date.now()`, network calls,
   env reads that can change — all inside `ctx.step.run`, never in the bare task function body
   (which re-executes on every replay).
2. **Step names must be unique and stable within a run.** Don't derive a name from something that
   can change between replays; in a loop, include the index (`` `send-${i}` ``).
3. **Steps should be idempotent where possible.** A step's side effect might be attempted more
   than once before its completion is durably recorded, in rare recovery paths.
4. **Keep step outputs small.** Capped at `maxStepOutputBytes` (256 KB default) and re-read on
   every replay — store a reference (an id, a URL), not a large blob.
5. **Don't reorder or remove steps while runs are waiting.** A deployed change that shifts step
   order or names breaks replay for any run suspended mid-flight (`NonDeterminismError`).
6. **Don't swallow exceptions around `step.*` calls.** Suspension is signaled internally by
   throwing, so a broad `try/catch` around `step.sleep`/`step.waitForToken` won't break
   suspension (it's authoritative) — but catching and discarding a **real** step failure defeats
   retries. Only catch what you intend to handle.
7. **No nested steps.** Calling `ctx.step.*` from inside a `step.run` callback throws
   `NestedStepError`.
8. **Move CPU-heavy work off the main thread.** Long synchronous work blocks the heartbeat and
   can look like a stall to the reconciler; use `worker_threads` for CPU-bound work.
9. **Don't call steps after an `await` inside concurrent branches.** Steps in a branch that keeps
   awaiting and calling more steps (e.g. `Promise.all(items.map(async i => { await
   ctx.step.run(a); await ctx.step.run(b); }))`) are recorded in **completion** order, not call
   order — that can differ between the original run and a replay and risks `NonDeterminismError`.
   Call steps before any `await` in a branch, or run them sequentially.
10. **A `void`/`undefined` step or task output is stored as `null`.** Don't rely on getting back
    exactly `undefined` from a memoized `step.run` or `runs.get(...).output`.
11. **A genuinely generic task needs a cast at the call site** (see above).
12. **`stop()` is at-least-once, not exactly-once.** A run still active past
    `shutdownTimeoutMs` is handed back and retried without an attempt increment — a side effect
    outside a memoized step may run again unless it's idempotent.
13. **Missed cron ticks are not backfilled.** If every process is down when a scheduled tick
    would have fired, that occurrence is skipped for good.
14. **A failed attempt waits (bounded) for its in-flight steps before recording the failure.**
    When the task function throws while other steps are still running, the engine waits for them
    (without aborting `ctx.signal`) so a step that completes is recorded and the retry reuses its
    output. Bounded by `stallTimeoutMs` (default 60 s) or `maxDurationMs` if sooner; past that,
    `ctx.signal` aborts and the attempt fails/retries with the **original** error, and unfinished
    steps re-run on retry. Honor `ctx.signal` in long steps.

## Reserved characters in step names

`#` and `:` are **reserved** in step names (internal names like `${name}#wait`; internal
`__child:*` keys). Using either in a step name fails the run **without retry**.

## `StepApi`

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

type WaitResult<T> =
	| { ok: true; output: T }
	| { ok: false; error: SerializedError }; // error.name is "Timeout" or "Canceled" on those paths
```

`waitForToken`, `triggerAndWait`, and each item of `batchTriggerAndWait` **never throw** on a
child's failure/timeout/cancellation — always check `.ok`.

### Sleep

`ctx.step.sleep(name, duration)` suspends without holding a worker slot. `duration` is a
`Duration`: a number of milliseconds, or a string like `"5m"`, `"2d"`, `"1.5h"`
(units: `ms`, `s`, `m`, `h`, `d`, `w`).

### Tokens

Create a token, hand its `id` to whatever external system will complete it (a webhook handler, a
human clicking a link), then wait for it:

```ts
const token = await ctx.step.createToken("approval", { timeout: "24h" });
await notifyApprover(token.id);
const result = await ctx.step.waitForToken(token);
if (!result.ok) { /* timed out or canceled */ }
```

Completed from outside with `engine.tokens.complete(tokenId, output)`.

### Subtasks and batches

- `ctx.step.trigger(name, task, payload, opts?)` — fire-and-forget, doesn't wait.
- `ctx.step.triggerAndWait(name, task, payload, { timeout? })` — waits for one child.
- `ctx.step.batchTriggerAndWait(name, task, payloads[], { timeout? })` — waits for many; the
  parent resumes **exactly once** for all of them.

### Debounce

```ts
await engine.trigger(recalcClassification, { courseId }, {
	debounce: { key: `course:${courseId}`, delay: "2s", payload: "last", maxDelay: "30s" },
});
```

Triggers sharing `(task, debounce.key)` collapse into **one pending run** while it's still
`queued` and hasn't started. Always **trailing** — there is no `mode` option; each trigger pushes
execution to `delay` after itself. `payload: "last"` (default) replaces the pending payload on
each collapse; `"first"` keeps the original. `maxDelay` (>= `delay`) caps how far a run can be
postponed from its first trigger. Not combinable with `idempotencyKey` or `delay`.

### Concurrency keys

```ts
defineTask({
	id: "recalc-classification",
	queue: { name: "classification", concurrency: 4, concurrencyKey: { limit: 1 } },
	run: async (payload: { courseId: string }) => { /* ... */ },
});
await engine.trigger(recalcClassification, { courseId }, { concurrencyKey: courseId });
```

At most `limit` runs sharing the same `trigger(..., { concurrencyKey })` execute at once, across
every process; a run over the limit goes back to the queue without spending an attempt. Only
`running` runs count — a run waiting on a sleep/token/child doesn't hold a slot. Only
`engine.trigger` accepts `concurrencyKey` (not `step.trigger*`). Pairing `debounce` +
`concurrencyKey` on the same key gives "one pending + one running".

### Progress

`ctx.progress.set(value)` (`value: T & Serializable<T>`) — coalesced writes (at most one every
250 ms), read back via `engine.runs.get(...).progress`. **Not a step**: it isn't memoized and
doesn't participate in non-determinism detection, so calling it a different number of times
across replays is fine.

### Streams

`ctx.stream(name, source: AsyncIterable<Json | string>)` appends items to a Redis Stream, read
live with `engine.runs.readStream(runId, name)`. **Not memoized** — a replay streams again.
Readers stop at the first end marker, so a retried attempt streaming under the **same** name is
invisible to a reader that started earlier; use a per-attempt name (e.g. `` `answer-${ctx.run.attempt}` ``)
if a stream must survive retries. **Not available with `transport: "postgres"`**: `ctx.stream`
throws (fatal, no retry) and `runs.readStream` throws when called — there's no Redis in that mode.

### Retries

Engine-controlled, not BullMQ's:

```ts
retry: { maxAttempts: 3, backoff: { type: "exponential", baseMs: 1_000, maxMs: 60_000, jitter: true } }
```

Default when `retry` is omitted: `{ maxAttempts: 3, backoff: { type: "exponential", baseMs:
1_000, maxMs: 60_000 } }`. Throwing `AbortTaskRunError` fails the run **permanently, no retry**,
regardless of `maxAttempts`.

### `maxDurationMs`

A per-**attempt** timeout, enforced via `ctx.signal`. Does not run while the run is suspended
(sleep/token/subtask) — only while an attempt actively executes.

### `ctx.signal`

Fires on: cancellation, `maxDurationMs` timeout, lost ownership (stolen by the reconciler, stall
recovery), shutdown hand-back, or the normal end of an attempt (reason `"finished"`). Honor it,
especially in long-running steps and un-awaited background work — code that ignores it keeps
running alongside the next attempt.

## Review checklist

When reviewing an existing task, walk through:

- Anything non-deterministic (random, `Date.now()`, network, mutable outside state) outside a
  `ctx.step.*` call?
- Step names derived from something that can change between replays, or missing an index inside a
  loop?
- `ctx.step.*` called from inside a `step.run` callback (nested steps)?
- A `step.trigger`/`triggerAndWait`/`waitForToken`/`sleep` call left un-awaited without a clear
  reason (a `void ctx.step.trigger(...)` fire-and-forget is fine; an accidentally dropped `await`
  on `triggerAndWait` isn't)?
- Step outputs that could grow past `maxStepOutputBytes` (256 KB) — should they store a reference
  instead?
- A broad `try/catch` around step calls that could swallow a real failure, not just let
  suspension propagate?
- Side effects inside a step that aren't safely repeatable if the step is attempted twice?
- Steps called after an `await` inside a `Promise.all`/concurrent branch (completion-order risk)?
- A step name using `#` or `:`?
- `debounce`/`concurrencyKey` options that don't match the task's actual idempotency/ordering
  needs (e.g. relying on FIFO ordering from `concurrencyKey`, which is polling, not FIFO)?

## What to tell the user

Summarize: files changed, which golden rules or reserved-name/nesting issues were fixed (if any),
and any follow-ups — e.g. "add a test for this task with `minitrigger-testing`" or "this task's
trigger site should move into a transaction — see `minitrigger-outbox`".
