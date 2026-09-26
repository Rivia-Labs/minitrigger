---
name: minitrigger-testing
description: Write tests for minitrigger tasks and for code that calls engine.trigger() — using runTask for fast pure-logic unit tests of a single task, createTestEngine for anything touching replay/suspension/retries/non-determinism/multi-task flows with no real Postgres/Redis, and assertTriggered/refuteTriggered for asserting what got triggered. Use when the user wants to test a task's logic, test that triggering code enqueues the right run, or verify replay/suspension/token/schedule behavior.
when_to_use: "test this minitrigger task", "unit test this task's logic", "test that this triggers the receipt task", "how do I test suspension/replay", "test a scheduled task", "assert engine.trigger was called"
---

# Test minitrigger tasks and triggering code

Three tools, in increasing order of what they exercise. Pick the cheapest one that actually tests
the thing in question.

| Tool | Exercises | Use for |
|---|---|---|
| `runTask` | just `task.run`'s logic | fast, pure-logic unit tests of one task |
| `createTestEngine` | replay, memoization, suspension, retries, non-determinism, multi-task flows | anything durability-shaped, with no real Postgres/Redis |
| a real `Engine` against disposable Postgres+Redis | leader election, reconciler, cross-process cancellation, PgBouncer/NOTIFY behavior | true integration tests `createTestEngine` explicitly doesn't model |

Both `createTestEngine` and `runTask`, plus `assertTriggered`/`refuteTriggered`/`allTriggered`,
come from the **`@rivia-labs/minitrigger/testing`** subpath — not the main entry.

## `createTestEngine({ tasks })`

In-memory store and queue, driven through the **same executor as production** (replay,
memoization, suspension, retries, non-determinism checks). Nothing runs in the background — the
test drives it.

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

- `runInline(task, payload, options?)` triggers and runs until finished/blocked; returns
  `{ runId, status, output, error, attempt }`. A **failed run is returned, not thrown** — `error`
  is the stored error (e.g. `{ name: "NonDeterminismError", ... }`).
- `drain()` runs everything currently due. `drive(run)` = `drain()` + that run's result.
- A child triggered with `step.trigger*` whose task isn't in `tasks` stays unprocessed: it fails
  with a retryable `TaskNotRegisteredError`. Register every task the test exercises.

### Clock

```ts
const engine = createTestEngine({ tasks: [...], clock: { start: new Date("2026-01-01"), autoAdvance: true } });
```

Default `autoAdvance: true`: when nothing is due but something is delayed (a sleep, a retry
backoff, `trigger({ delay })`, or a token/`triggerAndWait` timeout — even one no execution has
started waiting on yet), the clock jumps to the **earliest** delayed job of any kind. This means a
run that sleeps past a shorter timeout it already created will see that timeout fire first, exactly
as a real clock would.

Use `autoAdvance: false` + `await engine.clock.advance(duration)` to interleave manually — e.g.
completing a token before its timeout even though a later step would have slept past it:

```ts
const engine = createTestEngine({ tasks: [...], clock: { autoAdvance: false } });
const run = await engine.trigger(myTask, payload);
await engine.drain();
await engine.tokens.complete(tokenId, output); // complete before advancing the clock past the timeout
await engine.clock.advance("2h");
await engine.drain();
```

### Tokens

`engine.tokens.pending(runId)` finds a waiting token's id (from the run's `token`/`wait` steps);
`engine.tokens.complete(id, output)` completes it — same signature and semantics as production.

### Schedules

Cron schedules don't fire on their own in test mode: `await engine.tickSchedule(id)` fires one
occurrence (`id` is the task's own id for a `defineTask({ schedule })`). It syncs every declarative
schedule into the store first (lazily, once), rejects with `ConfigError` for an unknown id, and
rejects with whatever error the tick job itself failed with.

### Queues

`queues.pause`/`resume`/`list` work in memory (a paused queue's jobs are held until `resume`, which
doesn't drain by itself). `queues.scale` **throws** — the test engine is a single-job-at-a-time
model, there's nothing to scale.

## `assertTriggered` / `refuteTriggered` / `allTriggered`

Also from the testing subpath. Built entirely on `engine.runs.list()`, so they work for a child
triggered by `ctx.step.trigger*` exactly like a top-level `engine.trigger()` — and against a real
`Engine` too, via the shared `EngineClient` surface.

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

Match criteria (all optional, combined with AND): `task` (object or id), `payload` (deep
"contains" match — a nested array must match exactly, not as a subset), `status` (one or several),
`concurrencyKey`. `assertTriggered` resolves with the first match (oldest first) — chain further
assertions off it (`(await assertTriggered(...)).runId`); with no match it throws an
`AssertionError` listing what WAS triggered. `allTriggered(engine, match?)` returns every match.

## `runTask`: unit-testing one task's logic

No engine, no store, no queue — runs `task.run(payload, ctx)` directly, the way you'd unit test any
other function.

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

`RunTaskResult<O>`:

```ts
interface RunTaskResult<O = unknown> {
	output: O | null;              // set only when run resolved and its output validated
	error: SerializedError | null; // set when run rejected or its output failed validation
	steps: RunTaskStep[];          // { name, kind, output } in call order
	logs: RunTaskLogEntry[];       // { level, message, data }
	progress: Json[];              // every ctx.progress.set value, in call order
	triggered: RunTaskTriggered[]; // { name, taskId, mode, payload, output?, error? }
	streams: Record<string, (Json | string)[]>; // items per ctx.stream(name, ...) call
}
```

- `ctx.step.run(name, fn)` executes `fn` **immediately** — there is no replay to check against.
  `ctx.step.sleep` resolves instantly.
- `ctx.step.createToken`/`waitForToken` and `ctx.step.trigger`/`triggerAndWait`/
  `batchTriggerAndWait` need a test double or they **throw** (a missing stub is a test-authoring
  mistake, not a task bug, so it's never returned as `result.error`):
  - `options.tokens: { [tokenName]: value | (() => value) }` — resolves `waitForToken`. A
    `createToken` that's never awaited needs no entry.
  - `options.children: { [taskId]: (payload) => output }` — resolves `trigger*`. A stub that
    throws only fails its own `triggerAndWait`/`batchTriggerAndWait` result (`{ ok: false, error
    }`); `trigger` (fire-and-forget) propagates the throw like any step.
- `options.signal`, `options.attempt` (default 1), `options.limits` (override the size limits
  normally read from engine config) are also available.
- Payload/output validation and step-name validation reuse the real checks, so a task that passes
  here behaves the same in production. An invalid `payload` **argument** throws (like `trigger()`
  would); an invalid **output** (task's own or a step's) comes back as `result.error`.
- `runTask` never rejects for an error the task's own code threw (or an invalid output) — that's
  `result.error`, with `retryable: false` for the same deterministic bugs production treats as
  fatal (bad step name, duplicate step, non-serializable output, ...), and `true` otherwise unless
  the task threw `AbortTaskRunError`.
- **Does not test**: replay, memoization across attempts, suspension, non-determinism detection,
  real waitpoint timeouts, or anything else durability-shaped. Use `createTestEngine` for that.

## When to use which

- `runTask` — fast, pure-logic unit tests of one task's `run` function: does it produce the right
  output given inputs, does it call the right steps/children/tokens.
- `createTestEngine` — anything touching replay, suspension, retries, non-determinism, or flows
  spanning multiple tasks (parent/child), without needing real infra.
- a real `Engine` against a disposable Postgres+Redis — only for what `createTestEngine`
  explicitly doesn't model: leader election, the reconciler, cross-process cancellation,
  PgBouncer/`LISTEN` behavior for the outbox pattern (see `minitrigger-outbox`).

## What to tell the user

Summarize: which test file(s) were added/changed, which of the three approaches was used and why,
and any follow-ups — e.g. "this task also has an outbox-triggered call site; that path needs a
real `Engine`/test Postgres per `minitrigger-outbox`, not `createTestEngine`."
