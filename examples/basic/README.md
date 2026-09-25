# examples/basic

A real consumer project for `@rivia-labs/minitrigger`, installed the way an end
user would (`"@rivia-labs/minitrigger": "file:../.."`), not a workspace package
of the library. `src/index.ts` only ever imports from `"@rivia-labs/minitrigger"`
(the package's public entry point) — never an internal path.

## What it demonstrates

Running `src/index.ts` against the local Postgres/Redis (`docker-compose.yml`
at the repo root) exercises the full public API end to end:

- **Multi-step task** (`publish-article`): `ctx.step.run` (memoized steps) and
  `ctx.step.sleep("cooldown", "2s")` (durable sleep, doesn't hold a worker slot).
- **Approval token**: `ctx.step.createToken`/`ctx.step.waitForToken` suspend the
  run; the script itself plays the "outside" approver, watching the run with
  `engine.runs.subscribe`, reading the token id off `engine.runs.steps`, and
  calling `engine.tokens.complete(tokenId, true)`.
- **Subtask**: `ctx.step.triggerAndWait("archive", archiveArticle, ...)` runs a
  child task and waits for its result without copying it into `steps.output`.
- **Cron schedules**: a declarative one (`defineTask({ schedule })`, synced by
  `engine.start()`) and an ad-hoc one (`engine.schedules.upsert`, with a
  6-field/seconds cron so the demo can watch it actually fire), both cleaned
  up with `engine.schedules.delete()` before the process exits.
- **A simple AI agent loop with a mocked LLM** (`research-agent`): a
  deterministic fake LLM function decides "tool calls" (`search`, `calculate`)
  or a final answer; every LLM decision and every tool call is its own
  `ctx.step.run`, so a crash mid-loop would replay without re-invoking the
  (mocked) LLM or re-running a tool that already completed.
- **Progress output**: `engine.runs.subscribe` for the two triggered runs and
  `engine.events.on("run:completed"/"run:failed", ...)` for local events.
- **Lifecycle**: `engine.migrate()` → `engine.start()` → ... →
  `engine.stop()` (graceful shutdown), with a plain `parseDuration` demo at
  the top and a CJS `require()` smoke check (`src/cjs-check.cjs`) at the end.

The whole thing finishes and exits with code `0` on its own, typically in a
few seconds (well under the ~30s budget), and can be run repeatedly: it
creates its own runs each time (random ids) and deletes the schedules it
creates, so nothing accumulates between runs.

## Install & run

From the **repo root**, build the library first (the `file:` dependency needs
`dist/` to exist) and make sure the local infra is up:

```bash
pnpm build
pnpm infra:up
```

Then, from this directory:

```bash
cd examples/basic
pnpm install
pnpm typecheck
pnpm start
```

`pnpm start` runs `src/index.ts` with Node's native TypeScript stripping
(`node --experimental-strip-types`) to exercise the ESM build and the full
demo above, then runs `src/cjs-check.cjs` with plain `node` to exercise the
CJS build via `require()`.

By default it connects to the ports from the repo root's `docker-compose.yml`
(Postgres on `localhost:54329`, Redis on `localhost:63799`). Override with
`DATABASE_URL`/`REDIS_URL` env vars to point elsewhere.

## Refreshing after rebuilding the library

pnpm's `file:` protocol copies/hard-links the package into
`node_modules` at install time — it does **not** pick up a rebuilt `dist/`
automatically. After changing the library and running `pnpm build` at the
root again, refresh this example with:

```bash
pnpm install --force
```

## Why this isn't part of the root pnpm workspace

The repo root has a `pnpm-workspace.yaml`, but it only configures build-script
approvals for the library's own tooling — it does not declare `examples/basic`
as a workspace package. This directory has its own `pnpm-workspace.yaml`,
which makes pnpm treat it as an independent project root (pnpm stops walking
up the directory tree once it finds this file), so `pnpm install` here
resolves dependencies exactly as they would in a real, separate consumer
project. That local file also carries the same `allowBuilds` approvals
(`esbuild`, `msgpackr-extract`) needed by transitive dependencies so install
doesn't require an interactive `pnpm approve-builds`.
