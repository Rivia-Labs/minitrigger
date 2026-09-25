# examples/basic

A real consumer project for `minitrigger`, installed the way an end user would
(`"minitrigger": "file:../.."`), not a workspace package of the library.

## Install & run

From the **repo root**, build the library first (the `file:` dependency needs `dist/` to exist):

```bash
pnpm build
```

Then, from this directory:

```bash
cd examples/basic
pnpm install
pnpm typecheck
pnpm start
```

`pnpm start` runs `src/index.ts` with Node's native TypeScript stripping
(`node --experimental-strip-types`) to exercise the ESM build, then runs
`src/cjs-check.cjs` with plain `node` to exercise the CJS build via `require()`.

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
