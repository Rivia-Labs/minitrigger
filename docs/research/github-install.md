# Installing minitrigger via GitHub (`pnpm add github:user/minitrigger`)

Phase 10 (`docs/SPEC.md` §10, `docs/HANDOFF.md` "4. Fase 10") asks whether installing straight
from GitHub works, or needs to be documented as a known limitation with a `.tgz`-on-Releases
fallback. **Verdict: it works, but only after the consumer opts in to two `allowBuilds` entries in
their own `pnpm-workspace.yaml`; without that opt-in, `pnpm add`/`pnpm install` fails outright.**
This is inherent to pnpm's supply-chain hardening (not a minitrigger bug), so it's a documentation
problem, not a code fix. Both behaviors below were reproduced locally (not inferred from docs
alone), pinned to `pnpm@12.3.4` (the version this repo's `packageManager` field pins and the one
installed locally: `pnpm --version` → `12.3.4`).

## 1. Why `prepare` matters here

`dist/` is `.gitignore`d (see `.gitignore`) and is only ever produced by the `prepare` script
(`"prepare": "tsup"`, `package.json`). npm/pnpm run `prepare` automatically for a **git-hosted**
dependency (there is no pre-built tarball to install otherwise — unlike a registry install, which
gets whatever `npm publish` uploaded, or a local `.tgz`, which already has `dist/` inside it per
`"files": ["dist"]`). So a GitHub install only produces a working package if `prepare` actually
runs.

## 2. pnpm 10+ blocks dependency lifecycle scripts by default — git deps especially

Since pnpm v10, **no dependency's lifecycle scripts run at all** unless the package is explicitly
allowed (`onlyBuiltDependencies` / the newer `allowBuilds`, pnpm 10.26+, which replaces both
`onlyBuiltDependencies` and `ignoredBuiltDependencies` as the preferred single setting). pnpm
10.26 additionally called out **git-hosted dependencies' `prepare` scripts** by name as blocked by
default ("Stricter Git Dependency Security"), which is exactly minitrigger's situation as a
`github:`/`git+https:` install.

### Reproduced locally

Verified against a local bare clone of this repo at `df73ef6` (mirrors exactly what pnpm fetches
for a real `github:owner/minitrigger` specifier — a git clone of the pinned committed history, not
the working tree), added as a git dependency (`git+file://...`, equivalent resolution path to
`github:owner/repo` per pnpm's own docs — both normalize to the same git-URL resolution) to a
throwaway consumer project:

```
$ pnpm add "git+file:///.../bare.git"
Error: ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED

  × adding a new package
  ╰─▶ The git-hosted package "minitrigger@0.1.0" needs to execute build
      scripts but is not in the "allowBuilds" allowlist.
  help: Add the package to "allowBuilds" in your project's pnpm-workspace.yaml
        to allow it to run scripts. For example:
        allowBuilds:
          minitrigger@git+file:///.../bare.git#df73ef64cbb2138d30e9084e5fa2681bd669c66e: true
```

Install aborts (non-zero exit); `node_modules/minitrigger` has no `dist/` at all. This is not a
warning — it is a hard failure of `pnpm add`/`pnpm install`.

### The fix: an `allowBuilds` entry, keyed by the exact resolved git spec

Adding pnpm's own suggested entry to the **consumer's** `pnpm-workspace.yaml` (not minitrigger's —
this is entirely the installer's opt-in, minitrigger's own `package.json` cannot pre-authorize it)
makes `prepare` run and produces a working `dist/`:

```yaml
# consumer's pnpm-workspace.yaml
allowBuilds:
  minitrigger@github.com/<owner>/minitrigger#<commit-or-tag>: true
```

Reproduced: with this entry present, `pnpm add git+file://...` ran `prepare` (full `tsup` build:
ESM + CJS + d.ts, same output as `pnpm build` in this repo) inside an isolated temp dir (pnpm
installs the git package's own `devDependencies` — `tsup`, `typescript`, `bullmq`, `ioredis`,
`pg`, `vite`, `vitest`, `biome`, i.e. this whole repo's dev toolchain — just to run that one
script, then discards that temp dir), and the installed `node_modules/minitrigger/dist/` had all
six expected files (`index.js`, `index.cjs`, `index.d.ts`, `index.d.cts`, plus source maps). Both
entry points load and expose the public API:

```
$ node -e "import('minitrigger').then(m => console.log(Object.keys(m).slice(0,3)))"
[ 'AbortTaskRunError', 'ConfigError', 'DuplicateStepError' ]
$ node -e "console.log(Object.keys(require('minitrigger')).slice(0,3))"
[ 'AbortTaskRunError', 'ConfigError', 'DuplicateStepError' ]
```

**Important — the allowlist key must match the exact resolved spec** pnpm prints in its own error
(package name + `@` + the git URL pnpm normalized to + `#` + the commit it resolved to), not just
the bare package name (`minitrigger: true` alone was tried and does **not** work for a git
dependency — pnpm only matches git deps by their full resolved spec). This means the entry changes
every time the consumer bumps to a newer commit/tag of minitrigger — a real ergonomic cost pointed
at below.

## 3. A second, unrelated blocker: transitive native builds (`ioredis` → `msgpackr-extract`)

Independently of the git-`prepare` issue, plain pnpm 10+ dependency-script blocking (which applies
to *all* dependencies, not just git ones) also caught `msgpackr-extract` (ioredis's optional native
accelerator) needing to run its own build:

```
Error: ERR_PNPM_IGNORED_BUILDS
  × adding a new package
  ╰─▶ Ignored build scripts: msgpackr-extract@3.0.4
  help: Run "pnpm approve-builds" to pick which dependencies should be allowed to run scripts.
```

This also makes `pnpm add`/`pnpm install` exit non-zero (even though, unlike the `prepare` case,
`minitrigger` itself *had* installed correctly by that point — ioredis just falls back to its pure
JS msgpack implementation without it). A consumer needs this too:

```yaml
allowBuilds:
  msgpackr-extract: true
```

With both entries present, `pnpm add "git+..."` / `github:...` exits `0` cleanly and installs a
fully working package. This isn't minitrigger-specific — any project depending on `ioredis` hits
it — but it will surprise a first-time consumer following "just `pnpm add github:...`" instructions
without both entries, so it's worth calling out together.

## 4. Recommendation

Document, don't route around. `pnpm add github:<owner>/minitrigger` is a legitimate install path
once a consumer adds **two** entries to *their own* `pnpm-workspace.yaml`:

```yaml
allowBuilds:
  minitrigger@github.com/<owner>/minitrigger#<commit-or-tag>: true
  msgpackr-extract: true
```

(the exact `minitrigger@...` key must be copied from the error pnpm itself prints on a first,
failing `pnpm add` — that's the simplest way to get it exactly right, and is what the README
should tell people to do, rather than trying to predict pnpm's normalized spec string in advance).

**But prefer a prebuilt `.tgz` on GitHub Releases as the primary documented path**, and keep the
git install as a documented fallback for people already comfortable with `allowBuilds`:

- No `allowBuilds` opt-in needed at all: a `.tgz` produced by `pnpm pack` already contains `dist/`
  (per `"files": ["dist"]`), so there is no `prepare` step to run on install — `pnpm add
  https://github.com/<owner>/minitrigger/releases/download/vX.Y.Z/minitrigger-X.Y.Z.tgz` (or a
  local path to a downloaded file) just unpacks it. This sidesteps *both* issues above in one move
  (no build to permit, and no fresh install of minitrigger's own `devDependencies` pulls in a
  fresh, potentially-differently-resolved `ioredis`/`msgpackr-extract` transitively — the consumer's
  own already-declared `ioredis` peer dependency resolution is what's used).
- It's pinned and reproducible by construction (a fixed, published artifact per version) instead of
  "whatever `HEAD`/a branch currently contains."
- It doesn't force every consumer's toolchain to also have working native-module build tooling
  (`node-gyp` etc., for `msgpackr-extract` and tsup's own transitive deps) just to install a task
  library.
- `scripts/check-pack.sh` (added this phase) already exercises the exact `pnpm pack` → install →
  ESM+CJS load path a Releases `.tgz` would go through, so this path has ongoing local CI coverage
  without needing network access to GitHub.

Net: ship `.tgz` attachments on GitHub Releases as the documented default, mention `pnpm add
github:...` as a working-but-opt-in alternative with the exact `allowBuilds` recipe above (copied
from this file), so nobody has to rediscover `ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED` /
`ERR_PNPM_IGNORED_BUILDS` from scratch.

## 5. Reproduction notes (for re-running this check)

```bash
# from the repo root, at the commit you want to test:
git clone --bare --branch main . /tmp/mt-bare.git

mkdir /tmp/mt-consumer && cd /tmp/mt-consumer
cat > package.json <<'EOF'
{ "name": "probe", "version": "0.0.0", "private": true, "type": "module" }
EOF
: > pnpm-workspace.yaml   # empty first, to see the failure
pnpm add "git+file:///tmp/mt-bare.git"   # reproduces ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED

# then add the two allowBuilds entries pnpm's own error suggests and retry:
pnpm add "git+file:///tmp/mt-bare.git"   # exits 0, dist/ present, ESM+CJS load
```

No real network access to GitHub was needed or used for this check — `git+file://` against a local
bare clone resolves through the identical pnpm git-dependency code path (`github:owner/repo` and
`git+https://github.com/owner/repo.git` are documented by pnpm to normalize to the same
resolution), so the failure/success behavior above is exactly what a real
`pnpm add github:<owner>/minitrigger` hits.
