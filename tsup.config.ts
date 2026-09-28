import { defineConfig } from "tsup";

export default defineConfig({
	// Two public entries (main + `/testing` subpath, phase 9.2 follow-up): `splitting: true` so
	// the code they share (engine core, error classes, executor) lands in common chunk files that
	// both entries import at runtime, instead of each bundling its own copy — a duplicated copy
	// would break `instanceof NonDeterminismError` (and friends) for a consumer who imports the
	// error classes from the main entry but hits the testing entry's own copy of the throw site.
	// Verified for both ESM and CJS output by `scripts/check-pack.mjs` (tsup's CJS splitting is
	// newer/less exercised than its ESM splitting, so it's checked for real, not assumed).
	// ORM adapter subpaths (decision 26): tiny entries that import only the shared error classes
	// (same chunk rule: `instanceof ConfigError` holds across entries) and nothing from any ORM.
	entry: {
		index: "src/index.ts",
		"testing/index": "src/testing/index.ts",
		"typeorm/index": "src/adapters/typeorm.ts",
		"knex/index": "src/adapters/knex.ts",
		"drizzle/index": "src/adapters/drizzle.ts",
		"prisma/index": "src/adapters/prisma.ts",
	},
	format: ["esm", "cjs"],
	target: "node22",
	platform: "node",
	// tsup already externalizes every package.json `dependencies`/`peerDependencies` entry by
	// default (verified against `dist/`: `pg`/`cron-parser` show up as `require("pg")`/`from "pg"`
	// in the chunks that use them, never bundled inline, and `bullmq`/`ioredis` only inside the
	// dynamically-imported `bullmq-*` chunk). This list makes that explicit and future-proof
	// against a tsup upgrade changing the default, or a `noExternal` added later by mistake:
	// `bullmq`/`ioredis` (optional peers, decision 14 amendment) must never end up bundled into
	// the main entry/`/testing`/adapters, and `pg`/`cron-parser` (peer + direct dependency) must
	// stay a single copy shared with whatever version the consumer's app already has.
	external: ["bullmq", "ioredis", "pg", "cron-parser"],
	// tsup 8.5.1 vendors rollup-plugin-dts, which always sets `baseUrl`; TS 6 turns that
	// deprecation into a hard error, so it is silenced for the d.ts build only.
	dts: { compilerOptions: { ignoreDeprecations: "6.0" } },
	clean: true,
	sourcemap: true,
	splitting: true,
	treeshake: true,
	// Migrations are embedded in the bundle as strings (decision 18).
	loader: { ".sql": "text" },
});
