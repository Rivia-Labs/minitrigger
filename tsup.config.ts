import { defineConfig } from "tsup";

export default defineConfig({
	// Two public entries (main + `/testing` subpath, phase 9.2 follow-up): `splitting: true` so
	// the code they share (engine core, error classes, executor) lands in common chunk files that
	// both entries import at runtime, instead of each bundling its own copy — a duplicated copy
	// would break `instanceof NonDeterminismError` (and friends) for a consumer who imports the
	// error classes from the main entry but hits the testing entry's own copy of the throw site.
	// Verified for both ESM and CJS output by `scripts/check-pack.mjs` (tsup's CJS splitting is
	// newer/less exercised than its ESM splitting, so it's checked for real, not assumed).
	entry: { index: "src/index.ts", "testing/index": "src/testing/index.ts" },
	format: ["esm", "cjs"],
	target: "node22",
	platform: "node",
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
