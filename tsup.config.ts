import { defineConfig } from "tsup";

export default defineConfig({
	entry: ["src/index.ts"],
	format: ["esm", "cjs"],
	target: "node24",
	platform: "node",
	// tsup 8.5.1 vendors rollup-plugin-dts, which always sets `baseUrl`; TS 6 turns that
	// deprecation into a hard error, so it is silenced for the d.ts build only.
	dts: { compilerOptions: { ignoreDeprecations: "6.0" } },
	clean: true,
	sourcemap: true,
	splitting: false,
	treeshake: true,
	// Migrations are embedded in the bundle as strings (decision 18).
	loader: { ".sql": "text" },
});
