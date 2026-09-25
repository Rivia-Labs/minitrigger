#!/usr/bin/env node
/**
 * `pnpm check:pack` — SPEC.md §10 / CLAUDE.md "Fase 10" packaging check.
 *
 * `pnpm build && pnpm pack`s this repo, installs the resulting `.tgz` plus its peer dependencies
 * (`bullmq`, `ioredis`, `pg`) into a throwaway directory (`mktemp -d`, npm — this simulates what a
 * real consumer's package manager does with a plain tarball dependency, independent of anything
 * pnpm-workspace-specific in this monorepo), and verifies:
 *
 *   1. `import("minitrigger")` (ESM) loads and exposes `createEngine`/`defineTask`.
 *   2. `require("minitrigger")` (CJS) loads and exposes the same two.
 *   3. (best-effort) a one-file `tsc --strict --module NodeNext --moduleResolution NodeNext`
 *      check against the installed `.d.ts` resolves `createEngine`/`defineTask`'s types — this
 *      step is skipped with a warning (not a failure) if this repo's own `typescript` isn't
 *      reachable from the check dir for some reason, since it's a "plus", not the core contract.
 *
 * Everything runs through `node:child_process` + npm/pnpm CLIs already available in this repo's
 * toolchain — no new dependency.
 */
import { execFileSync, execSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

const PEER_VERSIONS = {
	bullmq: "^6.3.0",
	ioredis: "^5.11.1",
	pg: "^8.13.0",
};

function run(cmd, args, options = {}) {
	console.log(`+ ${cmd} ${args.join(" ")}${options.cwd ? `  (in ${options.cwd})` : ""}`);
	return execFileSync(cmd, args, { stdio: "inherit", ...options });
}

function runCapture(cmd, args, options = {}) {
	return execFileSync(cmd, args, { encoding: "utf8", ...options });
}

async function main() {
	console.log("=== minitrigger check:pack ===\n");

	console.log("--- pnpm build ---");
	run("pnpm", ["run", "build"], { cwd: REPO_ROOT });

	const packDir = mkdtempSync(join(tmpdir(), "minitrigger-pack-"));
	const installDir = mkdtempSync(join(tmpdir(), "minitrigger-install-"));
	console.log(`\npack dir:    ${packDir}`);
	console.log(`install dir: ${installDir}\n`);

	let ok = true;
	try {
		console.log("--- pnpm pack ---");
		// `pnpm pack` re-runs this package's own `prepare` (tsup) lifecycle script before packing
		// (redundant right after the explicit `pnpm run build` above, but that's pnpm's own
		// behavior, not something worth routing around) and interleaves its stdout with the
		// `--json` output; `--reporter silent` is what actually suppresses that lifecycle output
		// on stdout (verified locally: `--json` alone still leaks it), leaving pure JSON to parse.
		const packJson = runCapture(
			"pnpm",
			["pack", "--json", "--reporter", "silent", "--pack-destination", packDir],
			{ cwd: REPO_ROOT }
		);
		const jsonMatch = packJson.match(/^\{[\s\S]*\}$/m);
		if (!jsonMatch) {
			throw new Error(`pnpm pack --json produced no parseable JSON:\n${packJson}`);
		}
		const packed = JSON.parse(jsonMatch[0]);
		const tarball = packed.filename;
		console.log(`packed: ${tarball} (${packed.files.length} files)`);
		for (const f of packed.files) console.log(`  - ${f.path}`);

		console.log("\n--- npm install (tarball + peer deps) in clean temp dir ---");
		writeFileSync(
			join(installDir, "package.json"),
			`${JSON.stringify(
				{
					name: "minitrigger-check-pack",
					version: "0.0.0",
					private: true,
					type: "module",
					dependencies: {
						minitrigger: tarball,
						...PEER_VERSIONS,
					},
				},
				null,
				2
			)}\n`
		);
		run("npm", ["install", "--no-audit", "--no-fund"], { cwd: installDir });

		console.log('\n--- ESM: import("minitrigger") ---');
		writeFileSync(
			join(installDir, "esm-check.mjs"),
			[
				'import { createEngine, defineTask } from "minitrigger";',
				'if (typeof createEngine !== "function") throw new Error("createEngine is not a function (ESM)");',
				'if (typeof defineTask !== "function") throw new Error("defineTask is not a function (ESM)");',
				'console.log("ESM OK: createEngine, defineTask both loaded as functions");',
			].join("\n")
		);
		run("node", ["esm-check.mjs"], { cwd: installDir });

		console.log('\n--- CJS: require("minitrigger") ---');
		writeFileSync(
			join(installDir, "cjs-check.cjs"),
			[
				'const { createEngine, defineTask } = require("minitrigger");',
				'if (typeof createEngine !== "function") throw new Error("createEngine is not a function (CJS)");',
				'if (typeof defineTask !== "function") throw new Error("defineTask is not a function (CJS)");',
				'console.log("CJS OK: createEngine, defineTask both loaded as functions");',
			].join("\n")
		);
		run("node", ["cjs-check.cjs"], { cwd: installDir });

		console.log("\n--- type resolution (plus, best-effort) ---");
		ok = checkTypes(installDir) && ok;
	} catch (err) {
		ok = false;
		console.error("\ncheck:pack FAILED:", err.message ?? err);
	} finally {
		rmSync(packDir, { recursive: true, force: true });
		rmSync(installDir, { recursive: true, force: true });
	}

	console.log(`\n=== check:pack: ${ok ? "PASS" : "FAIL"} ===`);
	process.exit(ok ? 0 : 1);
}

/**
 * A tiny one-file `tsc` check against the installed `.d.ts`, using this repo's own `typescript`
 * (never a new dependency). Best-effort: a problem *running* `tsc` itself (not found, etc.) is a
 * warning, not a failure, since the ESM/CJS runtime checks above are the actual contract this
 * script verifies; a `tsc` that runs and reports a real type error IS a failure.
 */
function checkTypes(installDir) {
	const typeCheckFile = join(installDir, "type-check.ts");
	writeFileSync(
		typeCheckFile,
		[
			'import { createEngine, defineTask, type Engine } from "minitrigger";',
			"const _engine: (config: Parameters<typeof createEngine>[0]) => Engine = createEngine;",
			"const _define: typeof defineTask = defineTask;",
			"void _engine;",
			"void _define;",
		].join("\n")
	);
	const tsconfigFile = join(installDir, "tsconfig.type-check.json");
	writeFileSync(
		tsconfigFile,
		JSON.stringify(
			{
				compilerOptions: {
					target: "ES2024",
					lib: ["ES2024"],
					module: "NodeNext",
					moduleResolution: "NodeNext",
					strict: true,
					esModuleInterop: true,
					skipLibCheck: true,
					noEmit: true,
				},
				include: ["type-check.ts"],
			},
			null,
			2
		)
	);

	try {
		execSync(
			`node "${join(REPO_ROOT, "node_modules/typescript/bin/tsc")}" --noEmit -p "${tsconfigFile}"`,
			{
				cwd: installDir,
				stdio: "pipe",
				encoding: "utf8",
			}
		);
		console.log("type check OK: createEngine/defineTask types resolve under NodeNext");
		return true;
	} catch (err) {
		if (err.stdout || err.stderr) {
			// tsc ran and reported real diagnostics: a genuine failure.
			console.error(`type check FAILED:\n${err.stdout ?? ""}${err.stderr ?? ""}`);
			return false;
		}
		console.warn(`type check skipped (could not run this repo's tsc): ${err.message ?? err}`);
		return true;
	}
}

main();
