#!/usr/bin/env node
/**
 * `pnpm check:pack` — SPEC.md §10 / CLAUDE.md "Fase 10" packaging check.
 *
 * `pnpm build && pnpm pack`s this repo, installs the resulting `.tgz` plus its peer dependencies
 * (`bullmq`, `ioredis`, `pg`) into a throwaway directory (`mktemp -d`, npm — this simulates what a
 * real consumer's package manager does with a plain tarball dependency, independent of anything
 * pnpm-workspace-specific in this monorepo), and verifies:
 *
 *   1. `import("@rivia-labs/minitrigger")` (ESM) loads and exposes `createEngine`/`defineTask`.
 *   2. `require("@rivia-labs/minitrigger")` (CJS) loads and exposes the same two.
 *   3. `@rivia-labs/minitrigger/testing` (ESM + CJS) loads and exposes `createTestEngine`, and an
 *      error thrown by the testing entry (`createTestEngine`'s own config validation, e.g. a bad
 *      `schema`/`maxJobsPerDrain`) is `instanceof` the error classes (`ConfigError`,
 *      `MinitriggerError`) imported from the **main** entry — proving the two entries share the
 *      same class definitions at runtime (tsup `splitting: true` puts the shared engine
 *      core/error classes in a common chunk both entries import) rather than each bundling its
 *      own copy, in **both** module formats: tsup's CJS chunk splitting is newer/less exercised
 *      than its ESM splitting, so this is checked for real, not assumed.
 *   5. a `tsc --moduleResolution node10` check of the main entry, `/testing` and the ORM adapter
 *      subpaths (resolved through `typesVersions`, since node10 ignores `exports`).
 *   6. the ORM adapter subpaths (`/typeorm`, `/knex`, `/drizzle`, `/prisma`, decision 26) load in
 *      ESM and CJS with no ORM installed, import no module outside the package (only the shared
 *      error chunk), and throw `ConfigError`s that are `instanceof` the main entry's class.
 *   4. (best-effort) a one-file `tsc --strict --module NodeNext --moduleResolution NodeNext`
 *      check against the installed `.d.ts` resolves `createEngine`/`defineTask`'s types — this
 *      step is skipped with a warning (not a failure) if this repo's own `typescript` isn't
 *      reachable from the check dir for some reason, since it's a "plus", not the core contract.
 *
 * Everything runs through `node:child_process` + npm/pnpm CLIs already available in this repo's
 * toolchain — no new dependency.
 */
import { execFileSync, execSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

const PKG_NAME = "@rivia-labs/minitrigger";

/** ORM adapter subpaths and the function each exports (decision 26). */
const ADAPTERS = { typeorm: "typeormTx", knex: "knexTx", drizzle: "drizzleTx", prisma: "prismaTx" };

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
						[PKG_NAME]: tarball,
						...PEER_VERSIONS,
					},
				},
				null,
				2
			)}\n`
		);
		run("npm", ["install", "--no-audit", "--no-fund"], { cwd: installDir });

		console.log(`\n--- ESM: import("${PKG_NAME}") ---`);
		writeFileSync(
			join(installDir, "esm-check.mjs"),
			[
				`import { createEngine, defineTask } from "${PKG_NAME}";`,
				'if (typeof createEngine !== "function") throw new Error("createEngine is not a function (ESM)");',
				'if (typeof defineTask !== "function") throw new Error("defineTask is not a function (ESM)");',
				'console.log("ESM OK: createEngine, defineTask both loaded as functions");',
			].join("\n")
		);
		run("node", ["esm-check.mjs"], { cwd: installDir });

		console.log(`\n--- CJS: require("${PKG_NAME}") ---`);
		writeFileSync(
			join(installDir, "cjs-check.cjs"),
			[
				`const { createEngine, defineTask } = require("${PKG_NAME}");`,
				'if (typeof createEngine !== "function") throw new Error("createEngine is not a function (CJS)");',
				'if (typeof defineTask !== "function") throw new Error("defineTask is not a function (CJS)");',
				'console.log("CJS OK: createEngine, defineTask both loaded as functions");',
			].join("\n")
		);
		run("node", ["cjs-check.cjs"], { cwd: installDir });

		console.log(`\n--- ESM: import("${PKG_NAME}/testing") + cross-entry instanceof ---`);
		writeFileSync(join(installDir, "esm-testing-check.mjs"), esmTestingCheckSource());
		run("node", ["esm-testing-check.mjs"], { cwd: installDir });

		console.log(`\n--- CJS: require("${PKG_NAME}/testing") + cross-entry instanceof ---`);
		writeFileSync(join(installDir, "cjs-testing-check.cjs"), cjsTestingCheckSource());
		run("node", ["cjs-testing-check.cjs"], { cwd: installDir });

		console.log("\n--- ORM adapter subpaths (ESM + CJS, no ORM installed) ---");
		checkAdapters(installDir);

		console.log("\n--- type resolution (plus, best-effort) ---");
		ok = checkTypes(installDir) && ok;

		console.log("\n--- type resolution under moduleResolution node10 (typesVersions) ---");
		ok = checkTypesNode10(installDir) && ok;
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
 * Both `esm-testing-check.mjs` and `cjs-testing-check.cjs` do the same three things, in their
 * own module syntax: (a) confirm `createTestEngine` loaded from the `/testing` subpath is a
 * function; (b) trigger two errors `createTestEngine` throws synchronously from its own config
 * validation (before anything async) — an invalid `maxJobsPerDrain` (`MinitriggerError`) and an
 * invalid `schema` pattern (`ConfigError`); (c) assert each caught error is `instanceof` the
 * class of the same name imported from the **main** entry, not just structurally similar. A
 * mismatch here means the two entries ended up with two separate copies of the error classes
 * (e.g. `splitting` silently stopped producing a shared chunk for one format).
 */
function testingCheckBody({ mainImport, testingImport }) {
	return [
		mainImport,
		testingImport,
		"",
		'if (typeof createTestEngine !== "function") throw new Error("createTestEngine is not a function");',
		"",
		"let sawMaxJobsPerDrainError = false;",
		"try {",
		"\tcreateTestEngine({ tasks: [], maxJobsPerDrain: -1 });",
		'\tthrow new Error("expected createTestEngine to throw for a negative maxJobsPerDrain");',
		"} catch (e) {",
		"\tif (!(e instanceof MinitriggerError)) {",
		'\t\tthrow new Error("maxJobsPerDrain error is not instanceof MinitriggerError from the main entry (got " + e?.constructor?.name + ") -- testing entry does not share the main entry\'s chunk");',
		"\t}",
		"\tsawMaxJobsPerDrainError = true;",
		"}",
		"",
		"let sawSchemaError = false;",
		"try {",
		'\tcreateTestEngine({ tasks: [], schema: "Not Valid!!" });',
		'\tthrow new Error("expected createTestEngine to throw for an invalid schema");',
		"} catch (e) {",
		"\tif (!(e instanceof ConfigError)) {",
		'\t\tthrow new Error("schema error is not instanceof ConfigError from the main entry (got " + e?.constructor?.name + ") -- testing entry does not share the main entry\'s chunk");',
		"\t}",
		"\tsawSchemaError = true;",
		"}",
		"",
		'if (!sawMaxJobsPerDrainError || !sawSchemaError) throw new Error("one of the expected throws did not happen");',
		'console.log("OK: createTestEngine loaded from the /testing subpath, and its thrown errors are instanceof the main entry\'s error classes (shared chunk confirmed)");',
	].join("\n");
}

function esmTestingCheckSource() {
	return testingCheckBody({
		mainImport: `import { ConfigError, MinitriggerError } from "${PKG_NAME}";`,
		testingImport: `import { createTestEngine } from "${PKG_NAME}/testing";`,
	});
}

function cjsTestingCheckSource() {
	return testingCheckBody({
		mainImport: `const { ConfigError, MinitriggerError } = require("${PKG_NAME}");`,
		testingImport: `const { createTestEngine } = require("${PKG_NAME}/testing");`,
	});
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
			`import { createEngine, defineTask, type Engine } from "${PKG_NAME}";`,
			`import { createTestEngine, type TestEngine } from "${PKG_NAME}/testing";`,
			...adapterTypeCheckLines(),
			"const _engine: (config: Parameters<typeof createEngine>[0]) => Engine = createEngine;",
			"const _define: typeof defineTask = defineTask;",
			"const _testEngine: (config: Parameters<typeof createTestEngine>[0]) => TestEngine =",
			"\tcreateTestEngine;",
			"void _engine;",
			"void _define;",
			"void _testEngine;",
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
		console.log(
			"type check OK: createEngine/defineTask/createTestEngine types resolve under NodeNext"
		);
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

/**
 * Consumers still on `moduleResolution: "node10"` (`"node"`) ignore `exports`: the `/testing`
 * subpath's types only resolve through `typesVersions`. Unlike {@link checkTypes}, this one is not
 * best-effort about diagnostics: a resolution error here is a real packaging regression.
 */
function checkTypesNode10(installDir) {
	writeFileSync(
		join(installDir, "type-check-node10.ts"),
		[
			`import { createEngine, type Engine } from "${PKG_NAME}";`,
			`import { createTestEngine, type TestEngine } from "${PKG_NAME}/testing";`,
			...adapterTypeCheckLines(),
			"const _engine: (config: Parameters<typeof createEngine>[0]) => Engine = createEngine;",
			"const _testEngine: (config: Parameters<typeof createTestEngine>[0]) => TestEngine =",
			"\tcreateTestEngine;",
			"void _engine;",
			"void _testEngine;",
		].join("\n")
	);
	const tsconfigFile = join(installDir, "tsconfig.type-check-node10.json");
	writeFileSync(
		tsconfigFile,
		JSON.stringify(
			{
				compilerOptions: {
					target: "ES2024",
					lib: ["ES2024"],
					module: "CommonJS",
					moduleResolution: "node10",
					// TypeScript 6 deprecates node10 resolution; consumers on it set this too.
					ignoreDeprecations: "6.0",
					strict: true,
					esModuleInterop: true,
					skipLibCheck: true,
					noEmit: true,
				},
				include: ["type-check-node10.ts"],
			},
			null,
			2
		)
	);
	try {
		execSync(
			`node "${join(REPO_ROOT, "node_modules/typescript/bin/tsc")}" --noEmit -p "${tsconfigFile}"`,
			{ cwd: installDir, stdio: "pipe", encoding: "utf8" }
		);
		console.log(
			"type check OK: main entry, /testing and the adapter subpaths resolve under moduleResolution node10"
		);
		return true;
	} catch (err) {
		console.error(
			`node10 type check FAILED:\n${err.stdout ?? ""}${err.stderr ?? err.message ?? ""}`
		);
		return false;
	}
}

/**
 * Type-level use of every adapter subpath: each wrapper's return value is accepted as
 * `trigger`'s `options.tx` type (`TransactionClient` from the main entry).
 */
function adapterTypeCheckLines() {
	const lines = [`import type { TransactionClient } from "${PKG_NAME}";`];
	for (const [subpath, fn] of Object.entries(ADAPTERS)) {
		lines.push(`import { ${fn} } from "${PKG_NAME}/${subpath}";`);
		lines.push(`const _${fn}: (...args: never[]) => TransactionClient = ${fn};`);
		lines.push(`void _${fn};`);
	}
	return lines;
}

/**
 * Loads each adapter subpath in ESM and CJS (no ORM is installed in the check dir, proving the
 * adapters need none), checks a bad handle throws a `ConfigError` that is `instanceof` the main
 * entry's class, and scans the adapter's built files for any bare import/require: only relative
 * chunk imports are allowed (an ORM import would make every consumer install that ORM).
 */
function checkAdapters(installDir) {
	const pkgDir = join(installDir, "node_modules", ...PKG_NAME.split("/"));
	for (const [subpath, fn] of Object.entries(ADAPTERS)) {
		for (const entry of [`dist/${subpath}/index.js`, `dist/${subpath}/index.cjs`]) {
			// Walks the entry and every chunk it loads (transitively): all of it is what a consumer
			// of the subpath loads, so all of it must be package-free and small.
			const seen = new Set();
			const pending = [entry];
			let bytes = 0;
			while (pending.length > 0) {
				const file = pending.pop();
				if (seen.has(file)) continue;
				seen.add(file);
				const source = readFileSync(join(pkgDir, file), "utf8");
				bytes += Buffer.byteLength(source);
				for (const [, spec] of source.matchAll(
					/(?:^import[^'"\n]*|require\()\s*['"]([^'"]+)['"]/gm
				)) {
					if (!spec.startsWith(".")) {
						throw new Error(
							`${file} (loaded by ${entry}) imports "${spec}": adapters must import no package`
						);
					}
					pending.push(join(dirname(file), spec));
				}
			}
			if (bytes > 16 * 1024) {
				throw new Error(
					`${entry} loads ${bytes} bytes (${seen.size} files); adapters must stay tiny`
				);
			}
			console.log(`${entry}: ${seen.size} file(s), ${bytes} bytes, no package imports`);
		}
		const body = (mainImport, adapterImport) =>
			[
				mainImport,
				adapterImport,
				`if (typeof ${fn} !== "function") throw new Error("${fn} is not a function");`,
				"let threw = false;",
				`try { ${fn}({}); } catch (e) {`,
				`\tif (!(e instanceof ConfigError)) throw new Error("${fn} error is not instanceof the main entry's ConfigError: " + e);`,
				"\tthrew = true;",
				"}",
				`if (!threw) throw new Error("${fn}({}) did not throw");`,
				`console.log("OK: ${PKG_NAME}/${subpath} (${fn})");`,
			].join("\n");
		writeFileSync(
			join(installDir, `adapter-${subpath}.mjs`),
			body(
				`import { ConfigError } from "${PKG_NAME}";`,
				`import { ${fn} } from "${PKG_NAME}/${subpath}";`
			)
		);
		writeFileSync(
			join(installDir, `adapter-${subpath}.cjs`),
			body(
				`const { ConfigError } = require("${PKG_NAME}");`,
				`const { ${fn} } = require("${PKG_NAME}/${subpath}");`
			)
		);
		run("node", [`adapter-${subpath}.mjs`], { cwd: installDir });
		run("node", [`adapter-${subpath}.cjs`], { cwd: installDir });
	}
}

main();
