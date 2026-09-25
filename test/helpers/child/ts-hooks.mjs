/**
 * Node module hooks that let a plain `node` child process run the TypeScript sources the way
 * vitest does: `./x.js` specifiers resolve to the adjacent `./x.ts` (NodeNext convention used by
 * every import in `src/`), and `.sql` files load as a default-exported string (mirrors tsup's
 * `loader: { ".sql": "text" }` and the `sql-text` plugin in `vitest.config.ts`). Types are stripped
 * by Node's built-in type stripping (Node >= 23.6).
 */
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export async function resolve(specifier, context, nextResolve) {
	if (specifier.endsWith(".js") && (specifier.startsWith(".") || specifier.startsWith("/"))) {
		const url = new URL(specifier, context.parentURL);
		if (!existsSync(fileURLToPath(url))) {
			const tsUrl = new URL(url.href.replace(/\.js$/, ".ts"));
			if (existsSync(fileURLToPath(tsUrl))) return nextResolve(tsUrl.href, context);
		}
	}
	return nextResolve(specifier, context);
}

export async function load(url, context, nextLoad) {
	if (url.endsWith(".sql")) {
		const text = readFileSync(fileURLToPath(url), "utf8");
		return {
			format: "module",
			source: `export default ${JSON.stringify(text)};`,
			shortCircuit: true,
		};
	}
	return nextLoad(url, context);
}
