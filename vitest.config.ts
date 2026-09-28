import { readFileSync } from "node:fs";
import type { Plugin } from "vite";
import { defineConfig } from "vitest/config";

// Mirrors tsup's `loader: { ".sql": "text" }` so tests import migrations the same way.
const sqlText: Plugin = {
	name: "sql-text",
	load(id) {
		if (!id.endsWith(".sql")) return null;
		return `export default ${JSON.stringify(readFileSync(id, "utf8"))};`;
	},
};

export default defineConfig({
	plugins: [sqlText],
	test: {
		projects: [
			{
				extends: true,
				test: { name: "unit", include: ["test/unit/**/*.test.ts"] },
			},
			{
				extends: true,
				test: {
					name: "integration",
					include: ["test/integration/**/*.test.ts"],
					// Transports the parameterized suites run on (`test/helpers/engine-factory.ts`):
					// `MT_TRANSPORTS=postgres pnpm test:integration` narrows them to one.
					env: { MT_TRANSPORTS: process.env["MT_TRANSPORTS"] ?? "bullmq,postgres" },
					globalSetup: ["test/helpers/global-setup.ts"],
					testTimeout: 30_000,
					hookTimeout: 30_000,
				},
			},
			{
				// FLUSHALL wipes the whole Redis: run alone, after the other projects (`pnpm test`).
				extends: true,
				test: {
					name: "flushall",
					include: ["test/integration-flushall/**/*.test.ts"],
					globalSetup: ["test/helpers/global-setup.ts"],
					testTimeout: 30_000,
					hookTimeout: 30_000,
				},
			},
		],
	},
});
