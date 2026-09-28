/**
 * Review 5 item B7: `cron-parser` validates cron patterns in both transports and BullMQ uses it
 * internally to run them. A caret range could resolve to a newer copy than BullMQ's exact pin, and
 * the two copies could disagree on what a pattern means; the dependency is pinned to the same
 * exact version BullMQ pins.
 */
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);

function readJson(path: string): Record<string, Record<string, string> | undefined> {
	return JSON.parse(readFileSync(path, "utf8")) as Record<
		string,
		Record<string, string> | undefined
	>;
}

describe("review 5 B7: cron-parser pin", () => {
	it("package.json pins cron-parser to exactly BullMQ's version", () => {
		const own = readJson(new URL("../../package.json", import.meta.url).pathname);
		const bullmq = readJson(require.resolve("bullmq/package.json"));
		const bullmqPin = bullmq["dependencies"]?.["cron-parser"];
		expect(bullmqPin).toMatch(/^\d+\.\d+\.\d+$/);
		expect(own["dependencies"]?.["cron-parser"]).toBe(bullmqPin);
	});
});
