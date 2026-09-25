import { ConfigError } from "../errors.js";

/** The minimal shape `checkRedisEvictionPolicy` needs -- easy to satisfy with a fake in tests. */
export interface RedisConfigClient {
	config(subcommand: "GET", parameter: string): Promise<unknown>;
}

const REQUIRED_POLICY = "noeviction";

/**
 * Decision 22: environment checks are warnings, not fatal failures, when the
 * check itself is unavailable (e.g. `CONFIG` is blocked on managed Redis) --
 * only a check that *answers* and is wrong should throw.
 *
 * - `CONFIG GET maxmemory-policy` rejects (command disabled/unknown) -> returns
 *   a warning string for the caller to log; does not throw.
 * - It answers with a policy other than `"noeviction"` -> throws `ConfigError`
 *   (a policy that evicts keys under memory pressure can silently drop queued
 *   BullMQ job data).
 * - It answers `"noeviction"` -> returns `undefined` (nothing to warn about).
 */
export async function checkRedisEvictionPolicy(
	client: RedisConfigClient
): Promise<string | undefined> {
	let reply: unknown;
	try {
		reply = await client.config("GET", "maxmemory-policy");
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		return (
			`could not read Redis's "maxmemory-policy" (the CONFIG command is unavailable, e.g. on ` +
			`a managed Redis provider) -- skipping this check: ${message}`
		);
	}

	const policy = extractPolicy(reply);
	if (policy === undefined) {
		// Fix 10 (opus review): a reply that doesn't parse as `["maxmemory-policy", value]`/a
		// map-like object -- e.g. empty, null, or some other unexpected shape from a proxy in front
		// of Redis -- is the check being unable to get a definitive answer (decision 22), not the
		// definitive wrong answer that should fail startup.
		return (
			`Redis's "CONFIG GET maxmemory-policy" replied with something unexpected ` +
			`(${JSON.stringify(reply)}) -- skipping this check`
		);
	}
	if (policy !== REQUIRED_POLICY) {
		throw new ConfigError(
			`Redis "maxmemory-policy" is "${policy}", but minitrigger requires ` +
				`"${REQUIRED_POLICY}": any other policy lets Redis evict keys under memory pressure, ` +
				`which can silently drop queued/delayed BullMQ job data. Set ` +
				`"maxmemory-policy ${REQUIRED_POLICY}" in your Redis configuration.`
		);
	}
	return undefined;
}

/** `CONFIG GET` replies as a flat RESP2 array (`["maxmemory-policy", "noeviction"]`) or a RESP3 map-like object. */
function extractPolicy(reply: unknown): string | undefined {
	if (Array.isArray(reply)) {
		const value = reply[1];
		return typeof value === "string" ? value : undefined;
	}
	if (reply !== null && typeof reply === "object") {
		const value = (reply as Record<string, unknown>)["maxmemory-policy"];
		return typeof value === "string" ? value : undefined;
	}
	return undefined;
}
