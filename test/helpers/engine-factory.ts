/**
 * Transport-parameterized integration suites (Task 11 of the Postgres transport plan, decision
 * 28): the shared suites run once per transport listed in `MT_TRANSPORTS` (default
 * `bullmq,postgres`, set by `vitest.config.ts`), so the Postgres mode is held to the same
 * semantics as BullMQ.
 *
 * Each transport needs its own schema (the `schema_meta` marker refuses a second transport on the
 * same schema, decision 15), so call `useTestNamespace()` *inside* the `describeTransports`
 * callback: its `beforeAll`/`afterAll` hooks are then scoped to that transport's `describe`.
 */
import { describe } from "vitest";
import type {
	BullmqTransportConfig,
	CommonConfig,
	EngineConfig,
	PostgresTransportConfig,
	TransportKind,
} from "../../src/config.js";
import type { TestNamespace } from "./namespace.js";

const KNOWN_TRANSPORTS: readonly TransportKind[] = ["bullmq", "postgres"];

function parseTransports(raw: string | undefined): readonly TransportKind[] {
	const value = raw?.trim() ? raw : KNOWN_TRANSPORTS.join(",");
	const parsed = value
		.split(",")
		.map(part => part.trim())
		.filter(part => part.length > 0);
	for (const part of parsed) {
		if (!KNOWN_TRANSPORTS.includes(part as TransportKind)) {
			throw new Error(
				`MT_TRANSPORTS: unknown transport "${part}" (expected a comma-separated subset of ${KNOWN_TRANSPORTS.join(", ")})`
			);
		}
	}
	return [...new Set(parsed as TransportKind[])];
}

/** The transports the parameterized suites run on, from `process.env.MT_TRANSPORTS`. */
export const TRANSPORTS: readonly TransportKind[] = parseTransports(process.env["MT_TRANSPORTS"]);

/**
 * Overrides accepted by {@link engineConfigFor}: every common field, plus the transport-specific
 * ones of either mode. A BullMQ-only field (`redis`, `prefix`, `outbox`) passed with
 * `transport: "postgres"` is forwarded as is, so the engine rejects it (`ConfigError`) instead of
 * the helper silently dropping it.
 */
export type EngineOverrides = Partial<CommonConfig> &
	Partial<Omit<BullmqTransportConfig, "transport" | "polling">> &
	Partial<Omit<PostgresTransportConfig, "transport" | "redis" | "prefix" | "outbox">>;

/**
 * An engine config for `transport` on `ns`: BullMQ gets `{ redis: ns.redisUrl, prefix: ns.prefix }`,
 * Postgres gets `{ transport: "postgres" }` (no redis/prefix). `tasks` defaults to `[]`.
 */
export function engineConfigFor(
	transport: TransportKind,
	ns: TestNamespace,
	overrides: EngineOverrides = {}
): EngineConfig {
	const common = { postgres: ns.databaseUrl, schema: ns.schema, tasks: [] };
	if (transport === "bullmq") {
		return { ...common, redis: ns.redisUrl, prefix: ns.prefix, ...overrides } as EngineConfig;
	}
	return { ...common, transport: "postgres", ...overrides } as EngineConfig;
}

/** `describe.each(TRANSPORTS)`: one `describe` per transport, named `${name} [${transport}]`. */
export function describeTransports(name: string, fn: (transport: TransportKind) => void): void {
	describe.each(TRANSPORTS)(`${name} [%s]`, transport => fn(transport));
}
