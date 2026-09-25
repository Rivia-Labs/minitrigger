import type { EngineLogger } from "../config.js";
import type { ProcessSystemFn } from "../queue/transport.js";

/** Names of the jobs on the internal `${prefix}-system` queue (decision 8). */
export type SystemJobName = "waitpoint-expire" | "reconcile" | "retention" | "schedule-tick";

/**
 * Dispatcher for the `-system` worker. Handlers are filled in by later phases:
 * `waitpoint-expire` (Phase 5), `reconcile` and `retention` (Phase 6), `schedule-tick` (Phase 8).
 * Until then every job is acknowledged as a no-op. Handlers must never throw for expected
 * conditions (BullMQ jobs use `attempts: 1`).
 */
export function createSystemDispatcher(deps: { logger: EngineLogger }): ProcessSystemFn {
	return async (name, data) => {
		switch (name as SystemJobName) {
			case "waitpoint-expire":
			case "reconcile":
			case "retention":
			case "schedule-tick":
				deps.logger.debug({ job: name, data }, "system job has no handler yet; ignored");
				return;
			default:
				deps.logger.warn({ job: name }, "unknown system job ignored");
		}
	};
}
