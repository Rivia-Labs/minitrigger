/**
 * Review 4 item 6: the exported event-source type is `EngineEventSource`, not `EventSource`
 * (which collides with the Node/DOM global of that name). Checked by `pnpm typecheck`.
 */
import { describe, expectTypeOf, it } from "vitest";
import type { EngineClient } from "../../src/engine.js";
import type * as publicApi from "../../src/index.js";
import type { EngineEventSource } from "../../src/index.js";

describe("review 4 item 6: EngineEventSource", () => {
	it("is exported and is the engine's `events` side", () => {
		expectTypeOf<EngineEventSource>().toEqualTypeOf<Pick<EngineClient, "events">>();
	});

	it("the old name `EventSource` is no longer exported", () => {
		// @ts-expect-error -- renamed to EngineEventSource (collided with the global EventSource).
		type Removed = publicApi.EventSource;
		expectTypeOf<Removed>().toBeAny();
	});
});
