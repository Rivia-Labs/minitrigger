import { MinitriggerError } from "../errors.js";
import type { StepApi } from "../task.js";
import type { ExecutionState } from "./execution.js";

/**
 * Builds `ctx.step` for one execution.
 *
 * Phase 4 seam: this is where the real step context goes. It receives the {@link ExecutionState}
 * (ownership, abort signal, `trackInFlight`, `markSuspended`) and will load the step snapshot
 * (`StepStore.loadForExecution`) before the task function runs, assign `seq_index` at call time,
 * memoize via `StepStore.insertCompleted`, and throw the internal `SuspendSignal` (after which the
 * executor awaits `Promise.allSettled` of the in-flight steps and runs `RunStore.suspend`).
 */
export function createStepApi(_state: ExecutionState): StepApi {
	const notYet = (): Promise<never> =>
		Promise.reject(new MinitriggerError("steps are implemented in Phase 4"));
	return {
		run: notYet,
		sleep: notYet,
		createToken: notYet,
		waitForToken: notYet,
		trigger: notYet,
		triggerAndWait: notYet,
		batchTriggerAndWait: notYet,
	};
}
