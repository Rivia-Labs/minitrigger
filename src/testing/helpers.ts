/**
 * `assertTriggered`/`refuteTriggered`/`allTriggered` (testing subpath only): inspired by
 * `@rivia-labs/oban-ts`'s `assertEnqueued`/`refuteEnqueued`/`allEnqueued`, adapted to minitrigger's
 * `runs` (there is no separate "job" concept -- a triggered run IS the unit of work, and a
 * `ctx.step.trigger*` child is just another run with `parentRunId` set).
 *
 * Built entirely on the public `EngineClient["runs"]` surface (`runs.list`), so these work against
 * a real `Engine` too, not only `TestEngine` -- `TriggeredEngine` only requires `runs`.
 */
import type { EngineClient, ListRunsOptions, Run } from "../engine.js";
import { MinitriggerError } from "../errors.js";
import type { RunCursor, RunStatus } from "../store/types.js";
import type { AnyTask } from "../task.js";
import { truncateUtf8 } from "../utils/error-format.js";
import type { Json } from "../utils/serialization.js";

/** Everything these helpers need from an engine: works for `Engine` and `TestEngine` alike. */
export type TriggeredEngine = Pick<EngineClient, "runs">;

export interface TriggeredMatch {
	/** The task object returned by `defineTask`, or its `id`. */
	task?: AnyTask | string;
	/**
	 * Matches when the run's payload **contains** these keys with equal values (deep, like
	 * `expect(...).toMatchObject`): object keys are checked one by one (extra keys on the actual
	 * payload are ignored), but a nested array must match exactly (same length, same elements in
	 * order) -- there's no meaningful "partial array".
	 */
	payload?: unknown;
	/** One status, or any of several. Omitted: any status. */
	status?: RunStatus | readonly RunStatus[];
	/** `trigger(..., { concurrencyKey })` (decision 25). */
	concurrencyKey?: string;
}

/** One run matched by {@link allTriggered}/{@link assertTriggered}. */
export interface TriggeredRun {
	runId: string;
	taskId: string;
	status: RunStatus;
	payload: Json;
	concurrencyKey: string | null;
	/** Set for a run triggered by another run's `ctx.step.trigger*` (a child). */
	parentRunId: string | null;
	createdAt: Date;
}

/** Thrown by `assertTriggered`/`refuteTriggered` when the expectation doesn't hold. */
export class AssertionError extends MinitriggerError {
	override name = "AssertionError";
}

/** One page of `runs.list` at a time; large enough that most tests never see a second page. */
const LIST_PAGE_SIZE = 200;
/** Bound on how many runs a failure message lists, so a large test suite's output stays readable. */
const MAX_DESCRIBED_RUNS = 20;
/** Bound on a single payload's JSON preview in a failure message. */
const MAX_PAYLOAD_PREVIEW_BYTES = 200;

/**
 * Every triggered run matching `match`, oldest first (`createdAt`, then `runId` as a tie-break --
 * ties within the same millisecond have no other guaranteed order). Paginates through `runs.list`
 * internally; `runs.list` itself is newest-first, so the collected results are sorted before
 * returning rather than relied on to already be in call order.
 */
export async function allTriggered(
	engine: TriggeredEngine,
	match: TriggeredMatch = {}
): Promise<TriggeredRun[]> {
	const taskId = taskIdOf(match.task);
	const status = match.status;
	const listOptions: Pick<ListRunsOptions, "taskId" | "status"> = {
		...(taskId !== undefined ? { taskId } : {}),
		...(status !== undefined ? { status } : {}),
	};

	const results: TriggeredRun[] = [];
	let cursor: RunCursor | undefined;
	for (;;) {
		const page = await engine.runs.list({
			...listOptions,
			limit: LIST_PAGE_SIZE,
			...(cursor !== undefined ? { cursor } : {}),
		});
		for (const run of page.items) {
			if (matches(run, match)) results.push(toTriggeredRun(run));
		}
		if (page.nextCursor === null) break;
		cursor = page.nextCursor;
	}
	results.sort(
		(a, b) => a.createdAt.getTime() - b.createdAt.getTime() || (a.runId < b.runId ? -1 : 1)
	);
	return results;
}

/**
 * Throws {@link AssertionError} (listing what WAS triggered, for a readable diff) unless at least
 * one triggered run matches `match`; otherwise resolves with the first match (oldest first), so a
 * caller can chain further assertions on it (e.g. `(await assertTriggered(engine, ...)).runId`).
 */
export async function assertTriggered(
	engine: TriggeredEngine,
	match: TriggeredMatch = {}
): Promise<TriggeredRun> {
	const found = await allTriggered(engine, match);
	const first = found[0];
	if (first !== undefined) return first;

	// Scoped to the task (if one was given) for a relevant, still-bounded listing; unscoped when
	// the caller didn't narrow by task, since "nothing was triggered at all" is itself useful.
	const scope: TriggeredMatch = match.task !== undefined ? { task: match.task } : {};
	const others = matchHasOnlyTask(match) ? found : await allTriggered(engine, scope);
	const taskId = taskIdOf(match.task);
	const noneLine =
		taskId !== undefined
			? `No runs were triggered for task "${taskId}".`
			: "No runs were triggered.";
	throw new AssertionError(
		`expected a run matching ${describeMatch(match)} to have been triggered.\n` +
			(others.length === 0 ? noneLine : `Triggered runs:\n${describeRuns(others)}`)
	);
}

/** Throws {@link AssertionError} if any triggered run matches `match`. */
export async function refuteTriggered(
	engine: TriggeredEngine,
	match: TriggeredMatch = {}
): Promise<void> {
	const found = await allTriggered(engine, match);
	if (found.length > 0) {
		throw new AssertionError(
			`expected no run matching ${describeMatch(match)} to have been triggered, found ${found.length}:\n` +
				describeRuns(found)
		);
	}
}

function matchHasOnlyTask(match: TriggeredMatch): boolean {
	return (
		match.payload === undefined && match.status === undefined && match.concurrencyKey === undefined
	);
}

function taskIdOf(task: AnyTask | string | undefined): string | undefined {
	if (task === undefined) return undefined;
	return typeof task === "string" ? task : task.id;
}

function matches(run: Run, match: TriggeredMatch): boolean {
	if (match.concurrencyKey !== undefined && run.concurrencyKey !== match.concurrencyKey) {
		return false;
	}
	if (match.payload !== undefined && !containsPartial(run.payload, match.payload as Json)) {
		return false;
	}
	return true;
}

function toTriggeredRun(run: Run): TriggeredRun {
	return {
		runId: run.id,
		taskId: run.taskId,
		status: run.status,
		payload: run.payload,
		concurrencyKey: run.concurrencyKey,
		parentRunId: run.parentRunId,
		createdAt: run.createdAt,
	};
}

/**
 * `actual` **contains** `expected` (like `expect(...).toMatchObject`): every key of an `expected`
 * object must deep-match the same key of `actual` (extra keys on `actual` are ignored, checked
 * recursively), while an `expected` array must match `actual` exactly (same length, every element
 * matching in order) -- a "partial array" isn't a meaningful concept here. Primitives compare with
 * `===` (so `NaN`/`-0` are never produced by valid `Json` anyway).
 */
function containsPartial(actual: Json, expected: Json): boolean {
	if (expected === null || typeof expected !== "object") return actual === expected;
	if (Array.isArray(expected)) {
		return (
			Array.isArray(actual) &&
			actual.length === expected.length &&
			expected.every((e, i) => containsPartial((actual as readonly Json[])[i] as Json, e))
		);
	}
	if (actual === null || typeof actual !== "object" || Array.isArray(actual)) return false;
	const actualObj = actual as { [key: string]: Json | undefined };
	return Object.entries(expected).every(
		([key, value]) => value === undefined || containsPartial(actualObj[key] ?? null, value)
	);
}

function describeMatch(match: TriggeredMatch): string {
	const { task, payload, status, concurrencyKey } = match;
	const parts: string[] = [];
	if (task !== undefined) parts.push(`task: ${JSON.stringify(taskIdOf(task))}`);
	// `payload` is the caller's own (possibly huge) match criterion, not a stored run's payload:
	// reuse the same bounded preview as `describeRuns` so a large match object can't blow up the
	// failure message either.
	if (payload !== undefined) parts.push(`payload: ${previewPayload(payload as Json)}`);
	if (status !== undefined) parts.push(`status: ${JSON.stringify(status)}`);
	if (concurrencyKey !== undefined) parts.push(`concurrencyKey: ${JSON.stringify(concurrencyKey)}`);
	return `{ ${parts.join(", ")} }`;
}

function describeRuns(runs: readonly TriggeredRun[]): string {
	const shown = runs.slice(0, MAX_DESCRIBED_RUNS);
	const lines = shown.map(
		r =>
			`  - ${r.taskId} [${r.status}]${r.concurrencyKey !== null ? ` concurrencyKey=${r.concurrencyKey}` : ""} payload=${previewPayload(r.payload)}`
	);
	if (runs.length > shown.length) lines.push(`  ... and ${runs.length - shown.length} more`);
	return lines.join("\n");
}

/** Bounded JSON preview: never dumps a huge payload into an assertion failure message. */
function previewPayload(payload: Json): string {
	let json: string;
	try {
		json = JSON.stringify(payload) ?? "null";
	} catch {
		json = String(payload);
	}
	if (Buffer.byteLength(json, "utf8") <= MAX_PAYLOAD_PREVIEW_BYTES) return json;
	return `${truncateUtf8(json, MAX_PAYLOAD_PREVIEW_BYTES)}…`;
}
