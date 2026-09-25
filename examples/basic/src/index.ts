// Consumer example for the `minitrigger` package.
// Only ever import from the package's public entry point — never internal paths.
import { randomUUID } from "node:crypto";
import {
	createEngine,
	type Duration,
	defineTask,
	type Engine,
	type EngineEvents,
	parseDuration,
	type Run,
	type RunHandle,
} from "minitrigger";

// ---------------------------------------------------------------------------
// Connection settings — override via env vars, default to `docker-compose.yml`.
// ---------------------------------------------------------------------------

const DATABASE_URL =
	process.env["DATABASE_URL"] ?? "postgres://minitrigger:minitrigger@localhost:54329/minitrigger";
const REDIS_URL = process.env["REDIS_URL"] ?? "redis://localhost:63799";

// ---------------------------------------------------------------------------
// Small local helpers (not part of the library's API).
// ---------------------------------------------------------------------------

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitFor(
	check: () => Promise<boolean>,
	opts: { timeoutMs: number; intervalMs: number; label: string }
): Promise<void> {
	const deadline = Date.now() + opts.timeoutMs;
	for (;;) {
		if (await check()) return;
		if (Date.now() >= deadline) throw new Error(`timed out waiting for: ${opts.label}`);
		await delay(opts.intervalMs);
	}
}

// A quick demo of `parseDuration`, used throughout this file for `step.sleep`/timeouts/cron waits.
const durations: Duration[] = ["5m", "2d", "500ms", 250];
console.log("parseDuration examples:");
for (const example of durations) {
	console.log(`  ${JSON.stringify(example)} -> ${parseDuration(example)}ms`);
}

// ---------------------------------------------------------------------------
// Task: archive-article — a subtask triggered with `step.triggerAndWait`.
// ---------------------------------------------------------------------------

const archiveArticle = defineTask({
	id: "archive-article",
	run: async (payload: { title: string; summary: string }, ctx) => {
		const record = await ctx.step.run("write-archive-record", async () => {
			ctx.logger.info("archiving article", { title: payload.title });
			return { archivedAt: new Date().toISOString(), title: payload.title };
		});
		return { archiveId: randomUUID(), ...record };
	},
});

// ---------------------------------------------------------------------------
// Task: publish-article — multi-step task: `step.run`, `step.sleep`, an approval
// token (`createToken`/`waitForToken`, completed from the "outside" via
// `engine.tokens.complete`) and a subtask (`step.triggerAndWait`).
// ---------------------------------------------------------------------------

const publishArticle = defineTask({
	id: "publish-article",
	maxDurationMs: 60_000,
	run: async (payload: { url: string }, ctx) => {
		const draft = await ctx.step.run("fetch-draft", async () => {
			// Simulated fetch — no real network calls, so the example is fast and deterministic.
			await delay(50);
			return { title: `Draft from ${payload.url}`, body: "Lorem ipsum dolor sit amet." };
		});

		const summary = await ctx.step.run("summarize", async () => {
			await delay(50);
			return `${draft.title}: ${draft.body.slice(0, 24)}...`;
		});

		await ctx.step.sleep("cooldown", "2s");

		const token = await ctx.step.createToken("approval", { timeout: "20s" });
		await ctx.step.run("request-approval", async () => {
			ctx.logger.info("approval requested", { tokenId: token.id });
		});
		const approval = await ctx.step.waitForToken<boolean>(token);
		if (!approval.ok || !approval.output) {
			return { status: "rejected" as const, title: draft.title };
		}

		const archived = await ctx.step.triggerAndWait(
			"archive",
			archiveArticle,
			{ title: draft.title, summary },
			{ timeout: "10s" }
		);
		if (!archived.ok) {
			return {
				status: "archive-failed" as const,
				title: draft.title,
				error: archived.error.message,
			};
		}

		return {
			status: "published" as const,
			title: draft.title,
			summary,
			archiveId: archived.output.archiveId,
		};
	},
});

/**
 * Watches `publishArticle`'s run and approves its `"approval"` token as soon as it shows up
 * (`ctx.step.createToken` records a `token` step whose output is `{ id }`) — simulating an
 * external actor (e.g. a UI button) calling `engine.tokens.complete` from outside the task.
 */
async function driveApprovalToken(engine: Engine, handle: RunHandle<unknown>): Promise<void> {
	let completedToken = false;
	for await (const update of engine.runs.subscribe(handle)) {
		console.log(`  [publish-article] status -> ${update.status}`);
		if (completedToken || update.status !== "waiting") continue;
		const steps = await engine.runs.steps(handle);
		const tokenStep = steps.find(s => s.kind === "token" && s.name === "approval");
		const output = tokenStep?.output;
		if (output && typeof output === "object" && "id" in output && typeof output.id === "string") {
			completedToken = true;
			const outcome = await engine.tokens.complete(output.id, true);
			console.log(`  [publish-article] approved token ${output.id}:`, outcome);
		}
	}
}

// ---------------------------------------------------------------------------
// Task: research-agent — a simple AI agent loop with a mocked LLM: each LLM decision
// and each tool call is its own `step.run`, so a crash mid-loop replays without
// re-calling the (mocked) LLM or re-running a tool that already ran.
// ---------------------------------------------------------------------------

type ToolName = "search" | "calculate";
type AgentDecision =
	| { type: "tool_call"; tool: ToolName; args: string }
	| { type: "final"; answer: string };
type ToolCallRecord = { tool: ToolName; result: string };

/** Deterministic fake LLM: no network call, same input always produces the same decision. */
function fakeLlm(question: string, history: readonly ToolCallRecord[]): AgentDecision {
	if (history.length === 0) return { type: "tool_call", tool: "search", args: question };
	if (history.length === 1) return { type: "tool_call", tool: "calculate", args: "2+2" };
	const [searched, calculated] = history;
	return {
		type: "final",
		answer:
			`Based on "${searched?.result}" and "${calculated?.result}", ` +
			`the answer to "${question}" is 42.`,
	};
}

function runTool(tool: ToolName, args: string): string {
	if (tool === "search")
		return `top result for "${args}": minitrigger is a durable workflow library`;
	return `${args} = 4`;
}

const MAX_AGENT_ITERATIONS = 5;

const researchAgent = defineTask({
	id: "research-agent",
	run: async (payload: { question: string }, ctx) => {
		const history: ToolCallRecord[] = [];
		for (let i = 0; i < MAX_AGENT_ITERATIONS; i++) {
			const decision = await ctx.step.run(`llm-${i}`, () => fakeLlm(payload.question, history));
			if (decision.type === "final") {
				return { answer: decision.answer, toolCalls: history.length };
			}
			const result = await ctx.step.run(`tool-${decision.tool}-${i}`, () =>
				runTool(decision.tool, decision.args)
			);
			history.push({ tool: decision.tool, result });
		}
		throw new Error("research-agent: exceeded max iterations without a final answer");
	},
});

// ---------------------------------------------------------------------------
// Cron schedules: a declarative one (`defineTask({ schedule })`, synced on `start()`)
// and an ad-hoc one (`engine.schedules.upsert`), both on the `-system` queue.
// ---------------------------------------------------------------------------

// Declared with a cron pattern that won't fire during this short-lived demo (Mondays at
// 09:00 UTC) — this only demonstrates that `start()` syncs it into Postgres + BullMQ.
const weeklyDigest = defineTask({
	id: "weekly-digest",
	schedule: { cron: "0 9 * * 1", timezone: "UTC" },
	run: async () => {},
});

// Registered normally; scheduled ad-hoc below via `engine.schedules.upsert` with a
// 6-field (seconds) cron so the demo can actually observe it firing.
const pingTask = defineTask({
	id: "ping",
	run: async () => {},
});

const PING_SCHEDULE_ID = "ping-demo";

// ---------------------------------------------------------------------------
// Wiring it all together.
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	const engine = createEngine({
		postgres: DATABASE_URL,
		redis: REDIS_URL,
		schema: "minitrigger_example",
		prefix: "minitrigger_example",
		tasks: [publishArticle, archiveArticle, researchAgent, weeklyDigest, pingTask],
	});

	const unsubscribes: Array<() => void> = [];
	const on: EngineEvents["on"] = (event, handler) => {
		const off = engine.events.on(event, handler);
		unsubscribes.push(off);
		return off;
	};

	try {
		console.log("\n== engine.migrate() ==");
		const migration = await engine.migrate();
		console.log("migrate result:", migration);

		console.log("\n== engine.start() ==");
		await engine.start();

		on("run:completed", e => console.log(`  [event] run:completed ${e.taskId} (${e.runId})`));
		on("run:failed", e => console.log(`  [event] run:failed ${e.taskId} (${e.runId})`, e.error));

		console.log(
			"\n== workflow: publish-article (step.run, step.sleep, approval token, subtask) =="
		);
		const publishHandle = await engine.trigger(publishArticle, {
			url: "https://example.com/article",
		});
		await driveApprovalToken(engine, publishHandle);
		const publishRun: Run<unknown> | null = await engine.runs.get(publishHandle);
		console.log("publish-article finished:", publishRun?.status, publishRun?.output);
		if (publishRun?.status !== "completed") {
			throw new Error(`publish-article did not complete: ${JSON.stringify(publishRun)}`);
		}

		console.log("\n== AI agent loop with a mocked LLM (tool calls recorded as steps) ==");
		const agentHandle = await engine.trigger(researchAgent, { question: "What is minitrigger?" });
		for await (const update of engine.runs.subscribe(agentHandle)) {
			console.log(`  [research-agent] status -> ${update.status}`);
		}
		const agentRun = await engine.runs.get(agentHandle);
		console.log("research-agent finished:", agentRun?.status, agentRun?.output);
		if (agentRun?.status !== "completed") {
			throw new Error(`research-agent did not complete: ${JSON.stringify(agentRun)}`);
		}

		console.log("\n== cron schedules (declarative + engine.schedules.upsert) ==");
		const declared = (await engine.schedules.list()).find(s => s.id === weeklyDigest.id);
		console.log("declarative schedule synced on start():", declared);

		await engine.schedules.upsert({ id: PING_SCHEDULE_ID, task: pingTask, cron: "*/3 * * * * *" });
		console.log(`upserted ad-hoc schedule "${PING_SCHEDULE_ID}" (every 3s)`);
		await waitFor(async () => (await engine.runs.list({ taskId: pingTask.id })).items.length >= 1, {
			timeoutMs: 15_000,
			intervalMs: 250,
			label: `"${PING_SCHEDULE_ID}" schedule to fire at least once`,
		});
		console.log(`"${PING_SCHEDULE_ID}" schedule fired at least once`);

		console.log("\n== cleanup: removing the demo schedules ==");
		console.log("deleted ping-demo:", await engine.schedules.delete(PING_SCHEDULE_ID));
		console.log("deleted weekly-digest:", await engine.schedules.delete(weeklyDigest.id));
	} finally {
		for (const off of unsubscribes) off();
		console.log("\n== engine.stop() ==");
		await engine.stop();
	}
}

// Safety net only (does not keep the process alive on its own): forces a non-zero exit
// if something hangs well past the expected run time, instead of the script stalling.
const watchdog = setTimeout(() => {
	console.error("minitrigger example: watchdog timeout exceeded, forcing exit");
	process.exit(1);
}, 25_000);
watchdog.unref();

main().then(
	() => {
		clearTimeout(watchdog);
		console.log("\nminitrigger example ran successfully");
	},
	(error: unknown) => {
		clearTimeout(watchdog);
		console.error("\nminitrigger example failed:", error);
		process.exitCode = 1;
	}
);
