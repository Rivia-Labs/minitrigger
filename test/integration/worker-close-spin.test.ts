/**
 * Regression: BullMQ's `Worker.retryIfFailed` retries connection errors forever and skips its
 * delay once the worker is closing (bullmq 6.3.8/6.3.9). A job whose processor returns while or
 * after `closeWorkers` closes the workers/connections, and whose move to completed/failed then
 * fails with a connection error, used to spin in a zero-delay loop: 100% CPU, timers starved, the
 * process never exits. Fixed by `guardRetryLoopOnClose` in `src/queue/transport.ts`.
 *
 * The spin starves the event loop, so it would also starve vitest's own timeouts and hang the
 * suite. Each scenario therefore runs in a child process (`test/helpers/child/
 * worker-close-spin-child.ts`) that reports an event-loop-lag probe after every `stop()`; the
 * test kills it after a hard deadline, so a regression fails instead of hanging.
 */
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();

const register = fileURLToPath(new URL("../helpers/child/register-ts.mjs", import.meta.url));
const child = fileURLToPath(
	new URL("../helpers/child/worker-close-spin-child.ts", import.meta.url)
);
const cwd = fileURLToPath(new URL("../..", import.meta.url));

interface Probe {
	event: "probe";
	scenario: string;
	iteration: number;
	stopMs: number;
	maxLagMs: number;
	ticks: number;
	status: string | undefined;
}

interface ChildResult {
	probes: Probe[];
	done: boolean;
	killed: boolean;
	output: string;
}

/** Runs one scenario in a child process, killing it (SIGKILL) after `deadlineMs`. */
function runScenario(
	scenario: string,
	iterations: number,
	deadlineMs: number
): Promise<ChildResult> {
	return new Promise((resolve, reject) => {
		const proc = spawn(process.execPath, ["--import", register, child, scenario], {
			cwd,
			env: {
				...process.env,
				SPIN_SCHEMA: ns.schema,
				SPIN_PREFIX: ns.prefix,
				SPIN_DATABASE_URL: ns.databaseUrl,
				SPIN_REDIS_URL: ns.redisUrl,
				SPIN_ITERATIONS: String(iterations),
			},
			stdio: ["ignore", "pipe", "pipe"],
		});
		let stdout = "";
		let stderr = "";
		proc.stdout.on("data", chunk => {
			stdout += String(chunk);
		});
		proc.stderr.on("data", chunk => {
			stderr += String(chunk);
		});
		let killed = false;
		const timer = setTimeout(() => {
			killed = true;
			proc.kill("SIGKILL");
		}, deadlineMs);
		proc.on("error", error => {
			clearTimeout(timer);
			reject(error);
		});
		proc.on("close", () => {
			clearTimeout(timer);
			const events = stdout
				.split("\n")
				.filter(line => line.startsWith("{"))
				.map(line => JSON.parse(line) as { event: string });
			resolve({
				probes: events.filter((e): e is Probe => e.event === "probe"),
				done: events.some(e => e.event === "done"),
				killed,
				output: `${stdout}\n${stderr}`,
			});
		});
	});
}

/** The child finished every iteration, and no probe saw the event loop starved. */
function expectNoSpin(result: ChildResult, iterations: number): void {
	expect(result.killed, `child hung (BullMQ retry spin?):\n${result.output}`).toBe(false);
	expect(result.done, result.output).toBe(true);
	expect(result.probes).toHaveLength(iterations);
	for (const probe of result.probes) {
		expect(probe.maxLagMs, JSON.stringify(probe)).toBeLessThan(200);
		// Decision 20: the run was released in Postgres; its re-enqueue is the reconciler's job
		// if it did not land.
		expect(probe.status, JSON.stringify(probe)).toBe("queued");
	}
}

describe("stop() never leaves BullMQ spinning on a failed move to completed", () => {
	it("Redis unreachable with an active run (looped to beat the intermittency)", async () => {
		const iterations = 10;
		const result = await runScenario("redis-down", iterations, 50_000);
		expectNoSpin(result, iterations);
	}, 60_000);

	it("Redis up: the processor returns ~1 s after stop() closed every connection", async () => {
		const result = await runScenario("late-return", 1, 25_000);
		expectNoSpin(result, 1);
	}, 30_000);

	it("Redis up: a run ignoring ctx.signal returns ~1 s after stop() resolved", async () => {
		const result = await runScenario("ignores-signal", 2, 20_000);
		expectNoSpin(result, 2);
	}, 30_000);
});
