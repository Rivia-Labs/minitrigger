/**
 * Builds the engine's task/queue registry (SPEC.md §3, Phase 3 task 3.1).
 *
 * Grouping rule: tasks are grouped by `queue.name` (or the implicit
 * `"default"` queue when `queue` is omitted, see `task.ts`). A task that
 * declares no queue settings besides the name just *references* the queue
 * and inherits whatever another task in the same group declares. Two tasks
 * declaring *different* values for the same field of the same queue is a
 * configuration error, reported with the conflicting task ids and values.
 */

import { ConfigError, TaskNotRegisteredError } from "./errors.js";
import type { AnyTask, TaskQueueOptions } from "./task.js";
import { validateQueueName } from "./utils/identifiers.js";

/** A queue name's settings, resolved from every task that declares or references it. */
export type ResolvedQueue = {
	name: string;
	concurrency: number;
	globalConcurrency?: number;
	rateLimit?: { max: number; durationMs: number };
	/** Ids of every task assigned to this queue, in registration order. */
	taskIds: string[];
};

export type Registry = {
	tasks: Map<string, AnyTask>;
	queues: Map<string, ResolvedQueue>;
};

const DEFAULT_QUEUE_NAME = "default";
/** Applied when no task declaring this queue specifies a `concurrency`. */
const DEFAULT_QUEUE_CONCURRENCY = 10;

/** Builds the `{ tasks, queues }` registry from every task passed to `createEngine`. */
export function buildRegistry(tasks: readonly AnyTask[]): Registry {
	const taskMap = buildTaskMap(tasks);
	const queues = buildQueueMap(tasks);
	return { tasks: taskMap, queues };
}

function buildTaskMap(tasks: readonly AnyTask[]): Map<string, AnyTask> {
	const taskMap = new Map<string, AnyTask>();
	for (const task of tasks) {
		const existing = taskMap.get(task.id);
		if (existing) {
			throw new ConfigError(
				`duplicate task id "${task.id}": registered by two tasks ` +
					`(queue "${queueNameOf(existing)}" and queue "${queueNameOf(task)}")`
			);
		}
		taskMap.set(task.id, task);
	}
	return taskMap;
}

function queueNameOf(task: AnyTask): string {
	return task.options.queue?.name ?? DEFAULT_QUEUE_NAME;
}

type FieldEntry<T> = { taskId: string; value: T };

type QueueAccumulator = {
	name: string;
	taskIds: string[];
	concurrency: FieldEntry<number>[];
	globalConcurrency: FieldEntry<number>[];
	rateLimit: FieldEntry<{ max: number; durationMs: number }>[];
};

function buildQueueMap(tasks: readonly AnyTask[]): Map<string, ResolvedQueue> {
	const accumulators = new Map<string, QueueAccumulator>();

	for (const task of tasks) {
		const declared = task.options.queue;
		const name = declared?.name ?? DEFAULT_QUEUE_NAME;
		validateQueueName(name);

		let acc = accumulators.get(name);
		if (!acc) {
			acc = { name, taskIds: [], concurrency: [], globalConcurrency: [], rateLimit: [] };
			accumulators.set(name, acc);
		}
		acc.taskIds.push(task.id);
		if (declared) validateQueueNumbers(name, task.id, declared);

		if (declared?.concurrency !== undefined) {
			acc.concurrency.push({ taskId: task.id, value: declared.concurrency });
		}
		if (declared?.globalConcurrency !== undefined) {
			acc.globalConcurrency.push({ taskId: task.id, value: declared.globalConcurrency });
		}
		if (declared?.rateLimit !== undefined) {
			acc.rateLimit.push({ taskId: task.id, value: declared.rateLimit });
		}
	}

	const queues = new Map<string, ResolvedQueue>();
	for (const acc of accumulators.values()) {
		const concurrency =
			resolveField(acc.name, "concurrency", acc.concurrency, numberEquals, String) ??
			DEFAULT_QUEUE_CONCURRENCY;
		const globalConcurrency = resolveField(
			acc.name,
			"globalConcurrency",
			acc.globalConcurrency,
			numberEquals,
			String
		);
		const rateLimit = resolveField(
			acc.name,
			"rateLimit",
			acc.rateLimit,
			rateLimitEquals,
			formatRateLimit
		);

		const resolved: ResolvedQueue = { name: acc.name, concurrency, taskIds: acc.taskIds };
		if (globalConcurrency !== undefined) resolved.globalConcurrency = globalConcurrency;
		if (rateLimit !== undefined) resolved.rateLimit = rateLimit;
		queues.set(acc.name, resolved);
	}
	return queues;
}

/** `concurrency`, `globalConcurrency` and `rateLimit.{max,durationMs}` must be integers >= 1. */
function validateQueueNumbers(queueName: string, taskId: string, declared: TaskQueueOptions): void {
	const check = (field: string, value: unknown) => {
		if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
			throw new ConfigError(
				`queue "${queueName}" (declared by task "${taskId}"): ${field} must be an integer >= 1, got ${String(value)}`
			);
		}
	};
	if (declared.concurrency !== undefined) check("concurrency", declared.concurrency);
	if (declared.globalConcurrency !== undefined) {
		check("globalConcurrency", declared.globalConcurrency);
	}
	if (declared.rateLimit !== undefined) {
		check("rateLimit.max", declared.rateLimit.max);
		check("rateLimit.durationMs", declared.rateLimit.durationMs);
	}
}

/**
 * Resolves one queue field from every explicit declaration of it: `undefined`
 * when nobody declared it, the shared value when every declaration agrees,
 * or a {@link ConfigError} listing every conflicting task/value.
 */
function resolveField<T>(
	queueName: string,
	fieldName: string,
	entries: FieldEntry<T>[],
	equals: (a: T, b: T) => boolean,
	format: (value: T) => string
): T | undefined {
	const first = entries[0];
	if (first === undefined) return undefined;

	for (const entry of entries) {
		if (!equals(entry.value, first.value)) {
			const list = entries.map(e => `"${e.taskId}" (${format(e.value)})`).join(", ");
			throw new ConfigError(
				`queue "${queueName}" has conflicting "${fieldName}" declared across tasks: ${list}`
			);
		}
	}
	return first.value;
}

function numberEquals(a: number, b: number): boolean {
	return a === b;
}

function rateLimitEquals(
	a: { max: number; durationMs: number },
	b: { max: number; durationMs: number }
): boolean {
	return a.max === b.max && a.durationMs === b.durationMs;
}

function formatRateLimit(value: { max: number; durationMs: number }): string {
	return `{ max: ${value.max}, durationMs: ${value.durationMs} }`;
}

/** Looks up a task by id (or `Task` value) in the registry, or throws {@link TaskNotRegisteredError}. */
export function getTask(registry: Registry, taskOrId: AnyTask | string): AnyTask {
	const id = typeof taskOrId === "string" ? taskOrId : taskOrId.id;
	const task = registry.tasks.get(id);
	if (!task) {
		throw new TaskNotRegisteredError(`task "${id}" is not registered in this engine`);
	}
	return task;
}
