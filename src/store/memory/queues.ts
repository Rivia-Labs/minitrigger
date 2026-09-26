import type { QueueStateRecord, QueueStateStore } from "../types.js";
import type { Exec } from "./resolve.js";
import { pgText, type QueueStateRow } from "./state.js";

function mapQueueState(row: QueueStateRow): QueueStateRecord {
	return { queue: row.queue, paused: row.paused, updatedAt: new Date(row.updatedAt) };
}

/** Decision 27: in-memory `queue_state` (same semantics as `store/sql/queues.ts`). */
export function createQueueStateStore(exec: Exec): QueueStateStore {
	return {
		setPaused(queue, paused) {
			return exec.run(tx => {
				const name = pgText(queue);
				const prev = tx.db.queueState.get(name);
				const row: QueueStateRow = { queue: name, paused, updatedAt: tx.now };
				tx.set(tx.db.queueState, name, row);
				return { record: mapQueueState(row), changed: (prev?.paused ?? false) !== paused };
			});
		},
		get(queue) {
			return exec.run(tx => {
				const row = tx.db.queueState.get(queue);
				return row ? mapQueueState(row) : null;
			});
		},
		list() {
			return exec.run(tx =>
				[...tx.db.queueState.values()]
					.sort((a, b) => (a.queue < b.queue ? -1 : a.queue > b.queue ? 1 : 0))
					.map(mapQueueState)
			);
		},
	};
}
