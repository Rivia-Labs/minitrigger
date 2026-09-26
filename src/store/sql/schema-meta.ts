/** `schema_meta` (migration 009): per-schema facts, e.g. the transport in use (decision 28). */
import { MinitriggerError } from "../../errors.js";
import type { SchemaMetaStore } from "../types.js";
import { direct, type SqlContext, type SqlExecutor } from "./db.js";

export function createSchemaMetaStore(exec: SqlExecutor, ctx: SqlContext): SchemaMetaStore {
	const table = ctx.t("schema_meta");

	async function get(key: string): Promise<string | null> {
		const { rows } = await direct(exec).query<{ value: string }>(
			`SELECT value FROM ${table} WHERE key = $1`,
			[key]
		);
		return rows[0]?.value ?? null;
	}

	return {
		get,
		async setIfAbsent(key, value) {
			// ON CONFLICT DO NOTHING waits for a concurrent inserter to commit. When nothing was
			// inserted, the winner is re-read in a NEW statement: a SELECT inside this statement
			// would still see the snapshot from before the concurrent transaction committed.
			const { rows } = await direct(exec).query<{ value: string }>(
				`INSERT INTO ${table} (key, value) VALUES ($1, $2)
				 ON CONFLICT (key) DO NOTHING
				 RETURNING value`,
				[key, value]
			);
			const inserted = rows[0];
			if (inserted) return inserted.value;
			const existing = await get(key);
			if (existing === null) {
				throw new MinitriggerError(
					`schema_meta: key "${key}" conflicted but could not be read back`
				);
			}
			return existing;
		},
	};
}
