/**
 * ORM adapters (decision 26): `$n` placeholder scanning shared by the knex/drizzle adapters, and
 * a scan of every statement the `trigger(..., { tx })` path sends through the caller's client.
 */
import { describe, expect, it } from "vitest";
import { escapeKnexSql, splitPlaceholders, toKnexRaw } from "../../src/adapters/placeholders.js";
import { createSqlTxRunStore } from "../../src/store/sql/index.js";
import type { NewRun } from "../../src/store/types.js";

describe("splitPlaceholders", () => {
	it("splits on $n outside literals, in order, with repeats", () => {
		const { segments, indexes } = splitPlaceholders("SELECT $1, $2::uuid, $1 + $10");
		expect(segments).toEqual(["SELECT ", ", ", "::uuid, ", " + ", ""]);
		expect(indexes).toEqual([1, 2, 1, 10]);
	});

	it("ignores $n inside quoted literals, identifiers, comments and dollar quotes", () => {
		const text = `SELECT '$1', 'it''s $2', "col$3", E'\\'$4', $$ $5 $$, $tag$ $6 $tag$ -- $7
			/* $8 /* nested $9 */ still */ $1`;
		const { indexes, segments } = splitPlaceholders(text);
		expect(indexes).toEqual([1]);
		expect(segments.join("?")).toBe(`${text.slice(0, -2)}?`);
	});

	it("does not treat identifier characters followed by $n as a placeholder", () => {
		expect(splitPlaceholders("SELECT a$1 FROM t").indexes).toEqual([]);
	});

	it("rejects an unterminated literal (never guesses where the SQL ends)", () => {
		expect(() => splitPlaceholders("SELECT 'abc")).toThrow(/unterminated/);
		expect(() => splitPlaceholders("SELECT $x$ abc")).toThrow(/unterminated/);
	});

	it("memoizes per SQL text: the identical text returns the same result object", () => {
		const text = `memoize-probe-${Math.random()}`;
		const first = splitPlaceholders(`SELECT ${text}, $1`);
		const second = splitPlaceholders(`SELECT ${text}, $1`);
		expect(second).toBe(first);
		expect(second).toEqual(first);
	});

	it("does not cache a text that threw (an unterminated literal never gets memoized wrongly)", () => {
		const text = `SELECT 'unterminated-probe-${Math.random()}`;
		expect(() => splitPlaceholders(text)).toThrow(/unterminated/);
		expect(() => splitPlaceholders(text)).toThrow(/unterminated/);
	});

	it("bounds the cache: once past capacity, the oldest entries are evicted (recomputed on reuse)", () => {
		const prefix = `evict-probe-${Math.random()}-`;
		const first = splitPlaceholders(`SELECT ${prefix}0, $1`);
		// Fill well past the cache's capacity (64) with distinct texts.
		for (let i = 1; i < 100; i++) splitPlaceholders(`SELECT ${prefix}${i}, $1`);
		const recomputedFirst = splitPlaceholders(`SELECT ${prefix}0, $1`);
		expect(recomputedFirst).toEqual(first); // same shape
		expect(recomputedFirst).not.toBe(first); // but evicted: a fresh object

		// A text used most recently is still cached.
		const recent = splitPlaceholders(`SELECT ${prefix}99, $1`);
		const recentAgain = splitPlaceholders(`SELECT ${prefix}99, $1`);
		expect(recentAgain).toBe(recent);
	});
});

describe("toKnexRaw", () => {
	it("maps $n to ? (padded with spaces) with one binding per occurrence and escapes literal ?", () => {
		const raw = toKnexRaw("SELECT $2, data ? 'k', '?', $1, $2", ["a", "b"]);
		expect(raw.sql).toBe("SELECT  ? , data \\? 'k', '\\?',  ? ,  ? ");
		expect(raw.bindings).toEqual(["b", "a", "b"]);
	});

	it("sends undefined as null (knex rejects undefined bindings, pg sends them as NULL)", () => {
		expect(toKnexRaw("SELECT $1", [undefined]).bindings).toEqual([null]);
	});

	it("rejects a placeholder without a value", () => {
		expect(() => toKnexRaw("SELECT $2", ["a"])).toThrow(/\$2/);
		expect(() => toKnexRaw("SELECT $0", ["a"])).toThrow(/\$0/);
	});

	it("escapes every ? (jsonb operators, literals) and leaves :: casts alone", () => {
		expect(escapeKnexSql("a::text ? b")).toBe("a::text \\? b");
	});

	it("rejects a segment containing a backslash (unsafe next to an injected ? escape)", () => {
		expect(() => escapeKnexSql("a\\b")).toThrow(/backslash/);
		expect(() => toKnexRaw("SELECT $1, 'a\\b'", ["v"])).toThrow(/backslash/);
	});
});

describe("SQL sent by trigger(..., { tx })", () => {
	/** Records every statement of a tx-bound create, returning a created row to finish the call. */
	async function captureCreate(input: Partial<NewRun>) {
		const statements: { text: string; values: unknown[] }[] = [];
		const client = {
			async query(text: string, values: unknown[] = []) {
				statements.push({ text, values });
				const row = {
					id: "00000000-0000-4000-8000-000000000001",
					task_id: "t",
					queue: "q",
					status: "queued",
					enqueue_seq: 1,
					delay_ms: 0,
					updated_at: new Date(),
				};
				return { rows: input.idempotency ? [] : [row] };
			},
		};
		const store = createSqlTxRunStore(client, "my_schema");
		await store
			.create({
				id: "00000000-0000-4000-8000-000000000001",
				taskId: "t",
				queue: "q",
				payload: { q: "what? $1 'x'" },
				maxAttempts: 3,
				delayMs: 0,
				outbox: { channel: "mt_outbox_s_my_schema" },
				...input,
			})
			.catch(() => {});
		return statements;
	}

	it("uses only $n placeholders: no ? and no quote/dollar characters that would confuse the adapters", async () => {
		const statements = [
			...(await captureCreate({})),
			...(await captureCreate({ idempotency: { key: "k?'$1", ttlMs: 1000 } })),
			...(await captureCreate({ idempotency: { key: "k-null-ttl", ttlMs: null } })),
			...(await captureCreate({ concurrencyKey: "c" })),
		];
		expect(statements.length).toBeGreaterThanOrEqual(4);
		for (const { text, values } of statements) {
			// Values are never interpolated into the text.
			for (const value of values) {
				if (typeof value === "string" && value.length > 3) expect(text).not.toContain(value);
			}
			expect(text).not.toContain("?");
			// Every $n has a value, and the scanner finds exactly the placeholders there are.
			const { indexes } = splitPlaceholders(text);
			const all = [...text.matchAll(/\$(\d+)/g)].map(m => Number(m[1]));
			expect(indexes).toEqual(all);
			for (const i of indexes) expect(i).toBeLessThanOrEqual(values.length);
		}
	});

	it(
		"casts every $n explicitly: an ORM's raw-query path (e.g. Prisma) sends every value typed " +
			"as text, and Postgres has no implicit cast from text to uuid/jsonb/etc.",
		async () => {
			const statements = [
				...(await captureCreate({})),
				...(await captureCreate({ idempotency: { key: "k1", ttlMs: 1000 } })),
				...(await captureCreate({ idempotency: { key: "k2", ttlMs: null } })),
				...(await captureCreate({ concurrencyKey: "c" })),
			];
			// The idempotency cases also exercise the owner-lookup fallback statement (no claim row
			// returned by the fake client above), so this covers the claim CTE and that lookup too.
			expect(statements.length).toBeGreaterThanOrEqual(4);
			for (const { text } of statements) {
				for (const match of text.matchAll(/\$\d+/g)) {
					const after = text.slice(
						(match.index ?? 0) + match[0].length,
						(match.index ?? 0) + match[0].length + 2
					);
					expect(after, `param ${match[0]} not cast in: ${text}`).toBe("::");
				}
			}
		}
	);
});
