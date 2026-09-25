import { describe, expectTypeOf, it } from "vitest";
import type { Json, Serializable } from "../../src/utils/serialization.js";

/**
 * Compile-time only assertions for `Serializable<T>`. These bodies do
 * (almost) nothing at runtime -- the interesting checks are the type errors
 * `pnpm typecheck` would raise if `Serializable<T>` regressed, and the
 * `// @ts-expect-error` cases, which fail typecheck if the type stops
 * rejecting what it should.
 */

// Usage pattern 1: `T & Serializable<T>` as a parameter constraint, with T inferred
// from the call site (this is how `trigger()`/`step.run()` etc. are expected to use it).
function acceptSerializable<T>(value: T & Serializable<T>): T {
	return value;
}

// Usage pattern 2: `Serializable<T>` alone, with T supplied/inferred from elsewhere.
function declareSerializableOutput<T>(_value: Serializable<T>): void {
	// no-op: only here to prove this shape type-checks
}

interface UserPayload {
	id: string;
	count: number;
	active: boolean;
	tags: string[];
	meta?: { note: string };
	coords: readonly [number, number];
	nested: { list: string[][] };
	maybeNull: string | null;
	kind: "a" | "b";
}

type AliasPayload = {
	id: string;
	when: string; // ISO date string, not a Date
	items: readonly string[];
};

interface Simple {
	a: string;
	b: number | null;
	c?: boolean;
}

describe("Serializable<T> compile-time checks", () => {
	it("accepts an interface with optionals, nested arrays, tuples, unions and null", () => {
		const payload: UserPayload = {
			id: "1",
			count: 1,
			active: true,
			tags: ["a"],
			coords: [1, 2],
			nested: { list: [["a", "b"]] },
			maybeNull: null,
			kind: "a",
		};
		acceptSerializable(payload);
		declareSerializableOutput<UserPayload>(payload);
	});

	it("accepts a `type` alias with readonly arrays", () => {
		const payload: AliasPayload = { id: "1", when: "2024-01-01", items: ["a"] };
		acceptSerializable(payload);
	});

	it("resolves to an equal type for a flat interface with optional/nullable fields", () => {
		expectTypeOf<Serializable<Simple>>().toEqualTypeOf<Simple>();
	});

	it("rejects a Date field", () => {
		interface WithDate {
			when: Date;
		}
		const bad: WithDate = { when: new Date() };
		// @ts-expect-error Date is not JSON-serializable
		acceptSerializable(bad);
	});

	it("rejects a Map field", () => {
		interface WithMap {
			m: Map<string, number>;
		}
		const bad: WithMap = { m: new Map() };
		// @ts-expect-error Map is not JSON-serializable
		acceptSerializable(bad);
	});

	it("rejects a Set field", () => {
		type WithSet = { s: Set<string> };
		const bad: WithSet = { s: new Set() };
		// @ts-expect-error Set is not JSON-serializable
		acceptSerializable(bad);
	});

	it("rejects a function field", () => {
		interface WithFn {
			fn: () => void;
		}
		const bad: WithFn = { fn: () => undefined };
		// @ts-expect-error functions are not JSON-serializable
		acceptSerializable(bad);
	});

	it("rejects a Date nested inside an array", () => {
		interface WithDateArray {
			dates: Date[];
		}
		const bad: WithDateArray = { dates: [new Date()] };
		// @ts-expect-error nested Date is not JSON-serializable
		acceptSerializable(bad);
	});

	it("rejects a Date hidden inside a union member", () => {
		interface WithUnionDate {
			when: string | Date;
		}
		const bad: WithUnionDate = { when: new Date() };
		// @ts-expect-error union member Date is not JSON-serializable
		acceptSerializable(bad);
	});

	it("rejects bigint and symbol fields", () => {
		const badBigint: { n: bigint } = { n: 1n };
		// @ts-expect-error bigint is not JSON-serializable
		acceptSerializable(badBigint);

		const badSymbol: { s: symbol } = { s: Symbol("x") };
		// @ts-expect-error symbol is not JSON-serializable
		acceptSerializable(badSymbol);
	});

	// Regressions for the reviewed defects: `Serializable<void>` / `Serializable<unknown>`
	// used to resolve to the `SerializableError` brand instead of `T`, which broke
	// `defineTask({ run: async () => {} })` (an `async () => {}` returns `Promise<void>`) and
	// any task/step typed to return `unknown`. `Serializable<Json>` used to hit TS2589
	// ("type instantiation is excessively deep") because the recursive mapped-type branch
	// walked into `Json`'s own self-referential shape instead of short-circuiting.
	it("resolves void to void (task/step run returning nothing)", () => {
		expectTypeOf<Serializable<void>>().toEqualTypeOf<void>();

		async function run(): Promise<void> {
			/* no-op */
		}
		function defineTask<T>(opts: { run: () => Promise<T & Serializable<T>> }): typeof opts {
			return opts;
		}
		defineTask({ run });
	});

	it("resolves unknown to unknown", () => {
		expectTypeOf<Serializable<unknown>>().toEqualTypeOf<unknown>();

		function acceptUnknown<T>(value: Serializable<T>): T {
			return value as T;
		}
		acceptUnknown<unknown>("anything" as unknown);
	});

	it("resolves Json to Json without excessive recursion (TS2589)", () => {
		expectTypeOf<Serializable<Json>>().toEqualTypeOf<Json>();
	});

	it("short-circuits a Json-shaped object field (e.g. `Json[]`) via the Json guard", () => {
		expectTypeOf<Serializable<{ data: Json[] }>>().toEqualTypeOf<{ data: Json[] }>();
	});

	it("still rejects a Date field on an otherwise Json-shaped object", () => {
		interface WithDateField {
			d: Date;
		}
		const bad: WithDateField = { d: new Date() };
		// @ts-expect-error Date is not JSON-serializable, even next to Json-like siblings
		acceptSerializable(bad);
	});
});
