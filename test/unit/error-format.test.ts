import { createContext, runInContext } from "node:vm";
import { describe, expect, it } from "vitest";
import { formatError, truncateUtf8 } from "../../src/utils/error-format.js";

describe("formatError", () => {
	it("formats a plain Error", () => {
		const formatted = formatError(new Error("boom"), false);
		expect(formatted.name).toBe("Error");
		expect(formatted.message).toBe("boom");
		expect(formatted.retryable).toBe(false);
		expect(typeof formatted.stack).toBe("string");
		expect(formatted.cause).toBeUndefined();
	});

	it("preserves a custom error name", () => {
		class MyError extends Error {
			constructor(message: string) {
				super(message);
				this.name = "MyError";
			}
		}
		expect(formatError(new MyError("nope"), true).name).toBe("MyError");
	});

	it("truncates the stack to 4096 bytes", () => {
		const err = new Error("boom");
		err.stack = "x".repeat(10_000);
		const formatted = formatError(err, false);
		expect(Buffer.byteLength(formatted.stack ?? "", "utf8")).toBeLessThanOrEqual(4096);
	});

	it("formats the cause chain recursively, up to depth 3", () => {
		const root = new Error("root");
		const mid = new Error("mid", { cause: root });
		const top = new Error("top", { cause: mid });
		const formatted = formatError(top, false);
		expect(formatted.message).toBe("top");
		expect(formatted.cause?.message).toBe("mid");
		expect(formatted.cause?.cause?.message).toBe("root");
	});

	it("caps cause depth at 3 levels", () => {
		let err = new Error("level-0");
		for (let i = 1; i <= 5; i++) {
			err = new Error(`level-${i}`, { cause: err });
		}
		let formatted = formatError(err, false);
		let depth = 0;
		while (formatted.cause) {
			formatted = formatted.cause;
			depth += 1;
		}
		expect(depth).toBeLessThanOrEqual(3);
	});

	it("converts a thrown string sensibly", () => {
		const formatted = formatError("plain string failure", true);
		expect(formatted.name).toBe("Error");
		expect(formatted.message).toBe("plain string failure");
		expect(formatted.retryable).toBe(true);
	});

	it("converts other thrown values sensibly", () => {
		expect(formatError({ code: 42 }, false).message).toBe(JSON.stringify({ code: 42 }));
		expect(formatError(42, false).message).toBe("42");
		expect(formatError(undefined, false).message).toBe("undefined");
	});

	it("never copies arbitrary properties off the error", () => {
		const err = new Error("boom") as Error & { secret: string };
		err.secret = "should-not-leak";
		const formatted = formatError(err, false) as unknown as Record<string, unknown>;
		expect(formatted.secret).toBeUndefined();
	});

	it("caps message to 4096 bytes", () => {
		const err = new Error("x".repeat(10_000));
		const formatted = formatError(err, false);
		expect(Buffer.byteLength(formatted.message, "utf8")).toBeLessThanOrEqual(4096);
	});

	it("never splits a surrogate pair when truncating a stack made of astral characters", () => {
		// U+1F600 (😀) is a 4-byte UTF-8 sequence / UTF-16 surrogate pair. Pack the budget so a
		// naive byte-slice lands mid-character.
		const err = new Error("boom");
		err.stack = "😀".repeat(2_000); // 8000 bytes, well over the 4096 cap
		const formatted = formatError(err, false);
		const stack = formatted.stack ?? "";
		expect(Buffer.byteLength(stack, "utf8")).toBeLessThanOrEqual(4096);
		// A lone surrogate would make the string ill-formed; assert it's well-formed instead.
		expect(stack.isWellFormed()).toBe(true);
	});

	it("formats a cross-realm error (not instanceof this realm's Error) via util.types.isNativeError", () => {
		const context = createContext({});
		const crossRealmError = runInContext('new TypeError("cross realm")', context) as unknown;
		expect(crossRealmError instanceof Error).toBe(false);

		const formatted = formatError(crossRealmError, false);
		expect(formatted.name).toBe("TypeError");
		expect(formatted.message).toBe("cross realm");
	});
});

describe("truncateUtf8", () => {
	it("returns the text unchanged when it already fits", () => {
		expect(truncateUtf8("hello", 100)).toBe("hello");
	});

	it("truncates on a byte boundary without exceeding maxBytes", () => {
		const text = "x".repeat(100);
		const result = truncateUtf8(text, 10);
		expect(Buffer.byteLength(result, "utf8")).toBeLessThanOrEqual(10);
		expect(result).toBe("x".repeat(10));
	});

	it("never splits a UTF-16 surrogate pair (astral characters stay well-formed)", () => {
		const text = "😀".repeat(500); // each 😀 is 4 UTF-8 bytes / 2 UTF-16 code units
		for (const budget of [1, 2, 3, 4, 5, 7, 9, 13, 17]) {
			const result = truncateUtf8(text, budget);
			expect(result.isWellFormed()).toBe(true);
			expect(Buffer.byteLength(result, "utf8")).toBeLessThanOrEqual(budget);
		}
	});

	it("never splits a multi-byte (non-astral) UTF-8 sequence", () => {
		const text = "café".repeat(50); // "é" is 2 bytes in UTF-8
		const result = truncateUtf8(text, 5);
		expect(result.isWellFormed()).toBe(true);
		expect(Buffer.byteLength(result, "utf8")).toBeLessThanOrEqual(5);
	});
});
