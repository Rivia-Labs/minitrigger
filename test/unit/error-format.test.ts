import { createContext, runInContext } from "node:vm";
import { describe, expect, it } from "vitest";
import { formatError, sanitizeString, truncateUtf8 } from "../../src/utils/error-format.js";

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

	it("converts other thrown values sensibly, without dumping their properties (decision 12)", () => {
		// A plain object with no `message`/`name` collapses to a bounded type description, never
		// `JSON.stringify(value)` (that used to leak every property, however sensitive).
		expect(formatError({ code: 42 }, false).message).toBe(
			"a non-Error value of type Object was thrown"
		);
		expect(formatError(42, false).message).toBe("42");
		expect(formatError(undefined, false).message).toBe("undefined");
	});

	it("never copies arbitrary properties off the error", () => {
		const err = new Error("boom") as Error & { secret: string };
		err.secret = "should-not-leak";
		const formatted = formatError(err, false) as unknown as Record<string, unknown>;
		expect(formatted.secret).toBeUndefined();
	});

	describe("non-Error thrown values (decision 12: fixed format, no arbitrary properties)", () => {
		it("uses a non-Error's own string `message`/`name`, never its other properties", () => {
			const formatted = formatError(
				{ name: "AxiosError", message: "Request failed with status code 401" },
				true
			);
			expect(formatted.name).toBe("AxiosError");
			expect(formatted.message).toBe("Request failed with status code 401");
			expect(formatted.retryable).toBe(true);
		});

		it("axios-like error carrying headers/Authorization: those never appear anywhere in the result", () => {
			const axiosLike = {
				message: "Request failed with status code 401",
				name: "AxiosError",
				code: "ERR_BAD_REQUEST",
				config: {
					headers: { Authorization: "Bearer super-secret-token" },
					url: "https://api.internal/accounts",
				},
				response: {
					status: 401,
					headers: { "set-cookie": "session=super-secret-session" },
					data: { apiKey: "sk-super-secret-key" },
				},
			};
			const formatted = formatError(axiosLike, false);
			const dumped = JSON.stringify(formatted);
			expect(dumped).not.toContain("super-secret");
			expect(dumped).not.toContain("Authorization");
			expect(dumped).not.toContain("apiKey");
			expect(formatted).toEqual({
				name: "AxiosError",
				message: "Request failed with status code 401",
				retryable: false,
			});
		});

		it("throw response (a plain object with headers, no message): bounded type description only", () => {
			const response = {
				status: 500,
				headers: { authorization: "Bearer leaked-if-dumped" },
				body: { token: "should-not-leak" },
			};
			const formatted = formatError(response, true);
			const dumped = JSON.stringify(formatted);
			expect(dumped).not.toContain("leaked-if-dumped");
			expect(dumped).not.toContain("should-not-leak");
			expect(formatted.message).toBe("a non-Error value of type Object was thrown");
		});

		it("still formats a `cause` that is itself a non-Error object safely, and recurses", () => {
			const err = new Error("outer", {
				cause: { message: "inner failure", secret: "leak-me-not" },
			});
			const formatted = formatError(err, false);
			expect(formatted.cause?.message).toBe("inner failure");
			expect(JSON.stringify(formatted)).not.toContain("leak-me-not");
		});
	});

	describe("NUL byte / lone surrogate sanitization (Postgres jsonb rejects both)", () => {
		it("replaces a NUL byte in the message with U+FFFD instead of leaving it invalid", () => {
			const formatted = formatError(new Error("a\u0000b"), false);
			expect(formatted.message).toBe("a�b");
			expect(formatted.message.includes("\u0000")).toBe(false);
		});

		it("makes a lone surrogate in the message well-formed", () => {
			const formatted = formatError(new Error("a\uD800b"), false);
			expect(formatted.message.isWellFormed()).toBe(true);
		});

		it("sanitizes name, message, stack and the cause chain", () => {
			const err = new Error("msg\u0000\uD800", { cause: new Error("cause-msg\u0000\uD800") });
			err.name = "Bad\u0000Name";
			err.stack = "stack\u0000\uD800trace";
			const formatted = formatError(err, true);
			for (const s of [formatted.name, formatted.message, formatted.stack ?? ""]) {
				expect(s.includes("\u0000")).toBe(false);
				expect(s.isWellFormed()).toBe(true);
			}
			expect(formatted.cause?.message.includes("\u0000")).toBe(false);
			expect(formatted.cause?.message.isWellFormed()).toBe(true);
		});

		it("sanitizes a thrown string directly", () => {
			const formatted = formatError("bad\u0000\uD800string", false);
			expect(formatted.message.includes("\u0000")).toBe(false);
			expect(formatted.message.isWellFormed()).toBe(true);
		});

		it("sanitizes name/message extracted from a non-Error object", () => {
			const formatted = formatError({ name: "Bad\u0000", message: "msg\uD800" }, false);
			expect(formatted.name.includes("\u0000")).toBe(false);
			expect(formatted.message.isWellFormed()).toBe(true);
		});

		it("a NUL byte survives Postgres jsonb round-tripping once sanitized (sanity check on the fix)", () => {
			// Postgres rejects `\u0000` in jsonb text outright; the fix must ensure the character
			// never reaches the driver in the first place, not merely "not throw here".
			const formatted = formatError(new Error("a\u0000b\uD800"), false);
			const json = JSON.stringify(formatted);
			expect(() => JSON.parse(json)).not.toThrow();
			expect(json.includes("\\u0000")).toBe(false);
		});
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

describe("sanitizeString", () => {
	it("leaves an already-valid string unchanged", () => {
		expect(sanitizeString("hello world")).toBe("hello world");
	});

	it("replaces a NUL byte with U+FFFD", () => {
		expect(sanitizeString("a\u0000b")).toBe("a�b");
	});

	it("replaces every NUL byte, not just the first", () => {
		expect(sanitizeString("\u0000a\u0000b\u0000")).toBe("�a�b�");
	});

	it("makes a lone surrogate well-formed", () => {
		const result = sanitizeString("a\uD800b");
		expect(result.isWellFormed()).toBe(true);
	});

	it("leaves a valid surrogate pair (astral character) untouched", () => {
		expect(sanitizeString("😀")).toBe("😀");
	});

	it("handles both a NUL byte and a lone surrogate in the same string", () => {
		const result = sanitizeString("a\u0000b\uD800c");
		expect(result.includes("\u0000")).toBe(false);
		expect(result.isWellFormed()).toBe(true);
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
