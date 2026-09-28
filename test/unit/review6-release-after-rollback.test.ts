/**
 * Review 6 item L3: `releaseAfterRollback` must not rely on the localized `severity` alone. With a
 * non-English `lc_messages`, a FATAL arrives as e.g. "SCHWERWIEGEND"/"FATAL" translated, and the
 * connection-level SQLSTATE classes (57P admin shutdown & co., 08 connection exception) must still
 * destroy the client. `pg` 8 does not expose the non-localized severity (`V` field).
 */
import { describe, expect, it } from "vitest";
import { releaseAfterRollback } from "../../src/store/sql/db.js";

function outcome(err: unknown, rolledBack = true): "released" | "destroyed" {
	let result: "released" | "destroyed" | undefined;
	releaseAfterRollback(
		e => {
			result = e === undefined ? "released" : "destroyed";
		},
		err,
		rolledBack
	);
	if (!result) throw new Error("release not called");
	return result;
}

const serverError = (code: string, severity: string) =>
	Object.assign(new Error(`server error ${code}`), { code, severity });

describe("releaseAfterRollback (review 6 L3)", () => {
	it("an ordinary statement error rolled back returns the client", () => {
		expect(outcome(serverError("57014", "ERROR"))).toBe("released");
		expect(outcome(serverError("22012", "FEHLER"))).toBe("released");
	});

	it("an admin-shutdown class (57P*) destroys the client whatever the localized severity", () => {
		expect(outcome(serverError("57P01", "SCHWERWIEGEND"))).toBe("destroyed");
		expect(outcome(serverError("57P02", "FATAL"))).toBe("destroyed");
		expect(outcome(serverError("57P05", "ОШИБКА"))).toBe("destroyed");
	});

	it("a connection-exception class (08*) destroys the client", () => {
		expect(outcome(serverError("08006", "ERROR"))).toBe("destroyed");
		expect(outcome(serverError("08P01", "FEHLER"))).toBe("destroyed");
	});

	it("FATAL/PANIC severities, a failed ROLLBACK and non-server errors still destroy it", () => {
		expect(outcome(serverError("XX000", "FATAL"))).toBe("destroyed");
		expect(outcome(serverError("XX000", "PANIC"))).toBe("destroyed");
		expect(outcome(serverError("57014", "ERROR"), false)).toBe("destroyed");
		expect(outcome(new Error("Connection terminated unexpectedly"))).toBe("destroyed");
	});
});
