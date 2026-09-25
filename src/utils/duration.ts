import { ConfigError } from "../errors.js";

/**
 * A duration expressed either as a number of milliseconds, or as a string
 * with a unit suffix (`ms`, `s`, `m`, `h`, `d`, `w`), e.g. `"5m"`, `"2d"`,
 * `"1.5h"`, `"500ms"`.
 *
 * The `${number}` template is looser than the runtime grammar: TypeScript's numeric-string
 * matching for template literal types also accepts things like exponential notation
 * (`"1e21ms"`) or `"Infinity"`/`"NaN"`-shaped strings, which {@link DURATION_PATTERN} (only
 * plain digits, an optional single decimal point, then a unit) rejects at runtime. Tightening
 * the type itself to match exactly would need a much more elaborate template-literal pattern
 * for arguably little benefit, since `parseDuration` is strict regardless of what the type
 * accepted -- this is a deliberate compile-time/runtime split, not an oversight.
 */
export type Duration = number | `${number}${"ms" | "s" | "m" | "h" | "d" | "w"}`;

type DurationUnit = "ms" | "s" | "m" | "h" | "d" | "w";

const DURATION_PATTERN = /^(\d+(?:\.\d+)?)(ms|s|m|h|d|w)$/;

function isDurationUnit(value: string): value is DurationUnit {
	return (
		value === "ms" ||
		value === "s" ||
		value === "m" ||
		value === "h" ||
		value === "d" ||
		value === "w"
	);
}

function unitToMs(unit: DurationUnit): number {
	switch (unit) {
		case "ms":
			return 1;
		case "s":
			return 1_000;
		case "m":
			return 60_000;
		case "h":
			return 3_600_000;
		case "d":
			return 86_400_000;
		case "w":
			return 604_800_000;
	}
}

/**
 * Parses a {@link Duration} into an integer number of milliseconds.
 *
 * Numbers are treated as milliseconds directly and must be finite and >= 0.
 * Strings must match `<number><unit>` (e.g. `"5m"`, `"1.5h"`).
 *
 * @throws {ConfigError} if the input isn't a valid duration.
 */
export function parseDuration(duration: Duration): number {
	if (typeof duration === "number") {
		if (!Number.isFinite(duration) || duration < 0) {
			throw new ConfigError(
				`invalid duration: ${duration} (numeric durations must be finite and >= 0)`
			);
		}
		return Math.round(duration);
	}

	const match = DURATION_PATTERN.exec(duration);
	const amount = match?.[1];
	const unit = match?.[2];
	if (!match || amount === undefined || unit === undefined || !isDurationUnit(unit)) {
		throw new ConfigError(`invalid duration: "${duration}" (use ms, s, m, h, d or w)`);
	}

	const ms = Number(amount) * unitToMs(unit);
	if (!Number.isFinite(ms)) {
		// A numeric string with enough digits (e.g. hundreds of digits) still matches
		// `DURATION_PATTERN`, but `Number(amount)` overflows to `Infinity`.
		throw new ConfigError(`invalid duration: "${duration}" (numeric part is too large)`);
	}

	return Math.round(ms);
}
