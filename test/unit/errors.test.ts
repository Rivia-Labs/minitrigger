import { describe, expect, it } from "vitest";
import {
	AbortTaskRunError,
	ConfigError,
	DuplicateStepError,
	ExecutionLostError,
	MinitriggerError,
	NonDeterminismError,
	SerializationError,
	TaskNotRegisteredError,
} from "../../src/errors.js";

const subclasses = [
	ConfigError,
	SerializationError,
	AbortTaskRunError,
	NonDeterminismError,
	DuplicateStepError,
	ExecutionLostError,
	TaskNotRegisteredError,
];

describe("MinitriggerError hierarchy", () => {
	it("sets name on the base class", () => {
		const err = new MinitriggerError("oops");
		expect(err.name).toBe("MinitriggerError");
		expect(err).toBeInstanceOf(Error);
	});

	it.each(subclasses)("sets name correctly for %s", ErrorClass => {
		const err = new ErrorClass("oops");
		expect(err.name).toBe(ErrorClass.name);
		expect(err).toBeInstanceOf(MinitriggerError);
		expect(err).toBeInstanceOf(Error);
		expect(err.message).toBe("oops");
	});

	it("supports the `cause` option", () => {
		const cause = new Error("root cause");
		const err = new ConfigError("bad config", { cause });
		expect(err.cause).toBe(cause);
	});

	it("sets name from an explicit field, not from the (minification-unsafe) constructor name", () => {
		// Simulate a minifier/bundler renaming the class (e.g. to "a"): if `name` were still
		// derived from `this.constructor.name`, every instance created afterwards would report
		// the mangled name instead of "ConfigError".
		const originalName = ConfigError.name;
		Object.defineProperty(ConfigError, "name", { value: "a", configurable: true });
		try {
			const err = new ConfigError("oops");
			expect(err.name).toBe("ConfigError");
		} finally {
			Object.defineProperty(ConfigError, "name", { value: originalName, configurable: true });
		}
	});
});
