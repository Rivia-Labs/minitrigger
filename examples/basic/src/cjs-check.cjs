// Validates that the CJS build of `minitrigger` is usable via require().
"use strict";

const { parseDuration } = require("minitrigger");

if (typeof parseDuration !== "function") {
	console.error(`CJS check failed: parseDuration is not a function (got ${typeof parseDuration})`);
	process.exit(1);
}

const ms = parseDuration("5m");
if (ms !== 5 * 60_000) {
	console.error(`CJS check failed: parseDuration("5m") returned ${ms}, expected ${5 * 60_000}`);
	process.exit(1);
}

console.log('CJS check passed: require("minitrigger") works and parseDuration is a function');
