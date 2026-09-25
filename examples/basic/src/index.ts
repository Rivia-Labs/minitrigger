// Consumer example for the `minitrigger` package.
// Only ever import from the package's public entry point — never internal paths.
import { type Duration, parseDuration } from "minitrigger";

function describe(input: Duration): string {
	const ms = parseDuration(input);
	return `${JSON.stringify(input)} -> ${ms}ms`;
}

const examples: Duration[] = ["5m", "2d", "500ms", 250];

for (const example of examples) {
	console.log(describe(example));
}

console.log("minitrigger example ran successfully");
