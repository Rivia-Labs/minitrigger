# Express / Fastify

## Single process (simplest)

Wire it up in the same file as `app.listen(...)`:

```ts
// src/server.ts
import { createEngine } from "@rivia-labs/minitrigger";
import express from "express";
import { tasks } from "./jobs/tasks/index.js";

export const engine = createEngine({
	postgres: process.env.DATABASE_URL!,
	redis: process.env.REDIS_URL!,
	schema: "myapp",
	prefix: "myapp",
	tasks,
});

const app = express();
// ... routes, which call engine.trigger(...) ...

async function main() {
	await engine.migrate();
	await engine.start();
	const server = app.listen(process.env.PORT ?? 3000);

	async function shutdown() {
		await new Promise<void>((resolve) => server.close(() => resolve()));
		await engine.stop();
		process.exit(0);
	}
	process.on("SIGTERM", shutdown);
	process.on("SIGINT", shutdown);
}

main();
```

## Web + worker processes (recommended beyond a single instance)

Keep the HTTP process trigger-only (never call `start()` there — it can still `trigger()` and
query), and run a separate worker entrypoint:

```ts
// src/worker.ts
import { engine } from "./engine.js"; // the createEngine(...) instance, no HTTP server import

async function main() {
	await engine.migrate(); // or run this once from a deploy-time script instead — see the setup skill
	await engine.start();
}

async function shutdown() {
	await engine.stop();
	process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

main();
```

```ts
// src/server.ts
import { engine } from "./engine.js";
import express from "express";
// ... engine.trigger(...) from routes, engine.migrate()/start() never called here ...
```

Run the worker as its own process/service: `node dist/worker.js`. Both processes import the same
`createEngine(...)` instance definition (same `schema`/`prefix`/`tasks`) from a shared module —
they must agree on it, they just don't both call `start()`.
