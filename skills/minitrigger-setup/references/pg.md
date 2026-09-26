# Plain `pg` / other frameworks

No framework lifecycle to hook into — two files and `SIGTERM` handling.

```ts
// src/jobs/engine.ts
import { createEngine } from "@rivia-labs/minitrigger";
import { tasks } from "./tasks/index.js";

export const engine = createEngine({
	postgres: process.env.DATABASE_URL!,
	redis: process.env.REDIS_URL!,
	schema: "myapp",
	prefix: "myapp",
	tasks,
});
```

```ts
// src/worker.ts — the long-running process that actually executes tasks
import { engine } from "./jobs/engine.js";

async function main() {
	await engine.migrate();
	await engine.start();
	console.log("worker started");
}

async function shutdown() {
	await engine.stop(); // graceful; waits up to shutdownTimeoutMs, then hands back active runs
	process.exit(0);
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

main().catch((error) => {
	console.error(error);
	process.exit(1);
});
```

Any other entrypoint (a CLI script, a cron-triggered job, another service) that only needs to
enqueue work imports the same `engine` and calls `engine.trigger(...)` — it must never call
`engine.start()` unless it is meant to also run workers.

If this process created its own `pg.Pool`/`ioredis` instance for its own purposes (unrelated to
minitrigger), close those yourself after `engine.stop()` resolves — minitrigger never touches
connections it didn't create itself.
