# Next.js

**Hard requirement, not a suggestion:** minitrigger's workers need a long-running Node process. A
Next.js API route, route handler or server action runs in a serverless or edge runtime that does
not support a persistent background worker — `engine.start()` must never be called from any of
those. They may only `trigger()`.

```ts
// src/jobs/engine.ts — imported by both the Next.js app and the standalone worker below
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
// app/api/orders/route.ts (or a server action)
import { engine } from "@/jobs/engine";
import { sendReceipt } from "@/jobs/tasks";

export async function POST(request: Request) {
	const order = await createOrder(request);
	await engine.trigger(sendReceipt, { orderId: order.id });
	return Response.json({ id: order.id });
}
```

Deploy a **separate standalone worker** as its own service/container (not part of the Next.js
build/runtime):

```ts
// worker/index.ts — a small standalone script, deployed and run independently of the Next.js app
import { engine } from "../src/jobs/engine.js";

async function main() {
	await engine.migrate(); // or a deploy-time migration step, per the setup skill
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

The worker script and the Next.js app must share the exact same `schema`/`prefix`/task registry
(import from the same `src/jobs/engine.ts` and `src/jobs/tasks/index.ts`) so that triggers created
by the app are runnable by the worker.

If the Next.js app writes to Postgres inside a route handler and wants an outbox-style
`trigger(..., { tx })` (see `minitrigger-outbox`), get the transaction's `pg` client from
whatever data layer the app uses (`pg.Pool`, Prisma's `$queryRaw`/`$transaction` with a raw client
seam, Drizzle's transaction callback, etc.) — the same rules apply as any other framework.
