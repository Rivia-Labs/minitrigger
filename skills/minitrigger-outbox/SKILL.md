---
name: minitrigger-outbox
description: Find dual writes (a business database write followed by a separate engine.trigger() call outside that transaction) and refactor them to minitrigger's transactional trigger / outbox pattern (trigger(task, payload, { tx })), so the business write and the run commit or roll back together. Use when the user reports "the job never queued after a crash" / "order saved but email never sent" style bugs, or asks to wrap a trigger in a transaction.
when_to_use: "the job never got queued", "order saved but the email job never ran", "wrap this trigger in a transaction", "use the outbox pattern", "trigger inside a transaction"
---

# Refactor a dual write to minitrigger's transactional trigger (outbox)

## How to spot a dual write

Grep for `engine.trigger(` (or `.trigger(` against whatever the engine instance is called) and,
for each call site, check whether it runs on the same open transaction/connection as the business
write it's meant to accompany:

- A call using the pool directly (`pool.query(...)` then, in a separate `await`,
  `engine.trigger(...)` with no `tx`) right after (or before) a related business write is the
  smell: if the process crashes between the two, or the business write's transaction rolls back
  after the trigger already fired, the two writes disagree.
- A call already passing `{ tx: client }` where `client` is *not* the same client the business
  write used (e.g. a client from a different pool, or one that never ran `BEGIN`) is the same bug
  wearing a disguise — `tx` must be the transaction that also holds the business write.
- A call inside a `try {} catch {}` that only rolls back the business write on failure, but the
  trigger call sits outside that `try` (or before the `BEGIN`), is worth flagging even if it
  "usually" works.

## The fix

`tx` must be a node-postgres `PoolClient`/`Client` (or any object with the same
`query(text, values)` shape bound to an open transaction — the seam for ORM adapters) on which
`BEGIN` has already run. Passing the `Pool` itself is **rejected** — its queries would run outside
the transaction.

```ts
import { Pool } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL }); // same database as the engine

const client = await pool.connect();
try {
	await client.query("BEGIN");
	const { rows } = await client.query(
		"INSERT INTO orders (customer_id, total) VALUES ($1, $2) RETURNING id",
		[customerId, total],
	);
	await engine.trigger(sendReceipt, { orderId: rows[0].id }, {
		tx: client,
		idempotencyKey: `receipt-${rows[0].id}`, // optional; works inside the transaction too
	});
	await client.query("COMMIT"); // only now can the run execute
} catch (error) {
	await client.query("ROLLBACK"); // the run is gone too; nothing ever executes
	throw error;
} finally {
	client.release();
}
```

Nothing is enqueued before `COMMIT`. The insert also queues one `NOTIFY` per transaction
(however many runs it triggers), delivered on commit; every started engine `LISTEN`s on a
schema-scoped channel and dispatches the committed run(s) within milliseconds. If the
notification is lost (listener reconnecting, no started engine at commit time, a pooler that
breaks `LISTEN`), every started engine also sweeps every `outbox.pollIntervalMs` (default 2 s), so
the run is still dispatched — just not at millisecond latency.

## Worked before/after example

Before — two independent calls, no atomicity:

```ts
app.post("/orders", async (req, res) => {
	const order = await db.query("INSERT INTO orders (customer_id, total) VALUES ($1, $2) RETURNING id", [
		req.body.customerId,
		req.body.total,
	]);
	await engine.trigger(sendReceipt, { orderId: order.rows[0].id }); // separate write, separate failure mode
	res.json({ id: order.rows[0].id });
});
```

After — one transaction, one outcome:

```ts
app.post("/orders", async (req, res) => {
	const client = await pool.connect();
	try {
		await client.query("BEGIN");
		const order = await client.query(
			"INSERT INTO orders (customer_id, total) VALUES ($1, $2) RETURNING id",
			[req.body.customerId, req.body.total],
		);
		await engine.trigger(sendReceipt, { orderId: order.rows[0].id }, { tx: client });
		await client.query("COMMIT");
		res.json({ id: order.rows[0].id });
	} catch (error) {
		await client.query("ROLLBACK");
		throw error;
	} finally {
		client.release();
	}
});
```

## PgBouncer / connection pooler note

PgBouncer in **transaction-pooling mode** breaks `LISTEN` (the session isn't kept across
transactions): runs still dispatch, but only via the `outbox.pollIntervalMs` sweep (2 s default),
not the millisecond `NOTIFY`-driven path. Point the **engine's own** `postgres` config at a
session-mode pool, or directly at Postgres, if millisecond dispatch matters. The caller's own `tx`
client (the one running the business transaction) can come from anywhere that keeps one
transaction pinned to one server connection — that constraint is unrelated to how the engine
itself connects.

## `outbox` config (on `createEngine`, not per-call)

| Option | Default | Notes |
|---|---|---|
| `outbox.pollIntervalMs` | `2 000` | Sweep interval every started engine uses as a fallback to `NOTIFY`. |
| `outbox.batchSize` | `500` | Rows claimed per sweep statement. |
| `outbox.reconnectMaxMs` | `30 000` | Upper bound of the dedicated LISTEN client's reconnect backoff. |
| `outbox.notify` | `true` | `false` skips the commit-time `pg_notify` entirely, relying only on the poll sweep. |

Postgres serializes the commits of transactions that `NOTIFY` (a database-wide lock taken at
commit); at very high rates of small triggering transactions, either batch several triggers per
transaction or set `outbox.notify: false` to trade millisecond dispatch latency for avoiding that
lock — worth it specifically when the serialized-commit cost matters more than latency.

## Limitations to flag during a refactor

- **Not combinable with `debounce`** — a debounced trigger would lock the shared pending run until
  the caller's transaction ends.
- **Not supported by `createTestEngine`** — passing `tx` there throws `ConfigError`. Test
  outbox-triggered code paths against a real `Engine` on a disposable test Postgres, or restructure
  the test to exercise the non-`tx` `trigger()` path separately (e.g. unit-test the business logic
  and the trigger call independently).
- `delay` in `TriggerOptions` counts from the **transaction's own** `now()` (Postgres clock at
  `BEGIN`/statement time), not wall-clock time when the code runs.
- Idempotency inside a transaction works normally: the key claim and the run insert are **one
  statement** (no `SAVEPOINT`), so hundreds of keyed triggers in one transaction are fine — no
  subtransaction-cache overflow risk.

## What to tell the user

Summarize: which call site(s) were dual writes, what changed (the `tx` now shared with the
business write, any `idempotencyKey` added), and follow-ups — e.g. "if this code path is tested,
update the test per `minitrigger-testing`'s note on `tx` not being supported by
`createTestEngine`" or "the engine's own `postgres` connection should move to session-pooling mode
for millisecond dispatch, since production runs behind PgBouncer in transaction mode."
