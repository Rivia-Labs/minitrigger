# Modo de transporte só-Postgres — plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `createEngine({ transport: "postgres" })` roda o minitrigger sem Redis, com a mesma
semântica de runs, steps, waitpoints, schedules, pausa e batch ops do modo BullMQ (que continua o
padrão e não muda de comportamento).

**Architecture:** o núcleo (`createEngineCore`, executor, reconciler, retenção, schedules) não
bifurca; o modo Postgres é uma segunda montagem de `EngineBackend` (store SQL + transporte
Postgres + pub/sub por `NOTIFY`, sem streams). A fila é a própria linha de `runs`, reivindicada
com `FOR UPDATE SKIP LOCKED` num statement só; a `-system` vira a tabela `system_jobs`; um único
client `LISTEN` dedicado (o mesmo mecanismo do listener do outbox) acorda workers e assinantes, e
um poll adaptativo é a rede de segurança.

**Tech Stack:** TypeScript 6, Node >= 22, pg 8, Postgres >= 13, `cron-parser` 5.10.1 (única
dependência direta nova; é a versão que o BullMQ 6.3.8 já usa), vitest, Biome, tsup.

**Spec:** `docs/superpowers/specs/2026-09-26-postgres-transport-design.md` (ler inteira antes de
qualquer tarefa; as emendas à seção 2 da SPEC estão na seção 2 dela).

## Global Constraints

- Seção 2 de `docs/SPEC.md` é fixa; só as emendas literais da spec (seção 2) podem ser aplicadas.
- Migrations 001–008 não podem ser editadas; toda mudança de schema vai na `009_postgres_transport.sql`.
- Nenhuma dependência nova além de `cron-parser@^5.10.1` (`dependencies`, não peer).
- `bullmq` e `ioredis` viram peers opcionais; nada no grafo estático de `src/index.ts`,
  `src/testing/index.ts`, dos adaptadores e do backend Postgres pode importá-los em runtime nem
  expô-los em tipo público (hoje `src/testing/test-engine.ts` e `src/executor/system.ts` violam:
  Task 7 corrige). Só `src/backends/bullmq.ts` (carregado com `import()`) os alcança.
- Todo parâmetro enviado a uma query é referenciado no SQL (o `pg` manda parâmetros sem tipo e o
  Postgres recusa um não referenciado). Colunas em `UPDATE ... FROM` sempre qualificadas.
- Tempo sempre do banco (decisão 17): `now()` do Postgres alimenta `run_after`, `locked_until` e
  a próxima ocorrência de cron.
- Erros de infraestrutura transitória (timeouts de conexão) são `MinitriggerError`, nunca
  `ConfigError` (quarta revisão externa, item 2).
- Sem estado global (decisão 13): tudo em closures/instâncias.
- Comandos de verificação de cada tarefa: `pnpm typecheck && pnpm lint && pnpm test:unit`;
  integração com `pnpm infra:up` + `pnpm vitest run --project integration <arquivo>`.
- Biome: tabs, lineWidth 100; `import type` obrigatório para tipos (`verbatimModuleSyntax`).
- Mensagens de commit em inglês no formato do repositório (`feat(pg): ...`, `test: ...`), com a
  linha de atribuição `Co-Authored-By` que a sessão exigir.
- Nunca `SET` sem `LOCAL`, `LISTEN` ou outro estado de sessão em conexões do pool.

## Review Focus

1. `NOTIFY` com payload forjado ou vazio no canal de wake: deve acordar todas as filas
   registradas (um claim vazio cada) e nunca lançar nem parar o laço. Teste em Task 7.
2. Run `waiting` com `run_after` preenchido e um waitpoint ainda `pending` não aguardado (token
   criado, não esperado): o claim deve pegá-la (decisão 3), e um `waiting` com `run_after NULL`
   nunca. Teste em Task 3.
3. Processo morre entre o claim de um `schedule-tick` e a regravação da linha: `locked_until`
   vence, outro worker reprocessa, a chave `__schedule:id:ts` impede a segunda run, e a escrita
   tardia do primeiro worker não casa o fencing (`locked_by`). Testes em Task 3 e Task 10b.
4. `stop()` com `NOTIFY` chegando durante o hand-back, e com um claim em andamento no momento do
   `stop()`: nada é reivindicado depois do sinal de parada, a run trazida pelo claim em voo é
   devolvida, e a run devolvida é pega por outra engine em menos de 1 s. Testes em Task 7 e 10b.
5. `globalConcurrency` com duas engines reivindicando ao mesmo tempo: o pico de `running` nunca
   passa do limite (advisory lock por fila) e nunca há duplicata de execução. Testes em Task 3 e
   Task 12.
6. `NOTIFY` forjado no canal de cancel com o `runId` de uma run realmente em execução: a execução
   continua (o cancel é relido no banco). Testes em Task 5 e Task 12.
7. `concurrency_waits` continua crescendo entre claims `limited` sucessivos (o claim não o zera).
   Teste em Task 9.

---


### Task 1: Emendas à SPEC e esqueleto de docs

**Files:**
- Modify: `docs/SPEC.md` (seção 2: decisões 1, 8, 14, 15, 16, 21, 22 e nova 28; seção 3;
  seção 4)
- Modify: `CLAUDE.md` (lista "Decisões de arquitetura": nova linha 28)

**Interfaces:**
- Produces: o texto normativo que todas as tarefas seguintes citam.

- [ ] **Step 1: Aplicar as emendas literais**

Abrir `docs/superpowers/specs/2026-09-26-postgres-transport-design.md`, seção 2. Para cada uma
das decisões 1, 8, 14, 15, 16, 21 e 22 de `docs/SPEC.md`, acrescentar ao fim do item existente um
parágrafo começando com `**No modo Postgres:**` seguido do texto da spec, sem alterar o texto
anterior. Acrescentar a decisão 28 inteira depois da 27.

- [ ] **Step 2: Seção 3 e 4 da SPEC**

Na seção 3, depois do exemplo com `redis`, adicionar:

```ts
// Modo só-Postgres (decisão 28): sem redis nem prefix; ctx.stream, runs.readStream e
// queue.rateLimit são ConfigError neste modo.
const engineSemRedis = createEngine({
  postgres: process.env.DATABASE_URL!,
  transport: "postgres",
  schema: "minitrigger",
  tasks: [resumirVaga, salvarResumo, notificar],
  polling: { intervalMs: 1_000, jitterMs: 50 },
});
```

Na seção 4, acrescentar à árvore:

```
│   ├── backends/             # bullmq.ts (import() dinâmico), postgres.ts
│   ├── pg/                   # listener.ts, transport.ts, workers.ts, pubsub.ts, schedulers.ts
```

- [ ] **Step 3: CLAUDE.md**

Na lista numerada de decisões, depois da 27, adicionar:

```
28. Transporte Postgres (`transport: "postgres"`): fila = linha de `runs` com `SKIP LOCKED` (claim = `acquire`), `-system` = tabela `system_jobs` com fencing por `locked_by`, wake por `LISTEN/NOTIFY` (payload não confiável, agrupado) + poll adaptativo, pub/sub por `NOTIFY` só com id e sempre relido (cancel só aborta run `canceled`), `globalConcurrency` exato por advisory lock da fila, `cron-parser` para ticks (linha do tick nunca apagada); `prefix`/`redis`/`outbox`/`rateLimit`/streams são `ConfigError`; modo misto no mesmo schema recusado por `schema_meta`.
```

- [ ] **Step 4: Verificar e commitar**

Run: `pnpm lint` (Biome ignora `.md`, mas confirma que nada mais quebrou).

```bash
git add docs/SPEC.md CLAUDE.md docs/superpowers
git commit -m "docs(spec): amend section 2 for the Postgres transport (decision 28)"
```

---

### Task 2: Config — `transport`, `polling`, união discriminada, tipo estrutural de conexão

**Files:**
- Modify: `src/config.ts`
- Modify: `src/queue/connections.ts` (aceitar o tipo estrutural)
- Test: `test/unit/config.test.ts`, `test/unit/config-types.test.ts` (novo, testes de tipo)

**Interfaces:**
- Produces:
  - `EngineConfig` vira união discriminada (spec §3.10): `CommonConfig & (BullmqTransportConfig |
    PostgresTransportConfig)`. `BullmqTransportConfig = { transport?: "bullmq"; redis: string |
    { connection: RedisLike }; prefix?: string; outbox?: OutboxConfig; polling?: never }`;
    `PostgresTransportConfig = { transport: "postgres"; polling?: { intervalMs?: number;
    jitterMs?: number }; redis?: never; prefix?: never; outbox?: never }`. Os três tipos são
    exportados por `src/index.ts`.
  - `ResolvedConfig.transport: TransportKind`, `ResolvedConfig.redis: string | { connection:
    RedisLike } | undefined`, `ResolvedConfig.prefix: string` (no modo Postgres vale `schema`, só
    para código compartilhado que precisa de um nome; nunca vai para o Redis),
    `ResolvedConfig.polling: { intervalMs: number; jitterMs: number }`.
  - `export interface RedisLike` em `src/config.ts`: `{ duplicate(options?: object): RedisLike;
    publish(channel: string, message: string): Promise<number>; on(event: string, listener:
    (...args: unknown[]) => void): unknown; }`.
  - `DEFAULTS.polling = { intervalMs: 1_000, jitterMs: 50 }`, `MIN_POLLING_INTERVAL_MS = 100`.
  - Pool derivado: no modo Postgres `max(10, soma da concorrência das filas + systemConcurrency +
    5)`; no BullMQ a fórmula atual não muda.

- [ ] **Step 1: Testes que falham**

Acrescentar em `test/unit/config.test.ts`:

```ts
describe("transport", () => {
	const base = { postgres: "postgres://x", schema: "s", tasks: [] as AnyTask[] };

	it("defaults to bullmq and requires redis there", () => {
		const resolved = resolveConfig({ ...base, redis: "redis://r", prefix: "p" });
		expect(resolved.transport).toBe("bullmq");
		expect(() => resolveConfig({ ...base, prefix: "p" } as unknown as EngineConfig)).toThrow(
			/redis is required with transport "bullmq"/
		);
	});

	it("postgres mode rejects redis, prefix and outbox at runtime", () => {
		const pg = { ...base, transport: "postgres" as const };
		expect(() => resolveConfig({ ...pg, redis: "redis://r" } as unknown as EngineConfig)).toThrow(ConfigError);
		expect(() => resolveConfig({ ...pg, prefix: "p" } as unknown as EngineConfig)).toThrow(
			/prefix is not used with transport "postgres"/
		);
		expect(() => resolveConfig({ ...pg, outbox: { batchSize: 10 } } as unknown as EngineConfig)).toThrow(
			/outbox is not used with transport "postgres"/
		);
		const resolved = resolveConfig(pg);
		expect(resolved.redis).toBeUndefined();
		expect(resolved.prefix).toBe("s");
		expect(resolved.polling).toEqual({ intervalMs: 1_000, jitterMs: 50 });
	});

	it("postgres mode validates polling bounds", () => {
		const pg = { ...base, transport: "postgres" as const };
		expect(() => resolveConfig({ ...pg, polling: { intervalMs: 50 } })).toThrow(/polling.intervalMs/);
		expect(() => resolveConfig({ ...pg, polling: { jitterMs: -1 } })).toThrow(/polling.jitterMs/);
	});

	it("polling is rejected with transport bullmq", () => {
		expect(() =>
			resolveConfig({ ...base, redis: "redis://r", polling: { intervalMs: 500 } } as unknown as EngineConfig)
		).toThrow(/polling is only used with transport "postgres"/);
	});

	it("rejects an unknown transport", () => {
		expect(() => resolveConfig({ ...base, transport: "kafka" } as unknown as EngineConfig)).toThrow(ConfigError);
	});

	it("derived pool size includes systemConcurrency only in postgres mode", () => { /* comparar resolved.poolMax/derivação nos dois modos com a mesma fila de concurrency 20 */ });
});
```

`test/unit/config-types.test.ts` (padrão dos testes de tipo existentes, `expectTypeOf` +
`// @ts-expect-error`): `{ transport: "postgres", redis: "..." }`, `{ transport: "postgres",
prefix: "p" }`, `{ transport: "postgres", outbox: {} }` e `{ polling: {} }` sem `transport` não
compilam; `{ redis }` sem `transport` compila; um `Redis` do ioredis é atribuível a
`{ connection: RedisLike }`.

- [ ] **Step 2: Rodar e ver falhar**

Run: `pnpm vitest run test/unit/config.test.ts test/unit/config-types.test.ts`
Expected: FAIL (`transport` não existe em `ResolvedConfig`).

- [ ] **Step 3: Implementar**

Em `resolveConfig`, antes de validar `redis`:

```ts
const transport = (config as { transport?: unknown }).transport ?? "bullmq";
if (transport !== "bullmq" && transport !== "postgres") {
	throw new ConfigError(`transport must be "bullmq" or "postgres", got ${describeOffendingValue(transport)}`);
}
if (transport === "bullmq") {
	if (config.redis === undefined) throw new ConfigError('redis is required with transport "bullmq"');
	if (config.polling !== undefined) throw new ConfigError('polling is only used with transport "postgres"');
} else {
	if (config.redis !== undefined) throw new ConfigError('redis is not used with transport "postgres"; remove it or set transport: "bullmq"');
	if (config.prefix !== undefined) throw new ConfigError('prefix is not used with transport "postgres"; NOTIFY channels derive from schema');
	if (config.outbox !== undefined) throw new ConfigError('outbox is not used with transport "postgres"; trigger(..., { tx }) already writes to the queue');
}
const polling = {
	intervalMs: intInRange("polling.intervalMs", config.polling?.intervalMs ?? DEFAULTS.polling.intervalMs, MIN_POLLING_INTERVAL_MS, MAX_TIMER_MS),
	jitterMs: intInRange("polling.jitterMs", config.polling?.jitterMs ?? DEFAULTS.polling.jitterMs, 0, MAX_TIMER_MS),
};
```

(Dentro de `resolveConfig`, acessar os campos pela união com um alias `const c = config as
BullmqTransportConfig & PostgresTransportConfig & CommonConfig`, ou equivalente sem `any`.)

`prefix` resolvido: `transport === "postgres" ? schema : validateSqlIdentifier("prefix", config.prefix ?? DEFAULTS.prefix)`. Manter a
validação atual de `redis` e `outbox` só no ramo bullmq (no Postgres, `outbox` resolve para os
defaults, que ninguém lê). Substituir `import type { Redis } from "ioredis"` por
`RedisLike`; em `src/queue/connections.ts`, aceitar `RedisLike` na entrada e fazer o cast para
`Redis` do ioredis internamente (esse módulo só é alcançado pelo backend BullMQ, carregado com
`import()` dinâmico na Task 10c).

- [ ] **Step 4: Rodar tudo**

Run: `pnpm typecheck && pnpm lint && pnpm vitest run test/unit/config.test.ts test/unit/config-types.test.ts`
Expected: PASS. Se `typecheck` apontar usos de `resolved.redis` sem checar `undefined`, corrigir
com `if (resolved.redis === undefined) throw new ConfigError(...)` nos pontos do modo BullMQ
(serão movidos na Task 10a). Testes existentes que montam `EngineConfig` com spread podem precisar
de `as const` em `transport`.

- [ ] **Step 5: Commit**

```bash
git add src/config.ts src/queue/connections.ts src/index.ts test/unit/config.test.ts test/unit/config-types.test.ts
git commit -m "feat(config): transport union, polling and structural redis type for the Postgres mode"
```

---

### Task 3: Migration 009 e store SQL — `claimRuns`, `nextDueAt`, `SystemJobStore`, `SchemaMetaStore`

**Files:**
- Create: `src/migrations/009_postgres_transport.sql`
- Modify: `src/migrations/index.ts`
- Modify: `src/store/sql/db.ts` (`TableName` += `system_jobs`, `schema_meta`)
- Modify: `src/store/sql/resolve.ts` (`qualifiedRunColumns(alias)`)
- Create: `src/store/sql/claim.ts`
- Create: `src/store/sql/system-jobs.ts`
- Create: `src/store/sql/schema-meta.ts`
- Modify: `src/store/sql/index.ts` (novo tipo `SqlStore extends Store`)
- Modify: `src/store/types.ts` (tipos novos)
- Modify: `docs/store-contract.md` (seção "Transporte Postgres")
- Test: `test/integration/store-claim.test.ts`, `test/integration/migrate.test.ts`

**Interfaces:**
- Produces (em `src/store/types.ts`):

```ts
export interface ClaimRunsInput {
	queue: string;
	limit: number;
	/** `undefined` = sem limite global. Com limite, o claim é serializado por fila (exato). */
	globalConcurrency?: number;
}
export interface RunClaimStore {
	claimRuns(input: ClaimRunsInput): Promise<RunRecord[]>;
	/** Próximo `run_after` futuro da fila (só `queued`/`waiting`), ou null. */
	nextDueAt(queue: string): Promise<Date | null>;
}
export interface ScheduleTickData {
	scheduleId: string; cron: string; timezone?: string; schema: string; prevMillis: number;
}
export interface SystemJobRecord {
	jobId: string; name: string; data: Json; runAfter: Date; attempts: number; maxAttempts: number;
	/** Token do claim atual (fencing); `null` fora de um claim. */
	lockedBy: string | null;
}
export interface SystemJobStore {
	/** Insere se ausente (semântica de jobId do BullMQ: re-add é no-op). */
	upsert(input: { jobId: string; name: string; data?: Json; delayMs?: number; maxAttempts?: number }): Promise<void>;
	/** Tick de cron: cria, ou regrava só se `cron`/`timezone` mudaram (solta o lock). */
	upsertScheduleTick(input: { jobId: string; data: ScheduleTickData; runAt: Date }): Promise<"created" | "updated" | "unchanged">;
	/** Reivindica jobs vencidos: `locked_until = now() + lockMs`, `locked_by` novo. */
	claim(limit: number, lockMs: number): Promise<SystemJobRecord[]>;
	/** Todas as escritas abaixo casam `job_id` E `locked_by`; `false`/`"lost"` = lock perdido. */
	complete(jobId: string, lockedBy: string): Promise<boolean>;
	delay(jobId: string, lockedBy: string, delayMs: number): Promise<boolean>;
	/** attempts+1; abaixo de max → `run_after = now() + backoffMs`; senão apaga. */
	fail(jobId: string, lockedBy: string, backoffMs: number): Promise<"retried" | "exhausted" | "lost">;
	/** Tick concluído ou esgotado: próxima ocorrência no lugar, `attempts = 0`, lock solto. */
	rescheduleTick(jobId: string, lockedBy: string, next: { runAt: Date; prevMillis: number }): Promise<boolean>;
	/** Padrão: não apaga job travado (ativo), como o BullMQ. `evenIfLocked` para remover scheduler. */
	remove(jobId: string, options?: { evenIfLocked?: boolean }): Promise<boolean>;
	get(jobId: string): Promise<SystemJobRecord | null>;
	listByName(name: string): Promise<SystemJobRecord[]>;
	nextDueAt(): Promise<Date | null>;
	/** `now()` do banco (decisão 17), para calcular ocorrências de cron. */
	now(): Promise<Date>;
}
export interface SchemaMetaStore {
	get(key: string): Promise<string | null>;
	/** Grava se ausente; devolve o valor efetivo (o existente vence, relido se houve corrida). */
	setIfAbsent(key: string, value: string): Promise<string>;
}
```

  Em `src/store/sql/index.ts`: `export interface SqlStore extends Store { claim: RunClaimStore; systemJobs: SystemJobStore; meta: SchemaMetaStore; }` e `createSqlStore` devolve `SqlStore`.
  Em `src/store/sql/claim.ts`: `export function createRunClaimStore(exec: SqlExecutor, ctx: SqlContext): RunClaimStore`.

- [ ] **Step 1: Migration**

`src/migrations/009_postgres_transport.sql` (mesmo cabeçalho de comentário das anteriores sobre
nomes não qualificados e `search_path`):

```sql
CREATE TABLE system_jobs (
  job_id       text PRIMARY KEY,
  name         text NOT NULL,
  data         jsonb NOT NULL DEFAULT '{}'::jsonb,
  run_after    timestamptz NOT NULL,
  attempts     int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 1,
  locked_until timestamptz,
  locked_by    uuid,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_system_jobs_due ON system_jobs (run_after);

CREATE TABLE schema_meta (
  key        text PRIMARY KEY,
  value      text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_runs_claim ON runs (queue, run_after)
  WHERE status IN ('queued', 'waiting');
CREATE INDEX IF NOT EXISTS idx_runs_running_queue ON runs (queue)
  WHERE status = 'running';
```

Em `src/migrations/index.ts`: `import postgresTransportSql from "./009_postgres_transport.sql";`
e `{ version: 9, name: "postgres_transport", sql: postgresTransportSql }`. Em `db.ts`, `TableName`
ganha `"system_jobs" | "schema_meta"`.

- [ ] **Step 2: Teste de contrato que falha**

`test/integration/store-claim.test.ts`:

```ts
import { randomUUID } from "node:crypto";
import { beforeAll, describe, expect, it } from "vitest";
import { migrate } from "../../src/migrations/migrate.js";
import { createSqlStore, type SqlStore } from "../../src/store/sql/index.js";
import { useTestNamespace } from "../helpers/namespace.js";

const ns = useTestNamespace();
let store: SqlStore;
beforeAll(async () => {
	await migrate(ns.pool, ns.schema);
	store = createSqlStore(ns.pool, ns.schema);
});

async function newRun(queue: string, opts: { delayMs?: number; taskId?: string } = {}) {
	// `NewRun` (src/store/types.ts) exige `id` e `delayMs`; ver `test/helpers/run-fixtures.ts`.
	const created = await store.runs.create({
		id: randomUUID(), taskId: opts.taskId ?? "t", queue, payload: null, maxAttempts: 1,
		delayMs: opts.delayMs ?? 0,
	});
	if (created.kind !== "created") throw new Error(created.kind);
	return created.run;
}

describe("claimRuns", () => {
	it("claims due runs in run_after order and writes what acquire writes", async () => {
		const q = `q${Date.now()}`;
		const a = await newRun(q);
		const b = await newRun(q);
		const later = await newRun(q, { delayMs: 60_000 });
		await ns.pool.query(`UPDATE ${ns.schema}.runs SET concurrency_waits = 3 WHERE id = $1`, [b.id]);
		const claimed = await store.claim.claimRuns({ queue: q, limit: 10 });
		expect(claimed.map(r => r.id)).toEqual([a.id, b.id]);
		for (const r of claimed) {
			expect(r.status).toBe("running");
			expect(r.executionId).toMatch(/^[0-9a-f-]{36}$/);
			expect(r.startedAt).toBeInstanceOf(Date);
		}
		// enqueue_seq não muda no claim; concurrency_waits não é zerado (decisão 25, backoff crescente)
		expect(claimed[0]!.enqueueSeq).toBe(a.enqueueSeq);
		const { rows } = await ns.pool.query(`SELECT concurrency_waits FROM ${ns.schema}.runs WHERE id = $1`, [b.id]);
		expect(rows[0].concurrency_waits).toBe(3);
		expect((await store.runs.get(later.id))?.status).toBe("queued");
		expect(await store.claim.claimRuns({ queue: q, limit: 10 })).toEqual([]);
	});

	it("does not set started_at for a run with a concurrency key (the re-check does)", async () => {
		/* newRun + UPDATE concurrency_key = 'k'; claim → startedAt null */
	});

	it("respects limit, paused queues and globalConcurrency", async () => {
		const q = `q${Date.now()}p`;
		await newRun(q); await newRun(q); await newRun(q);
		expect((await store.claim.claimRuns({ queue: q, limit: 2 })).length).toBe(2);
		expect((await store.claim.claimRuns({ queue: q, limit: 5, globalConcurrency: 2 })).length).toBe(0);
		await store.queues.setPaused(q, true);
		expect((await store.claim.claimRuns({ queue: q, limit: 5 })).length).toBe(0);
		await store.queues.setPaused(q, false);
		expect((await store.claim.claimRuns({ queue: q, limit: 5 })).length).toBe(1);
	});

	it("globalConcurrency is exact under concurrent claims", async () => {
		const q = `q${Date.now()}g`;
		for (let i = 0; i < 10; i++) await newRun(q);
		const results = await Promise.all(
			Array.from({ length: 4 }, () => store.claim.claimRuns({ queue: q, limit: 5, globalConcurrency: 3 }))
		);
		expect(results.flat().length).toBe(3);
		expect(new Set(results.flat().map(r => r.id)).size).toBe(3);
	});

	it("claims a waiting run with run_after set, never one with run_after NULL", async () => {
		const q = `q${Date.now()}w`;
		const ready = await newRun(q);
		const blocked = await newRun(q);
		await ns.pool.query(
			`UPDATE ${ns.schema}.runs SET status = 'waiting', run_after = now() - interval '1 second' WHERE id = $1`, [ready.id]);
		await ns.pool.query(
			`UPDATE ${ns.schema}.runs SET status = 'waiting', run_after = NULL WHERE id = $1`, [blocked.id]);
		const claimed = await store.claim.claimRuns({ queue: q, limit: 10 });
		expect(claimed.map(r => r.id)).toEqual([ready.id]);
	});

	it("skips outbox_pending rows and rows locked by another transaction", async () => {
		const q = `q${Date.now()}o`;
		const pending = await newRun(q);
		await ns.pool.query(`UPDATE ${ns.schema}.runs SET outbox_pending = true WHERE id = $1`, [pending.id]);
		expect(await store.claim.claimRuns({ queue: q, limit: 10 })).toEqual([]);
		const locked = await newRun(q);
		const client = await ns.pool.connect();
		try {
			await client.query("BEGIN");
			await client.query(`SELECT 1 FROM ${ns.schema}.runs WHERE id = $1 FOR UPDATE`, [locked.id]);
			expect(await store.claim.claimRuns({ queue: q, limit: 10 })).toEqual([]);
			await client.query("ROLLBACK");
		} finally { client.release(); }
		expect((await store.claim.claimRuns({ queue: q, limit: 10 })).map(r => r.id)).toEqual([locked.id]);
	});

	it("nextDueAt returns the earliest future run_after of the queue", async () => {
		const q = `q${Date.now()}n`;
		expect(await store.claim.nextDueAt(q)).toBeNull();
		await newRun(q, { delayMs: 30_000 });
		await newRun(q, { delayMs: 5_000 });
		const next = await store.claim.nextDueAt(q);
		expect(next!.getTime() - Date.now()).toBeGreaterThan(3_000);
		expect(next!.getTime() - Date.now()).toBeLessThan(6_000);
	});
});

describe("systemJobs", () => {
	it("upsert keeps the existing row; claim locks with a token; complete is fenced", async () => {
		const id = `job-${Date.now()}`;
		await store.systemJobs.upsert({ jobId: id, name: "retention", data: { a: 1 } });
		await store.systemJobs.upsert({ jobId: id, name: "retention", data: { a: 2 } });
		expect((await store.systemJobs.get(id))?.data).toEqual({ a: 1 });
		const claimed = (await store.systemJobs.claim(10, 5_000)).find(j => j.jobId === id)!;
		expect(claimed.lockedBy).toMatch(/^[0-9a-f-]{36}$/);
		expect((await store.systemJobs.claim(10, 5_000)).map(j => j.jobId)).not.toContain(id);
		expect(await store.systemJobs.complete(id, randomUUID())).toBe(false);
		expect(await store.systemJobs.complete(id, claimed.lockedBy!)).toBe(true);
		expect(await store.systemJobs.get(id)).toBeNull();
	});

	it("delay reschedules in place; fail retries then exhausts (and deletes)", async () => {
		const id = `job-${Date.now()}f`;
		await store.systemJobs.upsert({ jobId: id, name: "waitpoint-expire", maxAttempts: 2 });
		const take = async () => (await store.systemJobs.claim(10, 5_000)).find(j => j.jobId === id);
		let job = await take();
		expect(await store.systemJobs.delay(id, job!.lockedBy!, 60_000)).toBe(true);
		expect(await take()).toBeUndefined();
		await ns.pool.query(`UPDATE ${ns.schema}.system_jobs SET run_after = now() WHERE job_id = $1`, [id]);
		job = await take();
		expect(await store.systemJobs.fail(id, job!.lockedBy!, 0)).toBe("retried");
		expect((await store.systemJobs.get(id))?.attempts).toBe(1);
		job = await take();
		expect(await store.systemJobs.fail(id, randomUUID(), 0)).toBe("lost");
		expect(await store.systemJobs.fail(id, job!.lockedBy!, 0)).toBe("exhausted");
		expect(await store.systemJobs.get(id)).toBeNull();
	});

	it("a lock that expired is claimable again, and the old holder's writes do not match", async () => {
		const id = `job-${Date.now()}l`;
		await store.systemJobs.upsert({ jobId: id, name: "reconcile" });
		const first = (await store.systemJobs.claim(10, 1)).find(j => j.jobId === id)!;
		await new Promise(r => setTimeout(r, 20));
		const second = (await store.systemJobs.claim(10, 5_000)).find(j => j.jobId === id)!;
		expect(second.lockedBy).not.toBe(first.lockedBy);
		expect(await store.systemJobs.complete(id, first.lockedBy!)).toBe(false);
		expect(await store.systemJobs.get(id)).not.toBeNull();
	});

	it("upsertScheduleTick is a no-op for the same cron/timezone and rewrites on change", async () => {
		const jobId = `schedule-tick:s${Date.now()}`;
		const data = { scheduleId: "s", cron: "* * * * *", schema: ns.schema, prevMillis: 1 };
		const at1 = new Date(Date.now() + 60_000);
		expect(await store.systemJobs.upsertScheduleTick({ jobId, data, runAt: at1 })).toBe("created");
		expect(await store.systemJobs.upsertScheduleTick({ jobId, data: { ...data, prevMillis: 2 }, runAt: new Date(Date.now() + 5_000) })).toBe("unchanged");
		expect((await store.systemJobs.get(jobId))?.runAfter.getTime()).toBe(at1.getTime());
		expect(await store.systemJobs.upsertScheduleTick({ jobId, data: { ...data, cron: "*/5 * * * *" }, runAt: at1 })).toBe("updated");
	});

	it("rescheduleTick moves the row to the next occurrence, resets attempts and releases the lock", async () => {
		/* upsertScheduleTick com runAt = now; claim; fail uma vez (attempts 1); claim; rescheduleTick(runAt futuro, prevMillis X) → true;
		   get: runAfter futuro, attempts 0, lockedBy null, data.prevMillis X; rescheduleTick com token velho → false */
	});

	it("remove refuses a locked job unless evenIfLocked", async () => { /* ... */ });
});

describe("schemaMeta", () => {
	it("setIfAbsent keeps the first value", async () => {
		expect(await store.meta.get("transport")).toBeNull();
		expect(await store.meta.setIfAbsent("transport", "postgres")).toBe("postgres");
		expect(await store.meta.setIfAbsent("transport", "bullmq")).toBe("postgres");
	});

	it("concurrent setIfAbsent with different values agree on one winner", async () => {
		const results = await Promise.all([store.meta.setIfAbsent("k2", "a"), store.meta.setIfAbsent("k2", "b")]);
		expect(results[0]).toBe(results[1]);
	});
});
```

Em `test/integration/migrate.test.ts`, atualizar a contagem esperada de migrations para 9 e o
teste que lista tabelas para incluir `system_jobs` e `schema_meta`.

- [ ] **Step 3: Rodar e ver falhar**

Run: `pnpm vitest run --project integration test/integration/store-claim.test.ts`
Expected: FAIL (`store.claim` undefined).

- [ ] **Step 4: Implementar `claim.ts`**

Em `src/store/sql/resolve.ts`, ao lado de `RUN_COLUMNS`:

```ts
/** RUN_COLUMNS com prefixo de alias: obrigatório em `UPDATE ... FROM` (a outra tabela também tem `id`). */
export function qualifiedRunColumns(alias: string): string {
	return RUN_COLUMNS.split(",").map(c => `${alias}.${c.trim()}`).join(", ");
}
```

```ts
import { atomic, direct, type Db, type SqlContext, type SqlExecutor } from "./db.js";
import { mapRun, qualifiedRunColumns, type RunRow } from "./resolve.js";
import type { ClaimRunsInput, RunClaimStore, RunRecord } from "../types.js";

export function createRunClaimStore(exec: SqlExecutor, ctx: SqlContext): RunClaimStore {
	const runs = ctx.t("runs");
	const queueState = ctx.t("queue_state");
	const returning = qualifiedRunColumns("r");

	async function claim(db: Db, queue: string, limit: number): Promise<RunRecord[]> {
		const { rows } = await db.query<RunRow>(
			`WITH picked AS (
			   SELECT r.id FROM ${runs} r
			   WHERE r.queue = $1
			     AND r.status IN ('queued', 'waiting')
			     AND r.run_after <= now()
			     AND NOT r.outbox_pending
			     AND NOT EXISTS (SELECT 1 FROM ${queueState} q WHERE q.queue = r.queue AND q.paused)
			   ORDER BY r.run_after, r.created_at, r.id
			   LIMIT $2::int
			   FOR UPDATE OF r SKIP LOCKED
			 )
			 UPDATE ${runs} r
			 SET status = 'running', execution_id = gen_random_uuid(), heartbeat_at = now(),
			     started_at = CASE WHEN r.concurrency_key IS NULL
			                       THEN coalesce(r.started_at, now()) ELSE r.started_at END,
			     debounce_key = NULL, updated_at = now()
			 FROM picked WHERE r.id = picked.id
			 RETURNING ${returning}`,
			[queue, limit]
		);
		// UPDATE ... RETURNING não garante ordem: reordenar como o SELECT.
		return rows.map(mapRun).sort(byRunAfterCreatedId);
	}

	return {
		async claimRuns({ queue, limit, globalConcurrency }: ClaimRunsInput): Promise<RunRecord[]> {
			if (limit <= 0) return [];
			if (globalConcurrency === undefined) return claim(direct(exec), queue, limit);
			return atomic(exec, async db => {
				// Decisão 28(e): serializa os claims da fila; o count abaixo vê todo `running` já
				// comitado (READ COMMITTED: snapshot novo por statement). Um bigint, fora de
				// `(hashtext(schema), 1|2)` (decisão 16), como o lock da decisão 25.
				await db.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
					`${ctx.schema}:${queue}:global`,
				]);
				const { rows } = await db.query<{ n: number }>(
					`SELECT count(*)::int AS n FROM ${runs} WHERE queue = $1 AND status = 'running'`,
					[queue]
				);
				const free = Math.min(limit, globalConcurrency - (rows[0]?.n ?? 0));
				return free > 0 ? claim(db, queue, free) : [];
			});
		},
		async nextDueAt(queue: string): Promise<Date | null> {
			const { rows } = await direct(exec).query<{ next: Date | null }>(
				`SELECT min(run_after) AS next FROM ${runs}
				 WHERE queue = $1 AND status IN ('queued', 'waiting') AND run_after > now()`,
				[queue]
			);
			return rows[0]?.next ?? null;
		},
	};
}
```

(`byRunAfterCreatedId` compara `runAfter`, `createdAt`, `id`; `runAfter` do registro reivindicado
é o valor anterior ao claim, que o `UPDATE` não muda.)

- [ ] **Step 5: Implementar `system-jobs.ts` e `schema-meta.ts`**

Pontos obrigatórios (os três primeiros eram bugs da versão anterior do plano):

- **Todo parâmetro enviado é referenciado no SQL** (o `pg` manda parâmetros sem tipo; um não
  referenciado dá "could not determine data type of parameter $n").
- **`fail` não modifica a mesma linha duas vezes no mesmo statement** (um `DELETE` num CTE sobre
  uma linha que outro CTE atualizou é ignorado em silêncio). Os dois CTEs têm predicados
  disjuntos sobre o mesmo snapshot:

```sql
WITH gone AS (
  DELETE FROM system_jobs
  WHERE job_id = $1 AND locked_by = $2 AND attempts + 1 >= max_attempts RETURNING 1),
bumped AS (
  UPDATE system_jobs
  SET attempts = attempts + 1, run_after = now() + ($3::float8 * interval '1 millisecond'),
      locked_until = NULL, locked_by = NULL
  WHERE job_id = $1 AND locked_by = $2 AND attempts + 1 < max_attempts RETURNING 1)
SELECT (SELECT count(*) FROM gone)::int AS gone, (SELECT count(*) FROM bumped)::int AS bumped
```

  → `gone` = `"exhausted"`, `bumped` = `"retried"`, nenhum = `"lost"`.
- **`setIfAbsent` relê num statement novo** quando o `INSERT ... ON CONFLICT DO NOTHING
  RETURNING value` não devolveu linha (o `SELECT` no mesmo statement usaria o snapshot de antes
  da transação concorrente comitar).
- `upsert`: `INSERT ... VALUES ($1, $2, $3::jsonb, now() + ($4::float8 * interval '1 millisecond'), $5) ON CONFLICT (job_id) DO NOTHING`.
- `upsertScheduleTick`:

```sql
INSERT INTO system_jobs (job_id, name, data, run_after, max_attempts)
VALUES ($1, 'schedule-tick', $2::jsonb, $3::timestamptz, 5)
ON CONFLICT (job_id) DO UPDATE
  SET data = EXCLUDED.data, run_after = EXCLUDED.run_after, attempts = 0,
      locked_until = NULL, locked_by = NULL
  WHERE system_jobs.data->>'cron' IS DISTINCT FROM EXCLUDED.data->>'cron'
     OR system_jobs.data->>'timezone' IS DISTINCT FROM EXCLUDED.data->>'timezone'
RETURNING (xmax = 0) AS inserted
```

  → linha com `inserted` = `"created"`, linha sem = `"updated"`, nenhuma linha = `"unchanged"`.
- `claim`: `WITH picked AS (SELECT job_id ... WHERE run_after <= now() AND (locked_until IS NULL OR locked_until <= now()) ORDER BY run_after LIMIT $1 FOR UPDATE SKIP LOCKED) UPDATE ... j SET locked_until = now() + ($2 ms), locked_by = gen_random_uuid() FROM picked WHERE j.job_id = picked.job_id RETURNING j.*`.
- `complete`/`delay`/`rescheduleTick`: `WHERE job_id = $1 AND locked_by = $2`, devolvem
  `rowCount > 0`. `delay` e `rescheduleTick` soltam o lock (`locked_until = NULL, locked_by =
  NULL`); `rescheduleTick` grava `run_after = $3`, `attempts = 0`, `data = jsonb_set(data,
  '{prevMillis}', to_jsonb($4::float8))`.
- `remove`: sem `evenIfLocked`, `AND (locked_until IS NULL OR locked_until <= now())`, espelhando
  "remoção nunca tira job ativo" do BullMQ.
- `now()`: `SELECT now() AS now`.

Em `createSqlStore`, acrescentar `claim: createRunClaimStore(poolExec, ctx)`, `systemJobs`, `meta`
(não é necessário no `StoreTx`).

- [ ] **Step 6: Rodar e ver passar; documentar o contrato**

Run: `pnpm typecheck && pnpm lint && pnpm vitest run --project integration test/integration/store-claim.test.ts test/integration/migrate.test.ts`
Expected: PASS.

Em `docs/store-contract.md`, adicionar a seção "Transporte Postgres (migration 009)" com as
interfaces acima, a nota do invariante `waiting` + `run_after`, o fencing por `locked_by` e a
ordem de locks do claim com limite global (advisory lock da fila → locks de linha `SKIP LOCKED`;
nunca espera lock de linha, então não entra em ciclo com a ordem pai → run → descendentes).

- [ ] **Step 7: Commit**

```bash
git add src/migrations src/store test/integration/store-claim.test.ts test/integration/migrate.test.ts docs/store-contract.md
git commit -m "feat(store): migration 009, run claim, system_jobs and schema_meta stores"
```

---

### Task 4: `createPgListener` — client LISTEN dedicado, multi-canal; outbox passa a usá-lo

**Files:**
- Create: `src/pg/listener.ts`
- Modify: `src/outbox.ts` (mantém `outboxChannel`, `createOutboxListener` vira um adaptador fino)
- Test: `test/unit/pg-listener.test.ts`, `test/unit/outbox.test.ts` (continua passando)

**Interfaces:**
- Produces:

```ts
export interface PgListenerOptions {
	clientConfig: ClientConfig;            // passa por leaderClientConfig()
	channels: readonly string[];           // nomes já validados (<= 63 bytes)
	logger: EngineLogger;
	onNotification(channel: string, payload: string): void;   // nunca lança (é embrulhado)
	onConnected?(): void;                  // a cada (re)conexão, depois do LISTEN
	reconnectMinMs?: number; reconnectMaxMs?: number; connectTimeoutMs?: number; closeTimeoutMs?: number;
}
export interface PgListener {
	start(): Promise<void>;   // resolve na primeira tentativa (sucesso ou agendamento de reconexão)
	isListening(): boolean;
	close(): Promise<void>;   // limitado a closeTimeoutMs; idempotente
}
export function createPgListener(options: PgListenerOptions): PgListener;
export function channelName(kind: "wake" | "status" | "cancel" | "outbox", schema: string): string;
```

  `channelName("outbox", s)` devolve exatamente o que `outboxChannel(s)` devolve hoje (mantém
  compatibilidade dos canais em produção).

- [ ] **Step 1: Testes que falham**

`test/unit/pg-listener.test.ts` (fake de `pg.Client` como o de `test/unit/outbox.test.ts`, copiar
o helper `fakeClientFactory` de lá):

```ts
it("LISTENs every channel and dispatches notifications by channel", async () => {
	const { factory, clients } = fakeClientFactory();
	const seen: Array<[string, string]> = [];
	const listener = createPgListener({
		clientConfig: {}, channels: ["a", "b"], logger: silent,
		onNotification: (c, p) => seen.push([c, p]), createClient: factory,
	});
	await listener.start();
	expect(clients[0].queries).toEqual(['LISTEN "a"', 'LISTEN "b"']);
	clients[0].emit("notification", { channel: "b", payload: "x" });
	expect(seen).toEqual([["b", "x"]]);
	await listener.close();
});

it("a throwing onNotification is logged and does not kill the client", async () => { /* onNotification lança; expect(logger.warn) chamado; listener.isListening() true */ });

it("reconnects with backoff after 'end' and re-LISTENs; onConnected fires each time", async () => { /* usar vi.useFakeTimers; emitir end; avançar 500 ms; clients.length === 2; queries LISTEN de novo; onConnected 2x */ });

it("no reconnect after close(); close() is bounded", async () => { /* end() que nunca resolve; close() resolve em closeTimeoutMs */ });

it("channelName matches the outbox channel and hashes long schemas", () => {
	expect(channelName("outbox", "abc")).toBe(outboxChannel("abc"));
	expect(channelName("wake", "a".repeat(60))).toMatch(/^mt_wake_h_[0-9a-f]{48}$/);
});
```

`createClient?: (config: ClientConfig) => Client` é uma opção de teste (padrão `new Client(config)`),
como o outbox já faz.

- [ ] **Step 2: Rodar e ver falhar**

Run: `pnpm vitest run test/unit/pg-listener.test.ts`
Expected: FAIL (módulo inexistente).

- [ ] **Step 3: Implementar**

Extrair de `src/outbox.ts` para `src/pg/listener.ts` o estado de conexão (`connect`, backoff
`min(reconnectMaxMs, reconnectMinMs * 2 ** attempt)` + jitter ≤ 250 ms, `client === c` nos
handlers, `'error'` sempre assinado, `connectTimeoutMs` com rejeição tardia engolida,
`close()` = `end()` limitado e `destroy()` do socket). Diferenças: lista de canais
(`LISTEN ${escapeIdentifier(ch)}` para cada), `onNotification(channel, payload)` embrulhado em
`try/catch` + `logger.warn` throttled, e sem poll/dispatch (isso é do chamador).

```ts
export function channelName(kind: "wake" | "status" | "cancel" | "outbox", schema: string): string {
	validateSqlIdentifier("schema", schema);
	const plain = `mt_${kind}_s_${schema}`;
	if (Buffer.byteLength(plain) <= 63) return plain;
	return `mt_${kind}_h_${createHash("sha256").update(schema).digest("hex").slice(0, 48)}`;
}
```

`createOutboxListener` passa a: criar um `PgListener` com `channels: [channel]`,
`onNotification: () => sweep()`, `onConnected: () => sweep()`, e manter o poll e o agrupamento de
varreduras exatamente como hoje. `outboxChannel(s)` = `channelName("outbox", s)`.

- [ ] **Step 4: Rodar tudo**

Run: `pnpm typecheck && pnpm lint && pnpm vitest run test/unit/pg-listener.test.ts test/unit/outbox.test.ts && pnpm vitest run --project integration test/integration/outbox.test.ts`
Expected: PASS (o outbox não muda de comportamento).

- [ ] **Step 5: Commit**

```bash
git add src/pg/listener.ts src/outbox.ts test/unit/pg-listener.test.ts
git commit -m "feat(pg): multi-channel LISTEN client; outbox listener built on it"
```

---

### Task 5: Pub/sub por NOTIFY (publicador agrupado, cancel relido)

**Files:**
- Create: `src/pg/notify-batcher.ts`
- Create: `src/pg/pubsub.ts`
- Test: `test/unit/pg-pubsub.test.ts`

**Interfaces:**
- Consumes: `PubSub`, `PubSubPublisher`, `RunStatusMessage` de `src/pubsub.ts` (os métodos do
  assinante são `subscribeStatus(runId, handler)` e `onCancel(handler)`); `RunStore.get`;
  `PgListener` (Task 4).
- Produces:

```ts
export interface NotifyBatcher {
	/** Enfileira; sai na próxima janela. Nunca lança; falha do envio = `warn` throttled e descarte. */
	notify(channel: string, payload: string): void;
	/** Envia o que está pendente agora (resolve depois do statement). */
	flush(): Promise<void>;
	/** flush + recusa novos; limitado a `closeTimeoutMs` (padrão 2 s). Idempotente. */
	close(): Promise<void>;
}
export function createNotifyBatcher(options: {
	db: Pick<Pool, "query">; logger: EngineLogger;
	windowMs?: number;   // padrão 5
	maxItems?: number;   // padrão 500: atingido, envia na hora
	setTimeoutFn?: typeof setTimeout;
}): NotifyBatcher;
// um statement por janela, deduplicado por (canal, payload):
//   SELECT pg_notify(c, p) FROM unnest($1::text[], $2::text[]) AS t(c, p)

export function createPgPublisher(options: { batcher: NotifyBatcher; schema: string }): PubSubPublisher;
// publishStatus({ runId }) → batcher.notify(channelName("status"), runId); ignora o resto da mensagem
// publishCancel(ids) → batcher.notify(channelName("cancel"), id) por id
export function createPgPubSub(options: {
	publisher: PubSubPublisher;
	store: Pick<Store, "runs">;
	schema: string;
	/** Runs ativas neste processo (o cancel só relê essas). */
	isActiveLocally(runId: string): boolean;
	subscribe(channels: readonly string[], onNotification: (channel: string, payload: string) => void): () => void; // registra no PgListener compartilhado
	logger: EngineLogger;
}): PubSub;
```

  O `PgListener` compartilhado da engine expõe `subscribe(channels, handler)` (Task 10b acrescenta
  esse método ao listener: um registro por consumidor; o listener já `LISTEN`a todos os canais
  desde o `start()`).

- [ ] **Step 1: Testes que falham**

```ts
it("the batcher sends one deduplicated statement per window", async () => {
	/* relógio falso; 3× notify("a","1") + notify("b","2") → nenhuma query antes de 5 ms; depois uma query com
	   valores [["a","b"],["1","2"]]; maxItems 2 → envia na hora; query que rejeita → warn, sem lançar */
});

it("publishStatus notifies only the run id; publishCancel one per id", async () => {
	const sent: Array<[string, string]> = [];
	const batcher = { notify: (c: string, p: string) => sent.push([c, p]), flush: async () => {}, close: async () => {} };
	const publisher = createPgPublisher({ batcher, schema: "s" });
	await publisher.publishStatus({ runId: "r1", status: "running", updatedAt: "x", progress: { big: "y".repeat(20_000) } });
	await publisher.publishCancel(["a", "b"]);
	expect(sent).toEqual([["mt_status_s_s", "r1"], ["mt_cancel_s_s", "a"], ["mt_cancel_s_s", "b"]]);
});

it("a status notification re-reads the run and emits the full message; invalid payloads ignored", async () => {
	/* store.runs.get fake devolve { id, status: "completed", updatedAt, progress }; pubsub.subscribeStatus("r1", h);
	   handler("mt_status_s_s", "r1") → h chamado com { runId: "r1", status: "completed", updatedAt: iso, progress };
	   handler("mt_status_s_s", "not-a-uuid") e "" → nada, sem lançar; run inexistente → nada;
	   notificação de um runId sem handler → nenhuma leitura */
});

it("a cancel notification only reaches onCancel handlers for local runs that are canceled in the database", async () => {
	/* isActiveLocally: r1 e r2 sim, r3 não; store: r1 canceled, r2 running.
	   handler cancel r1, r2, r3 → onCancel recebe só ["r1"]; r3 nem é lido; r2 (forjado) é ignorado */
});

it("ready resolves immediately (the shared listener is already up)", async () => { /* ... */ });
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `pnpm vitest run test/unit/pg-pubsub.test.ts` → FAIL.

- [ ] **Step 3: Implementar**

`createPgPublisher` só enfileira no batcher (as promessas resolvem na hora: pub/sub é
at-most-once). `createPgPubSub`: `subscribeStatus(runId, handler)`/`onCancel(handler)` com o mesmo
formato de `Map`/`Set` de `src/pubsub.ts`. Status: payload UUID com handlers registrados →
`store.runs.get(runId)` → `RunStatusMessage { runId, status, updatedAt: run.updatedAt.toISOString(), progress: run.progress ?? undefined }`. Cancel: payload UUID e `isActiveLocally(runId)` → agrupar
os ids recebidos na mesma volta do event loop, uma leitura (`store.runs.get` por id, ou uma query
por lista) → repassar aos handlers só os `canceled`. Erros de leitura → `logger.warn` throttled.
`ready = Promise.resolve()`. `close()` chama o unsubscribe devolvido por `subscribe` (o batcher é
fechado pelo backend, não aqui).

- [ ] **Step 4: Rodar, commitar**

Run: `pnpm typecheck && pnpm lint && pnpm vitest run test/unit/pg-pubsub.test.ts` → PASS.

```bash
git add src/pg/notify-batcher.ts src/pg/pubsub.ts test/unit/pg-pubsub.test.ts
git commit -m "feat(pg): batched NOTIFY publisher; status and cancel pub/sub with re-read"
```

---

### Task 6: `cron-parser` — próxima ocorrência e validação real de cron nos dois modos

**Files:**
- Modify: `package.json` (`dependencies: { "cron-parser": "^5.10.1" }`), `pnpm-lock.yaml`
- Create: `src/pg/schedulers.ts`
- Modify: `src/schedules.ts` (`validateCronPattern` delega ao `cron-parser`)
- Test: `test/unit/pg-schedulers.test.ts`, `test/unit/schedules.test.ts`

**Interfaces:**
- Produces:

```ts
export function nextOccurrence(pattern: string, timezone: string | undefined, after: Date): Date; // lança ConfigError se inválido
export function scheduleTickJobId(scheduleId: string): string; // `schedule-tick:${scheduleId}`
export function scheduleIdOfJob(jobId: string): string | undefined;
```

- [ ] **Step 1: Instalar a dependência**

Run: `pnpm add cron-parser@^5.10.1`
Expected: `package.json` com `dependencies.cron-parser`; `pnpm ls cron-parser` mostra 5.10.1 (a
mesma cópia que o BullMQ resolve).

- [ ] **Step 2: Testes que falham**

```ts
import { nextOccurrence, scheduleTickJobId, scheduleIdOfJob } from "../../src/pg/schedulers.js";

it("computes the next occurrence after a given instant in a timezone", () => {
	const after = new Date("2026-03-08T06:30:00Z"); // 01:30 America/New_York, dia da mudança para DST
	const next = nextOccurrence("0 3 * * *", "America/New_York", after);
	expect(next.toISOString()).toBe("2026-03-08T07:00:00.000Z"); // 03:00 EDT
});
it("is strictly after `after`", () => {
	const after = new Date("2026-01-01T00:00:00Z");
	expect(nextOccurrence("* * * * *", undefined, after).getTime()).toBe(after.getTime() + 60_000);
});
it("rejects invalid patterns with ConfigError", () => {
	expect(() => nextOccurrence("99 * * * *", undefined, new Date())).toThrow(ConfigError);
	expect(() => nextOccurrence("* * * * *", "Not/AZone", new Date())).toThrow(ConfigError);
});
it("job id round-trips", () => {
	expect(scheduleIdOfJob(scheduleTickJobId("daily"))).toBe("daily");
	expect(scheduleIdOfJob("wp-x-expire")).toBeUndefined();
});
```

Em `test/unit/schedules.test.ts`, adicionar: `schedules.upsert({ cron: "99 * * * *" })` lança
`ConfigError` já na validação (antes, a regex deixava passar e só o BullMQ reclamava).

- [ ] **Step 3: Implementar**

```ts
import { CronExpressionParser } from "cron-parser";
export function nextOccurrence(pattern: string, timezone: string | undefined, after: Date): Date {
	try {
		const expr = CronExpressionParser.parse(pattern, { currentDate: after, ...(timezone ? { tz: timezone } : {}) });
		return expr.next().toDate();
	} catch (error) {
		throw new ConfigError(`invalid cron pattern "${pattern}"${timezone ? ` (timezone ${timezone})` : ""}: ${error instanceof Error ? error.message : String(error)}`);
	}
}
```

Em `src/schedules.ts`, `validateCronPattern` mantém a checagem de tipo/forma e, em seguida, chama
`nextOccurrence(cron, undefined, new Date())` só para validar (descarta o resultado). O
`tz` continua validado por `Intl` antes.

- [ ] **Step 4: Rodar, commitar**

Run: `pnpm typecheck && pnpm lint && pnpm vitest run test/unit/pg-schedulers.test.ts test/unit/schedules.test.ts` → PASS.

```bash
git add package.json pnpm-lock.yaml src/pg/schedulers.ts src/schedules.ts test/unit
git commit -m "feat(schedules): cron-parser as a direct dependency; next occurrence for the Postgres transport"
```

---

### Task 7: Laços de worker — claim adaptativo por fila e laço da `-system`

**Files:**
- Create: `src/pg/workers.ts`
- Modify: `src/executor/system.ts` (`SystemJobDelayed` no lugar de `DelayedError`)
- Modify: `src/queue/transport.ts` (converte `SystemJobDelayed` em `DelayedError` do BullMQ)
- Modify: `src/testing/test-engine.ts` (deixa de importar `bullmq` em runtime: trata `SystemJobDelayed`)
- Test: `test/unit/pg-workers.test.ts`, `test/unit/system.test.ts`, `test/unit/test-engine.test.ts`

**Interfaces:**
- Consumes: `RunClaimStore`, `SystemJobStore` (Task 3); `nextOccurrence` (Task 6); `ProcessSystemFn` (`src/queue/transport.ts`).
- Produces:

```ts
export interface QueueWorkerOptions {
	queue: string;
	concurrency: number;                        // mutável via setConcurrency
	globalConcurrency?: number;
	claim: RunClaimStore;
	process(run: RunRecord): Promise<void>;     // executa (Task 9)
	/** Com `globalConcurrency`: chamado ao fim de cada execução (a vaga liberada acorda outros processos). */
	onSlotFreed?(): void;
	polling: { intervalMs: number; jitterMs: number };
	logger: EngineLogger;
	now?: () => number; setTimeoutFn?: typeof setTimeout; random?: () => number; // injeção para testes
}
export interface QueueWorker {
	start(): void;
	wake(): void;                    // NOTIFY ou poll externo; colapsa chamadas repetidas
	setConcurrency(n: number): void;
	get concurrency(): number;
	get inFlight(): number;
	stopClaiming(): void;            // nada mais é reivindicado
	/** Espera o claim em andamento (e o lançamento do que ele trouxe), depois as execuções, até o timeout. */
	drain(timeoutMs: number): Promise<RunRecord[]>; // as runs ainda em voo depois do timeout
}
export function createQueueWorker(options: QueueWorkerOptions): QueueWorker;

export interface SystemWorkerOptions {
	concurrency: number;
	systemJobs: SystemJobStore;
	lockMs: number;                             // stallTimeoutMs
	process: ProcessSystemFn;                   // recebe o job fake (abaixo)
	backoffMs(attempt: number): number;         // exponencial 1 s * 2^attempt, máx 60 s
	polling: { intervalMs: number; jitterMs: number };
	logger: EngineLogger;
	now?: () => number; setTimeoutFn?: typeof setTimeout; random?: () => number;
}
export function createSystemWorker(options: SystemWorkerOptions): QueueWorker;
export interface PgSystemJob {  // o que o dispatcher recebe como `Job`
	id: string; name: string; data: unknown; opts: { prevMillis?: number; repeatJobKey?: string };
	timestamp: number; token: string; repeatJobKey?: string; attemptsMade: number;
	moveToDelayed(timestamp: number, token?: string): Promise<void>;
}
```

- [ ] **Step 1: Testes que falham** (relógio falso com `vi.useFakeTimers()`; `claim` fake que
devolve o que o teste enfileira)

```ts
it("claims up to the free slots and runs them concurrently", async () => { /* concurrency 2, 3 runs enfileiradas → claim(limit 2) → depois de uma terminar, claim(limit 1) */ });
it("after an empty claim sleeps until nextDueAt (bounded by intervalMs) plus jitter", async () => {
	/* nextDueAt = now + 300 ms, intervalMs 1000, jitter 0 → próximo claim em 300 ms, não em 1000 */
	/* nextDueAt = now + 5000 → próximo claim em 1000 */
	/* nextDueAt = null → 1000 */
});
it("wake() during a claim schedules exactly one extra pass", async () => { /* 5 wake() durante claim em andamento → 1 claim extra */ });
it("wake() with no free slot does not query", async () => { /* concurrency 1 ocupada; wake() → claimRuns não chamado */ });
it("a forged/unknown wake never throws or stops the loop", async () => { /* wake() com claim que rejeita uma vez → backoff; próximo wake claima normal (Review Focus 1, parte do worker) */ });
it("claim errors back off (500 ms, 1 s, ... 30 s) and warn once per 10 s", async () => { /* ... */ });
it("stopClaiming() then drain() waits in-flight runs and returns the stragglers after timeout", async () => { /* ... */ });
it("drain() waits for a claim that was in flight when stopClaiming() ran and includes its runs", async () => {
	/* claim fake pendente; stopClaiming(); drain(1000) ainda não resolve; claim resolve com [r1];
	   process(r1) é chamado (o executor devolve pela checagem de isShuttingDown); drain espera r1; nenhum claim novo (Review Focus 4) */
});
it("globalConcurrency is passed to claimRuns and onSlotFreed fires when a run ends", async () => { /* ... */ });

describe("system worker", () => {
	it("delivers a job object with id/name/data/opts.prevMillis/repeatJobKey/token and completes on success", async () => { /* token = lockedBy; complete(jobId, lockedBy) */ });
	it("SystemJobDelayed after moveToDelayed re-delays in place; other errors fail with backoff; exhausted logs error", async () => { /* ... */ });
	it("schedule-tick: success and exhaustion both reschedule to the next occurrence, never complete/delete", async () => {
		/* cron "* * * * *", now() do banco fake; sucesso → rescheduleTick(jobId, token, { runAt: próxima, prevMillis: runAt.getTime() });
		   falha com attempts + 1 >= maxAttempts → logger.error + rescheduleTick; falha abaixo → fail(jobId, token, backoff) */
	});
	it("a fenced write that does not match (lock lost) is logged at debug and ignored", async () => { /* complete → false */ });
});
```

- [ ] **Step 2: Rodar e ver falhar** — `pnpm vitest run test/unit/pg-workers.test.ts` → FAIL.

- [ ] **Step 3: Implementar `createQueueWorker`**

Esqueleto (o laço é um único `pass()` reentrante, nunca duas passagens concorrentes; a promise da
passagem em andamento fica em `current` para o `drain()` esperar):

```ts
export function createQueueWorker(o: QueueWorkerOptions): QueueWorker {
	const now = o.now ?? Date.now, setT = o.setTimeoutFn ?? setTimeout, rnd = o.random ?? Math.random;
	let concurrency = o.concurrency, inFlight = 0, again = false;
	let stopped = false, timer: ReturnType<typeof setTimeout> | undefined, failures = 0;
	let current: Promise<void> | undefined;
	const active = new Map<string, { run: RunRecord; done: Promise<void> }>();
	let lastWarnAt = 0;

	function pass(): void {
		if (current) { again = true; return; }
		current = loop().finally(() => {
			current = undefined;
			if (again && !stopped) pass();
		});
	}
	async function loop(): Promise<void> {
		try {
			while (!stopped) {
				again = false;
				const free = concurrency - inFlight;
				if (free <= 0) return;                      // o fim de uma execução chama pass()
				const rows = await o.claim.claimRuns({ queue: o.queue, limit: free, globalConcurrency: o.globalConcurrency });
				failures = 0;
				for (const run of rows) launch(run);        // mesmo depois de stopClaiming: o executor devolve
				if (stopped) return;
				if (rows.length < free) {
					const next = await o.claim.nextDueAt(o.queue);
					const delay = next ? Math.max(0, Math.min(o.polling.intervalMs, next.getTime() - now())) : o.polling.intervalMs;
					schedule(delay);
					if (!again) return;
				}
			}
		} catch (error) {
			failures++;
			const backoff = Math.min(30_000, 500 * 2 ** Math.min(failures - 1, 6));
			if (now() - lastWarnAt > 10_000) { lastWarnAt = now(); o.logger.warn({ queue: o.queue, err: error, backoffMs: backoff }, "claim failed; backing off"); }
			schedule(backoff);
		}
	}
	function launch(run: RunRecord): void {
		inFlight++;
		const done = o.process(run)
			.catch(err => o.logger.error({ runId: run.id, err }, "run processing failed unexpectedly"))
			.finally(() => {
				inFlight--; active.delete(run.id);
				o.onSlotFreed?.();
				if (!stopped) pass();
			});
		active.set(run.id, { run, done });
	}
	function schedule(ms: number): void {
		if (stopped) return;
		clearTimeout(timer);
		const jitter = o.polling.jitterMs > 0 ? Math.floor(rnd() * o.polling.jitterMs) : 0;
		timer = setT(() => pass(), ms + jitter); timer.unref?.();
	}
	return {
		start() { pass(); },
		wake() { if (!stopped && concurrency - inFlight > 0) { clearTimeout(timer); pass(); } },
		setConcurrency(n) { concurrency = n; pass(); },
		get concurrency() { return concurrency; },
		get inFlight() { return inFlight; },
		stopClaiming() { stopped = true; clearTimeout(timer); },
		async drain(timeoutMs) {
			const deadline = now() + timeoutMs;
			const bounded = <T>(p: Promise<T>) => Promise.race([p, new Promise<void>(r => setT(r, Math.max(0, deadline - now())))]);
			if (current) await bounded(current);       // o claim em voo e o lançamento do que ele trouxe
			await bounded(Promise.allSettled([...active.values()].map(a => a.done)));
			return [...active.values()].map(a => a.run);
		},
	};
}
```

`createSystemWorker` reaproveita a mesma estrutura sobre `systemJobs.claim(free, lockMs)` e
`systemJobs.nextDueAt()`; `process` recebe o `PgSystemJob`:

```ts
const token = rec.lockedBy!;
let delayed = false;
const job: PgSystemJob = {
	id: rec.jobId, name: rec.name, data: rec.data, timestamp: rec.runAfter.getTime(), token,
	attemptsMade: rec.attempts,
	repeatJobKey: rec.name === "schedule-tick" ? scheduleIdOfJob(rec.jobId) : undefined,
	opts: { prevMillis: (rec.data as { prevMillis?: number })?.prevMillis, repeatJobKey: /* idem */ },
	async moveToDelayed(ts) { delayed = await o.systemJobs.delay(rec.jobId, token, Math.max(1, ts - now())); },
};
const isTick = rec.name === "schedule-tick";
const fenced = (ok: boolean) => { if (!ok) o.logger.debug({ jobId: rec.jobId }, "system job lock lost; result discarded"); };
try {
	await o.process(rec.name, rec.data, job as unknown as Job);
	fenced(isTick ? await rescheduleTick(rec, token) : await o.systemJobs.complete(rec.jobId, token));
} catch (error) {
	if (error instanceof SystemJobDelayed || delayed) return;          // já reagendado
	const exhausted = rec.attempts + 1 >= rec.maxAttempts;
	if (isTick && exhausted) {
		o.logger.error({ jobId: rec.jobId, err: error }, "schedule tick failed permanently; moving to the next occurrence");
		fenced(await rescheduleTick(rec, token));
		return;
	}
	const outcome = await o.systemJobs.fail(rec.jobId, token, o.backoffMs(rec.attempts));
	if (outcome === "lost") return fenced(false);
	o.logger[outcome === "exhausted" ? "error" : "warn"]({ jobId: rec.jobId, name: rec.name, err: error }, outcome === "exhausted" ? "system job failed permanently" : "system job failed; will retry");
}
```

`rescheduleTick(rec, token)`: `dbNow = await o.systemJobs.now()`; `after = max(dbNow, prevMillis)`;
`runAt = nextOccurrence(data.cron, data.timezone, after)`; `systemJobs.rescheduleTick(rec.jobId,
token, { runAt, prevMillis: runAt.getTime() })`. Um cron que ficou inválido na linha (não deveria:
é validado antes de gravar) → `logger.error` e `remove(jobId, { evenIfLocked: true })`; o
reconciler recria a partir de `schedules`.

`DelayedError` vem de `bullmq`, que não pode ser importado aqui: definir em `src/executor/system.ts`
um `export class SystemJobDelayed extends Error {}` e fazer o dispatcher lançar `SystemJobDelayed`
(o worker BullMQ em `src/queue/transport.ts` converte `SystemJobDelayed` em `DelayedError` do
BullMQ ao repassar). Assim `system.ts` deixa de importar `bullmq` em runtime (só `import type { Job }`).
Em `src/testing/test-engine.ts`, trocar `error instanceof DelayedError` por `error instanceof
SystemJobDelayed` e o import por `import type { Job } from "bullmq"`.

- [ ] **Step 4: Rodar, commitar**

Run: `pnpm typecheck && pnpm lint && pnpm vitest run test/unit/pg-workers.test.ts test/unit/system.test.ts test/unit/test-engine.test.ts` → PASS.
Conferir: `grep -n 'from "bullmq"' src/executor/system.ts src/testing/*.ts` só mostra `import type`.

```bash
git add src/pg/workers.ts src/executor/system.ts src/queue/transport.ts src/testing/test-engine.ts test/unit
git commit -m "feat(pg): adaptive claim loop per queue and fenced system_jobs worker"
```

---

### Task 8: `Transport` sobre Postgres

**Files:**
- Create: `src/pg/transport.ts`
- Test: `test/unit/pg-transport.test.ts`

**Interfaces:**
- Consumes: `SqlStore` (Task 3), `NotifyBatcher` (Task 5), `createQueueWorker`/`createSystemWorker`
  (Task 7), `nextOccurrence`/`scheduleTickJobId` (Task 6). O transporte recebe `wake: {
  subscribe(handler: (queue: string | undefined) => void): () => void }` (ligado ao listener na
  Task 10b).
- Produces:

```ts
export interface PgTransportOptions {
	store: SqlStore;
	batcher: NotifyBatcher;
	schema: string;
	queues: ResolvedQueueConfig[];
	systemConcurrency: number;
	stallTimeoutMs: number;
	polling: { intervalMs: number; jitterMs: number };
	wake: { subscribe(handler: (queue: string | undefined) => void): () => void };
	logger: EngineLogger;
}
export function createPgTransport(options: PgTransportOptions): Transport;
```

  Mapeamento dos métodos de `Transport`:
  - `enqueueRun({ queue })` → `batcher.notify(wake, queue)` **com ou sem atraso** (o `run_after`
    já foi gravado pela store; com atraso, o `NOTIFY` faz os workers da fila recalcularem
    `min(run_after)` em vez de esperarem o próximo poll). Nunca lança.
  - `removeRunJob` → no-op (a run é a linha).
  - `enqueueSystem(name, data, { jobId, delayMs })` → `systemJobs.upsert({ jobId: jobId ?? randomUUID(), name, data, delayMs, maxAttempts: 1 })` + wake local do worker `-system`.
  - `removeSystemJob(jobId)` → `systemJobs.remove(jobId)` (não tira job travado).
  - `upsertJobScheduler({ id, pattern, timezone })` → `dbNow = systemJobs.now()`; `runAt = nextOccurrence(pattern, timezone, dbNow)`; `systemJobs.upsertScheduleTick({ jobId: scheduleTickJobId(id), data: { scheduleId: id, cron: pattern, timezone, schema, prevMillis: runAt.getTime() }, runAt })` (no-op para o tick pendente quando nada mudou).
  - `getJobScheduler(id)` → lê o job e devolve `{ id, pattern: data.cron, timezone: data.timezone, schema }`; `listJobSchedulers` → `listByName("schedule-tick")`; `removeJobScheduler(id)` → `remove(scheduleTickJobId(id), { evenIfLocked: true })`.
  - `startWorkers({ queues, processRun, processSystem })`: cria um `QueueWorker` por fila (filtrada por `queues`) com `process: run => processRun(queue, { runId: run.id, seq: run.enqueueSeq, claimed: run }, fakeJob)` (Task 9) e, se a fila tem `globalConcurrency`, `onSlotFreed: () => batcher.notify(wake, queue)`; e o `SystemWorker`. Assina `wake.subscribe(q => q !== undefined && workers.has(q) ? workers.get(q)!.wake() : wakeAll())`; recusa `rateLimit` com `ConfigError` (já barrado no config, mas defensivo).
  - `closeWorkers({ timeoutMs, beforeClose })`: `stopClaiming()` em todos → `drain(timeoutMs)` (espera claims em voo) → `beforeClose(stillActive)` → devolve `{ stillActive }` no mesmo formato (`{ queue, runId }`).
  - `checkRedis()` → `undefined`.
  - `pauseQueue`/`resumeQueue` → no-op (o claim lê `queue_state`); `isQueuePaused(q)` → `store.queues.get(q)?.paused ?? false`.
  - `workerConcurrency(q)` / `setWorkerConcurrency(q, n)` → do `QueueWorker`.

- [ ] **Step 1: Testes que falham** (store fake em memória mínimo para `systemJobs`/`claim`; batcher fake)

```ts
it("enqueueRun notifies the wake channel with the queue name, delayed or not", async () => { /* delayMs undefined e 5000 → dois notify("mt_wake_s_s", "q1") */ });
it("upsertJobScheduler stores the next occurrence and getJobScheduler reads it back", async () => { /* ... */ });
it("re-upserting the same scheduler does not move the pending tick", async () => { /* upsertScheduleTick devolve "unchanged" */ });
it("startWorkers creates one worker per selected queue and wakes by queue name or all", async () => { /* payload desconhecido/vazio → todas */ });
it("closeWorkers stops claiming, drains and reports still-active runs to beforeClose", async () => { /* ... */ });
it("rateLimit queues are refused", () => { /* ... */ });
```

- [ ] **Step 2: Rodar e ver falhar** → FAIL.

- [ ] **Step 3: Implementar** conforme o mapeamento acima. O `fakeJob` passado a `processRun` é
`{ id: `${run.id}-${run.enqueueSeq}`, data }` (só para logs; o executor não lê mais nada dele).

- [ ] **Step 4: Rodar, commitar**

Run: `pnpm typecheck && pnpm lint && pnpm vitest run test/unit/pg-transport.test.ts` → PASS.

```bash
git add src/pg/transport.ts test/unit/pg-transport.test.ts
git commit -m "feat(pg): Transport implementation over runs, system_jobs and NOTIFY"
```

---

### Task 9: Executor aceita runs já reivindicadas (`RunJob.claimed`)

**Files:**
- Modify: `src/executor/execute-run.ts` (linhas ~59–130)
- Modify: `src/queue/transport.ts` (`RunJobData` ganha `claimed?: RunRecord` só no caminho interno; o BullMQ nunca o serializa) e o `processRun` da engine (`src/engine.ts` ~1921) repassa `claimed`
- Modify: `src/store/types.ts` + `src/store/sql/runs.ts` + `src/store/memory/runs.ts`:
  `RunStore.recheckConcurrencyLimit(runId, executionId, { limit?: number })` → `{ kind: "ok"; run: RunRecord } | { kind: "limited" } | { kind: "lost" }`
- Test: `test/integration/execute-claimed.test.ts`, `test/helpers/store-contract-debounce-suite.ts`

**Interfaces:**
- Produces: `RunJob { runId; seq; queue?; claimed?: RunRecord }`; `executeRun` com `claimed` pula
  `acquire`; `recheckConcurrencyLimit` (SQL, dentro de `atomic`):
  1. `SELECT queue, concurrency_key, concurrency_waits FROM runs WHERE ${OWNED} FOR UPDATE`
     (nada → `lost`).
  2. Se `limit` definido e `concurrency_key` não nulo: `pg_advisory_xact_lock(hashtextextended($schema:$queue:$key, 0))`
     (mesma chave de `acquireWithConcurrencyLimit`) e conta `running` da mesma chave **excluindo a
     própria run**. Se `>= limit` → `UPDATE runs SET status = 'queued', execution_id = NULL,
     run_after = now() + concurrencyLimitedDelayMs(concurrency_waits), concurrency_waits =
     concurrency_waits + 1, updated_at = now()` (backoff do valor **pré-incremento**, como hoje;
     `enqueue_seq` e `started_at` intocados) → `limited`.
  3. Senão → `UPDATE runs SET concurrency_waits = 0, started_at = coalesce(started_at, now())
     WHERE id = $1 RETURNING ${RUN_COLUMNS}` → `ok` com o registro atualizado.

- [ ] **Step 1: Testes que falham**

Contrato (nas duas stores, em `store-contract-debounce-suite.ts`):
- duas runs `running` da mesma chave e `limit: 1` → a segunda devolve `limited`, fica `queued`
  com `execution_id NULL`, `concurrency_waits = 1`, `run_after` ≈ now + 1 s;
- **backoff crescente através do claim** (Review Focus 7): repetir claim → recheck `limited` três
  vezes na mesma run; `concurrency_waits` vai a 3 e o atraso do terceiro é ≥ 4 s (o claim não
  zera o contador);
- `limit: 2` → `ok`, `concurrency_waits = 0`, `startedAt` preenchido;
- `limit` ausente numa run com chave → `ok` e `startedAt` preenchido;
- execução que não é mais dona → `lost`.

`test/integration/execute-claimed.test.ts`: monta `ExecutorDeps` reais (store SQL, transporte
Postgres da Task 8 com `startWorkers` não chamado, registry com uma task de 2 steps), cria a run
com `store.runs.create`, reivindica com `store.claim.claimRuns`, chama
`executeRun(deps, { runId, seq: run.enqueueSeq, queue, claimed: run })` e espera `completed` com
o output; segundo caso: `deps.isShuttingDown = () => true` → a run volta a `queued` (hand-back);
terceiro: fila com `concurrencyKey { limit: 1 }` e outra run `running` da chave → a run reivindicada
volta a `queued` sem gastar tentativa e sem evento `run:started`.

- [ ] **Step 2: Rodar e ver falhar** → FAIL (`claimed` desconhecido).

- [ ] **Step 3: Implementar**

Em `executeRun`, substituir o bloco de aquisição por:

```ts
let run: RunRecord | null;
let executionId: string;
try {
	const limit = job.queue === undefined ? undefined : deps.registry.queues.get(job.queue)?.concurrencyKey?.limit;
	if (job.claimed) {
		run = job.claimed; executionId = job.claimed.executionId!;
		if (run.concurrencyKey !== null) {
			const check = await deps.store.runs.recheckConcurrencyLimit(run.id, executionId, limit === undefined ? {} : { limit });
			if (check.kind === "limited") { log.debug({ runId: run.id }, "concurrency key at its limit; run requeued with backoff"); return; }
			run = check.kind === "ok" ? check.run : null;
		}
	} else {
		executionId = randomUUID();
		/* bloco atual de acquire / acquireWithConcurrencyLimit, inalterado */
	}
} catch (error) { /* inalterado */ }
```

O resto (`acquiredAtMonotonic`, `isShuttingDown` → `handBackLateAcquire`, `ExecutionState`,
`runAcquired`) não muda. `RunRecord.executionId` já existe em `RUN_COLUMNS`. No modo Postgres o
`limited` não chama `enqueue`: o poll da fila encontra a run no novo `run_after`.

- [ ] **Step 4: Rodar, commitar**

Run: `pnpm typecheck && pnpm lint && pnpm vitest run --project integration test/integration/execute-claimed.test.ts test/integration/store-contract-sql.test.ts && pnpm vitest run test/unit/memory-store.test.ts` → PASS.

```bash
git add src/executor/execute-run.ts src/queue/transport.ts src/engine.ts src/store test/integration/execute-claimed.test.ts test/helpers
git commit -m "feat(executor): run already-claimed jobs; concurrency key re-check after claim"
```

---

### Task 10a: Extrair o backend BullMQ (refactor puro)

**Files:**
- Create: `src/backends/types.ts`
- Create: `src/backends/bullmq.ts` (extraído de `src/engine.ts`: pool Redis, `createTransport`,
  streams, pub/sub Redis, `claimPrefix`, `assertRedisReachable`, checagens do Redis, subscriber
  pronto, listener do outbox)
- Modify: `src/engine.ts`

**Interfaces:**
- Produces (`src/backends/types.ts`):

```ts
export interface BackendRuntime {
	backend: EngineBackend;                 // para createEngineCore
	transport: Transport;
	/** Roda no start(), antes dos workers; pode lançar (start() falha e é reiniciável). */
	beforeStart(signal: { abandoned(): boolean }): Promise<void>;
	/** Depois dos workers (listener do outbox no BullMQ; autoteste de NOTIFY no Postgres). */
	afterWorkersStarted(): Promise<void>;
	/** Passo do beforeClose do stop() (fecha listener/outbox/publicador), limitado. */
	beforeClose(): Promise<void>;
	/** Último passo do stop(): conexões próprias. */
	close(): Promise<void>;
	dedicatedClientConfig(): ClientConfig;  // líder
	warnPoolSizing(): void;
}
export interface BackendContext {
	resolved: ResolvedConfig; registry: Registry; pool: Pool; store: SqlStore;
	logger: EngineLogger; isShuttingDown(): boolean;
	/** Runs ativas neste processo (`core.active`), para o cancel relido do pub/sub Postgres. */
	isActiveLocally(runId: string): boolean;
}
export function createBullmqBackend(ctx: BackendContext): BackendRuntime;
```

- [ ] **Step 1: Mover sem mudar comportamento**

Mover para `createBullmqBackend` tudo o que é específico de Redis/BullMQ em `createEngine`,
mantendo a ordem exata dos passos de `start()`/`stop()` (incluindo `assertStartNotAbandoned`
entre passos, os timeouts de 5 s e os tipos de erro da terceira e quarta revisões externas).
Nesta tarefa os imports continuam estáticos; `createEngine` chama `createBullmqBackend(ctx)`
diretamente. Nenhum teste muda.

- [ ] **Step 2: Rodar tudo**

Run: `pnpm typecheck && pnpm lint && pnpm test`
Expected: PASS, suíte inteira, sem nenhum teste alterado.

- [ ] **Step 3: Commit**

```bash
git add src/backends src/engine.ts
git commit -m "refactor(engine): extract the BullMQ backend behind BackendRuntime"
```

---

### Task 10b: Backend Postgres e ciclo de vida da engine

**Files:**
- Create: `src/backends/postgres.ts`
- Modify: `src/engine.ts` (`createEngine` escolhe o backend pelo `resolved.transport`)
- Modify: `src/pg/listener.ts` (`subscribe(channels, handler)` no listener compartilhado)
- Modify: `src/store/sql/runs.ts`, `src/store/types.ts` (`NewRun.outbox.payload?`/`pending?`)
- Modify: `src/env-check.ts` (`redis?` opcional; sem Redis, a checagem de eviction é pulada)
- Modify: `src/reconciler.ts` (`recoverLostEnqueues?: boolean`, padrão `true`; `false` pula casos 1/2 e reporta `skippedLostEnqueues: true`)
- Test: `test/integration/pg-engine.test.ts`, `test/unit/reconciler.test.ts` (caso novo), `test/unit/env-check.test.ts`

- [ ] **Step 1: Testes que falham**

`test/integration/pg-engine.test.ts` (mesmo `makeEngine` de `engine.test.ts`, mas
`transport: "postgres"`, sem `redis`/`prefix`):

```ts
it("runs a task end to end without Redis (trigger, steps, sleep, token, child)", async () => {
	/* task com step.run, step.sleep("200ms"), createToken + waitForToken, triggerAndWait de filha;
	   engine.start(); trigger; tokens.complete via engine.tokens; waitFor completed; output correto */
});
it("trigger(..., { tx }) is claimable right after commit (outbox_pending false, wake NOTIFY in the insert)", async () => {
	/* BEGIN; trigger com tx; antes do COMMIT nenhuma run visível; COMMIT; run:started em < 300 ms; outbox_pending = false na linha */
});
it("wake latency: a trigger is picked up in < 300 ms with LISTEN, and within polling.intervalMs + jitter when NOTIFY is broken", async () => {
	/* 1) medir trigger → run:started; 2) pg_terminate_backend do listener (application_name 'minitrigger-listener') e trigger logo em seguida; started dentro de 1.2 s */
});
it("a delayed run scheduled by another engine is picked up on time, not a poll interval later", async () => {
	/* engine A (workers) com polling.intervalMs 5000; engine B só de disparo faz trigger({ delay: 300 }); started entre 300 e 600 ms */
});
it("stop() hands back a running run with a NOTIFY and a second engine picks it up within 1 s", async () => { /* Review Focus 4 */ });
it("refuses to start in bullmq mode on a schema already used in postgres mode (and vice versa)", async () => { /* schema_meta; erro é ConfigError */ });
it("LISTEN connection timeout at start() is a MinitriggerError, not a ConfigError, and the engine can start again", async () => { /* clientConfig com host inalcançável só para o listener, via connectionTimeoutMs curto */ });
it("a schedule ticks every second and survives a leader restart", async () => { /* cron "* * * * * *"; stop engine A; start B; contagem de runs continua; sem duplicata por ts (idempotency_keys) */ });
it("restarting the engine does not skip a due schedule tick", async () => { /* tick vencido e não processado; start() de outra engine (sync declarativo) → a run desse ts é criada */ });
it("schedule-tick claimed by a process that dies is reprocessed once the lock expires, without a duplicate run", async () => { /* Review Focus 3: inserir job schedule-tick com locked_until = now()+300ms, locked_by aleatório e uma run já criada com __schedule:id:ts; após 300 ms o worker processa, não cria outra run e a linha vai para a próxima ocorrência */ });
it("a schedule-tick that exhausts its attempts moves to the next occurrence instead of disappearing", async () => { /* task não registrada em nenhum processo; após 5 falhas (backoff curto via stub) a linha existe com run_after futuro e attempts 0 */ });
it("waitpoint expiry fires from system_jobs and cancels the child", async () => { /* triggerAndWait com timeout 300ms; filha que dorme 5 s */ });
it("retention runs from system_jobs", async () => { /* retention.days 0, intervalMs 500; run terminada some */ });
it("ctx.stream, runs.readStream and queue.rateLimit are ConfigError", async () => { /* ... */ });
it("reconciler skips cases 1/2 (backlog is not a lost enqueue)", async () => { /* 5 runs queued com run_after vencido e workers parados; tick do líder; relatório staleQueued === 0 e skippedLostEnqueues true; nenhum warn */ });
```

`test/unit/env-check.test.ts`: sem `redis` a checagem `redis-eviction-policy` não aparece no relatório.

- [ ] **Step 2: Rodar e ver falhar** → FAIL (`transport` ignorado).

- [ ] **Step 3: Implementar**

1. `src/backends/postgres.ts`:

```ts
export function createPostgresBackend(ctx: BackendContext): BackendRuntime {
	const { resolved, store, pool, logger } = ctx;
	const wake = channelName("wake", resolved.schema);
	const wakeHandlers = new Set<(queue: string | undefined) => void>();
	const listener = createPgListener({
		clientConfig: { ...leaderClientConfig(dedicatedClientConfig()), application_name: "minitrigger-listener" },
		// Sem "outbox": no modo Postgres não há varredura de outbox (decisão 28(j)).
		channels: (["wake", "status", "cancel"] as const).map(k => channelName(k, resolved.schema)),
		logger,
		onNotification: (channel, payload) => dispatch(channel, payload),
		onConnected: () => { for (const h of wakeHandlers) h(undefined); },
	});
	const batcher = createNotifyBatcher({ db: pool, logger });
	const transport = createPgTransport({ store, batcher, schema: resolved.schema, queues: [...ctx.registry.queues.values()], systemConcurrency: resolved.systemConcurrency, stallTimeoutMs: resolved.stallTimeoutMs, polling: resolved.polling, wake: { subscribe: h => { wakeHandlers.add(h); return () => wakeHandlers.delete(h); } }, logger });
	const publisher = createPgPublisher({ batcher, schema: resolved.schema });
	const backend: EngineBackend = {
		store, transport, publisher,
		createPubSub: () => createPgPubSub({ publisher, store, schema: resolved.schema, isActiveLocally: ctx.isActiveLocally, subscribe: listener.subscribe, logger }),
		streams: { pipe: () => { throw new ConfigError('ctx.stream is not available with transport "postgres"'); } },
		readStream: () => { throw new ConfigError('runs.readStream is not available with transport "postgres"'); },
		createRunInTransaction: (tx, input) => createSqlTxRunStore(tx, resolved.schema).create({
			...input,
			outbox: { channel: wake, notify: true, pending: false, payload: input.queue },
		}),
	};
	// beforeStart: schema_meta marker → listener.start() limitado a 5 s (timeout = MinitriggerError)
	// afterWorkersStarted: autoteste NOTIFY no canal de wake (1 s, só warn)
	// beforeClose: batcher.close() (depois do hand-back) → listener.close(); close: nada (pool é da engine)
}
```

   `dispatch`: canal de wake → payload é uma fila registrada? acorda só ela; senão, acorda todas
   (payload não confiável). Canais de status/cancel → consumidores registrados por
   `listener.subscribe`.

   Em `createOnce` (`src/store/sql/runs.ts`), `NewRun.outbox` ganha dois campos opcionais:
   `payload?: string` (o `pg_notify(canal, $payload)` usa esse texto em vez de `''`) e
   `pending?: boolean` (padrão `true`; `false` grava `outbox_pending = false`). O caminho `tx` do
   modo Postgres passa **os dois** (`pending: false` é obrigatório: com `outbox_pending = true` o
   claim nunca pegaria a run e não existe varredura de outbox nesse modo). O modo BullMQ não passa
   os campos novos e continua exatamente como hoje. No caminho sem `tx`, o núcleo chama
   `transport.enqueueRun` como sempre, e no Postgres isso é um `NOTIFY` agrupado (o HANDOFF
   registra como pendência a medir embutir o `NOTIFY` nas transições da store).
2. `src/engine.ts`: `createEngine` faz `const runtime = resolved.transport === "postgres" ? createPostgresBackend(ctx) : createBullmqBackend(ctx)`; `start()` = `checkEnvironment({ pool, redis: opcional })` → `runtime.beforeStart()` → `syncDeclarativeSchedules` → `syncQueueStates` (no Postgres, só relê; `isQueuePaused` vem do próprio Postgres, então nunca "conserta" nada) → pub/sub `ready` (limitado) → `startWorkers` → `runtime.afterWorkersStarted()` → líder, com `assertStartNotAbandoned()` entre passos como hoje. `stop()` inalterado na ordem, com `runtime.beforeClose()` no `beforeClose` (depois do hand-back, para o `NOTIFY` do hand-back sair) e `runtime.close()` antes do `pool.end()`.
3. `schema_meta`: em `beforeStart` dos **dois** backends, `const mode = await store.meta.setIfAbsent("transport", resolved.transport); if (mode !== resolved.transport) throw new ConfigError(`schema "${schema}" is used by engines with transport "${mode}"; mixed transports on one schema are not supported (see docs/SPEC.md decision 28)`)`.
4. Pool derivado (Task 2) já inclui `systemConcurrency` no modo Postgres; `warnPoolSizing` do backend Postgres usa a mesma conta.
5. Reconciler: `createReconciler({ ..., recoverLostEnqueues: resolved.transport === "bullmq" })`. Os comentários e o relatório seguem a numeração do código (caso 3 = órfã, caso 4 = stall).
6. Retenção: `scheduleRetention` já usa `transport.enqueueSystem` → funciona sem mudança.

- [ ] **Step 4: Rodar tudo**

Run: `pnpm typecheck && pnpm lint && pnpm test:unit && pnpm vitest run --project integration test/integration/pg-engine.test.ts test/integration/engine.test.ts test/integration/outbox.test.ts test/integration/schedules.test.ts`
Expected: PASS nos dois modos (o modo BullMQ não pode ter mudado).

- [ ] **Step 5: Commit**

```bash
git add src/backends src/engine.ts src/pg src/env-check.ts src/reconciler.ts src/store test/integration/pg-engine.test.ts test/unit
git commit -m "feat(engine): Postgres backend and mixed-mode guard"
```

---

### Task 10c: `bullmq`/`ioredis` carregados sob demanda

**Files:**
- Modify: `src/engine.ts`
- Modify: `package.json` (`peerDependenciesMeta.bullmq.optional = true`, `.ioredis.optional = true`)
- Test: `test/unit/engine-lazy-backend.test.ts`

- [ ] **Step 1: Import dinâmico do módulo do backend (não dos pacotes)**

Não parametrizar `src/queue/transport.ts`, `src/pubsub.ts`, `src/streams.ts`,
`src/queue/connections.ts`, `src/queue/redis-check.ts`: eles continuam importando `bullmq`/
`ioredis` estaticamente. Basta que só `src/backends/bullmq.ts` os alcance e que `engine.ts`
carregue esse módulo com `import()`; com `splitting: true` o tsup põe o módulo e suas
dependências num chunk próprio, fora do grafo estático da entrada principal e de `/testing`.

```ts
async function loadBullmqBackend(): Promise<typeof import("./backends/bullmq.js")> {
	try {
		return await import("./backends/bullmq.js");
	} catch (error) {
		throw new ConfigError('transport "bullmq" needs the optional peers bullmq and ioredis: pnpm add bullmq ioredis (or use transport: "postgres")', { cause: error });
	}
}
```

   `createEngine` continua síncrono: no modo BullMQ o `BackendRuntime` vira `backendReady:
   Promise<BackendRuntime>` memoizado, aguardado no primeiro `start()`/`trigger()`/`runs.*`;
   `createEngineCore` recebe um `EngineBackend` cujos métodos assíncronos aguardam `backendReady`
   (wrapper fino). Métodos síncronos do transporte (`workerConcurrency`, `setWorkerConcurrency`)
   devolvem `undefined`/`false` antes do backend carregar (não há workers antes do `start()`).
   Distinguir o erro de "pacote ausente" (`ERR_MODULE_NOT_FOUND`/`MODULE_NOT_FOUND` citando
   `bullmq` ou `ioredis`) de qualquer outro erro de carga, que é relançado como está.

- [ ] **Step 2: Testes**

`test/unit/engine-lazy-backend.test.ts`: com `vi.doMock("./backends/bullmq.js")` lançando um erro
`MODULE_NOT_FOUND` de `ioredis`, `engine.start()` rejeita com `ConfigError` citando
`pnpm add bullmq ioredis`; `createEngine({ transport: "postgres" })` nunca chama o mock. A prova
real, sem os pacotes instalados, é o `check-pack` (Task 13).

Run: `pnpm typecheck && pnpm lint && pnpm test` → PASS.

- [ ] **Step 3: Commit**

```bash
git add src/engine.ts package.json pnpm-lock.yaml test/unit/engine-lazy-backend.test.ts
git commit -m "feat(engine): load the BullMQ backend on demand; bullmq/ioredis optional peers"
```

---

### Task 10d: Medição antecipada (bench nos dois modos)

**Files:**
- Modify: `bench/run.ts` (`--transport postgres|bullmq`)

Desempenho é a prioridade 1: medir antes de converter a suíte (Task 11).

- [ ] **Step 1:** `bench/run.ts` lê `--transport` (padrão `bullmq`) e monta a config conforme (no
  Postgres, sem `redis`/`prefix` e sem `obliterate` na limpeza). Rodar `pnpm bench -- --transport
  postgres` e `pnpm bench`, três vezes cada.
- [ ] **Step 2:** Comparar com a meta (vazão ±20 %, p99 de sleep ≤ 150 ms). Se a vazão ficar fora,
  medir antes de seguir: contar statements por run (pool contado, como em
  `test/integration/notify-perf.test.ts`) e checar `pg_stat_activity` por espera em
  `NotifyQueue`/`notify`. Candidatos, nesta ordem: janela do batcher, embutir o `NOTIFY` de wake
  nas transições da store, `nextDueAt` só a cada N claims vazios. Registrar números e decisão no
  HANDOFF; o release do modo opcional não é bloqueado pela meta.
- [ ] **Step 3: Commit**

```bash
git add bench/run.ts docs/HANDOFF.md
git commit -m "bench: --transport flag; first Postgres transport numbers"
```

---

### Task 11: Suíte de integração parametrizada por transporte

**Files:**
- Create: `test/helpers/engine-factory.ts`
- Modify: `test/integration/{engine,steps,sleep,subtasks,tokens,retry,stall,stop,schedules,queues,batch-ops,outbox,subscribe,progress,debounce-concurrency}.test.ts` (os nomes reais podem diferir: usar `ls test/integration` e escolher os que exercitam esses comportamentos)
- Modify: `vitest.config.ts` (variável `MT_TRANSPORTS`, padrão `bullmq,postgres`)

**Interfaces:**
- Produces:

```ts
export const TRANSPORTS: readonly TransportKind[]; // de process.env.MT_TRANSPORTS
export function engineConfigFor(transport: TransportKind, ns: TestNamespace, overrides?: Partial<EngineConfig>): EngineConfig;
// bullmq: { redis: ns.redisUrl, prefix: ns.prefix }; postgres: { transport: "postgres" } (sem redis/prefix)
export function describeTransports(name: string, fn: (transport: TransportKind) => void): void; // describe.each(TRANSPORTS)
```

- [ ] **Step 1: Helper e um arquivo convertido**

Converter `test/integration/engine.test.ts`: envolver o conteúdo em
`describeTransports("engine", transport => { ... makeEngine usa engineConfigFor(transport, ns, overrides) ... })`.
Casos que inspecionam o BullMQ diretamente (`rawQueue`) ficam em `if (transport === "bullmq")`.

Run: `MT_TRANSPORTS=postgres pnpm vitest run --project integration test/integration/engine.test.ts`
Expected: PASS. Depois sem a variável: PASS nos dois.

- [ ] **Step 2: Converter os demais arquivos da lista, um por commit**

Para cada arquivo: mesma transformação; casos que dependem de Redis (stream, FLUSHALL, worker
close spin, `rawQueue`) ficam condicionados ao `bullmq`. Ao terminar cada arquivo:

Run: `pnpm vitest run --project integration <arquivo>` → PASS nos dois transportes.

```bash
git add test/integration/<arquivo> && git commit -m "test: run <área> integration suite on both transports"
```

- [ ] **Step 3: Suíte inteira**

Run: `pnpm test`
Expected: PASS. Anotar o tempo total no HANDOFF (Task 14). Se passar de 2× o tempo anterior,
reduzir a lista de arquivos parametrizados aos que exercitam o transporte (engine, sleep, tokens,
subtasks, stop, schedules, queues, outbox) e documentar a escolha.

---

### Task 12: Resiliência e concorrência entre processos (Postgres)

**Files:**
- Create: `test/integration/pg-resilience.test.ts`

- [ ] **Step 1: Testes**

```ts
it("two engines on the same queue never execute the same run twice and never exceed globalConcurrency 3", async () => {
	/* 200 triggers de uma task que grava execution_id numa tabela auxiliar do teste e dorme 20 ms; duas engines transport postgres, concurrency 5, globalConcurrency 3;
	   ao final: 200 completed, count(distinct run) = 200, nenhuma run com 2 execution_ids, e o pico de running observado por polling do teste (a cada 5 ms) <= 3 (Review Focus 5) */
});
it("a slot freed in one engine wakes the other without waiting for the poll", async () => {
	/* globalConcurrency 1, polling.intervalMs 5000; engine A executa r1 (200 ms); r2 fica na fila; engine B (sem A) pega r2 em < 500 ms depois de r1 terminar */
});
it("listener backend terminated: reconnects and NOTIFY works again", async () => {
	/* pg_terminate_backend(pid where application_name = 'minitrigger-listener'); depois de 2 s, latência de um trigger < 300 ms */
});
it("Postgres connections dropped during execution: run goes lost and is recovered by the stall reconciler", async () => {
	/* task que dorme 3 s; pg_terminate_backend de todos os backends do schema exceto o do teste; stallTimeoutMs 2 s; waitFor completed com attempt 2 */
});
it("forged NOTIFY payloads on wake/status/cancel channels are harmless", async () => {
	/* NOTIFY mt_wake_s_<schema>, 'nao-existe'; NOTIFY status, 'zzz'; NOTIFY cancel, '<uuid aleatório>'; engine continua processando um trigger normal; sem error logs (Review Focus 1) */
});
it("a forged cancel NOTIFY with the id of a running run does not abort it", async () => {
	/* task que dorme 1 s; ao receber run:started, NOTIFY mt_cancel_s_<schema>, '<runId>' por um client cru; run completa com attempt 1 (Review Focus 6);
	   controle: engine.runs.cancel(runId) de outra engine aborta de verdade */
});
```

- [ ] **Step 2: Rodar e corrigir o que aparecer** — `pnpm vitest run --project integration test/integration/pg-resilience.test.ts` → PASS.

- [ ] **Step 3: Commit**

```bash
git add test/integration/pg-resilience.test.ts
git commit -m "test(pg): cross-process claims, listener reconnect, lost runs, forged NOTIFY"
```

---

### Task 13: Empacotamento — check-pack sem Redis, versão 0.4.0

(Os peers opcionais entraram na Task 10c e o `--transport` do bench na Task 10d.)

**Files:**
- Modify: `package.json` (versão `0.4.0`)
- Modify: `tsup.config.ts` (`external: ["bullmq", "ioredis", "pg", "cron-parser"]` — confirmar que `cron-parser` NÃO está em `noExternal`)
- Modify: `scripts/check-pack.mjs`

- [ ] **Step 1: check-pack sem bullmq/ioredis**

Acrescentar ao script um segundo diretório temporário com `npm install <tgz> pg cron-parser`
(sem `bullmq`/`ioredis`), e:

```js
// ESM + CJS: import/require do entry principal, de /testing e de cada adaptador sem lançar
// percorrer os chunks carregados (como o check dos adaptadores já faz): nenhum importa "bullmq"/"ioredis"
//   estaticamente; só o chunk do backend BullMQ os cita, e ele não é carregado
// tsc --strict --module NodeNext --skipLibCheck false contra: import { createEngine } from "@rivia-labs/minitrigger"; createEngine({ transport: "postgres", postgres: "x", schema: "s", tasks: [] });
// runtime: createEngine({ redis: "redis://x", ... }).start() rejeita com ConfigError mencionando "bullmq ioredis" (ESM e CJS)
// runtime real (docker): transport postgres, migrate + start + trigger de uma task + stop, sem Redis no ambiente
// /testing: createTestEngine + runInline de uma task com step.sleep, sem bullmq instalado
```

Run: `pnpm check:pack` → `PASS` nos dois diretórios.

- [ ] **Step 2: bench final**

Rodar de novo `pnpm bench -- --transport postgres` e `pnpm bench` (BullMQ) e registrar as duas
tabelas no CLAUDE.md "Benchmark" (Task 14). Critério da spec: vazão dentro de ±20 % e p99 de
sleep ≤ 150 ms; se não atingir, o item fica no HANDOFF como pendência antes de qualquer mudança
de padrão (não bloqueia o release do modo opcional).

- [ ] **Step 3: Commit**

```bash
git add package.json tsup.config.ts scripts/check-pack.mjs
git commit -m "chore(pkg): 0.4.0; check-pack without Redis"
```

---

### Task 14: Documentação e handoff

**Files:**
- Modify: `README.md` (config table: `transport`, `polling`; seção "Postgres-only mode"; "Upgrading to 0.4.0"; conexões; PgBouncer; autovacuum de `system_jobs`; o que não entra)
- Modify: `docs/pt/*` (espelho das seções acima; `docs/pt/03-configuracao.md`, `11-producao.md`, novo `14-modo-postgres.md`)
- Modify: `docs/HANDOFF.md` (estado, contagem de testes, tempo da suíte, pendências)
- Modify: `CLAUDE.md` (notas de implementação: backends, `pg/`, `system_jobs`, `schema_meta`, benchmark dos dois modos)
- Modify: `docs/store-contract.md` (já feito na Task 3; revisar)

- [ ] **Step 1: README "Upgrading to 0.4.0"**

Conteúdo obrigatório: migration 009 (SQL literal dos dois `CREATE INDEX IF NOT EXISTS` para
pré-criação `CONCURRENTLY` e a nota de que as tabelas novas são vazias e instantâneas); peers
opcionais (`bullmq`/`ioredis` continuam necessários no modo BullMQ, nada muda para quem já usa);
`cron-parser` passa a validar cron de verdade em `defineTask`/`schedules.upsert` (padrões que
antes só falhavam no BullMQ agora falham cedo com `ConfigError`); `EngineConfig` virou união
discriminada (quem monta a config com spread pode precisar de `as const` em `transport`);
`transport: "postgres"` é opt-in.

- [ ] **Step 2: Seção "Postgres-only mode"**

Quando usar; config mínima; o que é `ConfigError` (`redis`, `prefix`, `outbox`, `rateLimit`,
`ctx.stream`, `runs.readStream`); latências (NOTIFY ms; sem NOTIFY até `polling.intervalMs`);
`globalConcurrency` exato (claims da fila serializados); conexões por engine (`pool.max` derivado
inclui `systemConcurrency`; + até duas fora do pool: LISTEN e líder); PgBouncer (apontar as duas
conexões dedicadas direto; ou aceitar só poll); `NOTIFY` não confiável por desenho, e qualquer
role com `CONNECT` no database pode escutar os canais e ver `runId`s (use um database próprio se
isso importa); sem líder, retenção e reconciler param mas runs e cron continuam; modo misto
recusado e como migrar um schema de um modo para outro (todas as engines em 0.4+ antes, porque
0.3 não grava o marcador; parar todas as engines, `UPDATE schema_meta SET value = 'postgres'
WHERE key = 'transport'`, garantir que `system_jobs` está vazia ou que o BullMQ não tem jobs
pendentes); autovacuum de `system_jobs`; sem backfill de cron, e um tick em backoff atrasa os
seguintes.

- [ ] **Step 3: HANDOFF e CLAUDE.md**

HANDOFF: tabela de áreas com `src/backends`, `src/pg`, migration 009; contagem real de testes
(saída do `pnpm test`); "Próximas ondas": paridade de bench e eventual troca de padrão, `NOTIFY`
embutido nas transições da store (se o bench pedir), streams por tabela (se pedido). CLAUDE.md:
bloco "Transporte Postgres (decisão 28, migration 009)" com as decisões de implementação (claim
single-statement com colunas qualificadas, `started_at`/`concurrency_waits`/`enqueue_seq` no claim,
`globalConcurrency` por advisory lock, `waiting`+`run_after`, `system_jobs` com `locked_until` e
fencing por `locked_by`, `schedule-tick` regravado no lugar e nunca apagado, upsert de tick no-op
sem mudança, `SystemJobDelayed`, `NotifyBatcher`, cancel relido, `cron-parser`, `schema_meta`,
`recoverLostEnqueues`, import dinâmico só de `backends/bullmq.ts`, `RedisLike`, união
discriminada), e a tabela do benchmark nos dois modos.

- [ ] **Step 4: Verificação final e commit**

Run: `pnpm typecheck && pnpm lint && pnpm test && pnpm build && pnpm check:pack`
Expected: tudo PASS.

```bash
git add README.md docs CLAUDE.md
git commit -m "docs: Postgres transport mode, upgrading to 0.4.0, handoff"
```

---

## Auto-revisão do plano

- **Cobertura da spec:** emendas (T1); config e união discriminada (T2); migration/store/claim/
  system_jobs/schema_meta (T3); listener (T4); pub/sub NOTIFY com batcher e cancel relido (T5);
  cron (T6); laços, poll adaptativo, dreno com claim em voo, fencing (T7); Transport (T8);
  executor com run reivindicada e concurrency key (T9); backend BullMQ extraído (T10a); backend
  Postgres, ciclo de vida, autoteste de LISTEN, reconciler sem casos 1/2, retenção, modo misto
  (T10b); import dinâmico e peers opcionais (T10c); bench antecipado (T10d); suíte parametrizada
  (T11); resiliência e corrida entre processos (T12); check-pack (T13); docs (T14).
  `queue_state` no claim: T3. `globalConcurrency`: T3/T7/T12. Outbox no modo Postgres
  (`pending: false`, payload de wake): T10b.
- **Correções da segunda análise (2026-09-26):** colunas qualificadas no `RETURNING` do claim,
  `started_at`, `concurrency_waits` intacto no claim (T3/T9); `fail` sem dupla modificação,
  parâmetros sempre referenciados, `setIfAbsent` relido (T3); fencing por `locked_by` (T3/T7);
  `schedule-tick` nunca apagado e upsert no-op sem mudança (T3/T7/T8); `pending: false` no `tx`
  (T10b); `test-engine.ts` sem `bullmq` em runtime (T7); cancel forjado (T5/T12);
  `globalConcurrency` exato (T3/T12); dreno esperando o claim em voo (T7); `NOTIFY` agrupado e de
  runs atrasadas (T5/T8); timeout do `LISTEN` como `MinitriggerError` (T10b); Task 10 dividida;
  bench antecipado.
- **Tipos consistentes:** `RunClaimStore.claimRuns/nextDueAt`, `SystemJobStore.*` (com
  `lockedBy`), `SchemaMetaStore.get/setIfAbsent`, `createPgListener`/`channelName`,
  `createNotifyBatcher`, `createPgPublisher`/`createPgPubSub`, `nextOccurrence`/
  `scheduleTickJobId`/`scheduleIdOfJob`, `createQueueWorker`/`createSystemWorker`/`PgSystemJob`,
  `createPgTransport`, `RunJob.claimed`, `recheckConcurrencyLimit`, `BackendRuntime`/
  `BackendContext`, `SystemJobDelayed`, `RedisLike`, `TransportKind` são usados com os mesmos
  nomes em todas as tarefas.
- **Review Focus:** 1 → T7 e T12; 2 → T3; 3 → T3 e T10b; 4 → T7 e T10b; 5 → T3 e T12; 6 → T5 e
  T12; 7 → T9.
- **Placeholders:** os testes marcados com `/* ... */` descrevem o cenário e a asserção em
  linguagem natural onde o código exato depende de fakes locais já existentes no repositório
  (`fakeClientFactory` em `test/unit/outbox.test.ts`, `makeEngine` em `engine.test.ts`); o
  implementador deve copiar esses helpers, não reinventá-los.
