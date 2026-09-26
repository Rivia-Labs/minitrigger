# 03. Configuração

Tudo passa por `createEngine(config)`. Este guia cobre cada campo, organizado por seção. Para a
tabela crua e compacta, veja também [13. Referência da API](13-referencia-api.md#config-options).

```ts
import { createEngine, defineTask } from "@rivia-labs/minitrigger";

const engine = createEngine({
	postgres: process.env.DATABASE_URL!,
	redis: process.env.REDIS_URL!,
	tasks: [myTask],
});

await engine.migrate();
await engine.start();
```

## Conexão, schema e prefix

| Opção | Padrão | Notas |
|---|---|---|
| `postgres` | — | String de conexão, ou `{ pool }` com um `pg.Pool` já existente (nunca fechado pelo engine). |
| `redis` | — | String de conexão, ou `{ connection }` com uma instância `ioredis` já existente (só usada para produtores não bloqueantes; workers/subscribers sempre recebem um `duplicate()`). |
| `schema` | `"minitrigger"` | Schema Postgres. Precisa casar com `^[a-z_][a-z0-9_]{0,62}$`. |
| `prefix` | `"minitrigger"` | Prefixo de chave do BullMQ. Mesmo padrão de `schema`. |
| `tasks` | — | Array de tasks criadas com `defineTask`. |

**`prefix` precisa ser único por schema.** Toda estrutura do Redis (filas de task, a fila interna
`${prefix}-system`, canais de pub/sub, streams) é chaveada só por `prefix`, não por `schema`. Dois
engines com o mesmo `prefix` e `schema`s diferentes consumiriam jobs um do outro. O primeiro
`engine.start()` grava seu schema na chave Redis `${prefix}:owner-schema`; um `start()` com outro
schema no mesmo prefix lança `ConfigError`. Detalhes e o footgun completo em
[08. Agendamentos](08-agendamentos.md#prefix-precisa-ser-único-por-schema).

## Pool de conexões

| Opção | Padrão | Notas |
|---|---|---|
| `pool.max` | concorrência total das filas de task + 5 (no mínimo 10) | Só usado quando o engine cria o próprio pool (string em `postgres`). `start()` avisa quando está abaixo disso para as filas iniciadas. |
| `pool.connectionTimeoutMs` | `10 000` | `connectionTimeoutMillis` do pool próprio do engine: uma query esperando mais que isso por uma conexão livre falha (um heartbeat que falhe só gera um aviso). |

**Fórmula de dimensionamento:** por padrão, `pool.max = max(10, soma da concorrência de todas as
filas de task + 5)`. Os 5 extras são folga para as queries de bookkeeping do próprio engine (além
do que os seus workers consomem).

Além do pool, todo engine **iniciado** (`start()` chamado) segura **duas** conexões dedicadas fora
dele: uma para eleição de líder e outra para o `LISTEN` do outbox. Então, para dimensionar
`max_connections` do Postgres:

```
max_connections >= processos × (pool.max + 2)
```

Se você injeta seu próprio `pg.Pool`, dimensione-o da mesma forma e configure um
`connectionTimeoutMillis` — `start()` avisa quando ele é menor do que as filas iniciadas precisam,
ou quando não tem timeout de aquisição (um heartbeat esperaria por uma conexão sem limite em vez de
falhar com um aviso). Um pool dedicado para a biblioteca é recomendado em vez de compartilhar o
pool da sua aplicação — veja [11. Produção](11-producao.md).

## Heartbeat, stall e shutdown

| Opção | Padrão | Notas |
|---|---|---|
| `heartbeatIntervalMs` | `10 000` | Com que frequência uma execução `running` atualiza `heartbeat_at`. Precisa ser menor que `stallTimeoutMs`. |
| `heartbeatTimeoutMs` | `2 000` | `SET LOCAL statement_timeout` aplicado a cada query/transação de heartbeat. |
| `stallTimeoutMs` | `60 000` | Uma run `running` cujo heartbeat está mais velho que isso é considerada travada (stall) pelo reconciliador. |
| `shutdownTimeoutMs` | `30 000` | Quanto tempo `stop()` espera pelas runs ativas antes de devolvê-las à fila. |

## Retenção

| Opção | Padrão | Notas |
|---|---|---|
| `retention.days` | `30` | Runs terminadas mais velhas que isso são apagadas (só depois de não terem pai, ou o pai também ter terminado). |
| `retention.intervalMs` | `3 600 000` | Com que frequência (por janela) o líder agenda uma varredura de retenção. |
| `retention.batchSize` | `1 000` | Runs apagadas por statement durante uma varredura. |

## Reconciliador

| Opção | Padrão | Notas |
|---|---|---|
| `reconciler.intervalMs` | `15 000` | Tick de eleição de líder + reconciliador. Também a pior latência para recuperar um enqueue perdido (mais `graceMs`). |
| `reconciler.graceMs` | `30 000` | Quanto tempo uma run `queued`/`waiting` pode ficar depois do seu `run_after` antes do reconciliador reenfileirá-la. |
| `reconciler.batchSize` | `100` | Linhas tratadas por caso do reconciliador em cada tick. |

## Outbox (trigger transacional)

| Opção | Padrão | Notas |
|---|---|---|
| `outbox.pollIntervalMs` | `2 000` | Com que frequência todo engine iniciado varre por runs de `trigger(..., { tx })` já commitadas, mesmo sem um `NOTIFY`. Faixa 100–2³¹-1. |
| `outbox.batchSize` | `500` | Linhas reivindicadas por statement `claimOutbox`. Faixa 1–10 000. |
| `outbox.reconnectMaxMs` | `30 000` | Teto do backoff de reconexão do cliente `LISTEN` dedicado. Faixa 500–2³¹-1 (o backoff do listener começa em 500 ms). |
| `outbox.notify` | `true` | `false` pula o `pg_notify` no commit de `trigger(..., { tx })` (o despacho passa a depender só de `outbox.pollIntervalMs`). |

Ver [06. Outbox transacional](06-outbox.md) para o uso completo.

## Schedules

| Opção | Padrão | Notas |
|---|---|---|
| `schedules.pruneDeclarative` | `false` | No `start()`, também apaga (Postgres + BullMQ) um schedule de `defineTask({ schedule })` que nenhuma task registrada declara mais. Nunca toca em schedules de `engine.schedules.upsert()` (`"api"`). |

Ver [08. Agendamentos](08-agendamentos.md).

## Limites de payload/output/log/progresso

| Opção | Padrão | Notas |
|---|---|---|
| `limits.maxPayloadBytes` | `1 MB` | Payloads de trigger, outputs de token, outputs de task. |
| `limits.maxStepOutputBytes` | `256 KB` | Outputs de `step.run`. |
| `limits.maxLogEntryBytes` | `16 KB` | O campo `data` de uma entrada de `ctx.logger`. |
| `limits.maxProgressBytes` | `16 KB` | O valor de `ctx.progress.set` (`runs.progress`). |
| `limits.maxInlineStepOutputBytes` | `16 MB` | Limite acima do qual os outputs de step de uma run são carregados em lotes, em vez de numa única query de inicialização. |

## Logging

| Opção | Padrão | Notas |
|---|---|---|
| `logging.minLevel` | `"info"` | Entradas de `ctx.logger` abaixo desse nível não são gravadas nem espelhadas. |
| `logging.mirror` | `true` | Também envia entradas de `ctx.logger` para `logger` (com `runId`, `taskId`, `attempt`). |
| `logger` | console, só `warn`/`error` | `{ debug, info, warn, error }`, cada um `(fields, message) => void` (compatível com pino e com console). |

Ver [09. Observabilidade](09-observabilidade.md).

## Outros

| Opção | Padrão | Notas |
|---|---|---|
| `idempotencyKeyTtl` | `"7d"` | TTL padrão para uma chave de idempotência do usuário; não pode exceder `retention.days`. |
| `systemConcurrency` | `5` | Concorrência do worker interno da fila `${prefix}-system` (ticks, expirações, retenção). |

`Duration` (usado por vários campos, como `idempotencyKeyTtl`, e por `step.sleep`/`delay`/timeouts)
aceita um número de milissegundos ou uma string como `"5m"`, `"2d"`, `"1.5h"` (unidades: `ms`, `s`,
`m`, `h`, `d`, `w`).

## Duas engines, um processo

Duas instâncias de `createEngine()` com `schema`/`prefix` diferentes podem coexistir no mesmo
processo sem interferir uma na outra — não há estado global na biblioteca.
