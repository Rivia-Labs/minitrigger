# 11. Produção

## Checagens de ambiente no `start()`

`engine.start()` roda `checkEnvironment()` antes de iniciar qualquer worker: versão do servidor
Postgres, `maxmemory-policy` do Redis, e migrations pendentes contra `schema` (com `autoMigrate:
false` — chame `engine.migrate()` você mesmo antes). Uma checagem cujo comando nem consegue rodar
(ex.: `CONFIG GET` bloqueado por um provedor Redis gerenciado, ou uma resposta inesperada/vazia
dele) só gera um `warn` e entra no relatório devolvido; só uma checagem que recebe uma resposta
definitiva e errada (Postgres velho demais, política de eviction diferente de `noeviction`,
migrations de fato pendentes) lança `ConfigError` nomeando cada problema encontrado. A mesma função
(`checkEnvironment`, exportada da raiz do pacote junto com seus tipos) pode ser chamada
isoladamente também, por exemplo num health check ou antes de `migrate()` num script de deploy.

## Dimensionamento do pool

O pool precisa de `max >= concorrência total dos workers + 5` (as próprias queries de bookkeeping
do engine precisam de folga além do que seus workers consomem). Além do pool, todo engine
**iniciado** segura **duas** conexões dedicadas fora dele (eleição de líder e o `LISTEN` do
outbox), então dimensione `max_connections` para `processos × (pool.max + 2)`. Um pool que o engine
cria já recebe esse tamanho por padrão (no mínimo 10) e um `connectionTimeoutMillis` de 10 s
(`pool.connectionTimeoutMs`).

Se você injeta seu próprio `pg.Pool`, dimensione-o da mesma forma e configure um
`connectionTimeoutMillis`: `start()` avisa quando ele é menor do que as filas iniciadas precisam ou
não tem timeout de aquisição (um heartbeat esperaria por uma conexão sem limite em vez de falhar
com um aviso). **Um pool dedicado para a biblioteca é recomendado em vez de compartilhar o pool da
sua aplicação** — injetar o pool da sua aplicação faz as queries da biblioteca competirem com as da
sua aplicação por conexões; um `pool.max` pequeno demais pode causar falhas intermitentes de
heartbeat/aquisição sob carga.

Advisory locks: migrations usam `pg_advisory_xact_lock` dentro de uma transação; o líder
(dono do reconciliador/retenção) usa `pg_try_advisory_lock` num client dedicado mantido fora do
pool enquanto segura o lock — nunca numa conexão emprestada do pool, já que uma conexão pooled pode
ser reciclada silenciosamente enquanto o lock ainda parece estar sendo segurado.

## PgBouncer

PgBouncer em **transaction pooling** quebra o `LISTEN` do outbox (a sessão não é mantida entre
transações): o despacho de `trigger(..., { tx })` passa a depender só da varredura de
`outbox.pollIntervalMs` (2 s por padrão), perdendo a latência de milissegundos do `NOTIFY`. Aponte
a `postgres` do engine para um pool em modo sessão, ou direto para o Postgres, se precisar de
despacho rápido. Detalhes completos em [06. Outbox transacional](06-outbox.md).

## Shutdown gracioso

```ts
await engine.stop();
```

`stop()` para de aceitar novos jobs e espera pelas runs ativas até `shutdownTimeoutMs` (padrão
30 s). Uma run ainda ativa depois desse timeout é devolvida a `queued` (posse limpa, **sem
incrementar a tentativa**) e reenfileirada — isso é uma garantia **at-least-once**: se a lógica da
sua task já teve um efeito colateral antes do timeout, esse efeito pode rodar de novo na
retentativa a menos que esteja dentro de um step memoizado.

Ordem do shutdown: os workers param de buscar jobs primeiro, depois `stop()` espera (com limite) o
tick do líder, depois devolve as runs ainda ativas. Uma varredura de retenção em andamento para
entre lotes. `stop()` também termina quando o Redis está inacessível: conexões Redis que não estão
prontas são descartadas em vez de receberem um `QUIT` (que esperaria por uma reconexão que nunca
vem). Fechar o pool criado pelo engine também é limitado (5 s): se um client ainda está em uso,
`stop()` loga um `warn` e resolve, e o pool termina de fechar quando esse client for liberado. Um
pool injetado nunca é fechado pelo engine.

## Retenção

| Opção | Padrão | Notas |
|---|---|---|
| `retention.days` | `30` | Runs terminadas (`completed`/`failed`/`canceled`) mais velhas que isso são apagadas, só depois de não terem pai ou o pai também ter terminado. |
| `retention.intervalMs` | `3 600 000` | Janela na qual o líder agenda no máximo uma varredura. |
| `retention.batchSize` | `1 000` | Runs apagadas por statement durante a varredura. |

A retenção roda como um job `retention` na fila interna `-system`, no máximo uma vez por janela de
`retention.intervalMs` (a janela vem do relógio do banco); qualquer worker `-system` de qualquer
engine pode processá-la (uma varredura é idempotente).

## Líder e reconciliador

Todo engine iniciado roda um timer a cada `reconciler.intervalMs` (15 s por padrão) que tenta
adquirir o lock de líder. Só o líder roda o reconciliador (enqueues perdidos, runs `waiting`
obsoletas ou órfãs — logadas como `warn` com o `runId` —, runs travadas em stall, waitpoints
vencidos) e agenda a retenção. Ticks nunca se sobrepõem, e `stop()` espera o tick atual terminar e
libera o lock. Se a conexão dedicada do líder morrer, outro engine assume dentro de poucos ticks. O
líder segura uma conexão Postgres extra, fora do pool, enquanto lidera. `engine.migrate()` nunca
espera pelo líder (usa um advisory lock diferente).

**Postgres é a fonte da verdade.** `trigger()` faz duas escritas não atômicas: insere a run em
Postgres, depois enfileira no Redis. Se o processo morrer entre as duas, a run existe mas nunca foi
enfileirada — isso é recuperado pelo reconciliador, que reenfileira periodicamente runs
`queued`/`waiting` cujo `run_after` já passou; a latência de recuperação é limitada pelo intervalo
do reconciliador, não indefinida.

**Heartbeat e stall.** Uma execução em `running` atualiza `heartbeat_at` a cada
`heartbeatIntervalMs`. O reconciliador trata uma run `running` cujo heartbeat está mais velho que
`stallTimeoutMs` como travada e a recupera (reenfileira com tentativa incrementada, ou falha
definitivamente quando as tentativas se esgotam). Como o heartbeat roda no mesmo event loop do
código da sua task, **código síncrono que bloqueia o event loop por mais que `stallTimeoutMs`
causa um stall por si só** — mova trabalho pesado de CPU para dentro de um step usando
`worker_threads` em vez de rodar inline.

**Toda mudança de status é publicada** (best-effort) em `${prefix}:run-status` com o `updated_at`
da linha (a fonte para `engine.runs.subscribe`).

**O Redis pode ser apagado.** Depois de um `FLUSHALL` (ou qualquer job perdido), o reconciliador
reenfileira toda run a partir de Postgres dentro de aproximadamente
`reconciler.graceMs + reconciler.intervalMs`.

## Upgrade 0.2.0 para 0.3.0

- **Pacote renomeado** para `@rivia-labs/minitrigger`: troque a dependência `minitrigger` e todo
  `from "minitrigger"` por `from "@rivia-labs/minitrigger"`.
- **`createTestEngine` mudou** para o subpath `@rivia-labs/minitrigger/testing`; não é mais
  exportado da entrada principal.
- **Node.js >= 22** passa a ser exigido.
- **Migration `006_outbox`** adiciona `runs.outbox_pending boolean NOT NULL DEFAULT false`
  (metadata apenas, quase instantâneo) e um índice parcial com `CREATE INDEX` simples, que segura
  um lock `SHARE` em `runs` enquanto varre a tabela (mesmo trade-off da 005 abaixo). Numa tabela
  `runs` grande, aceite a pausa numa janela de manutenção, ou pré-construa o índice
  `CONCURRENTLY` e marque a versão 6 como aplicada manualmente — fora de uma transação, contra o
  schema alvo, antes de `engine.migrate()`:

  ```sql
  SET search_path TO "<schema>";
  SET lock_timeout = '5s';

  ALTER TABLE runs ADD COLUMN IF NOT EXISTS outbox_pending boolean NOT NULL DEFAULT false;

  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_outbox_pending
    ON runs (created_at) WHERE outbox_pending;

  -- Precisa devolver indisvalid = t; se f (build interrompido), DROP INDEX CONCURRENTLY e reconstrua.
  SELECT indisvalid FROM pg_index
  WHERE indexrelid = '"<schema>".idx_runs_outbox_pending'::regclass;

  -- Só quando indisvalid = t:
  INSERT INTO "<schema>".schema_migrations (version, name)
  VALUES (6, 'outbox')
  ON CONFLICT (version) DO NOTHING;
  ```

  Confira as definições contra `src/migrations/006_outbox.sql` da versão que você está instalando.
  Vindo de 0.1.x, aplique primeiro os passos de 0.2.0 abaixo.
- **Migration `007_queue_state`** (pause/resume de fila) só cria a pequena tabela `queue_state`:
  sem lock em `runs`, nada a preparar.
- **APIs novas:** `engine.queues` (pause/resume/list/scale) e `engine.runs.cancelMany`/
  `retryFailed`; `EngineClient` agora inclui `queues`.

## Upgrade 0.1.x/anterior para 0.2.0

A migration `005_debounce_concurrency` (debounce e chaves de concorrência) adiciona dois índices em
`runs` — `idx_runs_debounce` (único, parcial) e `idx_runs_concurrency_running` (parcial) — com
`CREATE INDEX` simples, não `CONCURRENTLY`: `engine.migrate()` roda toda migration pendente dentro
de **uma** transação (para que uma falha faça rollback de tudo em vez de deixar o schema
meio-migrado), e `CREATE INDEX CONCURRENTLY` não pode rodar dentro de um bloco de transação — o
Postgres rejeita isso de cara. Um `CREATE INDEX` simples segura um lock `SHARE` em `runs` pelo
tempo que o build do índice levar, o que bloqueia todo `INSERT`/`UPDATE`/`DELETE` em `runs`
(incluindo aquisição de run, conclusão e o heartbeat) — numa tabela `runs` com bastante histórico,
isso pode ser uma pausa real e visível para o usuário.

Duas formas de fazer o upgrade:

1. **Aceite a pausa, numa janela de manutenção.** Use `pg_stat_user_tables`/
   `pg_relation_size('runs')` para ter uma ideia de quanto tempo um `CREATE INDEX` (sem
   `CONCURRENTLY`) pode levar na sua tabela antes de rodar `engine.migrate()`; faça isso em baixo
   tráfego, ou tire o engine do ar durante o período.
2. **Evite a pausa: pré-construa os dois índices `CONCURRENTLY`, depois diga ao `migrate()` que já
   estão prontos.** Os próprios statements `CREATE INDEX` da migration 005 não têm `IF NOT EXISTS`
   (e não podem ser editados agora que já foram lançados — mudanças de schema só entram em
   migrations novas), então simplesmente pré-criar os índices e chamar `engine.migrate()` como de
   costume falharia em "relation already exists" e faria rollback do lote inteiro. Em vez disso,
   marque a versão 5 como aplicada diretamente, para que a própria contabilidade do `migrate()`
   (`schema_migrations`) pule a execução do SQL dela. Rode isso FORA de uma transação (ex.: `psql`
   com autocommit, um statement por vez), contra o schema alvo, ANTES de chamar `engine.migrate()`:

   ```sql
   SET search_path TO "<schema>";
   SET lock_timeout = '5s';

   ALTER TABLE runs
     ADD COLUMN IF NOT EXISTS debounce_key text,
     ADD COLUMN IF NOT EXISTS debounce_deadline timestamptz,
     ADD COLUMN IF NOT EXISTS concurrency_key text,
     ADD COLUMN IF NOT EXISTS concurrency_waits int NOT NULL DEFAULT 0;

   CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_debounce
     ON runs (task_id, debounce_key)
     WHERE status = 'queued' AND debounce_key IS NOT NULL;

   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_concurrency_running
     ON runs (queue, concurrency_key)
     WHERE status = 'running' AND concurrency_key IS NOT NULL;
   ```

   **Antes de marcar a versão 5 como aplicada, confirme que os dois índices realmente construíram.**
   `CREATE INDEX CONCURRENTLY` pode ser interrompido (sessão morta, `psql` desconectado, crash) e
   deixar um índice `INVALID` para trás em vez de falhar de cara — e como ele já tem o nome
   `idx_runs_debounce`/`idx_runs_concurrency_running`, rodar o bloco de novo (`IF NOT EXISTS`)
   pularia silenciosamente a reconstrução, já que o índice existe, só que inutilizável:

   ```sql
   SELECT indexrelid::regclass::text AS index_name, indisvalid
   FROM pg_index
   WHERE indexrelid IN (
     '"<schema>".idx_runs_debounce'::regclass,
     '"<schema>".idx_runs_concurrency_running'::regclass
   );
   ```

   Se alguma linha vier `indisvalid = f`, apague e reconstrua só aquele índice (ainda fora de uma
   transação, `CONCURRENTLY` nos dois statements) antes de continuar, depois rode a checagem de
   novo até os dois virem `indisvalid = t`, e só então marque a versão 5 aplicada:

   ```sql
   INSERT INTO "<schema>".schema_migrations (version, name)
   VALUES (5, 'debounce_concurrency')
   ON CONFLICT (version) DO NOTHING;
   ```

   Depois disso, `engine.migrate()` vê a versão 5 já registrada e não faz nada por ela (migrations
   anteriores/`CREATE TABLE IF NOT EXISTS schema_migrations` ainda rodam normalmente se também
   estiverem pendentes). Confira `<schema>` e as definições de coluna/índice acima contra
   `src/migrations/005_debounce_concurrency.sql` da versão para a qual você está migrando antes de
   rodar isso manualmente.

Se você está fazendo upgrade de um deployment v0.1.0 existente (que já tem as migrations 001-003,
incluindo o `idx_runs_created` da própria 003, aplicadas), a migration 004 é a única outra que a
0.2.0 adiciona em cima da 005, e é só metadata (`ALTER TABLE runs ADD COLUMN progress jsonb`, sem
índice, quase instantâneo) — a preocupação acima é específica dos dois novos índices da 005.
