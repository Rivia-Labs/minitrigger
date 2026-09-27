# Modo de transporte só-Postgres (`transport: "postgres"`) — desenho

Data: 2026-09-26. Estado: aprovado pelo dono em conversa (recomendações do revisor aceitas);
este documento é a spec de referência para a implementação. BullMQ continua o padrão.
Revisado em 2026-09-26 (segunda análise, aprovada pelo dono): SQL do claim, fencing de
`system_jobs` (`locked_by`), `globalConcurrency` exato por advisory lock, cancel relido, NOTIFY
agrupado, ciclo de vida de `schedule-tick`, erros transitórios como `MinitriggerError`.

## 1. Objetivo

Permitir rodar o minitrigger sem Redis: `createEngine({ transport: "postgres", postgres, schema,
tasks })`. O Postgres já é a fonte da verdade (decisão 1); neste modo ele passa a ser também o
transporte (fila, atraso, wake-up, `-system`, pub/sub). Nenhuma decisão fixa é revogada; as emendas
da seção 2 abaixo são acréscimos "no modo Postgres, ...". O modo BullMQ não muda de comportamento.

Prioridades do dono, nesta ordem: desempenho, segurança, facilidade de uso.

### Escopo do MVP

Entra:

- `transport: "bullmq" | "postgres"` na config (padrão `"bullmq"`).
- Fila = linhas de `runs`; claim com `FOR UPDATE SKIP LOCKED`; wake por `LISTEN/NOTIFY`; poll
  adaptativo como rede de segurança.
- Tabela `system_jobs` (migration 009) para `waitpoint-expire`, `retention`, `schedule-tick` e
  `reconcile`, com o dispatcher atual (`src/executor/system.ts`) intocado.
- Ticks de cron calculados na aplicação com `cron-parser` (única dependência direta nova).
- Pub/sub (status, cancel) por `NOTIFY`, payload só com identificador, sempre relido do banco.
- `globalConcurrency` em SQL, exato (claim serializado por fila com advisory lock quando há
  limite). Pausa de fila honrada pelo claim.
- Marcador de modo no schema; modo misto recusado.
- `bullmq` e `ioredis` como peers opcionais; tipos públicos sem depender do ioredis; nem a
  entrada principal nem `/testing` os carregam em runtime (hoje `src/testing/test-engine.ts` e
  `src/executor/system.ts` importam `DelayedError` do `bullmq`: isso muda).
- Suíte de integração parametrizada por transporte para o subconjunto compartilhado.

Fica de fora (explicitamente `ConfigError` ou no-op documentado):

- `queue.rateLimit` (`ConfigError` no `createEngine`).
- `ctx.stream` / `runs.readStream` (`ConfigError` na chamada; `ctx.stream` lança para a task).
- Trocar o padrão para Postgres (só depois de medir paridade com `pnpm bench`).
- Prioridade de runs, fairness entre filas além de FIFO por `run_after`.

## 2. Emendas à seção 2 da SPEC (texto a aplicar em `docs/SPEC.md`)

O agente aplica estes textos em `docs/SPEC.md` como primeira tarefa do plano. As decisões
existentes recebem um parágrafo "**No modo Postgres**"; a decisão 28 é nova.

**Decisão 1 — acrescentar:** No modo Postgres (`transport: "postgres"`, decisão 28) não existe
transporte separado: a própria linha de `runs` com `status IN ('queued','waiting')` e
`run_after <= now()` é a fila, e os workers a reivindicam com `UPDATE ... FROM (SELECT ...
FOR UPDATE SKIP LOCKED)` num único statement que grava o mesmo que o `acquire` de hoje
(`status = 'running'`, `execution_id`, `heartbeat_at`, `started_at` se nulo, `debounce_key =
NULL`); o claim é o `acquire`. `enqueue_seq` não muda no claim (não há job a invalidar) e
`concurrency_waits` só é zerado depois da re-checagem da decisão 25. `trigger()` vira uma escrita
atômica: nada pode se perder entre o insert e a fila. Um `NOTIFY` acorda os workers e o poll é a
rede de segurança. Os
casos 1 e 2 do reconciler (enqueue perdido) ficam desligados nesse modo: uma run vencida é
backlog, não perda. O invariante `waiting` + `run_after` preenchido = pronta para retomar em
`run_after`, `waiting` + `run_after NULL` = bloqueada, já vale hoje e é o que o claim usa.

**Decisão 8 — acrescentar:** No modo Postgres, os jobs da `-system` são linhas da tabela
`system_jobs` (migration 009: `job_id text PRIMARY KEY`, `name`, `data jsonb`, `run_after`,
`attempts`, `max_attempts`, `locked_until`, `locked_by`, `created_at`), reivindicadas com
`SKIP LOCKED` pelo mesmo dispatcher, apagadas ao concluir. Todo claim grava um `locked_by` novo
(uuid) e toda escrita de quem processou (concluir, adiar, falhar, reagendar) é condicionada a
`locked_by` (fencing): um worker cujo lock venceu e foi reivindicado por outro não apaga nem
reagenda o job alheio. `job_id` deduplica exatamente como no BullMQ (`wp-<id>-expire`,
`retention-<janela>`, `schedule-tick:<id>`). O tick de cron é uma linha `schedule-tick:<id>`
cujo `run_after` é a próxima ocorrência, calculada com `cron-parser` (dependência direta, mesma
semântica de cron e timezone do BullMQ) a partir do `now()` do banco (decisão 17). A linha nunca
é apagada pelo worker: ao concluir, ou ao esgotar as tentativas, ela é atualizada no lugar para a
próxima ocorrência (um statement, sem janela em que o schedule some), como o scheduler do BullMQ,
que não morre com um tick que falhou. Regravar um schedule (sync declarativo, `schedules.upsert`,
reparo) só mexe no `run_after` quando `cron`/`timezone` mudaram; igual ao BullMQ, é no-op para o
tick pendente. Sem backfill: ticks de um período sem nenhum worker `-system` são perdidos, igual a
hoje; um tick em backoff atrasa os seguintes (não há ticks paralelos do mesmo schedule). A chave
`__schedule:id:ts` (decisão 9) continua sendo a barreira contra duplicata.

**Decisão 14 — acrescentar:** `bullmq` e `ioredis` passam a peers opcionais
(`peerDependenciesMeta`); só o modo BullMQ os carrega, por um `import()` dinâmico do módulo
`src/backends/bullmq.ts` (que continua importando `bullmq`/`ioredis` estaticamente; o
`splitting` do tsup o põe num chunk próprio), e a ausência vira `ConfigError` com a instrução de
instalação. Os tipos públicos não importam do `ioredis`: `redis.connection` é um tipo estrutural
mínimo. No modo Postgres, a conta de conexões é `pool.max` (workers das filas + worker `-system`
+ polls) mais até duas fora do pool (líder e `LISTEN`); nesse modo o `pool.max` derivado inclui
`systemConcurrency` (o `-system` passa a usar o pool). No modo BullMQ a fórmula não muda.

**Decisão 15 — acrescentar:** No modo Postgres, `prefix`, `redis` e `outbox` são recusados com
`ConfigError` (configuração explícita, nada é ignorado em silêncio; `outbox` não tem efeito porque
o trigger transacional já é a fila, decisão 28(j)). Canais de `NOTIFY` derivam do schema, como o
do outbox (`mt_<fim>_s_<schema>` ou `mt_<fim>_h_<sha256[:48]>` quando passa de 63 bytes, com `fim`
em `wake`, `status`, `cancel`, `outbox`). O dono do schema deixa de ser uma chave no Redis: a
tabela `schema_meta` (migration 009) guarda `transport`; o primeiro `start()` de uma engine 0.4+
grava (nos dois modos), e um `start()` com outro modo no mesmo schema é `ConfigError` (modo misto
teria duas fontes de jobs da `-system`). Engines 0.3 não gravam o marcador: para passar um schema
ao modo Postgres, todas as engines BullMQ dele precisam estar em 0.4+ (ou paradas) antes.

**Decisão 16 — acrescentar:** A conexão `LISTEN` fica fora do pool, com o mesmo tratamento do
client do líder (keepalive TCP, reconexão com backoff 0,5–30 s, fechamento limitado no `stop()`).
É uma só por engine iniciada, para todos os canais do modo (no Postgres: wake, status, cancel;
sem `outbox`, que não existe nesse modo). O listener do outbox do modo BullMQ (decisão 26) é
reescrito sobre o mesmo cliente genérico, sem mudar de comportamento. Um pooler em modo transação (PgBouncer) quebra
`LISTEN` e o advisory lock de sessão do líder: as duas conexões dedicadas devem apontar direto
para o Postgres, ou o sistema cai para o poll (decisão 22).

**Decisão 21 — acrescentar:** No modo Postgres, o sync declarativo grava a linha em `schedules` e
faz o upsert do `schedule-tick` em `system_jobs` (no-op para o tick pendente quando `cron` e
`timezone` não mudaram, decisão 8); a remoção continua opt-in
(`pruneDeclarative`). O hook de reconciliação Postgres↔BullMQ vira a reconciliação
`schedules`↔`system_jobs` (linha sem tick → upsert; tick sem linha → remoção, com a mesma
re-checagem antes de remover).

**Decisão 22 — acrescentar:** No modo Postgres, as checagens do Redis não rodam e
`checkEnvironment` aceita `redis` ausente. Entra um autoteste de `LISTEN` no `start()`: `NOTIFY`
no canal de wake e espera de até 1 s pelo eco; sem eco, `warn` "notificações não chegam (pooler em
modo transação?); a latência de wake é o poll" e o sistema segue só com o poll.

**Decisão 28 (nova) — Transporte Postgres.** `createEngine({ transport: "postgres" })` monta o
backend com store SQL, transporte Postgres, pub/sub por `NOTIFY` e sem streams. Regras:
(a) claim por fila com `LIMIT` igual às vagas livres do worker deste processo, ordem
`run_after, created_at, id`, num statement só; a transação do claim termina antes de executar
(nenhum lock de linha é segurado durante a execução; a posse é `execution_id`, decisão 5);
(b) wake: todo enqueue de hoje (insert, retomada, retry, hand-back, recuperação de stall, colapso
de debounce), com ou sem atraso, vira `pg_notify(wake, '<fila>')`. No `trigger` com `tx` o
`NOTIFY` vai no próprio insert (entregue no commit); nos demais caminhos é emitido depois do
commit, pelo transporte (uma perda aqui só custa latência: o poll cobre). Um `NOTIFY` de run
atrasada faz o worker recalcular `min(run_after)`, então nenhum processo espera um intervalo
inteiro de poll por uma run que outro processo agendou mais cedo. O nome da fila no payload é
**dica**: o listener só acorda o worker daquela fila se ela está registrada, e qualquer payload
inesperado acorda todas — payload de `NOTIFY` é sempre tratado como não confiável (qualquer role
com `CONNECT` pode notificar e escutar qualquer canal); (c) poll adaptativo: após um claim vazio
o worker lê `min(run_after)` da fila e dorme até lá, limitado por `polling.intervalMs` (padrão
1 s), com jitter de até `polling.jitterMs` (padrão 50 ms) para diluir o rebanho de trovão dos
`NOTIFY`; (d) `pg_notify` de status e cancel carregam só `runId` (limite de 8000 bytes do
Postgres; nunca progresso), e o assinante relê `runs` antes de agir: para status, antes de emitir
(`runs.subscribe` mantém a mesma ordem obrigatória de hoje); para cancel, só aborta a execução
local se a run relida está `canceled` (um cancel forjado nunca derruba uma execução). As
publicações de um processo são agrupadas numa janela curta (5 ms) e saem num único statement
(`SELECT pg_notify(c, p) FROM unnest(...)`), para não pagar um commit com o lock global de
`NOTIFY` por transição; (e) `globalConcurrency` é exato: com limite, o claim primeiro toma
`pg_advisory_xact_lock(hashtextextended('<schema>:<fila>:global', 1))` (semente 1, para nunca
colidir com a chave da decisão 25 quando `concurrencyKey === "global"`) e conta os `running` da
fila uma vez (índice parcial `(queue) WHERE status = 'running'`), então claims concorrentes da
mesma fila se serializam e nunca passam do limite; sem limite, não há lock. Nos dois caminhos o
claim roda numa transação curta com `SET LOCAL statement_timeout` (limite no servidor, para um
claim nunca terminar depois do limite de dreno do `stop()`), e o "vencido" é avaliado com
`statement_timestamp()` (emenda aprovada pelo dono em 2026-09-26). O worker que termina uma execução numa fila com limite emite o `NOTIFY` de
wake dela (a vaga liberada acorda outros processos). Pausa (decisão 27) é lida de `queue_state` no
próprio claim; `scale` continua local; (f) `queue.rateLimit`, `ctx.stream` e `runs.readStream`
são `ConfigError` neste modo; (g) `stop()`: os laços de claim param de reivindicar, esperam o
claim em andamento terminar (as runs que ele trouxer entram no dreno e no hand-back), as execuções
drenam com `shutdownTimeoutMs`, o hand-back é `release` mais um `NOTIFY`, tudo limitado;
(h) Postgres indisponível: polls e claims falham com backoff e `warn` throttled (padrão do
listener do outbox); execuções em voo já viram `lost` pelo heartbeat; timeouts de conexão no
`start()` são `MinitriggerError` (infraestrutura transitória, como na quarta revisão externa),
nunca `ConfigError`; (i) `createTestEngine` não muda de comportamento, mas deixa de importar
`bullmq` em runtime (o `DelayedError` vira `SystemJobDelayed`, próprio da lib); (j) a decisão 26
neste modo é um caso particular da 1: `trigger` com `tx` é o mesmo insert com `NOTIFY` de wake no
`RETURNING` e `outbox_pending = false`; sem varredura de outbox.

## 3. Arquitetura

### 3.1 Fábrica de backend

`createEngine` já monta um `EngineBackend` (`store`, `transport`, `publisher`, `createPubSub`,
`streams`, `readStream`, `createRunInTransaction`, `onScaled`) e passa a `createEngineCore`.
O modo Postgres é uma segunda montagem desse mesmo objeto; o núcleo (`trigger`, `runs.*`,
executor, reconciler, retenção, schedules) não bifurca.

```
src/engine.ts               createEngine: escolhe a fábrica pelo config.transport; carrega
                            backends/bullmq.ts com import() dinâmico (nunca estático)
src/backends/bullmq.ts      (movido de engine.ts) monta o backend atual; imports estáticos de
                            bullmq/ioredis e dos módulos Redis (queue/, pubsub.ts, streams.ts) inalterados
src/backends/postgres.ts    monta o backend Postgres
src/pg/listener.ts          createPgListener({ channels, onNotify(channel, payload) }) — generaliza src/outbox.ts
src/pg/transport.ts         Transport sobre Postgres (claim loops, system_jobs, schedulers, pause, scale)
src/pg/workers.ts           laço de claim por fila + laço da -system; wake, poll adaptativo, stop
src/pg/pubsub.ts            PubSub/PubSubPublisher por NOTIFY (status, cancel)
src/pg/schedulers.ts        next occurrence com cron-parser; upsert/remove/list de schedule-tick
src/store/sql/claim.ts      claimRuns(queue, limit, ...) e claimSystemJobs(limit)
src/store/sql/system-jobs.ts SystemJobStore (upsert, claim, complete, delay, remove, list)
src/store/sql/schema-meta.ts marcador de transporte
src/migrations/009_postgres_transport.sql
```

`src/outbox.ts` passa a usar `createPgListener` (um canal); a engine BullMQ continua com o
listener do outbox como hoje, sem mudança de comportamento.

Regra de import: nada no grafo estático de `src/index.ts`, `src/testing/index.ts` ou dos
adaptadores importa `bullmq`/`ioredis` em runtime (`import type` é permitido). Hoje violam:
`src/queue/transport.ts` e `src/queue/connections.ts` (só alcançados via `engine.ts`, resolvido
pelo import dinâmico do backend), `src/executor/system.ts` e `src/testing/test-engine.ts`
(`DelayedError`, trocado por `SystemJobDelayed`). O `scripts/check-pack.mjs` prova a regra.

### 3.2 Tabelas e índices (migration 009)

```sql
CREATE TABLE system_jobs (
  job_id       text PRIMARY KEY,
  name         text NOT NULL,          -- waitpoint-expire | retention | schedule-tick | reconcile
  data         jsonb NOT NULL DEFAULT '{}'::jsonb,
  run_after    timestamptz NOT NULL,
  attempts     int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 1,
  locked_until timestamptz,
  locked_by    uuid,                   -- fencing: token do claim atual
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_system_jobs_due ON system_jobs (run_after);

CREATE TABLE schema_meta (
  key   text PRIMARY KEY,
  value text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_runs_claim ON runs (queue, run_after)
  WHERE status IN ('queued', 'waiting');
CREATE INDEX idx_runs_running_queue ON runs (queue) WHERE status = 'running';
```

`locked_until`: um job reivindicado recebe `locked_until = now() + stallTimeoutMs` e um
`locked_by` novo; se o processo morrer, outro worker o pega depois disso (o handler é idempotente:
expiração re-checa o waitpoint, retenção é `SKIP LOCKED`, tick colide na chave de idempotência).
Um handler mais lento que `stallTimeoutMs` (retenção grande) pode rodar duas vezes em paralelo;
é inofensivo pelos mesmos motivos, e o fencing por `locked_by` garante que só o dono atual
conclui/reagenda a linha. Sem `CONCURRENTLY` (decisão 16 e
precedente das migrations 005/006/008); README "Upgrading" ensina a pré-criar os dois índices de
`runs`.

### 3.3 Claim de runs (o statement central)

Sem `globalConcurrency` (transação curta: `SET LOCAL statement_timeout` + o statement abaixo;
emenda aprovada pelo dono em 2026-09-26):

```sql
BEGIN;
SET LOCAL statement_timeout = <min(1500, tempo restante do dreno do stop())>;
WITH picked AS (
  SELECT r.id FROM runs r
  WHERE r.queue = $1
    AND r.status IN ('queued', 'waiting')
    AND r.run_after <= statement_timestamp()
    AND NOT r.outbox_pending
    AND NOT EXISTS (SELECT 1 FROM queue_state q WHERE q.queue = r.queue AND q.paused)
  ORDER BY r.run_after, r.created_at, r.id
  LIMIT $2
  FOR UPDATE OF r SKIP LOCKED
)
UPDATE runs r SET status = 'running', execution_id = gen_random_uuid(),
  heartbeat_at = statement_timestamp(), started_at = <ver abaixo>, debounce_key = NULL,
  updated_at = statement_timestamp()
FROM picked WHERE r.id = picked.id
RETURNING r.id, r.task_id, ...;   -- RUN_COLUMNS qualificadas com r. (picked também tem id)
COMMIT;
```

`statement_timestamp()`, não `now()` (congelado no `BEGIN`): uma run que venceu, foi inserida ou
foi reenfileirada entre o `BEGIN` e o statement é vista como vencida, e o `updated_at`/`started_at`
gravados nunca ficam antes do `updated_at` que ela já tinha. O `stop()` passa ao claim em
andamento um prazo (o limite de dreno): se o prazo já passou quando a conexão sai do pool, nada é
reivindicado; o `statement_timeout` usa o tempo restante; e, se o prazo passou antes do `COMMIT`,
o claim faz `ROLLBACK`.

Com `globalConcurrency` (a mesma transação curta, serializada por fila):

```sql
BEGIN;
SET LOCAL statement_timeout = <idem>;
SELECT pg_advisory_xact_lock(hashtextextended('<schema>:<fila>:global', 1));
-- a mesma CTE acima com LIMIT least($2, $3 - (SELECT count(*) FROM runs
--   WHERE queue = $1 AND status = 'running')), contado uma vez (não por linha candidata)
COMMIT;
```

Observações:

- Espelha o `acquireRow` de hoje, com duas diferenças deliberadas: `enqueue_seq` não muda (não
  há job a invalidar; `release`/`retry`/`suspend` continuam incrementando como hoje) e
  `concurrency_waits` não é zerado (o backoff da decisão 25 cresce a partir dele; quem zera é a
  re-checagem abaixo, no caminho `ok`).
- `started_at`: `CASE WHEN r.concurrency_key IS NULL THEN coalesce(r.started_at,
  statement_timestamp()) ELSE r.started_at END`. Uma run com chave só "começa" depois da re-checagem `ok`, que grava
  `coalesce(started_at, now())`; uma run devolvida por `limited` não ganha `started_at`.
- `globalConcurrency` é exato (o advisory lock serializa os claims da fila e o `count` roda com
  snapshot novo sob READ COMMITTED). O lock é de transação: nunca fica preso a uma conexão.
  Chave de um bigint, fora do espaço `(hashtext(schema), 1|2)` da decisão 16, como o lock da
  decisão 25; semente 1 (a decisão 25 usa semente 0), para uma `concurrencyKey` `"global"` nunca
  produzir a mesma chave.
- `concurrency_key` (decisão 25) continua no caminho do executor: o claim não checa a chave; logo
  depois do claim, `recheckConcurrencyLimit` (no mesmo `FOR UPDATE` + advisory lock de
  `acquireWithConcurrencyLimit`) conta os `running` da chave **excluindo a própria run**. Se
  `>= limit` → `status = 'queued'`, `execution_id = NULL`, `run_after = now() +
  concurrencyLimitedDelayMs(concurrency_waits)` (valor pré-incremento), `concurrency_waits + 1`,
  sem gastar tentativa nem publicar status (o poll a encontra de novo). Se `ok` →
  `concurrency_waits = 0`, `started_at = coalesce(started_at, now())`. Dois claims concorrentes
  da mesma chave nunca ficam ambos `limited` para sempre: o advisory lock serializa as
  re-checagens e a segunda vê a primeira já devolvida. Uma run com chave numa fila que este
  processo não conhece como limitada passa pela mesma função com `limit` ausente (só grava
  `started_at`/zera `concurrency_waits`).
- O `execution_id` é gerado no banco para o claim ser um statement; o executor recebe o registro
  completo e segue como depois do `acquire`.

### 3.4 Laço de worker por fila

```
loop:
  livres = concurrency - emVoo
  se livres == 0: esperar slot liberar
  rows = claimRuns(queue, livres, globalConcurrency)
  para cada row: executar (executeRun com `claimed: row`, sem novo acquire)
  se rows.length == livres: continuar sem dormir
  senão:
    prox = SELECT min(run_after) FROM runs WHERE queue=$1 AND status IN (...) AND run_after > now()
    dormir até min(prox, now + polling.intervalMs) + jitter, ou até wake(queue)
```

`wake(queue)` é alimentado pelo listener (payload validado contra o registro; se não bate, acorda
todas as filas). Várias notificações durante um claim em andamento colapsam num único "rodar de
novo" (mesmo padrão das varreduras agrupadas do outbox). Com o worker cheio (`livres == 0`), um
wake não faz query. O executor precisa de um ponto de entrada que aceite a run já reivindicada:
hoje `executeRun` faz o `acquire` a partir de `RunJobData { runId, seq }`. Decisão: `RunJob`
ganha `claimed?: RunRecord`; com ele, o executor pula `acquire`, chama `recheckConcurrencyLimit`
quando a run tem `concurrency_key` (`limited` → retorna sem mais nada) e entra no mesmo caminho de
hoje (`isShuttingDown` → hand-back, `ExecutionState`, `runAcquired`). Tudo o que vem depois
(heartbeat, steps, escritas terminais, hand-back) é idêntico nos dois modos.

Parada: `stopClaiming()` impede novos claims; `drain()` espera primeiro o claim em andamento (as
runs que ele trouxer são lançadas e, como `isShuttingDown()` já é verdadeiro, devolvidas pelo
próprio executor), depois as execuções, até o timeout. Nada é reivindicado depois do sinal de
parada, e nenhuma run reivindicada fica fora do dreno.

### 3.5 `-system`

Um laço igual ao de fila sobre `system_jobs` (`concurrency = systemConcurrency`), entregando ao
`createSystemDispatcher` um objeto de job mínimo (`id`, `name`, `data`, `opts.prevMillis`,
`timestamp`, `token` = `locked_by`, `repeatJobKey`, `moveToDelayed`) como o do transporte em
memória. Toda escrita do worker sobre a linha é condicionada a `job_id = $1 AND locked_by = $2`
(fencing); se nada casou, o lock foi perdido para outro worker e o resultado é descartado com
`debug`.

- `moveToDelayed(ts)` + `SystemJobDelayed` (caso `not_due` da expiração) = `UPDATE system_jobs
  SET run_after = now() + delay, locked_until = NULL, locked_by = NULL`. `SystemJobDelayed` é
  uma classe própria (`src/executor/system.ts`); o worker BullMQ a converte em `DelayedError`.
- Sucesso = `DELETE`, exceto `schedule-tick` (abaixo). Falha = `attempts + 1`; abaixo de
  `max_attempts` → `run_after = now() + backoff exponencial`; senão `DELETE` com `logger.error`
  (só `schedule-tick` tem `max_attempts = 5`, como hoje, e não é apagado).
- `schedule-tick`: o dispatcher cria a run com `__schedule:id:prevMillis`. Ao concluir **ou**
  esgotar as tentativas, o worker faz um único `UPDATE` fenced que regrava a linha para a próxima
  ocorrência depois do `now()` do banco (`run_after`, `data.prevMillis`, `attempts = 0`, lock
  solto), calculada com o `cron`/`timezone` gravados na linha. Reentrega do mesmo tick (crash
  entre criar a run e regravar a linha) cai na chave e vira no-op.
- `retention-<janela>`: o líder faz `INSERT ... ON CONFLICT DO NOTHING`.
- `reconcile`: mantido para paridade do dispatcher.

### 3.6 Schedules

`upsertJobScheduler` = `next = nextOccurrence(cron, tz, now_do_banco)` → `INSERT` em
`system_jobs` (`schedule-tick:<id>`, `data: { scheduleId, cron, timezone, schema, prevMillis }`,
`run_after = next`, `max_attempts = 5`) com `ON CONFLICT (job_id) DO UPDATE ... WHERE
system_jobs.data->>'cron' IS DISTINCT FROM EXCLUDED.data->>'cron' OR ...'timezone'...`: igual ao
BullMQ, regravar o mesmo padrão é no-op para o tick pendente (senão cada `start()` pularia um tick
vencido e ainda não processado). Quando o padrão muda, a linha é regravada com o lock solto
(`locked_by = NULL`), e o worker que estivesse processando o tick antigo perde o fencing.
`getJobScheduler`/`listJobSchedulers` leem essas linhas; `removeJobScheduler` apaga (inclusive
travada: é remoção do scheduler, não de um job ativo; o tick em andamento termina e sua escrita
fenced não casa). `processScheduleTick` não muda (usa `job.repeatJobKey` e `job.opts.prevMillis`).
A tag de schema em `data.schema` fica para o código compartilhado (tabela por schema, então nunca
dispara aqui). A regex de validação de cron de `src/schedules.ts` passa a delegar ao
`cron-parser` nos dois modos (validação real, mesma biblioteca que o BullMQ usa).

### 3.7 Pub/sub

`src/pg/pubsub.ts`: `publishStatus({ runId })` e `publishCancel(ids)` entram num buffer do
publicador; a cada 5 ms (ou 500 itens) o buffer sai num único `SELECT pg_notify(c, p) FROM
unnest($1::text[], $2::text[]) AS t(c, p)`, deduplicado por `(canal, payload)`. Falha do envio:
`warn` throttled, publicações descartadas (pub/sub é at-most-once, como hoje). `close()` esvazia o
buffer, limitado. O assinante (`createPubSub`) recebe do listener e valida que o payload é UUID:

- `run-status` (`subscribeStatus`): só se há handler para esse `runId`, faz `store.runs.get` e
  emite `RunStatusMessage` completo (status, `updatedAt`, progresso do banco). A rede de segurança
  de 15 s do `runs.subscribe` continua.
- cancel (`onCancel`): relê as runs notificadas que estão ativas neste processo e só repassa aos
  handlers as que estão `canceled` no banco. Um `NOTIFY` forjado com um `runId` real (os ids são
  visíveis a qualquer role que dê `LISTEN` no canal de status) não aborta nada.

### 3.8 Ciclo de vida

`start()`: `checkEnvironment` (sem Redis) → `schema_meta` (`transport`, recusa modo misto) →
`syncDeclarativeSchedules` → `syncQueueStates` (só Postgres; sem espelho) → listener `LISTEN`
(limitado a 5 s; falha = `MinitriggerError`, engine reiniciável) → autoteste de `NOTIFY` (1 s, só
`warn`) → workers → líder. `stop()`: laços param de reivindicar e esperam o claim em andamento →
drenagem (`shutdownTimeoutMs`) → hand-back (`release` + `NOTIFY`) → publicador esvaziado → líder
e listener fechados (limitados) → assinaturas encerradas → `closed` → `pool.end()` limitado.
Mesmas guardas `assertNotStopped`/`assertNotClosed`/`assertStartNotAbandoned`.

`schema_meta.setIfAbsent` sob corrida: o `INSERT ... ON CONFLICT DO NOTHING` espera a transação
concorrente; se não inseriu, o valor efetivo é relido num statement novo (nunca presumido).

### 3.9 Reconciler e retenção

`createReconciler` ganha `recoverLostEnqueues: boolean` (false no modo Postgres: casos 1 e 2
pulados; o relatório traz `skippedLostEnqueues: true`). Casos 3 (órfã `waiting` sem `run_after`),
4 (stall), 5 (expirações) e os hooks de filas/schedules continuam; o caso 5 remove o job de
`system_jobs` em vez de do BullMQ (mesma interface `removeSystemJob`). Retenção: o líder insere
`retention-<janela>` em `system_jobs`.

### 3.10 Config

União discriminada por `transport`, para o compilador pegar a combinação errada:

```ts
type EngineConfig = CommonConfig & (
  | { transport?: "bullmq"; redis: string | { connection: RedisLike }; prefix?: string;
      outbox?: OutboxConfig; polling?: never }
  | { transport: "postgres"; polling?: { intervalMs?: number; jitterMs?: number };
      redis?: never; prefix?: never; outbox?: never }
);
```

`polling`: 1000 / 50 ms; mínimos 100 / 0. Em runtime, as mesmas recusas viram `ConfigError`
(JS puro ou cast). `checkEnvironment({ redis? })`. `EngineLimits`, `reconciler`, `retention`
inalterados.

## 4. Tratamento de erros (cenários e comportamento esperado)

| Cenário | Comportamento |
|---|---|
| `NOTIFY` perdido (listener reconectando, pooler, falha do publicador) | Poll adaptativo pega a run em até `polling.intervalMs`; `warn` throttled na reconexão. |
| Listener travado sem ler | Keepalive + reconexão do client dedicado; sem isso a fila de NOTIFY do cluster enche e COMMITs falham (documentado). |
| Postgres fora | Claim/poll com backoff 0,5–30 s e `warn` throttled; execuções em voo viram `lost` pelo heartbeat; `stop()` continua limitado. |
| Postgres fora no `start()` | Timeout do `LISTEN` (5 s) → `MinitriggerError`; engine reiniciável. |
| Processo morre com run `running` | Caso 4 do reconciler (stall) como hoje. |
| Processo morre com job da `-system` reivindicado | `locked_until` vence; outro worker repete com `locked_by` novo; handlers idempotentes; escritas tardias do primeiro não casam o fencing. |
| `schedule-tick` esgota as tentativas | `logger.error`; a linha é regravada para a próxima ocorrência (o schedule nunca some por falha). |
| Dois processos reivindicam a mesma run | Impossível: `SKIP LOCKED` + `status` no predicado. |
| Engine BullMQ e Postgres no mesmo schema | `ConfigError` no `start()` via `schema_meta`, inclusive com os dois `start()` simultâneos. Engines 0.3 não gravam o marcador: atualizar todas para 0.4+ antes de trocar (documentado). |
| `prefix`/`redis`/`outbox`/`rateLimit`/streams no modo Postgres | Erro de tipo e `ConfigError` na construção ou na chamada. |
| Payload de `NOTIFY` forjado | Wake: acorda todas as filas (custo de um claim vazio); status: `runId` inválido é ignorado, válido só dispara uma releitura; cancel: relê e só aborta run `canceled` no banco. |
| Cluster sem líder | Retenção e reconciler não rodam; runs e ticks de cron continuam (qualquer worker `-system` reivindica `system_jobs`). Documentado. |
| Cron inválido | `cron-parser` lança na validação de `defineTask`/`schedules.upsert` (`ConfigError`), nos dois modos. |
| `globalConcurrency` com várias engines | Exato: claims da fila serializados pelo advisory lock de transação. |
| Claim demora (lock de linha de um cancel/resolvedor) | `SKIP LOCKED` pula a linha; volta no próximo ciclo. |
| `stop()` durante claim em andamento | O dreno espera o claim; as runs que ele trouxer são devolvidas pelo executor (`isShuttingDown`); nada é reivindicado depois do sinal de parada. |

## 5. Segurança

- Nenhum SQL novo com string de usuário: fila e chaves já validadas; `job_id` interno.
- Canais derivam do schema validado; payload de `NOTIFY` nunca é interpretado além de dica/UUID,
  e nunca dispara uma ação sem releitura do banco (cancel só aborta run `canceled`).
- Qualquer role com `CONNECT` no database pode dar `LISTEN` nos canais e ver `runId`s (nunca
  payloads, outputs ou progresso). Documentar; quem precisa isolar usa um database próprio.
- `system_jobs.data` só de código interno; `schedule-tick` lê `payload`/`task` de `schedules`,
  nunca do job (o `cron`/`timezone` gravados no job só calculam a próxima ocorrência e são
  reparados a partir de `schedules` como hoje).
- Marcador de modo protege contra duas fontes de verdade da `-system`.
- Advisory locks continuam globais ao cluster por hash (risco pré-existente, documentado).
- `pg_notify` exige só `CONNECT`; documentar que a role da aplicação não precisa de grants extras.

## 6. Desempenho

- Índices novos: `idx_runs_claim` (o claim é um probe por fila), `idx_runs_running_queue`
  (`globalConcurrency`), `idx_system_jobs_due`.
- Sem escrita extra por run: o claim substitui o `UPDATE` do `acquire`. Polls só leem.
- `NOTIFY` agrupado: cada `NOTIFY` fora de transação é um commit que toma o lock global da fila
  de notificações. Sem agrupamento, uma run custaria ~4 commits desses (wake, `queued`,
  `running`, `completed`, mais progresso). O publicador junta tudo de uma janela de 5 ms num
  statement; o wake do `trigger` com `tx` já vai no insert.
- Latência: wake em ms com `NOTIFY`; retomada por tempo (sleep/backoff/delay) em `min(run_after)`
  com precisão de jitter, sem poll apertado; um `NOTIFY` de run atrasada faz todos os workers da
  fila recalcularem `min(run_after)`.
- Rebanho de trovão: um claim por fila registrada por processo por `NOTIFY`, com jitter e
  colapso de notificações repetidas; worker cheio não consulta.
- Meta de aceitação: `pnpm bench` nos dois modos; o modo Postgres deve ficar dentro de ±20 % da
  vazão do BullMQ e com p99 de retomada por sleep ≤ 150 ms. Medido cedo (logo depois do backend
  Postgres existir), antes de converter a suíte de integração. Sem isso, não se discute trocar o
  padrão.
- `system_jobs` tem churn alto: linhas apagadas ao concluir; documentar autovacuum.

## 7. Testes

- Contrato da store: `claimRuns` (ordem, `LIMIT`, pausa, `globalConcurrency` exato com claims
  concorrentes, `outbox_pending`, `waiting` com `run_after`, `started_at`, `concurrency_waits`
  intacto, `enqueue_seq` intacto), `recheckConcurrencyLimit` (backoff crescente), `SystemJobStore`
  (dedupe por `job_id`, `locked_until`, fencing por `locked_by`, delay, attempts, exaustão,
  reagendamento de tick, upsert de tick no-op sem mudança), `schema_meta` (inclusive corrida).
- Unitários: `createPgListener` (reconexão, canais, `close()` limitado), `pg/workers.ts` (poll
  adaptativo com relógio falso, wake, jitter, parada com claim em andamento), `pg/schedulers.ts`
  (próxima ocorrência com timezone, DST), pub/sub por NOTIFY (payload inválido ignorado, cancel
  forjado ignorado, agrupamento), config (`transport`, recusas, tipos da união).
- Integração parametrizada por transporte (`describe.each(["bullmq","postgres"])`) para:
  `engine`, `steps`, `sleep`, `subtasks`, `tokens`, `retry`, `stall`, `stop`/hand-back,
  `schedules`, `queues` (pausa), `batch-ops`, `outbox` (`tx` nos dois modos), `subscribe`,
  `progress`, `debounce-concurrency`. Os arquivos que dependem de Redis (`stream`, `flushall`,
  `worker-close-spin`) ficam só no BullMQ.
- Resiliência Postgres: `pg_terminate_backend` do listener (reconecta, poll cobre), Postgres
  reiniciado durante execução (runs viram `lost` e são recuperadas), dois processos disputando a
  mesma fila (nenhuma run executada duas vezes, contagem por `execution_id`; pico de `running`
  nunca acima de `globalConcurrency`), cancel forjado com `runId` real.
- Modo misto recusado; `prefix`/`redis`/`outbox`/`rateLimit`/stream → `ConfigError`.
- `scripts/check-pack.mjs`: instalar o pacote num diretório sem `bullmq`/`ioredis`, importar a
  entrada principal e `/testing` (ESM e CJS), compilar tipos (`skipLibCheck: false`) e subir uma
  engine Postgres real.
- `pnpm bench --transport postgres`.

## 8. Documentação

README: seção "Postgres-only mode" (quando usar, o que não entra, conexões, PgBouncer, autovacuum,
visibilidade de `runId`s por `LISTEN`, upgrade com pré-criação dos índices, rollout a partir de
engines 0.3), tabela de config, "Upgrading to 0.4.0" (migration 009, peers opcionais, cron
validado de verdade). `docs/pt` espelhado. `docs/store-contract.md` (claim, `system_jobs`,
`schema_meta`). `docs/HANDOFF.md` e `CLAUDE.md` (resumo da decisão 28, notas de implementação).
SPEC seção 2 com as emendas da seção 2 deste documento, seção 3 com um exemplo `transport:
"postgres"`, seção 4 com `src/backends`, `src/pg`.

## 9. Fora de escopo e riscos aceitos

- Streams por tabela (só se houver demanda).
- Rate limit global em SQL.
- Trocar o padrão de transporte.
- Latência de retomada por tempo depende do jitter e do relógio do processo entre polls; aceito.
- `NOTIFY` de wake fora do caminho `tx` sai depois do commit (não é atômico com a escrita); uma
  perda só custa latência até o poll. Embutir o `NOTIFY` em cada transição da store fica como
  otimização a medir com o bench.
- Um tick de cron em backoff atrasa os ticks seguintes do mesmo schedule; aceito.
