# Handoff — estado do projeto e próximos passos

Atualizado em 2026-09-27 (branch `feat/postgres-transport`, quinta revisão externa, blocos A/B/C
aplicados; antes: 2026-09-26, Task 14 — documentação e handoff do transporte Postgres). Leia junto com `CLAUDE.md` (decisões e notas de implementação),
`docs/SPEC.md` (fonte da verdade) e `docs/PLAN.md` (grafo de tarefas e modelos); para o histórico
detalhado tarefa a tarefa desta feature (13 tarefas, quatro revisões externas), ver
`.superpowers/sdd/2026-09-26-postgres-transport/progress.md`.

## Estado atual

- **Fases 1 a 10 concluídas.** `pnpm check:pack` ok (última vez rodado); `examples/basic` roda de ponta a ponta.
- **Terceira revisão externa (2026-09-25):** aplicada e commitada (ver `CLAUDE.md`, "Terceira
  revisão externa"); testes novos em `test/{unit,integration}/review3-{engine,io}.test.ts`.
- **Quarta revisão externa (2026-09-25):** aplicada, ainda não commitada nesta sessão (`src/engine.ts`,
  `src/index.ts`, `src/telemetry.ts`, `src/testing/run-task.ts` + testes novos/ajustados em
  `test/{unit,integration}/review4-engine.test.ts`, `test/unit/run-task.test.ts`,
  `test/integration/review3-engine.test.ts`); ver `CLAUDE.md`, "Quarta revisão externa".
- **Migration 008 (`idx_runs_active_created`):** índice parcial para `runs.cancelMany()`; ver
  `CLAUDE.md`. Migrations 001–008 congeladas depois da publicação da 0.3.0.
- **Verificação completa (2026-09-25, antes do transporte Postgres):** `pnpm typecheck && pnpm lint
  && pnpm test` verde — 1416 testes (unit + integration) + 3 no projeto flushall; `pnpm check:pack`
  e `pnpm skills:validate` ok.
- **Verificação completa (2026-09-26, fim da Task 14, branch `feat/postgres-transport`):**
  `pnpm typecheck && pnpm lint && pnpm test && pnpm build && pnpm check:pack` verde — 106 arquivos
  de teste, **1762 testes passando + 14 pulados** (1776 coletados) nos projetos `unit`+`integration`
  em ~35 s, mais **3 testes passando** no projeto `flushall` isolado (~5 s); `pnpm build` e
  `pnpm check:pack` (agora com a checagem sem Redis do transporte Postgres, ver "Transporte
  Postgres" no `CLAUDE.md`) verdes. `pnpm bench` e `pnpm bench -- --transport postgres` rodados uma
  vez cada nesta sessão — números somados à tabela do `CLAUDE.md` ("Benchmark").
- **Verificação completa (2026-09-26, pendências pós-revisão final):** `pnpm typecheck && pnpm lint
  && pnpm test && pnpm check:pack` verde — **1780 testes passando + 14 pulados** (106 arquivos) em
  `unit`+`integration`, 3 no `flushall`; `pg-engine` e `pg-resilience` 3× seguidas verdes.
  `pnpm bench -- --transport postgres`: 1360 runs/s, sleep p99 29 ms, token p99 10,1 ms.
- **Verificação completa (2026-09-27, fim do bloco C da quinta revisão externa):** `pnpm typecheck
  && pnpm lint && pnpm test && pnpm check:pack` verde — 121 arquivos, **1851 testes passando + 14
  pulados** (1865 coletados) em `unit`+`integration`, 3 no `flushall`; `check:pack` PASS (inclusive
  o diretório sem Redis). Arquivos de teste alterados/novos do bloco C (`pg-engine`,
  `pg-resilience`, `pg-db-unreachable`, `migrate-009-precreated`) 3× seguidas verdes. Bench:
  Postgres 1629 runs/s, sleep p99 61 ms, token p50 8,4 ms; BullMQ 1353 runs/s, sleep p99 118 ms,
  token p99 5,0 ms (tabela no `CLAUDE.md`).

### O que existe (resumo)

| Área                             | Arquivos principais                                                                                                              |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Base, utilitários                | `src/utils/*`, `src/errors.ts`                                                                                                   |
| Schema e migrations              | `src/migrations/001_init.sql` … `008_runs_active_created_idx.sql` (001–008, congeladas depois da publicação da 0.3.0), `009_postgres_transport.sql` (`system_jobs`, `schema_meta`, `idx_runs_claim`, `idx_runs_running_queue` — decisão 28), `migrate.ts` |
| Store (contrato + SQL + memória) | `src/store/types.ts`, `src/store/sql/*`, `src/store/memory/*`, `docs/store-contract.md`                                          |
| Tasks e filas                    | `src/task.ts`, `src/registry.ts`, `src/queue/*`                                                                                  |
| Executor                         | `src/executor/*` (aquisição, heartbeat, steps, suspensão, tokens, subtasks, logs, notificação)                                   |
| Engine                           | `src/engine.ts`, `src/config.ts`                                                                                                 |
| Resiliência                      | `src/leader.ts`, `src/reconciler.ts`, `src/retention.ts`, `src/pubsub.ts`                                                        |
| Logs e streams                   | `src/logs/buffer.ts`, `src/streams.ts`                                                                                           |
| Outbox transacional (decisão 26) | `src/outbox.ts`, `createSqlTxRunStore` em `src/store/sql/runs.ts`, `dispatchOutboxBatch` em `src/engine.ts` |
| Adaptadores de ORM               | `src/adapters/{typeorm,knex,drizzle,prisma,placeholders,shared}.ts`, subpaths `@rivia-labs/minitrigger/{typeorm,knex,drizzle,prisma}` |
| Modo de teste / helpers          | `src/testing/{test-engine,memory-transport,memory-pubsub,helpers,run-task}.ts`, subpath `@rivia-labs/minitrigger/testing` |
| Telemetria                       | `src/telemetry.ts` (`attachDefaultLogger`), `durationMs`/`startedAt` em `src/executor/notify.ts`/`src/engine.ts` |
| Controle de filas / lote         | `engine.queues.*`, `runs.cancelMany`/`retryFailed` em `src/engine.ts`; `queue_state` (migration 007) |
| Transporte Postgres (decisão 28) | `src/backends/{postgres,bullmq,lazy,shared,types}.ts` (fábricas de backend + `import()` dinâmico do BullMQ via `lazy.ts`), `src/pg/{listener,notify-batcher,pubsub,schedulers,transport,workers}.ts`, `src/store/sql/{claim,system-jobs,schema-meta}.ts`, migration 009; ver "Transporte Postgres (decisão 28, migration 009)" no `CLAUDE.md` |
| Testes                           | `test/unit`, `test/integration`, `test/integration-flushall`, `test/helpers` (namespace por arquivo, suíte de contrato de store, `engineConfigFor` parametrizado por transporte), `test/{unit,integration}/review3-{engine,io}.test.ts` (terceira revisão externa); testes do transporte Postgres espalhados por `test/unit`/`test/integration` (`describe.each(["bullmq","postgres"])` na maior parte dos arquivos compartilhados) e casos dedicados de resiliência Postgres |
| Docs                             | `README.md` (seção "Postgres-only mode", "Upgrading to 0.4.0"), `docs/pt/*` (inclui `14-modo-postgres.md`), `examples/basic` |

### Decisões tomadas com o dono do projeto

- Biome (config do dono) no lugar de ESLint/Prettier.
- Decisão 3: **só waitpoints aguardados bloqueiam a retomada** (token criado sem `waitForToken` não segura um `sleep`).

## O que falta

### Próximos passos (fora das fases do kickoff)

- ✅ **v0.1.0 publicado** no repositório privado `igorabreu29/minitrigger` (GitHub Release com o `.tgz`). A partir daqui, mudanças de schema só em migrations novas (001–003 não podem mais ser editadas). Novo release: subir `version`, `pnpm check:pack`, tag `vX.Y.Z`, `gh release create` com o `.tgz`.
- Opcional: `TestEngine.start()` que drena em segundo plano (hoje o test engine só roda quando o teste chama `drain`/`runInline`/`drive`).

### Pontos em aberto registrados (sem urgência, comportamento documentado)

- ✅ Token que expirou sozinho agora reporta `timed_out`; `run_finished` só quando o timeout veio da saída terminal da run (SQL: `resolved_at = finished_at` na mesma transação; memória: flag `timedOutByRunExit`).
- ✅ Task que lança com steps em voo: o executor espera os steps terminarem (sem abortar) antes de gravar falha/retry; o retry reaproveita os que concluíram. Erro fatal não espera.
- Filtros de data de `runs.list` se chamam `createdFrom`/`createdTo` (nomes mantidos, decisão do dono).
- ✅ `step.trigger*` aceita task filha não registrada (spec sai do próprio objeto); objeto malformado, fila inválida e payload de filha inválido são fatais.
- ✅ `sideEffects: false` deixou de ser o desfecho: o modo de teste (`createTestEngine`) foi movido para o subpath `@rivia-labs/minitrigger/testing` (fase 9.2), com `splitting: true` no tsup para as duas entradas compartilharem o chunk das classes de erro (`instanceof ConfigError`/etc. funciona vindo de qualquer uma). Quem nunca importa `/testing` não paga pelo backend em memória no bundle da entrada principal; ver `CLAUDE.md`, "Fase 9.2".

### Parte 2 (2026-09-25, depois do v0.1.0)

- Issue #1 (progresso por run, decisão 23, migration 004) e issue #2 (debounce e concurrency key, decisões 24/25, migration 005) implementadas. Debounce sempre trailing (`mode` removido por decisão do dono); runs limitadas por concurrency key esperam com backoff de 1 s até 30 s, sem ordem FIFO.
- Bug do BullMQ 6.3.8/6.3.9 contornado (laço sem espera ao fechar o worker); reportado em taskforcesh/bullmq#4831.
- Segunda revisão externa aplicada (debounce preso no deadline, corrida do líder no close, tipo do `progress.set`, limite da espera por steps em voo e itens baixos); notas no `CLAUDE.md`.
- ✅ **v0.2.0 publicado** (GitHub Release com o `.tgz`). Migrations 001–005 congeladas: mudanças de schema só em migrations novas.
  **Antes de rodar `engine.migrate()` num deploy existente com uma tabela `runs` grande, ler
  "Upgrading to 0.2.0" no README:** a migration 005 cria dois índices com `CREATE INDEX` simples
  (não `CONCURRENTLY` — impossível dentro da transação única que `migrate()` usa para todo o lote),
  o que bloqueia escrita em `runs` (inclusive aquisição/heartbeat/conclusão de run) pelo tempo de
  build do índice. O README tem o SQL exato para pré-criar os dois índices com `CONCURRENTLY`
  fora de uma transação e marcar a migration 5 como aplicada em `schema_migrations` diretamente
  (as duas `CREATE INDEX` de 005 não têm `IF NOT EXISTS`, então só pré-criar os índices e rodar
  `migrate()` normalmente FALHARIA com "relation already exists" e desfaria o lote inteiro).

### Próximas ondas (decididas pelo dono em 2026-09-25)

1. ✅ **v0.3.0 publicado no npm** como `@rivia-labs/minitrigger` (tag `v0.3.0`, GitHub Release com o `.tgz`; `v0.2.0` e anteriores ficam só como a API antiga, sem escopo). Propostas da comparação com `@rivia-labs/oban-ts`, por valor: outbox transacional (`trigger(..., { tx })`), pacote `@rivia-labs/minitrigger` público (LICENSE MIT, subpath `/testing`, Node >= 22), gestão de filas e operações em lote, helpers de teste, telemetria (`durationMs`, `attachDefaultLogger`), skills de Claude Code e docs em PT.
2. ✅ Adapters de ORM para o outbox (`/typeorm`, `/knex`, `/drizzle`, `/prisma`), testados contra TypeORM 1.1, Knex 3.3, Drizzle 0.45 (node-postgres e postgres.js) e Prisma 7.10.

3. ✅ **Modo só-Postgres** (`transport: "postgres" | "bullmq"`, BullMQ continua padrão) — implementado no branch `feat/postgres-transport` (13 tarefas + 4 revisões externas, decisão 28, migration 009); ainda **não mesclado em `main` nem publicado**. Ver "Transporte Postgres (decisão 28, migration 009)" no `CLAUDE.md`, [Postgres-only mode](../README.md#postgres-only-mode) no README e `docs/pt/14-modo-postgres.md`. Meta de paridade de `pnpm bench` batida (Task 10d/14, tabela no `CLAUDE.md`) sem qualquer otimização — vazão do modo Postgres ficou acima do BullMQ, não só dentro de ±20%.

### Auditoria externa (2026-09-25)

- 13 achados + itens baixos, todos confirmados e corrigidos (resumo em `CLAUDE.md`, "Auditoria externa"). Único ajuste de decisão: `:` reservado em nomes de step (decisão 11, autorizado pelo dono).
- Conhecido e não tratado: `{}` como tipo de payload/output passa no `Serializable` (atalho `[T] extends [Json]` evita TS2589).

### Concluído nesta sessão (2026-09-25)

- Notificador sem `runs.get` extra por transição (`test/integration/notify-perf.test.ts`).
- Fase 8: `src/schedules.ts` (API, tick com idempotência por ocorrência e retries, sync declarativo com guarda de `source`, reconciliação bidirecional com tag de schema).
- Fase 9: `checkEnvironment` no `start()`; `createTestEngine` (`src/testing/`); mensagens de erro acionáveis; inferência de `step.run` com união.
- Fase 10: `examples/basic` completo, README final, `pnpm bench`, `pnpm check:pack`, `docs/research/github-install.md`.
- Menores: migration 003 (`idx_runs_created`), argumentos inválidos de step fatais, flake do teste de retenção.

### Task 10d (2026-09-26): bench antecipado dos dois transportes

`bench/run.ts` ganhou `--transport postgres|bullmq` (padrão `bullmq`; `postgres` não usa
`redis`/`prefix`, `transport: "postgres"` explícito, limpeza só `DROP SCHEMA`, sem obliterate).
Três execuções de cada, máquina/infra da nota "Benchmark" do `CLAUDE.md` (Apple M5 Pro, Postgres
16-alpine + Redis 7-alpine locais, `pnpm bench` builda antes):

| Métrica | BullMQ #1 | BullMQ #2 | BullMQ #3 | Postgres #1 | Postgres #2 | Postgres #3 |
|---|---|---|---|---|---|---|
| Vazão (1.000 runs, 3 steps) | 1312.9 runs/s | 1333.6 runs/s | 1280.3 runs/s | 1512.4 runs/s | 1401.1 runs/s | 1499.6 runs/s |
| Sleep resume p50 | 105.0 ms | 43.0 ms | 98.0 ms | 26.0 ms | 13.0 ms | 48.0 ms |
| Sleep resume p99 | 108.0 ms | 45.0 ms | 100.0 ms | 30.0 ms | 17.0 ms | 50.0 ms |
| Token resume p50 | 5.3 ms | 5.2 ms | 5.4 ms | 8.8 ms | 9.2 ms | 9.4 ms |
| Token resume p99 | 7.1 ms | 7.0 ms | 8.3 ms | 15.7 ms | 11.2 ms | 17.2 ms |

**Meta da spec §6 batida sem qualquer otimização:** vazão do modo Postgres ficou **acima** da do
BullMQ nas três execuções (não só dentro de ±20 %), e o p99 de retomada por sleep (17–50 ms) ficou
bem abaixo do teto de 150 ms — na verdade mais rápido que o BullMQ (45–108 ms), porque a retomada
por `NOTIFY` evita o `moveToDelayed`/polling de fila atrasada do Redis. Retomada por token ficou um
pouco mais lenta que o BullMQ (11–17 ms vs. 7–8 ms) mas irrelevante frente à meta (só a vazão e o
sleep têm meta numérica) — plausível pelo `SELECT pg_notify(...)` do publicador em vez do `publish`
direto do Redis, não investigado (a meta não pede).

**Decisão: nenhum diagnóstico/otimização necessário** (o passo 2 da tarefa só entra se a vazão
ficar fora da meta). Nenhuma mudança em `src/` nesta tarefa. `bench/run.ts` é o único arquivo de
código alterado.

### Task 14 (2026-09-26): documentação e handoff do transporte Postgres

Última tarefa do plano `docs/superpowers/plans/2026-09-26-postgres-transport.md` (branch
`feat/postgres-transport`, código já implementado e revisado nas tarefas 1–13). Só documentação:
README (seção "Postgres-only mode", "Upgrading to 0.4.0", tabela de config, exports, debounce,
benchmark, contributing), `docs/pt/*` (novo `14-modo-postgres.md`, espelhando `03-configuracao.md`,
`07-filas.md`, `11-producao.md`, `02-instalacao.md`, `13-referencia-api.md`, `README.md`),
`docs/store-contract.md` (revisado — já estava correto e completo desde a Task 3, nenhuma mudança
necessária), este arquivo e `CLAUDE.md` (bloco "Transporte Postgres (decisão 28, migration 009)" +
linha de fase + tabela de benchmark dos dois transportes). Bench rodado uma vez para cada
transporte nesta sessão (números na tabela do `CLAUDE.md`, consistentes com os da Task 10d).

**Próximos passos antes do merge para `main`:**

- **Mesclar o branch e publicar como 0.4.0** (ou a versão que estiver em `package.json` na hora),
  depois de uma revisão final humana da branch inteira (13 tarefas de código + esta de docs) —
  ainda não mesclado nesta sessão.
- **Pendências pós-revisão final resolvidas (2026-09-26):** os dois "minor (deferred)" das Tasks 9
  e 12 (re-checagem que lança → `release`; teste do cancel forjado sincronizado) e as cinco
  pendências da revisão final: claim grava `statement_timestamp()`; claim inteiro limitado pelo
  prazo do dreno do `stop()` (checkout, `statement_timeout`, `ROLLBACK` em vez de `COMMIT`);
  re-checagem que falha sempre devolve a run com backoff crescente, sem wake nem publicação, com
  `error` throttled; `start()` repetido chama `reconnectNow()` do listener; `schedules.upsert`/
  `delete` carregam o backend BullMQ antes de gravar. Emenda da decisão 28(e) (semente 1 do lock
  do `globalConcurrency`, claim em transação curta com `statement_timeout`, `statement_timestamp()`)
  aprovada pelo dono e aplicada na SPEC e no design spec. Detalhes no `CLAUDE.md` ("Transporte
  Postgres").
- **Quinta revisão externa (2026-09-27) aplicada** em três blocos (notas "Quinta revisão externa,
  bloco A/B/C" no `CLAUDE.md`): A — claim/workers do modo Postgres (checagem de pausa não
  correlacionada, `nextDueInMs` com 0 = vencida, `onGlobalLimit`, limitador de wake que só cobra
  passes de wake sem claim, prazo e `statement_timestamp()` no claim da `-system`, client mantido
  depois de erro com ROLLBACK limpo, timers limitados, logger seguro); B — guarda de modo misto em
  engine só de disparo, debounce vencido liberado só no modo Postgres, tipos de config exportados +
  checagem do `redis` injetado, pub/sub Postgres agrupando releituras, `ctx.stream` fatal no modo
  Postgres, `cron-parser` fixado; C — testes (banco inacessível no modo Postgres, migration 009 com
  índices pré-criados, condições observáveis/limites mais largos em `pg-engine`/`pg-resilience`),
  bench sem chave de dono sobrando, docs. Um bug de `src/` achado pelo bloco C: `start()` com o
  Postgres inacessível no modo Postgres lançava o erro cru do `pg` em vez de `MinitriggerError`
  (decisão 28(h)) — corrigido em `4b9e07e`. Relatórios em `.superpowers/review5/` (não versionado).
- **Pendências abertas da quinta revisão (para o dono):** `docs/SPEC.md` decisão 16 (seção 2,
  fixa) ainda diz que um pooler em modo transação faz o sistema "cair para o poll", sem citar que a
  eleição de líder fica duplicada/instável (o README, `docs/pt` e o design spec já dizem); o
  comentário da migration 009 e o tipo `SystemJobName` ainda citam um job `reconcile` que nunca é
  enfileirado (o dispatcher o trata como no-op; limpeza cosmética, não feita para não mexer em
  `src/` sem bug); a guarda de modo misto de uma engine só de disparo relê um marcador ausente a
  cada 30 s (um produtor pode escrever por até 30 s depois de uma engine do outro modo iniciar).
- Itens "minor (deferred)" restantes no mesmo ledger (dezenas, a maioria cosmética ou de robustez
  de teste, nenhum bloqueando o merge) — não repetidos aqui; ver o arquivo para a lista completa
  tarefa por tarefa.

**Próximas ondas (depois do merge/publicação, se o dono priorizar):**

- Paridade de bench contínua: reexecutar `pnpm bench` nos dois modos a cada mudança relevante no
  executor/store, para pegar uma regressão de latência cedo.
- Considerar mover o `NOTIFY` de wake para dentro de cada transição da store (hoje só o `trigger`
  com `tx` o embute no próprio insert; os demais caminhos publicam depois do commit) **se** o bench
  algum dia mostrar isso como gargalo — explicitamente fora de escopo por ora (`docs/superpowers/specs/2026-09-26-postgres-transport-design.md` §9).
- Streams por tabela para o modo Postgres, só se houver demanda real (hoje `ctx.stream`/
  `runs.readStream` são `ConfigError` nesse modo — ver "Postgres-only mode" no README).
- Eventual troca do transporte padrão de `"bullmq"` para `"postgres"` — só depois de rodar em
  produção por um tempo e confirmar a paridade medida aqui se sustenta sob carga real (não decidido,
  não agendado).

## Como retomar

1. `pnpm infra:up` (Docker Desktop precisa estar aberto) e `pnpm install`.
2. `pnpm typecheck && pnpm lint && pnpm test` para confirmar o verde.
3. Por frente de trabalho: implementador → revisão `mt-reviewer` (opus) → correções → verificação completa → commit convencional → atualizar `CLAUDE.md` e este arquivo.
4. Agents do projeto em `.claude/agents/` (`mt-implementer`, `mt-reviewer`, `mt-fixer`). Em sessões novas eles aparecem como tipos de agent; nesta sessão foram usados via `general-purpose` com o arquivo do agent como instrução.
