# Handoff — estado do projeto e próximos passos

Atualizado em 2026-09-25. Leia junto com `CLAUDE.md` (decisões e notas de implementação), `docs/SPEC.md` (fonte da verdade) e `docs/PLAN.md` (grafo de tarefas e modelos).

## Estado atual

- **Verificação completa verde:** `pnpm typecheck && pnpm lint && pnpm test` → 877 testes (unit + integration) + 1 (flushall); `pnpm build` ok (ESM, CJS, DTS).
- **Fases 1 a 10 concluídas.** `pnpm check:pack` ok; `examples/basic` roda de ponta a ponta.
- Árvore git limpa; commits convencionais por fase/frente.

### O que existe (resumo)

| Área                             | Arquivos principais                                                                                                              |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Base, utilitários                | `src/utils/*`, `src/errors.ts`                                                                                                   |
| Schema e migrations              | `src/migrations/001_init.sql`, `002_schedules.sql`, `migrate.ts`                                                                 |
| Store (contrato + SQL + memória) | `src/store/types.ts`, `src/store/sql/*`, `src/store/memory/*`, `docs/store-contract.md`                                          |
| Tasks e filas                    | `src/task.ts`, `src/registry.ts`, `src/queue/*`                                                                                  |
| Executor                         | `src/executor/*` (aquisição, heartbeat, steps, suspensão, tokens, subtasks, logs, notificação)                                   |
| Engine                           | `src/engine.ts`, `src/config.ts`                                                                                                 |
| Resiliência                      | `src/leader.ts`, `src/reconciler.ts`, `src/retention.ts`, `src/pubsub.ts`                                                        |
| Logs e streams                   | `src/logs/buffer.ts`, `src/streams.ts`                                                                                           |
| Testes                           | `test/unit`, `test/integration`, `test/integration-flushall`, `test/helpers` (namespace por arquivo, suíte de contrato de store) |
| Docs                             | `README.md` (rascunho quase completo), `examples/basic` (mínimo)                                                                 |

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
- `sideEffects: false` só ajuda bundlers; em Node o `dist/index.js` único é carregado inteiro, incluindo o modo de teste. **Avaliado em 2026-09-25 e mantido:** o código do minitrigger inteiro carrega em ~2,3 ms, contra ~44 ms das peers (bullmq/ioredis/pg). Se um dia for necessário: subpath `minitrigger/testing` com `splitting: true` no tsup (chunk compartilhado para não duplicar as classes de erro e quebrar o `instanceof`), o que é uma mudança incompatível de import.

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

1. ✅ (0.3.0, não publicado no npm ainda) Propostas da comparação com `@rivia-labs/oban-ts`, por valor: outbox transacional (`trigger(..., { tx })`), pacote `@rivia-labs/minitrigger` público (LICENSE MIT, subpath `/testing`, Node >= 22), gestão de filas e operações em lote, helpers de teste, telemetria (`durationMs`, `attachDefaultLogger`), skills de Claude Code e docs em PT.
2. ✅ Adapters de ORM para o outbox (`/typeorm`, `/knex`, `/drizzle`, `/prisma`), testados contra TypeORM 1.1, Knex 3.3, Drizzle 0.45 (node-postgres e postgres.js) e Prisma 7.10.
3. **Modo só-Postgres** (`transport: "postgres" | "bullmq"`, BullMQ continua padrão): fazer depois das propostas atuais. Escopo mínimo avaliado: fábricas de backend, fetch+acquire direto em `runs` com `SKIP LOCKED`, LISTEN/NOTIFY + timer + poll, ticks de cron pelo líder, pub/sub por NOTIFY com payload enxuto, `globalConcurrency` em SQL; `rateLimit` e streams com `ConfigError` no MVP. Exige emendas às decisões 1, 8, 14, 15, 16, 21 e 22 (aprovação do dono antes de implementar). Estimativa ~3 semanas; medir paridade com `pnpm bench` antes de pensar em trocar o padrão.

### Auditoria externa (2026-09-25)

- 13 achados + itens baixos, todos confirmados e corrigidos (resumo em `CLAUDE.md`, "Auditoria externa"). Único ajuste de decisão: `:` reservado em nomes de step (decisão 11, autorizado pelo dono).
- Conhecido e não tratado: `{}` como tipo de payload/output passa no `Serializable` (atalho `[T] extends [Json]` evita TS2589).

### Concluído nesta sessão (2026-09-25)

- Notificador sem `runs.get` extra por transição (`test/integration/notify-perf.test.ts`).
- Fase 8: `src/schedules.ts` (API, tick com idempotência por ocorrência e retries, sync declarativo com guarda de `source`, reconciliação bidirecional com tag de schema).
- Fase 9: `checkEnvironment` no `start()`; `createTestEngine` (`src/testing/`); mensagens de erro acionáveis; inferência de `step.run` com união.
- Fase 10: `examples/basic` completo, README final, `pnpm bench`, `pnpm check:pack`, `docs/research/github-install.md`.
- Menores: migration 003 (`idx_runs_created`), argumentos inválidos de step fatais, flake do teste de retenção.

## Como retomar

1. `pnpm infra:up` (Docker Desktop precisa estar aberto) e `pnpm install`.
2. `pnpm typecheck && pnpm lint && pnpm test` para confirmar o verde.
3. Por frente de trabalho: implementador → revisão `mt-reviewer` (opus) → correções → verificação completa → commit convencional → atualizar `CLAUDE.md` e este arquivo.
4. Agents do projeto em `.claude/agents/` (`mt-implementer`, `mt-reviewer`, `mt-fixer`). Em sessões novas eles aparecem como tipos de agent; nesta sessão foram usados via `general-purpose` com o arquivo do agent como instrução.
