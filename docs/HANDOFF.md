# Handoff — estado do projeto e próximos passos

Atualizado em 2026-09-24, HEAD `615ccac`. Leia junto com `CLAUDE.md` (decisões e notas de implementação), `docs/SPEC.md` (fonte da verdade) e `docs/PLAN.md` (grafo de tarefas e modelos).

## Estado atual

- **Verificação completa verde:** `pnpm typecheck && pnpm lint && pnpm test` → 706 testes (unit + integration) + 1 (flushall); `pnpm build` ok (ESM, CJS, DTS).
- Fases **1 a 7 concluídas**. Fase 9.1 (store em memória) pronta. Fase 8 parcial. Fases 9 (restante) e 10 pendentes.
- Árvore git limpa; 13 commits convencionais.

### O que existe (resumo)

| Área | Arquivos principais |
|---|---|
| Base, utilitários | `src/utils/*`, `src/errors.ts` |
| Schema e migrations | `src/migrations/001_init.sql`, `002_schedules.sql`, `migrate.ts` |
| Store (contrato + SQL + memória) | `src/store/types.ts`, `src/store/sql/*`, `src/store/memory/*`, `docs/store-contract.md` |
| Tasks e filas | `src/task.ts`, `src/registry.ts`, `src/queue/*` |
| Executor | `src/executor/*` (aquisição, heartbeat, steps, suspensão, tokens, subtasks, logs, notificação) |
| Engine | `src/engine.ts`, `src/config.ts` |
| Resiliência | `src/leader.ts`, `src/reconciler.ts`, `src/retention.ts`, `src/pubsub.ts` |
| Logs e streams | `src/logs/buffer.ts`, `src/streams.ts` |
| Testes | `test/unit`, `test/integration`, `test/integration-flushall`, `test/helpers` (namespace por arquivo, suíte de contrato de store) |
| Docs | `README.md` (rascunho quase completo), `examples/basic` (mínimo) |

### Decisões tomadas com o dono do projeto

- Biome (config do dono) no lugar de ESLint/Prettier.
- Decisão 3: **só waitpoints aguardados bloqueiam a retomada** (token criado sem `waitForToken` não segura um `sleep`).

## O que falta

### 1. Pendência de desempenho (pequena, fazer primeiro)
- `src/executor/notify.ts` (RunNotifier) ainda faz um `runs.get` extra por transição para obter `updatedAt`. Os resultados do store já trazem `status`/`updatedAt` (commit `615ccac`; mapa dos campos em `docs/store-contract.md`). Trocar para usar esses campos. Eventos ainda precisam de `taskId`/`attempt` — obter do contexto da execução, não do banco.

### 2. Fase 8 — agendamentos na engine (sonnet + revisão opus)
Pronto: migration 002 e `ScheduleStore` (upsert/delete/get/list/listDeclarative/deleteDeclarativeExcept).
Falta:
- `engine.schedules.upsert/delete/list` gravando no Postgres e depois `upsertJobScheduler` na fila `${prefix}-system` (ticks na `-system`, decisão 8).
- Handler do tick na `-system`: ler o schedule no Postgres; inexistente → remover o scheduler e não criar run; cron/timezone divergentes → refazer `upsertJobScheduler`; payload sempre do Postgres. Chave `__schedule:${id}:${timestampAgendado}` (o id do job do tick é `repeat:<schedulerId>:<nextMillis>`, ver `docs/research/api-notes.md`).
- `schedule: { cron, timezone }` em `defineTask`: sync no `start()` (cria/atualiza; remoção só com `schedules: { pruneDeclarative: true }`).
- Reconciliação bidirecional no reconciler (hook `reconcileSchedules` já existe em `src/reconciler.ts`).
- Documentar que ticks perdidos com todos os processos parados não são recuperados.
- Aceite: cron de alta frequência com dois processos sem duplicatas; declarativo removido persiste sem `pruneDeclarative`; `delete` com falha no BullMQ não gera run no tick seguinte; payload alterado só no Postgres é o usado.
- Atualizar a seção `TODO(phase 8)` do README (linha ~349).

### 3. Fase 9 — modo de teste e DX (opus para 9.2a, sonnet para o resto)
- `createTestEngine()` / `engine.runInline(task, payload)` usando `createMemoryStore` (`src/store/memory`), `sleep` instantâneo (relógio injetável), tokens completáveis manualmente, sem Redis. Exige um transporte em memória no lugar do BullMQ (interface de `src/queue/transport.ts`).
- Observação: a engine lê o relógio do banco direto via `pool.query` para a janela de retenção — no modo de teste usar o relógio da store em memória.
- Checagens na inicialização: versão do Postgres, `maxmemory-policy` (já em `src/queue/redis-check.ts`), migrations pendentes (`pendingMigrations`). Decisão 22: comando indisponível → aviso; resposta errada → erro.
- Mensagens de erro acionáveis (revisar as existentes).
- Testes de tipo de `Serializable<T>` com erros legíveis (já há boa cobertura em `test/unit/serializable-types.test.ts` e `task-types.test.ts`; completar).
- Aceite: exemplo de teste de task do usuário sem infra; teste da checagem de Redis simulando `CONFIG` bloqueado.

### 4. Fase 10 — exemplo, docs, empacotamento (sonnet)
- `examples/basic` completo: steps, sleep, token de aprovação, subtask, cron, agente de IA simples (LLM mockado). Só importar de `"minitrigger"`.
- `pnpm pack` → instalar o `.tgz` num diretório limpo, importar em ESM e CJS, rodar o exemplo.
- Instalação via GitHub (`pnpm add github:usuario/minitrigger`): o `prepare` roda o tsup e o pnpm pode bloquear scripts de dependências git; validar ou documentar e adotar `.tgz` em GitHub Releases.
- README: remover os `TODO(phase N)` restantes (linha ~61 é a instalação via GitHub), conferir toda a API pública contra `src/index.ts`.
- Benchmark `pnpm bench`: 1.000 runs de 3 steps, throughput e latência de retomada; registrar no `CLAUDE.md`.

### 5. Pontos menores registrados (sem urgência)
- `runs.list` sem `taskId` ordena todas as linhas: considerar índice `runs(created_at DESC, id DESC)` (nota em `docs/store-contract.md`).
- Token que expirou sozinho numa run que depois terminou reporta `run_finished`.
- Task que lança com steps ainda em voo não espera por eles (gravações tardias são rejeitadas pelo ownership).
- Filtros de data de `runs.list` se chamam `createdFrom`/`createdTo` (spec não fixa nomes).
- Argumentos inválidos de step (fn não-função, duração inválida) ainda contam tentativa; só nome inválido é fatal.

## Como retomar

1. `pnpm infra:up` (Docker Desktop precisa estar aberto) e `pnpm install`.
2. `pnpm typecheck && pnpm lint && pnpm test` para confirmar o verde.
3. Seguir a ordem acima (1 → 2 → 3 → 4). Por fase: implementador → revisão `mt-reviewer` (opus) → correções → verificação completa → commit convencional → atualizar `CLAUDE.md` e este arquivo.
4. Agents do projeto em `.claude/agents/` (`mt-implementer`, `mt-reviewer`, `mt-fixer`). Em sessões novas eles aparecem como tipos de agent; nesta sessão foram usados via `general-purpose` com o arquivo do agent como instrução.
