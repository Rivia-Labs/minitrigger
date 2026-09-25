# Handoff — estado do projeto e próximos passos

Atualizado em 2026-09-25. Leia junto com `CLAUDE.md` (decisões e notas de implementação), `docs/SPEC.md` (fonte da verdade) e `docs/PLAN.md` (grafo de tarefas e modelos).

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

### 1. Pendência de desempenho — ✅ resolvida
O `RunNotifier` recebe `RunStatusInfo` dos resultados da store; teste `test/integration/notify-perf.test.ts`.

### 2. Fase 8 — ✅ concluída
`src/schedules.ts` (API, tick, sync declarativo, hook do reconciler); revisão opus aplicada (ordem de leitura do reconciler, retries do tick, guarda de `source`, tag de schema, tipos do payload). Notas no `CLAUDE.md`.

### 3. Fase 9 — modo de teste e DX (opus para 9.2a, sonnet para o resto)
- `createTestEngine()` / `engine.runInline(task, payload)` usando `createMemoryStore` (`src/store/memory`), `sleep` instantâneo (relógio injetável), tokens completáveis manualmente, sem Redis. Exige um transporte em memória no lugar do BullMQ (interface de `src/queue/transport.ts`).
- Observação: a engine lê o relógio do banco direto via `pool.query` para a janela de retenção — no modo de teste usar o relógio da store em memória.
- ✅ Checagens na inicialização: `src/env-check.ts` (`checkEnvironment`, Postgres ≥ 13, `maxmemory-policy`, migrations pendentes), ligado ao `start()` e exportado.
- Mensagens de erro acionáveis (revisar as existentes).
- Testes de tipo de `Serializable<T>` com erros legíveis (já há boa cobertura em `test/unit/serializable-types.test.ts` e `task-types.test.ts`; completar).
- Aceite: exemplo de teste de task do usuário sem infra; teste da checagem de Redis simulando `CONFIG` bloqueado.

### 4. Fase 10 — exemplo, docs, empacotamento (sonnet)
- ✅ `examples/basic` completo (`pnpm start` em `examples/basic`, sai com 0 em ~3 s).
- `pnpm pack` → instalar o `.tgz` num diretório limpo, importar em ESM e CJS, rodar o exemplo.
- Instalação via GitHub (`pnpm add github:usuario/minitrigger`): o `prepare` roda o tsup e o pnpm pode bloquear scripts de dependências git; validar ou documentar e adotar `.tgz` em GitHub Releases.
- README: remover os `TODO(phase N)` restantes (linha ~61 é a instalação via GitHub), conferir toda a API pública contra `src/index.ts`.
- ✅ `pnpm bench` e `pnpm check:pack` (resultados no `CLAUDE.md`); instalação via GitHub investigada em `docs/research/github-install.md` (falta levar a recomendação ao README).

### 5. Pontos menores registrados (sem urgência)
- `runs.list` sem `taskId` ordena todas as linhas: considerar índice `runs(created_at DESC, id DESC)` (nota em `docs/store-contract.md`).
- Token que expirou sozinho numa run que depois terminou reporta `run_finished`.
- Task que lança com steps ainda em voo não espera por eles (gravações tardias são rejeitadas pelo ownership).
- Filtros de data de `runs.list` se chamam `createdFrom`/`createdTo` (spec não fixa nomes).
- ✅ `step.run` com retorno em união agora infere sem genérico explícito.
- Argumentos inválidos de step (fn não-função, duração inválida) ainda contam tentativa; só nome inválido é fatal.

## Como retomar

1. `pnpm infra:up` (Docker Desktop precisa estar aberto) e `pnpm install`.
2. `pnpm typecheck && pnpm lint && pnpm test` para confirmar o verde.
3. Seguir a ordem acima (1 → 2 → 3 → 4). Por fase: implementador → revisão `mt-reviewer` (opus) → correções → verificação completa → commit convencional → atualizar `CLAUDE.md` e este arquivo.
4. Agents do projeto em `.claude/agents/` (`mt-implementer`, `mt-reviewer`, `mt-fixer`). Em sessões novas eles aparecem como tipos de agent; nesta sessão foram usados via `general-purpose` com o arquivo do agent como instrução.
