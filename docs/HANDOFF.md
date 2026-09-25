# Handoff — estado do projeto e próximos passos

Atualizado em 2026-09-25. Leia junto com `CLAUDE.md` (decisões e notas de implementação), `docs/SPEC.md` (fonte da verdade) e `docs/PLAN.md` (grafo de tarefas e modelos).

## Estado atual

- **Verificação completa verde:** `pnpm typecheck && pnpm lint && pnpm test` → 877 testes (unit + integration) + 1 (flushall); `pnpm build` ok (ESM, CJS, DTS).
- **Fases 1 a 10 concluídas.** `pnpm check:pack` ok; `examples/basic` roda de ponta a ponta.
- Árvore git limpa; commits convencionais por fase/frente.

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

### Próximos passos (fora das fases do kickoff)
- **Publicar o primeiro release:** `pnpm check:pack`, criar tag `v0.1.0` e anexar o `.tgz` ao GitHub Release (caminho recomendado no README). Depois disso, mudanças de schema só em migrations novas (a 001 não pode mais ser editada).
- Opcional: `TestEngine.start()` que drena em segundo plano (hoje o test engine só roda quando o teste chama `drain`/`runInline`/`drive`).

### Pontos em aberto registrados (sem urgência, comportamento documentado)
- Token que expirou sozinho numa run que depois terminou reporta `run_finished`.
- Task que lança com steps ainda em voo não espera por eles (gravações tardias são rejeitadas pelo ownership).
- Filtros de data de `runs.list` se chamam `createdFrom`/`createdTo` (spec não fixa nomes).
- Payload de filha não serializável e task filha não registrada continuam retryable (os demais argumentos inválidos de step são fatais).
- `sideEffects: false` só ajuda bundlers: em Node o `dist/index.js` único é carregado inteiro (inclui o modo de teste).

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
