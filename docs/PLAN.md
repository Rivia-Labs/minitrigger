# Plano de implementação — minitrigger

Fonte da verdade: `docs/SPEC.md`. Notas de API (Context7): `docs/research/api-notes.md`.

## Modelos por tipo de tarefa

| Tipo                                                                                  | Modelo                              | Motivo                                                          |
| ------------------------------------------------------------------------------------- | ----------------------------------- | --------------------------------------------------------------- |
| Núcleo concorrente (executor, suspensão, waitpoints, reconciler, líder, cancelamento) | **opus**                            | Invariantes de concorrência e ownership; erro aqui é silencioso |
| SQL/migrations, repositórios, fila BullMQ, schedules, logs/streams                    | **sonnet**                          | Bem especificado, volume médio                                  |
| Utilitários, scaffolding, exemplo, README, benchmark                                  | **sonnet**                          | Mecânico/documental                                             |
| Correções de lint/format/tipos triviais                                               | **haiku** (`mt-fixer` com override) | Barato                                                          |
| Revisão de fase contra as decisões da seção 2                                         | **opus** (`mt-reviewer`)            | Precisa raciocinar sobre corridas                               |

Agents especializados em `.claude/agents/`: `mt-implementer`, `mt-reviewer`, `mt-fixer`.

## Grafo de tarefas

Legenda: `∥` = pode rodar em paralelo com as irmãs do mesmo bloco.

### Fase 1 — Fundação

- 1.0 Scaffold (package.json, tsconfig, tsup, biome, vitest, docker-compose) — orquestrador
- 1.1 ∥ Utilitários + testes unitários (`src/utils/*`, `src/errors.ts` base) — sonnet
- 1.2 ∥ Infra de testes de integração (schema/prefix por arquivo) — sonnet
- 1.3 ∥ `examples/basic` consumidor via `file:../..` — sonnet
- 1.R Revisão + verificação + commit

### Fase 2 — Schema e store

- 2.0 `store/types.ts` (interfaces + `withTransaction`) — opus (contrato que tudo usa)
- 2.1 ∥ `001_init.sql` + `migrate()` com advisory xact lock — sonnet
- 2.2 ∥ Repositórios SQL (runs, steps, waitpoints, idempotency, logs) — sonnet (depende de 2.0/2.1)
- 2.R Revisão + commit

### Fase 3 — Trigger, fila, execução simples

- 3.1 ∥ `task.ts` (`defineTask`, tipos, `Serializable`) + agrupamento/validação de filas — sonnet
- 3.2 ∥ `queue/` (Queue/Worker por fila, `-system`, jobIds, conexões dedicadas) — sonnet
- 3.3 Executor simples (aquisição, heartbeat, completed/failed, retries, maxDuration) + `engine.ts` (trigger, start, stop) — opus
- 3.R Revisão + commit

### Fase 4 — Steps, replay, sleep — opus (sequencial, núcleo)

### Fase 5 — Tokens e subtasks — opus (sequencial, núcleo)

### Fase 6 — Resiliência

- 6.1 ∥ Líder + reconciler — opus
- 6.2 ∥ Cancelamento cross-process (pub/sub) — opus
- 6.3 ∥ Retenção — sonnet

### Fase 7 — Logs, eventos, consulta

- 7.1 ∥ Logger em lote + consulta (`runs.get/list/logs/steps`) — sonnet
- 7.2 ∥ `events.on` + `runs.subscribe` + streams — sonnet

### Fase 8 — Schedules — sonnet (+ revisão opus)

### Fase 9 — Modo de teste e DX

- 9.1 ∥ Store em memória + `createTestEngine` — opus
- 9.2 ∥ Checagens de ambiente, mensagens de erro, testes de tipo — sonnet

### Fase 10 — Exemplo, docs, empacotamento

- 10.1 ∥ README — sonnet
- 10.2 ∥ Exemplo completo + validação `pnpm pack` ESM/CJS — sonnet
- 10.3 ∥ Benchmark — sonnet

## Protocolo por fase

1. Implementadores trabalham em arquivos disjuntos quando em paralelo.
2. Orquestrador integra, roda `pnpm typecheck && pnpm lint && pnpm test`.
3. Falhas → `mt-fixer`. Revisão → `mt-reviewer` contra seção 2 do SPEC.
4. Atualiza `CLAUDE.md` e faz commit convencional.
