# minitrigger

Biblioteca TypeScript de execução durável de tasks/workflows (BullMQ + Redis + Postgres), instalável como dependência.

- Especificação completa: `docs/SPEC.md` (**a seção 2 é fixa; não mudar sem perguntar ao dono**).
- Plano, grafo de tarefas e modelos por tarefa: `docs/PLAN.md`.
- Notas de API verificadas via Context7: `docs/research/api-notes.md`.
- Agents do projeto: `.claude/agents/mt-implementer.md`, `mt-reviewer.md`, `mt-fixer.md`.

## Comandos

```bash
pnpm infra:up            # Postgres 16 (localhost:54329) + Redis 7 (localhost:63799, noeviction)
pnpm typecheck && pnpm lint && pnpm test
pnpm test:unit           # só unitários, sem infra
pnpm test:integration    # precisa de infra
pnpm build               # tsup → dist (ESM + CJS + d.ts)
```

Env de teste: `TEST_DATABASE_URL`, `TEST_REDIS_URL` (padrões apontam para o docker-compose).

## Decisões de arquitetura (resumo — detalhes em docs/SPEC.md §2)

1. Postgres é a fonte da verdade; BullMQ é só transporte. Enqueue perdido é recuperado pelo reconciler via `run_after`.
2. Durabilidade por replay com memoização; `SuspendSignal` interno; `Promise.allSettled` dos steps em voo antes de suspender; suspensão autoritativa.
3. `seq_index` atribuído na chamada; múltiplas esperas = "esperar todos"; resolvedores e transação de suspensão fazem `FOR UPDATE` na run.
4. Retries pelo motor (`attempts: 1` no BullMQ); `attempt` só incrementa em falha ou stall; `maxDurationMs` por tentativa.
5. Aquisição atômica com `enqueue_seq`; escritas checam ownership com `FOR SHARE`; toda saída de `running` zera `execution_id`.
6. Stall por heartbeat no Postgres (HOT update só de `heartbeat_at`, `fillfactor=80`, `SET LOCAL statement_timeout`).
7. jobId `${runId}-${seq}` (sem `:`); remoção agressiva de jobs.
8. Fila `${prefix}-system` com worker próprio para ticks, expirações e retenção.
9. Idempotência em `idempotency_keys` (PK `(task_id,key)`), `ON CONFLICT ... WHERE expires_at < now() RETURNING`; namespace `__` reservado.
10. Waitpoints criados na mesma transação da causa; resultados de filhas/tokens lidos de `runs`/`waitpoints`, não copiados em `steps.output`.
11. Não-determinismo por nome × `seq_index`; `#` reservado (`${name}#wait`).
12. `Serializable<T>` + validação runtime; limites de payload/step/log separados; formato fixo de erro.
13. Sem estado global. 14. Conexões injetáveis (duplicate() do ioredis; só fecha o que criou). 15. schema/prefix validados.
16. Advisory locks: `(hashtext(schema),1)` migrations (xact), `(hashtext(schema),2)` líder (client dedicado).
17. Tempo sempre do banco. 18. Migrations embutidas no bundle. 19. Cliente/worker separados. 20. Stop graceful devolve runs.
21. Schedules declarativos não apagam por padrão. 22. Checagens de ambiente indisponíveis são avisos.

## Decisões de implementação

- **Biome 2.5.14** para lint e formatação (sem ESLint/Prettier), com a config do dono do projeto (tabs, lineWidth 100). `pnpm lint` = `biome check .`, `pnpm format` = `biome check --write .`. `useImportType` está desligado no Biome, mas `verbatimModuleSyntax` no tsconfig continua exigindo `import type`.
- **TypeScript 6.0.x** (não 7.x): o dts do tsup usa a API JS do compilador, que o TS 7 nativo não oferece.
- **ioredis ^5.11.1** como peer (não 6.x): o BullMQ 6.3.x é testado com ioredis 5.11.1 e seu adapter não trata RESP3, padrão do ioredis 6.
- **vite** como devDependency explícita: o `vitest.config.ts` tipa um plugin que carrega `.sql` como texto, espelhando o `loader: { ".sql": "text" }` do tsup.
- Imports: `import { Pool, escapeIdentifier } from "pg"` e `import { Redis } from "ioredis"` (named). O default import do ioredis não tipa com NodeNext + verbatimModuleSyntax.
- tsup `dts.compilerOptions.ignoreDeprecations: "6.0"`: o rollup-plugin-dts embutido no tsup 8.5.1 sempre define `baseUrl`, que o TS 6 transforma em erro.
- Portas do docker-compose não padrão (54329/63799) para não colidir com serviços locais.

## Notas para as próximas fases (revisão da Fase 1)

- **Checagem de payload em `defineTask`:** `Serializable<P>` no parâmetro de `run` é ignorado (contravariância). A checagem tem de ficar no objeto de opções (ex.: `opts & ([Serializable<P>] extends [P] ? unknown : { __payloadNotSerializable: Serializable<P> })`) e/ou em `trigger(task, payload: P & Serializable<P>)`.
- Output `void` de task/step é gravado como `null`.
- Tasks genéricas (`<T extends Json>`) exigem cast; `any` passa como válvula de escape. Documentar no README (fase 10).
- Quando os tipos públicos referenciarem `pg.Pool`, adicionar `@types/pg` como peer opcional (`peerDependenciesMeta`).
- Instalação via GitHub com pnpm pode bloquear o `prepare` de dependências git; validar na fase 10.

- **`runs.created_at` truncado em milissegundos** (`date_trunc('milliseconds', now())`) em todo insert de run: o cursor `(created_at, id)` trafega como `Date` do JS; com microssegundos, a paginação pularia linhas. A store em memória (fase 9) deve seguir o mesmo.
- Cancelamento recursivo percorre descendentes nível a nível com `FOR UPDATE` (CTE recursiva não permite `FOR UPDATE`); ordem de locks pai → run → descendentes → waitpoints (ver `docs/store-contract.md`).
- Default de TTL de chave de usuário (`idempotencyKeyTtl`, 7 dias) é aplicado pela engine (fase 3), não pela store.

- Falha definitiva de uma run **não** cancela descendentes (filhas de `step.trigger` podem sobreviver ao pai, como na conclusão). Pendência: os waitpoints pendentes da própria run que falhou continuam `pending` (tratar na fase 5/6: vencer junto com a falha e remover jobs de expiração).
- Suspensão em `waiting_on_waitpoints` também faz `enqueue_seq + 1` (sem enqueue), para que uma reentrega do job antigo não readquira a run.

- Output de task é limitado por `maxPayloadBytes` (não `maxStepOutputBytes`): vai para `runs.output` e o pai o lê direto (decisão 10). Output inválido ou grande demais falha a run sem retry.
- Run de task desconhecida pelo processo (ex.: deploy em andamento) conta como tentativa com falha retryable.
- `stop()` devolve runs cuja função ignora o `signal`: semântica at-least-once (decisão 20); o resultado tardio é descartado pelo ownership.
- `@types/pg` é peer opcional: os tipos públicos citam `pg.Pool` e `Redis` do ioredis.

- Task sem parâmetro de payload tem payload `void` (opcional em `trigger`); `payload: unknown` explícito conta igual. Payload `undefined` é gravado como `null`.
- Valores alimentados em `setTimeout` (heartbeat, stall, shutdown, `maxDurationMs`) são limitados a 2^31-1 ms; delays do BullMQ não.

## Status das fases

| Fase | Status |
|---|---|
| 1 — Fundação | ✅ concluída — build ESM/CJS/DTS, 103 testes unitários, infra de integração por arquivo, `examples/basic` (ESM+CJS) |
| 2 — Schema e store | ✅ concluída — migration 001, `migrate()` com advisory xact lock, contrato `src/store/types.ts` + `docs/store-contract.md`, repositórios SQL, 127 testes de integração (revisão opus aplicada) |
| 3 — Trigger/fila/execução | ✅ concluída — `defineTask`/registry de filas, transporte BullMQ, executor (aquisição, heartbeat, retry, timeout), engine (`trigger`, `start`, `stop` graceful, `runs.*`); revisão opus aplicada |
| 4 — Steps/replay/sleep | pendente |
| 5 — Tokens/subtasks | pendente |
| 6 — Resiliência | em andamento (`src/leader.ts` pronto; falta ligar à engine) |
| 7 — Logs/eventos/consulta | pendente |
| 8 — Schedules | em andamento (migration 002 + `ScheduleStore` prontos; falta BullMQ/engine) |
| 9 — Modo de teste/DX | pendente |
| 10 — Exemplo/docs/empacotamento | pendente |
