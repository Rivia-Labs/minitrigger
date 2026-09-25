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
pnpm test:flushall       # FLUSHALL no Redis inteiro: projeto vitest isolado, só Redis local + NODE_ENV=test
pnpm build               # tsup → dist (ESM + CJS + d.ts)
```

Env de teste: `TEST_DATABASE_URL`, `TEST_REDIS_URL` (padrões apontam para o docker-compose).

## Decisões de arquitetura (resumo — detalhes em docs/SPEC.md §2)

1. Postgres é a fonte da verdade; BullMQ é só transporte. Enqueue perdido é recuperado pelo reconciler via `run_after`.
2. Durabilidade por replay com memoização; `SuspendSignal` interno; `Promise.allSettled` dos steps em voo antes de suspender; suspensão autoritativa.
3. `seq_index` atribuído na chamada; múltiplas esperas = "esperar todos"; resolvedores e transação de suspensão fazem `FOR UPDATE` na run. **Decisão do dono: só waitpoints aguardados bloqueiam a retomada** (token só conta depois do step `${nome}#wait`; `child_run`/`batch` sempre contam).
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

- Falha definitiva de uma run **não** cancela descendentes (filhas de `step.trigger` podem sobreviver ao pai, como na conclusão). Os waitpoints `pending` da própria run viram `timed_out` na mesma transação em **toda** saída terminal (`complete`, `fail`, `recoverStalled`→failed, `cancel`), e os jobs `wp-*-expire` deles são removidos (`expireJobsToRemove`, fase 5).
- Suspensão em `waiting_on_waitpoints` também faz `enqueue_seq + 1` (sem enqueue), para que uma reentrega do job antigo não readquira a run.

- Output de task é limitado por `maxPayloadBytes` (não `maxStepOutputBytes`): vai para `runs.output` e o pai o lê direto (decisão 10). Output inválido ou grande demais falha a run sem retry.
- Run de task desconhecida pelo processo (ex.: deploy em andamento) conta como tentativa com falha retryable.
- `stop()` devolve runs cuja função ignora o `signal`: semântica at-least-once (decisão 20); o resultado tardio é descartado pelo ownership.
- `@types/pg` é peer opcional: os tipos públicos citam `pg.Pool` e `Redis` do ioredis.

- Task sem parâmetro de payload tem payload `void` (opcional em `trigger`); `payload: unknown` explícito conta igual. Payload `undefined` é gravado como `null`.
- Valores alimentados em `setTimeout` (heartbeat, stall, shutdown, `maxDurationMs`) são limitados a 2^31-1 ms; delays do BullMQ não.

- **Fase 4 (steps):** `StepContext.register(kind, name)` é o ponto único de registro (checa step aninhado, erro fatal/suspensão/abort pendente, duplicata, atribui `seq_index` de forma síncrona, compara nome×índice e `kind` com o snapshot) e `start()` valida o nome do usuário e rastreia o step em `state.inFlight`; a fase 5 só adiciona tipos de step. Nomes internos (`${name}#wait`) usam `startInternal()` (pula `validateStepName`, mesmo registro + `trackInFlight`).
- **`steps.kind`** (`run`, `sleep`, `token`, `wait`, `trigger`, `trigger_wait`, `batch_wait`), `NOT NULL` + CHECK, adicionado **editando a migration 001** (pacote ainda não publicado; depois do primeiro release, mudanças de schema só em migrations novas). Gravado por `insertCompleted`/`insertWaitRef` (parâmetro `kind`), `insertSleep` (`sleep`), `triggerChildren` (por `mode`) e `createToken` (`token`); o replay compara o `kind` exatamente (ex.: `createToken("x")` trocado por `step.run("x")` → `NonDeterminismError`).
- `NonDeterminismError`, `DuplicateStepError`, `NestedStepError` e nome de step inválido (`validateStepName`, `ConfigError`) são bug de código: ficam em `state.fatalError` e falham a run **sem retry**, mesmo que o usuário capture o erro ou a execução suspenda depois. Depois disso, todo `step.*` relança o mesmo erro fatal (o código do usuário não consegue seguir executando steps).
- **Steps aninhados não são suportados:** `ctx.step.*` chamado dentro do `fn` de um `step.run` lança `NestedStepError` (detectado com `AsyncLocalStorage` ativo só enquanto o `fn` roda). O `step.run` externo também não é gravado, mesmo que o `fn` capture o erro.
- **Timeout durante o `allSettled` pré-suspensão:** se `maxDurationMs` estoura depois que a execução suspendeu (esperando steps em voo), a suspensão vence (decisão 2): `runs.suspend` roda normalmente, `attempt` não incrementa e o step em voo que não terminou reexecuta no replay. Cancel/lost/shutdown nesse intervalo não gravam nada.
- **Concorrência e determinismo:** `seq_index` na chamada torna determinístico um `Promise.all` direto de steps. Mas ramos assíncronos concorrentes que chamam **mais** steps depois de um `await` (ex.: `Promise.all(items.map(async i => { await ctx.step.run(a); await ctx.step.run(b) }))`) registram esses steps na ordem de conclusão, que muda entre execuções, e podem gerar `NonDeterminismError` no replay. Regra: não chamar steps depois de `await` dentro de ramos concorrentes (vai para as "regras de ouro" do README na fase 10).
- Output de `step.run` é normalizado antes de devolver: `void` → `null` já na primeira execução (igual ao replay). Output inválido/grande (`SerializationError`) propaga para a função como erro comum.
- `sleep` no replay: vencido se `snapshot.now + tempo monotônico decorrido >= wake_at` (limite inferior do `now()` do banco); `completeSleep` re-checa no banco (`not_due` → suspende de novo). A suspensão corre na corrida da execução (`onSuspend`), sem esperar a função terminar; heartbeat e `maxDurationMs` seguem ativos durante o `allSettled`. Depois de `runs.suspend`, `ctx.signal` aborta com o novo `AbortKind` `"suspended"` para parar código que engoliu o sinal (`logger.warn`).
- Outputs adiados (`maxInlineStepOutputBytes`) carregam em lotes alinhados de 50 `seq_index` (`DEFERRED_OUTPUT_BATCH_SIZE`), compartilhados por steps concorrentes.
- `NonDeterminismError`, `DuplicateStepError` e `NestedStepError` são exportados por `src/index.ts`.

- **Fase 5 (tokens/subtasks):** `StepContextDeps.waits` (`StepWaitDeps`) é a costura do executor: `triggerChildren`/`listChildResults`, `createToken`/`getToken`, `resolveTask`, `enqueueRun` e `scheduleExpire` (esses dois nunca lançam; enqueue perdido fica para o reconciler). `waitForToken(name)` sem `createToken` na execução registra os dois steps (`name` + `name#wait`) de forma síncrona. Replay de wait: status do snapshot (`pending` → suspende direto; a transação de suspensão re-checa); resolvido → uma query (`getToken` ou `listChildResults`). No primeiro `waitForToken`, depois do `insertWaitRef` há um `getToken`: token já completado (run ainda `running`) retorna sem suspender.
- **Expiração:** `createToken`/`triggerChildren` agendam `wp-${id}-expire` com o delay do banco depois do commit. No replay, `createToken` re-adiciona o job se o token ainda está `pending` com `expires_at` (mesmo jobId → no-op do BullMQ; recria se o processo morreu entre commit e enqueue). `not_due` no handler: `job.moveToDelayed(Date.now() + delayMs_do_banco, job.token)` + `throw new DelayedError()` (mesmo jobId; re-adicionar o id do job ativo seria ignorado pelo BullMQ e um id novo escaparia da remoção pelos resolvedores). `createToken` na store é idempotente por `(run_id, step_name)`.
- `WaitResult` de timeout: `{ ok: false, error: { name: "Timeout", retryable: false } }`. Batch com timeout: só as filhas canceladas PELA expiração (`error.message === WAITPOINT_TIMEOUT_REASON`, constante em `store/types.ts`) viram `Timeout`; filhas que completaram, falharam ou foram canceladas por `runs.cancel(childId)` antes do timeout mantêm o resultado real. Filha cancelada → `error.name = "Canceled"`. `batchTriggerAndWait` com `[]` consome `seq_index`, não grava nada e devolve `[]`; número de filhas diferente do de payloads no replay → `NonDeterminismError`.
- `engine.tokens.complete(id, output)` → `{ completed: true }` ou `{ completed: false, reason: "not_found" | "completed" | "timed_out" | "run_finished" }` (id que não é UUID → `not_found`; `run_finished` quando o token está `timed_out` e a run dele é terminal, lido sob o `FOR UPDATE` da run no `completeToken`); `output` validado com `maxPayloadBytes` antes de tocar o banco. `step.trigger` aceita `{ delay }` (`TriggerChildrenInput.delayMs`).
- O reconciler (não ligado à engine ainda, fase 6) já remove `expireJobsToRemove` de `recoverStalled`→failed. No caso 5 (expiração) remove os jobs de run das filhas canceladas (`removeRunJob`, best-effort) e chama `onCanceled?(runIds)` (opção de `createReconciler`; a engine vai passar `cancelLocally` + pub/sub). Falha do hook é logada/contada em `errors` e não impede a retomada do pai.
- **Decisão do dono (decisão 3): só waitpoints aguardados bloqueiam a retomada.** Um `token` só conta como pendente para a retomada se existir o step `${step_name}#wait` (kind `wait`) da run; `child_run`/`batch` sempre contam (nascem com o step de espera). Fonte única: `blockingWaitpointExistsSql` (`store/sql/resolve.ts`), usada por `runs.suspend`, `applyResumeRule` e pelo reconciler (`listStaleWaiting`, `repairOrphanWaiting`). Assim `createToken("ok")` → `sleep("lembrete")` → `step.run` → `waitForToken("ok")` acorda depois do sleep sem o token. Completar um token ainda não aguardado enquanto a run dorme usa a regra de retomada normal (`run_after = max(wake_at)`), sem retomada adiantada.
- **Steps não aguardados:** se a função retorna com steps em voo (ex.: `void ctx.step.trigger(...)`), o executor loga `warn` (`step "x" was not awaited`) e faz o mesmo `allSettled` com abort de antes da suspensão (`drainInFlight`) antes do `complete`: o trigger cria a filha, o `step.run` é gravado. Se um desses steps suspender (`waitForToken`/`sleep`/`triggerAndWait` não aguardado), a suspensão vence (decisão 2): a run suspende em vez de completar e o replay devolve o output quando nada mais estiver pendente. Só no caminho `success`; erro/abort seguem como antes (escritas tardias caem no ownership). `ExecutionState.inFlight` virou `Map<Promise, nomeDoStep>`.

- **Fase 6 (resiliência ligada à engine):** `start()` sobe, depois dos workers, o subscriber de pub/sub (conexão dedicada `subscriber`) e o loop do líder (`setInterval` unref de `reconciler.intervalMs`, primeiro tick imediato, sem sobreposição: tick em andamento → o próximo é pulado). Tick: `leader.tryAcquire()`; só o líder roda `reconciler.runOnce()` e agenda a retenção. Depois de perder a liderança (`onLose`), o processo pula uma tentativa para dar a vez a outro. `stop()`: para o loop (espera o tick, `leader.close()`) antes de fechar os workers; `pubsub.close()` e flush das publicações pendentes no `beforeClose`.
- Client do líder: `connectionString` com `postgres` string, `() => new Client(pool.options)` com pool injetado (decisão 16). O líder segura uma conexão extra fora do pool.
- **Retenção:** job `retention` na `-system` com jobId `retention-${floor(now_do_banco_ms / retention.intervalMs)}` (decisão 17). Como os jobs são removidos ao completar, o jobId só deduplica entre processos enquanto o job existe; o processo guarda a última janela enfileirada. Qualquer worker `-system` executa o job, sem checar liderança: a varredura é idempotente (`SKIP LOCKED`), uma segunda na mesma janela (troca de líder) só custa queries.
- **Cancelamento entre processos:** `cancelLocally` (engine `runs.cancel`, expiração na `-system`) aborta locais, publica status e `publishCancel`; o `onCanceled` do reconciler aborta locais e publica. Toda engine iniciada assina `onCancel` → `abort("canceled")`. Engine só de disparo publica pela conexão compartilhada (`createPubSubPublisher`), sem assinar. `PubSub.ready` (o `start()` espera o `SUBSCRIBE`).
- **Status publicado (costura da fase 7):** `RunNotifier` (`executor/notify.ts`) nas transições: `queued` (trigger, filhas, retry, release/hand-back), `running` (acquire), `waiting`/`queued` (suspend), `completed`/`failed`/`canceled`, stall recuperado (`onStatusChange` do reconciler). `updatedAt` vem da linha do banco: do registro retornado (create/acquire) ou de um `store.runs.get` depois da transição (as outras operações da store não devolvem `updated_at`; se virar gargalo, fazer a store devolver `updatedAt`). Tudo best-effort, fora do caminho crítico, nunca falha a run.
- Config nova: `reconciler: { intervalMs 15 s, graceMs 30 s, batchSize 100 }`, `retention: { days 30, intervalMs 1 h, batchSize 1000 }`.
- Teste de FLUSHALL em `test/integration-flushall/` (projeto vitest `flushall`, roda depois de `unit`+`integration` no `pnpm test`); pula com aviso se o Redis não for local ou `NODE_ENV !== "test"` (o vitest define `test`).

- **Fase 7 (logs/eventos/consulta):** `ctx.logger` = `createLogBuffer` por execução (`executor/logger.ts`): `store.logs.insertBatch` (um `INSERT ... unnest` por lote de até 500 — `maxBatchSize` novo no buffer, para uma rajada síncrona não virar um statement gigante), flush a cada 500 ms / 50 entradas e **sempre antes** da escrita terminal/suspensão (`logger.close()` antes do `settle`, de novo no `finally`, idempotente). Execução que perdeu a run (`lost`/`canceled`/`shutdown`/`handedBack`) descarta os pendentes com um `warn` local (espírito da decisão 5; `insertBatch` não tem guarda). Falha: 1 retry, depois descarta com `warn`. Config `logging: { minLevel "info", mirror true }`; espelho para `config.logger` com `runId`, `taskId`, `attempt` e o `data` original (truncagem só no armazenado). 10k linhas numa run = 20 `INSERT` (teste conta os statements no pool injetado).
- `RunNotifier.status(runId, known?, transition?)`: `RunTransition` (`queued`/`started`/`waiting`/`completed`/`failed`/`canceled`/`retrying`) nomeada por quem fez a transição (sem ela, derivada do status lido); stall recuperado pelo líder → `retrying` se voltou a `queued`. O notifier publica o status (pub/sub) **e** emite `engine.events` (`run:${transition}`, handlers com erro síncrono/assíncrono logados com `logger.error`). Filhas criadas por `step.trigger*` agora também publicam `queued`.
- `runs.subscribe(runId, { signal })`: generator; ordem obrigatória (assina o canal e bufferiza → lê o Postgres e emite → só emite mensagens com `updatedAt` maior, ou igual com status diferente, porque o `Date` do JS perde os microssegundos → termina depois de status terminal). Run inexistente termina sem emitir. Rede de segurança: 15 s sem mensagem → relê o Postgres (pub/sub é at-most-once). Subscriber criado sob demanda (`getPubSub()`, conexão `dedicated("subscriber")`), também numa engine só de disparo; `stop()` encerra as assinaturas abertas antes de `pubsub.close()`.
- `runs.readStream`: um `shared().duplicate({ maxRetriesPerRequest: null })` por leitor (não `connections.dedicated`, que guardaria cada conexão até o `stop()`), desconectado no fim da iteração ou no `stop()`. `ctx.stream` = `streams.pipe` (não é step: não é memoizado, replay transmite de novo).
- `runs.logs(run, { afterId?, limit? })` → `{ items, nextAfterId }` (padrão 100, máx. 1 000); `runs.steps(run)` → `{ name, seq, kind, status, output, wakeAt, createdAt }` (waits só com `{ waitpointId }`). `runs.list` mantém `createdFrom` (inclusivo)/`createdTo` (exclusivo), nomes da store. Id que não é UUID → vazio.
- **Em aberto (performance):** o `RunNotifier` faz um `store.runs.get` extra por transição (exceto create/acquire) só para obter `updatedAt` (e `taskId`/`attempt`/`error` dos eventos). Trocar para os valores devolvidos pela store quando os resultados trouxerem `updatedAt` (mudança de store em andamento por outro agente em `src/store/types.ts`; os eventos ainda precisam de `taskId`/`attempt`, que o executor tem no `ExecutionState`).

## Status das fases

| Fase | Status |
|---|---|
| 1 — Fundação | ✅ concluída — build ESM/CJS/DTS, 103 testes unitários, infra de integração por arquivo, `examples/basic` (ESM+CJS) |
| 2 — Schema e store | ✅ concluída — migration 001, `migrate()` com advisory xact lock, contrato `src/store/types.ts` + `docs/store-contract.md`, repositórios SQL, 127 testes de integração (revisão opus aplicada) |
| 3 — Trigger/fila/execução | ✅ concluída — `defineTask`/registry de filas, transporte BullMQ, executor (aquisição, heartbeat, retry, timeout), engine (`trigger`, `start`, `stop` graceful, `runs.*`); revisão opus aplicada |
| 4 — Steps/replay/sleep | ✅ concluída — `StepContext` (`src/executor/step-context.ts`): snapshot numa query, `seq_index` na chamada, memoização, `sleep`, `SuspendSignal` autoritativo, `allSettled` pré-suspensão + `runs.suspend`; `steps.kind`, `NestedStepError`; 21 testes de integração + 18 unitários |
| 5 — Tokens/subtasks | ✅ concluída — `createToken`/`waitForToken`, `engine.tokens.complete`, handler `waitpoint-expire` na `-system`, `step.trigger`/`triggerAndWait`/`batchTriggerAndWait`, timeout dos waitpoints próprios em toda saída terminal; revisão aplicada (steps não aguardados, `run_finished`, timeout de batch preciso, jobs das filhas no reconciler, só waitpoints aguardados bloqueiam) |
| 6 — Resiliência | ✅ concluída — loop do líder na engine (reconciler + agendamento da retenção na `-system`), cancelamento entre processos por pub/sub, publicação de status, config `reconciler`/`retention`; testes de cada caso do reconciler via engine, stall, perda/retomada de liderança, `migrate()` com líder ativo, órfã com `warn`, cancelamento entre processos, retenção em lotes e FLUSHALL (projeto isolado) |
| 7 — Logs/eventos/consulta | ✅ concluída — `ctx.logger` em lote no `run_logs` (flush antes da escrita terminal/suspensão), config `logging`, `runs.list/logs/steps`, `engine.events.on`, `runs.subscribe` (ordem obrigatória, engine só de disparo), `ctx.stream`/`runs.readStream`; testes `logs`/`subscribe`/`events`/`stream`/`query` |
| 8 — Schedules | em andamento (migration 002 + `ScheduleStore` prontos; falta BullMQ/engine) |
| 9 — Modo de teste/DX | pendente |
| 10 — Exemplo/docs/empacotamento | pendente |
