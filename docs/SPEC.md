# Kickoff: `minitrigger` — lib de workflows duráveis (BullMQ + Redis + Postgres)

> Especificação original do projeto. Fonte da verdade para todas as fases. Não altere as decisões da seção 2 sem aprovação do dono do projeto.

Você vai construir uma biblioteca TypeScript de **execução durável de tasks e workflows**, inspirada no Trigger.dev, mas no formato de **lib instalável** em projetos pessoais. Não há plataforma, deploy, billing nem multi-tenant. O usuário instala o pacote, aponta para um Postgres e um Redis e passa a definir tasks no próprio código.

Trabalhe **fase por fase**, na ordem, e só avance quando os critérios de aceite da fase estiverem cumpridos e os testes estiverem verdes.

---

## 0. Regras de trabalho

- Use o **Context7 MCP** para consultar a documentação atual de `bullmq`, `ioredis`, `pg`, `tsup` e `vitest` antes de usar qualquer API. A API do BullMQ mudou em versões recentes (ex.: `upsertJobScheduler` substituiu os repeatable jobs antigos, `setGlobalConcurrency`, restrições de caracteres em jobIds customizados), então **não confie em memória**. (Notas consolidadas em `docs/research/api-notes.md`.)
- Gerenciador de pacotes: **pnpm**. Node **>= 22** em runtime (`"engines": { "node": ">=22" }` no `package.json` e `target: "node22"` no tsup — Node 20 chegou ao fim de vida em abril de 2026); as ferramentas de dev continuam fixadas em Node 24 (`.node-version` com `24`, é o que roda localmente/CI), porque `test/helpers/child/` usa type stripping nativo do Node (padrão só a partir do 23.6) — rationale completo em `CLAUDE.md` ("Decisões de implementação").
- TypeScript em `strict` total, incluindo `noUncheckedIndexedAccess` e `exactOptionalPropertyTypes`.
- Manter `CLAUDE.md` na raiz com as decisões de arquitetura (seção 2) e o status de cada fase.
- Ao final de cada fase: rode `pnpm typecheck && pnpm lint && pnpm test`, atualize o `CLAUDE.md` e faça um commit com mensagem convencional (`feat(core): ...`).
- **Não mude decisões da seção 2 sem perguntar antes.** Se encontrar um problema real numa decisão, pare e explique.
- Sem ORM. Use `pg` diretamente com SQL parametrizado.
- Dependências mínimas. Qualquer dependência nova além das listadas precisa de justificativa no `CLAUDE.md`.

---

## 1. Objetivo e não-objetivos

**Objetivo:** tasks definidas em código, disparadas por `trigger()`, executadas por workers no próprio processo da aplicação, com:

- steps memoizados (`step.run`) e replay após falha ou suspensão;
- espera durável (`step.sleep`, `step.waitForToken`) sem ocupar slot de worker;
- retries com backoff, filas com concorrência e rate limit;
- subtasks (`step.trigger`, `step.triggerAndWait`, `step.batchTriggerAndWait`);
- agendamentos cron;
- idempotência, cancelamento e timeout;
- logs por run e consulta de status;
- uso confiável como dependência: instalar num projeto pessoal e funcionar sem surpresas.

**Não-objetivos:** dashboard web (fase opcional), multi-tenant, autenticação, deploy de código, checkpoint de processo (CRIU), suporte a outros bancos que não Postgres.

---

## 2. Decisões de arquitetura (fixas)

1. **Postgres é a fonte da verdade.** Todo estado de run, step, waitpoint, chave de idempotência e log vive no Postgres. **BullMQ é só transporte** (entrega, delay, concorrência, rate limit). Se o Redis for zerado, nenhuma run é perdida: o reconciler reenfileira a partir do Postgres. O `trigger()` faz duas escritas não atômicas (insert no Postgres, depois enqueue no Redis); um enqueue perdido é recuperado pelo reconciler usando `runs.run_after` (o próximo momento em que a run deve executar), e a latência dessa recuperação é o intervalo do reconciler. Isso é aceito e deve estar documentado.

   **No modo Postgres:** No modo Postgres (`transport: "postgres"`, decisão 28) não existe transporte separado: a própria linha de `runs` com `status IN ('queued','waiting')` e `run_after <= now()` é a fila, e os workers a reivindicam com `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)` num único statement que grava o mesmo que o `acquire` de hoje (`status = 'running'`, `execution_id`, `heartbeat_at`, `started_at` se nulo, `debounce_key = NULL`); o claim é o `acquire`. `enqueue_seq` não muda no claim (não há job a invalidar) e `concurrency_waits` só é zerado depois da re-checagem da decisão 25. `trigger()` vira uma escrita atômica: nada pode se perder entre o insert e a fila. Um `NOTIFY` acorda os workers e o poll é a rede de segurança. Os casos 1 e 2 do reconciler (enqueue perdido) ficam desligados nesse modo: uma run vencida é backlog, não perda. O invariante `waiting` + `run_after` preenchido = pronta para retomar em `run_after`, `waiting` + `run_after NULL` = bloqueada, já vale hoje e é o que o claim usa.

2. **Durabilidade por replay com memoização.** A função da task é reexecutada do início a cada retomada. Steps já concluídos devolvem o resultado salvo. Suspender = lançar um sinal interno (`SuspendSignal`) capturado pelo motor, nunca exposto ao usuário. Antes de efetivar a suspensão, o executor aguarda `Promise.allSettled` de todos os steps em voo naquela execução, para que um `Promise.all([step.run, step.sleep])` não reexecute o `step.run` no replay.
   A suspensão é **autoritativa**: depois que um `SuspendSignal` foi lançado numa execução, (a) toda chamada subsequente a `step.*` relança o sinal e (b) o desfecho da função é ignorado. Qualquer retorno ou erro vira suspensão, com `logger.warn` de uso incorreto se o usuário capturou o sinal. Assim um `try/catch` do usuário não consegue quebrar o replay nem gerar retry indevido. Se um step em voo falhar durante o `Promise.allSettled` pré-suspensão, a suspensão vence, a falha não conta tentativa e o step reexecuta no replay.

3. **Steps concorrentes e múltiplas esperas.** `seq_index` é atribuído no momento da **chamada** de `step.*`, não da conclusão, então `Promise.all` de vários `step.run` é determinístico. Múltiplas suspensões na mesma execução (`Promise.all` de vários `waitForToken`/`sleep`/`triggerAndWait`) são permitidas e viram "esperar todos": a run só é retomada quando todos os waitpoints pendentes daquela execução resolverem e todo `sleep` pendente tiver vencido.
   - Quem resolve um waitpoint (completar token, filha terminar, expiração) faz, **na mesma transação**, `SELECT ... FROM runs WHERE id=$1 FOR UPDATE` antes de verificar se ainda há esperas pendentes. Sem esse lock, dois tokens completados ao mesmo tempo veem um ao outro como pendente (READ COMMITTED) e ninguém reenfileira a run. Com a linha travada, o resolvedor só enfileira se a run estiver `waiting` e sem waitpoints pendentes. Nesse caso faz `enqueue_seq + 1`, `run_after = greatest(max(wake_at dos sleeps pendentes), now())` e enfileira com o delay correspondente; assim um sleep ainda não vencido não gera um replay inútil. Se a run ainda estiver `running`, ele só completa o waitpoint e não enfileira nada.
   - A **transação de suspensão** usa o mesmo lock: `FOR UPDATE` na run (com ownership), re-checagem dos waitpoints pendentes dentro da transação e só então a decisão:
     - com waitpoint pendente: `waiting`, `run_after = NULL`, e **nenhum enqueue**. Quem resolver o último waitpoint enfileira;
     - sem waitpoint pendente, mas com sleep futuro: `waiting`, `enqueue_seq + 1`, `run_after` = maior `wake_at` pendente, enqueue com esse delay;
     - sem nenhuma espera pendente (tudo foi resolvido enquanto a run ainda executava): `queued`, `enqueue_seq + 1`, `run_after = now()`, enqueue imediato.

     Sem essa re-checagem, um token completado entre sua criação e a suspensão deixaria a run `waiting` sem ninguém para acordá-la.

   - O reconciler é a rede de segurança: run `waiting` sem waitpoint pendente e com `run_after < now() - grace` é reenfileirada com o **mesmo** `enqueue_seq` (o jobId garante que o job só é criado se não existir).

4. **Retries controlados pelo motor, não pelo BullMQ.** Jobs do BullMQ usam `attempts: 1`. O motor incrementa `attempt` no Postgres e reenfileira com delay de backoff. `attempt` incrementa apenas em falha de tentativa e em stall recuperado pelo reconciler; **nunca** em retomada de `waiting`. `maxDurationMs` é por tentativa: o timer começa na aquisição e não corre enquanto a run está suspensa.

5. **Execução exclusiva por transição atômica com ownership explícito.** Um worker só executa uma run se conseguir:

   ```sql
   UPDATE runs SET status='running', execution_id=$2, heartbeat_at=now(), started_at=coalesce(started_at, now())
   WHERE id=$1 AND status IN ('queued','waiting') AND enqueue_seq=$3
   RETURNING *
   ```

   O `enqueue_seq=$3` rejeita jobs obsoletos (ex.: a retomada agendada de um `sleep` que ficou para trás porque a run já foi reenfileirada por outro caminho). Um job rejeitado termina como no-op, sem erro. Toda escrita posterior daquela execução confere ownership travando a linha da run:

   ```sql
   ... WHERE EXISTS (
     SELECT 1 FROM runs WHERE id=$1 AND execution_id=$2 AND status='running' FOR SHARE
   )
   ```

   O `FOR SHARE` impede que o reconciler ou um cancelamento troque o status entre a checagem e a escrita; sem ele, sob READ COMMITTED, uma execução zumbi ainda consegue gravar um step. `rowCount = 0` significa que a execução perdeu a run (cancelada, timeout, stall recuperado): o executor lança `ExecutionLostError`, dispara o `AbortSignal` e não grava mais nada. Isso é também a checagem de cancelamento por fronteira de step, sem query extra por step.
   **Toda saída de `running`** (completed, failed, waiting, canceled, timeout, stop graceful, stall) faz `execution_id = NULL`, para que uma execução zumbi não consiga voltar a escrever.

6. **Heartbeat, não lock do BullMQ, define stall.** O executor atualiza `runs.heartbeat_at` a cada `heartbeatIntervalMs` (padrão 10 s) com `execution_id` no WHERE. O reconciler considera stalled `status='running' AND heartbeat_at < now() - stallTimeoutMs` (padrão 60 s). O estado do job no Redis não é consultado para isso: o Postgres é a fonte da verdade, e o timeout de stall passa a ser configurável independentemente do `lockDuration` do BullMQ.
   - O heartbeat roda no mesmo event loop, então um trecho síncrono que bloqueie o loop por mais que `stallTimeoutMs` **também** vai causar stall. Documente isso e recomende `worker_threads` para trabalho CPU-bound dentro de steps.
   - O BullMQ pode marcar o job como stalled e reentregá-lo; a reentrega falha na aquisição (decisão 5) e vira no-op. Configure os workers para que o stall do BullMQ nunca falhe nem duplique nada relevante (`maxStalledCount` e `stalledInterval`).
   - O heartbeat atualiza **só** `heartbeat_at` (sem tocar `updated_at`), e `heartbeat_at` não entra em nenhum índice, para que o update seja HOT e não gere escrita de índice a cada 10 s por run ativa. `runs` é criada com `fillfactor = 80`. O stall é encontrado por um índice parcial em `status='running'`, filtrando `heartbeat_at` no conjunto pequeno de runs ativas.
   - Queries de heartbeat usam timeout curto via `SET LOCAL statement_timeout` dentro de uma transação curta. **Nunca** `SET statement_timeout` sem `LOCAL`: vale para a sessão e vaza para quem pegar a conexão do pool depois (inclusive o código do usuário, se o pool for injetado). O `query_timeout` do `pg` é só do lado do cliente e não cancela a query no servidor; se usado, é complemento, não substituto. Falha de heartbeat (inclusive por timeout de aquisição de conexão no pool) gera `warn` e não falha a run; o `stallTimeoutMs` cobre várias falhas seguidas.

7. **Job IDs determinísticos no BullMQ:** `${runId}-${seq}`, em que `seq` é `enqueue_seq` incrementado no Postgres a cada enfileiramento. Reenfileirar o mesmo `seq` é idempotente; retomadas diferentes nunca colidem. Não use `:` no jobId. `removeOnComplete`/`removeOnFail` agressivos, porque o histórico está no Postgres.

8. **Fila interna do sistema.** Ticks de scheduler, expiração de waitpoints, retenção e qualquer job que não seja "executar uma run" rodam na fila `${prefix}-system`, com worker próprio. Nunca na fila da task do usuário, para não competir com `concurrency` e `rateLimit` dele. Todo `engine.start()` sobe o worker `-system` (concorrência própria, padrão 5); a opção `queues` de `start()` filtra apenas filas de task. Engines que só disparam (sem `start()`) não sobem nenhum worker.

   **No modo Postgres:** No modo Postgres, os jobs da `-system` são linhas da tabela `system_jobs` (migration 009: `job_id text PRIMARY KEY`, `name`, `data jsonb`, `run_after`, `attempts`, `max_attempts`, `locked_until`, `locked_by`, `created_at`), reivindicadas com `SKIP LOCKED` pelo mesmo dispatcher, apagadas ao concluir. Todo claim grava um `locked_by` novo (uuid) e toda escrita de quem processou (concluir, adiar, falhar, reagendar) é condicionada a `locked_by` (fencing): um worker cujo lock venceu e foi reivindicado por outro não apaga nem reagenda o job alheio. `job_id` deduplica exatamente como no BullMQ (`wp-<id>-expire`, `retention-<janela>`, `schedule-tick:<id>`). O tick de cron é uma linha `schedule-tick:<id>` cujo `run_after` é a próxima ocorrência, calculada com `cron-parser` (dependência direta, mesma semântica de cron e timezone do BullMQ) a partir do `now()` do banco (decisão 17). A linha nunca é apagada pelo worker: ao concluir, ou ao esgotar as tentativas, ela é atualizada no lugar para a próxima ocorrência (um statement, sem janela em que o schedule some), como o scheduler do BullMQ, que não morre com um tick que falhou. Regravar um schedule (sync declarativo, `schedules.upsert`, reparo) só mexe no `run_after` quando `cron`/`timezone` mudaram; igual ao BullMQ, é no-op para o tick pendente. Sem backfill: ticks de um período sem nenhum worker `-system` são perdidos, igual a hoje; um tick em backoff atrasa os seguintes (não há ticks paralelos do mesmo schedule). A chave `__schedule:id:ts` (decisão 9) continua sendo a barreira contra duplicata.

9. **Idempotência em tabela própria, escopada por task.** Chaves ficam em `idempotency_keys(task_id, key, run_id, expires_at)` com PK `(task_id, key)` e `run_id` referenciando `runs(id) ON DELETE CASCADE`, não em coluna de `runs`. O `trigger()` gera o `runId` na aplicação e, numa transação:
   1. insere a run;
   2. executa
      ```sql
      INSERT INTO idempotency_keys (task_id, key, run_id, expires_at) VALUES ($1, $2, $3, $4)
      ON CONFLICT (task_id, key) DO UPDATE SET run_id=EXCLUDED.run_id, expires_at=EXCLUDED.expires_at
      WHERE idempotency_keys.expires_at < now()
      RETURNING run_id
      ```
   3. se o statement devolveu uma linha, a chave é desta run (inserção nova **ou** tomada de uma chave expirada): faz commit e enfileira;
   4. se não devolveu nada, a chave pertence a uma run viva: `ROLLBACK`, busca a run pela chave e a devolve sem enfileirar nada.

   Não use `xmax = 0` para distinguir os casos. Chaves geradas pela lib usam namespace reservado (`__child:${parentRunId}:${stepName}`, `__child:${parentRunId}:${stepName}:${batchIndex}` para filhas de batch, `__schedule:${id}:${ts}`); chaves de usuário começando com `__` são rejeitadas. `runs.replay()` cria uma run nova **sem** reaproveitar a chave original.
   Chaves de usuário sem TTL explícito recebem `idempotencyKeyTtl` (padrão 7 dias). A engine rejeita na criação um TTL maior que `retention.days`. Chaves internas usam `expires_at NULL` (vivas até a retenção da run).

10. **Waitpoints nascem na mesma transação que sua causa.** Em `triggerAndWait`, o insert da run filha (com `parent_run_id`, `parent_waitpoint_id`), o insert do waitpoint e a marcação do step `waiting` acontecem numa única transação, antes de qualquer enqueue. `batchTriggerAndWait` cria **um** waitpoint `kind='batch'` com `pending_count`, decrementado atomicamente; cada filha guarda `batch_index`; o pai é retomado uma única vez, quando o contador chegar a zero. `triggerAndWait` e `batchTriggerAndWait` aceitam `timeout` opcional; no timeout, o waitpoint vira `timed_out`, retorna `{ ok: false }` e as filhas pendentes são canceladas.
    Todo waitpoint com timeout (token, `child_run`, `batch`) agenda a expiração na fila `-system` com jobId determinístico `wp-${waitpointId}-expire`. O handler só age se o waitpoint ainda estiver `pending`; caso contrário é no-op. Quem resolve o waitpoint antes do prazo remove esse job (best-effort, fora da transação). Cancelar uma run cancela recursivamente todas as descendentes pendentes.
    Resultados de `triggerAndWait`/`batchTriggerAndWait` **não** são copiados para `steps.output`. O step guarda só a referência ao waitpoint; no replay, o executor lê `runs.output`/`runs.error` das filhas numa única query por `parent_waitpoint_id ORDER BY batch_index`. Tokens seguem a mesma regra: o step `${name}#wait` guarda só a referência e o replay lê `waitpoints.output`.
    **Qualquer** estado terminal de uma filha com `parent_waitpoint_id` resolve o waitpoint do pai: `completed` → `{ ok: true, output }`, `failed` definitivo → `{ ok: false, error }`, `canceled` → `{ ok: false, error: { name: "Canceled" } }`. Numa filha de batch, isso decrementa `pending_count`. Se o waitpoint já estiver `timed_out` ou `completed`, é no-op.
    `parent_run_id` e `parent_waitpoint_id` são FKs com `ON DELETE SET NULL`. A retenção só apaga uma run sem pai ou cujo pai já está finalizado. Uma filha que termina com `parent_waitpoint_id` nulo não tenta resolver nada.

11. **Detecção de não-determinismo.** Cada step é gravado com `name` e `seq_index`. No replay, se o step na posição `i` tem nome diferente do salvo, lance `NonDeterminismError`. Nome repetido na mesma execução lança `DuplicateStepError`. `createToken` e `waitForToken` são steps e consomem `seq_index`: `createToken(name)` grava o step `${name}` (output `{ id }`) e `waitForToken` grava `${name}#wait`. `waitForToken(name)` sem `createToken` prévio equivale a `createToken(name)` seguido da espera. `#` e `:` são reservados: nomes de step do usuário contendo `#` ou `:` são rejeitados (`:` separa os componentes das chaves internas `__child:*` da decisão 9).

12. **Serialização JSON explícita e limites separados.** Payloads, outputs de step e outputs de task são tipados com uma checagem estrutural `Serializable<T>` (rejeita `Date`, `Map`, funções em tempo de compilação e aceita tanto `type` quanto `interface`; um `T extends Json` com index signature rejeitaria interfaces) e validados em runtime. Limites: `maxPayloadBytes` (padrão 1 MB), `maxStepOutputBytes` (padrão 256 KB), `maxLogEntryBytes` (padrão 16 KB, truncando `data` com marcador). `tokens.complete(id, output)` valida `output` com as mesmas regras do payload. Erros gravados em `runs.error` seguem o formato `{ name, message, stack (truncado a 4 KB), cause?, retryable }`; nunca serializar o objeto de erro inteiro nem propriedades arbitrárias.

13. **Sem estado global.** Tudo passa por `createEngine(config)`. Duas engines no mesmo processo com schemas/prefixos diferentes precisam funcionar.

14. **Conexões injetáveis.** A config aceita connection string **ou** `pg.Pool` / instância `ioredis` já existente. Se a lib criou a conexão, a lib fecha; se recebeu pronta, não fecha. Uma instância `ioredis` injetada nunca é usada diretamente por workers, subscriber de pub/sub ou `XREAD BLOCK`: a lib faz `duplicate()` para cada uso que exige conexão exclusiva ou `maxRetriesPerRequest: null`, e fecha só as duplicatas que criou. Com `pg.Pool` injetado, documente que `pool.max >= concurrency total + 5` e recomende um pool separado para a lib. `bullmq`, `pg` **e `ioredis`** são `peerDependencies`.

   **No modo Postgres:** `bullmq` e `ioredis` passam a peers opcionais (`peerDependenciesMeta`); só o modo BullMQ os carrega, por um `import()` dinâmico do módulo `src/backends/bullmq.ts` (que continua importando `bullmq`/`ioredis` estaticamente; o `splitting` do tsup o põe num chunk próprio), e a ausência vira `ConfigError` com a instrução de instalação. Os tipos públicos não importam do `ioredis`: `redis.connection` é um tipo estrutural mínimo. No modo Postgres, a conta de conexões é `pool.max` (workers das filas + worker `-system` + polls) mais até duas fora do pool (líder e `LISTEN`); nesse modo o `pool.max` derivado inclui `systemConcurrency` (o `-system` passa a usar o pool). No modo BullMQ a fórmula não muda.

15. **Isolamento de namespace com identificadores validados.** Schema Postgres e `prefix` do BullMQ configuráveis (padrão `minitrigger`). Validados contra `^[a-z_][a-z0-9_]{0,62}$` na criação da engine e o schema é sempre citado com `pg.escapeIdentifier`.

   **No modo Postgres:** No modo Postgres, `prefix`, `redis` e `outbox` são recusados com `ConfigError` (configuração explícita, nada é ignorado em silêncio; `outbox` não tem efeito porque o trigger transacional já é a fila, decisão 28(j)). Canais de `NOTIFY` derivam do schema, como o do outbox (`mt_<fim>_s_<schema>` ou `mt_<fim>_h_<sha256[:48]>` quando passa de 63 bytes, com `fim` em `wake`, `status`, `cancel`, `outbox`). O dono do schema deixa de ser uma chave no Redis: a tabela `schema_meta` (migration 009) guarda `transport`; o primeiro `start()` de uma engine 0.4+ grava (nos dois modos), e um `start()` com outro modo no mesmo schema é `ConfigError` (modo misto teria duas fontes de jobs da `-system`). Engines 0.3 não gravam o marcador: para passar um schema ao modo Postgres, todas as engines BullMQ dele precisam estar em 0.4+ (ou paradas) antes.

16. **Advisory locks nunca em conexão de pool solta.** Migrations usam `pg_advisory_xact_lock` dentro de uma transação. O reconciler e a retenção usam `pg_try_advisory_lock` num **client dedicado** mantido fora do pool enquanto o lock estiver ativo. Se esse client cair: trate o evento `error`, descarte o client, marque o papel de líder como perdido e tente readquirir num client novo no próximo ciclo. Locks de dois inteiros: `(hashtext(schema), 1)` para migrations e `(hashtext(schema), 2)` para o líder.

   **No modo Postgres:** A conexão `LISTEN` fica fora do pool, com o mesmo tratamento do client do líder (keepalive TCP, reconexão com backoff 0,5–30 s, fechamento limitado no `stop()`). É uma só por engine iniciada, para todos os canais do modo (no Postgres: wake, status, cancel; sem `outbox`, que não existe nesse modo). O listener do outbox do modo BullMQ (decisão 26) é reescrito sobre o mesmo cliente genérico, sem mudar de comportamento. Um pooler em modo transação (PgBouncer) quebra `LISTEN` e o advisory lock de sessão do líder: as duas conexões dedicadas devem apontar direto para o Postgres, ou o sistema cai para o poll (decisão 22) e a eleição de líder deixa de ser confiável (o lock fica na conexão de servidor que o tomou: líderes duplicados ou instáveis, às vezes nenhum; não é um "sem líder" limpo, e nenhum autoteste detecta isso) (emenda aprovada pelo dono em 2026-09-27).

17. **Tempo é do banco.** Comparações com `wake_at`, `expires_at`, `run_after`, `heartbeat_at` usam `now()` do Postgres, nunca `Date.now()`. Delays calculados para o BullMQ partem de valores devolvidos pelo banco.

18. **Migrations embutidas e versionadas**, executadas por `engine.migrate()`, registradas em `schema_migrations`, SQL embutido no bundle (não ler arquivo do disco em runtime).

19. **Separação cliente/worker.** A mesma engine pode só disparar e consultar sem iniciar workers. Workers só existem após `engine.start()`.

20. **Shutdown graceful com devolução.** `engine.stop()` para de aceitar jobs e espera os ativos até `shutdownTimeoutMs`. Uma run que ultrapassa o timeout volta para `queued` com `execution_id = NULL`, **sem** contar tentativa, e é reenfileirada (`enqueue_seq + 1`). O `AbortSignal` é disparado e o ownership impede qualquer escrita.

21. **Schedules declarativos não apagam nada por padrão.** O sync no `start()` cria e atualiza schedules declarados em `defineTask`; remoção é opt-in (`schedules: { pruneDeclarative: true }`).

   **No modo Postgres:** No modo Postgres, o sync declarativo grava a linha em `schedules` e faz o upsert do `schedule-tick` em `system_jobs` (no-op para o tick pendente quando `cron` e `timezone` não mudaram, decisão 8); a remoção continua opt-in (`pruneDeclarative`). O hook de reconciliação Postgres↔BullMQ vira a reconciliação `schedules`↔`system_jobs` (linha sem tick → upsert; tick sem linha → remoção, com a mesma re-checagem antes de remover).

22. **Checagens de ambiente são avisos, não falhas fatais**, quando o comando não está disponível (`CONFIG GET maxmemory-policy` bloqueado em Redis gerenciado). Falhar apenas quando a checagem responde e está errada.

   **No modo Postgres:** No modo Postgres, as checagens do Redis não rodam e `checkEnvironment` aceita `redis` ausente. Entra um autoteste de `LISTEN` no `start()`: `NOTIFY` no canal de wake e espera de até 1 s pelo eco; sem eco, `warn` "notificações não chegam (pooler em modo transação?); a latência de wake é o poll" e o sistema segue só com o poll.

23. **Progresso por run.** `ctx.progress.set(value)` mantém um único valor mutável por run (`runs.progress`, migration 004, coluna não indexada), escrito só pela execução dona (mesma guarda de ownership da decisão 5). Não é step: não participa da checagem de não-determinismo nem consome `seq_index`. `value` é validado como um payload (`Serializable`, `limits.maxProgressBytes`, padrão 16 KB); estourar o limite lança `SerializationError` comum, capturável pelo usuário (ao contrário de um argumento inválido de `ctx.step.*`, que é fatal). Gravações são agrupadas (no máximo uma a cada 250 ms; só o último valor de uma rajada é escrito) e sempre esvaziadas antes da próxima escrita terminal ou de suspensão da run (mesmo ponto onde `ctx.logger` é esvaziado). Publicado no pub/sub best-effort (mesmo canal `run-status`, sem evento local dedicado); `runs.subscribe` emite quando só o progresso muda. Sobrevive a retry e a retomada — a engine nunca zera o valor sozinha.

24. **Debounce por chave.** `trigger(task, payload, { debounce: { key, delay, payload?, maxDelay? } })` colapsa triggers com o mesmo `(task_id, key)` numa única run **pendente**: `queued` e ainda não iniciada (`runs.debounce_key`, migration 005, com índice único parcial `(task_id, debounce_key) WHERE status = 'queued' AND debounce_key IS NOT NULL`). Numa transação, `SELECT ... FOR UPDATE` da run pendente da chave; se existe: sempre trailing (`run_after = least(now() + delay, debounce_deadline)`); `payload: "last"` (padrão) troca o payload, `"first"` mantém; **sempre** `enqueue_seq + 1` (o `acquire` não olha `run_after`, só o seq cerca o job antigo), e o chamador enfileira o seq novo e remove o antigo best-effort. O colapso nunca muda `concurrency_key`, `debounce_deadline` (o teto de `maxDelay`) nem `max_attempts` da run pendente — só `run_after` e, conforme `payload`, o payload; esses outros campos ficam fixos no primeiro trigger que criou a run. Se não existe: insere com `debounce_key` e `debounce_deadline = now() + maxDelay` (NULL sem `maxDelay`); violação do índice único (corrida entre processos) faz rollback ao savepoint e repete, colapsando na run vencedora. Sem `xmax`. O `acquire` zera `debounce_key` (a chave é liberada quando a run sai de `queued` pela primeira vez; uma run que volta a `queued` — retry, retomada, devolução — nunca absorve triggers). Um trigger com a run da chave já `running` cria uma nova pendente. A regra vive no Postgres (sobrevive a `FLUSHALL`; o reconciler reenfileira o seq atual). Não combina com `idempotencyKey` nem com `delay`; a chave segue as regras de chave de usuário (não vazia, até 512 caracteres, prefixo `__` reservado). Um trigger colapsado não muda o status e não publica nada.

25. **Concurrency key por fila.** Uma fila que declara `concurrencyKey: { limit }` executa no máximo `limit` runs `running` com a mesma `trigger(..., { concurrencyKey })` (`runs.concurrency_key`, migration 005, índice parcial `(queue, concurrency_key) WHERE status = 'running'`). Só `engine.trigger` aceita a chave: filhas de `step.trigger*` numa fila com `concurrencyKey` rodam sem limite (`runs.replay()` preserva a chave da run original). A aquisição dessas runs é uma transação: `SELECT ... FOR UPDATE` da run (mesmo predicado da decisão 5), `pg_advisory_xact_lock(hashtextextended(schema || ':' || queue || ':' || key, 0))` (lock de um bigint, num espaço separado dos locks de dois inteiros da decisão 16), contagem das `running` da mesma `(queue, key)`; no limite, a run não é adquirida: `enqueue_seq + 1`, status e `attempt` inalterados, sem publicar status, e o executor reenfileira. O atraso da nova tentativa cresce por tentativa `limited` consecutiva da run (`runs.concurrency_waits`, migration 005): `run_after = now() + min(1 s * 2^waits, 30 s)` mais jitter; `waits` volta a 0 quando a run é adquirida (por `acquire` ou `acquireWithConcurrencyLimit`). Não é FIFO: nada garante que a run que espera há mais tempo seja a próxima a adquirir a chave quando uma vaga libera; combinar com `debounce` (decisão 24) dá o padrão "um pendente + um rodando" por chave. Só `running` conta (uma run `waiting` não ocupa vaga). O limite vem do registro de filas do processo que recebe o job; um processo que não conhece a configuração adquire sem a checagem — mantenha a configuração idêntica entre deploys. Usar `concurrencyKey` numa fila sem `concurrencyKey` é `ConfigError`. Um processo derrubado com uma run `running` segura a vaga da chave até a recuperação de stall (`stallTimeoutMs`) liberá-la.

26. **Trigger na transação do chamador (outbox).** `trigger(task, payload, { tx })` recebe um client do chamador (`PoolClient`/`Client` do `pg` já em `BEGIN`, ou qualquer adaptador `{ query(text, values) }` com placeholders `$n` — a costura para adaptadores de ORM) e insere a run **nessa** transação: a escrita de negócio e a run fazem commit ou rollback juntas. Nada é enfileirado antes do commit (o job não pode rodar antes do commit nem depois de um rollback): a run nasce `queued` com `runs.outbox_pending = true` (migration 006, índice parcial `(created_at) WHERE outbox_pending`) e o mesmo `INSERT` executa `pg_notify(canal, '')`. O canal é por schema (`mt_outbox_s_<schema>`, ou `mt_outbox_h_<sha256>` se passar de 63 bytes); o payload é constante, então o Postgres entrega **uma** notificação por transação, por mais triggers que ela tenha, e nada é lido do payload (um NOTIFY forjado ou perdido só causa ou atrasa uma varredura). Todo `engine.start()` mantém um client **dedicado** (fora do pool, como o do líder da decisão 16; keepalive TCP; reconexão com backoff; fechamento limitado no `stop()`) com `LISTEN` no canal; a cada notificação, a cada (re)conexão/`start()` e a cada 2 s (poll), varre `UPDATE runs SET outbox_pending = false ... FOR UPDATE SKIP LOCKED` em lotes de 500 (vários processos nunca despacham a mesma run duas vezes; varreduras concorrentes do mesmo processo são agrupadas), publica `queued` (pub/sub + evento local no processo que despachou; o `trigger` com `tx` não publica nada) e enfileira o `enqueue_seq` atual com o delay calculado pelo banco. Engines só de disparo não escutam. Cada engine iniciada segura **duas** conexões fora do pool (líder e `LISTEN`); todo NOTIFY acorda todas as engines iniciadas do schema (varreduras agrupadas, claim exatamente-uma-vez); um backend em `LISTEN` travado (sem ler notificações) segura a fila de NOTIFY do cluster (8 GB), e com ela cheia os COMMITs que notificam falham — mitigado pelo keepalive TCP e pela reconexão do client dedicado. Latência: milissegundos com o listener ativo; até o poll (2 s) com a notificação perdida (listener reconectando, pooler em modo transação como o PgBouncer, que quebra `LISTEN`); na partida da próxima engine se nenhuma estava iniciada; um enqueue perdido depois do claim cai na decisão 1 (reconciler: `intervalMs` + `graceMs`). O NOTIFY custa um lock global no commit (serializa commits que notificam no banco inteiro) — um por transação, não por run. Com `idempotencyKey`, claim da chave (decisão 9) e insert da run são **um único statement** (CTE `INSERT ... ON CONFLICT ... WHERE expires_at < now() RETURNING` alimentando `INSERT INTO runs ... WHERE EXISTS (claim)`; o FK da chave é checado no fim do statement), sem `SAVEPOINT`: nenhuma subtransação por trigger (centenas de triggers numa transação não estouram o cache de subxids do backend), a transação do chamador continua usável quando a chave já pertence a uma run viva (nada é inserido nem notificado; a run dona é lida num segundo statement), um client sem `BEGIN` também funciona (cada trigger faz autocommit) e a linha da chave fica travada até o commit dele. No client do chamador só rodam statements parametrizados e qualificados pelo schema — nunca `SET` sem `LOCAL`, `LISTEN` ou outro estado de sessão. `now()` é o início da transação do chamador (`delay` conta a partir dele). Não combina com `debounce` (colapsar travaria a run pendente compartilhada, e os workers dela, até o fim da transação do chamador) nem com `createTestEngine` (`ConfigError`); um `pg.Pool` como `tx` é `ConfigError`. Adaptadores de ORM (adição, sem mudar o acima): subpaths `@rivia-labs/minitrigger/typeorm|knex|drizzle|prisma` exportam `typeormTx(manager|queryRunner)`, `knexTx(trx)`, `drizzleTx(tx, sql)` e `prismaTx(tx)`, que embrulham o handle de transação do ORM num `TransactionClient` (tipos estruturais, nenhum import do ORM, nenhuma dependência nova); o SQL continua parametrizado (knex: `$n` → `?` com um binding por ocorrência e `?` literal escapado; drizzle: `sql.param`), e um handle fora de transação detectável é `ConfigError`. Um handle de ORM cru passado como `tx` é `ConfigError` apontando o adaptador (nunca embrulhado implicitamente). O poll de 2 s e os lotes de 500 acima são só os defaults: `outbox.pollIntervalMs`, `outbox.batchSize` e `outbox.reconnectMaxMs` são configuráveis por `createEngine` (com limites — ver `CLAUDE.md`), e `outbox.notify: false` faz o `trigger(..., { tx })` pular o `pg_notify` no commit, contando só com o poll (correção autorizada pelo dono, 2026-09-25).

27. **Pausa de fila persistida no Postgres, espelhada no BullMQ, ressincronizada pelo líder.** `engine.queues.pause(name)`/`resume(name)` gravam primeiro `queue_state(queue text PK, paused boolean, updated_at)` (migration 007; o Postgres é a fonte da verdade, decisão 1; fila sem linha = não pausada) e só depois aplicam a pausa **global de fila do BullMQ** (`Queue.pause()`/`resume()`, o flag `paused` no hash `meta` do Redis), não `Worker.pause()` por processo: o flag vale na hora para todo worker de todo processo (inclusive processos que não assinam nada e versões anteriores durante um rolling deploy), sobrevive ao restart dos processos e é O(1) no BullMQ 6 (os jobs continuam em `wait`; o `moveToActive` só deixa de buscá-los). Por isso não há mensagem nova de pub/sub: a propagação é o próprio Redis, e o que pode divergir (FLUSHALL apaga o flag; a chamada ao Redis falhou depois do commit; duas chamadas concorrentes de processos diferentes aplicam os flags em ordem trocada) é corrigido a partir do Postgres (a) por todo `engine.start()`, **antes** de iniciar os workers (falha aqui falha o `start()`, que pode ser repetido: iniciar os workers poderia rodar jobs de uma fila pausada), (b) pelo tick do líder, como primeiro caso do reconciler (antes do caso 1 re-adicionar jobs perdidos), e (c) pela própria chamada, que relê a linha depois de aplicar o flag e reaplica se outra escrita venceu. Falha do Redis na chamada é só `warn` (o estado já está gravado). Pausar nunca aborta execuções em andamento (terminam ou suspendem normalmente); retries, retomadas e sleeps da fila esperam enquanto ela está pausada; jobs podem ser adicionados. Só filas de tarefas: a `-system` (decisão 8) é recusada (`ConfigError`), assim como um nome que nenhuma task registrada usa e que não tem linha persistida. Depois de um FLUSHALL, a fila fica despausada no Redis até o próximo tick do líder (`reconciler.intervalMs`). `queues.scale(name, n)` muda só a concorrência do worker **deste** processo (`Worker.concurrency`, lido a cada volta do laço de busca do BullMQ), não é persistido nem propagado. `createTestEngine` segura os jobs de fila pausada (nunca são tomados nem movem o relógio). O tick do líder também lê quais filas estão pausadas (depois de reaplicar os flags, caso acima) e as exclui dos casos 1/2 do reconciler (`listStaleQueued`/`listStaleWaiting`, cláusula SQL `AND NOT (r.queue = ANY($n::text[]))`): um backlog grande de uma fila pausada nunca atrasa achar jobs de verdade perdidos de outra fila; falha ao ler o estado de pausa não exclui nenhuma fila naquele tick (só `warn`).

28. **Transporte Postgres.** `createEngine({ transport: "postgres" })` monta o backend com store SQL, transporte Postgres, pub/sub por `NOTIFY` e sem streams. Regras: (a) claim por fila com `LIMIT` igual às vagas livres do worker deste processo, ordem `run_after, created_at, id`, num statement só; a transação do claim termina antes de executar (nenhum lock de linha é segurado durante a execução; a posse é `execution_id`, decisão 5); (b) wake: todo enqueue de hoje (insert, retomada, retry, hand-back, recuperação de stall, colapso de debounce), com ou sem atraso, vira `pg_notify(wake, '<fila>')`. No `trigger` com `tx` o `NOTIFY` vai no próprio insert (entregue no commit); nos demais caminhos é emitido depois do commit, pelo transporte (uma perda aqui só custa latência: o poll cobre). Um `NOTIFY` de run atrasada faz o worker recalcular `min(run_after)`, então nenhum processo espera um intervalo inteiro de poll por uma run que outro processo agendou mais cedo. O nome da fila no payload é **dica**: o listener só acorda o worker daquela fila se ela está registrada, e qualquer payload inesperado acorda todas — payload de `NOTIFY` é sempre tratado como não confiável (qualquer role com `CONNECT` pode notificar e escutar qualquer canal); (c) poll adaptativo: após um claim vazio o worker lê `min(run_after)` da fila e dorme até lá, limitado por `polling.intervalMs` (padrão 1 s), com jitter de até `polling.jitterMs` (padrão 50 ms) para diluir o rebanho de trovão dos `NOTIFY`; (d) `pg_notify` de status e cancel carregam só `runId` (limite de 8000 bytes do Postgres; nunca progresso), e o assinante relê `runs` antes de agir: para status, antes de emitir (`runs.subscribe` mantém a mesma ordem obrigatória de hoje); para cancel, só aborta a execução local se a run relida está `canceled` (um cancel forjado nunca derruba uma execução). As publicações de um processo são agrupadas numa janela curta (5 ms) e saem num único statement (`SELECT pg_notify(c, p) FROM unnest(...)`), para não pagar um commit com o lock global de `NOTIFY` por transição; (e) `globalConcurrency` é exato: com limite, o claim primeiro toma `pg_advisory_xact_lock(hashtextextended('<schema>:<fila>:global', 1))` (semente 1, para nunca colidir com a chave da decisão 25 quando `concurrencyKey === "global"`) e conta os `running` da fila uma vez (índice parcial `(queue) WHERE status = 'running'`), então claims concorrentes da mesma fila se serializam e nunca passam do limite; sem limite, não há lock. Nos dois caminhos o claim roda numa transação curta com `SET LOCAL statement_timeout` (limite no servidor, para um claim nunca terminar depois do limite de dreno do `stop()`), e o "vencido" é avaliado com `statement_timestamp()` (emenda aprovada pelo dono em 2026-09-26). O worker que termina uma execução numa fila com limite emite o `NOTIFY` de wake dela (a vaga liberada acorda outros processos). Pausa (decisão 27) é lida de `queue_state` no próprio claim; `scale` continua local; (f) `queue.rateLimit`, `ctx.stream` e `runs.readStream` são `ConfigError` neste modo; (g) `stop()`: os laços de claim param de reivindicar, esperam o claim em andamento terminar (as runs que ele trouxer entram no dreno e no hand-back), as execuções drenam com `shutdownTimeoutMs`, o hand-back é `release` mais um `NOTIFY`, tudo limitado; (h) Postgres indisponível: polls e claims falham com backoff e `warn` throttled (padrão do listener do outbox); execuções em voo já viram `lost` pelo heartbeat; timeouts de conexão no `start()` são `MinitriggerError` (infraestrutura transitória, como na quarta revisão externa), nunca `ConfigError`; (i) `createTestEngine` não muda de comportamento, mas deixa de importar `bullmq` em runtime (o `DelayedError` vira `SystemJobDelayed`, próprio da lib); (j) a decisão 26 neste modo é um caso particular da 1: `trigger` com `tx` é o mesmo insert com `NOTIFY` de wake no `RETURNING` e `outbox_pending = false`; sem varredura de outbox.

---

## 3. API pública alvo

```ts
import { createEngine, defineTask } from "@rivia-labs/minitrigger";

export const resumirVaga = defineTask({
  id: "resumir-vaga",
  queue: {
    name: "llm",
    concurrency: 2,
    rateLimit: { max: 10, durationMs: 60_000 },
  },
  retry: {
    maxAttempts: 3,
    backoff: { type: "exponential", baseMs: 1_000, maxMs: 60_000 },
  },
  maxDurationMs: 5 * 60_000, // por tentativa, não conta tempo suspenso
  run: async (payload: { url: string }, ctx) => {
    const html = await ctx.step.run("baixar", () =>
      fetch(payload.url).then((r) => r.text()),
    );
    const resumo = await ctx.step.run("llm", () =>
      chamarLLM(html, { signal: ctx.signal }),
    );

    const token = await ctx.step.createToken("aprovacao", { timeout: "2d" });
    await ctx.step.run("pedir-aprovacao", () => enviarLinkAprovacao(token.id));
    const aprovado = await ctx.step.waitForToken<boolean>(token);
    if (!aprovado.ok || !aprovado.output)
      return { status: "rejeitado" as const };

    await ctx.step.sleep("esfriar", "1h");
    const filho = await ctx.step.triggerAndWait(
      "salvar",
      salvarResumo,
      { resumo },
      { timeout: "1h" },
    );
    await ctx.step.trigger("notificar", notificar, { url: payload.url }); // dispara sem esperar
    ctx.logger.info("finalizado", { filhoOk: filho.ok });
    await ctx.progress.set({ step: "concluido" }); // runs.progress; não é step, sem seq_index
    return { status: "ok" as const, resumo };
  },
});

const engine = createEngine({
  postgres: process.env.DATABASE_URL!, // ou { pool }
  redis: process.env.REDIS_URL!, // ou { connection }
  schema: "minitrigger",
  prefix: "minitrigger",
  tasks: [resumirVaga, salvarResumo, notificar],
});

// Modo só-Postgres (decisão 28): sem redis nem prefix; ctx.stream, runs.readStream e
// queue.rateLimit são ConfigError neste modo.
const engineSemRedis = createEngine({
  postgres: process.env.DATABASE_URL!,
  transport: "postgres",
  schema: "minitrigger",
  tasks: [resumirVaga, salvarResumo, notificar],
  polling: { intervalMs: 1_000, jitterMs: 50 },
});

await engine.migrate();
await engine.start(); // opcional: { queues: ["llm"] }

const handle = await engine.trigger(
  resumirVaga,
  { url: "..." },
  { idempotencyKey: "vaga-123", delay: "5m" },
);
await engine.runs.get(handle.id);
await engine.runs.list({ taskId: "resumir-vaga", status: "failed", limit: 20 });
await engine.runs.cancel(handle.id);
await engine.runs.replay(handle.id);
await engine.tokens.complete(tokenId, true);
await engine.schedules.upsert({
  id: "diario",
  task: resumirVaga,
  cron: "0 9 * * *",
  timezone: "America/Belem",
  payload: { url: "..." },
});

await engine.stop();
```

Adições das decisões 24-27 (outbox transacional, debounce, concurrency key, controle de fila):

```ts
// Decisão 26: trigger na transação do chamador (outbox); não enfileira nada até o commit.
// `tx` precisa ser o client da SUA transação (BEGIN/COMMIT/ROLLBACK), nunca o `pg.Pool` — cada
// query de um Pool roda numa conexão diferente, fora da transação do chamador.
const client = await pool.connect();
try {
  await client.query("BEGIN");
  await client.query("INSERT INTO orders ...");
  await engine.trigger(notificar, { url: "..." }, { tx: client });
  await client.query("COMMIT");
} catch {
  await client.query("ROLLBACK");
} finally {
  client.release();
}

// Decisão 24: debounce por chave (sempre trailing; "last" troca o payload, "first" mantém).
await engine.trigger(
  resumirVaga,
  { url: "..." },
  { debounce: { key: `vaga:${vagaId}`, delay: "10s", maxDelay: "2m", payload: "last" } },
);

// Decisão 25: concurrency key por fila (a fila precisa declarar `concurrencyKey: { limit }`).
await engine.trigger(processarCurso, { courseId }, { concurrencyKey: courseId });

// Decisão 27: pausa/retomada/escala de fila e operações em lote.
await engine.queues.pause("llm");
await engine.queues.resume("llm");
await engine.queues.scale("llm", 10); // só este processo
await engine.runs.cancelMany({ taskId: "resumir-vaga", limit: 500 });
await engine.runs.retryFailed({ queue: "llm", createdFrom: new Date(Date.now() - 86_400_000), limit: 100 });
```

Requisitos de tipos:

- Tipo `Serializable<T>` exportado e aplicado a payloads, outputs de step e outputs de task, funcionando com `type` e `interface`.
- `defineTask` infere `Payload` e `Output` a partir de `run`.
- `trigger`, `step.trigger`, `step.triggerAndWait` e `step.batchTriggerAndWait` só aceitam o payload correto para aquela task.
- `triggerAndWait`, `batchTriggerAndWait` (por item) e `waitForToken` retornam `{ ok: true, output } | { ok: false, error }`, sem lançar exceção para falha da subtask ou timeout.
- `ctx` expõe: `step`, `logger`, `signal` (AbortSignal: cancelamento, `maxDurationMs`, perda de ownership ou shutdown), `run` (`{ id, taskId, attempt, createdAt }`), `progress.set(value)` (decisão 23).

---

## 4. Estrutura do repositório

```
minitrigger/
├── src/
│   ├── index.ts              # exports públicos da entrada principal
│   ├── engine.ts             # createEngine, ciclo de vida
│   ├── config.ts              # resolução/validação de EngineConfig
│   ├── registry.ts            # registro de tasks/filas (buildRegistry)
│   ├── task.ts                # defineTask, tipos, Serializable<T>
│   ├── executor/             # executeRun, contexto de step, sinais, heartbeat
│   ├── queue/                # adaptação BullMQ (workers, fila -system, jobIds, schedulers)
│   ├── backends/             # bullmq.ts (import() dinâmico), postgres.ts
│   ├── pg/                   # listener.ts, transport.ts, workers.ts, pubsub.ts, schedulers.ts
│   ├── store/                # interfaces de store + repositórios SQL + memória (modo de teste)
│   ├── migrations/           # 001_init.sql, ... embutidos no bundle
│   ├── adapters/              # subpaths /typeorm, /knex, /drizzle, /prisma (decisão 26)
│   ├── testing/                # subpath /testing: createTestEngine, runTask, helpers de asserção
│   ├── outbox.ts               # canal + listener LISTEN/NOTIFY do trigger transacional (decisão 26)
│   ├── telemetry.ts            # attachDefaultLogger
│   ├── schedules.ts            # engine.schedules, sync declarativo, tick handler
│   ├── env-check.ts            # checkEnvironment (decisão 22)
│   ├── pubsub.ts               # canal run-status, cancelamento entre processos
│   ├── streams.ts              # ctx.stream / runs.readStream
│   ├── logs/                  # ctx.logger em lote (buffer.ts)
│   ├── leader.ts
│   ├── reconciler.ts
│   ├── retention.ts
│   ├── errors.ts
│   └── utils/                # duration, backoff, serialização, identificadores
├── test/
├── examples/basic/           # projeto consumidor real
├── docker-compose.yml        # postgres 16 + redis 7
├── CLAUDE.md
└── README.md
```

A API pública fica nas entradas em `package.json`'s `exports`: a entrada principal
(`src/index.ts`) e os subpaths `@rivia-labs/minitrigger/testing`, `/typeorm`, `/knex`, `/drizzle`
e `/prisma` (cada um com seu próprio `src/<nome>/index.ts` ou `src/adapters/<nome>.ts`, ver acima).
Nada de importar caminhos internos fora dessas entradas no exemplo.

---

## 5. Fases

### Fase 1 — Fundação do pacote

- `package.json` com `"type": "module"`, build via **tsup** gerando ESM + CJS + `.d.ts`, campo `exports` correto, `files: ["dist"]`, `sideEffects: false`.
- Script `prepare` que roda o build.
- `bullmq`, `pg` e `ioredis` como **peerDependencies** (e devDependencies), com faixas compatíveis entre si.
- Biome (lint + format, sem ESLint/Prettier), Vitest, `docker-compose.yml` com Postgres 16 e Redis 7 (com `maxmemory-policy noeviction`).
- Utilitários: parser de duração (`"5m"`, `"2d"`, número em ms), backoff com jitter, validação de serialização e tamanho, validação de identificadores, formatação de erro.
- Infra de testes de integração: **um schema Postgres e um prefix Redis por arquivo de teste**, criados e removidos pelo setup.

**Aceite:** `pnpm build` gera `dist` válido; testes unitários dos utilitários passam; `examples/basic` importa o pacote via `file:../..` e compila.

### Fase 2 — Schema e store

- Migration `001_init.sql` no schema configurável:
  - `runs`: `id uuid`, `task_id`, `queue`, `payload jsonb`, `status` (`queued|running|waiting|completed|failed|canceled`), `output jsonb`, `error jsonb`, `attempt int`, `max_attempts int`, `execution_id uuid NULL`, `enqueue_seq int`, `heartbeat_at`, `run_after`, `parent_run_id` e `parent_waitpoint_id` (FKs `ON DELETE SET NULL`), `parent_step_name`, `batch_index int`, `started_at`, `finished_at`, `created_at`, `updated_at`. **Sem** colunas de idempotência.
  - `idempotency_keys(task_id text, key text, run_id uuid REFERENCES runs(id) ON DELETE CASCADE, expires_at timestamptz NULL, PRIMARY KEY (task_id, key))`.
  - `steps`: PK `(run_id, name)`, `seq_index int`, `status` (`completed|waiting`), `output jsonb`, `wake_at`, `created_at`.
  - `waitpoints`: `id uuid`, `run_id`, `step_name`, `kind` (`token|child_run|batch`), `status` (`pending|completed|timed_out`), `pending_count int`, `output jsonb`, `expires_at`, índice único `(run_id, step_name)`.
  - `run_logs`: `id bigserial`, `run_id`, `level`, `message`, `data jsonb`, `ts`.
- `runs` com `fillfactor = 80`.
- Índices: `runs(status, run_after)`; `runs(id) WHERE status='running'` (parcial, sem `heartbeat_at`); `runs(finished_at) WHERE status IN ('completed','failed','canceled')`; `runs(task_id, created_at desc, id desc)`; `runs(parent_run_id)`; `runs(parent_waitpoint_id, batch_index)`; `waitpoints(run_id) WHERE status='pending'`; `waitpoints(expires_at) WHERE status='pending'`; `run_logs(run_id, id)`; `idempotency_keys(run_id)`. Não indexar `heartbeat_at` nem `updated_at`.
- `engine.migrate()` com `pg_advisory_xact_lock` numa transação, tabela `schema_migrations`, SQL embutido.
- Defina **primeiro** as interfaces `RunStore`, `StepStore`, `WaitpointStore`, `IdempotencyStore`, `LogStore` e um `withTransaction` em `store/types.ts`. Executor e engine dependem só dessas interfaces; a implementação em memória da fase 9 implementa as mesmas.
- Repositórios SQL tipados. Operações multi-tabela em transação. Inserção de logs via `INSERT ... SELECT FROM unnest($1::uuid[], $2::text[], ...)`.
- Listagem com cursor composto `(created_at, id)` e `limit` máximo 100.

**Aceite:** `migrate()` em 5 chamadas paralelas aplica uma vez só; testes de integração dos repositórios passam; teste de idempotência cobre chave nova, chave viva, chave expirada tomada por outra run, duas inserções concorrentes com a mesma chave e chave de usuário sem TTL recebendo o padrão.

### Fase 3 — Trigger, fila e execução simples (sem steps)

- `defineTask`, registro de tasks na engine (erro claro em ID duplicado).
- Configuração de fila compartilhada: `concurrency`, `rateLimit` e concorrência global são por fila. Agrupe tasks por `queue.name`: tasks que só referenciam o nome herdam; valores diferentes para a mesma fila → erro claro listando tasks e valores. Valide nomes de fila (`^[a-z0-9_-]{1,64}$`) e rejeite o nome reservado da fila `-system`.
- `engine.trigger()`: valida payload, fluxo de idempotência da decisão 9, grava `run_after`, depois enfileira com jobId `${runId}-${seq}` e `delay` se houver.
- Um `Queue` e um `Worker` do BullMQ **por fila declarada**, com `concurrency` local e `setGlobalConcurrency` / `limiter`. Fila `${prefix}-system` com worker próprio, sempre iniciado em `start()`. Conexões Redis de workers, subscriber e streams sempre dedicadas, com `maxRetriesPerRequest: null` nos workers.
- Executor: aquisição com `enqueue_seq`, heartbeat, gravação de `completed`/`failed` com ownership e `execution_id = NULL` em toda saída de `running`.
- Retries pelo motor com backoff. Erros `AbortTaskRunError` não fazem retry.
- `maxDurationMs` via `AbortSignal` + timeout que falha a tentativa.
- `engine.stop()` conforme decisão 20; fecha só as conexões que a lib criou.

**Aceite:** testes de sucesso, falha com retry até `maxAttempts`, idempotência, delay, timeout, job obsoleto (seq antigo) virando no-op, dois workers competindo pela mesma run (só um executa), `stop()` devolvendo uma run lenta para `queued` sem incrementar `attempt`, `start({ queues: ["llm"] })` subindo também o worker `-system`, duas tasks na mesma fila com `concurrency` diferente falhando na criação da engine e duas tasks na mesma fila, uma declarando e outra só referenciando, funcionando.

### Fase 4 — Steps, replay e sleep

- `ctx.step.run(name, fn)`: no início da execução, carregue os steps numa única query:
  ```sql
  SELECT name, seq_index, status, wake_at,
         CASE WHEN sum(pg_column_size(output)) OVER () <= $2 THEN output END AS output,
         sum(pg_column_size(output)) OVER () > $2 AS outputs_deferred
  FROM steps WHERE run_id = $1 ORDER BY seq_index
  ```
  `$2` configurável (padrão 16 MB). Só quando `outputs_deferred` for verdadeiro os outputs são carregados em lotes por faixa de `seq_index`, sob demanda. Steps de `#wait` e de subtasks não têm output próprio.
- Cache hit devolve o output; miss executa, valida serialização e `maxStepOutputBytes`, grava com `INSERT ... ON CONFLICT DO NOTHING` condicionado ao ownership. `rowCount = 0` → `ExecutionLostError`.
- `seq_index` na chamada, `DuplicateStepError`, `NonDeterminismError`.
- `ctx.step.sleep(name, duration)`: primeira vez grava step `waiting` com `wake_at = now() + duração` e lança `SuspendSignal`. No replay, se `wake_at <= now()`, vira `completed` e segue.
- Suspensão: `Promise.allSettled` dos steps em voo; depois a transação de suspensão da decisão 3.
- Run suspensa **não ocupa slot**.
- Suspensão autoritativa.

**Aceite:** crash depois de 2 de 4 steps sem reexecutar os 2 primeiros; `Promise.all` de 3 `step.run` determinístico; `Promise.all([step.run, step.sleep])` sem reexecução; não-determinismo; escrita de step após perda de ownership; `try/catch` em volta de `step.sleep` não quebra replay nem gera retry; step em voo que falha durante a suspensão reexecuta no replay sem contar tentativa.

### Fase 5 — Waitpoints: tokens e subtasks

- `ctx.step.createToken(name, { timeout })` / `ctx.step.waitForToken(tokenOrName)`; expiração na `-system` com jobId `wp-${waitpointId}-expire`; steps `${name}` e `${name}#wait`.
- `engine.tokens.complete(id, output)`: valida, trava a run, completa só se `pending`, reenfileira só se `waiting` sem esperas.
- `ctx.step.trigger(name, task, payload)` com chave `__child:${parentRunId}:${stepName}`, memoiza o handle.
- `ctx.step.triggerAndWait(name, task, payload, { timeout? })`: transação única da decisão 10.
- `ctx.step.batchTriggerAndWait(name, task, payloads[], { timeout? })`: waitpoint `batch`, `pending_count`, `batch_index`, chave `__child:${parentRunId}:${stepName}:${batchIndex}`.
- Cancelamento recursivo.

**Aceite:** token completado, expirado, completado duas vezes (no-op), dois tokens simultâneos num `Promise.all` (retomada exatamente uma vez), output inválido rejeitado, subtask sucesso/falha/timeout cancelando a filha, batch com 20 filhas retomando o pai uma vez, cancelamento recursivo em três níveis, token completado com a run `running` (retomada imediata), batch com filhas de 1 MB cada sem violar `maxStepOutputBytes`, filha de `step.trigger` sobrevivendo à retenção do pai, token com output de 1 MB, replay que recria token com timeout sem segundo job de expiração, `runs.cancel(childId)` em filha de `triggerAndWait` e de batch retomando o pai com `{ ok: false, error: { name: "Canceled" } }`, pai waiting em token + sleep de 1 s acordando uma única vez quando o token é completado antes do sleep vencer.

### Fase 6 — Resiliência operacional

- **Líder** (`leader.ts`): client dedicado com `pg_try_advisory_lock` e reconexão. Reconciler e retenção só no líder.
- **Reconciler** (padrão 15 s):
  - `queued` com `run_after < now() - grace` → reenfileira com o `enqueue_seq` atual;
  - `waiting` sem waitpoint pendente e `run_after < now() - grace` → reenfileira com o **mesmo** `enqueue_seq`;
  - `waiting` com `run_after IS NULL` e sem waitpoint pendente → `run_after = greatest(max(wake_at), now())`, `enqueue_seq + 1`, reenfileira, `logger.warn`;
  - `running` com `heartbeat_at < now() - stallTimeoutMs` → `queued`, `execution_id = NULL`, `attempt + 1` (ou `failed`), reenfileira;
  - waitpoints `pending` vencidos → expira e retoma;
  - reconciliação bidirecional de schedules (fase 8).
- Teste de `FLUSHALL` (só se Redis local **e** `NODE_ENV=test`).
- **Cancelamento:** `canceled` + `execution_id = NULL`, descendentes, remove jobs, pub/sub para `AbortSignal`.
- **Retenção:** job na `-system`, runs finalizadas mais antigas que `retention.days` (padrão 30), em lotes; só sem pai ou pai finalizado.

**Aceite:** cada caso do reconciler, stall por heartbeat, perda e retomada de liderança, `migrate()` enquanto outro processo é líder (não bloqueia), run `waiting` órfã retomada com `warn`, cancelamento entre processos (duas engines), retenção em lotes.

### Fase 7 — Logs, eventos e consulta

- `ctx.logger` com buffer e flush em lote via `unnest` (500 ms ou 50 entradas, e ao final), `maxLogEntryBytes`, nível mínimo, espelho para `config.logger`.
- `engine.runs.get`, `list`, `logs(runId)`, `steps(runId)`.
- `engine.events.on(...)` local e `engine.runs.subscribe(runId)` (`AsyncIterable` via pub/sub) na ordem: assinar e bufferizar, ler Postgres e emitir, emitir do buffer só com `updated_at` posterior, encerrar em status terminal.
- `ctx.stream(name, asyncIterable)` via **Redis Streams** (`XADD MAXLEN ~`, expira ao fim), `engine.runs.readStream(runId, name)`.

**Aceite:** 10k logs sem uma query por linha; subscribe recebendo a sequência; subscribe em run que termina durante a assinatura e em run já finalizada.

### Fase 8 — Agendamentos

- `engine.schedules.upsert/delete/list` com `upsertJobScheduler`, timezone, ticks na `-system`. Tabela `schedules` (nova migration).
- Postgres manda. Handler do tick: schedule inexistente → remove scheduler; cron/timezone divergentes → refaz upsert; payload sempre do Postgres.
- Chave `__schedule:${id}:${timestampAgendado}`.
- Iterações perdidas não são executadas depois (limitação documentada).
- `schedule: { cron, timezone }` em `defineTask`, sync no `start()`; remoção só com `pruneDeclarative: true`.

**Aceite:** cron de alta frequência com dois processos sem duplicatas; schedule declarativo removido persiste sem `pruneDeclarative`; `delete` com falha no BullMQ não gera run no tick seguinte; payload alterado só no Postgres é o usado.

### Fase 9 — Modo de teste e DX

- `createTestEngine()` / `engine.runInline(task, payload)` com store em memória, `sleep` instantâneo e tokens completáveis manualmente.
- Mensagens de erro acionáveis.
- Checagens na inicialização: versão do Postgres, `maxmemory-policy`, migrations pendentes (decisão 22).
- `Serializable<T>` com erros legíveis, testes de tipo (`expectTypeOf`) com `interface`, `type`, opcionais, arrays aninhados e `Date` (rejeitado).

**Aceite:** exemplo de teste de task do usuário sem infra; teste da checagem de Redis simulando `CONFIG` bloqueado.

### Fase 10 — Exemplo, documentação e validação como dependência

- `examples/basic` via `file:`: steps, sleep, token de aprovação, subtask, cron, agente de IA simples (LLM mockado).
- `pnpm pack` + instalação em diretório limpo, import ESM **e** CJS; instalação via GitHub (`pnpm add github:usuario/minitrigger`) validada ou documentada (alternativa: `.tgz` em GitHub Releases).
- `README.md` completo + "Regras de ouro".

**Aceite:** `.tgz` instala e roda num projeto limpo; caminho GitHub validado ou documentado; README cobre toda a API pública.

### Fase 11 (opcional, só quando pedido) — Dashboard

---

## 6. Performance: requisitos transversais

- Uma query de leitura de steps por execução; escritas de step individuais e curtas.
- Nenhuma transação aberta durante a execução de código do usuário.
- Logs sempre em lote.
- Redis enxuto.
- Pool do Postgres configurável, padrão 10, mais o client dedicado do líder.
- Benchmark `pnpm bench`: 1.000 runs de 3 steps, throughput e latência de retomada; resultado no `CLAUDE.md`.
