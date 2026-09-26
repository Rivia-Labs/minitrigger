# 12. Troubleshooting

Comece sempre checando o básico:

```ts
console.log(await engine.runs.get(runId));
console.log(await engine.runs.steps(runId));
console.log(await engine.runs.logs(runId, { limit: 50 }));
```

```sql
SELECT id, task_id, status, attempt, run_after, heartbeat_at, updated_at
FROM "<schema>".runs
WHERE id = '<run-id>';
```

Para um diagnóstico mais profundo direto no banco (queries prontas para cada um dos sintomas
abaixo), veja a skill `minitrigger-debug` (`skills/minitrigger-debug/`, com o script
`scripts/diagnose.sql`) — não reproduzimos o SQL aqui, essa skill é a ferramenta complementar de
diagnóstico para quando os sintomas abaixo não bastam.

## Run presa em `queued`

Causa provável: o enqueue no Redis se perdeu (o processo caiu entre o insert em Postgres e o
enfileiramento em BullMQ) ou nenhum engine com essa task registrada está iniciado.

- Confirme que algum processo chamou `engine.start()` para a fila da task.
- O reconciliador (rodando no líder) reenfileira runs `queued`/`waiting` cujo `run_after` já passou
  — a recuperação leva até `reconciler.graceMs + reconciler.intervalMs` (por padrão, até ~45 s), não
  é instantânea. Se já passou muito mais que isso, confira se algum engine é líder (a eleição de
  líder pode estar falhando — veja "líder" em [11. Produção](11-producao.md#líder-e-reconciliador)).
- Se a run veio de `trigger(..., { tx })` (outbox), veja "Outbox não despachando" abaixo.

## Run presa em `running`

Causa provável: stall (o processo que segurava a run morreu ou travou) ou o event loop bloqueado.

- O reconciliador trata uma run `running` cujo `heartbeat_at` está mais velho que `stallTimeoutMs`
  (padrão 60 s) como travada e a recupera. Se a run está presa há muito mais que isso, confira se
  há um líder ativo.
- **Código síncrono que bloqueia o event loop por mais que `stallTimeoutMs` parece um stall para o
  reconciliador**, mesmo que o processo esteja vivo — o heartbeat roda no mesmo event loop do
  código da sua task. Mova trabalho pesado de CPU para dentro de um step usando `worker_threads`.
- Um `concurrencyKey` ocupado por uma run `running` travada bloqueia outras runs da mesma chave até
  a recuperação do stall — não é um bug de `concurrencyKey`, é o sintoma acima se propagando.

## `NonDeterminismError` / `DuplicateStepError` / `NestedStepError`

Esses três são **bugs de código, não problemas de infraestrutura** — a run falha sem retentativa.

- `NonDeterminismError`: a sequência de steps observada no replay divergiu da que foi gravada
  (nome ou tipo errado numa posição). Causas comuns: um deploy que reordenou/renomeou steps
  enquanto havia runs suspensas (regra de ouro 5); nomes de step derivados de algo não
  determinístico (regra de ouro 2); steps chamados depois de um `await` dentro de ramos
  concorrentes, registrados em ordem de conclusão em vez de ordem de chamada (regra de ouro 9). Veja
  [04. Tasks e steps](04-tasks-e-steps.md#regras-de-ouro).
- `DuplicateStepError`: o mesmo nome de step foi usado duas vezes numa execução — normalmente um
  nome de step dentro de um loop sem incluir o índice.
- `NestedStepError`: `ctx.step.*` foi chamado de dentro do callback de um `step.run` — steps não
  aninham (regra de ouro 7).

Nenhum dos três é recuperado automaticamente; corrija o código da task e, se necessário,
`engine.runs.replay(runId)` para uma nova tentativa (nova run, mesmo payload).

## `TaskNotRegisteredError`

- `engine.trigger`/um schedule referenciou uma task que não está registrada neste engine, ou um
  worker adquiriu uma run de uma task que ele não conhece.
- É **retentável** — o cenário mais comum é um deploy gradual, onde processos com registros de task
  diferentes coexistem por um tempo; assim que o deploy termina, o processo certo pega a run.
- `step.trigger*` aceita uma task filha não registrada neste processo: outro processo que a
  registre executa a filha — isso não é um erro, é o comportamento esperado.
- Se persiste além da duração de um deploy, confirme que a task está de fato registrada em
  `tasks: [...]` de algum `createEngine()` iniciado apontando para o mesmo `schema`/`prefix`.

## Outbox não despachando

- Confirme que passou `tx` (o `PoolClient`/`Client` da sua transação, não o `Pool`) para
  `engine.trigger(..., { tx })`, e que a transação de fato commitou.
- **PgBouncer em transaction pooling quebra o `LISTEN`**: o despacho passa a depender só da
  varredura de `outbox.pollIntervalMs` (2 s por padrão) em vez do `NOTIFY` de milissegundos. Aponte
  a `postgres` do engine para um pool em modo sessão ou direto para o Postgres.
- Confirme que algum engine está `start()`ado — um engine só de disparo nunca escuta `LISTEN` nem
  varre o outbox.
- Veja [06. Outbox transacional](06-outbox.md) para as garantias completas e o custo do `NOTIFY`.

## Schedule não disparando

- `engine.schedules.list()` mostra o schedule? Se não, confira se `engine.start()` já rodou (a
  sincronização declarativa acontece no fim do `start()`) ou se `engine.schedules.upsert()` de fato
  foi chamado.
- Um `upsert()` mirando um id que já pertence a um schedule declarativo (`defineTask({ schedule
  })`, mesmo id da task) lança `ConfigError`; a sincronização declarativa, no sentido oposto, só
  loga um erro e pula — confira os logs por essa mensagem.
- Um scheduler do BullMQ pode estar temporariamente ausente/divergente logo depois de uma falha na
  sincronização — o tick do reconciliador (no líder) e o próximo disparo do schedule se
  autocorrigem; confira se há um líder ativo.
- Ticks perdidos enquanto todo processo estava fora do ar **não são recuperados** — isso é esperado
  (limitação do próprio BullMQ), não um bug.
- Se o `prefix` é compartilhado entre schemas diferentes, um tick pode estar sendo processado (ou
  removido) pelo engine errado — veja
  [08. Agendamentos § `prefix` precisa ser único por schema](08-agendamentos.md#prefix-precisa-ser-único-por-schema).

## Fila pausada inesperadamente

- `engine.queues.list()` mostra `paused: true`? O estado vem de Postgres (`queue_state`), então uma
  pausa sobrevive a reinicializações e ao `FLUSHALL` do Redis — se alguém chamou
  `engine.queues.pause(name)` (mesmo de um script administrativo, mesmo sem `start()`), a fila
  continua pausada até um `resume` explícito.
- Depois de um `FLUSHALL` do Redis, uma fila que estava pausada em Postgres volta a ficar pausada
  no Redis dentro de `reconciler.intervalMs` (15 s por padrão) — nesse intervalo, runs dela podem
  chegar a iniciar.
- Pausar **nunca aborta execuções em andamento** — se runs em progresso pararam, o sintoma é outra
  coisa (veja "Run presa em `running`" acima), não a pausa da fila.
