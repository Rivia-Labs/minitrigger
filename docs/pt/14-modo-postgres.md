# 14. Modo Postgres

```ts
import { createEngine, defineTask } from "@rivia-labs/minitrigger";

const engine = createEngine({
	postgres: process.env.DATABASE_URL!,
	transport: "postgres",
	tasks: [myTask],
});

await engine.migrate();
await engine.start();
```

Desde a 0.4.0, `transport: "postgres"` (decisão 28 em `docs/SPEC.md`) roda o engine inteiro — fila,
atraso, wake-up, a fila interna `-system` e pub/sub — só em cima do Postgres: sem Redis, sem
BullMQ, e `bullmq`/`ioredis` nem precisam estar instalados. `transport: "bullmq"` (o padrão, sem
nenhuma mudança) continua sendo a escolha melhor quando você já roda Redis, ou precisa de Redis
Streams (`ctx.stream`) ou de rate limit; use `"postgres"` quando preferir não subir Redis, ou já
tem capacidade de Postgres sobrando o bastante para uma segunda peça móvel ser custo puro. Fora do
que está listado abaixo, os dois transportes são equivalentes: mesmo `defineTask`, mesmo
`ctx.step.*`, mesmo replay/memoização, mesmo contrato de store (`docs/store-contract.md`).

## Quando usar

- Você quer tasks duráveis sem adicionar Redis como dependência operacional.
- Seu Postgres já tem folga de conexão para mais um `pool.max` de conexões por processo (ver
  [Conexões por engine](#conexões-por-engine) abaixo) e tolera o tráfego extra de queries de
  claim/poll contra `runs`.
- Você não precisa de `ctx.stream`/`runs.readStream`, `queue.rateLimit`, ou de um
  `globalConcurrency` no estilo Redis (aproximado sob disputa pesada) — este transporte é exato em
  vez disso — nenhum dos dois existe neste modo.

## O que não está disponível

| Campo / chamada | Resultado |
|---|---|
| `redis`, `prefix` | `ConfigError` no `createEngine()` — sem Redis neste modo; canais de `NOTIFY` derivam de `schema` em vez disso. |
| `outbox.*` | `ConfigError` no `createEngine()` — `trigger(..., { tx })` já escreve direto na linha que o claim lê (decisão 28(j)); não há outbox separado para configurar. |
| `queue.rateLimit` | `ConfigError` no `createEngine()` — não implementado para este transporte (ainda sem rate limiter em SQL). |
| `ctx.stream(name, source)` | `ConfigError` lançado para a task quando chamado; como um argumento de step inválido, falha a run sem retry, mesmo que a task capture o erro. |
| `runs.readStream(run, name)` | `ConfigError` lançado quando chamado. |

## Como funciona

- **A fila é a própria tabela `runs`.** Uma linha `queued`/`waiting` com `run_after <= now()` é um
  job vencido; um worker reivindica um lote com `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP
  LOCKED)` num único statement, gravando os mesmos campos que o `acquire` do modo BullMQ
  (`status = 'running'`, um `execution_id` novo, `heartbeat_at`, `started_at` se ainda não setado).
  `trigger()` vira uma escrita atômica: nada pode se perder entre "a run existe" e "a run está
  enfileirada" (não há um passo de enfileiramento separado para se perder). Por isso os casos de
  enqueue perdido do reconciliador (casos 1/2 do modo BullMQ) não rodam neste transporte — uma run
  passada do seu `run_after` é backlog comum, não um sinal de que algo se perdeu.
- **O wake-up é `NOTIFY`, com um poll adaptativo como rede de segurança.** Todo enqueue (insert,
  retomada, retentativa, devolução no shutdown, recuperação de stall, um colapso de debounce)
  também dispara um `pg_notify` nomeando a fila; todo engine iniciado dá `LISTEN` numa conexão
  dedicada e acorda só o laço de claim daquela fila (um payload não reconhecido ou forjado acorda
  todas as filas registradas — o payload é sempre uma dica, nunca confiável, já que qualquer role
  com `CONNECT` pode dar `NOTIFY` nesses canais). Se a notificação se perde (listener reconectando,
  um pooler na frente que quebra `LISTEN`, nenhum engine iniciado ainda), todo worker que não
  encheu seu lote também faz poll: lê o `run_after` mais cedo da fila e dorme até lá, limitado por
  `polling.intervalMs` (padrão 1 000 ms, mínimo 100) mais até `polling.jitterMs` de jitter (padrão
  50 ms, para diluir o rebanho de trovão que uma rajada de `NOTIFY`s causaria). Então, na pior
  hipótese, a latência de wake-up sem `NOTIFY` é limitada por `polling.intervalMs`, nunca
  indefinida.
- **`globalConcurrency` é exato.** Com um limite, o claim roda dentro de uma transação curta que
  primeiro toma um advisory lock por fila, depois conta as linhas `running` da fila uma única vez
  — claims concorrentes da mesma fila se serializam nesse lock, então a contagem de `running` entre
  todos os processos nunca passa do limite. Sem aproximação, ao contrário de contar do lado do
  Redis sob claims concorrentes.
- **A fila interna `-system` é uma tabela, não uma fila BullMQ.** `system_jobs` (migration 009)
  guarda expirações de waitpoint (`wp-<id>-expire`), varreduras de retenção (`retention-<janela>`)
  e ticks de cron (`schedule-tick:<id>`), reivindicadas da mesma forma (`SKIP LOCKED`) com um
  token de fencing (`locked_by`): um worker cujo lock venceu nunca completa, adia ou reagenda uma
  linha que outro worker já reivindicou. O reconciliador não é um job: o líder o roda inline no
  próprio timer (`reconciler.intervalMs`), como no modo BullMQ.
- **Ticks de cron nunca somem numa falha.** Em vez de um job scheduler do BullMQ, um schedule é uma
  única linha (`schedule-tick:<id>`) que é *regravada no lugar* para a próxima ocorrência quando o
  tick tem sucesso ou esgota as tentativas — nunca apagada e recriada — então não existe uma janela
  em que o schedule não existe. Ticks perdidos (nenhum processo rodando) continuam sem backfill,
  igual ao modo BullMQ; um tick que precisa retentar (backoff) atrasa os seus próprios ticks
  seguintes, já que nunca há mais de um em voo do mesmo schedule.

## Latências

- **Wake-up:** milissegundos via `NOTIFY`, quando chega. Limitado por `polling.intervalMs` quando
  não chega (ver acima).
- **`globalConcurrency`:** exato, com só a contenção do próprio advisory lock (tipicamente
  sub-milissegundo) como latência extra.
- Veja a tabela completa no `CLAUDE.md` ("Benchmark") com os dois transportes medidos lado a lado
  na mesma máquina: o transporte Postgres bateu a meta de aceitação do desenho (vazão dentro de
  ±20% do BullMQ, p99 de retomada por sleep ≤ 150 ms) sem ajuste extra nenhum — na prática, saiu na
  frente tanto em vazão quanto em latência de retomada por sleep, já que uma retomada via `NOTIFY`
  pula o polling de job atrasado do próprio Redis.

## Conexões por engine

`pool.max`, quando o engine cria o próprio pool, é derivado como `max(10, soma da concorrência das
filas de task + systemConcurrency + 5)` neste modo: o worker `-system` roda contra o pool também
(não há um worker `-system` separado, com Redis, para segurar sua própria conexão), ao contrário do
modo `"bullmq"`, onde a fórmula não soma `systemConcurrency` (ver
[11. Produção](11-producao.md#dimensionamento-do-pool)). Além do pool, todo engine **iniciado**
segura **até duas** conexões dedicadas fora dele: a conexão `LISTEN` (notificações de
wake/status/cancel) e o client de eleição de líder — menos enquanto uma delas ainda está
conectando ou em backoff depois de cair. Dimensione `max_connections` para
`processos × (pool.max + 2)`, mesma forma da nota do outbox no modo BullMQ em
[06. Outbox transacional](06-outbox.md). Se você injeta seu próprio `pg.Pool`, dimensione-o da
mesma forma e configure um `connectionTimeoutMillis` — `start()` avisa quando não, igual no modo
BullMQ.

O Postgres é a única dependência deste modo, então a alcançabilidade dele faz o papel do `PING` do
Redis no modo BullMQ: com o banco inacessível, `start()` rejeita com um `MinitriggerError` ("cannot
reach Postgres", com o erro do driver em `cause`) em vez de travar — dentro de
`pool.connectionTimeoutMs` (padrão 10 s; para um pool injetado, o `connectionTimeoutMillis` dele
quando definido), seja o pool criado pelo engine ou injetado, com ou sem `connectionTimeoutMillis`:
a checagem tem timer próprio, e uma conexão que só completa depois dele é destruída, nunca fica no
pool. Nada fica rodando, e `start()` pode ser chamado de novo quando o banco voltar. O modo BullMQ
faz a mesma checagem logo depois do `PING` do Redis. Um pool injetado sem `connectionTimeoutMillis`
ainda fica com uma vaga presa pela tentativa de conexão abandonada até o SO desistir dela (o
`pg-pool` não consegue cancelar um connect pendente), então um pool injetado pequeno pode continuar
fazendo o `start()` falhar enquanto isso: configure `connectionTimeoutMillis` nos pools que você
injeta. `stop()` resolve dentro dos limites de
sempre com o banco fora, tanto depois de um `start()` que falhou quanto com o engine rodando (uma
run que ele não conseguiu devolver fica `running` até a recuperação de stall de outro engine).

## PgBouncer

Um pooler em modo transação quebra as duas conexões dedicadas que este modo precisa manter abertas
entre statements: a conexão `LISTEN` (uma conexão em modo transação não mantém o estado de
`LISTEN` entre transações) e o advisory lock de sessão do líder. `start()` roda um autoteste logo
depois de conectar — um `NOTIFY` no canal de wake que precisa ecoar de volta no próprio `LISTEN`
dentro de 1 s — e só gera um `warn` se não ecoar ("notificações não chegam... a latência de wake
cai para o poll"), então um pooler na frente dessas duas conexões não falha o `start()`; ele só cai
silenciosamente para wake-ups só por poll. A eleição de líder quebra de um jeito menos óbvio: o
advisory lock de sessão fica na conexão de servidor que rodou `pg_try_advisory_lock`, e um eleitor
que venceu nunca mais confere o lock. Outro engine cuja tentativa o pooler encaminha para essa
mesma conexão de servidor também recebe o lock (dois líderes ao mesmo tempo); o pooler reciclar
essa conexão de servidor libera o lock enquanto o primeiro engine ainda se acha líder (outro pode
vencer também); e um unlock ou um client fechado nunca chega a essa conexão de servidor, então o
lock pode ficar vazado (sem líder até ela ser reciclada). Ou seja: espere líderes duplicados ou
instáveis, e às vezes nenhum (ver [Sem líder](#sem-líder) abaixo), não um "sem líder" limpo.
Nenhum autoteste detecta isso: aponte a `postgres` do engine para um pool em modo sessão, ou
direto para o Postgres, para wake-ups em milissegundos e uma eleição de líder funcionando; as
queries comuns de claim/heartbeat/conclusão pelo pool principal funcionam bem atrás de um pooler em
modo transação de qualquer forma (são todas statements únicos).

## Visibilidade de `NOTIFY`

`pg_notify`/`LISTEN` só precisam de `CONNECT` no database — nenhum grant extra para a role da
aplicação. Isso também significa que **qualquer role com `CONNECT` pode dar `LISTEN`** nos canais
deste modo (os nomes derivam do nome do schema, então são fáceis de adivinhar). O que essa role
vê: os `runId`s nos canais de status e cancel, os **nomes de fila** no canal de wake e o
**momento** de todo enqueue, mudança de status e cancel — nunca payloads, outputs ou progresso
(toda mensagem carrega só um id puro ou um nome de fila, e todo assinante relê o Postgres antes de
agir sobre ela). Se run ids, nomes de fila ou padrões de atividade são sensíveis no seu contexto,
use um database dedicado a este engine em vez de um compartilhado com outras roles/times.

A mesma role também pode dar `NOTIFY` nesses canais. Notificações de wake e de status são só dicas
(uma forjada custa uma passada de claim ou uma releitura; passadas disparadas por wake que não
reivindicam nada têm taxa limitada), e um cancel forjado nunca aborta uma run que não está
`canceled` no Postgres. O autoteste do `start()` também não é autenticado: o eco carrega um token
aleatório, mas uma role que vê esse token no canal de wake pode devolvê-lo. Um eco forjado só
esconde o aviso "notificações não chegam"; nunca muda o que o engine faz.

## Sem líder

Sem um líder (a tentativa de advisory lock de todo engine perdendo, ou nenhum engine iniciado), o
reconciliador e as varreduras de retenção não rodam — mas a execução normal de runs, os ticks de
cron (qualquer worker `-system` de qualquer engine iniciado reivindica linhas `schedule-tick`, sem
checagem de liderança) e os laços de claim/wake/poll continuam funcionando normalmente. O efeito
prático de ficar sem líder por um tempo: uma run travada (stall) não é recuperada, um enqueue
perdido (raro neste modo, ver acima) não é reconciliado, e runs terminadas se acumulam sem serem
apagadas — tudo limitado e autocurável assim que algum engine consegue virar líder de novo.

## Guarda de modo misto, e mover um schema entre transportes

Um schema só tem um transporte por vez: o primeiro `start()` (qualquer um dos modos) grava isso em
`schema_meta` (`key = 'transport'`), e qualquer `start()` depois no mesmo schema com o transporte
*outro* recebe `ConfigError` — um engine BullMQ e um engine Postgres compartilhando um schema
seriam duas fontes independentes de jobs `-system` (disputando para rodar o mesmo tick de cron,
expirar o mesmo waitpoint, e assim por diante).

Um engine que nunca chama `start()` (produtor só de disparo) nunca grava o marcador, mas o lê antes
da primeira escrita (`trigger`, inclusive com `{ tx }`; `runs.replay`/`cancel`/`cancelMany`/
`retryFailed`; `tokens.complete`; `queues.pause`/`resume`; `schedules.upsert`/`delete`) e recusa a
escrita com o mesmo `ConfigError` quando o schema pertence ao outro transporte. Um modo gravado é
lido uma vez por engine. Enquanto nenhum engine gravou o modo (ou o schema ainda não tem a tabela
`schema_meta`, sem a migration 009), as escritas passam e o marcador é relido no máximo a cada
30 s, então um produtor pode continuar escrevendo por até 30 s depois que um engine do outro modo
inicia pela primeira vez nesse schema.

**Engines em 0.3.x e anteriores nunca gravam esse marcador** (é novo na migration 009). Para mover
um schema existente de `"bullmq"` para `transport: "postgres"`:

1. Atualize todo engine desse schema para 0.4.0+ primeiro (eles podem continuar rodando
   `transport: "bullmq"` enquanto isso — atualizar só a versão do pacote não troca de transporte;
   um engine 0.4+ ainda no padrão `"bullmq"` grava o marcador como `"bullmq"` no próximo `start()`,
   igual antes).
2. Pare todo engine desse schema.
3. Confirme que `system_jobs` está vazia e que o BullMQ não tem jobs pendentes na antiga fila
   `${prefix}-system` (expirações de waitpoint, ticks de schedule, uma varredura de retenção
   enfileirada) — o `-system` do transporte Postgres nunca olha para o BullMQ, então qualquer coisa
   ainda ali é silenciosamente abandonada depois da troca.
4. `UPDATE "<schema>".schema_meta SET value = 'postgres' WHERE key = 'transport';` (ou apague essa
   linha e deixe o próximo `start()` com `transport: "postgres"` gravá-la do zero).
5. Suba os engines com `transport: "postgres"`.

Não existe uma migração de dados separada para runs/waitpoints/schedules em voo durante a troca —
eles sempre foram linhas do Postgres (Postgres é a fonte da verdade nos dois modos); só qual lado
lê e escreve os jobs `-system` muda.

## Autovacuum de `system_jobs`

Linhas são apagadas assim que um job completa (expirações de waitpoint, varreduras de retenção) ou
esgota as tentativas — ticks de schedule são a exceção, regravados no lugar (ver acima) —, então
essa tabela tem bastante churn em relação ao próprio tamanho: espere um fluxo constante de tuplas
mortas. Dê a ela a mesma atenção que qualquer outra tabela de alto churn — um
`autovacuum_vacuum_scale_factor` mais baixo (`ALTER TABLE system_jobs SET
(autovacuum_vacuum_scale_factor = ...)` no schema alvo) evita que ela infle entre os limiares
padrão de autovacuum do banco.

## Jobs lentos da `-system` (retenção)

Uma linha de `system_jobs` reivindicada fica travada por `stallTimeoutMs` (`locked_until`), e um
handler em execução nunca renova esse lock. Um handler que roda mais que `stallTimeoutMs` — na
prática, uma varredura de retenção grande — pode ser reivindicado de novo por outro worker
`-system` e rodar duas vezes em paralelo. O design aceita isso: a retenção apaga com `SKIP LOCKED`
(uma segunda varredura só custa queries), uma expiração de waitpoint re-checa o waitpoint antes de
agir, um tick de cron colide na chave de idempotência (`__schedule:<id>:<ts>`), e o fencing por
`locked_by` deixa só o dono atual concluir ou reagendar a linha. Uma varredura apaga lotes até o
backlog acabar, então, se as varreduras passam de `stallTimeoutMs` com frequência, rode a retenção
mais vezes (`retention.intervalMs`, menos a apagar por varredura) ou aumente `stallTimeoutMs`.

## Diferenças de modo que vale saber

- **`trigger(..., { tx })` não emite `run:queued`.** No modo BullMQ, o listener dedicado do outbox
  observa o commit do chamador e publica `queued` quando despacha a run. No modo Postgres não há
  um dispatcher de outbox separado — o insert já gravou a linha que um claim pode pegar, e o engine
  nunca observa separadamente "a transação do chamador commitou" como um momento próprio — então o
  primeiro evento/status que a run de um trigger `{ tx }` produz é `run:started`, quando algum
  worker a reivindica. Use `run:started` (ou faça poll em `runs.get`) em vez de `run:queued` para
  triggers transacionais sob este transporte.
- **`runs.subscribe` pode colapsar status intermediários próximos no tempo.** Mudanças de status
  são agrupadas em `NOTIFY`s em lote, e o assinante relê a linha atual em vez de reproduzir cada
  mensagem individual, então dois status publicados dentro da mesma janela curta podem colapsar
  numa entrega só (a mais recente). O status **terminal** (`completed`/`failed`/`canceled`) sempre
  é entregue. Se você precisa de todo status intermediário, assine `engine.events` no mesmo
  processo em vez disso — ali é um emit direto em processo, sem round trip pelo Postgres para
  colapsar contra.
- **Sem streams, sem rate limit** — ver [O que não está disponível](#o-que-não-está-disponível)
  acima.
- **Chame `stop()` depois de um `start()` que falhou.** Se `start()` lançar (ex.: a conexão
  `LISTEN` deu timeout), o client do listener continua tentando reconectar em segundo plano de
  qualquer forma — chame `await engine.stop()` para desligá-lo direito antes de descartar o engine,
  ou antes de tentar `start()` de novo na mesma instância (também suportado).
- **Um `start()` retentado reconecta o listener na hora.** Chamar `start()` de novo depois de um
  que falhou pula o backoff de reconexão do listener (0,5–30 s) e tenta conectar imediatamente;
  continua esperando no máximo 5 s o `LISTEN` ficar pronto.
