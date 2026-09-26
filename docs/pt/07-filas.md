# 07. Filas

## Configuração de fila

```ts
defineTask({
	id: "summarize",
	queue: { name: "llm", concurrency: 2, rateLimit: { max: 10, durationMs: 60_000 } },
	run: async (payload: { url: string }) => { /* ... */ },
});
```

`TaskQueueOptions`:

| Campo | Tipo | Descrição |
|---|---|---|
| `name` | `string` | Nome da fila. |
| `concurrency` | `number` | Quantas runs dessa fila rodam ao mesmo tempo, por processo. |
| `globalConcurrency` | `number` | Limite de concorrência global (entre processos), aplicado pelo BullMQ. |
| `rateLimit` | `{ max, durationMs }` | Limite de taxa da fila (no máximo `max` jobs iniciados a cada `durationMs`). |
| `concurrencyKey` | `{ limit }` | Ver [Chaves de concorrência](#chaves-de-concorrência-concurrencykey) abaixo. |

Configurações de fila são compartilhadas por **nome**: várias tasks podem referenciar a mesma fila,
mas só uma delas pode declarar `concurrency`/`globalConcurrency`/`rateLimit`/`concurrencyKey` para
ela — uma segunda declaração diferente para o mesmo nome falha a criação do engine com um erro
claro. Uma task sem `queue` pertence à fila `"default"` (concorrência 10, salvo declaração
diferente).

## Chaves de concorrência (`concurrencyKey`)

```ts
defineTask({
	id: "recalc-classification",
	queue: { name: "classification", concurrency: 4, concurrencyKey: { limit: 1 } },
	run: async (payload: { courseId: string }) => { /* ... */ },
});

await engine.trigger(recalcClassification, { courseId }, { concurrencyKey: courseId });
```

Uma fila que declara `concurrencyKey: { limit }` executa no máximo `limit` runs com o mesmo
`trigger(..., { concurrencyKey })` ao mesmo tempo, entre **todos** os processos (a `concurrency` da
fila continua limitando o total por worker). A checagem acontece na aquisição, numa única
transação Postgres serializada por chave (um advisory lock): uma run acima do limite não é
iniciada — ela volta para a fila **sem gastar uma tentativa**, e tenta de novo depois.

- **Não é FIFO, é polling**: uma run que espera há mais tempo não é necessariamente a próxima a
  adquirir a chave quando um slot libera. Combine `concurrencyKey` com `debounce` quando quiser o
  padrão "um pendente + um rodando" em vez de uma fila justa por chave.
- Cada vez que uma run é encontrada acima do limite, ela retenta com um backoff crescente
  (dobrando aproximadamente, começando perto de um segundo, com teto em 30 s, mais jitter); a
  contagem zera assim que a run adquire. Uma chave ocupada por muito tempo significa que o custo de
  polling por run esperando cresce até esse teto de 30 s, não um loop apertado — concretamente,
  depois que o backoff de uma run esperando já subiu perto do teto, a LATÊNCIA entre o slot da
  chave realmente liberar e essa run adquiri-lo pode chegar a ~30 s (a próxima tentativa agendada
  dela), não só "cerca de um segundo"; uma run nova (ainda não limitada nenhuma vez) sempre tenta
  de novo dentro de cerca de um segundo, independente disso.
- **Só runs `running` contam**: uma run esperando um sleep, token ou filho não segura um slot,
  então outra run da mesma chave pode executar enquanto ela espera.
- Runs disparadas sem `concurrencyKey` não são limitadas. Usar `concurrencyKey` numa task cuja fila
  não declara um é `ConfigError`. `limit` é compartilhado por todas as tasks da fila, como as
  outras configurações de fila.
- **Só `engine.trigger` aceita `concurrencyKey` por enquanto** (não `step.trigger*`);
  `runs.replay()` mantém a chave da run original **só enquanto a fila ainda declarar
  `concurrencyKey`** — se ela foi removida da configuração da fila desde então, a run replayada
  fica sem chave e roda sem limite. Filhas disparadas via
  `step.trigger`/`step.triggerAndWait`/`step.batchTriggerAndWait` numa fila com `concurrencyKey`
  rodam **sem limite** — o limite só se aplica a uma run cujo próprio `concurrencyKey` foi definido
  por `engine.trigger`.
- Um processo que não conhece a configuração `concurrencyKey` da fila (um deploy antigo, por
  exemplo) adquire sem a checagem; mantenha a configuração idêntica entre deploys. O limite é lido
  do registro do próprio worker, não compartilhado centralmente.
- Uma run `running` de um processo que caiu segura o slot da chave até a recuperação de stall
  (`stallTimeoutMs`) perceber e reenfileirá-la — um worker travado pode bloquear uma chave por até
  esse tempo.

## Debounce

```ts
await engine.trigger(recalcClassification, { courseId }, {
	debounce: { key: `course:${courseId}`, delay: "2s" },
});
```

Triggers com o mesmo `(task, debounce.key)` colapsam em **uma run pendente** enquanto essa run
ainda estiver `queued` e não tiver iniciado: o trigger devolve o handle da run pendente em vez de
criar outra (800 mudanças de nota → um recálculo). Uma vez que a run inicia, a chave é liberada e o
próximo trigger cria uma nova run pendente, na qual os triggers seguintes colapsam — no máximo uma
run pendente por chave, e ela sempre vê os dados mais recentes. A regra vive em Postgres (um índice
único parcial), então vale entre processos e sobrevive a um `FLUSHALL` do Redis.

- **Sempre trailing** (não existe modo `"leading"`): cada trigger empurra a execução para `delay`
  depois dele, então a run executa quando a chave ficou quieta por `delay`.
- `delay` (obrigatório, > 0): a janela.
- `payload`: `"last"` (padrão) — um trigger colapsado substitui o payload pendente; `"first"` —
  mantém o original.
- `maxDelay` (>= `delay`): limita o quanto uma run pode ser adiada a partir do seu primeiro
  trigger, para que uma chave que nunca fica quieta ainda execute periodicamente.
- Chaves são escopadas por task, validadas como chaves de idempotência (não vazias, no máximo 512
  caracteres, prefixo `__` reservado). `debounce` não combina com `idempotencyKey` nem com `delay`.
- Um trigger colapsado não muda o status da run, então não emite evento nem atualização de
  `runs.subscribe`. Uma run que já iniciou e voltou para `queued` (retentativa, retomada de um
  sleep, devolução graciosa no shutdown) nunca absorve triggers.
- Um trigger colapsado também não muda o `concurrencyKey` da run pendente, o prazo de `maxDelay`
  nem o `maxAttempts` — só o `payload` (conforme a opção `payload`) e o `run_after` se movem; os
  outros campos são fixados por qualquer que tenha sido o trigger que criou a run pendente (o
  primeiro trigger vence).

## Combinando debounce + concurrencyKey: "um pendente + um rodando"

```ts
defineTask({
	id: "recalc-classification",
	queue: { name: "classification", concurrency: 4, concurrencyKey: { limit: 1 } },
	run: async (payload: { courseId: string }) => { /* ... */ },
});

await engine.trigger(recalcClassification, { courseId }, {
	concurrencyKey: courseId,
	debounce: { key: courseId, delay: "2s" },
});
```

Debounce mantém uma run pendente por chave; para também impedir que duas runs da mesma chave
executem ao mesmo tempo, adicione uma chave de concorrência — juntos, os dois dão "um pendente + um
rodando".

## Pausar, retomar e escalar filas

```ts
await engine.queues.pause("llm");    // todo processo para de iniciar novas runs de "llm"
await engine.queues.resume("llm");
await engine.queues.list();          // [{ name, paused, updatedAt, declared, declaredConcurrency, concurrency, activeLocal }]
await engine.queues.scale("llm", 5); // só o worker DESTE processo, até ele reiniciar
```

- `pause`/`resume` funcionam entre processos e sobrevivem a reinicializações. O estado é salvo
  primeiro em Postgres (tabela `queue_state`, a fonte da verdade) e só então aplicado como a pausa
  em nível de fila do BullMQ no Redis, que todo worker de todo processo respeita dentro de uma
  busca. Qualquer engine pode chamá-los, iniciado ou não (ex.: um script administrativo).
  Idempotente.
- **Pausar nunca aborta execuções em andamento**: elas terminam (ou suspendem) normalmente. Só
  novos jobs da fila param de ser pegos; triggers continuam criando runs (elas esperam como
  `queued`), e as retentativas, sleeps e retomadas da fila também esperam até `resume`.
- Todo `start()` reaplica o estado salvo antes de seus workers buscarem jobs, e o líder reaplica a
  cada tick do reconciliador. Depois de um `FLUSHALL` do Redis (que apaga a flag do BullMQ), uma
  fila pausada volta a ser pausada dentro de `reconciler.intervalMs` (15 s por padrão); runs dela
  podem iniciar nessa janela.
- O reconciliador do líder exclui filas pausadas das suas varreduras de enqueue perdido/waiting
  parada, então o backlog de uma fila pausada nunca atrasa achar jobs de verdade perdidos de
  **outras** filas — mas excluir essas linhas ainda custa algo em cada tick: é um filtro aplicado
  durante a varredura ordenada por `run_after`, não um índice que pula essas linhas de cara. Conte
  algo como 0,1 ms de trabalho extra por tick a cada 1 000 linhas `queued`/`waiting` numa fila
  pausada; uma fila pausada com um backlog muito grande (milhões de runs `queued` deixadas de
  propósito) deixa todo tick do líder proporcionalmente mais lento enquanto ela ficar pausada e
  esse backlog continuar lá.
- Só filas usadas pelas tasks deste engine (ou com estado salvo) podem ser pausadas; a fila interna
  `${prefix}-system` não pode.
- `list()` devolve as filas deste engine mais toda fila com estado salvo. `paused` vem de Postgres;
  `concurrency` é a concorrência atual do worker deste processo (`null` quando este processo não
  roda worker para a fila); `activeLocal` conta as execuções de runs da fila em andamento **neste
  processo** (as de outros processos não são contadas).
- `scale(name, n)` muda a concorrência do worker deste processo em tempo real (uma redução para
  novas buscas além de `n` de uma vez; jobs em andamento terminam; um aumento vale na próxima
  busca). Não é salvo e não afeta outros processos: para mudar em todo lugar, mude `concurrency` na
  task e faça o deploy. Mantenha `pool.max` acima da concorrência total + 5 (um aviso é logado
  quando não está).
