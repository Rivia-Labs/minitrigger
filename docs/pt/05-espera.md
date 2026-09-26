# 05. Espera (sleep, tokens, subtasks)

Todas as formas de "aguardar algo" em uma task suspendem a run sem ocupar um slot de worker: a run
vai para o status `waiting` e só volta a rodar quando o que ela espera se resolve. Isso é diferente
de bloquear uma função com `await` num processo comum — durante a espera, o worker está livre para
executar outras runs.

## `step.sleep`

```ts
await ctx.step.sleep("cooldown", "2s");
```

Suspende a run até `duration` decorrer. `duration` é um `Duration`: um número de milissegundos, ou
uma string como `"5m"`, `"2d"`, `"1.5h"` (unidades: `ms`, `s`, `m`, `h`, `d`, `w`). Não ocupa slot
de worker enquanto espera — a retomada é feita por um job atrasado do BullMQ, que reaquire a run
quando o horário chega.

## Tokens (espera por um sinal externo)

```ts
const token = await ctx.step.createToken("approval", { timeout: "20s" });
await ctx.step.run("request-approval", async () => {
	await notifyApprover(token.id); // webhook, e-mail com link, etc.
});
const approval = await ctx.step.waitForToken<boolean>(token);
if (!approval.ok || !approval.output) {
	return { status: "rejected" as const };
}
```

- **`createToken(name, { timeout? })`** cria um token e devolve `{ id, name }`. Entregue o `id` a
  quem vai completá-lo de fora (um handler de webhook, um humano clicando num link).
- **`waitForToken(tokenOrName)`** suspende a run até o token ser completado (ou expirar, se
  `timeout` foi passado). Devolve um `WaitResult<T>` — nunca lança por causa de timeout.
- Completar um token de fora da task:

```ts
const outcome = await engine.tokens.complete(tokenId, { approved: true });
// outcome: { completed: true } | { completed: false, reason: "not_found" | "completed" | "timed_out" | "run_finished" }
```

`output` é validado com `maxPayloadBytes` antes de tocar o banco. Um `tokenId` que não é UUID
devolve `not_found`; `run_finished` acontece quando o token já tinha expirado (`timed_out`) e a run
dona dele já é terminal.

## Subtasks

```ts
// Dispara sem esperar (fire-and-forget dentro da run).
await ctx.step.trigger("notify", sendNotification, { userId });

// Dispara e espera o resultado.
const archived = await ctx.step.triggerAndWait("archive", archiveArticle, { title, summary }, {
	timeout: "10s",
});
if (!archived.ok) { /* archived.error.name pode ser "Timeout", "Canceled" ou o erro real */ }

// Dispara vários e espera todos (a run pai retoma exatamente uma vez).
const results = await ctx.step.batchTriggerAndWait("archive-all", archiveArticle, items, {
	timeout: "30s",
});
```

- **`trigger(name, task, payload, opts?)`** inicia um subtask sem aguardar. `opts.delay` atrasa o
  início.
- **`triggerAndWait(name, task, payload, { timeout? })`** inicia e suspende até o subtask terminar
  (ou expirar).
- **`batchTriggerAndWait(name, task, payloads[], { timeout? })`** inicia vários subtasks e suspende
  até todos terminarem (ou expirarem); a run pai retoma exatamente uma vez, com um `WaitResult<T>`
  por item, na mesma ordem dos `payloads`. `payloads: []` consome o `seq_index` do step, não grava
  nada e devolve `[]`; um número de filhas diferente do de `payloads` observado no replay causa
  `NonDeterminismError`.

O resultado de uma filha (`triggerAndWait`/`batchTriggerAndWait`) é lido direto de `runs`/
`waitpoints` quando o pai retoma — não é copiado para dentro de `steps.output`.

## Timeout de batch: só as filhas ainda pendentes são canceladas

Quando um `batchTriggerAndWait` expira, só as filhas **ainda pendentes** (e as árvores delas) são
canceladas pela expiração. Filhas que já tinham completado, falhado, ou que foram canceladas por
outro motivo antes do timeout mantêm seu resultado real — só quem estava mesmo pendente no momento
do timeout vira `{ ok: false, error: { name: "Timeout", retryable: false } }`. Uma filha cancelada
(por `engine.runs.cancel` antes do timeout, por exemplo) aparece como `error.name: "Canceled"`.

## Cancelamento recursivo

```ts
await engine.runs.cancel(runId);
```

Cancela a run e, **recursivamente**, seus descendentes pendentes — subtasks iniciados via
`step.trigger`/`triggerAndWait`/`batchTriggerAndWait` que ainda não terminaram. O `ctx.signal` da
run cancelada dispara; código que ignora o signal e continua rodando não consegue mais escrever
nada (a checagem de posse rejeita toda escrita de uma execução que perdeu a run).

Cancelamento funciona entre processos: os ids das runs canceladas são publicados via pub/sub do
Redis (`${prefix}:cancel`), e todo engine iniciado que compartilha o mesmo `prefix` aborta a
execução local dessas runs. Um engine que só dispara (nunca chamou `start()`) também pode cancelar:
ele publica, só não assina. O pub/sub é só o caminho rápido — se uma mensagem se perde, a run
continua `canceled` em Postgres e a checagem de posse rejeita qualquer escrita tardia da execução
antiga (o próximo heartbeat dela também aborta).

Note que uma **falha definitiva** de uma run (não um cancelamento) **não** cancela descendentes —
filhas de `step.trigger` podem sobreviver ao pai, como acontece também quando o pai completa
normalmente.

### Cancelamento e retentativa em massa

```ts
let cursor;
do {
	const res = await engine.runs.cancelMany({ taskId: "import-csv", limit: 500, cursor });
	console.log(`canceladas ${res.canceled.length}`);
	cursor = res.nextCursor ?? undefined;
} while (cursor);

const { retried, skipped, hasMore, nextCursor } = await engine.runs.retryFailed({
	queue: "emails",
	createdFrom: new Date(Date.now() - 24 * 3_600_000),
	limit: 100,
});
```

- Filtro: `taskId?`, `queue?`, `createdFrom?` (inclusivo), `createdTo?` (exclusivo), `cursor?`, e um
  `limit` **obrigatório** (1 a 1 000 por chamada). Runs são percorridas da mais antiga para a mais
  nova; quando há mais correspondências, `hasMore` é `true` — passe `nextCursor` de volta como
  `cursor`.
- `cancelMany` cancela as runs `queued`/`running`/`waiting` que casam com o filtro, cada uma
  exatamente como `runs.cancel()` (descendentes pendentes também, execuções em andamento abortadas,
  eventos e `runs.subscribe` notificados), cada uma em sua própria transação curta.
- `retryFailed` retenta as runs `failed` de nível raiz que casam com o filtro, como
  `runs.replay()`: uma run **nova** da mesma task com o mesmo payload (a tentativa recomeça; a run
  falhada original fica como estava; a chave de concorrência dela é mantida). Cada run falhada é
  retentada no máximo uma vez, mesmo com chamadas concorrentes. Runs com pai ficam de fora (a falha
  delas já foi entregue ao pai: retente o pai). `skipped` lista runs cuja task este processo não
  registra (`task_not_registered`) ou que uma chamada concorrente retentou primeiro
  (`already_retried`).
