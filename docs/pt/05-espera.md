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

### Exemplo: um fluxo de aprovação

Uma run que carrega dados, pede aprovação a um humano por um link, espera até três dias pela
resposta (sem segurar um worker) e então paga depois de um período de espera. Todo efeito colateral
está dentro de um step, então um crash ou uma retentativa a qualquer momento repete os steps já
concluídos a partir do Postgres em vez de rodá-los de novo. Steps são pelo menos uma vez, porém: um
que é interrompido no meio do caminho (um crash depois do e-mail sair mas antes do resultado do
step ser salvo) roda de novo, então torne os próprios efeitos colaterais idempotentes onde uma
duplicata importa (por exemplo, a chave de idempotência do provedor de pagamento, como em
[Retentativas e backoff](04-tasks-e-steps.md#exemplo-retentativas-e-aborttaskrunerror)).

```ts
import { AbortTaskRunError, defineTask } from "@rivia-labs/minitrigger";

type Decision = { approved: boolean; note: string };

export const approveExpense = defineTask({
	id: "approve-expense",
	retry: { maxAttempts: 5, backoff: { type: "exponential", baseMs: 2_000, maxMs: 60_000 } },
	run: async (payload: { expenseId: string; approver: string }, ctx) => {
		const expense = await ctx.step.run("load", () => db.expenses.find(payload.expenseId));
		if (!expense) throw new AbortTaskRunError(`expense ${payload.expenseId} not found`); // sem retentativa

		const token = await ctx.step.createToken("approval", { timeout: "3d" });
		await ctx.step.run("email-approver", () =>
			mailer.send(payload.approver, `https://app.example.com/approve?token=${token.id}`),
		);

		const decision = await ctx.step.waitForToken<Decision>(token); // suspende: não ocupa slot de worker
		if (!decision.ok) {
			// decision.error.name === "Timeout": ninguém respondeu dentro de 3 dias
			await ctx.step.run("escalate", () => mailer.send("finance@example.com", `stuck: ${expense.id}`));
			return { status: "escalated" as const };
		}
		if (!decision.output.approved) return { status: "rejected" as const, note: decision.output.note };

		await ctx.step.sleep("cool-off", "1h"); // durável: sobrevive a restarts e deploys
		const payment = await ctx.step.run("pay", () => payments.pay(expense.id, expense.amount));
		ctx.logger.info("expense paid", { paymentId: payment.id });
		return { status: "paid" as const, paymentId: payment.id };
	},
});
```

O handler do link completa o token a partir de qualquer processo (um engine que nunca chamou
`start()` já basta):

```ts
// ex.: POST /approve?token=...
async function handleApproval(tokenId: string, approved: boolean, note: string) {
	const outcome = await engine.tokens.complete(tokenId, { approved, note });
	if (!outcome.completed) {
		// "not_found" | "completed" (já respondido) | "timed_out" | "run_finished"
		return { status: 409, body: { reason: outcome.reason } };
	}
	return { status: 204 };
}
```

`tokens.complete` só registra o output e faz a run ficar devida; um worker a pega na hora. Completar
duas vezes é inofensivo (`reason: "completed"`), então o handler pode ser retentado pelo client.

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

### Exemplo: subtasks

```ts
import { AbortTaskRunError, defineTask } from "@rivia-labs/minitrigger";

export const resizeImage = defineTask({
	id: "resize-image",
	queue: { name: "images", concurrency: 8 },
	run: async (payload: { url: string; width: number }) => ({ url: await resize(payload.url, payload.width) }),
});

export const notifyFollowers = defineTask({
	id: "notify-followers",
	run: async (payload: { albumId: string }) => { /* ... */ },
});

export const publishAlbum = defineTask({
	id: "publish-album",
	run: async (payload: { albumId: string; photos: string[] }, ctx) => {
		// Fan-out: uma run filha por foto; o pai suspende e retoma exatamente uma vez.
		const thumbs = await ctx.step.batchTriggerAndWait(
			"thumbnails",
			resizeImage,
			payload.photos.map(url => ({ url, width: 320 })),
			{ timeout: "30m" },
		);
		// Um WaitResult por payload, na mesma ordem. Uma filha falha ou expirada nunca lança aqui.
		const ok = thumbs.flatMap(r => (r.ok ? [r.output.url] : []));
		const failed = thumbs.filter(r => !r.ok).length;
		if (failed > 0) ctx.logger.warn("some thumbnails failed", { failed });

		const cover = await ctx.step.triggerAndWait("cover", resizeImage, {
			url: payload.photos[0] ?? "",
			width: 1200,
		});
		// Um replay devolve a mesma falha memoizada, então retentar o pai não ajudaria: aborte.
		if (!cover.ok) throw new AbortTaskRunError(`cover failed: ${cover.error.message}`);

		// Fire-and-forget: a filha roda por conta própria, o pai não espera por ela.
		await ctx.step.trigger("notify", notifyFollowers, { albumId: payload.albumId });
		return { thumbnails: ok, cover: cover.output.url };
	},
});
```

Cada filha é uma run comum (visível em `runs.list()` com `parentRunId` setado, retentada pela sua
própria política de `retry`); o pai só vê o resultado final dela. Cancelar o pai cancela suas
filhas pendentes.

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

Uma varredura noturna que retenta toda run `send-email` falhada do último dia, página por página
(segura de rodar duas vezes: uma run já retentada nunca é candidata de novo):

```ts
import type { BatchRunsFilter } from "@rivia-labs/minitrigger";

const filter: BatchRunsFilter = {
	taskId: "send-email",
	createdFrom: new Date(Date.now() - 24 * 3_600_000),
	limit: 200,
};
let page = await engine.runs.retryFailed(filter);
for (;;) {
	for (const { runId, newRunId } of page.retried) console.log(`retentada ${runId} como ${newRunId}`);
	for (const { runId, reason } of page.skipped) console.warn(`pulada ${runId}: ${reason}`);
	if (!page.hasMore || !page.nextCursor) break;
	page = await engine.runs.retryFailed({ ...filter, cursor: page.nextCursor });
}
```

- Filtro: `taskId?`, `queue?`, `createdFrom?` (inclusivo), `createdTo?` (exclusivo), `cursor?`, e um
  `limit` **obrigatório** (1 a 1 000 por chamada). Runs são percorridas da mais antiga para a mais
  nova; quando há mais correspondências, `hasMore` é `true` — passe `nextCursor` de volta como
  `cursor`.
- `cancelMany` cancela as runs `queued`/`running`/`waiting` que casam com o filtro, cada uma
  exatamente como `runs.cancel()` (descendentes pendentes também, execuções em andamento abortadas,
  eventos e `runs.subscribe` notificados), cada uma em sua própria transação curta. Uma run que
  falha ao cancelar (hiccup de store/Redis) entra em `failed` (`{ runId, error }`) sem travar o
  resto do lote; para retentar só essas, chame `cancelMany` de novo com o **mesmo** `cursor` desta
  chamada (não `nextCursor` — os candidatos desta página continuam `queued`/`running`/`waiting`,
  já que falharam ao cancelar, então são casados de novo a partir do mesmo ponto de partida); sem
  cursor nenhum (começar do zero) também os retenta, junto com tudo antes dessa página.
- `retryFailed` retenta as runs `failed` de nível raiz que casam com o filtro, como
  `runs.replay()`: uma run **nova** da mesma task com o mesmo payload (a tentativa recomeça; a run
  falhada original fica como estava; a chave de concorrência dela é mantida só enquanto a fila
  ainda declarar `concurrencyKey`, igual a `runs.replay()`). Uma run retentada por esta chamada é
  **excluída da candidatura** em toda chamada futura (casada pela chave de idempotência
  `__retry:<id>`, decisão 9): paginar com `nextCursor` é seguro mesmo com outras chamadas retentando
  runs ao mesmo tempo, e repetir o mesmo filtro do início (ex.: uma varredura diária) só retenta
  cada run falhada uma vez — você não precisa rastrear quais já tratou. Runs com pai ficam de fora
  (a falha delas já foi entregue ao pai: retente o pai). `skipped` lista runs cuja task este
  processo não registra (`task_not_registered`), que uma chamada concorrente retentou primeiro na
  pequena janela entre listar e criar a retentativa (`already_retried`, raro na prática dado o
  acima), ou cujo payload guardado não valida mais (`invalid_payload`).
- **Custo em tabelas `runs` grandes.** O `cancelMany` lê as runs ativas por um índice parcial
  (`idx_runs_active_created`, migration 008), então a primeira página não percorre o histórico
  finalizado: ~0,04 ms numa tabela com 1M de runs finalizadas (antes do índice, ~143 ms). O
  `retryFailed` avança a partir da run falha mais antiga; como as falhas costumam estar espalhadas
  pelo histórico, continua barato (~0,5 ms na mesma medição), mas `createdFrom` (ex.: "não mais que
  24h") ainda limita a varredura se a tabela tiver uma longa sequência de sucessos antes das falhas
  que interessam. A paginação por `cursor` busca pelo índice e nunca varre de novo.
