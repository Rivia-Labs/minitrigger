# 04. Tasks e steps

## `defineTask`

```ts
const sendWelcomeEmail = defineTask({
	id: "send-welcome-email",
	queue: { name: "emails", concurrency: 10 },
	retry: { maxAttempts: 3, backoff: { type: "exponential", baseMs: 1_000, maxMs: 60_000 } },
	maxDurationMs: 30_000,
	run: async (payload: { userId: string }, ctx) => {
		await ctx.step.run("send", () => sendEmail(payload.userId));
	},
});
```

Campos de `defineTask`:

| Campo | Obrigatório | Descrição |
|---|---|---|
| `id` | sim | Identificador da task. É um contrato: não renomeie uma task com runs pendentes em produção sem migrar os dados. |
| `queue` | não | `{ name, concurrency?, globalConcurrency?, rateLimit?, concurrencyKey? }`. Sem `queue`, a task pertence à fila `"default"` (concorrência 10, salvo declaração diferente). Ver [07. Filas](07-filas.md). |
| `retry` | não | `{ maxAttempts, backoff: { type: "exponential" \| "fixed", baseMs, maxMs, jitter? } }`. Padrão: `{ maxAttempts: 3, backoff: { type: "exponential", baseMs: 1_000, maxMs: 60_000 } }`, `jitter` padrão `true`. |
| `maxDurationMs` | não | Timeout por **tentativa**, via `ctx.signal`. Não conta enquanto a run está suspensa. |
| `schedule` | não | `{ cron, timezone?, payload? }` — schedule declarativo. Ver [08. Agendamentos](08-agendamentos.md). |
| `run` | sim | `(payload, ctx) => Promise<Output>`. `Payload`/`Output` são inferidos daqui. |

Uma task genérica (`<T extends Json>`) não consegue ter seu parâmetro de tipo inferido a partir de
`trigger()` sozinho — precisa de um cast explícito no call site. Uma task tipada com `payload:
unknown` explícito é tratada como "sem payload" (`trigger(task)` chamável sem segundo argumento) —
use um tipo nomeado, mesmo que permissivo, se precisar que `trigger()` exija um argumento.

## `ctx` (segundo argumento de `run`)

```ts
type TaskContext = {
	step: StepApi;
	logger: TaskLogger;              // debug/info/warn/error(message, data?)
	signal: AbortSignal;              // cancelamento, maxDurationMs, perda de posse, shutdown, ou fim da tentativa
	run: { id: string; taskId: string; attempt: number; createdAt: Date };
	stream(name: string, source: AsyncIterable<Json | string>): Promise<void>;
	progress: { set<T>(value: T & Serializable<T>): Promise<void> };
};
```

- `ctx.logger` — ver [09. Observabilidade](09-observabilidade.md#logs).
- `ctx.signal` — dispara em cancelamento, `maxDurationMs` estourado, perda de posse (roubada pelo
  reconciliador, execução perdida) e também **quando a tentativa termina** (completou, falhou,
  retentativa agendada, liberada, ou qualquer outra saída do executor — motivo `"finished"`), para
  que trabalho que a task deixou rodando (uma promise não aguardada, um stream, um step ainda em
  voo quando `maxDurationMs` cortou a espera) pare em vez de duplicar efeitos colaterais junto com
  a próxima tentativa. Uma suspensão também dispara o signal.
- `ctx.run` — metadados fixos da execução atual (id da run, id da task, número da tentativa, data
  de criação).
- `ctx.stream(name, source)` e `ctx.progress.set(value)` — ver
  [09. Observabilidade](09-observabilidade.md#streams) e
  [09. Observabilidade](09-observabilidade.md#progresso).

## `StepApi`

```ts
interface StepApi {
	run<T>(name: string, fn: () => Promise<T> | T): Promise<T>;
	sleep(name: string, duration: Duration): Promise<void>;
	createToken(name: string, opts?: { timeout?: Duration }): Promise<{ id: string; name: string }>;
	waitForToken<T = Json>(tokenOrName: TokenHandle | string): Promise<WaitResult<T>>;
	trigger<T extends AnyTask>(name: string, task: T, payload?, opts?: { delay?: Duration }): Promise<RunHandle<TaskOutput<T>>>;
	triggerAndWait<T extends AnyTask>(name: string, task: T, payload?, opts?: { timeout?: Duration }): Promise<WaitResult<TaskOutput<T>>>;
	batchTriggerAndWait<T extends AnyTask>(name: string, task: T, payloads: TaskPayload<T>[], opts?: { timeout?: Duration }): Promise<WaitResult<TaskOutput<T>>[]>;
}
```

- **`run(name, fn)`** — roda `fn` uma vez e memoiza seu retorno (JSON serializável). Replays
  seguintes devolvem o valor salvo sem chamar `fn` de novo.
- **`sleep(name, duration)`** — suspende a run até `duration` decorrer, sem ocupar slot de worker.
- **`createToken`/`waitForToken`**, **`trigger`/`triggerAndWait`/`batchTriggerAndWait`** — ver
  [05. Espera](05-espera.md) para o guia completo.

## `WaitResult<T>`

`waitForToken`, `triggerAndWait` e cada item de `batchTriggerAndWait` devolvem um `WaitResult<T>`
em vez de lançar em caso de falha/timeout:

```ts
type WaitResult<T> =
	| { ok: true; output: T }
	| { ok: false; error: SerializedError }; // error.name é "Timeout" ou "Canceled" nesses casos
```

Então um subtask que falha, expira ou é cancelado nunca lança dentro do pai — sempre confira
`.ok`:

```ts
const result = await ctx.step.triggerAndWait("archive", archiveArticle, { title });
if (!result.ok) {
	// result.error.name pode ser "Timeout", "Canceled" ou o nome do erro real da filha
	return { status: "archive-failed" as const, error: result.error.message };
}
return { status: "published" as const, archiveId: result.output.archiveId };
```

## Nomes de step reservados

Nomes de step não podem conter `#` nem `:`:

- `#` é reservado para nomes internos, como `${nome}#wait` (o step de espera associado a um
  `waitForToken`).
- `:` separa componentes das chaves internas de run filha (`__child:*`).

Um nome inválido falha a run **sem retentativa** (`ConfigError`, marcado fatal antes mesmo do
registro do step).

## Serialização

Payloads, outputs de step, outputs de token e outputs de task precisam ser JSON puro: nada de
`Date`, `Map`, `Set`, `RegExp`, funções, `bigint` ou instâncias de classe com métodos. Isso é
verificado tanto em **tempo de compilação** (o tipo `Serializable<T>`, que funciona com `interface`
e com `type`) quanto em **tempo de execução**.

| O quê | Limite padrão |
|---|---|
| Payload de trigger / output de token / **output de task** | `maxPayloadBytes` — 1 MB |
| Output de `step.run` | `maxStepOutputBytes` — 256 KB |
| `data` de uma entrada de log | `maxLogEntryBytes` — 16 KB (truncado com um marcador acima disso) |
| Valor de `ctx.progress.set` | `maxProgressBytes` — 16 KB |

Uma função de task/step que retorna `void` (ou `undefined`) tem esse output gravado como JSON
`null` — não espere receber exatamente `undefined` de volta de um `step.run` memoizado ou de
`runs.get(...).output`.

## Regras de ouro

1. **Nada não determinístico fora de um step.** Valores aleatórios, `Date.now()`, chamadas de
   rede, leituras de env que podem mudar — tudo isso pertence dentro de `ctx.step.run`, não no
   corpo puro da função da task, que reexecuta a cada replay.
2. **Nomes de step devem ser únicos e estáveis dentro de uma run.** Não derive um nome de step de
   algo que pode mudar entre replays; ao nomear steps num loop, inclua o índice
   (`` `send-${i}` ``) para que iterações visualmente idênticas não colidam.
3. **Steps devem ser idempotentes quando possível.** O efeito colateral de um step pode, em
   caminhos raros de recuperação, ser tentado mais de uma vez antes de sua conclusão ser
   durabilizada.
4. **Mantenha outputs de step pequenos.** Eles têm um teto (`maxStepOutputBytes`, 256 KB por
   padrão) e são lidos de volta a cada replay — guarde referências (um id, uma URL) em vez de
   blobs grandes.
5. **Não reordene nem remova steps enquanto runs estão aguardando.** Uma mudança em deploy que
   muda a ordem ou os nomes dos steps quebra o replay de qualquer run suspensa no meio
   (`NonDeterminismError`).
6. **Não engula exceções ao redor de chamadas `step.*`.** Internamente, a suspensão é sinalizada
   lançando uma exceção; um `try/catch` genérico ao redor de `step.sleep`/`step.waitForToken` não
   quebra o replay (a suspensão é autoritativa), mas capturar e descartar uma falha **real** de
   step anula as retentativas — só capture o que você pretende de fato tratar.
7. **Sem steps aninhados.** Chamar `ctx.step.*` de dentro do callback de um `step.run` lança
   `NestedStepError` — steps não aninham.
8. **Mova trabalho pesado de CPU para fora da thread principal.** Trabalho síncrono longo dentro de
   um step bloqueia o heartbeat e pode parecer um stall para o reconciliador; use
   `worker_threads` para trabalho intensivo em CPU.
9. **Não chame steps depois de um `await` dentro de ramos concorrentes.** Steps são registrados na
   ordem de **conclusão**, não na ordem de chamada, uma vez que um ramo continua fazendo `await` e
   chamando mais steps (ex.: `Promise.all(items.map(async i => { await ctx.step.run(a); await
   ctx.step.run(b); }))`); essa ordem pode diferir entre a run original e um replay e arrisca
   `NonDeterminismError`. Chame os steps antes de qualquer `await` dentro de um ramo, ou rode-os
   sequencialmente.
10. **Um output `void`/`undefined` de step ou task é gravado como `null`.** Não conte em receber
    exatamente `undefined` de volta de um `step.run` memoizado ou de `runs.get(...).output` — ele
    volta como JSON `null`.
11. **Uma task genuinamente genérica precisa de um cast no call site.** `defineTask` infere
    `Payload`/`Output` a partir de `run`; uma task tipada `<T extends Json>` não consegue ter `T`
    inferido a partir de `trigger()` sozinho.
12. **`stop()` é at-least-once, não exactly-once.** Uma run ainda ativa depois de
    `shutdownTimeoutMs` é devolvida à fila e retentada sem incrementar a tentativa; um efeito
    colateral que já aconteceu fora de um step memoizado pode rodar de novo, a menos que seja
    idempotente.
13. **Ticks de cron perdidos não são recuperados.** Se todo processo está fora do ar quando um
    tick agendado deveria disparar, essa ocorrência é pulada para sempre — não roda atrasada nem
    acumula.
14. **Uma tentativa que falhou espera pelos steps ainda em voo — por um tempo limitado.** Quando a
    função da task lança enquanto outros steps ainda estão em execução (ex.: um step de um
    `Promise.all` rejeita), o engine espera por eles antes de gravar a falha, sem abortar
    `ctx.signal`: um step que completa é gravado e a retentativa reaproveita seu output em vez de
    rodá-lo de novo. A espera é limitada por `stallTimeoutMs` (padrão 60 s), ou por
    `maxDurationMs` se este terminar antes; passado o limite, `ctx.signal` aborta e a tentativa
    falha (ou retenta) com o erro original, e os steps não terminados reexecutam na retentativa.
    Um padrão como `Promise.race([ctx.step.run("x", ...), prazo])` cujo step perdedor nunca
    assenta custa no máximo `stallTimeoutMs` do slot do worker, não para sempre. Um step que
    suspenderia nesse meio-tempo é ignorado — o erro vence. Um step que ignora `ctx.signal`
    continua rodando depois do fim da tentativa (decisão 20 — o resultado tardio é descartado pela
    checagem de posse).
