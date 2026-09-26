# 10. Testes

## `createTestEngine`

`createTestEngine({ tasks })`, importado do subpath **`@rivia-labs/minitrigger/testing`** (não a
entrada principal), roda seu código de task de verdade com **nenhum Redis e nenhum Postgres**: um
store e uma fila em memória, dirigidos pelo mesmo executor de produção (replay, memoização,
suspensão, retentativas, checagens de não determinismo). Nada roda em background; é o teste que
dirige. O subpath mantém `createTestEngine` e seu backend em memória fora do bundle/tempo de
carregamento da entrada principal para quem nunca testa dessa forma; as classes de erro
(`ConfigError`, `NonDeterminismError`, etc.) continuam vindo da entrada principal, e checagens
`instanceof` contra elas funcionam igual para erros lançados por uma task rodando sob
`createTestEngine` — as duas entradas compartilham o mesmo chunk compilado, não cópias separadas.

```ts
import { createTestEngine } from "@rivia-labs/minitrigger/testing";
import { expect, it } from "vitest";
import { approval } from "./tasks.js";

it("aprova um documento", async () => {
	const engine = createTestEngine({ tasks: [approval] });

	// Dispara e roda até a run terminar ou bloquear. Sleeps e backoff de retentativa são instantâneos.
	const first = await engine.runInline(approval, { doc: "contrato.pdf" });
	expect(first.status).toBe("waiting"); // bloqueado em ctx.step.waitForToken

	const [token] = await engine.tokens.pending(first.runId);
	await engine.tokens.complete(token!.id, { approved: true });

	const final = await engine.drive(first.runId); // roda de novo até ficar ocioso
	expect(final).toMatchObject({ status: "completed", output: { status: "approved" } });
	await engine.stop();
});
```

Pontos importantes:

- Uma filha disparada com `step.trigger*` cuja task não está em `tasks` do test engine fica sem
  processar: o test engine só roda tasks registradas, então a filha falha com um
  `TaskNotRegisteredError` retentável. Registre toda task que o seu teste exercita.
- `runInline(task, payload, options?)` devolve `{ runId, status, output, error, attempt }`. Filhas
  de `triggerAndWait`/`batchTriggerAndWait` também rodam inline. Uma run falhada é devolvida, não
  lançada: `error` é o erro armazenado (ex.: `{ name: "NonDeterminismError", ... }`).
- `trigger` só enfileira; `drain()` roda tudo que está devido; `drive(run)` = `drain()` + o
  resultado da run.
- **Relógio.** O engine tem seu próprio relógio (`clock: { start }`, padrão: agora), usado como o
  "relógio do banco". Com o padrão `autoAdvance: true`, quando nada está devido mas alguma run está
  atrasada (um `sleep`, um backoff de retentativa, `trigger({ delay })`), o relógio pula direto para
  o job atrasado **mais cedo** de **qualquer** tipo, em ordem de tempo — um timeout de token ou de
  `triggerAndWait` incluso, mesmo um que nenhuma execução tenha alcançado
  `waitForToken`/`triggerAndWait` ainda. Use `autoAdvance: false` e `await engine.clock.advance(...)`
  para intercalar em vez disso (ex.: completar um token antes do timeout dele mesmo que um step
  posterior fosse dormir além dele).
- `runs.*`, `tokens.complete`, `events.on`, `schedules.*` e `runs.subscribe`/`readStream` funcionam
  como em `createEngine`. Não há `migrate`/`start`. Schedules de cron não disparam sozinhos:
  `await engine.tickSchedule(id)` dispara uma ocorrência (um `defineTask({ schedule })` usa o id da
  task) — sincroniza todo `defineTask({ schedule })` no store primeiro (de forma preguiçosa, uma
  vez; `schedules.*`, `drain`/`drive`/`runInline`/`clock.advance` e `trigger` também disparam essa
  sincronização), rejeita com `ConfigError` para um id de schedule desconhecido, e rejeita com o
  erro que o próprio job de tick falhou.
- `EngineClient` (`Pick<Engine, "trigger" | "runs" | "tokens" | "schedules" | "events" |
  "queues">`) é a superfície que `Engine` e `TestEngine` compartilham: use-a para tipar um helper
  que funciona contra qualquer um dos dois.
- `queues.pause`/`resume`/`list` funcionam em memória. `queues.scale` lança (sem workers reais: um
  job por vez).
- `debounce` funciona como em produção para triggers emitidos antes do próximo drain.
  `concurrencyKey` também é checado, mas o test engine roda um job por vez, então duas runs de uma
  chave nunca se sobrepõem por conta própria; use `autoAdvance: false` em testes que exercitam o
  backoff.
- `maxDurationMs`, heartbeats e flushes de log continuam usando timers reais. Um drain que processa
  mais que `maxJobsPerDrain` jobs (padrão 10 000) lança, para pegar runs que nunca assentam.

### Unit test simples de uma task com `runTask`

`runTask(task, payload, options?)` roda a função `run` de uma task diretamente — **sem engine, sem
store, sem fila** — para um teste unitário rápido da lógica isolada, do jeito que você testaria
qualquer outra função:

```ts
import { runTask } from "@rivia-labs/minitrigger/testing";
import { expect, it } from "vitest";
import { approval } from "./tasks.js";

it("aprova quando o token resolve com valor truthy", async () => {
	const result = await runTask(approval, { doc: "contrato.pdf" }, {
		tokens: { approval: { approved: true } },
	});
	expect(result).toMatchObject({ output: { status: "approved" }, error: null });
});
```

- `ctx.step.run(name, fn)` chama `fn` imediatamente (sem memoização/replay para checar contra) e
  registra a chamada; `ctx.step.sleep` resolve na hora. Ambos ficam registrados em `result.steps`.
- `ctx.step.createToken`/`waitForToken` e `ctx.step.trigger`/`triggerAndWait`/
  `batchTriggerAndWait` não têm um engine de verdade para resolvê-los, então precisam de um dublê
  de teste — `options.tokens: { [nome]: valor }` (ou uma função `() => valor`, chamada
  preguiçosamente) e `options.children: { [taskId]: (payload) => output }`, respectivamente — ou
  eles **lançam** um erro acionável (não devolvido como `result.error`: é um dublê de teste
  faltando, não um bug da task).
- `ctx.logger` cai em `result.logs`; `ctx.progress.set`, em ordem de chamada, em `result.progress`;
  `ctx.stream(name, ...)`, por nome de stream, em `result.streams`.
- Validação de payload/output reutiliza os mesmos checks reais de `Serializable`/tamanho
  (`config.limits`, sobrescrevível via `options.limits`). Um `payload` inválido lança (como
  `trigger()` faria). Um output de **task** inválido (o valor que o próprio `run` devolve) volta
  como `result.error`, não lançado — igual a uma run falhada em produção, `retryable: false`
  (determinístico: retentar não ajudaria). Um output de **step** inválido
  (`ctx.step.run` devolve um valor não serializável ou grande demais) é diferente: como em
  produção, é um `SerializationError` comum devolvido ao próprio código da task, não fatal — não
  capturado, vira um `result.error` normal com `retryable: true` (a task poderia capturá-lo, ou
  uma tentativa futura com output menor poderia funcionar).
- Se a task lança com steps em voo, `runTask` espera até `options.settleTimeoutMs` (padrão: o
  padrão de `stallTimeoutMs`, 60 s) por eles antes de reportar o erro, espelhando o limite de
  produção (`stallTimeoutMs`); um erro fatal (nome de step inválido, step duplicado ou aninhado)
  nunca espera, e um retorno bem-sucedido sempre espera por todo step.
- **O que isso *não* testa:** replay, memoização entre tentativas, suspensão, detecção de não
  determinismo, timeouts reais de waitpoint, ou qualquer outra coisa que depende de durabilidade.
  Use `createTestEngine` para isso; use `runTask` para a lógica que uma task calcula dados seus
  inputs.

## `assertTriggered` / `refuteTriggered` / `allTriggered`

Também do subpath `@rivia-labs/minitrigger/testing`, inspirados em `assertEnqueued`/
`refuteEnqueued`/`allEnqueued` do `@rivia-labs/oban-ts`: asserções sobre quais runs um `TestEngine`
disparou, construídas inteiramente sobre `engine.runs.list()` — então funcionam para uma filha
disparada por `ctx.step.trigger*` exatamente como para um `engine.trigger()` de nível raiz, e
também para um `Engine` de verdade (só precisam de `runs`, via a superfície compartilhada
`EngineClient`).

```ts
import { assertTriggered, createTestEngine, refuteTriggered } from "@rivia-labs/minitrigger/testing";
import { expect, it } from "vitest";
import { placeOrder, sendReceipt } from "./tasks.js";

it("cria um pedido e dispara um recibo", async () => {
	const engine = createTestEngine({ tasks: [placeOrder, sendReceipt] });
	const { id } = await engine.trigger(placeOrder, { orderId: 1 });
	await engine.drive(id); // roda placeOrder, que faz ctx.step.trigger("receipt", sendReceipt, ...)

	await assertTriggered(engine, { task: sendReceipt, payload: { orderId: 1 } });
	await refuteTriggered(engine, { task: sendReceipt, payload: { orderId: 2 } });
	await engine.stop();
});
```

Critérios de match (todos opcionais, combinados com E):

| Campo | Casa quando |
|---|---|
| `task` | a task (objeto ou id) é igual. |
| `payload` | o payload da run **contém** essas chaves com valores iguais (comparação profunda; um array aninhado precisa bater exatamente, não como subconjunto). |
| `status` | um status, ou qualquer um de vários. |
| `concurrencyKey` | é igual. |

`assertTriggered` resolve com o primeiro match (mais antigo primeiro), então você pode encadear
mais asserções a partir dele (`(await assertTriggered(engine, ...)).runId`); sem match, lança um
`AssertionError` listando o que FOI disparado (limitado, payloads truncados, nunca um dump
completo). `allTriggered(engine, match?)` devolve todos os matches, mais antigo primeiro.

## Quando usar cada abordagem

| Cenário | Ferramenta |
|---|---|
| Testar a lógica de uma task isoladamente (dado um input, qual output/steps/logs) | `runTask` |
| Testar um fluxo completo com suspensão real (sleep, token, subtask), memoização entre tentativas, não determinismo | `createTestEngine` + `runInline`/`drain`/`drive` |
| Confirmar que uma task disparou um subtask ou um `engine.trigger` com certo payload | `assertTriggered`/`refuteTriggered`/`allTriggered` |
| Testar contra infraestrutura real (Postgres/Redis), reconciliador, líder, shutdown gracioso | um `Engine` real com `pnpm infra:up` (testes de integração) |
