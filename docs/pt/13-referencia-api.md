# 13. Referência da API

Referência compacta. Para explicação e exemplos de uso, veja os outros guias — cada seção aqui
linka para o guia correspondente.

## `createEngine(config)`

Ver [03. Configuração](03-configuracao.md) e a [tabela de config](#config-options) abaixo.

## `Engine`

```ts
interface Engine {
	migrate(): Promise<MigrateResult>;         // aplica migrations pendentes; seguro chamar concorrentemente
	start(options?: { queues?: string[] }): Promise<void>; // inicia workers; sem `queues` = toda fila de task declarada (o worker `-system` sempre inicia)
	stop(): Promise<void>;                      // shutdown gracioso; idempotente
	trigger<T extends AnyTask>(task: T, payload?, options?: TriggerOptions): Promise<RunHandle<TaskOutput<T>>>;
	runs: {
		get<O>(run: string | RunHandle<O>): Promise<Run<O> | null>;
		list(options?: ListRunsOptions): Promise<RunList>;
		cancel(run: string | RunHandle<unknown>): Promise<"canceled" | "already_finished" | "not_found">;
		replay<O>(run: string | RunHandle<O>): Promise<RunHandle<O>>; // run nova, mesmo payload, sem chave de idempotência
		cancelMany(filter: BatchRunsFilter): Promise<CancelManyResult>;
		retryFailed(filter: BatchRunsFilter): Promise<RetryFailedResult>;
		logs(run, options?: { afterId?: string | null; limit?: number }): Promise<RunLogPage>;
		steps(run): Promise<RunStep[]>;
		subscribe(run, options?: { signal?: AbortSignal }): AsyncIterable<RunStatusUpdate>;
		readStream<T = Json | string>(run, name: string, options?: { signal?: AbortSignal }): AsyncIterable<T>;
	};
	tokens: {
		complete(tokenId: string, output?: unknown): Promise<CompleteTokenOutcome>;
	};
	schedules: {
		upsert<T extends AnyTask>(input: ScheduleUpsertInput<T>): Promise<Schedule>;
		delete(id: string): Promise<boolean>;
		list(): Promise<Schedule[]>;
	};
	events: {
		on(event: RunEventName, handler: (event: RunEvent) => void | Promise<void>): () => void;
	};
	queues: {
		pause(name: string): Promise<QueueState>;
		resume(name: string): Promise<QueueState>;
		list(): Promise<QueueInfo[]>;
		scale(name: string, concurrency: number): Promise<void>;
	};
	logger: EngineLogger; // config.logger (padrão: warn/error no console); usado por attachDefaultLogger()
}
```

`payload` é obrigatório a menos que o `run` da task não receba nenhum (então é opcional e precisa
ser omitido ou `undefined`) — o tipo exato é `PayloadArgs<TaskPayload<T>, [options?: TriggerOptions]>`.
Um engine que nunca chama `start()` ainda pode `trigger()` e consultar — sem workers, sem consumo
de fila.

`attachDefaultLogger(engine, logger?)` — subscreve um `EngineLogger` (dado, ou `engine.logger`) a
`engine.events` e devolve uma função de unsubscribe. Opt-in; exportado da entrada principal, não é
um método de `Engine`. Ver [09. Observabilidade](09-observabilidade.md#attachdefaultlogger).

## `TriggerOptions`

```ts
type TriggerOptions = {
	idempotencyKey?: string;
	idempotencyKeyTtl?: Duration;
	delay?: Duration;
	debounce?: { key: string; delay: Duration; payload?: "last" | "first"; maxDelay?: Duration };
	concurrencyKey?: string;
	tx?: TransactionClient; // ver 06-outbox.md
};
```

`TransactionClient` é só `{ query(text, values?) }` — um client node-postgres (`PoolClient`/`Client`,
nunca um `Pool`) com `BEGIN` já rodado, ou o retorno de um dos adaptadores de ORM
(`typeormTx`/`knexTx`/`drizzleTx`/`prismaTx`, ver
[06. Outbox](06-outbox.md#com-um-orm-typeorm-knex-drizzle-prisma)).

Ver [05. Espera](05-espera.md), [06. Outbox](06-outbox.md), [07. Filas](07-filas.md).

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

Ver [04. Tasks e steps](04-tasks-e-steps.md), [05. Espera](05-espera.md).

## `TaskContext` (`ctx`)

```ts
type TaskContext = {
	step: StepApi;
	logger: TaskLogger;               // debug/info/warn/error(message, data?)
	signal: AbortSignal;
	run: { id: string; taskId: string; attempt: number; createdAt: Date };
	stream(name: string, source: AsyncIterable<Json | string>): Promise<void>;
	progress: { set<T>(value: T & Serializable<T>): Promise<void> };
};
```

Ver [04. Tasks e steps](04-tasks-e-steps.md#ctx-segundo-argumento-de-run),
[09. Observabilidade](09-observabilidade.md).

## Erros

| Erro | Quando |
|---|---|
| `ConfigError` | Configuração inválida de engine/task (durações, identificadores, conflitos de fila, opções de retry/backoff, env ruim). |
| `SerializationError` | Um valor de payload/output/log/progress falhou a checagem de serializabilidade JSON ou excedeu seu limite de tamanho. |
| `AbortTaskRunError` | Lançado pelo código do usuário para falhar a run permanentemente, sem retentativa. |
| `NonDeterminismError` | A sequência de steps observada no replay divergiu da gravada (nome ou tipo errado numa posição). |
| `DuplicateStepError` | O mesmo nome de step foi usado duas vezes numa execução. |
| `NestedStepError` | `ctx.step.*` foi chamado de dentro do callback de um `step.run` (steps não aninham). |
| `ExecutionLostError` | O processo em execução perdeu a posse da run (cancelada, expirada, roubada pelo reconciliador, devolvida no shutdown) — lançado em chamadas de step em voo; nenhuma escrita adicional acontece. |
| `TaskNotRegisteredError` | `engine.trigger`/um schedule referenciou uma task não registrada neste engine, ou um worker adquiriu uma run de uma task que não conhece (retentável, ex. durante um deploy gradual). `step.trigger*` aceita uma task filha não registrada — outro processo que a registre a executa. |

Todos os erros acima estendem `MinitriggerError`. Ver [12. Troubleshooting](12-troubleshooting.md).

## Config options

| Opção | Padrão | Guia |
|---|---|---|
| `postgres` | — | [03](03-configuracao.md#conexão-schema-e-prefix) |
| `redis` | — | [03](03-configuracao.md#conexão-schema-e-prefix) |
| `schema` | `"minitrigger"` | [03](03-configuracao.md#conexão-schema-e-prefix) |
| `prefix` | `"minitrigger"` | [03](03-configuracao.md#conexão-schema-e-prefix), [08](08-agendamentos.md#prefix-precisa-ser-único-por-schema) |
| `tasks` | — | [04](04-tasks-e-steps.md) |
| `pool.max` | concorrência total das filas + 5 (mín. 10) | [03](03-configuracao.md#pool-de-conexões), [11](11-producao.md#dimensionamento-do-pool) |
| `pool.connectionTimeoutMs` | `10 000` | [03](03-configuracao.md#pool-de-conexões) |
| `heartbeatIntervalMs` | `10 000` | [03](03-configuracao.md#heartbeat-stall-e-shutdown) |
| `heartbeatTimeoutMs` | `2 000` | [03](03-configuracao.md#heartbeat-stall-e-shutdown) |
| `stallTimeoutMs` | `60 000` | [03](03-configuracao.md#heartbeat-stall-e-shutdown) |
| `shutdownTimeoutMs` | `30 000` | [03](03-configuracao.md#heartbeat-stall-e-shutdown), [11](11-producao.md#shutdown-gracioso) |
| `idempotencyKeyTtl` | `"7d"` | [03](03-configuracao.md#outros) |
| `retention.days` | `30` | [03](03-configuracao.md#retenção), [11](11-producao.md#retenção) |
| `retention.intervalMs` | `3 600 000` | [03](03-configuracao.md#retenção) |
| `retention.batchSize` | `1 000` | [03](03-configuracao.md#retenção) |
| `schedules.pruneDeclarative` | `false` | [08](08-agendamentos.md#prunedeclarative) |
| `reconciler.intervalMs` | `15 000` | [03](03-configuracao.md#reconciliador), [11](11-producao.md#líder-e-reconciliador) |
| `reconciler.graceMs` | `30 000` | [03](03-configuracao.md#reconciliador) |
| `reconciler.batchSize` | `100` | [03](03-configuracao.md#reconciliador) |
| `outbox.pollIntervalMs` | `2 000` | [06](06-outbox.md#configuração-outbox) |
| `outbox.batchSize` | `500` | [06](06-outbox.md#configuração-outbox) |
| `outbox.reconnectMaxMs` | `30 000` | [06](06-outbox.md#configuração-outbox) |
| `outbox.notify` | `true` | [06](06-outbox.md#configuração-outbox) |
| `limits.maxPayloadBytes` | `1 MB` | [04](04-tasks-e-steps.md#serialização) |
| `limits.maxStepOutputBytes` | `256 KB` | [04](04-tasks-e-steps.md#serialização) |
| `limits.maxLogEntryBytes` | `16 KB` | [04](04-tasks-e-steps.md#serialização), [09](09-observabilidade.md#logs) |
| `limits.maxProgressBytes` | `16 KB` | [04](04-tasks-e-steps.md#serialização), [09](09-observabilidade.md#progresso) |
| `limits.maxInlineStepOutputBytes` | `16 MB` | [03](03-configuracao.md#limites-de-payloadoutputlogprogresso) |
| `logging.minLevel` | `"info"` | [03](03-configuracao.md#logging), [09](09-observabilidade.md#logs) |
| `logging.mirror` | `true` | [03](03-configuracao.md#logging), [09](09-observabilidade.md#logs) |
| `systemConcurrency` | `5` | [03](03-configuracao.md#outros) |
| `logger` | console, só warn/error | [03](03-configuracao.md#logging) |

`Duration` (usado em vários campos) aceita um número de milissegundos ou uma string como `"5m"`,
`"2d"`, `"1.5h"` (unidades: `ms`, `s`, `m`, `h`, `d`, `w`).

## Outros exports (entrada principal)

Não repetidos acima porque já cobertos por seção própria ou por uma tabela: os tipos auxiliares de
`runs.*`/`queues.*`/`schedules.*`/`events.*` (`Run`, `RunList`, `ListRunsOptions`, `RunStep`,
`RunLogEntry`, `RunLogPage`, `CancelRunOutcome`, `CancelManyResult`, `BatchRunsFilter`,
`BatchRunError`, `RetryFailedResult`, `RetrySkipReason`, `CompleteTokenOutcome`, `QueueInfo`,
`QueueState`, `RunEvent`, `RunEventName`, `RunEventHandler`, `RunStatusUpdate`, `SubscribeOptions`,
`ReadStreamOptions`, `Schedule`, `ScheduleUpsertInput`), os tipos de task/step (`Task`, `AnyTask`,
`TaskOptions`, `TaskPayload<T>`, `TaskOutput<T>`, `TaskLogger`, `TaskProgress`,
`StepTriggerOptions`, `TokenHandle`, `WaitResult<T>`, `RunHandle<O>`, `TaskQueueOptions`,
`TaskScheduleOptions`), `SerializedError` (formato `{ name, message, stack?, cause? }` de um erro
persistido) e os erros (`MinitriggerError` e subclasses, ver [Erros](#erros)).

O que falta:

| Export | O que é |
|---|---|
| `checkEnvironment`, `CheckEnvironmentOptions`, `EnvCheckReport`, `EnvCheckWarning`, `EnvCheckName`, `MIN_POSTGRES_VERSION_NUM` | Checagens autônomas de versão do Postgres / política de eviction do Redis / migrations pendentes (decisão 22) — `engine.start()` já chama isto com `autoMigrate: false`; chame direto para, por exemplo, um readiness probe. `MIN_POSTGRES_VERSION_NUM` é o piso checado (`130000`, ou seja 13.0). |
| `MigrateResult` | Retorno de `engine.migrate()`: `{ applied: number[] }`, as versões aplicadas por aquela chamada. |
| `attachDefaultLogger`, `EngineEventSource` | Ver [09. Observabilidade](09-observabilidade.md#attachdefaultlogger); `EngineEventSource` é o formato mínimo `{ events }` que a função aceita (qualquer `Engine`/`TestEngine`). |
| `RunStatus`, `RunCursor`, `StepKind`, `StepStatus`, `LogLevel` | `Run.status`; o formato opaco do cursor de `list()`; o tipo e status de um step; o nível de uma entrada de log. |
| `Json`, `Serializable<T>` | Um valor JSON simples; a checagem em tempo de compilação usada por `defineTask`/`trigger`/`ctx.progress.set`/etc. para rejeitar um tipo de payload/output que não é serializável em JSON — ver [Serialização](04-tasks-e-steps.md#serialização). |
| `Duration`, `parseDuration` | O tipo `number \| "5m"` aceito por `delay`/`maxDurationMs`/etc., e a função que o interpreta. |
| `BackoffOptions`, `RetryOptions` | Formato de `defineTask({ retry })` — ver [05. Espera](05-espera.md). |

## Testing subpath (`@rivia-labs/minitrigger/testing`)

| Export | O que é |
|---|---|
| `createTestEngine`, `TestEngine`, `TestEngineConfig` | Client em memória parecido com `Engine`, para testes, e sua config. |
| `TestClock`, `TestClockOptions` | API de `testEngine.clock` (`now()`, `advance()`) e sua config `{ start?, autoAdvance? }`. |
| `InlineRunResult` | Retorno de `runInline`/`drive`: `{ runId, status, output, error, attempt }`. |
| `PendingToken`, `TestEngineTokens` | Formato de item de `tokens.pending(run)`; o tipo da sub-API `tokens` (acrescenta `pending` a `EngineTokens`). |
| `assertTriggered`, `refuteTriggered`, `allTriggered` | Ver [10. Testes](10-testes.md). |
| `AssertionError` | Lançado por `assertTriggered`/`refuteTriggered` quando a expectativa falha (estende `MinitriggerError`). |
| `TriggeredEngine`, `TriggeredMatch`, `TriggeredRun` | O formato mínimo `{ runs }` aceito pelos helpers acima; o formato do filtro `match`; o formato de uma run casada. |
| `runTask` | Ver [10. Testes](10-testes.md). |
| `RunTaskOptions`, `RunTaskResult`, `RunTaskStep`, `RunTaskLogEntry`, `RunTaskTriggered`, `ChildStub` | Opções de `runTask` (inclusive dublês de token/filha), seu retorno, e os formatos dos steps/linhas de log/filhas disparadas que ele registrou; `ChildStub` é o dublê de uma task filha. |

## Adaptadores de ORM (`@rivia-labs/minitrigger/typeorm\|knex\|drizzle\|prisma`)

Cada subpath exporta sua função `*Tx`, o(s) tipo(s) estrutural(is) que ela aceita, e reexporta
`TransactionClient`. Nenhum deles importa o ORM em si — os tipos são estruturais, casados contra o
que o seu ORM devolver. Ver [06. Outbox](06-outbox.md#com-um-orm-typeorm-knex-drizzle-prisma).

| Subpath | Wrapper | Tipo(s) estrutural(is) aceito(s) |
|---|---|---|
| `/typeorm` | `typeormTx(manager \| queryRunner)` | `TypeormEntityManagerLike`, `TypeormQueryRunnerLike` |
| `/knex` | `knexTx(trx)` | `KnexTransactionLike` |
| `/drizzle` | `drizzleTx(tx, sql)` | `DrizzleTransactionLike`, `DrizzleSqlTag` |
| `/prisma` | `prismaTx(tx)` | `PrismaTransactionLike` |
