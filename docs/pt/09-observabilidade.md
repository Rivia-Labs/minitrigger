# 09. Observabilidade

## Eventos (`engine.events.on`)

```ts
const off = engine.events.on("run:completed", event => {
	// { name, runId, taskId, status, attempt, error, updatedAt, startedAt?, durationMs? }
});
```

Nomes de evento: `run:queued`, `run:started`, `run:waiting`, `run:completed`, `run:failed`,
`run:canceled`, `run:retrying`.

- **Só transições feitas por este processo** são emitidas: o processo que disparou, adquiriu,
  suspendeu, finalizou, retentou ou cancelou a run; recuperações de stall no líder também emitem.
- Handlers rodam depois do commit, fora do caminho crítico da run; um handler que lança (síncrono
  ou assíncrono) é logado com `logger.error`.
- `run:completed`/`run:failed`/`run:retrying`/`run:waiting` também carregam `startedAt` (início
  desta tentativa, relógio do banco) e `durationMs` (quanto tempo a tentativa rodou antes desta
  escrita, relógio monotônico, `performance.now()` — nunca persistido em Postgres, é só telemetria).
  Ausente em `run:queued`, `run:started` e `run:canceled`, que não estão marcando o fim de uma
  tentativa.

## Logs

```ts
run: async (payload, ctx) => {
	ctx.logger.info("processando item", { itemId: payload.id });
	// ...
},
```

```ts
const page = await engine.runs.logs(runId, { limit: 100 }); // { items, nextAfterId }
await engine.runs.logs(runId, { afterId: page.nextAfterId });
```

Entradas de `ctx.logger` no nível `logging.minLevel` (padrão `"info"`) ou acima são guardadas em
`run_logs` em lotes — um `INSERT ... unnest` por lote, com flush a cada 500 ms, a cada 50 entradas,
e **sempre** antes da conclusão/falha/suspensão da run ser gravada — e espelhadas para o `logger`
do engine com `runId`, `taskId` e `attempt` (`logging.mirror`, padrão `true`). `data` maior que
`limits.maxLogEntryBytes` é truncado com um marcador (o espelho recebe o original). Entradas ainda
em buffer quando a execução perde a posse da run (cancelada, travada/stall, devolvida no shutdown)
são descartadas com um `warn`. Um lote que falha é retentado uma vez, depois descartado com um
`warn`: logs nunca falham uma run.

`runs.logs(run, { afterId?, limit? })` — mais antigos primeiro, paginado com `afterId` (`null` =
desde o início; padrão 100, máximo 1 000).

## Progresso

```ts
run: async (payload, ctx) => {
	for (let i = 0; i < items.length; i++) {
		await process(items[i]);
		await ctx.progress.set({ processed: i + 1, total: items.length });
	}
},
```

```ts
(await engine.runs.get(runId))?.progress; // { processed: 340, total: 800 } | null
```

`ctx.progress.set(value)` sobrescreve um único valor por run (`runs.progress`; `null` até a
primeira chamada) — **não é um log, não é um step**: não é memoizado e não participa da detecção de
não determinismo, então chamá-lo um número diferente de vezes entre replays não tem problema. O
tipo de `value` é inferido por chamada (`set<T>(value: T & Serializable<T>)`, igual a
`step.run`/um payload), então uma interface ou tipo de objeto comum funciona direto, não só `Json`.
É validado como um payload (`Serializable`, `limits.maxProgressBytes`, padrão 16 KB) e rejeita com
`SerializationError` se for grande demais — um erro comum que a task pode capturar, diferente de um
argumento inválido em `ctx.step.*`.

Escritas são coalescidas (no máximo uma a cada 250 ms; só o último valor de uma rajada sobrevive) e
o último valor definido é liberado antes da próxima conclusão, falha ou suspensão da run ser
gravada, e antes de `engine.stop()` devolver uma run ainda em execução para a fila (o flush
acontece antes da liberação, enquanto este processo ainda é dono da run). Um valor é descartado só
quando esta execução já perdeu a posse da run — mesmo fallback de "descarta com aviso" que
`ctx.logger` usa nessa situação. Uma escrita que falha é retentada uma vez; se a retentativa também
falhar, só aquele valor é descartado (com aviso). O progresso sobrevive a retentativas e retomadas
(o engine nunca o reseta sozinho).

`engine.runs.get`/`runs.list` leem o último valor de volta; `runs.subscribe` emite
`update.progress` a cada mudança persistida (best-effort via pub/sub, com uma releitura periódica
como rede de segurança; uma mudança só de progresso é emitida sozinha, não apenas como efeito
colateral de uma transição de status).

## `runs.subscribe`

```ts
for await (const update of engine.runs.subscribe(runId, { signal })) {
	console.log(update.status, update.progress, update.updatedAt); // termina depois de completed/failed/canceled
}
```

- Funciona num engine que nunca chamou `start()` (abre sua própria conexão de subscriber, fechada
  por `stop()`).
- **Ordem garantida**: assina o pub/sub primeiro, depois lê a run em Postgres e emite esse status,
  depois só emite mudanças mais novas que a última emitida, e termina logo depois de um status
  terminal (na hora, se a run já estiver terminada; sem emitir nada se a run não existir).
- `break`, `options.signal` e `engine.stop()` também terminam a iteração.
- **Rede de segurança**: pub/sub é at-most-once, então depois de 15 s sem mensagens a run é relida
  do Postgres (status e progresso).

## `ctx.stream` / `runs.readStream`

```ts
// dentro da task
await ctx.stream("answer", tokenGenerator());

// de fora
for await (const token of engine.runs.readStream<string>(runId, "answer")) {
	process.stdout.write(token);
}
```

`ctx.stream(name, source)` acrescenta cada item de `source` (strings ou valores JSON) a um Redis
Stream desta run (`XADD MAXLEN ~ 10000`), depois grava um marcador de fim — **sempre**, também
quando `source` lança ou `ctx.signal` aborta (então o marcador carrega o erro e `ctx.stream`
rejeita com ele). Todo append renova a expiração da chave (1 dia), então um stream cujo escritor
morreu ainda expira sozinho. **Não é um step**: não é memoizado, então um replay transmite de novo.

`runs.readStream` lê do início do stream (um leitor tardio recebe todo chunk ainda guardado) e
termina no marcador de fim do escritor. Quando o stream terminou com erro, a iteração **lança** um
`MinitriggerError` (com o `Name: message` original) depois de entregar os chunks escritos antes
dele. Cada leitor usa sua própria conexão Redis, fechada quando a iteração termina (ou em
`stop()`, que termina a iteração de forma limpa, sem erro, como `runs.subscribe`).

**Leitores param no primeiro marcador de fim.** Se uma run retenta depois de um erro de stream, a
próxima tentativa que chama `ctx.stream` com o mesmo nome acrescenta ao mesmo stream, depois desse
marcador: um leitor que começa do início nunca vê os chunks da retentativa. Use um nome por
tentativa (ex.: `` `answer-${ctx.run.attempt}` ``) quando um stream precisa sobreviver a
retentativas.

## `attachDefaultLogger`

```ts
import { attachDefaultLogger } from "@rivia-labs/minitrigger";

const detach = attachDefaultLogger(engine); // usa o próprio config.logger do engine
// attachDefaultLogger(engine, pino());     // ou um logger explícito compatível com pino/console

// depois, para parar:
detach();
```

Opt-in — nem `createEngine` nem `start()` chama isso por você. Assina `engine.events` e escreve uma
linha estruturada por transição de run:

| Evento | Nível | Campos extras |
|---|---|---|
| `run:started` | `info` | — |
| `run:completed` | `info` | `durationMs` |
| `run:failed` | `error` | `durationMs`, `error: { name, message }` |
| `run:retrying` | `warn` | `durationMs`, `error: { name, message }` |
| `run:canceled` | `warn` | — |

Toda linha também carrega `runId`, `taskId`, `attempt`. **Segurança por padrão:** uma linha de
falha/retentativa nunca inclui o `SerializedError` completo (sem `stack`, sem `cause` encadeado) e
nunca o payload ou output da run — só `error.name`/`error.message`. `run:queued`/`run:waiting` não
são logados (mais barulhentos, menos acionáveis por padrão); assine-os você mesmo via
`engine.events.on` se precisar. Uma chamada de log que lança (um `logger` customizado quebrado) é
capturada e reportada uma vez via `logger.error`, nunca relançada dentro de `engine.events`.
