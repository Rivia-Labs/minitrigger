# 01. Conceitos

## O que é o minitrigger

`minitrigger` executa **tasks** — funções assíncronas definidas no seu próprio código — de forma
durável: se o processo cair no meio, a task continua de onde parou (ou refaz só o que não foi
memoizado) assim que outro processo (ou o mesmo, reiniciado) assumir a run. Não há plataforma
hospedada, nem dashboard por padrão, nem deploy separado: a biblioteca roda dentro dos seus
próprios processos Node.js, usando um Postgres e um Redis que você já opera.

Os três conceitos centrais:

- **Task**: definida com `defineTask({ id, run, ... })`. É código seu, registrado no engine.
- **Run**: uma execução de uma task, criada com `engine.trigger(task, payload)`. Cada run vira uma
  linha em Postgres, com id, status, payload, tentativa atual e (quando termina) output ou erro.
- **Step**: uma unidade de trabalho dentro de uma task (`ctx.step.run`, `ctx.step.sleep`,
  `ctx.step.waitForToken`, `ctx.step.trigger`, ...). Steps são **memoizados**: uma vez completos,
  seu resultado é lido de volta em vez de reexecutado.

## Postgres é a fonte da verdade; BullMQ é só transporte

Toda run, step, waitpoint, chave de idempotência e log vive em Postgres. O BullMQ (sobre Redis) é
usado só como mecanismo de entrega: quem decide se uma run existe, em que status está e o que ela
já fez é sempre o Postgres. Isso tem uma consequência direta: **se o Redis for perdido (um
`FLUSHALL`, por exemplo), nenhuma run é perdida** — o reconciliador (rodando no processo líder)
volta a enfileirar toda run pendente lendo direto do Postgres, dentro de um tempo limitado (por
padrão, na ordem de `reconciler.graceMs + reconciler.intervalMs`, ou seja, dezenas de segundos, não
instantâneo). Veja [11. Produção](11-producao.md) para os detalhes de líder e reconciliador.

O inverso não vale: uma escrita em Postgres sem o enfileiramento correspondente em Redis (o
processo cai entre as duas escritas de `trigger()`) também é recuperada da mesma forma — é
exatamente o cenário que o reconciliador existe para cobrir.

## Quando usar

- Projetos pessoais ou de time pequeno com tasks que precisam sobreviver a um crash ou a um
  redeploy no meio da execução — por exemplo, scraping + chamada de LLM + aprovação humana +
  notificação.
- Você já roda Postgres e Redis (ou não se importa de adicionar os dois) e quer durabilidade sem
  montar uma plataforma de workflow separada.
- Você aceita que a função da task seja **reexecutada do início a cada retomada** (veja a seção
  seguinte) — código fora de `ctx.step.*` precisa ser determinístico.

## Quando não usar (non-goals)

- **Não é uma plataforma hospedada**: sem dashboard web por padrão (um dashboard opcional é uma
  fase futura possível), sem billing, sem multi-tenancy, sem camada de autenticação.
- **Não faz deploy do seu código**: você continua rodando seus próprios processos.
- **Não faz checkpoint de processo** (nada como CRIU): a durabilidade vem de replay + memoização,
  não de congelar o processo em execução.
- **Só Postgres**: nenhum outro backend de banco está planejado.

## O modelo de replay com memoização

Este é o ponto mais importante para entender antes de escrever a primeira task: **a função da task
é reexecutada do início a cada retomada** (depois de um crash, uma suspensão ou uma retentativa).

- Steps que já completaram devolvem o valor salvo em vez de rodar `fn` de novo.
- Código que **não** está dentro de `ctx.step.*` roda de novo, sempre, a cada retomada.

Isso implica:

- Tudo que é não determinístico — valores aleatórios, `Date.now()`, chamadas de rede, mutação de
  estado externo — precisa estar **dentro** de um step.
- Um step é identificado pelo **nome**, na ordem de chamada (`seq_index`). Se o nome observado no
  replay não bate com o que foi salvo, a run falha com `NonDeterminismError`.
- Nomes de step não podem conter `#` nem `:` (ambos reservados internamente: `#` para nomes como
  `${nome}#wait`, `:` para chaves internas de run filha); um nome inválido falha a run sem
  retentativa.

```ts
const importReport = defineTask({
	id: "import-report",
	run: async (payload: { url: string }, ctx) => {
		// ERRADO: Date.now() e o fetch fora de um step reexecutam a cada replay,
		// podendo produzir valores diferentes a cada vez.
		// const startedAt = Date.now();
		// const raw = await fetch(payload.url);

		const raw = await ctx.step.run("fetch", async () => {
			const res = await fetch(payload.url);
			return res.text();
		});

		const parsed = await ctx.step.run("parse", () => parseReport(raw));
		return { rows: parsed.length };
	},
});
```

Veja as 14 regras de ouro completas em [04. Tasks e steps](04-tasks-e-steps.md#regras-de-ouro).

## Status de uma run

| Status | Significado |
|---|---|
| `queued` | Aguardando ser adquirida por um worker. |
| `running` | Em execução em algum processo. |
| `waiting` | Suspensa: aguardando um step (`sleep`, um token, um subtask). Não ocupa slot de worker. |
| `completed` | Terminou com sucesso; `output` disponível. |
| `failed` | Esgotou as tentativas (ou um erro fatal/`AbortTaskRunError`); `error` disponível. |
| `canceled` | Cancelada via `engine.runs.cancel`/`cancelMany`, direta ou recursivamente (descendentes pendentes). |

`queued`, `running`, `waiting` são estados não terminais. `completed`, `failed`, `canceled` são
terminais — uma vez ali, a run não muda mais de status (para tentar de novo, use
`engine.runs.replay()`, que cria uma run **nova**).

Para consultar runs, ver [09. Observabilidade](09-observabilidade.md) (`engine.runs.get`,
`runs.list`, `runs.subscribe`, `runs.logs`, `runs.steps`).
