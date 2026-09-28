# Documentação do minitrigger (pt-BR)

`minitrigger` é uma biblioteca TypeScript para execução durável de tasks e workflows, construída
sobre **BullMQ + Redis + Postgres** (transporte padrão) e instalada como uma dependência comum no
seu próprio projeto. Não há plataforma, deploy separado ou dashboard: você aponta a biblioteca para
um Postgres (e, no modo padrão, um Redis) que já roda, define tasks no seu próprio código, e o
Postgres é a fonte da verdade de cada run — no modo padrão, se o Redis for perdido (um `FLUSHALL`,
por exemplo), nenhuma run é perdida, só atrasada. Desde a 0.4.0, `transport: "postgres"` roda o
engine inteiro só em cima do Postgres, sem Redis nenhum — ver [14. Modo Postgres](14-modo-postgres.md).

Esta documentação é um guia em português complementar ao README (inglês, na raiz do repositório),
que continua sendo a referência oficial para toda assinatura de API, opção de configuração e
comportamento. Em caso de dúvida ou divergência, o README em inglês vale.

## Por onde começar

Leia nesta ordem se é a primeira vez que você usa o minitrigger:

1. [01. Conceitos](01-conceitos.md) — o que é o minitrigger, quando usar, o modelo de replay.
2. [02. Instalação](02-instalacao.md) — dependências, infraestrutura mínima, formas de instalar.
3. [03. Configuração](03-configuracao.md) — todas as opções de `createEngine`.
4. [04. Tasks e steps](04-tasks-e-steps.md) — `defineTask`, `ctx`, `StepApi`, regras de ouro.

## Sumário

| Guia | Conteúdo |
|---|---|
| [01. Conceitos](01-conceitos.md) | O que é o minitrigger, quando usar (e quando não usar), o modelo de replay com memoização, status de uma run. |
| [02. Instalação](02-instalacao.md) | Dependências peer, requisitos de versão, infraestrutura Docker, as três formas de instalar o pacote. |
| [03. Configuração](03-configuracao.md) | Cada campo de `createEngine(config)`, organizado por seção, com a fórmula de dimensionamento do pool. |
| [04. Tasks e steps](04-tasks-e-steps.md) | `defineTask`, `ctx`, `StepApi` completo, `WaitResult`, as 14 regras de ouro, serialização. |
| [05. Espera (sleep, tokens, subtasks)](05-espera.md) | `step.sleep`, tokens externos, subtasks (`trigger`/`triggerAndWait`/`batchTriggerAndWait`), timeout de batch, cancelamento recursivo. |
| [06. Outbox transacional](06-outbox.md) | `trigger(task, payload, { tx })`, NOTIFY/LISTEN, PgBouncer, custo, limitações. |
| [07. Filas](07-filas.md) | Concorrência, `rateLimit`, `concurrencyKey`, `debounce`, pause/resume/scale de filas. |
| [08. Agendamentos](08-agendamentos.md) | Schedules imperativos vs. declarativos, idempotência de tick, ticks perdidos, `prefix` único por schema. |
| [09. Observabilidade](09-observabilidade.md) | Eventos, logs, progresso, `runs.subscribe`, streams, `attachDefaultLogger`. |
| [10. Testes](10-testes.md) | `createTestEngine`, `assertTriggered`/`refuteTriggered`/`allTriggered`, `runTask`. |
| [11. Produção](11-producao.md) | Dimensionamento de pool, PgBouncer, shutdown gracioso, upgrades de schema, retenção, líder/reconciler. |
| [12. Troubleshooting](12-troubleshooting.md) | Sintomas comuns e causa raiz: runs presas, erros de determinismo, deploy gradual, outbox, schedules, filas. |
| [13. Referência da API](13-referencia-api.md) | Referência compacta de `Engine`, `TriggerOptions`, `StepApi`, `TaskContext`, erros e config. |
| [14. Modo Postgres](14-modo-postgres.md) | `transport: "postgres"` (decisão 28): quando usar, o que não está disponível, conexões, PgBouncer, sem líder, guarda de modo misto, autovacuum de `system_jobs`, diferenças de comportamento. |

## Material complementar

- README em inglês (raiz do repositório): fonte da verdade para toda a API pública.
- `examples/basic/`: projeto consumidor real, instalado como dependência (`file:../..`), que
  exercita a API pública de ponta a ponta.
- `CLAUDE.md`: decisões de arquitetura e histórico de implementação (documento interno de
  engenharia, em português).
