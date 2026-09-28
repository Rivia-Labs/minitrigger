# 08. Agendamentos (cron)

## Imperativo vs. declarativo

```ts
// Imperativo: fonte "api" em Postgres, chaveado por qualquer id que você escolher.
await engine.schedules.upsert({
	id: "daily",
	task: myTask,
	cron: "0 9 * * *",         // 5 ou 6 campos separados por espaço (um campo de segundos no
	                           // início é opcional; 7 campos são rejeitados desde a 0.4.0 — ver
	                           // "Upgrade 0.3.0 para 0.4.0" em 11. Produção)
	timezone: "America/Belem", // fuso IANA opcional; omitido = padrão do BullMQ (baseado em UTC)
	payload: { url: "..." },   // obrigatório (tipado como o payload de myTask) a menos que myTask não tenha payload
});
await engine.schedules.delete("daily"); // true se um schedule com esse id existia
await engine.schedules.list();          // todo schedule (api + declarativo), por id

// Declarativo: sincronizado por todo engine.start() (fonte "declarative" em Postgres, id = id da task).
export const dailyDigest = defineTask({
	id: "daily-digest",
	schedule: { cron: "0 9 * * *", timezone: "America/Belem", payload: { url: "..." } },
	run: async (payload: { url: string }) => { /* ... */ },
});
```

Postgres é a fonte da verdade para `cron`/`timezone`/`payload`: tanto `engine.schedules.*` quanto a
sincronização declarativa gravam ali primeiro, e só então sincronizam o job scheduler do BullMQ na
fila interna `${prefix}-system`; uma falha do BullMQ logo depois da escrita em Postgres só é
logada — o tick do reconciliador (no líder) e o próprio disparo seguinte do schedule se
autocorrigem (um schedule sem seu scheduler do BullMQ é recriado; um scheduler órfão sem linha
correspondente em Postgres é removido; um scheduler cujo cron/timezone divergiu de Postgres é
recriado). Todo tick lê `cron`/`timezone`/`payload` direto de Postgres — nunca da cópia guardada no
próprio job do BullMQ.

## Idempotência de tick

Cada tick dispara a task com a chave de idempotência `` `__schedule:${id}:${scheduledAtMs}` `` (o
horário lógico agendado do tick, não `Date.now()`), então dois processos com seus workers
`-system` correndo o mesmo tick (ou uma redelivery em nível de BullMQ) criam exatamente uma run.

## Ticks perdidos não são recuperados

**Ticks perdidos enquanto todo processo está fora do ar não são recuperados.** Um job scheduler do
BullMQ só mantém uma única "próxima iteração" pendente por vez; se nenhum processo está rodando
para processá-la ao longo de vários intervalos, só a única ocorrência seguinte ao momento em que um
processo volta é agendada — toda ocorrência no meio é silenciosamente pulada. Isso é uma limitação
do próprio BullMQ, não algo o minitrigger pode contornar.

## `pruneDeclarative`

Por padrão, nada declarado com `defineTask({ schedule })` é apagado automaticamente, mesmo depois
de remover a declaração do código — configure `schedules: { pruneDeclarative: true }` no engine
para também apagar (linha em Postgres + scheduler no BullMQ) um schedule declarativo que nenhuma
task declara mais, no próximo `start()`. Isso nunca toca em schedules `"api"` criados com
`engine.schedules.upsert()`.

**Footgun de `pruneDeclarative`: ele olha para toda task registrada *neste* engine.** Se processos
diferentes iniciam engines com registros de task heterogêneos contra o mesmo schema — por exemplo,
um deploy gradual onde a versão antiga e a nova rodam lado a lado por um tempo, ou dois serviços
compartilhando intencionalmente um schema mas cada um registrando só algumas tasks — um `start()`
com `pruneDeclarative: true` num processo cujo registro não inclui uma dada task vai tratar o
schedule declarativo dela como "não declarado mais" e apagá-lo, mesmo que outro processo ainda a
declare (e precise dela). Prefira `pruneDeclarative: true` só num único processo/deploy garantido
de ter o conjunto completo e atual de tasks agendadas registrado; caso contrário, deixe desligado e
apague schedules obsoletos explicitamente.

## Um id de schedule não troca de fonte sozinho

`engine.schedules.upsert()` (fonte `"api"`) e `defineTask({ schedule })` (fonte `"declarative"`,
chaveado pelo id da própria task) nunca sobrescrevem a linha um do outro para o mesmo id: um
`upsert()` mirando um id já dono de um schedule declarativo lança `ConfigError` (renomeie o
schedule, ou remova a opção `schedule` daquele `defineTask()` se ele deveria ser gerenciado pela
API); a sincronização declarativa, simetricamente, pula (loga um erro nomeando a task, deixa a
linha `"api"` existente intocada) em vez de tomar posse de um id que um `upsert()` já possui —
todas as outras tasks continuam sincronizando normalmente, e `pruneDeclarative` também.

## Tick cujo task não está registrado retenta

Se `processScheduleTick` não consegue resolver o `taskId` do schedule no registro deste processo
(por exemplo, no meio de um deploy gradual), o job `schedule-tick` **falha** em vez de ser
silenciosamente descartado — o template do seu job scheduler do BullMQ dá a ele `attempts: 5` com
backoff exponencial (a única exceção à regra "jobs de run usam `attempts: 1`": é seguro aqui porque
a chave de idempotência `__schedule:${id}:${scheduledAtMs}` torna uma retentativa que cai num
processo que já teve sucesso um no-op), para que outro processo (ou este mesmo, quando o deploy
terminar) tenha a chance de pegar o mesmo tick antes de ele se perder de vez.

## `prefix` precisa ser único por schema

Toda estrutura Redis — as filas de task, a fila interna `${prefix}-system` (ticks de schedule,
expirações de waitpoint, retenção), canais de pub/sub e streams — é chaveada só por `prefix`, não
por `schema`. Dois engines com o mesmo `prefix` e `schema`s diferentes consumiriam jobs um do
outro: um job de run adquirido contra o schema errado fica perdido até o reconciliador reenfileirá-
lo, e um tick de schedule processado pelo engine do outro schema costumava remover o job scheduler
do BullMQ daquele schedule. Compartilhar um prefix é por isso recusado: o primeiro
`engine.start()` grava seu schema na chave Redis `${prefix}:owner-schema`, e um `start()` com outro
schema nesse prefix lança `ConfigError` (apague essa chave só quando o schema dono dela for
aposentado de verdade).

Duas guardas adicionais cobrem deploys que contornam essa checagem (ex.: engines iniciados antes de
ela existir): todo job scheduler é marcado com o schema do engine que o criou, o reconciliador nunca
apaga um scheduler marcado com outro schema, e um `schedule-tick` marcado com outro schema é
recusado (logado como erro, retentado pelo BullMQ, nada criado ou removido). Elas limitam o dano;
não fazem um prefix compartilhado funcionar.

A exceção é um schema **aposentado**: quando um job scheduler (ou tick) marcado com outro schema é
encontrado e esse schema, sua tabela `schedules` ou a linha do schedule não existem mais no banco
deste engine, o scheduler é removido (um `warn`) em vez de falhar todo tick para sempre. Essa
checagem só enxerga o banco do próprio engine, então sozinha ela chamaria erroneamente um schema de
"aposentado" sempre que ele vive num banco **diferente** (um deploy blue/green: o engine antigo no
banco A, o novo no banco B, ambos compartilhando um prefix Redis) — ela não tem como ver que o
schema do engine antigo ainda está lá, ainda ticando, só que não no banco B. Uma segunda guarda,
independente de banco, fecha essa brecha: a remoção só acontece enquanto ESTE engine segura a chave
dona do prefix (`${prefix}:owner-schema`) EXCLUSIVAMENTE para o próprio schema — a mesma chave que
`start()` grava acima. **Não apague `${prefix}:owner-schema` enquanto o schema dono dela ainda
puder estar rodando** (ex.: antes de um engine antigo ter parado completamente durante um cutover
blue/green): enquanto essa chave estiver ausente ou apontando para outro schema, este engine trata
todo scheduler marcado com outro schema como vivo e nunca o remove, não importa o que o próprio
banco diga. Apague a chave só quando o engine do schema aposentado estiver confirmadamente parado,
para que `claimPrefix()` do novo engine possa assumi-la.
