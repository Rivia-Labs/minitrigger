# 06. Outbox transacional

Passe sua própria transação como `tx` e a run é inserida **dentro dela**: sua escrita de negócio e
a run commitam — ou fazem rollback — juntas. Sem mais "pedido salvo, mas o job do e-mail nunca foi
enfileirado" (ou o inverso).

```ts
import { Pool } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL }); // mesmo banco do engine

const client = await pool.connect();
try {
	await client.query("BEGIN");
	const { rows } = await client.query(
		"INSERT INTO orders (customer_id, total) VALUES ($1, $2) RETURNING id",
		[customerId, total],
	);
	await engine.trigger(sendReceipt, { orderId: rows[0].id }, {
		tx: client,
		idempotencyKey: `receipt-${rows[0].id}`, // opcional; funciona dentro da transação também
	});
	await client.query("COMMIT"); // só agora a run pode executar
} catch (error) {
	await client.query("ROLLBACK"); // a run some também; nada é executado
	throw error;
} finally {
	client.release();
}
```

## Requisitos do `tx`

`tx` é um `PoolClient`/`Client` do node-postgres no qual você já rodou `BEGIN`, uma transação de
ORM embrulhada pelo [adaptador](#com-um-orm-typeorm-knex-drizzle-prisma) dela, ou qualquer objeto
com o mesmo formato `query(text, values)` vinculado a uma transação aberta. **Passar o `Pool` em
si é rejeitado**: as queries dele rodariam fora da sua transação.

O engine só roda statements parametrizados e qualificados por schema no seu client — nunca `SET`,
`LISTEN` ou outro estado de sessão.

## Com um ORM (TypeORM, knex, Drizzle, Prisma)

Cada ORM tem um adaptador pequeno num subpath próprio que transforma o handle de transação do ORM
no client `tx`. Os adaptadores não importam nada dos ORMs (nenhuma dependência extra; o ORM que
você não usa nunca é carregado) e mandam o SQL do engine pela API de query crua do próprio ORM,
**na conexão da transação**, sempre com parâmetros (bind), nunca interpolando valores. A run faz
commit ou rollback junto com a transação do ORM; todo o resto desta página vale igual.

```ts
// TypeORM (0.3.x / 1.x, driver postgres)
import { typeormTx } from "@rivia-labs/minitrigger/typeorm";
await dataSource.transaction(async manager => {
  const order = await manager.save(Order, { customerId, total });
  await engine.trigger(sendReceipt, { orderId: order.id }, { tx: typeormTx(manager) });
});

// knex (client pg)
import { knexTx } from "@rivia-labs/minitrigger/knex";
await knex.transaction(async trx => {
  const [order] = await trx("orders").insert({ customer_id: customerId, total }).returning("id");
  await engine.trigger(sendReceipt, { orderId: order.id }, { tx: knexTx(trx) });
});

// Drizzle (driver node-postgres ou postgres.js): passe também a tag `sql` do drizzle
import { sql } from "drizzle-orm";
import { drizzleTx } from "@rivia-labs/minitrigger/drizzle";
await db.transaction(async tx => {
  const [order] = await tx.insert(orders).values({ customerId, total }).returning();
  await engine.trigger(sendReceipt, { orderId: order.id }, { tx: drizzleTx(tx, sql) });
});

// Prisma (transação interativa, PostgreSQL)
import { prismaTx } from "@rivia-labs/minitrigger/prisma";
await prisma.$transaction(async tx => {
  const order = await tx.order.create({ data: { customerId, total } });
  await engine.trigger(sendReceipt, { orderId: order.id }, { tx: prismaTx(tx) });
});
```

| Subpath | Embrulha | Executa via | Rejeitado com `ConfigError` |
|---|---|---|---|
| `/typeorm` → `typeormTx(managerOuQueryRunner)` | o `manager` de `dataSource.transaction(...)`, ou um `QueryRunner` depois de `startTransaction()` | `queryRunner.query(sql, params, true)` | `dataSource.manager` (sem transação), runner sem transação ativa ou liberado (checado a cada query), driver que não é `postgres` |
| `/knex` → `knexTx(trx)` | o `trx` de `knex.transaction(...)` | `trx.raw(sql, bindings)`: cada `$n` vira `?` (um binding por ocorrência) e todo `?` literal é escapado como `\?` | a instância `knex` em si, transação já concluída (checado a cada query), client que não é `pg` |
| `/drizzle` → `drizzleTx(tx, sql)` | o `tx` de `db.transaction(...)` | `` tx.execute(sql`...`) `` montado com a sua tag `sql`, cada `$n` virando `sql.param(valor)` | o `db` em si, tag `sql` ausente |
| `/prisma` → `prismaTx(tx)` | o `tx` de `prisma.$transaction(async tx => ...)` | `tx.$queryRawUnsafe(sql, ...valores)` (placeholders `$n` como estão) | o `PrismaClient` em si (cada statement faria autocommit) |

- Os wrappers são explícitos de propósito: passar o handle cru do ORM como `tx` é rejeitado com um
  `ConfigError` que diz qual adaptador usar (um manager do TypeORM até tem um `query()` que seria
  chamado com o formato de resultado errado).
- Testados contra Postgres real com TypeORM 1.1, knex 3.3, drizzle-orm 0.45 (node-postgres e
  postgres.js) e Prisma 7.10 (gerador `prisma-client` + `@prisma/adapter-pg`). No Prisma, lembre do
  `timeout` da transação interativa (5 s por padrão), como para qualquer statement dela.
- Qualquer outro driver/ORM funciona do mesmo jeito: passe um objeto com `query(text, values)` que
  rode o statement `$1..$n` na conexão da transação e resolva para `{ rows }`.
- `drizzleTx` faz **uma ida e volta extra por chamada de `query()`** (mais uma no momento do
  wrap): o drizzle-orm 0.45.x não expõe um jeito confiável de saber se a transação já terminou, então
  o adaptador compara a identidade da própria transação no Postgres (`pg_current_xact_id()` +
  `pg_backend_pid()`) a cada chamada e lança `ConfigError` numa divergência (usar o wrapper depois
  que o callback de `db.transaction(...)` retornou rodaria em silêncio dentro de outra transação que
  reaproveitar a mesma conexão reciclada depois).
- `knexTx` **rejeita SQL cru que contenha uma barra invertida literal** — não só o escape `?`/`\?`
  acima, mas qualquer barra invertida no texto passado por ele (um literal de escape, um operador de
  regex, ...) — porque o escape de `?` do knex não consegue distinguir com segurança um placeholder
  injetado de um adjacente a essa barra. O SQL do próprio minitrigger nunca tem uma; isso só importa
  se você mandar outro SQL cru pelo mesmo `tx` embrulhado.

### Mais exemplos: transação manual, Prisma, `pg` puro e várias runs

Os exemplos abaixo assumem um único engine para o app inteiro (só disparando, ou iniciado — o
caminho `tx` é o mesmo) e uma task como:

```ts
export const sendReceipt = defineTask({
	id: "send-receipt",
	run: async (payload: { orderId: string }) => { /* ... */ },
});
```

Em todo caso abaixo, se a transação faz rollback (um erro lançado dentro do callback, um rollback
explícito, um `COMMIT` que falha), **a run nunca existiu**: nenhuma linha, nenhum job, nenhum
evento — não há nada para limpar. O engine precisa usar o mesmo banco que o ORM (qualquer conexão; a
linha da run é escrita pela sua transação, não pelo pool do engine).

**TypeORM com `QueryRunner`**, quando você mesmo controla a transação:

```ts
const runner = dataSource.createQueryRunner();
await runner.connect();
await runner.startTransaction();
try {
	const order = await runner.manager.save(Order, { customerId, total });
	await engine.trigger(sendReceipt, { orderId: order.id }, { tx: typeormTx(runner) });
	await runner.commitTransaction(); // só agora a run pode executar
} catch (error) {
	await runner.rollbackTransaction(); // a run some junto com o pedido
	throw error;
} finally {
	await runner.release();
}
```

`typeormTx(dataSource.manager)` (um manager fora de qualquer transação) lança `ConfigError` na
hora, assim como um wrapper usado depois de `commitTransaction()`/`rollbackTransaction()`/
`release()`: o estado da transação é checado a cada query, então uma run nunca faz autocommit em
silêncio por conta própria.

**knex com um objeto de transação** que você comita/desfaz você mesmo:

```ts
const trx = await knex.transaction();
try {
	const [order] = await trx("orders").insert({ customer_id: customerId, total }).returning("id");
	await engine.trigger(sendReceipt, { orderId: order.id }, { tx: knexTx(trx) });
	await trx.commit();
} catch (error) {
	await trx.rollback();
	throw error;
}
```

**Drizzle**, com rollback explícito (`tx.rollback()` lança; o pedido e a run desfazem juntos):

```ts
await db.transaction(async tx => {
	const [order] = await tx.insert(orders).values({ customerId, total }).returning();
	await engine.trigger(sendReceipt, { orderId: order!.id }, { tx: drizzleTx(tx, sql) });
	if (order!.total > creditLimit) tx.rollback();
});
```

**Prisma**, lembrando que o `timeout` da transação interativa (5 s por padrão) cobre também o
trigger:

```ts
const order = await prisma.$transaction(
	async tx => {
		const order = await tx.order.create({ data: { customerId, total } });
		await engine.trigger(sendReceipt, { orderId: order.id }, { tx: prismaTx(tx) });
		return order;
	},
	{ timeout: 10_000 },
);
```

Só a forma **interativa** funciona: `prisma.$transaction([...])` (a forma em array/lote) nunca
entrega um client de transação, então não há nada para passar como `tx`.

**`pg` puro, e qualquer outro driver.** Um `PoolClient`/`Client` do node-postgres com um `BEGIN`
aberto já é um `tx` válido (ver o primeiro exemplo desta página). Para qualquer outro driver, passe
um objeto cujo `query(text, values)` rode o statement (placeholders `$1..$n`, valores como
parâmetros vinculados) **na conexão da transação** e resolva para `{ rows }`. Por exemplo,
postgres.js sem ORM nenhum:

```ts
import postgres from "postgres";
import type { TransactionClient } from "@rivia-labs/minitrigger";

const sql = postgres(process.env.DATABASE_URL!);

await sql.begin(async tx => {
	const [order] = await tx`INSERT INTO orders (customer_id, total) VALUES (${customerId}, ${total}) RETURNING id`;
	const client: TransactionClient = {
		query: async (text, values = []) => ({ rows: await tx.unsafe(text, values as never[]) }),
	};
	await engine.trigger(sendReceipt, { orderId: order!.id }, { tx: client });
});
```

O engine só manda strings, números, booleanos e `null` como valores (o payload vai como texto JSON
com `::jsonb` explícito), e o insert da run converte todo placeholder explicitamente, então um
driver que tipa todo parâmetro vinculado como `text` (como as queries cruas do Prisma) também
funciona.

**Vários triggers numa mesma transação.** Dispare quantas runs precisar na mesma transação — da
mesma task ou de tasks diferentes. Elas comitam juntas, e com `outbox.notify` no padrão a
transação inteira manda um único `NOTIFY`:

```ts
await dataSource.transaction(async manager => {
	const order = await manager.save(Order, { customerId, total, items });
	const tx = typeormTx(manager); // um wrapper por transação já basta
	await engine.trigger(sendReceipt, { orderId: order.id }, { tx });
	await engine.trigger(reserveStock, { orderId: order.id }, { tx });
	for (const item of items) {
		await engine.trigger(notifyWarehouse, { orderId: order.id, sku: item.sku }, { tx });
	}
});
```

Dispare um depois do outro (`await` em cada um), não com `Promise.all`: eles compartilham uma única
conexão, que roda um statement por vez de qualquer forma.

**Com `transport: "postgres"`.** `trigger(..., { tx })` funciona igual em
[14. Modo Postgres](14-modo-postgres.md), e os adaptadores acima são usados sem mudança (o mesmo
insert parametrizado passa por eles): a própria linha da run é a fila nesse modo, então o insert na
sua transação é tudo o que existe — no commit, um `NOTIFY` acorda os workers da fila (o poll pega
de qualquer forma), e no rollback a linha nunca existiu. Não há uma varredura de outbox separada
nesse modo, e é por isso que as opções `outbox.*` são rejeitadas com `transport: "postgres"`.

## NOTIFY/LISTEN + varredura de 2s como rede de segurança

Nada é enfileirado antes do seu `COMMIT`. O insert também agenda um `NOTIFY` (entregue só no
commit, **um por transação**, não importa quantas runs ela dispara); todo engine iniciado faz
`LISTEN` num canal por schema, sobre uma conexão dedicada, e despacha as runs commitadas em poucos
milissegundos. `trigger()` devolve o handle na hora; o evento `run:queued`/o status é publicado
pelo engine que despacha a run, depois do commit.

**Se a notificação se perder** (listener reconectando, nenhum engine iniciado no momento do commit,
um pooler que quebra o `LISTEN`), todo engine iniciado também varre a cada **2 s** (e sempre que
(re)conecta ou inicia), então a run ainda é despachada; um enqueue perdido depois disso é recuperado
pelo reconciliador (`reconciler.intervalMs` + `graceMs`, como para qualquer trigger). Um engine só
de disparo nunca escuta: algum engine precisa estar iniciado para a run executar, como sempre.

## PgBouncer (transaction pooling)

PgBouncer em modo **transaction pooling** quebra o `LISTEN` (a sessão não é mantida): as runs
passam a ser despachadas só pela varredura de 2 s. Aponte a `postgres` do engine para um pool em
modo sessão ou direto para o Postgres se quiser despacho em milissegundos. O client `tx` que você
passa pode vir de onde quiser, desde que mantenha uma transação numa única conexão de servidor.

## Custo

Um `NOTIFY` extra por transação; o Postgres serializa os commits de transações que notificam (um
lock em nível de banco de dados tomado no commit), então, em taxas muito altas de transações
pequenas que disparam, agrupe vários `trigger()` por transação. Todo `NOTIFY` acorda **todo**
engine iniciado do schema; cada um roda uma varredura de reivindicação (rajadas são coalescidas, e
`SKIP LOCKED` garante que exatamente um deles despache cada run), então o custo por commit cresce
com o número de engines iniciados, não com o número de runs.

Um listener travado pode segurar todo commit que notifica: o Postgres mantém notificações não
entregues numa fila única do cluster (8 GB) até o backend mais lento que está escutando consumi-las;
uma sessão `LISTEN` que para de ler (um processo congelado com o socket ainda aberto) prende essa
fila, e uma vez cheia, `COMMIT`s que notificam falham. O listener do engine usa TCP keepalive e
reconecta em caso de erro, então um peer morto é descartado; para um processo travado mas vivo,
monitore `pg_notification_queue_usage()` e encerre o backend responsável (`pg_stat_activity` onde
`query LIKE 'LISTEN %mt_outbox_%'`).

## Idempotência dentro da transação

Com `idempotencyKey`, a reivindicação da chave e o insert da run são **um único statement** (sem
`SAVEPOINT`): uma chave que já pertence a uma run viva devolve essa run, não insere nada e sua
transação continua utilizável. Nenhuma subtransação é aberta por trigger, então centenas de
triggers com chave numa transação não têm problema (sem estourar o cache de subxid). A linha da
chave fica bloqueada até sua transação terminar, então um trigger concorrente com a mesma chave
espera por ela. Um client sem `BEGIN` também funciona (cada trigger então faz autocommit por conta
própria — você só perde a atomicidade com suas próprias escritas).

## Guarda de modo misto no caminho `tx`

A guarda de transporte misto (`schema_meta.transport`, decisão 28) é lida através deste mesmo
client `tx`, nunca do pool — assim ela não compete por uma conexão que sua aplicação esteja
segurando toda (ver [11. Produção](11-producao.md#dimensionamento-do-pool)): primeiro
`to_regclass` (seguro mesmo antes da migration 009 ter rodado), depois `schema_meta.transport` em
si, só se a role do chamador conseguir lê-la. Uma divergência lança `ConfigError` e deixa sua
transação utilizável — você decide se comita ou desfaz o resto dela. **Se a role de `tx` não tem
`SELECT` em `<schema>.schema_meta`, essa checagem é silenciosamente pulada** (tratada como se
nenhum engine tivesse gravado um transporte ainda: a escrita passa, e o marcador é relido depois de
30 s) em vez de abortar sua transação com um erro de permissão — conceda `SELECT` em
`<schema>.schema_meta` para a role que roda triggers `tx` se quiser a checagem ativa nesse caminho.
Ver [14. Modo Postgres](14-modo-postgres.md#guarda-de-modo-misto-e-mover-um-schema-entre-transportes)
para o resto da guarda.

## Limitações

- `delay` conta a partir do início da sua transação (o `now()` do Postgres).
- **Não combina com `debounce`** — travaria a run pendente compartilhada até sua transação
  terminar.
- **Não suportado por `createTestEngine`** — lança `ConfigError`.
- **Two-phase commit (2PC).** O Postgres recusa `PREPARE TRANSACTION` numa transação que já rodou
  `NOTIFY`/`pg_notify` (`ERROR: cannot PREPARE a transaction that has executed NOTIFY`). Como
  `trigger(..., { tx })` roda `pg_notify` no commit quando `outbox.notify` está no padrão (`true`),
  uma transação que também faz `PREPARE TRANSACTION` (2PC entre vários recursos) falha se disparou
  algo. Configure `outbox.notify: false` nessa engine se sua aplicação usa 2PC — o despacho passa a
  depender só da varredura de `outbox.pollIntervalMs`, que nunca roda `NOTIFY` na conexão da sua
  transação.
- Upgrading de 0.2.x: a migration `006_outbox` adiciona uma coluna e um índice parcial; veja
  [11. Produção](11-producao.md#upgrade-020-para-030).

## Configuração (`outbox.*`)

| Opção | Padrão | Notas |
|---|---|---|
| `outbox.pollIntervalMs` | `2 000` | Frequência da varredura de rede de segurança. Faixa 100–2³¹-1. |
| `outbox.batchSize` | `500` | Linhas por statement `claimOutbox`. Faixa 1–10 000. |
| `outbox.reconnectMaxMs` | `30 000` | Teto do backoff de reconexão do client `LISTEN`. Faixa 500–2³¹-1 (o backoff começa em 500 ms; um teto menor colapsaria num laço de reconexão quase contínuo). |
| `outbox.notify` | `true` | `false` pula o `pg_notify` no commit — vale a troca (latência de milissegundos por nunca tomar o lock de `NOTIFY` no commit) para transações pequenas em taxa muito alta, quando o custo de serialização de commit pesa mais que a latência de despacho. |

Ver [03. Configuração](03-configuracao.md#outbox-trigger-transacional) para o quadro completo.
