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

`tx` é um `PoolClient`/`Client` do node-postgres no qual você já rodou `BEGIN` (ou qualquer objeto
com o mesmo formato `query(text, values)` vinculado a uma transação aberta — a costura para
adaptadores de ORM). **Passar o `Pool` em si é rejeitado**: as queries dele rodariam fora da sua
transação.

O engine só roda statements parametrizados e qualificados por schema no seu client — nunca `SET`,
`LISTEN` ou outro estado de sessão.

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

## Limitações

- `delay` conta a partir do início da sua transação (o `now()` do Postgres).
- **Não combina com `debounce`** — travaria a run pendente compartilhada até sua transação
  terminar.
- **Não suportado por `createTestEngine`** — lança `ConfigError`.
- Upgrading de 0.2.x: a migration `006_outbox` adiciona uma coluna e um índice parcial; veja
  [11. Produção](11-producao.md#upgrade-020-para-030).

## Configuração (`outbox.*`)

| Opção | Padrão | Notas |
|---|---|---|
| `outbox.pollIntervalMs` | `2 000` | Frequência da varredura de rede de segurança. |
| `outbox.batchSize` | `500` | Linhas por statement `claimOutbox`. |
| `outbox.reconnectMaxMs` | `30 000` | Teto do backoff de reconexão do client `LISTEN`. |
| `outbox.notify` | `true` | `false` pula o `pg_notify` no commit — vale a troca (latência de milissegundos por nunca tomar o lock de `NOTIFY` no commit) para transações pequenas em taxa muito alta, quando o custo de serialização de commit pesa mais que a latência de despacho. |

Ver [03. Configuração](03-configuracao.md#outbox-trigger-transacional) para o quadro completo.
