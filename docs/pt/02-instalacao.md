# 02. Instalação

## Dependências peer

`bullmq`, `ioredis` e `pg` são **peer dependencies** — nunca vêm empacotadas junto com
`@rivia-labs/minitrigger` — então instale-as junto:

```bash
pnpm add @rivia-labs/minitrigger bullmq ioredis pg
pnpm add -D @types/pg   # opcional, só necessário se você usa TypeScript
```

(`npm install`/`yarn add` funcionam da mesma forma — não há nada específico de pnpm no pacote
publicado.)

| Pacote | Faixa de versão | Por quê |
|---|---|---|
| `bullmq` | `^6.3.0` | — |
| `ioredis` | `^5.11.1` | **Não use 6.x**: o BullMQ 6.3.x é testado contra o `ioredis` 5.11.1, e o adapter dele não trata o RESP3, que é o padrão do `ioredis` 6.x. |
| `pg` | `^8.13.0` | — |
| `@types/pg` | `^8.11.0` | Peer opcional — só necessário porque os tipos públicos referenciam `pg.Pool`. |

Requisitos adicionais: **Node.js >= 22** e **Postgres >= 13** (checados por `checkEnvironment()` /
`engine.start()`), além de **Redis com `maxmemory-policy noeviction`** (próxima seção).

## Infraestrutura mínima

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: minitrigger
      POSTGRES_PASSWORD: minitrigger
      POSTGRES_DB: minitrigger
    ports:
      - "5432:5432"
  redis:
    image: redis:7-alpine
    # O BullMQ exige noeviction: ele nunca pode perder chaves das quais depende
    # para jobs atrasados/ativos, ou jobs somem silenciosamente.
    command: ["redis-server", "--maxmemory-policy", "noeviction"]
    ports:
      - "6379:6379"
```

`noeviction` importa porque o BullMQ guarda estado de job (incluindo jobs atrasados — ou seja, tudo
que `step.sleep`, backoff de retentativa e runs agendadas dependem) como chaves comuns do Redis.
Qualquer outra política de eviction deixa o Redis descartar silenciosamente esse estado sob pressão
de memória; o `minitrigger` checa isso em `engine.start()` e avisa (ou lança erro, se conseguir
confirmar que a política está errada — decisão 22 do `docs/SPEC.md`) quando não está configurado
corretamente.

## Formas de instalar

### Recomendada: registry npm

```bash
pnpm add @rivia-labs/minitrigger bullmq ioredis pg
pnpm add -D @types/pg
```

### Alternativa: `.tgz` de GitHub Releases

Útil para fixar um artefato exato sem passar pelo registry (por exemplo, vendorizar, ou instalar de
um mirror privado). Se o repositório for privado para você, os assets de release exigem
autenticação do GitHub — baixe o tarball com o GitHub CLI (logado com acesso ao repo) e adicione a
partir do disco:

```bash
gh release download v0.2.0 -R igorabreu29/minitrigger -p "*.tgz"
pnpm add ./minitrigger-0.2.0.tgz bullmq ioredis pg
pnpm add -D @types/pg
```

(o tarball da `v0.2.0` se chama `minitrigger-0.2.0.tgz` — sem escopo, ver nota abaixo; a partir da
`v0.3.0` é `rivia-labs-minitrigger-<versão>.tgz`, e o comando de instalação usa
`@rivia-labs/minitrigger` como na seção "Recomendada: registry npm" acima.)

O tarball já contém o `dist/` buildado — nada é compilado na instalação: sem precisar do
`allowBuilds` do pnpm, e sem puxar transitivamente `ioredis`/`msgpackr-extract` só para rodar um
script de build. É também fixo e reproduzível: um artefato específico por versão, não "o que quer
que o `HEAD` contenha agora". O `files` do `package.json` também inclui o plugin de Claude Code
deste pacote (`.claude-plugin`, `skills`, `docs/pt`) — nada disso é importado pela biblioteca em
si, é só conteúdo extra para quem instala dentro de um projeto Claude Code.

> Releases até `v0.2.0` (inclusive) foram publicadas sob o nome sem escopo `minitrigger` (o
> tarball e seu `package.json` dizem `minitrigger`, não `@rivia-labs/minitrigger`) — isso só
> importa se você fixar em uma dessas tags antigas; a partir de `v0.3.0`, tudo (npm, tarball de
> release, fallback de instalação via GitHub) usa `@rivia-labs/minitrigger`.

### Fallback: instalar direto do GitHub

Exige acesso git ao repositório (chave SSH ou um credential helper para HTTPS) — só relevante
enquanto/se o repositório for privado; um repositório público não precisa de autenticação para
isso.

```bash
pnpm add github:igorabreu29/minitrigger#v0.2.0 bullmq ioredis pg
```

O `dist/` está no `.gitignore` e só é produzido pelo script `prepare` do próprio minitrigger, e o
pnpm 10+ bloqueia scripts de lifecycle de uma dependência git por padrão — então um simples `pnpm
add github:...` falha com `ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED` até você habilitar isso, no seu
**próprio** `pnpm-workspace.yaml`:

```yaml
# pnpm-workspace.yaml do seu projeto
allowBuilds:
  minitrigger@github.com/igorabreu29/minitrigger#<commit-resolvido>: true # tags até v0.2.0
  # "@rivia-labs/minitrigger@github.com/igorabreu29/minitrigger#<commit-resolvido>": true  # v0.3.0+
  msgpackr-extract: true # acelerador nativo opcional do ioredis; bloqueado pelo mesmo mecanismo
```

A chave `<nome-do-pacote>@github.com/...` precisa bater exatamente com o spec git resolvido que o
próprio pnpm imprime no erro `ERR_PNPM_GIT_DEP_PREPARE_NOT_ALLOWED` (nome do pacote + URL git
normalizada + commit resolvido) — copie do erro em vez de adivinhar (o nome do pacote nessa chave é
o que o `package.json` diz **na tag/commit que você está instalando**, conforme a nota acima), e
espere ter que atualizar essa chave sempre que atualizar para um commit/tag mais novo. Reprodução
completa e raciocínio: `docs/research/github-install.md`.

## Próximo passo

Com a biblioteca instalada e a infraestrutura no ar, veja [03. Configuração](03-configuracao.md)
para montar seu `createEngine(config)`.
