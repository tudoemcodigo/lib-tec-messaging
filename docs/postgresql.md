[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › 🐘 PostgreSQL

# 🐘 PostgreSQL

> Persistência transacional do Outbox e do Inbox no PostgreSQL com EF Core e Npgsql: reserva de lotes com `FOR UPDATE SKIP LOCKED` e conclusão sem contenção entre processos.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [🗄️ Esquema e índices](#️-esquema-e-índices)
- [🔒 Concorrência e locks](#-concorrência-e-locks)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

O pacote `TEC.Messaging.PostgreSql` implementa as mesmas abstrações do [🗄️ SQL Server](sqlserver.md) (`IOutboxStore`, `IOutboxAdministration`, `IOutbox`, `IInboxStore`) sobre o `DbContext` da aplicação, com os recursos do PostgreSQL:

| Recurso do PostgreSQL | Utilização |
|---|---|
| `SELECT ... FOR UPDATE SKIP LOCKED` | Reserva de lotes sem contenção nem espera entre relays concorrentes |
| `UPDATE ... FROM batch ... RETURNING target.*` | Reserva e leitura do lote em uma única viagem ao servidor |
| `NOW()` | Validade da reserva pelo relógio do banco (`timestamptz`) |
| Índices parciais (`WHERE "Status" = 0`, `1`, `2`) | Reserva, limpeza, estatísticas e listagem de falhas sem varrer as publicadas retidas |
| `PostgresErrorCodes.UniqueViolation` (`23505`) | Detecção da corrida do Inbox pela chave primária |

| Peça | Implementa |
|---|---|
| `PostgreSqlOutboxStore<TContext>` | `IOutboxStore` · `IOutboxAdministration` |
| `PostgreSqlOutbox<TContext>` | `IOutbox` |
| `PostgreSqlInboxStore<TContext>` | `IInboxStore` |
| `TecMessagingPostgreSqlSaveChangesInterceptor` | Converte eventos de domínio em linhas do Outbox no `SaveChanges` |
| `IPostgreSqlDomainEventsSavingObserver` | Gancho para gravar dados próprios na mesma transação |

O pacote não é compatível com Native AOT (`IsAotCompatible=false`). Usa o EF Core e o Npgsql EF Core 10.x (alvo único `net10.0`).

---

## 🚀 Uso

### 1. Registro dos serviços

```csharp
services.AddTecMessaging()
    .AddEventType<PedidoAprovadoV1>()
    .Map<PedidoAprovado>(e => new PedidoAprovadoV1(e.PedidoId, e.Total))
    .UsePostgreSql<VendasDbContext>()
    .AddOutboxRelay()
    .AddInboxCleanup();                                   // se a aplicação consome mensagens

services.AddDbContext<VendasDbContext>((sp, o) =>
    o.UseNpgsql(conexao).UseTecMessagingPostgreSql(sp));
```

`UsePostgreSql<TContext>()` registra, com `TryAdd`: `IOutboxStore`, `IOutboxAdministration`, `IOutbox` e `IInboxStore` como **scoped** e o `TecMessagingPostgreSqlSaveChangesInterceptor` como singleton. Diferente do SQL Server, o nome da extensão que liga o interceptor ao contexto é `UseTecMessagingPostgreSql(sp)`.

### 2. Modelo e migration

```csharp
public sealed class VendasDbContext(DbContextOptions<VendasDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.AddTecMessagingPostgreSql("vendas");     // schema opcional; tabelas outbox_messages e inbox_messages
    }
}
```

Gere a migration depois (`dotnet ef migrations add Mensageria`). Para nomes próprios: `AddTecMessagingPostgreSql(schema, outboxTable, inboxTable)`.

### 3. Limpeza do Inbox, evento sem agregado e Inbox

```csharp
services.AddTecMessaging().UsePostgreSql<FinanceiroDbContext>().AddInboxCleanup(o => o.Retention = TimeSpan.FromDays(30));

outbox.Enqueue(new ParametrosAlteradosV1(chave));       // IOutbox: gravado no próximo SaveChanges

if (!await inbox.TryBeginAsync(message.Envelope.MessageId, "faturamento", ct))
    return ConsumeResult.Completed;
db.Faturas.Add(fatura);
try { await db.SaveChangesAsync(ct); }
catch (DbUpdateException ex) when (inbox.IsDuplicate(ex)) { return ConsumeResult.Completed; }
```

Os fluxos estão em [📤 Outbox](outbox.md) e [📥 Inbox](inbox.md).

> [!WARNING]
> O pacote do PostgreSQL não tem a extensão `AddDomainEventsObserver` do SQL Server: para usar um `IPostgreSqlDomainEventsSavingObserver`, registre-o você mesmo (`services.AddSingleton<IPostgreSqlDomainEventsSavingObserver, MeuObservador>()`); o interceptor recebe todos os registrados. A suíte cobre o mapeamento do modelo (`PostgreSqlUnitTests`) e a resolução do interceptor pelo container (`MessagingIssueRegressionTests`), mas não há integração com um PostgreSQL real: a reserva com `SKIP LOCKED` nunca rodou contra um servidor nos testes. Valide-a na sua aplicação antes de produção (ver [🔍 Revisão técnica](revisao-tecnica.md#-complemento-da-revisão-documental--2026-10-24)).

> [!NOTE]
> O `DateTimeOffset` gravado em colunas `timestamptz` precisa ter deslocamento zero (UTC): o Npgsql recusa outros deslocamentos. O pacote converte para UTC o `OccurredAt` do evento (na fábrica de envelopes e de novo ao montar a linha) e o `NextAttemptAt`, então um `IDomainEvent.OccurredAt` com fuso (ex.: `-03:00`) é gravado como o mesmo instante em UTC. Colunas `DateTimeOffset` das suas próprias entidades continuam sujeitas à regra do Npgsql.

---

## 🗄️ Esquema e índices

`AddTecMessagingPostgreSql` mapeia `OutboxMessage` e `InboxMessage` (`TEC.Messaging.PostgreSql.Entities`). Os nomes de coluna são os das propriedades (`"Id"`, `"OccurredAt"`...), a menos que o modelo aplique uma convenção de nomes.

**`outbox_messages`**:

| Coluna | Tipo | Observação |
|---|---|---|
| `Id` | `uuid` | Chave primária (o `MessageId`, UUIDv7) |
| `Type` | `varchar(200)` | Contrato |
| `Version` | `integer` | Versão do contrato |
| `OccurredAt` | `timestamptz` | Quando o fato ocorreu |
| `CorrelationId` · `CausationId` · `TraceParent` | `varchar(128)` | Anuláveis |
| `Payload` | `text` | JSON do evento |
| `Status` | `smallint` | `0` Pending · `1` Published · `2` Dead |
| `Attempts` | `integer` | Falhas de publicação |
| `NextAttemptAt` | `timestamptz` | Quando pode ser publicada |
| `LeaseToken` · `LeasedUntil` | `uuid?` · `timestamptz?` | Reserva vigente |
| `PublishedAt` | `timestamptz?` | Quando foi publicada |
| `LastError` | `varchar(1000)` | Último erro, já sanitizado |

**`inbox_messages`**: chave composta `(MessageId, Consumer)`, `Consumer` `varchar(100)`, `ProcessedAt`.

| Índice (nomes em minúsculas, a partir do nome da tabela) | Chave | Filtro | Para quê |
|---|---|---|---|
| `ix_outbox_messages_pending` | `OccurredAt, Id` (inclui `NextAttemptAt`, `LeasedUntil`, `Attempts`) | `"Status" = 0` | Reserva na ordem de ocorrência |
| `ix_outbox_messages_published` | `PublishedAt` | `"Status" = 1` | Limpeza das publicadas vencidas |
| `ix_outbox_messages_dead` | `OccurredAt` | `"Status" = 2` | Contagem e listagem das mortas |
| `ix_inbox_messages_processed_at` | `ProcessedAt` | — | Limpeza do Inbox |

---

## 🔒 Concorrência e locks

A reserva é um único `UPDATE` sobre uma CTE com `SKIP LOCKED`:

```sql
WITH batch AS (
    SELECT "Id" FROM "vendas"."outbox_messages"
    WHERE "Status" = 0 AND "NextAttemptAt" <= @now
        AND ("LeasedUntil" IS NULL OR "LeasedUntil" < NOW())
    ORDER BY "OccurredAt", "Id"
    LIMIT @batchSize
    FOR UPDATE SKIP LOCKED)
UPDATE "vendas"."outbox_messages" AS target
SET "LeaseToken" = @token,
    "LeasedUntil" = NOW() + (@leaseSeconds * interval '1 second')
FROM batch
WHERE target."Id" = batch."Id"
RETURNING target.*;
```

| Garantia | Como |
|---|---|
| Dois relays não reservam a mesma mensagem | `FOR UPDATE SKIP LOCKED`: o PostgreSQL pula as linhas travadas por outro relay e entrega as seguintes, sem enfileirar transações nem gerar deadlocks |
| A reserva sobrevive à transação | `LeaseToken` e `LeasedUntil` ficam gravados; a publicação acontece **fora** de transação |
| Conclusão só por quem reservou | `CompleteAsync` atualiza com `LeaseToken = @token`; reserva assumida por outro relay torna a conclusão inócua |
| Administração não desfaz reserva ativa | `RequeueAsync` só devolve mortas e pendentes com erro sem reserva vigente, comparando `LeasedUntil` com `now()` do banco, o mesmo relógio da reserva |

A conclusão, as estatísticas, a listagem de falhas, o reprocessamento e o descarte têm a mesma semântica do [SQL Server](sqlserver.md#-concorrência-e-locks) (mesmos limites: 1 a 1000 ids, `batchSize` 1 a 1000). A limpeza do Outbox apaga em lotes com `DELETE ... WHERE "Id" IN (SELECT ... LIMIT @max)`. A do Inbox apaga pela chave inteira, `WHERE ("MessageId", "Consumer") IN (SELECT "MessageId", "Consumer" ... LIMIT @max)`: o registro vencido de um consumidor não leva junto o de outro consumidor para a mesma mensagem.

---

## ⚙️ Opções

O pacote não tem classe de opções própria: as do relay estão em [📤 Outbox](outbox.md#️-opções) e as da limpeza do Inbox em [📥 Inbox](inbox.md#️-opções). O que se configura é o mapeamento:

| Parâmetro de `AddTecMessagingPostgreSql` | Padrão | Descrição |
|---|---|---|
| `schema` | `null` | Esquema das duas tabelas (`null` = o padrão do modelo, normalmente `public`) |
| `outboxTable` | `outbox_messages` | Tabela do Outbox (compõe o nome dos índices, em minúsculas) |
| `inboxTable` | `inbox_messages` | Tabela do Inbox |

---

## ❌ Erros

| Código / exceção | Causa | Tratamento |
|---|---|---|
| `23505` (`unique_violation`) na constraint do Inbox | Mensagem já processada por outra entrega concorrente | `IInboxStore.IsDuplicate(ex)` devolve `true` só para a chave primária do Inbox; encerre como `Completed`. Sem o Inbox no modelo, nenhuma violação conta como duplicata (mesma regra do SQL Server) |
| `InvalidOperationException: OutboxMessage não está no modelo de ...` | Faltou `modelBuilder.AddTecMessagingPostgreSql()` | Chame no `OnModelCreating` e gere a migration |
| `42P01` (`relation does not exist`) | Tabelas da mensageria não criadas | Gere e aplique a migration com `AddTecMessagingPostgreSql()` |
| `ArgumentOutOfRangeException` em `RequeueAsync`/`DiscardAsync` | Coleção vazia ou com mais de 1000 ids | Envie de 1 a 1000 ids (`null` em `RequeueAsync` = todas) |
| `ArgumentOutOfRangeException` em `TryBeginAsync` | Nome do consumidor com mais de 100 caracteres | Use um nome curto e estável |

---

## 🛡️ Segurança

> [!WARNING]
> O relay publica o que está na tabela: quem escreve em `outbox_messages` publica no broker. Conceda escrita só ao usuário da aplicação.

- Nomes de tabela e coluna vêm do modelo e são delimitados (`"` escapado); valores sempre por parâmetro (`NpgsqlParameter`).
- `LastError` é sanitizado e truncado em 1000 caracteres antes de ser gravado ([🛡️ Segurança](seguranca.md)).

## ❓ Perguntas frequentes

<details>
<summary><b>Por que o Inbox precisa estar no mesmo DbContext do efeito?</b></summary>

Porque o registro do Inbox entra na unidade de trabalho do contexto e é gravado no mesmo `SaveChanges` do efeito: ou os dois acontecem, ou nenhum.
</details>

<details>
<summary><b>Qual a diferença para o SQL Server?</b></summary>

Mesmo contrato e mesma semântica. Mudam o mecanismo de reserva (`SKIP LOCKED` e `RETURNING` no lugar de `UPDLOCK, READPAST` e `OUTPUT`), os nomes padrão (`outbox_messages`) e o nome das extensões (`UsePostgreSql`, `AddTecMessagingPostgreSql`, `UseTecMessagingPostgreSql`). Veja [🗄️ SQL Server](sqlserver.md).
</details>

---
⬅️ [🗄️ SQL Server](sqlserver.md) · [📚 Índice](README.md) · [🐇 RabbitMQ](rabbitmq.md) ➡️
