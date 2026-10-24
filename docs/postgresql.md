[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › PostgreSQL

# 🐘 PostgreSQL

> Persistência transacional do Outbox e Inbox no PostgreSQL com EF Core e Npgsql.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [⚙️ Configuração](#️-configuração)
- [🔒 Concorrência e Locks](#-concorrência-e-locks)
- [❌ Erros](#-erros)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

O pacote `TEC.Messaging.PostgreSql` implementa os componentes `IOutboxStore` e `IInboxStore` aproveitando recursos nativos de alto desempenho do PostgreSQL:

| Recurso | Utilização |
|---|---|
| `SELECT ... FOR UPDATE SKIP LOCKED` | Reserva de lotes sem contenção nem bloqueio entre múltiplos relays concorrentes |
| `RETURNING target.*` | Conclusão atômica de lotes de mensagens em uma única viagem ao servidor |
| Índices parciais (filtrados) | `IX_outbox_messages_pending` e `IX_outbox_messages_dead` com filtros em `status = 0` e `status = 2` |
| `PostgresErrorCodes.UniqueViolation` (`23505`) | Detecção determinística de corridas no Inbox na constraint da chave primária |

---

## 🚀 Uso

### 1. Registro dos Serviços

```csharp
services.AddTecMessaging()
    .AddEventType<PedidoAprovadoV1>()
    .Map<PedidoAprovado>(e => new PedidoAprovadoV1(e.PedidoId, e.Total))
    .UsePostgreSql<VendasDbContext>()
    .AddOutboxRelay();

services.AddDbContext<VendasDbContext>((sp, o) => 
    o.UseNpgsql(conexao).UseTecMessaging(sp));
```

### 2. Configuração no DbContext

```csharp
public sealed class VendasDbContext(DbContextOptions<VendasDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.AddTecMessagingPostgreSql("vendas"); // schema opcional
    }
}
```

---

## 🔒 Concorrência e Locks

A reserva de mensagens do Outbox utiliza a cláusula moderna de controle de concorrência pessimista:

```sql
SELECT "Id", "OccurredAt", "Type", "Version", "Payload", "Attempts"
FROM "vendas"."outbox_messages"
WHERE "Status" = 0 AND ("NextAttemptAt" IS NULL OR "NextAttemptAt" <= clock_timestamp())
ORDER BY "OccurredAt", "Id"
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

Com o `SKIP LOCKED`, caso outro relay esteja processando um lote de mensagens, o PostgreSQL simplesmente pula as linhas bloqueadas e entrega as próximas disponíveis sem enfileirar transações nem causar deadlocks.

---

## ⚙️ Configuração

O registro via `UsePostgreSql<TContext>()` adiciona os stores e o interceptor `TecMessagingPostgreSqlSaveChangesInterceptor`.
Para limpeza de Inbox:

```csharp
services.AddInboxCleanup(o => o.Retention = TimeSpan.FromDays(30));
```

---

## ❌ Erros

| Código / Exceção | Causa | Tratamento |
|---|---|---|
| `23505` (UniqueViolation) | Mensagem já processada por outro consumidor concorrente | Reconhecida como duplicata por `IInboxStore.IsDuplicate(ex)`, encerrando como `Completed` |
| `Relation does not exist` | Tabelas de mensageria não criadas | Executar as migrations do EF Core contendo `AddTecMessagingPostgreSql()` |

---

⬅️ [Outbox](outbox.md) · [📚 Índice](README.md) · [RabbitMQ](rabbitmq.md) ➡️
