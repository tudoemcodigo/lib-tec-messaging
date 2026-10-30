<div align="center">

<img src="https://www.tudoemcodigo.com/marca/PNG/tudoemcodigo-icone-512.png" alt="TEC.Messaging" width="100" />

# 🐘 TEC.Messaging.PostgreSql

**Outbox e Inbox do TEC.Messaging no PostgreSQL com EF Core e Npgsql: eventos gravados na mesma transação do agregado e reserva de lotes com `FOR UPDATE SKIP LOCKED`.**

[📚 Documentação do pacote](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/postgresql.md) · [📚 TEC.Messaging](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/README.md) · [🐙 Repositório](https://github.com/tudoemcodigo/lib-tec-messaging)

</div>

## ✨ O que é

Implementa, sobre o `DbContext` da aplicação, as abstrações de armazenamento do núcleo:

| Peça | Papel |
|---|---|
| `TecMessagingPostgreSqlSaveChangesInterceptor` | No `SaveChanges`, converte os eventos de domínio dos agregados em linhas do Outbox, na mesma transação |
| `IOutboxStore` · `IOutboxAdministration` · `IOutbox` | Reserva de lotes, conclusão, limpeza, estatísticas, reprocessamento e descarte; gravação de evento sem agregado |
| `IInboxStore` | Registro `(MessageId, Consumer)` na mesma transação do efeito; `IsDuplicate` reconhece a violação de unicidade `23505` |
| `AddTecMessagingPostgreSql(...)` (no `ModelBuilder`) | Mapeia as tabelas `outbox_messages` e `inbox_messages` e os índices parciais |

A reserva é um único `UPDATE` sobre uma CTE com `FOR UPDATE SKIP LOCKED` e `RETURNING`: relays concorrentes pegam lotes diferentes sem contenção, e a validade da reserva usa o relógio do banco. Entrega *pelo menos uma vez*; a idempotência do consumidor é do Inbox.

## 🎯 Quando usar

Aplicações com **PostgreSQL** (12+) e EF Core que publicam eventos de integração. Para SQL Server use o `TEC.Messaging.SqlServer`. Precisa também de um transporte (`TEC.Messaging.RabbitMQ`, `.AzureServiceBus` ou `.Aws`) para o relay publicar.

## 📥 Instalação

```bash
dotnet nuget add source https://nuget.pkg.github.com/tudoemcodigo/index.json -n tec-interno -u <usuario> -p <PAT>
dotnet add package TEC.Messaging.PostgreSql --version 0.0.1
```

O pacote traz o `TEC.Messaging` junto. EF Core / Npgsql 10.x.

## 🚀 Início rápido

```csharp
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.PostgreSql.DependencyInjection;

builder.Services.AddTecMessaging()
    .AddEventType<PedidoAprovadoV1>()
    .Map<PedidoAprovado>(e => new PedidoAprovadoV1(e.PedidoId, e.Total))
    .UsePostgreSql<VendasDbContext>()
    .UseRabbitMq(o => o.Exchange = "vendas.eventos")       // ou outro transporte
    .AddOutboxRelay();

builder.Services.AddDbContext<VendasDbContext>((sp, o) => o.UseNpgsql(conexao).UseTecMessagingPostgreSql(sp));
```

No modelo (e depois gere a migration):

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddTecMessagingPostgreSql("vendas");
```

## 📚 Documentação

Esquema e índices, concorrência e locks, erros e diferenças para o SQL Server: [docs/postgresql.md](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/postgresql.md). O fluxo completo: [Outbox](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/outbox.md) e [Inbox](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/inbox.md).

`net10.0` · sem Native AOT (EF Core) · [Apache 2.0](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/LICENSE) · Roberto Oliveira, equipe Tudo em Código
