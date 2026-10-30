<div align="center">

<img src="https://www.tudoemcodigo.com/marca/PNG/tudoemcodigo-icone-512.png" alt="TEC.Messaging" width="100" />

# 🗄️ TEC.Messaging.SqlServer

**Outbox e Inbox do TEC.Messaging no SQL Server com EF Core: o evento é gravado na mesma transação do agregado e a reserva de lotes não disputa linhas entre processos.**

[📚 Documentação do pacote](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/sqlserver.md) · [📚 TEC.Messaging](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/README.md) · [🐙 Repositório](https://github.com/tudoemcodigo/lib-tec-messaging)

</div>

## ✨ O que é

Implementa, sobre o `DbContext` da aplicação, as abstrações de armazenamento do núcleo:

| Peça | Papel |
|---|---|
| `TecMessagingSaveChangesInterceptor` | No `SaveChanges`, converte os eventos de domínio dos agregados em linhas do Outbox, na mesma transação; `IDomainEventsSavingObserver` grava dados próprios junto |
| `IOutboxStore` · `IOutboxAdministration` · `IOutbox` | Reserva de lotes, conclusão, limpeza, estatísticas, reprocessamento (`RequeueAsync`) e descarte; gravação de evento sem agregado |
| `IInboxStore` | Registro `(MessageId, Consumer)` na mesma transação do efeito; `IsDuplicate` reconhece a corrida (erro 2627) |
| `AddTecMessaging(...)` (no `ModelBuilder`) | Mapeia as tabelas `OutboxMessages` e `InboxMessages` e os índices filtrados |

A reserva é um único `UPDATE` com `UPDLOCK, READPAST, ROWLOCK` e `OUTPUT`: relays concorrentes pegam lotes diferentes sem esperar uns pelos outros, e a validade da reserva usa o relógio do banco. Entrega *pelo menos uma vez*; a idempotência do consumidor é do Inbox.

## 🎯 Quando usar

Aplicações com **SQL Server / Azure SQL** (2016+) e EF Core que publicam eventos de integração. Para PostgreSQL use o `TEC.Messaging.PostgreSql`. Precisa também de um transporte (`TEC.Messaging.RabbitMQ`, `.AzureServiceBus` ou `.Aws`) para o relay publicar.

## 📥 Instalação

```bash
dotnet nuget add source https://nuget.pkg.github.com/tudoemcodigo/index.json -n tec-interno -u <usuario> -p <PAT>
dotnet add package TEC.Messaging.SqlServer --version 0.0.1
```

O pacote traz o `TEC.Messaging` junto. EF Core 10.x.

## 🚀 Início rápido

```csharp
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.SqlServer.DependencyInjection;

builder.Services.AddTecMessaging()
    .AddEventType<PedidoAprovadoV1>()
    .Map<PedidoAprovado>(e => new PedidoAprovadoV1(e.PedidoId, e.Total))
    .UseSqlServer<VendasDbContext>()
    .UseRabbitMq(o => o.Exchange = "vendas.eventos")       // ou outro transporte
    .AddOutboxRelay();

builder.Services.AddDbContext<VendasDbContext>((sp, o) => o.UseSqlServer(conexao).UseTecMessaging(sp));
```

No modelo (e depois gere a migration):

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.AddTecMessaging("vendas");
```

No caso de uso, só altere o agregado: o evento de domínio vira mensagem no mesmo `SaveChanges`.

## 📚 Documentação

Esquema e índices, concorrência e locks, limites, erros e o interceptor em detalhe: [docs/sqlserver.md](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/sqlserver.md). O fluxo completo: [Outbox](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/outbox.md) e [Inbox](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/inbox.md).

`net10.0` · sem Native AOT (EF Core) · [Apache 2.0](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/LICENSE) · Roberto Oliveira, equipe Tudo em Código
