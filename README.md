<div align="center">

<img src="https://www.tudoemcodigo.com/marca/PNG/tudoemcodigo-icone-512.png" alt="TEC" width="96" />

# 📮 TEC.Messaging

**Mensageria corporativa de alto desempenho: Outbox transacional, Inbox idempotente, sanitização de PII (LGPD), contratos versionados e suporte agnóstico a RabbitMQ, Azure Service Bus e AWS SQS/SNS.**

[![CI](https://github.com/tudoemcodigo/lib-tec-messaging/actions/workflows/ci.yml/badge.svg)](https://github.com/tudoemcodigo/lib-tec-messaging/actions/workflows/ci.yml)
![.NET](https://img.shields.io/badge/.NET-8%20%7C%2010-512BD4)
![AOT](https://img.shields.io/badge/Native%20AOT-n%C3%BAcleo%20%26%20RabbitMQ-success)
![Versão](https://img.shields.io/badge/vers%C3%A3o-0.0.1-blue)
![Licença](https://img.shields.io/badge/licen%C3%A7a-MIT-green)

[Início rápido](#-início-rápido) · [Documentação](docs/README.md) · [Changelog](CHANGELOG.md)

</div>

---

## 📑 Sumário

- [✨ Por que usar](#-por-que-usar)
- [📦 Pacotes](#-pacotes)
- [🧬 Ecossistema TEC](#-ecossistema-tec)
- [📥 Instalação](#-instalação)
- [🚀 Início rápido](#-início-rápido)
- [📚 Documentação](#-documentação)
- [⚡ Compatibilidade](#-compatibilidade)
- [🛡️ Segurança · 🧪 Testes](#️-segurança--testes)
- [🤝 Contribuição · 🏷️ Versionamento · 📄 Licença](#-contribuição--️-versionamento--licença)

---

## ✨ Por que usar

| Sem o TEC.Messaging | Com o TEC.Messaging |
|---|---|
| Publica no broker dentro do caso de uso: commit falha, evento já saiu (ou o contrário) | **Outbox transacional**: o evento é gravado na mesma transação do agregado e publicado depois |
| Um relay trava a fila inteira numa mensagem com defeito | **Falha isolada**: backoff exponencial por mensagem e status `Dead` depois de N tentativas |
| Publicação dentro de transação longa, com travas no banco | **Reserva (lease)** em transação curta e publicação **fora** da transação |
| Evento de domínio serializado direto para outros serviços | **Contratos de integração** com nome e versão estáveis (`[IntegrationEvent]`) e mapeadores |
| Reentrega processa duas vezes | **Inbox**: registro da mensagem processada na mesma transação do efeito |
| Retry com `Thread.Sleep` e mensagens perdidas | **RabbitMQ** com publisher confirms, filas de espera, DLQ, pausa e administração da DLQ |
| Trace e correlação quebrados entre serviços | **Correlação, causa e `traceparent`** propagados; métricas e spans `TEC.Messaging` |

---

## 📦 Pacotes

| Pacote | Para que serve | Quando instalar | Depende de |
|---|---|---|---|
| `TEC.Messaging` | Envelope, contratos, mapeadores, Outbox/Inbox (abstrações), sanitizador LGPD de PII, relay, correlação, métricas | Sempre | `TEC.Core` |
| `TEC.Messaging.SqlServer` | Outbox e Inbox no SQL Server com EF Core, interceptor de eventos de domínio, administração | Persistência em SQL Server com EF Core | `TEC.Messaging`, EF Core |
| `TEC.Messaging.PostgreSql` | Outbox e Inbox no PostgreSQL com EF Core (`FOR UPDATE SKIP LOCKED`, `RETURNING`), interceptor | Persistência em PostgreSQL com EF Core | `TEC.Messaging`, EF Core, Npgsql |
| `TEC.Messaging.RabbitMQ` | Publicador com confirms, consumidores com concorrência paralela, retry escalonado, DLQ, pausa, health check | Broker RabbitMQ 3.13+/4.x | `TEC.Messaging`, `TEC.Vault`, RabbitMQ.Client 7 |
| `TEC.Messaging.AzureServiceBus` | Publicador e consumidor para filas e tópicos do Azure Service Bus, confirms, DLQ, concorrência, health check | Nuvem Microsoft Azure | `TEC.Messaging`, `TEC.Vault`, Azure.Messaging.ServiceBus |
| `TEC.Messaging.Aws` | Publicador e consumidor para filas Amazon SQS e tópicos SNS, long polling, concorrência, DLQ, health check | Nuvem Amazon Web Services (AWS) | `TEC.Messaging`, `TEC.Vault`, AWSSDK (SQS/SNS) |

Todos os pacotes são versionados e distribuídos em conjunto com rigorosa compatibilidade.

---

## 🧬 Ecossistema TEC

```mermaid
flowchart BT
    Core["🧰 TEC.Core<br/><sub>Result, erros, domínio</sub>"]
    Vault["🔐 TEC.Vault<br/><sub>segredos, chaves, certificados</sub>"]
    Cqrs["🧭 TEC.Cqrs<br/><sub>commands, queries, pipeline</sub>"]
    Sec["🛡️ TEC.Security<br/><sub>autenticação, autorização</sub>"]
    Obs["📡 TEC.Observability<br/><sub>traces, métricas, logs</sub>"]
    Orm["🗄️ TEC.ORM<br/><sub>persistência</sub>"]
    Msg["📮 TEC.Messaging<br/><sub>Outbox, Inbox, brokers</sub>"]
    Vault --> Core
    Cqrs --> Core
    Sec --> Core
    Sec --> Vault
    Orm --> Core
    Orm --> Cqrs
    Orm --> Vault
    Msg --> Core
    Msg --> Vault
    style Msg fill:#1f6feb,color:#fff
```

O TEC.Messaging **não depende** do TEC.Cqrs nem do TEC.ORM: funciona de forma autônoma com qualquer `DbContext`. Traces, logs e métricas `TEC.Messaging` são assinados automaticamente pelo TEC.Observability e exportáveis para qualquer coletor OpenTelemetry (Grafana, Datadog, Zabbix, Azure Monitor, etc.).

---

## 📥 Instalação

Feed do GitHub Packages (PAT com `read:packages`):

```bash
dotnet nuget add source https://nuget.pkg.github.com/tudoemcodigo/index.json -n tec-interno -u <usuario-github> -p <PAT>

# Núcleo e persistência
dotnet add package TEC.Messaging
dotnet add package TEC.Messaging.SqlServer    # ou TEC.Messaging.PostgreSql

# Broker desejado
dotnet add package TEC.Messaging.RabbitMQ     # ou TEC.Messaging.AzureServiceBus ou TEC.Messaging.Aws
```

---

## 🚀 Início rápido

```csharp
// 1. Contrato de integração (público, versionado) e o evento de domínio (interno)
[IntegrationEvent("vendas.pedido-aprovado", Version = 1)]
public sealed record PedidoAprovadoV1(Guid PedidoId, decimal Total);

public sealed record PedidoAprovado(Guid PedidoId, decimal Total, DateTimeOffset OccurredAt) : IDomainEvent;

// 2. Registro
builder.Services.AddTecMessaging()
    .AddEventType<PedidoAprovadoV1>()
    .Map<PedidoAprovado>(e => new PedidoAprovadoV1(e.PedidoId, e.Total))
    .UseSqlServer<VendasDbContext>()                                  // Outbox/Inbox no banco da aplicação
    .UseRabbitMq(o => o.Exchange = "vendas.eventos")                  // URI no segredo "rabbitmq" do TEC.Vault
    .AddOutboxRelay();

builder.Services.AddDbContext<VendasDbContext>((sp, o) => o.UseSqlServer(conexao).UseTecMessaging(sp));

// 3. No modelo: modelBuilder.AddTecMessaging("vendas");  (e gere a migration)

// 4. No caso de uso: só altere o agregado. O evento vira mensagem no mesmo SaveChanges.
pedido.Aprovar(agora);              // RaiseDomainEvent(new PedidoAprovado(...))
await db.SaveChangesAsync(ct);
```

Consumidor idempotente:

```csharp
public sealed class Faturamento(RabbitMqConsumerDependencies deps, IServiceScopeFactory scopes) : RabbitMqConsumer(deps)
{
    protected override QueueDefinition Queue { get; } = new("financeiro.faturamento", ["pedido-aprovado"]);

    protected override async Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        if (!await scope.ServiceProvider.GetRequiredService<IInboxStore>().TryBeginAsync(message.Envelope.MessageId, "faturamento", ct))
            return ConsumeResult.Completed;                      // reentrega: já processada
        // ... efeito ...
        await scope.ServiceProvider.GetRequiredService<FinanceiroDbContext>().SaveChangesAsync(ct);
        return ConsumeResult.Completed;
    }
}
```

---

## 📚 Documentação

| Arquivo | O que responde |
|---|---|
| [📜 Contratos de integração](docs/contratos-de-integracao.md) | Como nomear e versionar eventos, mapear eventos de domínio e registrar com AOT |
| [📤 Outbox](docs/outbox.md) | Como o evento entra no Outbox, como o relay publica, falhas, `Dead` e administração |
| [📥 Inbox](docs/inbox.md) | Como tornar o consumidor idempotente |
| [🐘 PostgreSQL](docs/postgresql.md) | Persistência com PostgreSQL (SKIP LOCKED, RETURNING) |
| [🐇 RabbitMQ](docs/rabbitmq.md) | Topologia, retry, DLQ, concorrência paralela, pausa e health check |
| [☁️ Azure Service Bus](docs/azureservicebus.md) | Publicador e consumidor em tópicos e filas no Azure Service Bus |
| [🟧 AWS SQS / SNS](docs/aws.md) | Publicador e consumidor com Amazon SQS e SNS |
| [📡 Telemetria](docs/telemetria.md) | Spans, métricas e correlação |
| [🛡️ Segurança e LGPD](docs/seguranca.md) | Sanitização de PII, segredos no cofre e limites de payload |
| [🧪 Testes](docs/testes.md) | Categorias, testes unitários e de integração |
| [🛠️ Desenvolvimento](docs/desenvolvimento.md) | Build, modo local e publicação |

---

## ⚡ Compatibilidade

| Item | Suporte |
|---|---|
| .NET | 8 (LTS) e 10 (LTS) |
| Native AOT | `TEC.Messaging` e `TEC.Messaging.RabbitMQ` (contratos com `JsonTypeInfo`); provedores EF Core e SDKs de nuvem marcam `<IsAotCompatible>false</IsAotCompatible>` |
| Bancos de Dados | SQL Server 2016+ / Azure SQL (`OUTPUT`, `READPAST`) e PostgreSQL 12+ (`SKIP LOCKED`, `RETURNING`) |
| Brokers | RabbitMQ 3.13+ e 4.x (filas quorum), Azure Service Bus (Standard/Premium), AWS SQS/SNS |

---

## 🛡️ Segurança · 🧪 Testes

Higienização ativa de PII em logs e erros (LGPD), credenciais de brokers e bancos resolvidas estritamente via `TEC.Vault`, limite de payload configurável (4 MiB) e correlação sanitizada. Unitários sem dependências; integração com SQL Server, PostgreSQL, RabbitMQ e mocks de nuvem. Detalhes em [docs/seguranca.md](docs/seguranca.md) e [docs/testes.md](docs/testes.md).

## 🤝 Contribuição · 🏷️ Versionamento · 📄 Licença

PR para a `main` com o `ci-ok` verde. [SemVer](https://semver.org/lang/pt-BR/) (em `0.x`, mudanças incompatíveis podem
ocorrer em versões MINOR); prévias `X.Y.Z-preview.N` a cada merge. Licença [MIT](LICENSE).
