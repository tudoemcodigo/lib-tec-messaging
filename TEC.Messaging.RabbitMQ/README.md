<div align="center">

<img src="https://www.tudoemcodigo.com/marca/PNG/tudoemcodigo-icone-512.png" alt="TEC.Messaging" width="100" />

# 🐇 TEC.Messaging.RabbitMQ

**Transporte RabbitMQ do TEC.Messaging: publicação com confirmação do broker, consumidores com retry escalonado, DLQ, pausa e concorrência, com a URI no TEC.Vault.**

[📚 Documentação do pacote](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/rabbitmq.md) · [📚 TEC.Messaging](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/README.md) · [🐙 Repositório](https://github.com/tudoemcodigo/lib-tec-messaging)

</div>

## ✨ O que é

| Peça | Papel |
|---|---|
| `IMessagePublisher` | Publica com *publisher confirms*, `mandatory` e mensagens persistentes; transporte fora do ar sai como `ITransportUnavailableException` (o Outbox não conta tentativa); mensagem sem fila ligada sai como `RabbitMqUnroutableException` (conta tentativa) |
| `RabbitMqConsumer` | Base dos consumidores: filas **quorum**, ack só depois do handler, retry por filas de espera com TTL (`fila.retry.<atraso>`), DLQ (`fila.dlq`), `Pause()`/`Resume()`, concorrência (`ConcurrencyLimit`, 1 a 200) e desligamento com drenagem (`DrainTimeout`) |
| `IDeadLetterAdministration` | Contar, listar, reprocessar e descartar mensagens da DLQ, pelo protocolo AMQP |
| Conexão | Única por processo, URI AMQP lida do segredo `rabbitmq` do TEC.Vault, com circuit breaker (`Polly.Core`) e reconexão com jitter |
| `AddTecRabbitMq()` | Health check da conexão |

| `ConsumeResult` | Efeito |
|---|---|
| `Completed` | ack |
| `Retry(motivo)` | Fila de espera do próximo atraso; na `MaxAttempts`-ésima falha, DLQ |
| `Reject(motivo)` | DLQ |
| `Requeue(motivo)` | Volta depois da primeira espera, sem consumir tentativa |

## 🎯 Quando usar

Serviços que usam **RabbitMQ** (3.13+ ou 4.x) como broker de eventos de integração. Combine com um armazenamento (`TEC.Messaging.SqlServer` ou `.PostgreSql`) para o Outbox e o Inbox.

## 📥 Instalação

```bash
dotnet nuget add source https://nuget.pkg.github.com/tudoemcodigo/index.json -n tec-interno -u <usuario> -p <PAT>
dotnet add package TEC.Messaging.RabbitMQ --version 0.0.1
```

O pacote traz o `TEC.Messaging` e o `TEC.Vault` junto (RabbitMQ.Client 7).

## 🚀 Início rápido

```csharp
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.RabbitMQ;
using TEC.Messaging.RabbitMQ.DependencyInjection;

builder.Services.AddTecVault(...);                       // segredo "rabbitmq" = amqps://usuario:senha@host/vhost
builder.Services.AddTecMessaging()
    .UseSqlServer<VendasDbContext>()
    .UseRabbitMq(o =>
    {
        o.Exchange = "vendas.eventos";
        o.RoutingKeyPrefixToRemove = "vendas.";            // "vendas.pedido-aprovado" vira a routing key "pedido-aprovado"
    })
    .AddConsumer<Faturamento>()
    .AddOutboxRelay();
builder.Services.AddHealthChecks().AddTecRabbitMq();
```

```csharp
public sealed class Faturamento(RabbitMqConsumerDependencies deps) : RabbitMqConsumer(deps)
{
    protected override QueueDefinition Queue { get; } = new("financeiro.faturamento", ["pedido-aprovado"]);

    protected override Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken ct) => ...;
}
```

Fora de Development, `amqp://` é recusado (`RequireTls`): use `amqps://`. A URI (com senha) vem só do cofre e nunca é registrada em log.

## 📚 Documentação

Topologia, formato no fio, opções (`RabbitMqOptions`, `QueueDefinition`), circuit breaker, administração da DLQ, logs e erros: [docs/rabbitmq.md](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/rabbitmq.md). O desenho do retry e da DLQ: [ADR 0007](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/adr/0007-retry-escalonado-e-dlq-no-rabbitmq-com-filas-de-espera.md).

`net10.0` · Native AOT · [Apache 2.0](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/LICENSE) · Roberto Oliveira, equipe Tudo em Código
