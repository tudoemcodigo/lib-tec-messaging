[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › RabbitMQ

# 🐇 RabbitMQ

> Publicação confirmada, consumidores com nova tentativa escalonada, fila de mensagens mortas e pausa.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

```mermaid
flowchart LR
    X["exchange topic"] -->|routing key| Q["fila (quorum)"]
    Q --> C["RabbitMqConsumer"]
    C -- Retry --> W["fila.retry.30s / 2m / 10m / 30m<br/>(TTL)"] -->|expira| Q
    C -- "Reject ou tentativas esgotadas" --> D["fila.dlq"]
    C -- Requeue --> Q
    Q -- "x-delivery-limit" --> D
```

| `ConsumeResult` | Efeito | Conta tentativa? |
|---|---|:---:|
| `Completed` | ack | — |
| `Retry(motivo)` | fila de espera do próximo atraso; na `MaxAttempts`-ésima falha, DLQ | ✅ |
| `Reject(motivo)` | DLQ | — |
| `Requeue(motivo)` | NACK com requeue (dependência fora do ar), limitado por `DeliveryLimit` | ❌ |
| Exceção | Como `Retry` | ✅ |

O ack só acontece depois do handler (e do commit dele). Se o processo cai, o broker reentrega.

---

## 🚀 Uso

```csharp
services.AddTecVault(...);                                   // segredo "rabbitmq" = amqps://usuario:senha@host/vhost
services.AddTecMessaging()
    .UseRabbitMq(o => o.RoutingKeyPrefixToRemove = "vendas.", builder.Configuration.GetSection(RabbitMqOptions.SectionName))
    .AddConsumer<Faturamento>()
    .AddOutboxRelay();
services.AddHealthChecks().AddTecRabbitMq();
```

```csharp
public sealed class Faturamento(RabbitMqConsumerDependencies deps) : RabbitMqConsumer(deps)
{
    protected override QueueDefinition Queue { get; } = new("financeiro.faturamento", ["pedido-aprovado", "pedido-cancelado"])
    {
        Prefetch = 5,
        MaxAttempts = 6,
        RetryDelays = [TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10)],
    };

    protected override Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken ct) => ...;
}
```

Pausa (ex.: circuit breaker de uma dependência): resolva o consumidor (singleton) e chame `Pause()`/`Resume()`.

Administração da DLQ (`IDeadLetterAdministration`): `CountAsync`, `ListAsync` (sem remover), `RequeueAsync` (tentativas
zeradas; um `handler` opcional pode tratar a mensagem por conta própria) e `DiscardAsync`.

---

## ⚙️ Opções

| Opção (`RabbitMqOptions`, seção `Messaging:RabbitMQ`) | Padrão | Descrição |
|---|---|---|
| `Enabled` | `true` | `false`: consumidores não sobem; publicação falha e o Outbox retém |
| `ConnectionSecretName` | `rabbitmq` | Segredo do TEC.Vault com a URI AMQP |
| `Exchange` | (obrigatória) | Exchange topic |
| `RoutingKeyPrefixToRemove` | `""` | Prefixo removido do tipo para formar a routing key |
| `PublishTimeout` | 10 s | Espera pela confirmação |
| `Heartbeat` | 30 s | Heartbeat da conexão |
| `DeadLetterMonitorInterval` | 1 min | Contagem das DLQs para a métrica (0 desliga) |

| `QueueDefinition` | Padrão | Descrição |
|---|---|---|
| `Prefetch` | 10 | Mensagens sem ack ao mesmo tempo |
| `MaxAttempts` | 5 | Falhas até a DLQ |
| `RetryDelays` | 30 s, 2 min, 10 min, 30 min | Esperas (a última se repete) |
| `DeliveryLimit` | 50 | `x-delivery-limit` da fila quorum |
| `Transient` | `false` | Fila exclusiva auto-delete, sem retry nem DLQ |

## ❌ Erros

| Situação | Causa | O que fazer |
|---|---|---|
| `InvalidOperationException: Segredo 'rabbitmq' ... indisponível` | Segredo ausente no cofre | Crie o segredo com a URI |
| `PRECONDITION_FAILED` ao declarar fila | Fila existente com argumentos diferentes (ex.: clássica) | Apague a fila antiga ou use outro nome |
| Health check `Unhealthy` | Broker fora ou credencial inválida | Verifique rede e segredo |

## 🛡️ Segurança

> [!WARNING]
> Use `amqps://` fora da máquina local. A URI (com senha) vem só do cofre e nunca é registrada em log.

## ❓ Perguntas frequentes

<details>
<summary><b>Por que filas quorum?</b></summary>

Replicadas, com `x-delivery-limit` (proteção contra laço de reentrega) e dead-lettering `at-least-once`.
</details>

---
⬅️ [Inbox](inbox.md) · [📚 Índice](README.md) · [Telemetria](telemetria.md) ➡️
