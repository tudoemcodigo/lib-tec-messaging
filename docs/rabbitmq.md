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

### Resiliência da conexão

- **Circuit breaker da conexão** (`Polly.Core`, `RabbitMqOptions.CircuitBreaker`, ligado por padrão): a conexão é única no
  processo e serve publicador, consumidores, monitor de DLQ e health check. Com o broker (ou o cofre) fora do ar, depois de
  falhas suficientes o circuito abre e quem pede a conexão recebe `RabbitMqCircuitOpenException` na hora, sem tocar no cofre
  nem no broker: acaba a tempestade de reconexões. Depois de `BreakDuration`, uma tentativa de teste decide se fecha. O
  Outbox mantém as mensagens e tenta de novo depois (o circuito aberto conta como falha da tentativa).
- **Reconexão do consumidor** com espera exponencial e variação de ±20% (`ReconnectDelay` → `MaxReconnectDelay`), zerada
  quando o consumo é retomado.
- **Health check** com o circuito aberto responde `Unhealthy` na hora, sem nova tentativa. Fora disso espera no máximo 5 s,
  sem cancelar a criação da conexão (o desfecho real é contado pelo circuito).
- Na tentativa de teste (meia-abertura), cancelamento conta como falha: o circuito só fecha com o broker respondendo.
- O retry das **mensagens** continua sendo do broker (filas de espera e DLQ): o processamento nunca é repetido em memória.

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
| `ReconnectDelay` | 1 s | Espera inicial para reabrir o consumo depois de uma queda (100 ms a 1 min); dobra a cada falha seguida |
| `MaxReconnectDelay` | 1 min | Maior espera entre tentativas de reabrir o consumo (1 s a 10 min) |
| `CircuitBreaker:Enabled` | `true` | Circuit breaker da criação da conexão |
| `CircuitBreaker:FailureRatio` | 0,5 | Proporção de falhas que abre o circuito (> 0 e ≤ 1) |
| `CircuitBreaker:MinimumThroughput` | 5 | Mínimo de tentativas de conexão na janela (2 a 10.000) |
| `CircuitBreaker:SamplingDuration` | 30 s | Janela de amostragem (0,5 s a 1 h) |
| `CircuitBreaker:BreakDuration` | 30 s | Tempo aberto antes da tentativa de teste (0,5 s a 1 h) |

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
| `RabbitMqCircuitOpenException` · health check "circuito aberto" | Falhas repetidas ao conectar (broker, rede, credencial ou cofre); log 4401 | Corrija a causa; o circuito testa de novo depois de `BreakDuration` (logs 4402 e 4403) |

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
