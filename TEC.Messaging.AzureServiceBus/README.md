<div align="center">

<img src="https://www.tudoemcodigo.com/marca/PNG/tudoemcodigo-icone-512.png" alt="TEC.Messaging" width="100" />

# ☁️ TEC.Messaging.AzureServiceBus

**Transporte Azure Service Bus do TEC.Messaging: publicação em tópicos e filas com confirmação, consumidores concorrentes e DLQ nativa, com a connection string no TEC.Vault.**

[📚 Documentação do pacote](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/azureservicebus.md) · [📚 TEC.Messaging](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/README.md) · [🐙 Repositório](https://github.com/tudoemcodigo/lib-tec-messaging)

</div>

## ✨ O que é

| Peça | Papel |
|---|---|
| `IMessagePublisher` | `ServiceBusSender` por destino; o envelope viaja em propriedades do `ServiceBusMessage` e o corpo é o payload JSON; propaga `traceparent` e correlação. Só falha transitória conta como transporte indisponível |
| `AzureServiceBusConsumer` | Base dos consumidores sobre o `ServiceBusProcessor`, com conclusão manual, `MaxConcurrentCalls` (padrão 10), validação do envelope, `MaxPayloadLength` e métricas de consumo |
| Nova tentativa | Em fila, cópia agendada com espera crescente (5 s a 15 min) e `MaxAttempts` (padrão 5); em assinatura, espera no handler e `Abandon` |
| DLQ | `Reject`, mensagem inválida e tentativas esgotadas vão à DLQ nativa com motivo e descrição sanitizados (sem PII) |
| Credencial | Connection string relida do cofre quando o serviço a recusa (rotação sem reinício); emulador sem TLS recusado fora de Development (`RequireTls`) |
| `AddTecAzureServiceBus()` | Health check que abre o link de envio do `DefaultTopic` (até 5 s) |

| `ConsumeResult` | Operação no Service Bus |
|---|---|
| `Completed` | `CompleteMessageAsync` |
| `Retry` · `Requeue` · exceção (fila) | `ScheduleMessageAsync` da cópia + `CompleteMessageAsync` |
| `Retry` · `Requeue` · exceção (assinatura) | espera + `AbandonMessageAsync` |
| `Reject` · tentativas esgotadas | `DeadLetterMessageAsync` |

## 🎯 Quando usar

Serviços hospedados no **Azure** que usam Service Bus (Standard ou Premium) para eventos de integração. Combine com um armazenamento (`TEC.Messaging.SqlServer` ou `.PostgreSql`) para o Outbox e o Inbox. O pacote não cria tópicos, filas nem assinaturas.

## 📥 Instalação

```bash
dotnet nuget add source https://nuget.pkg.github.com/tudoemcodigo/index.json -n tec-interno -u <usuario> -p <PAT>
dotnet add package TEC.Messaging.AzureServiceBus --version 0.0.1
```

O pacote traz o `TEC.Messaging` e o `TEC.Vault` junto (Azure.Messaging.ServiceBus 7.x).

## 🚀 Início rápido

```csharp
using TEC.Messaging.AzureServiceBus.DependencyInjection;
using TEC.Messaging.DependencyInjection;

builder.Services.AddTecVault(...);                       // segredo "azure-servicebus" = connection string
builder.Services.AddTecMessaging()
    .UseSqlServer<VendasDbContext>()
    .UseAzureServiceBus(o => o.DefaultTopic = "vendas-eventos")
    .AddAzureServiceBusConsumer<FaturamentoConsumer>()
    .AddOutboxRelay();
builder.Services.AddHealthChecks().AddTecAzureServiceBus();
```

```csharp
public sealed class FaturamentoConsumer(AzureServiceBusConsumerDependencies deps) : AzureServiceBusConsumer(deps)
{
    protected override ServiceBusConsumerDefinition Definition { get; } = new("vendas-eventos", Subscription: "faturamento");

    protected override Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken ct) => ...;
}
```

A connection string vem só do cofre e nunca é registrada em log.

## 📚 Documentação

Formato da mensagem, opções, resiliência e DLQ (inclusive o risco de entrega duplicada no reenvio agendado), erros e limitações atuais (sem `Pause`, só connection string): [docs/azureservicebus.md](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/azureservicebus.md).

`net10.0` · sem Native AOT (SDK do Azure) · [Apache 2.0](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/LICENSE) · Roberto Oliveira, equipe Tudo em Código
