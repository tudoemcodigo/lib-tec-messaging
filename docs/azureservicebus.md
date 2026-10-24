[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › Azure Service Bus

# ☁️ Azure Service Bus

> Publicador e consumidor resiliente para filas e tópicos do Azure Service Bus com credenciais pelo TEC.Vault.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [⚙️ Opções](#️-opções)
- [🛡️ Resiliência e DLQ](#️-resiliência-e-dlq)
- [❌ Erros](#-erros)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

O pacote `TEC.Messaging.AzureServiceBus` conecta a mensageria do ecossistema TEC ao Azure Service Bus:

| Recurso | Detalhes |
|---|---|
| `ServiceBusSender` com pool | Cache de remetentes por entidade com publicação confirmada |
| `ServiceBusProcessor` | Consumo assíncrono gerenciado pelo SDK oficial `Azure.Messaging.ServiceBus` |
| `MaxConcurrentCalls` | Concorrência paralela configurável por consumidor |
| W3C Trace Context | Propagação nativa de `traceparent`, `CorrelationId` e metadados TEC |
| Dead-Letter Queue (DLQ) | Encaminhamento para a DLQ com motivo e descrição sanitizados contra vazamento de PII |
| Health Check | `AddTecAzureServiceBus()` com verificação de conectividade |

---

## 🚀 Uso

### 1. Registro dos Serviços

```csharp
services.AddTecVault(...); // Segredo "azure-servicebus" contendo a connection string do Azure

services.AddTecMessaging()
    .UseAzureServiceBus(o => 
    {
        o.TopicOrQueuePrefix = "minhaempresa.";
    })
    .AddConsumer<FaturamentoConsumer>()
    .AddOutboxRelay();

services.AddHealthChecks().AddTecAzureServiceBus();
```

### 2. Implementação do Consumidor

```csharp
public sealed class FaturamentoConsumer(
    AzureServiceBusConsumerDependencies deps,
    IServiceScopeFactory scopes) 
    : AzureServiceBusConsumer(deps)
{
    protected override AzureServiceBusSubscription Definition { get; } = new(
        TopicName: "vendas",
        SubscriptionName: "faturamento",
        MaxConcurrentCalls: 10,
        AutoCompleteMessages: false);

    protected override async Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var inbox = scope.ServiceProvider.GetRequiredService<IInboxStore>();
        
        if (!await inbox.TryBeginAsync(message.Envelope.MessageId, "faturamento", ct))
            return ConsumeResult.Completed;

        // Processar negócio...
        return ConsumeResult.Completed;
    }
}
```

---

## ⚙️ Opções

| Opção (`AzureServiceBusOptions`) | Padrão | Descrição |
|---|---|---|
| `ConnectionSecretName` | `azure-servicebus` | Nome do segredo no TEC.Vault |
| `TopicOrQueuePrefix` | `""` | Prefixo opcional para os tópicos ou filas |
| `PublishTimeout` | 10 s | Tempo limite de confirmação da publicação |
| `Enabled` | `true` | Habilita ou desabilita o broker |

---

## 🛡️ Resiliência e DLQ

- Em caso de falha transitória ou chamada a `ConsumeResult.Retry(motivo)`, a mensagem é abandonada para entrega subsequente gerenciada pelas políticas da assinatura no Azure.
- Em caso de chamada a `ConsumeResult.Reject(motivo)` ou exceções não recuperáveis, a mensagem é movida diretamente para a Dead-Letter Queue via `DeadLetterMessageAsync`, com o motivo da falha devidamente higienizado contra PII.

---

⬅️ [RabbitMQ](rabbitmq.md) · [📚 Índice](README.md) · [AWS SQS/SNS](aws.md) ➡️
