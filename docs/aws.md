[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › AWS SQS / SNS

# 🟧 AWS SQS e SNS

> Publicação e consumo desacoplado em filas SQS e tópicos SNS com AWS SDK oficial e credenciais do TEC.Vault.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [⚙️ Opções](#️-opções)
- [🛡️ Concorrência e DLQ](#️-concorrência-e-dlq)
- [❌ Erros](#-erros)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

O pacote `TEC.Messaging.Aws` conecta o ecossistema TEC à nuvem Amazon Web Services:

| Recurso | Detalhes |
|---|---|
| Publicação híbrida | Envio para Tópicos SNS (Publish / Fan-out) ou Filas SQS (Direct Queue) |
| Long Polling SQS | `WaitTimeSeconds = 20` para minimizar requisições desnecessárias e custo |
| `MaxConcurrency` | Processamento assíncrono paralelo com `SemaphoreSlim` (1 a 100 mensagens simultâneas) |
| Dead-Letter Queue | Suporte a Dead-Letter Queue nativa do SQS ou descarte com DLQ dedicada |
| Credenciais seguras | AccessKey/SecretKey via TEC.Vault ou Role IAM nativa do ambiente (ECS/EKS/EC2) |
| Health Check | `AddTecAwsMessaging()` com verificação de conectividade |

---

## 🚀 Uso

### 1. Registro dos Serviços

```csharp
services.AddTecVault(...); // Segredo "aws-messaging" com credenciais ou string de acesso

services.AddTecMessaging()
    .UseAwsMessaging(o => 
    {
        o.Region = "sa-east-1";
    })
    .AddConsumer<EnvioNotaFiscalConsumer>()
    .AddOutboxRelay();

services.AddHealthChecks().AddTecAwsMessaging();
```

### 2. Implementação do Consumidor

```csharp
public sealed class EnvioNotaFiscalConsumer(
    AwsSqsConsumerDependencies deps,
    IServiceScopeFactory scopes) 
    : AwsSqsConsumer(deps)
{
    protected override AwsSqsQueueDefinition Definition { get; } = new(
        QueueUrl: "https://sqs.sa-east-1.amazonaws.com/123456789012/vendas-notafiscal",
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 20,
        MaxConcurrency: 8);

    protected override async Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var inbox = scope.ServiceProvider.GetRequiredService<IInboxStore>();
        
        if (!await inbox.TryBeginAsync(message.Envelope.MessageId, "envio-nf", ct))
            return ConsumeResult.Completed;

        // Processar emissão...
        return ConsumeResult.Completed;
    }
}
```

---

## ⚙️ Opções

| Opção (`AwsMessagingOptions`) | Padrão | Descrição |
|---|---|---|
| `Region` | `us-east-1` | Região AWS (ex.: `sa-east-1`, `us-east-1`) |
| `CredentialsSecretName` | `aws-messaging` | Nome do segredo no TEC.Vault (opcional se usando IAM Roles) |
| `DefaultTopicArnPrefix` | `""` | Prefixo ou ARN base dos tópicos SNS |
| `DefaultQueueUrlPrefix` | `""` | URL base das filas SQS |
| `PublishTimeout` | 10 s | Tempo limite de confirmação da publicação |

---

## 🛡️ Concorrência e DLQ

O consumidor executa recepção em lote (até 10 mensagens por requisição) e despacha as mensagens para execução paralela controlada por um `SemaphoreSlim(MaxConcurrency)`.

Em falhas:
- Falhas recuperáveis (`ConsumeResult.Retry`): a visibilidade da mensagem é alterada para `0` ou deixada expirar para reprocessamento pelo SQS redrive policy.
- Falhas permanentes (`ConsumeResult.Reject`): se configurada `DeadLetterQueueUrl`, a mensagem é encaminhada com cabeçalho sanitizado de erro e deletada da fila de origem.

---

⬅️ [Azure Service Bus](azureservicebus.md) · [📚 Índice](README.md) · [Telemetria](telemetria.md) ➡️
