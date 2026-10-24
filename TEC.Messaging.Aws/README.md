<div align="center">

<img src="https://www.tudoemcodigo.com/marca/PNG/tudoemcodigo-icone-512.png" alt="TEC.Messaging" width="100" />

# 🟧 TEC.Messaging.Aws

**Transporte AWS do TEC.Messaging: publicação em tópicos SNS ou filas SQS, consumo com long polling e concorrência, DLQ nativa do SQS e credenciais do TEC.Vault ou do papel IAM.**

[📚 Documentação do pacote](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/aws.md) · [📚 TEC.Messaging](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/README.md) · [🐙 Repositório](https://github.com/tudoemcodigo/lib-tec-messaging)

</div>

## ✨ O que é

| Peça | Papel |
|---|---|
| `IMessagePublisher` | Destino `arn:aws:sns:...` publica no tópico SNS; qualquer outro destino é a URL de uma fila SQS. O envelope viaja em atributos de mensagem e o corpo é o payload JSON |
| `AwsSqsConsumer` | Base dos consumidores SQS: long polling (`WaitTimeSeconds` 20), `ConcurrencyLimit` (padrão 5), correlação e `traceparent` restaurados |
| DLQ | A **nativa do SQS** (`RedrivePolicy` da fila): `Reject` deixa a mensagem visível de novo até o `maxReceiveCount` |
| Credenciais | Segredo `aws-messaging` do TEC.Vault (`ACCESS_KEY_ID:SECRET_ACCESS_KEY`) ou a cadeia padrão da AWS (variáveis, perfil, papel IAM de ECS/EKS/EC2) |
| `AddTecAws()` | Health check |

| `ConsumeResult` | Operação no SQS |
|---|---|
| `Completed` | `DeleteMessage` |
| `Retry` · exceção | Nenhuma: a mensagem reaparece após o `VisibilityTimeout` |
| `Reject` · `Requeue` | `ChangeMessageVisibility(0)` |

## 🎯 Quando usar

Serviços na **AWS** que usam SNS/SQS para eventos de integração. Combine com um armazenamento (`TEC.Messaging.SqlServer` ou `.PostgreSql`) para o Outbox e o Inbox. O pacote não cria tópicos, filas nem assinaturas; ao ligar SNS a SQS ative a entrega bruta (*raw message delivery*).

## 📥 Instalação

```bash
dotnet nuget add source https://nuget.pkg.github.com/tudoemcodigo/index.json -n tec-interno -u <usuario> -p <PAT>
dotnet add package TEC.Messaging.Aws --version 0.0.1
```

O pacote traz o `TEC.Messaging` e o `TEC.Vault` junto (AWSSDK.SQS e AWSSDK.SimpleNotificationService).

## 🚀 Início rápido

```csharp
using TEC.Messaging.Aws.DependencyInjection;
using TEC.Messaging.DependencyInjection;

builder.Services.AddTecMessaging()
    .UseSqlServer<VendasDbContext>()
    .UseAws(o =>
    {
        o.Region = "sa-east-1";
        o.DefaultDestination = "arn:aws:sns:sa-east-1:123456789012:vendas-eventos";   // tópico SNS ou URL da fila SQS
    })
    .AddAwsSqsConsumer<EnvioNotaFiscalConsumer>()
    .AddOutboxRelay();
builder.Services.AddHealthChecks().AddTecAws();
```

```csharp
public sealed class EnvioNotaFiscalConsumer(AwsSqsConsumerDependencies deps) : AwsSqsConsumer(deps)
{
    protected override SqsConsumerDefinition Definition { get; } =
        new("https://sqs.sa-east-1.amazonaws.com/123456789012/vendas-notafiscal", ConcurrencyLimit: 8);

    protected override Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken ct) => ...;
}
```

Prefira o papel IAM do ambiente a chaves de longa duração; se usar o segredo, ele vem só do cofre.

## 📚 Documentação

Formato da mensagem, opções, credenciais, concorrência e DLQ, erros e limitações atuais (sem `ITransportUnavailableException`, sem `Pause`): [docs/aws.md](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/docs/aws.md).

`net8.0` e `net10.0` · sem Native AOT (AWS SDK) · [MIT](https://github.com/tudoemcodigo/lib-tec-messaging/blob/main/LICENSE) · Roberto Oliveira, equipe Tudo em Código
