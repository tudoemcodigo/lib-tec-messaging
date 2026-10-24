[🏠 TEC.Messaging](../README.md) › 📚 Documentação

# 📚 Documentação do TEC.Messaging

> Referência completa dos pacotes, um arquivo por tema: o README dá a visão geral; aqui ficam os detalhes.

## 📑 Sumário

- [🗂️ Temas](#️-temas)
- [🗺️ Mapa dos temas](#️-mapa-dos-temas)

---

## 🗂️ Temas

| # | Arquivo | O que responde | Principais tipos |
|:-:|---|---|---|
| 1 | [📜 Contratos de integração](contratos-de-integracao.md) | Como nomear, versionar e mapear eventos | `[IntegrationEvent]`, `IMessageTypeRegistry`, `IIntegrationEventMapper`, `MessagingBuilder` |
| 2 | [📤 Outbox](outbox.md) | Como o evento é gravado e publicado sem perda | `IOutboxStore`, `OutboxOptions`, `IOutboxAdministration`, `IOutbox`, `TecMessagingSaveChangesInterceptor` |
| 3 | [📥 Inbox](inbox.md) | Como processar cada mensagem uma única vez | `IInboxStore`, `InboxOptions` |
| 4 | [🐘 PostgreSQL](postgresql.md) | Persistência com PostgreSQL (SKIP LOCKED, RETURNING) | `PostgreSqlOutboxStore`, `PostgreSqlInboxStore`, `UsePostgreSql` |
| 5 | [🐇 RabbitMQ](rabbitmq.md) | Topologia, consumidores, retry, DLQ, concorrência e pausa | `RabbitMqOptions`, `RabbitMqConsumer`, `QueueDefinition`, `ConsumeResult`, `IDeadLetterAdministration` |
| 6 | [☁️ Azure Service Bus](azureservicebus.md) | Publicação e consumo em tópicos e filas no Azure | `AzureServiceBusOptions`, `AzureServiceBusConsumer`, `UseAzureServiceBus` |
| 7 | [🟧 AWS SQS / SNS](aws.md) | Publicação e consumo com Amazon SQS e SNS | `AwsMessagingOptions`, `AwsSqsConsumer`, `UseAwsMessaging` |
| 8 | [📡 Telemetria](telemetria.md) | Spans, métricas e correlação | `MessagingDiagnostics`, `MessageContext` |
| 9 | [🛡️ Segurança e LGPD](seguranca.md) | Sanitização de PII, segredos, limites e dados sensíveis | `SensitiveDataSanitizer` |
| 10 | [🧪 Testes](testes.md) | Categorias, integração local, carga e benchmarks | — |
| 11 | [🛠️ Desenvolvimento](desenvolvimento.md) | Build, modo local, lock files e publicação | — |
| 12 | [🔍 Revisão técnica](revisao-tecnica.md) | Achados de segurança, bugs e organização (2026-10-19) e melhorias sugeridas | — |

Fora de `docs/`: [🧰 Samples](../samples/README.md) · [⚙️ CI/CD](../.github/workflows/README.md) · [📝 Changelog](../CHANGELOG.md).

---

## 🗺️ Mapa dos temas

```mermaid
flowchart LR
    C["📜 contratos"] --> O["📤 outbox"]
    O --> DB["🗄️ SQL Server / 🐘 PostgreSQL"]
    DB --> BROKERS["🐇 RabbitMQ / ☁️ Azure Service Bus / 🟧 AWS"]
    BROKERS --> I["📥 inbox"]
    BROKERS --> T["📡 telemetria"]
    T --> S["🛡️ seguranca e LGPD"]
    S --> TS["🧪 testes"]
    TS --> D["🛠️ desenvolvimento"]
    D --> RV["🔍 revisao-tecnica"]
```
