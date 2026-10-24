# ☁️ TEC.Messaging.AzureServiceBus

Transporte para **Azure Service Bus** do **TEC.Messaging**.

## Funcionalidades

- **Publicação Confiável**: Publicação em tópicos ou filas com publisher confirms e integração de credenciais pelo `TEC.Vault`.
- **Processamento Concorrente**: Consumo via `ServiceBusProcessor` com suporte a concorrência (`MaxConcurrentCalls`), auto-renovação de lock e ack/abandon explícito.
- **Dead Lettering Nativo**: Encaminhamento para a DLQ do Azure Service Bus (`$DeadLetterQueue`) com motivo sanitizado (sem vazamento de PII/LGPD).
- **Telemetria W3C**: Propagação ponta a ponta de `traceparent`, correlação e causa.
