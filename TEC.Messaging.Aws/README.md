# 🔶 TEC.Messaging.Aws

Transporte para **AWS (SQS e SNS)** do **TEC.Messaging**.

## Funcionalidades

- **Publicação Confiável**: Publicação em tópicos SNS (fanout) ou filas SQS com publisher confirms e credenciais integradas pelo `TEC.Vault`.
- **Consumo Concorrente com Long Polling**: Consumo SQS via long polling com suporte a paralelismo (`ConcurrencyLimit`) e cancelamento limpo.
- **Dead Lettering e Retentativas**: Integração com redrive policy do SQS para DLQ nativa e sanitização automática de erros (conformidade LGPD).
- **Telemetria W3C**: Propagação ponta a ponta de `traceparent`, correlação e causa nos atributos da mensagem.
