[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › Telemetria

# 📡 Telemetria

> Spans, métricas e correlação de ponta a ponta, do caso de uso ao consumidor.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

| Sinal | Nome | Detalhe |
|---|---|---|
| Span | `<tipo> publish` (Producer) | `messaging.system`, `messaging.destination.name`, `messaging.message.id`, `messaging.message.conversation_id` |
| Span | `<tipo> process` (Consumer) | Continua o trace do produtor pelo `traceparent` |
| Contador | `tec.messaging.outbox.enqueued` / `published` / `publish_failures` / `dead` | Tag `tec.messaging.type` |
| Contador + histograma | `tec.messaging.consumer.messages` / `tec.messaging.consumer.duration` | Tags `messaging.destination.name`, `tec.messaging.outcome` |
| Gauge | `tec.messaging.outbox.pending` / `pending_with_errors` / `dead_messages` / `oldest_pending_age` | Atualizados pelo relay |
| Gauge | `tec.messaging.dlq.messages` | Por fila, pelo monitor do RabbitMQ |

ActivitySource e Meter: `TEC.Messaging` (`MessagingDiagnostics.ActivitySourceName`/`MeterName`), assinados automaticamente pelo
TEC.Observability.

---

## 🚀 Uso

Correlação na borda HTTP:

```csharp
app.Use(async (http, next) =>
{
    using var _ = MessageContext.Begin(http.Request.Headers["X-Correlation-ID"]);
    await next(http);
});
```

O consumidor abre o escopo sozinho (correlação do envelope e causa = `MessageId` recebido): mensagens gravadas no Outbox
durante o processamento herdam os dois.

Alertas sugeridos: `oldest_pending_age > 300 s`, `dead_messages > 0`, `dlq.messages > 0`.

## ⚙️ Opções

`OutboxOptions.StatisticsInterval` e `RabbitMqOptions.DeadLetterMonitorInterval` controlam os gauges (0 desliga).

## ❌ Erros

| Situação | Causa | O que fazer |
|---|---|---|
| Gauges sem valor | Sem `IOutboxAdministration` registrado ou intervalo 0 | Use `UseSqlServer<TContext>()`; ajuste o intervalo |

## 🛡️ Segurança

> [!NOTE]
> Spans e métricas nunca carregam o payload; só tipo, id e correlação. A correlação de entrada é sanitizada (tamanho e
> caracteres de controle).

## ❓ Perguntas frequentes

<details>
<summary><b>Sem escopo aberto, qual correlação vai para a mensagem?</b></summary>

O `TraceId` da atividade atual (W3C), se houver.
</details>

---
⬅️ [RabbitMQ](rabbitmq.md) · [📚 Índice](README.md) · [Segurança](seguranca.md) ➡️
