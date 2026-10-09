# 📝 Changelog

Todas as mudanças relevantes do **TEC.Messaging** são registradas aqui. O formato segue o [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/) e o projeto usa [Versionamento Semântico](https://semver.org/lang/pt-BR/). Enquanto a versão for `0.x`, mudanças incompatíveis podem ocorrer em versões MINOR. Os três pacotes saem sempre juntos, com a mesma versão.

## [0.1.0] - 2026-10-09

Primeira versão, extraída e generalizada da mensageria do AuraTicket.

### ✨ Adicionado

#### 📮 TEC.Messaging

- `MessageEnvelope` (id, tipo, versão, ocorrência, correlação, causa, `traceparent`, payload) com validação de limites.
- Contratos de integração: `[IntegrationEvent(nome, Version)]`, `IMessageTypeRegistry` e `MessageTypeInfo`; registro por `AddEventType<T>()` (JSON por reflexão, padrão `JsonDefaults` do TEC.Core), `AddEventType<T>(JsonTypeInfo<T>)` (Native AOT) e `AddEventTypesFromAssembly`.
- Mapeamento domínio → integração: `IIntegrationEventMapper`, `IntegrationEventMapper<TDomainEvent>` e `Map<TDomainEvent>(func)`; `IntegrationEnvelopeFactory` (UUIDv7, correlação, causa e trace do fluxo).
- `MessageContext` (correlação/causa por `AsyncLocal`, com fallback para o `TraceId` da atividade e sanitização da entrada).
- Outbox: `IOutboxStore` com reserva (lease) e conclusão em transações curtas, `OutboxRelay` (publicação fora da transação, backoff exponencial por mensagem, `Dead` depois de `MaxAttempts`, pausa por falhas seguidas, limpeza e estatísticas), `IOutboxAdministration` (estatísticas, falhas, reprocessar, descartar) e `IOutbox` (eventos sem agregado).
- Inbox: `IInboxStore` e limpeza periódica (`AddInboxCleanup`).
- `IDeadLetterAdministration` (contar, listar, reprocessar, descartar DLQ), independente do broker.
- Telemetria `TEC.Messaging`: spans de publicação e consumo; contadores, histograma e gauges (pendentes, idade da mais antiga, mortas, DLQ) por `IMeterFactory`.

#### 🗄️ TEC.Messaging.SqlServer

- `modelBuilder.AddTecMessaging(schema)`: tabelas `OutboxMessages` e `InboxMessages` com índices filtrados; o índice das pendentes (`IX_<tabela>_Pending`, `OccurredAt, Id` com `NextAttemptAt` e `LeasedUntil` incluídos) segue a ordem da reserva, para reservas simultâneas não pularem mensagens livres.
- `TecMessagingSaveChangesInterceptor`: eventos de domínio → Outbox no mesmo `SaveChanges`; eventos descartados só depois do commit; falha desfaz o que foi adicionado (sem duplicar na nova tentativa); `IDomainEventsSavingObserver` para dados próprios na mesma transação.
- `SqlServerOutboxStore` (reserva com `UPDLOCK, READPAST, ROWLOCK` e `OUTPUT`, conclusão só com o token vigente), `SqlServerInboxStore`, `UseSqlServer<TContext>()` e `UseTecMessaging(sp)`.

#### 🐇 TEC.Messaging.RabbitMQ

- `UseRabbitMq(...)`: conexão com URI do TEC.Vault, publicador com publisher confirms e timeout, routing key derivada do tipo.
- `RabbitMqConsumer` (prefetch, ack manual depois do handler, retry por filas de espera com TTL, DLQ, limite de entregas, pausa/retomada, correlação e trace), `QueueDefinition`, `ConsumeResult`, `AddConsumer<T>()`.
- Administração da DLQ por AMQP, monitor da métrica das DLQs e health check `AddTecRabbitMq()`.
- Resiliência da conexão: circuit breaker da criação da conexão (`RabbitMqOptions.CircuitBreaker`, `Polly.Core`, ligado por padrão) com `RabbitMqCircuitOpenException`, health check sem nova tentativa com o circuito aberto, reconexão do consumidor com espera exponencial e variação (`ReconnectDelay`, `MaxReconnectDelay`); métrica `tec.messaging.connection.circuit.state_changes` e eventos 4211 e 4401–4404.
