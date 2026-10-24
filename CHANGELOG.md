# 📝 Changelog

Todas as mudanças relevantes do **TEC.Messaging** são registradas aqui. O formato segue o [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/) e o projeto usa [Versionamento Semântico](https://semver.org/lang/pt-BR/). Os pacotes do ecossistema saem em conjunto, alinhados com a mesma versão.

## [Não lançado]

### 📚 Documentação

- **Conferência da documentação com o código**: todos os temas de `docs/`, o README e os READMEs dos pacotes foram revisados contra o código. Corrigidos nomes de API e padrões (AWS: `UseAws`, `AddAwsSqsConsumer`, `AddTecAws`, `ConnectionSecretName`, `DefaultDestination`, `SqsConsumerDefinition`; Azure Service Bus: `DefaultTopic`, `ServiceBusConsumerDefinition`, `AddAzureServiceBusConsumer`; PostgreSQL: `UseTecMessagingPostgreSql`, SQL de reserva e nomes de índice), o mascaramento do sanitizador (`***REDACTED***`), o limite de payload (padrão 256 KiB, teto 4 MiB) e os pacotes do CI (seis).
- **Novos**: `docs/sqlserver.md`; `docs/adr/` com oito ADRs retroativos; READMEs dos pacotes `TEC.Messaging.SqlServer` e `TEC.Messaging.RabbitMQ`; READMEs de `PostgreSql`, `Aws` e `AzureServiceBus` no molde dos demais componentes TEC.
- **Ampliados**: Inbox, Contratos de integração, Outbox, RabbitMQ, Azure Service Bus, AWS, Telemetria (referência de spans, métricas e logs), Segurança (ameaças, riscos residuais e checklist), Testes e Desenvolvimento.
- A [Revisão técnica](docs/revisao-tecnica.md) ganhou um complemento com suspeitas de defeito encontradas na conferência (não corrigidas no código).

## [0.0.1] - 2026-10-23

Versão inaugural oficial do **TEC.Messaging** no ecossistema TEC, unificando mensageria transacional, resiliência corporativa, conformidade com a LGPD e suporte agnóstico a múltiplos bancos e brokers.

### ✨ Adicionado

#### 📮 TEC.Messaging (Núcleo)
- **Envelope e Contratos**:
  - `MessageEnvelope` com `MessageId` (UUIDv7 sequencial), tipo estável, versão, ocorrência, correlação, causa, `traceparent` (W3C Trace Context) e payload JSON.
  - Registro de contratos tipados via `[IntegrationEvent(nome, Version)]`, `IMessageTypeRegistry` e `MessageTypeInfo`.
  - Suporte completo a **Native AOT** com `JsonTypeInfo<T>` e mapeamento desacoplado de eventos de domínio (`IIntegrationEventMapper<TDomainEvent>` / `Map<TDomainEvent>`).
  - `MessageContext` baseado em `AsyncLocal` para propagação automática de correlação e causa com higienização de entrada.
- **Outbox Transacional**:
  - Abstração `IOutboxStore` com ciclo de vida transacional curto: reserva concorrente (lease) com token, conclusão condicionada ao token vigente e reservas expiradas que voltam a ficar disponíveis.
  - `OutboxRelay`: publicação resiliente fora da transação de dados, backoff exponencial por mensagem, isolamento de falhas, transição para status `Dead` após esgotamento de tentativas e pausa automática em caso de indisponibilidade do transporte.
  - `IOutboxAdministration` para monitoramento de estatísticas, reprocessamento (`RequeueAsync`) e descarte (`DiscardAsync`).
- **Inbox Idempotente**:
  - Abstração `IInboxStore` e `TryBeginAsync` para garantia de processamento de efeitos exatos uma única vez (*exactly-once processing semantics*).
  - Tarefa em background para limpeza periódica de registros processados (`AddInboxCleanup`).
- **Segurança e LGPD**:
  - `SensitiveDataSanitizer` baseado em regex geradas em tempo de compilação (`[GeneratedRegex]`, zero-allocation e AOT-safe).
  - Sanitização automática de dados pessoais e credenciais (CPF, CNPJ, cartões de crédito, códigos CVV/CVC, senhas, tokens Bearer e connection strings) em mensagens de erro de persistência (`LastError`), logs e cabeçalhos de erro de mensageria morta (`x-tec-error`).
  - Validação do limite de payload (`MaxPayloadLength`: padrão 256 KiB, configurável de 1 KiB a 4 MiB) na gravação no Outbox e no consumo do RabbitMQ.
- **Telemetria**:
  - Spans OpenTelemetry para publicação e processamento de mensagens.
  - Métricas corporativas integradas ao `IMeterFactory`: contadores de mensagens enfileiradas e publicadas, histograma de duração do consumo (RabbitMQ) e gauges de mensagens pendentes, com erro, mortas, idade do backlog e mensagens nas DLQs do RabbitMQ.

#### 🗄️ TEC.Messaging.SqlServer
- Persistência de Outbox e Inbox no Microsoft SQL Server utilizando Entity Framework Core.
- Reserva de mensagens com travamento pessimista de alta performance (`UPDLOCK, READPAST, ROWLOCK`) e projeção atômica via cláusula `OUTPUT`.
- `TecMessagingSaveChangesInterceptor` para conversão e persistência atômica de eventos de domínio no mesmo commit do agregado.
- Índices filtrados otimizados para evitar gargalos em tabelas de grande volume: `IX_OutboxMessages_Pending` (ordenado por `OccurredAt, Id`), `IX_OutboxMessages_Published` e `IX_OutboxMessages_Dead`.
- Detecção determinística de duplicações de chave primária do Inbox (SQL Server Error 2627).

#### 🐘 TEC.Messaging.PostgreSql
- Persistência de Outbox e Inbox no PostgreSQL utilizando Entity Framework Core e Npgsql.
- Reserva sem contenção utilizando a cláusula nativa `SELECT ... FOR UPDATE SKIP LOCKED` e retorno de lotes com `RETURNING target.*`.
- `TecMessagingPostgreSqlSaveChangesInterceptor` integrado ao ciclo de vida do `DbContext`.
- Índices parciais de banco: `ix_outbox_messages_pending`, `ix_outbox_messages_published` e `ix_outbox_messages_dead`.
- Reconhecimento automático de corridas de consumo via código de violação de unicidade PostgreSQL `23505` (`UniqueViolation`).

#### 🐇 TEC.Messaging.RabbitMQ
- Publicador com Publisher Confirms obrigatório e timeout configurável.
- Filas Quorum de alta disponibilidade com dead-lettering nativo (`x-delivery-limit`).
- Retry escalonado sem retenção de threads através de filas de espera baseadas em TTL (`fila.retry.<delay>`).
- Suporte a concorrência configurável (`QueueDefinition.ConcurrencyLimit`) via `SemaphoreSlim` (1 a 200 mensagens concorrentes).
- Desligamento gracioso com drenagem de handlers em andamento (`DrainTimeout`).
- Pausa e retomada dinâmica do consumo (`Pause()` / `Resume()`).
- Resiliência integrada com circuit breaker de conexão (`Polly.Core`) e health checks via `AddTecRabbitMq()`.

#### ☁️ TEC.Messaging.AzureServiceBus
- Publicador baseado em cache de remetentes `ServiceBusSender` com confirmação e propagação de W3C Trace Context.
- Consumidor baseado em `ServiceBusProcessor` com suporte a concorrência paralela (`MaxConcurrentCalls`).
- Integração nativa com Dead-Letter Queue (DLQ) com sanitização de PII em motivo e descrição da falha.
- Credenciais seguras obtidas via `TEC.Vault` e health check dedicado (`AddTecAzureServiceBus()`).

#### 🟧 TEC.Messaging.Aws
- Publicador com suporte inteligente a envio para Tópicos SNS (Publish / Fan-out) e Filas SQS (Direct Send).
- Consumidor assíncrono para Filas SQS com suporte a Long Polling (`WaitTimeSeconds = 20`) e concorrência paralela configurável (`SemaphoreSlim`).
- Dead-Letter Queue pela `RedrivePolicy` nativa do SQS e exclusão da mensagem (`DeleteMessage`) após o processamento.
- Resolução de credenciais via `TEC.Vault` ou IAM Role nativa de ambiente AWS e health check dedicado (`AddTecAws()`).
