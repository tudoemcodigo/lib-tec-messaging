# 📝 Changelog

Todas as mudanças relevantes do **TEC.Messaging** são registradas aqui. O formato segue o [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/) e o projeto usa [Versionamento Semântico](https://semver.org/lang/pt-BR/). Os pacotes do ecossistema saem em conjunto, alinhados com a mesma versão.

## [0.0.1] - 2026-10-30

Versão inaugural oficial do **TEC.Messaging** no ecossistema TEC, unificando mensageria transacional, resiliência corporativa, conformidade com a LGPD e suporte agnóstico a múltiplos bancos e brokers.

### ✨ Adicionado

#### 📮 TEC.Messaging (Núcleo)
- **Alvo único `net10.0`**, com EF Core e Npgsql 10.x, sem polyfill de `System.Threading.Lock` nem versões condicionadas por TFM.
- **TLS exigido fora de Development** (`RequireTls` em `RabbitMqOptions`, `AzureServiceBusOptions` e `AwsMessagingOptions`, padrão `null`): `amqp://` no RabbitMQ, o emulador do Service Bus e URLs `http://` na AWS são recusados. `RequireTls = false` libera essas conexões.
- **Falhas de publicação classificadas**: no Azure Service Bus e na AWS, só falha transitória (5xx, *throttling*, `ServiceBusy`, `ServiceTimeout`, `ServiceCommunicationProblem`, rede, tempo esgotado, cofre fora) é `ITransportUnavailableException`. Mensagem grande demais, destino inexistente ou desabilitado, permissão negada e credencial recusada contam tentativa e chegam a `Dead` no Outbox.
- **Envelope e Contratos**:
  - `OccurredAt` normalizado para UTC na fábrica de envelopes, nas linhas do Outbox e na leitura dos três consumidores (o Npgsql recusa fusos diferentes de zero).
  - `MessageEnvelope` com `MessageId` (UUIDv7 sequencial), tipo estável, versão, ocorrência, correlação, causa, `traceparent` (W3C Trace Context) e payload JSON.
  - Registro de contratos tipados via `[IntegrationEvent(nome, Version)]`, `IMessageTypeRegistry` e `MessageTypeInfo`.
  - Suporte completo a **Native AOT** com `JsonTypeInfo<T>` e mapeamento desacoplado de eventos de domínio (`IIntegrationEventMapper`, base `IntegrationEventMapper<TDomainEvent>` e `Map<TDomainEvent>`).
  - `MessageContext` baseado em `AsyncLocal` para propagação automática de correlação e causa com higienização de entrada.
- **Outbox Transacional**:
  - Abstração `IOutboxStore` com ciclo de vida transacional curto: reserva concorrente (lease) com token, conclusão condicionada ao token vigente e reservas expiradas que voltam a ficar disponíveis.
  - `OutboxRelay`: publicação resiliente fora da transação de dados, backoff exponencial por mensagem, isolamento de falhas, transição para status `Dead` após esgotamento de tentativas e pausa automática em caso de indisponibilidade do transporte.
  - `IOutboxAdministration` para monitoramento de estatísticas, reprocessamento (`RequeueAsync`) e descarte (`DiscardAsync`).
- **Inbox Idempotente**:
  - Abstração `IInboxStore` e `TryBeginAsync` para garantia de processamento de efeitos exatos uma única vez (*exactly-once processing semantics*).
  - Tarefa em background para limpeza periódica de registros processados (`AddInboxCleanup`).
- **Segurança e LGPD**:
  - `SensitiveDataSanitizer` baseado em regex geradas em tempo de compilação (`[GeneratedRegex]`, compatível com Native AOT).
  - Sanitização automática de dados pessoais e credenciais (CPF, CNPJ, cartões de crédito, códigos CVV/CVC, e-mails, telefones, senhas, tokens Bearer e connection strings) em mensagens de erro de persistência (`LastError`), logs e cabeçalhos de erro de mensageria morta (`x-tec-error`).
  - Sanitizador limitado: texto truncado em 4.096 caracteres antes das expressões, tempo máximo por expressão (falha fechada), padrão de e-mail sem custo quadrático e telefone que não pega qualquer sequência de 8 dígitos.
  - Validação do limite de payload (`MaxPayloadLength`: padrão 256 KiB, configurável de 1 KiB a 4 MiB) na gravação no Outbox e no consumo do RabbitMQ, do Azure Service Bus e da AWS.
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
- Reconhecimento automático de corridas de consumo via código de violação de unicidade PostgreSQL `23505` (`UniqueViolation`). Sem o Inbox no modelo, `IsDuplicate` não trata qualquer violação de unicidade como duplicata.
- A limpeza do Inbox apaga por `(MessageId, Consumer)`, sem levar registros de outros consumidores. `RequeueAsync` compara a reserva com o relógio do banco (`now()`).

#### 🐇 TEC.Messaging.RabbitMQ
- Publicador com Publisher Confirms obrigatório e timeout configurável.
- Publicação com `mandatory`: evento sem fila ligada à routing key lança `RabbitMqUnroutableException`, conta tentativa e chega a `Dead`, em vez de ser confirmado e descartado pelo broker.
- Filas Quorum de alta disponibilidade com dead-lettering nativo (`x-delivery-limit`).
- Retry escalonado sem retenção de threads através de filas de espera baseadas em TTL (`fila.retry.<delay>`).
- Suporte a concorrência configurável (`QueueDefinition.ConcurrencyLimit`) via `SemaphoreSlim` (1 a 200 mensagens concorrentes).
- Desligamento gracioso com drenagem de handlers em andamento (`DrainTimeout`). A contagem de mensagens em processamento volta mesmo quando o desligamento chega durante a espera por vaga, sem segurar a drenagem até o `DrainTimeout`.
- Pausa e retomada dinâmica do consumo (`Pause()` / `Resume()`).
- Resiliência integrada com circuit breaker de conexão (`Polly.Core`) e health checks via `AddTecRabbitMq()`.

#### ☁️ TEC.Messaging.AzureServiceBus
- Publicador baseado em cache de remetentes `ServiceBusSender` com confirmação e propagação de W3C Trace Context.
- Consumidor baseado em `ServiceBusProcessor` com suporte a concorrência paralela (`MaxConcurrentCalls`).
- Integração nativa com Dead-Letter Queue (DLQ) com sanitização de PII em motivo e descrição da falha.
- Nova tentativa por reenvio agendado (`ScheduledEnqueueTime`, 5 s a 15 min) com o contador em `tec-attempt`, em vez de `Abandon` imediato em fila; agenda antes de concluir e cancela a cópia se o lock foi perdido ([ADR 0009](docs/adr/0009-reenvio-agendado-no-azure-service-bus-e-rotacao-de-credenciais.md)). Em assinatura, espera no handler e `Abandon`. A cópia agendada tem `MessageId` próprio (`{id}:{reenvio}`); o id do envelope fica em `tec-message-id`.
- `ServiceBusConsumerDefinition` com `MaxAttempts` (padrão 5), validada ao iniciar o consumidor. Os eventos de log do consumidor são 4601 a 4611.
- O consumidor valida o envelope (`MessageEnvelope.Validate` e `MaxPayloadLength`): mensagem sem `MessageId`, sem tipo ou com payload grande demais vai para a DLQ sem chegar ao handler. Emite `tec.messaging.consumer.messages` e `tec.messaging.consumer.duration` e lê números e datas com `CultureInfo.InvariantCulture`.
- Credenciais seguras obtidas via `TEC.Vault`, com rotação: quando a credencial é recusada, o cliente é descartado e recriado com o segredo relido, com single-flight.
- Health check dedicado (`AddTecAzureServiceBus()`) que toca a rede: exige `Send` no `DefaultTopic` (sem ele responde `Degraded`).
- `AddAzureServiceBusConsumer` idempotente; o processador é liberado quando a subida falha.
- Sem dependência de `Polly.Core`.

#### 🟧 TEC.Messaging.Aws
- Publicador com suporte inteligente a envio para Tópicos SNS (Publish / Fan-out) e Filas SQS (Direct Send).
- Consumidor assíncrono para Filas SQS com suporte a Long Polling (`WaitTimeSeconds = 20`) e concorrência paralela configurável (`SemaphoreSlim`).
- Dead-Letter Queue pela `RedrivePolicy` nativa do SQS e exclusão da mensagem (`DeleteMessage`) após o processamento.
- Renovação da visibilidade enquanto o handler roda.
- `DefaultDestination` obrigatório e validado na subida com o transporte habilitado; `SqsConsumerDefinition` validada ao iniciar o consumidor.
- O consumidor valida o envelope (`MessageEnvelope.Validate` e `MaxPayloadLength`): mensagem sem `MessageId`, sem tipo ou com payload grande demais vai para a DLQ sem chegar ao handler. Emite `tec.messaging.consumer.messages` e `tec.messaging.consumer.duration` e lê números e datas com `CultureInfo.InvariantCulture`.
- Falha ao obter o cliente na subida não derruba o consumidor, e a vaga do semáforo é sempre liberada. `AddAwsSqsConsumer` é idempotente.
- Resolução de credenciais via `TEC.Vault` ou, com `UseAmbientCredentials = true`, pela identidade do ambiente AWS (IAM Role). Com o cofre, rotação: quando a credencial é recusada, o cliente é descartado e recriado com o segredo relido, com single-flight; aceita `accessKey:secretKey:sessionToken` (STS).
- Health check dedicado (`AddTecAws()`) que toca a rede: exige `sqs:GetQueueAttributes` ou `sns:GetTopicAttributes`.
- Sem dependência de `Polly.Core` (continua só no RabbitMQ).

### 📚 Documentação

- **Conferência da documentação com o código**: todos os temas de `docs/`, o README e os READMEs dos pacotes foram revisados contra o código: nomes de API e padrões (AWS: `UseAws`, `AddAwsSqsConsumer`, `AddTecAws`, `ConnectionSecretName`, `DefaultDestination`, `SqsConsumerDefinition`; Azure Service Bus: `DefaultTopic`, `ServiceBusConsumerDefinition`, `AddAzureServiceBusConsumer`; PostgreSQL: `UseTecMessagingPostgreSql`, SQL de reserva e nomes de índice), o mascaramento do sanitizador (`***REDACTED***`), o limite de payload (padrão 256 KiB, teto 4 MiB) e os pacotes do CI (seis). Os exemplos de XML docs não usam `RoutingKeyPrefixToRemove`, que não casava com a ligação do consumidor.
- `docs/sqlserver.md`; `docs/adr/` com oito ADRs retroativos e o ADR 0009; READMEs de todos os pacotes (`SqlServer`, `PostgreSql`, `RabbitMQ`, `AzureServiceBus` e `Aws`) no molde dos demais componentes TEC.
- Guias de Inbox, Contratos de integração, Outbox, RabbitMQ, Azure Service Bus, AWS, Telemetria (referência de spans, métricas e logs), Segurança (ameaças, riscos residuais e checklist), Testes e Desenvolvimento.
- A [Revisão técnica](docs/revisao-tecnica.md) tem um complemento com suspeitas de defeito encontradas na conferência (não corrigidas no código).
