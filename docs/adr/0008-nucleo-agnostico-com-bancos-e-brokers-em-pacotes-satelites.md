# ADR 0008: Núcleo Agnóstico com Bancos e Brokers em Pacotes Satélites

## Status
Aprovado

## Data
2026-10-24 (registro retroativo de uma decisão já implementada na versão 0.0.1)

## Contexto
A mensageria precisa servir a aplicações com bancos diferentes (SQL Server, PostgreSQL) e brokers diferentes (RabbitMQ, Azure Service Bus, AWS SQS/SNS), com ou sem CQRS. Um pacote único traria para toda aplicação o EF Core, os drivers e os SDKs de nuvem que ela não usa, e impediria que o núcleo fosse compatível com Native AOT (o EF Core e os SDKs de nuvem não são).

## Decisão
Dividir em um **núcleo sem banco nem broker** e **satélites** por tecnologia, todos versionados e publicados juntos:

- **Núcleo** ([`TEC.Messaging`](../../TEC.Messaging/TEC.Messaging.csproj)): envelope, contratos, mapeadores, relay, sanitizador, telemetria e as **portas**: [`IOutboxStore`](../../TEC.Messaging/Outbox/IOutboxStore.cs), [`IInboxStore`](../../TEC.Messaging/Inbox/IInboxStore.cs), [`IMessagePublisher`](../../TEC.Messaging/Transport/IMessagePublisher.cs), [`IDeadLetterAdministration`](../../TEC.Messaging/Administration/IDeadLetterAdministration.cs) e a interface marcadora `ITransportUnavailableException` ([ADR 0003](0003-falhas-isoladas-backoff-e-transporte-indisponivel-sem-contar-tentativa.md)). Depende só de abstrações do `Microsoft.Extensions` e do TEC.Core; é compatível com Native AOT.
- **Persistência**: [`TEC.Messaging.SqlServer`](../../TEC.Messaging.SqlServer/TEC.Messaging.SqlServer.csproj) e [`TEC.Messaging.PostgreSql`](../../TEC.Messaging.PostgreSql/TEC.Messaging.PostgreSql.csproj), sobre o `DbContext` da aplicação, com a mesma semântica e o SQL próprio de cada banco ([ADR 0002](0002-reserva-concorrente-do-outbox-com-token-e-relogio-do-banco.md)).
- **Transporte**: [`TEC.Messaging.RabbitMQ`](../../TEC.Messaging.RabbitMQ/TEC.Messaging.RabbitMQ.csproj), [`TEC.Messaging.AzureServiceBus`](../../TEC.Messaging.AzureServiceBus/TEC.Messaging.AzureServiceBus.csproj) e [`TEC.Messaging.Aws`](../../TEC.Messaging.Aws/TEC.Messaging.Aws.csproj). Só os transportes dependem do TEC.Vault: as credenciais (URI, connection string, chaves) vêm de segredos do cofre, nunca da configuração.
- **Registro encadeado**: cada satélite acrescenta métodos ao `MessagingBuilder` (`UseSqlServer<TContext>()`, `UsePostgreSql<TContext>()`, `UseRabbitMq(...)`, `UseAzureServiceBus(...)`, `UseAws(...)`).
- **Acesso aos internos**: o núcleo declara `InternalsVisibleTo` para os cinco satélites (telemetria, `GuidV7`, `SensitiveDataSanitizer`, regra comum de `RequireTls`).
- **Native AOT por pacote**: `IsAotCompatible=false` justificado em `SqlServer`, `PostgreSql` (EF Core, modelo dinâmico), `AzureServiceBus` e `Aws` (reflexão interna dos SDKs); núcleo e RabbitMQ seguem compatíveis.
- **Alvo único**: `net10.0` (LTS), com EF Core e Npgsql 10.x. O alvo `net8.0` foi retirado em 2026-10-29, junto com os polyfills e as versões condicionadas por TFM.
- **Sem TEC.Cqrs nem TEC.ORM**: a integração com a aplicação é pelo `SaveChanges` (interceptor) e por interfaces próprias, para servir a qualquer `DbContext`.

## Consequências
### Positivas
- A aplicação instala só o que usa; trocar de banco ou broker é trocar o satélite, sem mudar contratos nem casos de uso.
- O núcleo permanece pequeno, testável com armazenamento e publicador em memória (relay, carga) e compatível com Native AOT.
- Credenciais de broker e banco ficam fora de arquivos de configuração.

### Neutras / Negativas
- O interceptor, as entidades e o armazenamento do SQL Server e do PostgreSQL são código quase duplicado, com divergências de nome (ex.: `UseTecMessaging` × `UseTecMessagingPostgreSql`, `IDomainEventsSavingObserver` × `IPostgreSqlDomainEventsSavingObserver`; só o SQL Server tem `AddDomainEventsObserver`).
- Os transportes não têm paridade completa: pausa, administração da DLQ, circuit breaker de conexão e drenagem configurável no desligamento existem só no RabbitMQ. Desde 2026-10-29 os três validam o envelope no consumo, aplicam `MaxPayloadLength`, emitem as mesmas métricas de consumo, exigem TLS fora de Development e só marcam com `ITransportUnavailableException` as falhas transitórias ([ADR 0003](0003-falhas-isoladas-backoff-e-transporte-indisponivel-sem-contar-tentativa.md)). A espera entre tentativas difere: filas de espera no RabbitMQ, reenvio agendado no Azure Service Bus ([ADR 0009](0009-reenvio-agendado-no-azure-service-bus-e-rotacao-de-credenciais.md)) e visibilidade da mensagem na AWS (2 s a 300 s).
- Seis pacotes e uma única versão exigem publicar e atualizar em conjunto.
