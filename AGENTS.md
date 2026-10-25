# Instruções para agentes — TEC.Messaging

Este arquivo complementa as instruções herdadas de `AGENT_BASE.md`. O TEC.Messaging implementa mensageria corporativa de alto desempenho com Outbox transacional, Inbox idempotente, sanitização de PII (LGPD), contratos versionados e suporte agnóstico a RabbitMQ, Azure Service Bus e AWS SQS/SNS.

## Escopo e segurança de alterações

- O escopo padrão é este repositório. Não altere outros repositórios `TEC.*`, não publique pacotes/tags/releases e não rode sincronizações que escrevam fora daqui sem pedido explícito.
- Antes de editar, confira `git status` e `git diff`. Preserve alterações não commitadas; não use reset, checkout destrutivo ou limpeza para descartar trabalho existente.
- Mudanças em contratos de integração (`[IntegrationEvent]`), envelope, Outbox/Inbox, sanitizador LGPD, relays de broker ou alvos .NET exigem revisão de compatibilidade, documentação, changelog e consumidores.
- Testes devem atravessar contratos públicos e verificar resultados observáveis. Para mudanças de comportamento, cubra entradas inválidas, limites, falhas e concorrência quando aplicável.
- Trate todo dado externo como não confiável. Não exponha segredos, PII ou conteúdo controlado pelo usuário em exceções, logs, URLs ou sumários.
- Sanitização de PII é obrigatória em erros do Outbox, cabeçalhos DLQ e resumos do relay. Payload tem limite configurável (padrão 256 KiB, até 4 MiB).

## Mapa da implementação

- `TEC.Messaging/`: biblioteca multi-target (`net8.0` e `net10.0`), núcleo agnóstico.
  - `Abstractions/`: `IMessageEnvelope`, `[IntegrationEvent]`, `IOutboxStore`, `IInboxStore`, `IMessageMapper`, `ISanitizer`, `IRelay`, `IConsumer`.
  - `Sanitization/`: sanitizador LGPD de PII (CPF, CNPJ, e-mail, telefone, cartão, documentos, tokens, chaves).
  - `Correlation/`: propagação de `traceparent`, `baggage`, `X-Correlation-ID` entre serviços.
  - `Telemetry/`: `ActivitySource` e `Meter` `TEC.Messaging` da BCL; assinados pelo TEC.Observability via prefixo `TEC.*`.
  - `DependencyInjection/`: `AddTecMessaging` com encadeamento fluente (`AddEventType`, `Map`, `UseSqlServer`, `UsePostgreSql`, `UseRabbitMq`, `UseAzureServiceBus`, `UseAws`, `AddOutboxRelay`).
- `TEC.Messaging.SqlServer/`: Outbox e Inbox no SQL Server com EF Core, interceptor de eventos de domínio, `UPDLOCK`/`READPAST`, administração.
- `TEC.Messaging.PostgreSql/`: Outbox e Inbox no PostgreSQL com EF Core (`FOR UPDATE SKIP LOCKED`, `RETURNING`), interceptor.
- `TEC.Messaging.RabbitMQ/`: publicador com confirms, consumidores com concorrência paralela, retry escalonado, DLQ, pausa, health check, circuit breaker.
- `TEC.Messaging.AzureServiceBus/`: publicador e consumidor para filas e tópicos do Azure Service Bus, confirms, DLQ, concorrência, health check.
- `TEC.Messaging.Aws/`: publicador e consumidor para filas Amazon SQS e tópicos SNS, long polling, concorrência, DLQ, health check.
- `TEC.Messaging.Tests/`: testes unitários sem dependências; integração com SQL Server e RabbitMQ em container.
- `TEC.Messaging.LoadTests/` e `TEC.Messaging.Benchmarks/`: carga e benchmarks.
- `samples/`: exemplos de uso.
- `build/`: props/targets compartilhados.
- `docs/adr/`: decisões arquiteturais.
- `docs/`: guias por contratos, Outbox, Inbox, SQL Server, PostgreSQL, RabbitMQ, Azure Service Bus, AWS, telemetria, segurança, testes, desenvolvimento.

## Contratos relevantes

- `Directory.Build.props` é a fonte única de versão do pacote (uma versão para todos os pacotes). A versão inicial é `0.0.1`; não introduza versões nos `.csproj`.
- `Directory.Packages.props` pode declarar versões distintas por TFM; condições por TFM são intencionais.
- Dependências TEC.* são declaradas com `<TecReference Include="..." />`, não com `PackageReference` direto.
- `[IntegrationEvent("nome", Version = N)]` define contrato público estável; alteração requer nova versão do evento.
- `IOutboxStore` e `IInboxStore` são contratos de persistência; implementações EF Core usam interceptors transacionais.
- Credenciais de brokers resolvidas exclusivamente via `TEC.Vault` (`ISecretReader`).
- Native AOT: `TEC.Messaging` e `TEC.Messaging.RabbitMQ` são compatíveis (`IsAotCompatible`, contratos com `JsonTypeInfo`); provedores EF Core e SDKs de nuvem marcam `<IsAotCompatible>false</IsAotCompatible>`.
- Workflows dos consumidores chamam `tudoemcodigo/tec-workflows`; confira as tags realmente publicadas antes de alterar referências.
- A licença é Apache-2.0. Mantenha `LICENSE`, `NOTICE`, badge/README e `<PackageLicenseExpression>` coerentes.

## Documentação

- `README.md` e `CHANGELOG.md` têm conteúdo próprio do TEC.Messaging: siga o padrão do template, mantenha a marca Tudo em Código em todos os READMEs e não substitua o conteúdo específico por placeholders genéricos.
- Atualize `README.md`, `CHANGELOG.md` e os guias em `docs/` quando uma mudança alterar comportamento ou contrato.
- Registre decisões transversais em um ADR numerado e mantenha `docs/adr/README.md` e `docs/README.md` atualizados.
- Consulte `docs/revisao-tecnica.md` como relatório de auditoria com data e escopo; não trate resultados históricos como estado atual sem nova verificação.
- Workflows e configurações centrais do consumidor estão em `.github/workflows/`; mantenha `.github/workflows/README.md` fiel aos gatilhos, permissões e inputs reais.

## Verificação local

```powershell
dotnet restore TEC.Messaging.slnx
dotnet build TEC.Messaging.slnx -c Release
dotnet test --solution TEC.Messaging.slnx -c Release
$env:DOTNET_SYSTEM_GLOBALIZATION_INVARIANT = "1"; dotnet test --solution TEC.Messaging.slnx -c Release; Remove-Item Env:DOTNET_SYSTEM_GLOBALIZATION_INVARIANT
git diff --check
```

O CI também valida convenções, pacote e workflows reutilizáveis. Não afirme que o CI está funcional apenas com build/testes locais: confirme que as tags chamadas existem e que os `permissions` do chamador incluem o mínimo exigido pelo workflow reutilizável.