# ADR 0006: Sanitização de PII e Credenciais em Erros, Logs e Cabeçalhos

## Status
Aprovado

## Data
2026-10-24 (registro retroativo de uma decisão já implementada na versão 0.0.1)

## Contexto
Quando uma publicação ou um processamento falha, a mensagem da exceção (do driver do banco, do SDK do broker, ou do handler da aplicação) é guardada em lugares de longa vida e de leitura ampla: a coluna `LastError` do Outbox, o cabeçalho `x-tec-error` das filas de espera e da DLQ do RabbitMQ, o motivo da DLQ do Azure Service Bus e os logs. Essas mensagens podem conter connection strings, senhas, tokens e dados pessoais (CPF, CNPJ, cartão), o que fere a LGPD e o PCI-DSS. Também chegam ao componente valores de **fontes não confiáveis**: o `X-Correlation-ID` de uma requisição HTTP e o `type`/`correlationId` de uma mensagem recebida do broker.

## Decisão
Aplicar uma **sanitização por padrões** antes de gravar ou registrar texto de erro, e validar as entradas externas que viram metadados:

- [`SensitiveDataSanitizer`](../../TEC.Messaging/Internal/SensitiveDataSanitizer.cs) (interno ao núcleo, estático) mascara com `***REDACTED***` tokens Bearer em formato JWT, pares `senha/password/pwd/secret/token/apikey/client_secret/access_token` com valor, segredos de connection string (`pwd`, `password`, `user id`, `uid`, `secret`, `accountkey`, `sharedaccesskey`), cartões de crédito, CPF, CNPJ e CVV/CVC. Os padrões são `[GeneratedRegex]`: gerados em tempo de compilação, sem alocação extra e compatíveis com Native AOT.
- **Onde se aplica**: erro gravado no Outbox (`LastError`, truncado em 1000 caracteres) e resumo do relay ([`OutboxRelay`](../../TEC.Messaging/Outbox/OutboxRelay.cs)); cabeçalho `x-tec-error` e motivo de retry/DLQ ([`RabbitMqConsumer`](../../TEC.Messaging.RabbitMQ/Consumers/RabbitMqConsumer.cs)); motivo e descrição da DLQ ([`AzureServiceBusConsumer`](../../TEC.Messaging.AzureServiceBus/Consumers/AzureServiceBusConsumer.cs)); motivo derivado de exceções do handler nos três consumidores.
- **Correlação**: [`MessageContext`](../../TEC.Messaging/Context/MessageContext.cs) descarta valores vazios, com caracteres de controle ou acima de 128 caracteres.
- **Telemetria**: o nome do span de consumo só usa o `Type` da mensagem se ele tiver o formato de nome de contrato; senão usa o destino (evita cardinalidade criada por um produtor).
- **Payload**: tamanho limitado por `MessagingOptions.MaxPayloadLength` (padrão 256 KiB, teto de 4 MiB); o conteúdo do payload **não** é sanitizado, e a orientação é colocar só ids e metadados nos contratos ([ADR 0005](0005-contratos-de-integracao-versionados-separados-do-dominio.md)).
- A API não é pública: o sanitizador é `internal` (acessível aos satélites por `InternalsVisibleTo`).

## Consequências
### Positivas
- Credenciais e identificadores pessoais que vazam em mensagens de exceção não chegam ao banco, à DLQ nem aos logs do relay.
- Custo previsível e sem reflexão; funciona em Native AOT.
- Entradas externas (correlação, tipo) não inflam metadados nem telemetria.

### Neutras / Negativas
- A sanitização é por regex e tem limites: tokens opacos sem pontos (`Bearer abc123`) não são mascarados, documentos fora dos formatos cobertos (RG, passaporte) passam, e qualquer sequência de 11 dígitos é mascarada como CPF.
- O objeto de exceção entregue ao `ILogger` (ex.: o evento de erro do handler no RabbitMQ) não passa pelo sanitizador; só os textos gravados e os resumos do relay.
- Não protege o payload: dados pessoais no evento continuam sendo responsabilidade do contrato.
- Mudar um padrão muda o texto gravado: testes de regressão em `SanitizerTests` cobrem os formatos principais.
