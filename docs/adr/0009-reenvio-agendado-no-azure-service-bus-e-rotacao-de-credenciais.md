# ADR 0009: Reenvio Agendado no Azure Service Bus, Rotação de Credenciais e TLS Obrigatório

## Status
Aprovado

## Data
2026-10-29

## Contexto
A auditoria de 2026-10 encontrou três lacunas nos transportes de nuvem:

- No Azure Service Bus, `Retry` e `Requeue` faziam `Abandon` na hora. Com uma dependência fora do ar, a mensagem voltava em milissegundos, gastava o `MaxDeliveryCount` da entidade em segundos e ia parar na DLQ sem nunca ter tido chance real de sucesso.
- O cliente do Azure e os clientes da AWS eram criados uma vez e nunca mais. Chave rotacionada no cofre, SAS vencida ou token de sessão do STS expirado deixavam o processo falhando até um reinício. O RabbitMQ já recriava a conexão relendo o segredo.
- Nada impedia `amqp://` (sem TLS) num ambiente de produção, nem o emulador local do Service Bus.

## Decisão

**Reenvio agendado no Azure Service Bus** ([`AzureServiceBusConsumer`](../../TEC.Messaging.AzureServiceBus/Consumers/AzureServiceBusConsumer.cs)). Em fila, `Retry` e `Requeue` agendam uma cópia na própria fila com `ScheduledEnqueueTime` (espera de 5 s, 10 s, 20 s... até 15 min) e só depois concluem a original. As tentativas ficam na propriedade `tec-attempt`, porque o `DeliveryCount` recomeça na cópia; `Retry` acima de `MaxAttempts` (padrão 5) vai para a DLQ.

Agendar e concluir não são atômicos. Uma transação do Service Bus resolveria, mas exige camada Standard ou Premium e o mesmo link de envio e recebimento. A ordem escolhida é **agendar primeiro**:

- conclusão falhou com `MessageLockLost` (a original volta com certeza): a cópia agendada é cancelada;
- qualquer outra falha na conclusão (tempo esgotado, rede): a cópia fica, e a mensagem pode ser processada duas vezes.

Duplicidade é preferível a perda, e o Inbox absorve a repetição ([ADR 0004](0004-inbox-idempotente-na-mesma-transacao-do-efeito.md)). A cópia leva `MessageId` próprio (`{id}:{reenvio}`), senão a detecção de duplicatas da fila a descartaria e a mensagem sumiria; o identificador do envelope segue na propriedade `tec-message-id`.

Em **assinatura de tópico**, uma cópia enviada ao tópico chegaria a todas as assinaturas. Ali a espera acontece no próprio handler, com o lock renovado pelo processador (limitada a `AutoRenewLockDuration` menos 30 s), e a mensagem volta com `Abandon`, levando `tec-attempt` nas propriedades. O custo é ocupar uma vaga de concorrência durante a espera.

**Rotação de credenciais com single-flight.** [`AzureServiceBusConnection`](../../TEC.Messaging.AzureServiceBus/Internal/AzureServiceBusConnection.cs) e [`AwsClientFactory`](../../TEC.Messaging.Aws/Internal/AwsClientFactory.cs) ganham `InvalidateAsync`: quem recebe a recusa da credencial (`UnauthorizedAccessException` no Azure; `InvalidClientTokenId`, `SignatureDoesNotMatch`, `ExpiredToken` e afins na AWS) descarta o cliente, se ele ainda for o atual. Criação e descarte passam pela mesma trava, então vinte falhas simultâneas geram um descarte e uma recriação, com uma única leitura do cofre. O consumidor do Azure derruba e recria o processador quando o erro chega pelo `ProcessErrorAsync`. Na AWS o segredo aceita também `accessKey:secretKey:sessionToken`.

**TLS obrigatório fora de Development.** `RequireTls` (`bool?`) nos três transportes: `null` exige TLS quando `IHostEnvironment.IsDevelopment()` é falso, mesma regra do TEC.Http e do TEC.Security; sem ambiente registrado, o processo é tratado como produção. O RabbitMQ recusa `amqp://` ao abrir a conexão, o Azure recusa `UseDevelopmentEmulator=true` ao criar o cliente e a AWS recusa URLs `http://` na validação das opções e das definições de consumidor.

**Health checks que tocam a rede.** Azure: abre o link de envio do `DefaultTopic` e pede um lote vazio. AWS: `GetQueueAttributes` ou `GetTopicAttributes` no destino padrão. Os dois respondem em até 5 s.

## Consequências
### Positivas
- Uma dependência fora do ar não queima as tentativas do Service Bus em segundos.
- Rotação de chave no cofre não exige reiniciar o processo.
- Conexão sem TLS em produção falha cedo e com mensagem clara, sem tocar no broker.
- O health check deixa de dizer "saudável" só porque um objeto foi criado em memória.

### Neutras / Negativas
- Entrega duplicada continua possível no reenvio agendado; consumidores precisam do Inbox.
- Em assinatura, a espera ocupa vagas de concorrência; com muitas falhas seguidas o consumo desacelera (o que também funciona como freio).
- O `MessageId` da cópia agendada não é mais o id do envelope. Ferramentas que leem o `MessageId` cru do Service Bus precisam olhar `tec-message-id`.
- O health check da AWS exige `sqs:GetQueueAttributes` ou `sns:GetTopicAttributes`; o do Azure exige permissão `Send` e um `DefaultTopic` (sem ele responde `Degraded`).
- Ambientes que usavam `amqp://` ou o emulador fora de Development precisam de `amqps://` ou de `RequireTls = false` explícito.
