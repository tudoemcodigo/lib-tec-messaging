# ADR 0003: Falhas Isoladas por Mensagem, Backoff e Transporte Indisponível Sem Contar Tentativa

## Status
Aprovado

## Data
2026-10-24 (registro retroativo de uma decisão já implementada na versão 0.0.1)

## Contexto
Um relay que trava a fila inteira numa mensagem com defeito (*poison message*) deixa todas as outras sem publicar. Por outro lado, tratar toda falha como defeito da mensagem tem um efeito perverso quando o **broker** é que está fora do ar: numa primeira versão do relay, as mensagens mais antigas chegavam a `Dead` em ~14 minutos de indisponibilidade e exigiam ação manual, perdendo a ordem ([🔍 Revisão técnica](../revisao-tecnica.md), A1). O núcleo, por sua vez, não pode conhecer as exceções de cada transporte ([ADR 0008](0008-nucleo-agnostico-com-bancos-e-brokers-em-pacotes-satelites.md)).

## Decisão
O [`OutboxRelay`](../../TEC.Messaging/Outbox/OutboxRelay.cs) classifica cada falha de publicação em três caminhos, configurados por [`OutboxOptions`](../../TEC.Messaging/Outbox/OutboxOptions.cs):

1. **Falha da mensagem** (qualquer exceção sem a marca abaixo): a mensagem entra em espera com *backoff* exponencial por mensagem (`InitialRetryDelay × 2^(n-1)`, teto `MaxRetryDelay`) e as demais seguem. Ao esgotar `MaxAttempts` ela vira `Dead`: não volta sozinha e **não bloqueia as outras**; o administrador reprocessa (`RequeueAsync`) ou descarta (`DiscardAsync`).
2. **Falhas seguidas** (`ConsecutiveFailuresToPause`): o resto do lote volta sem contar tentativa e o relay pausa por `PauseDuration`. As falhas em si contam: podem ser mensagens defeituosas, e não contá-las travaria o Outbox atrás delas.
3. **Transporte indisponível**: a exceção (ou uma `InnerException`) implementa a interface marcadora [`ITransportUnavailableException`](../../TEC.Messaging/Transport/IMessagePublisher.cs). Então a mensagem, as falhas seguidas que a precederam (provavelmente o começo da mesma queda) e o resto do lote voltam como `Released` **sem contar tentativa**, e o relay pausa. Uma indisponibilidade longa não leva mensagens a `Dead`.

A marca é uma **interface** (não um atributo) porque é testada na instância, e fica no núcleo para que cada satélite a implemente nas próprias exceções. No RabbitMQ: [`RabbitMqUnavailableException`](../../TEC.Messaging.RabbitMQ/RabbitMqOptions.cs) (transporte desligado, sem conexão ou canal, conexão perdida, confirmação fora do prazo) e `RabbitMqCircuitOpenException` (circuito aberto). O cancelamento por desligamento ou fim do prazo da reserva também devolve a mensagem sem contar tentativa. Falhas, mortes e pausas são registradas com os eventos de log 4001 a 4006 e nas métricas `publish_failures` e `dead`.

**Revisão de 2026-10-29: só falha transitória leva a marca.** Cada transporte classifica a falha antes de marcá-la:

| Transporte | Marcada (não conta tentativa) | Sem marca (conta tentativa, pode virar `Dead`) |
|---|---|---|
| RabbitMQ | sem conexão ou canal, circuito aberto, conexão perdida, confirmação fora do prazo | `nack`, canal fechado com a conexão aberta, `basic.return` da publicação `mandatory` (`RabbitMqUnroutableException`) |
| Azure Service Bus | cliente impossível de criar (cofre, connection string), `ServiceBusException.IsTransient`, `ServiceBusy`, `ServiceTimeout`, `ServiceCommunicationProblem`, erro de rede, confirmação fora do prazo | mensagem grande demais, entidade inexistente ou desabilitada, `UnauthorizedAccessException` (que também descarta o cliente para reler o segredo), demais `ServiceBusException` não transitórias |
| AWS | cliente impossível de criar, HTTP 5xx, HTTP 429 e códigos de *throttling*, erro de rede, confirmação fora do prazo | fila ou tópico inexistente, `AccessDenied`, credencial recusada (que também descarta os clientes), parâmetro ou mensagem inválida |

As mensagens das exceções de indisponibilidade passam pelo `SensitiveDataSanitizer` antes de compor o texto.

## Consequências
### Positivas
- Uma mensagem defeituosa não atrasa as demais, e o histórico de erro fica na linha (`Attempts`, `LastError`, sanitizado, [ADR 0006](0006-sanitizacao-de-pii-e-credenciais-em-erros-logs-e-cabecalhos.md)).
- Uma queda longa do broker não mata mensagens nem exige reprocessamento manual.
- A pausa evita bater no broker (e no cofre) enquanto ele está fora.

### Neutras / Negativas
- Falhas seguidas **sem** a marca continuam contando tentativa: um publicador próprio que não implementa `ITransportUnavailableException` leva as mensagens a `Dead` numa queda longa, recuperáveis com `RequeueAsync`.
- Até a revisão de 2026-10-29, o Azure Service Bus e a AWS marcavam **toda** exceção do SDK como indisponibilidade, e uma mensagem recusada (tamanho, destino inexistente, permissão) nunca virava `Dead` e bloqueava o Outbox. Agora ela conta tentativa como no RabbitMQ. O preço: destino errado ou permissão negada por configuração levam as mensagens a `Dead` depois de `MaxAttempts`, e é preciso `RequeueAsync` depois de corrigir.
- Com `mandatory`, evento publicado no RabbitMQ sem nenhuma fila ligada também chega a `Dead`. Antes ele era confirmado e descartado em silêncio pelo broker.
- A ordem se perde entre lotes e tentativas (confirmado como premissa do desenho).
- Os limites (`MaxAttempts`, tetos de espera) são do operador: valores altos atrasam o diagnóstico de mensagens realmente defeituosas.
