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

## Consequências
### Positivas
- Uma mensagem defeituosa não atrasa as demais, e o histórico de erro fica na linha (`Attempts`, `LastError`, sanitizado, [ADR 0006](0006-sanitizacao-de-pii-e-credenciais-em-erros-logs-e-cabecalhos.md)).
- Uma queda longa do broker não mata mensagens nem exige reprocessamento manual.
- A pausa evita bater no broker (e no cofre) enquanto ele está fora.

### Neutras / Negativas
- Falhas seguidas **sem** a marca continuam contando tentativa: um transporte que não implementa `ITransportUnavailableException` (hoje, Azure Service Bus e AWS) leva as mensagens a `Dead` numa queda longa, recuperáveis com `RequeueAsync`.
- A ordem se perde entre lotes e tentativas (confirmado como premissa do desenho).
- Os limites (`MaxAttempts`, tetos de espera) são do operador: valores altos atrasam o diagnóstico de mensagens realmente defeituosas.
