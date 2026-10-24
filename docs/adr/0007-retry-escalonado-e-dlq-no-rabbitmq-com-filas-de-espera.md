# ADR 0007: Retry Escalonado e DLQ no RabbitMQ com Filas de Espera

## Status
Aprovado

## Data
2026-10-24 (registro retroativo de uma decisão já implementada na versão 0.0.1)

## Contexto
O consumidor precisa tentar de novo as mensagens que falham, com espera crescente, e mandar para uma fila de mensagens mortas (DLQ) as que esgotam as tentativas ou são inválidas. Segurar a mensagem em memória com `Thread.Sleep`/`Task.Delay` prende o canal e o prefetch, perde a espera se o processo cai e esgota threads. Um `NACK` com requeue reentrega na hora: com a dependência ainda fora, vira laço de reentregas que a fila quorum interrompe mandando a mensagem para a DLQ em segundos (problema M4 da [🔍 Revisão técnica](../revisao-tecnica.md)).

## Decisão
Implementar o retry e a DLQ **na topologia do broker**, sem reter o processamento em memória ([`Topology`](../../TEC.Messaging.RabbitMQ/Internal/Topology.cs), [`QueueDefinition`](../../TEC.Messaging.RabbitMQ/QueueDefinition.cs)):

- **Fila principal `quorum`** (replicada) com `x-delivery-limit` (`DeliveryLimit`, 50 por padrão) como rede de segurança contra laço de reentrega, DLX para a DLQ, `at-least-once` e `reject-publish` no overflow.
- **Filas de espera por atraso** `<fila>.retry.<atraso>`: `quorum`, `x-message-ttl` igual ao atraso e DLX de volta para a fila principal. `RetryDelays` (30 s, 2 min, 10 min, 30 min por padrão) define as esperas; a última se repete. Atrasos fracionários usam rótulo em milissegundos para nunca colidir nomes com TTLs diferentes.
- **DLQ `<fila>.dlq`**: `quorum`, sem TTL e sem limite de entregas, para a listagem do administrador poder devolver as mensagens.
- **Desfechos** ([`RabbitMqConsumer`](../../TEC.Messaging.RabbitMQ/Consumers/RabbitMqConsumer.cs)): `Completed` dá ack; `Retry` republica na fila de espera do próximo atraso (na `MaxAttempts`-ésima falha, na DLQ); `Reject` e mensagens ilegíveis vão para a DLQ; `Requeue` republica na primeira fila de espera **sem** consumir tentativa; exceção do handler é `Retry`. A republicação é confirmada antes do ack da mensagem original; se falhar, a mensagem volta à fila sem perda.
- **Estado da mensagem nos cabeçalhos**: `x-tec-attempt` (falhas registradas) e `x-tec-error` (último erro, sanitizado, [ADR 0006](0006-sanitizacao-de-pii-e-credenciais-em-erros-logs-e-cabecalhos.md)); `x-tec-requeued` marca o retorno pela administração.
- **Administração** ([`IDeadLetterAdministration`](../../TEC.Messaging/Administration/IDeadLetterAdministration.cs)): contar, listar sem remover, reprocessar (tentativas zeradas) e descartar, pelo protocolo AMQP, sem a API de gerenciamento.
- **Filas temporárias** (`Transient`): exclusivas e auto-delete, sem espera nem DLQ, para notificações em que perder um evento é aceitável.
- **Desligamento e pausa**: o consumidor drena os handlers em andamento até `DrainTimeout` e devolve o resto sem NACK; `Pause()`/`Resume()` cancelam e retomam o consumo sem requeue em laço.

## Consequências
### Positivas
- Nenhuma espera é feita em memória: o processo pode cair sem perder a contagem e o canal não fica preso.
- O broker garante a durabilidade (quorum) e o *dead-lettering* `at-least-once`.
- A DLQ preserva a mensagem e o motivo para análise e reprocessamento.

### Neutras / Negativas
- Mudar `RetryDelays` ou `DeliveryLimit` numa fila existente falha com `PRECONDITION_FAILED` (os argumentos da fila não mudam): é preciso apagar a fila ou trocar o argumento por uma *policy* do broker. O publicador, que declara as filas dos consumidores do processo, registra o aviso (evento 4212) e segue publicando.
- Há uma fila de espera por atraso distinto, por fila de negócio.
- O cabeçalho `x-tec-attempt` vem da mensagem e é confiado: um produtor com acesso à exchange pode mandar a mensagem direto para a DLQ (premissa de broker interno).
- `Requeue` não tem limite: com a dependência fora para sempre, a mensagem circula a cada `RetryDelays[0]` sem ir à DLQ.
