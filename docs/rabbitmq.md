[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › 🐇 RabbitMQ

# 🐇 RabbitMQ

> Publicação confirmada, consumidores com nova tentativa escalonada, fila de mensagens mortas e pausa.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [🧩 Topologia e formato no fio](#-topologia-e-formato-no-fio)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

```mermaid
flowchart LR
    X["exchange topic"] -->|routing key| Q["fila (quorum)"]
    Q --> C["RabbitMqConsumer"]
    C -- Retry --> W["fila.retry.30s / 2m / 10m / 30m<br/>(TTL)"] -->|expira| Q
    C -- "Reject ou tentativas esgotadas" --> D["fila.dlq"]
    C -- Requeue --> W
    Q -- "x-delivery-limit" --> D
```

O pacote `TEC.Messaging.RabbitMQ` (RabbitMQ.Client 7, broker 3.13+ ou 4.x) entrega:

| Peça | Papel |
|---|---|
| `RabbitMqPublisher` (`IMessagePublisher`) | Publicação `mandatory` com *publisher confirms* e mensagens persistentes, um canal compartilhado serializado por trava |
| `RabbitMqConsumer` | Base dos consumidores: topologia, prefetch, ack manual depois do handler, retry escalonado, DLQ, pausa/retomada, drenagem no desligamento |
| `RabbitMqDeadLetterAdministration` (`IDeadLetterAdministration`) | Contagem, listagem, reprocessamento e descarte da DLQ pelo protocolo AMQP |
| `DeadLetterMonitor` | Conta as DLQs dos consumidores duráveis para a métrica `tec.messaging.dlq.messages` |
| `RabbitMqConnection` | Conexão única do processo, URI lida do TEC.Vault, circuit breaker de criação |
| `AddTecRabbitMq()` | Health check da conexão |

| `ConsumeResult` | Efeito | Conta tentativa? |
|---|---|:---:|
| `Completed` | ack | — |
| `Retry(motivo)` | fila de espera do próximo atraso; na `MaxAttempts`-ésima falha, DLQ | ✅ |
| `Reject(motivo)` | DLQ | — |
| `Requeue(motivo)` | Volta depois da primeira espera de `RetryDelays` (fila de espera), sem laço nem DLQ; em fila temporária, NACK com requeue. Para indisponibilidade longa, combine com `Pause()` | ❌ |
| Exceção | Como `Retry` (o motivo é sanitizado) | ✅ |

O ack só acontece depois do handler (e do commit dele). Se o processo cai, o broker reentrega. No desligamento o consumidor
para de processar novas entregas, espera os handlers em andamento até `DrainTimeout` e só então os cancela; as mensagens
não processadas voltam à fila com o fechamento do canal, sem NACK. `Prefetch` define o teto de mensagens antecipadas pelo
canal e `ConcurrencyLimit` controla o número de mensagens processadas simultaneamente em paralelo pelo consumidor (1 a 200).
Se o handler for cancelado no desligamento (depois da drenagem), a mensagem também volta à fila sem ack nem NACK.

Corrida do Inbox (duas entregas da mesma mensagem): trate a chave duplicada com
`catch (DbUpdateException ex) when (inbox.IsDuplicate(ex)) { return ConsumeResult.Completed; }`.

Mensagem ilegível (JSON inválido, sem `messageId` ou `type`, payload acima de `MaxPayloadLength`) vai direto para a DLQ com o motivo, sem exceção no consumidor. O desenho do retry e da DLQ está no [ADR 0007](adr/0007-retry-escalonado-e-dlq-no-rabbitmq-com-filas-de-espera.md).

---

## 🚀 Uso

```csharp
services.AddTecVault(...);                                   // segredo "rabbitmq" = amqps://usuario:senha@host/vhost
services.AddTecMessaging()
    .UseRabbitMq(o => o.RoutingKeyPrefixToRemove = "vendas.", builder.Configuration.GetSection(RabbitMqOptions.SectionName))
    .AddConsumer<Faturamento>()
    .AddOutboxRelay();
services.AddHealthChecks().AddTecRabbitMq();
```

`UseRabbitMq(configure, configuration)`: a seção de configuração é aplicada primeiro e o código depois. `AddConsumer<T>()` é idempotente por tipo e registra o consumidor como **singleton** (para pausar e retomar de fora) e como serviço em segundo plano. `AddTecRabbitMq(name = "rabbitmq", tags = ["ready"])`.

```csharp
public sealed class Faturamento(RabbitMqConsumerDependencies deps) : RabbitMqConsumer(deps)
{
    protected override QueueDefinition Queue { get; } = new("financeiro.faturamento", ["pedido-aprovado", "pedido-cancelado"])
    {
        Prefetch = 5,
        MaxAttempts = 6,
        RetryDelays = [TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(10)],
    };

    protected override Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken ct) => ...;
}
```

`ReceivedMessage` traz o `Envelope`, as falhas anteriores (`PreviousFailures`, cabeçalho `x-tec-attempt`) e se o broker reentregou (`Redelivered`). `ConsumeResult.Retry(reason, attempt)` permite informar a tentativa. A routing key de publicação é o `Type` com o prefixo `RoutingKeyPrefixToRemove` removido (`vendas.pedido-aprovado` → `pedido-aprovado`). Sem o prefixo configurado, a ligação do consumidor teria de ser `vendas.pedido-aprovado`.

### Publicação `mandatory`

A publicação usa `mandatory: true`. Se nenhuma fila está ligada à exchange com a routing key da mensagem (binding esquecido, prefixo errado, consumidor que ainda não declarou a fila), o broker devolve a mensagem (`basic.return`) e o publicador lança `RabbitMqUnroutableException`. Ela **não** é `ITransportUnavailableException`: o Outbox conta a tentativa e, esgotadas as tentativas, a mensagem vira `Dead`, em vez de ser confirmada e descartada em silêncio. O canal continua em uso depois do retorno. Evento sem nenhum interessado precisa de pelo menos uma fila ligada (mesmo que só para auditoria) ou vai acumular mortas.

Pausa (ex.: circuit breaker de uma dependência): resolva o consumidor (singleton) e chame `Pause()`/`Resume()`; `IsPaused` informa o estado. O consumo é cancelado no broker (`basic.cancel`) e as mensagens ficam na fila, sem requeue em laço.

### Resiliência da conexão

- **Circuit breaker da conexão** (`Polly.Core`, `RabbitMqOptions.CircuitBreaker`, ligado por padrão): a conexão é única no
  processo e serve publicador, consumidores, monitor de DLQ e health check. Com o broker (ou o cofre) fora do ar, depois de
  falhas suficientes o circuito abre e quem pede a conexão recebe `RabbitMqCircuitOpenException` na hora, sem tocar no cofre
  nem no broker: acaba a tempestade de reconexões. Depois de `BreakDuration`, uma tentativa de teste decide se fecha. O
  Outbox mantém as mensagens e tenta de novo depois, sem contar tentativa (`RabbitMqCircuitOpenException` é `ITransportUnavailableException`).
- **Sem recuperação automática do cliente**: o consumidor detecta o canal fechado e pede de novo, o que recria a conexão e relê
  o segredo (acompanha a rotação de senha).
- **Reconexão do consumidor** com espera exponencial e variação de ±20% (`ReconnectDelay` → `MaxReconnectDelay`), zerada
  quando o consumo é retomado.
- **Health check** com o circuito aberto responde `Unhealthy` na hora, sem nova tentativa. Fora disso espera no máximo 5 s,
  sem cancelar a criação da conexão (o desfecho real é contado pelo circuito). Com `Enabled = false` responde `Healthy`.
- Na tentativa de teste (meia-abertura), cancelamento conta como falha: o circuito só fecha com o broker respondendo.
- O retry das **mensagens** continua sendo do broker (filas de espera e DLQ): o processamento nunca é repetido em memória.

### Administração da DLQ

`IDeadLetterAdministration` (independente do broker, resolvida do container):

| Método | Efeito |
|---|---|
| `CountAsync(queue)` | Quantidade de mensagens na DLQ da fila |
| `ListAsync(queue, limit)` | Lê até `limit` mensagens (1 a 500) sem removê-las |
| `RequeueAsync(queue, messageIds, handler)` | Devolve à fila principal com as tentativas zeradas e o cabeçalho `x-tec-requeued`. `null` = todas; 1 a 1000 ids; lista vazia = erro `IDS_INVALIDOS`. Um `handler` opcional pode tratar a mensagem por conta própria: retornando `true`, ela só sai da DLQ |
| `DiscardAsync(queue, messageIds)` | Remove definitivamente (1 a 1000 ids) |

Só a fila inexistente (404) vira `FILA_NAO_ENCONTRADA`; outras recusas (ex.: 403) sobem como exceção. Nome de fila inválido devolve `FILA_INVALIDA`; limite fora de 1 a 500, `LIMITE_INVALIDO`. A listagem lê com `basic.get` sem ack e devolve com NACK; o reprocessamento só dá ack depois de republicar com confirmação na fila principal.

---

## 🧩 Topologia e formato no fio

**Exchange**: `topic`, durável, declarada pelo publicador e pelos consumidores. **Fila durável** (`QueueDefinition.Transient = false`):

| Fila | Argumentos | Função |
|---|---|---|
| `<fila>` | `quorum`, `x-delivery-limit` = `DeliveryLimit`, DLX `""` → `<fila>.dlq`, `at-least-once`, `x-overflow = reject-publish` | Fila principal, ligada à exchange pelas routing keys (aceitam `*` e `#`) |
| `<fila>.dlq` | `quorum`, `x-delivery-limit = -1` | Mensagens mortas (sem TTL nem limite de entregas, para a listagem devolvê-las) |
| `<fila>.retry.<atraso>` | `quorum`, `x-message-ttl` = atraso, DLX `""` → `<fila>`, `at-least-once` | Espera: ao expirar o TTL a mensagem volta à principal |

O rótulo do atraso é `30s`, `2m` etc. (segundos ou minutos inteiros); com fração de segundo vai em milissegundos (`1500ms`), para atrasos diferentes nunca darem o mesmo nome. **Fila temporária** (`Transient = true`): exclusiva, não durável e `auto-delete`, sem espera nem DLQ (`Retry` e `Reject` descartam com log 4209; `Requeue` volta na hora).

O publicador declara a exchange e, antes da primeira publicação, as filas duráveis dos consumidores **do mesmo processo** (um canal por fila): evento publicado antes de a fila existir não teria para onde ir. Uma declaração recusada (`PRECONDITION_FAILED`) só afeta aquele consumidor: o publicador registra o aviso 4212 e segue.

**Formato no fio**: corpo JSON com o envelope inteiro (`messageId`, `type`, `version`, `occurredAt`, `correlationId`, `causationId`, `traceparent` e o `payload` embutido como objeto), mensagem persistente, `application/json`, e os metadados repetidos nas propriedades AMQP (`message-id`, `type`, `correlation-id`, `timestamp`) e nos cabeçalhos:

| Cabeçalho | Conteúdo |
|---|---|
| `version` · `causation-id` · `traceparent` | Metadados do envelope |
| `x-tec-attempt` | Falhas já registradas da mensagem (retry com backoff e DLQ) |
| `x-tec-error` | Último erro, sanitizado e truncado em 1000 caracteres (fila de espera ou DLQ) |
| `x-tec-requeued` | `true` quando o administrador devolveu a mensagem da DLQ |

O corpo lido aceita até 16 MiB e JSON com profundidade máxima de 64; payloads acima de `MessagingOptions.MaxPayloadLength` são rejeitados para a DLQ.

---

## ⚙️ Opções

| Opção (`RabbitMqOptions`, seção `Messaging:RabbitMQ`) | Padrão | Descrição |
|---|---|---|
| `Enabled` | `true` | `false`: consumidores não sobem; a publicação falha com `RabbitMqUnavailableException` e o Outbox retém as mensagens sem contar tentativa |
| `DrainTimeout` | 15 s | Espera pelos handlers em andamento no desligamento (0 a 5 min). Mantenha a soma abaixo do `HostOptions.ShutdownTimeout` (30 s por padrão) |
| `ConnectionSecretName` | `rabbitmq` | Segredo do TEC.Vault com a URI AMQP (`amqps://`; `amqp://` só sem exigência de TLS) |
| `RequireTls` | `null` | `null` = exige TLS fora de Development (`IHostEnvironment.IsDevelopment()`). Exigindo, uma URI `amqp://` no segredo é recusada ao abrir a conexão, sem tocar no broker; `false` libera `amqp://` em qualquer ambiente |
| `Exchange` | (obrigatória) | Exchange topic: minúsculas, dígitos, `.`, `-`, `_`, até 200 caracteres |
| `RoutingKeyPrefixToRemove` | `""` | Prefixo removido do tipo para formar a routing key (até 100 caracteres) |
| `PublishTimeout` | 10 s | Espera pela confirmação (1 s a 5 min) |
| `Heartbeat` | 30 s | Heartbeat da conexão (5 s a 5 min) |
| `DeadLetterMonitorInterval` | 1 min | Contagem das DLQs para a métrica (0 a 1 h; 0 desliga) |
| `ReconnectDelay` | 1 s | Espera inicial para reabrir o consumo depois de uma queda (100 ms a 1 min); dobra a cada falha seguida |
| `MaxReconnectDelay` | 1 min | Maior espera entre tentativas de reabrir o consumo (1 s a 10 min, não menor que `ReconnectDelay`) |
| `CircuitBreaker:Enabled` | `true` | Circuit breaker da criação da conexão |
| `CircuitBreaker:FailureRatio` | 0,5 | Proporção de falhas que abre o circuito (> 0 e ≤ 1) |
| `CircuitBreaker:MinimumThroughput` | 5 | Mínimo de tentativas de conexão na janela (2 a 10.000) |
| `CircuitBreaker:SamplingDuration` | 30 s | Janela de amostragem (0,5 s a 1 h) |
| `CircuitBreaker:BreakDuration` | 30 s | Tempo aberto antes da tentativa de teste (0,5 s a 1 h) |

| `QueueDefinition` | Padrão | Descrição |
|---|---|---|
| `Name` | (obrigatório) | Minúsculas, dígitos, `.`, `-`, `_`, até 200; não pode terminar em `.dlq` |
| `RoutingKeys` | (obrigatório) | Até 100 routing keys, cada uma com até 255 caracteres |
| `Prefetch` | 10 | Mensagens sem ack ao mesmo tempo no canal (1 a 1000) |
| `ConcurrencyLimit` | 1 | Número de mensagens processadas em paralelo pelo consumidor (1 a 200) |
| `MaxAttempts` | 5 | Falhas até a DLQ (1 a 100) |
| `RetryDelays` | 30 s, 2 min, 10 min, 30 min | Esperas (1 a 10 itens, de 1 s a 1 dia); a última se repete |
| `DeliveryLimit` | 50 | `x-delivery-limit` da fila quorum (1 a 1000) |
| `Transient` | `false` | Fila exclusiva auto-delete, sem retry nem DLQ |

Opções inválidas falham na subida (`OptionsValidationException`); uma `QueueDefinition` inválida falha ao iniciar o consumidor (`InvalidOperationException`).

## ❌ Erros

| Situação | Causa | O que fazer |
|---|---|---|
| `InvalidOperationException: Segredo 'rabbitmq' ... indisponível` | Segredo ausente no cofre | Crie o segredo com a URI |
| `InvalidOperationException: O segredo ... não contém uma URI amqp:// ou amqps:// válida` | Conteúdo do segredo incorreto | Corrija a URI |
| `InvalidOperationException: ... URI amqp:// (sem TLS), recusada ...` (dentro de `RabbitMqUnavailableException` na publicação) | `amqp://` com `RequireTls` exigido (padrão fora de Development) | Use `amqps://`; só com o tráfego protegido de outra forma, `RequireTls = false` |
| `RabbitMqUnroutableException` | Nenhuma fila ligada para a routing key (publicação `mandatory`) | Crie a ligação ou ajuste `RoutingKeyPrefixToRemove`; as mensagens que morreram voltam com `RequeueAsync` do Outbox |
| `PRECONDITION_FAILED` ao declarar fila | Fila existente com argumentos diferentes (ex.: clássica, outro `DeliveryLimit`, outro `RetryDelays`) | Apague a fila antiga ou use outro nome. Só o consumidor afetado deixa de subir: o publicador registra o aviso 4212 e continua publicando |
| Health check `Unhealthy` | Broker fora ou credencial inválida | Verifique rede e segredo |
| `RabbitMqCircuitOpenException` · health check "circuito aberto" | Falhas repetidas ao conectar (broker, rede, credencial ou cofre); log 4401 | Corrija a causa; o circuito testa de novo depois de `BreakDuration` (logs 4402 e 4403) |
| `RabbitMqUnavailableException` | Transporte desligado, sem conexão ou canal, conexão perdida ou confirmação fora do prazo | O Outbox mantém a mensagem sem contar tentativa; verifique o broker (bloqueio por alarme de memória ou disco) |
| Mensagens na DLQ | `Reject`, `MaxAttempts` esgotado, mensagem ilegível ou `x-delivery-limit` | Veja o cabeçalho `x-tec-error` (`ListAsync`), corrija a causa e `RequeueAsync` |

Eventos de log: 4201 consumindo · 4202 pausado · 4203 falha no canal · 4204 falha no handler · 4205 retry · 4206 DLQ · 4207 mensagem inválida · 4208 falha ao confirmar · 4209 descartada (fila temporária) · 4210 transporte desligado · 4211 circuito aberto (nível Debug) · 4212 declaração recusada · 4213 devolvida sem consumir tentativa · 4214 drenagem expirada · 4301 falha ao contar a DLQ · 4401–4404 circuito. No desligamento, uma mensagem que ainda esperava vaga de `ConcurrencyLimit` deixa de contar como "em processamento" na hora, sem segurar a drenagem até o `DrainTimeout`.

## 🛡️ Segurança

> [!WARNING]
> Fora de Development, `amqp://` é recusado (`RequireTls`); use `amqps://`. A URI (com senha) vem só do cofre e nunca é registrada em log, nem na mensagem de recusa.

> [!WARNING]
> Conceda ao usuário do broker só o vhost e as permissões necessárias (`configure` só para as filas do serviço).

> [!NOTE]
> O cabeçalho `x-tec-attempt` vem da mensagem e é confiado: um produtor com acesso à exchange pode mandar uma mensagem direto para a DLQ. A premissa é um broker interno com produtores confiáveis ([🔍 Revisão técnica](revisao-tecnica.md), I3).

## ❓ Perguntas frequentes

<details>
<summary><b>Por que filas quorum?</b></summary>

Replicadas, com `x-delivery-limit` (proteção contra laço de reentrega) e dead-lettering `at-least-once`.
</details>

<details>
<summary><b>Por que o Prefetch não paraleliza o processamento?</b></summary>

`Prefetch` só define quantas mensagens o broker antecipa ao canal. O paralelismo de processamento é o `ConcurrencyLimit` (semáforo no consumidor).
</details>

<details>
<summary><b>Posso rodar vários consumidores da mesma fila?</b></summary>

Sim: várias instâncias do serviço consomem a mesma fila quorum e o broker distribui as mensagens. A ordem não é garantida; use o [Inbox](inbox.md).
</details>

---
⬅️ [🐘 PostgreSQL](postgresql.md) · [📚 Índice](README.md) · [☁️ Azure Service Bus](azureservicebus.md) ➡️
