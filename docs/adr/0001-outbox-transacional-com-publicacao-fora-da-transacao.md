# ADR 0001: Outbox Transacional com Publicação Fora da Transação

## Status
Aprovado

## Data
2026-10-24 (registro retroativo de uma decisão já implementada na versão 0.0.1)

## Contexto
Um caso de uso que altera o banco e também avisa outros serviços tem duas gravações que não participam da mesma transação (*dual write*): o banco da aplicação e o broker. Publicar no broker dentro do caso de uso produz dois defeitos opostos:
1. **Evento sem fato**: o broker confirma, mas o `COMMIT` do banco falha; outros serviços reagem a algo que não aconteceu.
2. **Fato sem evento**: o `COMMIT` acontece, mas a publicação falha ou o processo cai; o evento se perde para sempre.

Publicar *dentro* de uma transação aberta também prende conexões e travas do banco enquanto se espera a confirmação do broker. O próprio contrato [`IMessagePublisher`](../../TEC.Messaging/Transport/IMessagePublisher.cs) registra a regra: o publicador é usado pelo relay do Outbox, "nunca diretamente pelos casos de uso".

## Decisão
Adotar o padrão **Outbox transacional**: o evento de integração é gravado numa tabela do próprio banco da aplicação, na **mesma transação** do agregado, e publicado depois por um serviço em segundo plano, **fora de qualquer transação**.

- **Gravação**: o interceptor de `SaveChanges` ([SQL Server](../../TEC.Messaging.SqlServer/Interception/TecMessagingSaveChangesInterceptor.cs), [PostgreSQL](../../TEC.Messaging.PostgreSql/Interception/TecMessagingPostgreSqlSaveChangesInterceptor.cs)) coleta os eventos de domínio dos agregados rastreados (`IHasDomainEvents`), aplica os mapeadores ([ADR 0005](0005-contratos-de-integracao-versionados-separados-do-dominio.md)) e adiciona as linhas `OutboxMessage` ao contexto. Para eventos sem agregado há o [`IOutbox`](../../TEC.Messaging/Outbox/IOutboxStore.cs).
- **Consistência do agregado**: os eventos só saem do agregado depois de um `SaveChanges` bem-sucedido; se ele falha, as linhas adicionadas (e as dos observadores) são retiradas do rastreamento e a nova tentativa gera tudo de novo, sem duplicar. A métrica `tec.messaging.outbox.enqueued` também só conta depois do sucesso.
- **Publicação**: o [`OutboxRelay`](../../TEC.Messaging/Outbox/OutboxRelay.cs) repete um ciclo curto: reserva um lote (transação curta, [ADR 0002](0002-reserva-concorrente-do-outbox-com-token-e-relogio-do-banco.md)), publica cada mensagem fora de transação aguardando a confirmação do broker, e grava os desfechos em outra transação curta.
- **Garantia**: entrega *pelo menos uma vez*. A ordem é a de ocorrência dentro de um lote, mas **não** é garantida entre lotes, relays e novas tentativas. A idempotência é do consumidor ([ADR 0004](0004-inbox-idempotente-na-mesma-transacao-do-efeito.md)).
- **Abstração**: o relay depende só de [`IOutboxStore`](../../TEC.Messaging/Outbox/IOutboxStore.cs) e [`IMessagePublisher`](../../TEC.Messaging/Transport/IMessagePublisher.cs) ([ADR 0008](0008-nucleo-agnostico-com-bancos-e-brokers-em-pacotes-satelites.md)).

## Consequências
### Positivas
- Nenhum evento se perde por falha de commit ou de broker, e nenhum evento sai de uma transação que não foi confirmada.
- O caso de uso não espera o broker: o `SaveChanges` só grava no banco.
- Transações do banco curtas; a espera pelo broker não segura travas.

### Neutras / Negativas
- Há latência entre o commit e a publicação (até `PollInterval`, 1 s por padrão).
- A entrega pode duplicar (relay cai entre publicar e concluir; reserva vencida): consumidores precisam ser idempotentes.
- Quem escreve na tabela do Outbox publica no broker: a escrita deve ser restrita ao login da aplicação.
- A tabela cresce e exige limpeza (`PublishedRetention`, `CleanupInterval`) e uma migration ao adotar o pacote.
- Com transação explícita em volta do `SaveChanges` (ex.: unidade de trabalho do TEC.ORM), os eventos saem do agregado antes do `COMMIT`: se o commit falhar, quem controla a transação precisa descartar o rastreador (registrado em [🔍 Revisão técnica](../revisao-tecnica.md), B13).
