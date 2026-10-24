# ADR 0004: Inbox Idempotente na Mesma Transação do Efeito

## Status
Aprovado

## Data
2026-10-24 (registro retroativo de uma decisão já implementada na versão 0.0.1)

## Contexto
O Outbox e os brokers entregam *pelo menos uma vez* ([ADR 0001](0001-outbox-transacional-com-publicacao-fora-da-transacao.md)): reentrega após falha, relay que cai entre publicar e concluir, reserva vencida. Um consumidor que acumula efeito (inserir, somar, enviar) processaria a mesma mensagem duas vezes. Verificar "já processei?" e gravar o efeito em transações separadas deixa uma janela em que duas entregas simultâneas passam pela verificação.

## Decisão
Registrar cada mensagem processada num **Inbox** gravado **na mesma transação do efeito**, com chave `(MessageId, Consumer)`:

- [`IInboxStore.TryBeginAsync(messageId, consumer)`](../../TEC.Messaging/Inbox/IInboxStore.cs) devolve `false` se a mensagem já foi processada pelo consumidor; senão adiciona o registro à unidade de trabalho atual (gravado no próximo `SaveChanges`, junto com o efeito) e devolve `true`.
- A **chave primária** composta é a trava final: duas entregas simultâneas passam pela verificação, mas uma falha no commit com chave duplicada. Na reentrega ela recebe `false`.
- [`IInboxStore.IsDuplicate(exception)`](../../TEC.Messaging/Inbox/IInboxStore.cs) permite ao consumidor reconhecer **essa** falha e encerrar como processada, sem nova tentativa nem log de erro. Só a chave do Inbox conta (SQL Server: erro 2627 com o nome da chave, [store](../../TEC.Messaging.SqlServer/Stores/SqlServerInboxStore.cs); PostgreSQL: `23505` na constraint, [store](../../TEC.Messaging.PostgreSql/Stores/PostgreSqlInboxStore.cs)); duplicidade em tabela da aplicação não vira "já processada". A implementação padrão da interface devolve sempre `false`.
- O nome do consumidor é estável, parte da chave e limitado a `InboxOptions.MaxConsumerLength` (100).
- Os registros são limpos periodicamente por `AddInboxCleanup`, com `Retention` (30 dias por padrão) maior que o tempo máximo de reentrega.

## Consequências
### Positivas
- Efeito e registro são atômicos: ou os dois acontecem, ou nenhum.
- Não depende do broker (funciona igual em RabbitMQ, Azure Service Bus e AWS).
- A corrida entre entregas simultâneas é resolvida pelo banco, sem trava distribuída.

### Neutras / Negativas
- Só cobre efeitos **no mesmo banco**: e-mail e chamadas a APIs externas exigem chave de idempotência própria no destino.
- O `IInboxStore` e o efeito precisam usar o **mesmo** `DbContext` (mesmo escopo).
- Renomear o consumidor faz mensagens antigas reentregues serem processadas de novo; `Retention` menor que o tempo de reentrega (ex.: DLQ reprocessada) também.
- A tabela cresce por consumidor e depende da limpeza em lotes (5.000 linhas por chamada, até 1 minuto por intervalo).
