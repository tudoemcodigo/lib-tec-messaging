# ADR 0002: Reserva Concorrente do Outbox com Token e Relógio do Banco

## Status
Aprovado

## Data
2026-10-24 (registro retroativo de uma decisão já implementada na versão 0.0.1)

## Contexto
O [`OutboxRelay`](../../TEC.Messaging/Outbox/OutboxRelay.cs) pode rodar em vários processos ao mesmo tempo (mais instâncias do serviço, escala horizontal). Sem coordenação, dois relays publicariam a mesma mensagem, ou um travaria o outro esperando as mesmas linhas. Além disso, a publicação acontece fora da transação ([ADR 0001](0001-outbox-transacional-com-publicacao-fora-da-transacao.md)): a "posse" da mensagem precisa **sobreviver** à transação que a reservou, e precisa expirar se o relay morrer. A revisão técnica registrou dois problemas de uma primeira versão: reserva medida pelo relógio da aplicação (relógios divergentes duplicavam) e `RequeueAsync` desfazendo reservas vigentes ([🔍 Revisão técnica](../revisao-tecnica.md), B1 e B2).

## Decisão
Reservar lotes por **lease persistido**, com token e validade gravados na própria linha, em um único comando de banco:

- **Reserva** ([`IOutboxStore.LeaseAsync`](../../TEC.Messaging/Outbox/IOutboxStore.cs)): um `UPDATE` sobre os primeiros pendentes vencidos, na ordem de ocorrência, que grava `LeaseToken` e `LeasedUntil`.
  - [SQL Server](../../TEC.Messaging.SqlServer/Stores/SqlServerOutboxStore.cs): `TOP (@batchSize) ... WITH (UPDLOCK, READPAST, ROWLOCK)` e `OUTPUT inserted.*`.
  - [PostgreSQL](../../TEC.Messaging.PostgreSql/Stores/PostgreSqlOutboxStore.cs): `FOR UPDATE SKIP LOCKED` e `RETURNING target.*`.
- **Validade pelo relógio do banco**: `LeasedUntil` é gravado e comparado com `SYSUTCDATETIME()` (SQL Server) ou `NOW()` (PostgreSQL), único para todas as instâncias. Só o `NextAttemptAt` usa o relógio da aplicação.
- **Conclusão só com o token vigente**: [`CompleteAsync`](../../TEC.Messaging/Outbox/IOutboxStore.cs) atualiza com `WHERE LeaseToken = @token`; se outra reserva assumiu a mensagem, a conclusão é ignorada.
- **Prazo no relay**: a publicação de cada mensagem é cancelada ao fim de 80% de `LeaseDuration` (2 min por padrão); a mensagem e o resto do lote voltam (`Released`) sem contar tentativa, antes que outro relay possa assumi-las.
- **Administração respeita a reserva**: `RequeueAsync` ignora pendentes com reserva vigente (em publicação). No SQL Server a comparação usa o relógio do banco; no PostgreSQL, hoje, usa o da aplicação (ver [PostgreSQL](../postgresql.md#-concorrência-e-locks)).
- **Índice na ordem da reserva**: `IX_<tabela>_Pending`, filtrado em `Status = 0`, com chave `(OccurredAt, Id)` e `NextAttemptAt`, `LeasedUntil` e `Attempts` incluídos ([esquema](../sqlserver.md#️-esquema-e-índices)). Com outra ordem, o SQL Server leria e travaria todas as pendentes e as reservas simultâneas pulariam mensagens livres.

## Consequências
### Positivas
- Relays concorrentes pegam lotes diferentes sem esperar uns pelos outros, e não há travas longas no banco.
- A reserva expira sozinha se o relay cair: o lote volta a ficar disponível.
- Relógios de servidores divergentes não fazem outra instância assumir um lote em publicação.

### Neutras / Negativas
- Ainda é *pelo menos uma vez*: uma reserva que vence com a publicação em andamento, ou um crash entre publicar e concluir, duplica a mensagem.
- O índice e a ordem das suas colunas fazem parte do contrato de desempenho: alterá-los exige uma migration e cuidado (`HasFilter` usa o nome real da coluna `Status`).
- Cada banco tem sua implementação do mesmo contrato (código duplicado entre os dois satélites, [ADR 0008](0008-nucleo-agnostico-com-bancos-e-brokers-em-pacotes-satelites.md)).
