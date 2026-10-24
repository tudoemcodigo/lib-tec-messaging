# 🐘 TEC.Messaging.PostgreSql

Outbox e Inbox transacionais do **TEC.Messaging** para **PostgreSQL** com Entity Framework Core.

## Funcionalidades

- **Outbox Transacional**: Grava mensagens de integração na mesma transação dos agregados de domínio.
- **Reserva concorrente de alto throughput**: Utiliza `SELECT ... FOR UPDATE SKIP LOCKED` para reservar lotes sem contenção ou bloqueio entre múltiplas instâncias concorrentes.
- **Inbox Idempotente**: Registro determinístico da mensagem processada na mesma transação do efeito, prevenindo execuções duplicadas (`unique_violation` 23505).
- **Limpeza e Administração**: Purga eficiente e reprocessamento de mensagens falhas/mortas.
