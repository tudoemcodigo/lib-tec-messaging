[🏠 TEC.Messaging](../README.md) › Samples

# 🧰 Samples

> Exemplos executáveis (nunca viram pacote).

| Projeto | O que mostra | Requisitos |
|---|---|---|
| `TEC.Messaging.SampleWorker` | Agregado com evento de domínio → Outbox no SQL Server (mesma transação) → relay → RabbitMQ → consumidor idempotente com Inbox | SQL Server e RabbitMQ |

```bash
dotnet user-secrets set "TecTestes:MessagingSqlConexao" "Server=localhost,1433;Database=tec_exemplo;..." --id tudoemcodigo-tec-testes
dotnet user-secrets set "TecTestes:MessagingRabbitUri" "amqp://usuario:senha@localhost:5672/tec-testes" --id tudoemcodigo-tec-testes
dotnet run --project samples/TEC.Messaging.SampleWorker
```

> [!WARNING]
> O exemplo usa o TEC.Vault em memória e `EnsureCreated` por simplicidade. Em produção, o segredo vem de um cofre real e o
> esquema, de migrations.
