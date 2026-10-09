[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › Testes

# 🧪 Testes

> Como rodar unitários, integração (SQL Server e RabbitMQ), carga e benchmarks.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

| Categoria | Projeto | Onde roda | Conteúdo |
|---|---|---|---|
| *(sem)* | `TEC.Messaging.Tests` | PR, main, release (net10.0, net8.0, sem ICU) | Contratos, envelope, correlação, relay (Outbox em memória), interceptor (EF InMemory) |
| `Integracao` | `TEC.Messaging.Tests` | PR e main (SQL Server e RabbitMQ em container) | Reserva concorrente, conclusão, administração, Inbox; publicação, retry, DLQ |
| `Carga-CI` | `TEC.Messaging.LoadTests` | `performance.yml` | Relays concorrentes publicam cada mensagem uma vez |
| `Carga-Pesada` | `TEC.Messaging.LoadTests` | `performance.yml` | O mesmo com volume |

## 🚀 Uso

```bash
dotnet run --project TEC.Messaging.Tests -c Release -f net10.0 -- --treenode-filter "/*/*/*/*[Category!=Integracao]"

# Integração local (variáveis, ou user-secrets id tudoemcodigo-tec-testes, seção TecTestes)
export TEC_TESTES_MESSAGING_SQL_CONEXAO="Server=localhost,1433;Database=tec_testes;...;TrustServerCertificate=True"
export TEC_TESTES_MESSAGING_RABBITMQ_URI="amqp://usuario:senha@localhost:5672/tec-testes"
dotnet run --project TEC.Messaging.Tests -c Release -f net10.0 -- --treenode-filter "/*/*/*/*[Category=Integracao]"

dotnet run --project TEC.Messaging.Benchmarks -c Release -f net10.0
```

## ⚙️ Opções

| Variável | Uso |
|---|---|
| `TEC_TESTES_MESSAGING_SQL_CONEXAO` / `TecTestes:MessagingSqlConexao` | SQL Server da integração (cria o esquema `tecmsg_testes`) |
| `TEC_TESTES_MESSAGING_RABBITMQ_URI` / `TecTestes:MessagingRabbitUri` | RabbitMQ da integração (exchange e filas descartáveis por teste) |
| `TEC_CARGA_MENSAGENS` | Volume da carga |
| `TEC_CARGA_RELATORIOS` | Pasta dos relatórios `.md` |

## ❌ Erros

| Situação | Causa | O que fazer |
|---|---|---|
| Testes de integração pulados | Variáveis ausentes | Defina as conexões (o motivo aparece no relatório) |

## 🛡️ Segurança

> [!NOTE]
> No CI, senhas do SQL Server e do RabbitMQ são aleatórias por execução e mascaradas no log.

## ❓ Perguntas frequentes

<details>
<summary><b>Por que o interceptor é testado com EF InMemory?</b></summary>

A lógica do interceptor não depende do provedor; o SQL específico (reserva) é coberto pela integração com SQL Server.
</details>

---
⬅️ [Segurança](seguranca.md) · [📚 Índice](README.md) · [Desenvolvimento](desenvolvimento.md) ➡️
