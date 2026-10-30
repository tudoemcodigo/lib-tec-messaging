[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › 🧪 Testes

# 🧪 Testes

> Como rodar unitários, integração (SQL Server e RabbitMQ), carga e benchmarks.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [🗂️ Suítes](#️-suítes)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

Os testes usam o [TUnit](https://tunit.dev) sobre o *Microsoft Testing Platform* (`global.json`): cada projeto de teste é um executável, rodado com `dotnet run`. A seleção é sempre por **categoria**.

| Categoria | Projeto | Onde roda | Conteúdo |
|---|---|---|---|
| *(sem)* | `TEC.Messaging.Tests` | PR, main, release (net10.0, também sem ICU) | Contratos, envelope, correlação, sanitizador, relay (Outbox em memória), interceptor (EF InMemory), modelo e SQL do PostgreSQL, opções e registro de RabbitMQ/Azure/AWS, classificação de falhas de publicação, `RequireTls`, rotação de credencial, leitura do envelope nos consumidores do Azure e da AWS, circuit breaker |
| `Integracao` | `TEC.Messaging.Tests` | PR e main (SQL Server e RabbitMQ em container) | Reserva concorrente, conclusão, administração, Inbox; publicação, retry, DLQ, requeue, drenagem |
| `Carga-CI` | `TEC.Messaging.LoadTests` | `performance.yml` | Quatro relays concorrentes publicam cada mensagem uma vez |
| `Carga-Pesada` | `TEC.Messaging.LoadTests` | `performance.yml` | O mesmo com 16 relays e volume (teste `Explicit`) |
| — | `TEC.Messaging.Benchmarks` | `performance.yml` (opcional) | Custo de montar e validar o envelope (BenchmarkDotNet) |

```mermaid
flowchart LR
    U["Unitários<br/>sem dependências externas"] --> I["Integração<br/>SQL Server + RabbitMQ reais"]
    I --> C["Carga<br/>relays concorrentes em memória"]
    C --> B["Benchmarks<br/>BenchmarkDotNet"]
```

Os testes de **PostgreSQL**, **Azure Service Bus** e **AWS** são unitários: modelo e SQL gerado, validação de opções e de definições, registro no container, classificação das falhas de publicação, recusa sem TLS, recriação do cliente com o segredo relido, leitura e validação do envelope nos consumidores e health check com o cofre fora. Não há integração com esses serviços: o laço dos consumidores (reenvio agendado, `Abandon` em assinatura, renovação de visibilidade, concorrência) e o `RequeueAsync` do PostgreSQL com o relógio do banco não têm teste automatizado.

Os testes de integração do RabbitMQ sobem o host em `Development`, onde `RequireTls` aceita `amqp://`.

---

## 🚀 Uso

```bash
# Unitários, como no CI
dotnet run --project TEC.Messaging.Tests -c Release -f net10.0 -- --treenode-filter "/*/*/*/*[Category!=Integracao]"

# Integração local (variáveis, ou user-secrets id tudoemcodigo-tec-testes, seção TecTestes)
export TEC_TESTES_MESSAGING_SQL_CONEXAO="Server=localhost,1433;Database=tec_testes;...;TrustServerCertificate=True"
export TEC_TESTES_MESSAGING_RABBITMQ_URI="amqp://usuario:senha@localhost:5672/tec-testes"
dotnet run --project TEC.Messaging.Tests -c Release -f net10.0 -- --treenode-filter "/*/*/*/*[Category=Integracao]"

# Carga rápida e pesada
dotnet run --project TEC.Messaging.LoadTests -c Release -f net10.0 -- --treenode-filter "/*/*/*/*[Category=Carga-CI]"
TEC_CARGA_MENSAGENS=200000 dotnet run --project TEC.Messaging.LoadTests -c Release -f net10.0 -- --treenode-filter "/*/*/*/*[Category=Carga-Pesada]"

# Benchmarks
dotnet run --project TEC.Messaging.Benchmarks -c Release -f net10.0
```

### Credenciais no user-secrets

O `UserSecretsId` é o mesmo em todos os projetos de teste dos componentes TEC, então um único comando vale para todos:

```bash
dotnet user-secrets set "TecTestes:MessagingSqlConexao" "Server=localhost,1433;Database=tec_testes;..." --id tudoemcodigo-tec-testes
dotnet user-secrets set "TecTestes:MessagingRabbitUri" "amqp://usuario:senha@localhost:5672/tec-testes" --id tudoemcodigo-tec-testes
```

Ou, sem user-secrets, num `appsettings.Local.json` ao lado do projeto de testes (seção `TecTestes`; ignorado pelo git e copiado para a saída).

### Serviços locais para a integração

Os mesmos que o CI sobe (`.github/scripts/integration-setup.sh`): SQL Server 2022 com o banco `tec_testes` e RabbitMQ 4.x com o vhost `tec-testes` (filas quorum). Qualquer instância equivalente serve.

---

## 🗂️ Suítes

### Unitários (`TEC.Messaging.Tests`)

| Área | Pasta | O que cobre |
|---|---|---|
| Contratos e envelope | `Core/ContractsTests` | Registro por nome, versão e tipo; varredura de assembly; `JsonTypeInfo`; JSON padrão do TEC.Core; nomes inválidos; duplicidade; fábrica (mapeamento, carimbo de contexto, rejeição de tipo não registrado e payload grande); `AddTecMessaging` idempotente; limites do envelope; `MessageContext`; UUIDv7 |
| Sanitização | `Core/SanitizerTests` | CPF, CNPJ, cartão, CVV, senhas e tokens (e-mail e telefone em `MessagingIssueRegressionTests`) |
| Relay do Outbox | `Outbox/OutboxRelayTests` | Ordem, falha isolada, `Dead`, pausa por falhas seguidas, reserva, backoff, opções inválidas, estatísticas, transporte indisponível (nunca conta tentativa), prazo da reserva, limpeza em lotes. Usa `InMemoryOutboxStore`, `FakePublisher` e relógio manual (`ManualTimeProvider`) |
| Interceptor e modelo (SQL Server) | `SqlServer/InterceptorTests`, `SqlServerUnitTests` | Eventos de domínio viram linhas do Outbox no mesmo `SaveChanges`; falha não duplica; `IOutbox`; esquema; filtros de índice com o nome real da coluna; métrica `enqueued` só depois do sucesso; limpeza do Inbox em lotes. Usa EF Core InMemory |
| Modelo (PostgreSQL) | `PostgreSql/PostgreSqlUnitTests` | Tabelas, esquema e nomes delimitados |
| Regressões de revisão | `MessagingIssueRegressionTests` | Interceptor do PostgreSQL resolve pelo container; publicadores do Azure e da AWS com `Enabled = false` lançam `ITransportUnavailableException`; sanitizador mascara e-mail, telefone e cartão; `DeterministicGuid` estável |
| RabbitMQ | `RabbitMq/RabbitMqUnitTests`, `RabbitMqResilienceTests` | Transporte desligado/indisponível, `Dispose` aguarda a publicação, requeue vazio, fila inexistente (404), nomes das filas de espera, formato do envelope e envelopes malformados, limite de payload, nome do span, circuit breaker (abre, meia-abertura, cancelamento), reconexão com jitter, opções inválidas |
| Azure Service Bus e AWS | `AzureServiceBus/…`, `Aws/…` | Validação das opções e registro no container |

### Integração (`Integracao`)

| Classe | O que cobre |
|---|---|
| `SqlServerStoreIntegrationTests` | Eventos gravados com o agregado; reservas concorrentes nunca se sobrepõem; conclusão só com o token vigente; descarte só de mortas e limpeza de publicadas; duplicidade do Inbox entre unidades de trabalho e só da própria chave; `RequeueAsync` não toca em mensagens em publicação; validade da reserva pelo relógio do banco; estatísticas sem as publicadas |
| `RabbitMqIntegrationTests` | Publicação e consumo com correlação e causa; retry pela fila de espera; `Reject` → DLQ → reprocessamento; fila inexistente; `Requeue` sem consumir tentativa nem ir à DLQ; desligamento drena o handler em andamento; publicador independe da topologia de um consumidor quebrado |

Cada teste de RabbitMQ usa exchange e filas descartáveis (`tec-testes.<sufixo>`), apagadas no fim. Os de SQL Server usam o esquema `tecmsg_testes` e o recriam/limpam a cada execução.

### Carga e benchmarks

`RelayLoadTests` usa o Outbox em memória e um publicador falso: valida que vários relays concorrentes publicam **cada mensagem uma única vez** e mede a vazão (relatório `.md` em `TEC_CARGA_RELATORIOS`). `EnvelopeBenchmarks` mede `FromDomainEvent` (mapeamento + JSON + UUIDv7) e `Validate`.

### No CI

`ci.yml` roda convenções, build e pacote, unitários em matriz, integração (SQL Server e RabbitMQ em container) e CodeQL, e só então publica a prévia na `main`. Carga e benchmarks só no `performance.yml` (manual). Detalhes em [⚙️ CI/CD](../.github/workflows/README.md).

---

## ⚙️ Opções

| Variável | Uso |
|---|---|
| `TEC_TESTES_MESSAGING_SQL_CONEXAO` / `TecTestes:MessagingSqlConexao` | SQL Server da integração (cria e usa o esquema `tecmsg_testes`) |
| `TEC_TESTES_MESSAGING_RABBITMQ_URI` / `TecTestes:MessagingRabbitUri` | RabbitMQ da integração (exchange e filas descartáveis por teste) |
| `TEC_CARGA_MENSAGENS` | Volume da carga (padrão 5.000 na `Carga-CI` e 200.000 na `Carga-Pesada`) |
| `TEC_CARGA_RELATORIOS` | Pasta dos relatórios `.md` |

A ordem de busca das conexões é: variável de ambiente, user-secrets, `appsettings.Local.json`.

## ❌ Erros

| Situação | Causa | O que fazer |
|---|---|---|
| Testes de integração pulados | Variáveis ausentes, ou globalização invariante (o `Microsoft.Data.SqlClient` exige ICU) | Defina as conexões e rode com ICU (o motivo aparece no relatório) |
| `Carga-Pesada` não roda | O teste é `Explicit` | Selecione a categoria explicitamente, como no `performance.yml` |
| Falha de integração do RabbitMQ por `PRECONDITION_FAILED` | Fila de uma execução anterior com argumentos diferentes | Apague as filas `tec-testes.*` do vhost de testes |
| `NU1301`/`401` ao compilar | Sem credencial do feed `tec-interno` | Veja [🛠️ Desenvolvimento](desenvolvimento.md) |

## 🛡️ Segurança

> [!NOTE]
> No CI, senhas do SQL Server e do RabbitMQ são aleatórias por execução e mascaradas no log.

> [!CAUTION]
> Os testes de integração do SQL Server **apagam e recriam** as tabelas do esquema `tecmsg_testes`. Aponte `TEC_TESTES_MESSAGING_SQL_CONEXAO` para um banco descartável, nunca para um banco de produção ou compartilhado.

## ❓ Perguntas frequentes

<details>
<summary><b>Por que o interceptor é testado com EF InMemory?</b></summary>

A lógica do interceptor não depende do provedor; o SQL específico (reserva) é coberto pela integração com SQL Server.
</details>

<details>
<summary><b>Como testar a minha aplicação sem broker?</b></summary>

Com `RabbitMqOptions.Enabled = false` os consumidores não sobem e a publicação falha como transporte indisponível (o Outbox retém as mensagens sem contar tentativa). Para testar o relay sem broker, registre um `IMessagePublisher` falso e um `IOutboxStore` em memória, como `FakePublisher` e `InMemoryOutboxStore` (`TEC.Messaging.Tests/Shared/TestSupport.cs`).
</details>

<details>
<summary><b>Por que não há integração para PostgreSQL, Azure e AWS?</b></summary>

Hoje só SQL Server e RabbitMQ têm serviços no CI. Os demais satélites têm testes unitários de modelo, opções e registro; a reserva com `SKIP LOCKED` e os transportes de nuvem ainda não são exercitados contra serviços reais ([🔍 Revisão técnica](revisao-tecnica.md#-complemento-da-revisão-documental--2026-10-24)).
</details>

---
⬅️ [🛡️ Segurança](seguranca.md) · [📚 Índice](README.md) · [🛠️ Desenvolvimento](desenvolvimento.md) ➡️
