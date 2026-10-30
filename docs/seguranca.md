[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › 🛡️ Segurança

# 🛡️ Segurança

> O que o componente garante e o que é responsabilidade da aplicação.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [🧱 Ameaças e controles](#-ameaças-e-controles)
- [⚠️ Riscos residuais](#️-riscos-residuais)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança-1)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

| Ameaça | Controle |
|---|---|
| Vazamento de dados sensíveis (LGPD / PCI-DSS) | Sanitização de PII no erro gravado em `LastError` do Outbox, no cabeçalho `x-tec-error` e no motivo da DLQ, e nos resumos de erro do relay (CPF, CNPJ, cartões de crédito, CVV/CVC, e-mails, telefones, senhas, tokens Bearer em formato JWT e connection strings) |
| Vazamento de credencial do broker | URI/connection string só pelo TEC.Vault (`ConnectionSecretName`); nunca em configuração nem em log |
| Credencial rotacionada ou vencida | RabbitMQ recria a conexão; Azure e AWS descartam o cliente quando a credencial é recusada e releem o segredo (single-flight) |
| Tráfego sem criptografia | `RequireTls` nos três transportes: fora de Development, `amqp://`, o emulador do Service Bus e URLs `http://` da AWS são recusados |
| Mensagem gigante (DoS) | `MaxPayloadLength` (padrão 256 KiB, até 4 MiB) na gravação e no consumo dos três transportes; o limite que protege a memória do consumidor é o do broker (16 MiB no RabbitMQ 4, o da entidade no Azure, 256 KiB no SQS); JSON com profundidade máxima de 64 |
| Entrada não confiável na correlação | `MessageContext` descarta valores longos ou com caracteres de controle |
| Mensagem forjada ou fora do contrato | Os três consumidores validam o envelope (`MessageEnvelope.Validate`) e rejeitam antes do handler; `MessageId` vazio nunca vira `Guid.Empty` |
| Texto hostil no sanitizador (ReDoS) | Entrada truncada em 4.096 caracteres antes das expressões, padrões com quantificadores limitados e tempo máximo de 250 ms por expressão; estourado, o texto inteiro vira `***REDACTED***` |
| SQL injection | Nomes de tabela/coluna vêm do modelo e são delimitados; valores sempre parametrizados |
| Desserialização insegura | `System.Text.Json` com tipos registrados (sem polimorfismo aberto) |
| Cardinalidade de telemetria por produtor malicioso | O `Type` só vira nome de span se tiver o formato de contrato |
| Publicação fora da transação perdendo eventos | Outbox transacional ([ADR 0001](adr/0001-outbox-transacional-com-publicacao-fora-da-transacao.md)) |

## 🚀 Uso

```bash
# Segredo do broker no cofre (exemplo com HashiCorp Vault)
vault kv put secret/minha-app rabbitmq="amqps://app:<senha>@rabbit.interno:5671/producao"
```

Os segredos esperados (nomes padrão, configuráveis em `ConnectionSecretName`):

| Transporte | Segredo | Conteúdo |
|---|---|---|
| RabbitMQ | `rabbitmq` | URI `amqps://` completa, com usuário, senha e vhost (`amqp://` só sem exigência de TLS) |
| Azure Service Bus | `azure-servicebus` | Connection string do namespace |
| AWS SQS/SNS | `aws-messaging` | `ACCESS_KEY_ID:SECRET_ACCESS_KEY` ou `ACCESS_KEY_ID:SECRET_ACCESS_KEY:SESSION_TOKEN` (opcional só com `AwsMessagingOptions.UseAmbientCredentials = true`, que passa a valer a cadeia padrão da AWS; sem ela, a falta do segredo é erro) |

---

## 🧱 Ameaças e controles

### Sanitização de PII (LGPD)

O `SensitiveDataSanitizer` (interno do núcleo, compilado com `[GeneratedRegex]`: sem custo de construção das expressões em tempo de execução e compatível com Native AOT) primeiro trunca o texto em 4.096 caracteres, recuando até o último separador dos 64 finais para não deixar um documento ou segredo cortado ao meio (marca ` …[truncado]`). Depois mascara, nesta ordem:

| Padrão | Substituição |
|---|---|
| Token Bearer em formato JWT (`Bearer a.b.c`) | `Bearer ***REDACTED***` |
| Chaves `password`, `senha`, `pwd`, `secret`, `token`, `apikey`, `api_key`, `client_secret`, `access_token` seguidas de `:` ou `=` e valor | `chave:***REDACTED***` |
| Connection string: `pwd`, `password`, `user id`, `uid`, `secret`, `accountkey`, `sharedaccesskey` seguidos de `=` | `chave=***REDACTED***` |
| Cartão de crédito (13 a 19 dígitos, com ou sem espaço/hífen) | `***REDACTED***` |
| CPF (11 dígitos, com ou sem pontuação) e CNPJ numérico | `***REDACTED***` |
| `cvv`/`cvc` com 3 ou 4 dígitos | `cvv:***REDACTED***` |
| Endereço de e-mail (parte local até 64 caracteres, até 10 rótulos de domínio) | `***REDACTED***` |
| Telefone com estrutura de telefone: DDD entre parênteses (`(11) 98765-4321`), prefixo `+55`, ou grupos separados por hífen (`98765-4321`, `11 98765-4321`). Oito dígitos seguidos, sem essa estrutura (pedido, protocolo), não são mascarados | `***REDACTED***` |

Onde é aplicado:

| Local | Dado sanitizado |
|---|---|
| `LastError` do Outbox e logs do relay | Texto `Tipo: mensagem` da exceção de publicação (truncado em 1000 caracteres) |
| Cabeçalho AMQP `x-tec-error` (fila de espera e DLQ do RabbitMQ) | Motivo da falha (truncado em 1000 caracteres) |
| DLQ do Azure Service Bus | Motivo e descrição enviados ao `DeadLetterMessageAsync`; propriedade `tec-error` do reenvio agendado |
| `ConsumeResult.Retry` por exceção no handler (RabbitMQ, Azure, AWS) | Motivo derivado da exceção |
| Exceções de indisponibilidade do Azure e da AWS | Mensagem da causa (o SDK pode repetir nomes de entidade ou trechos da requisição) |
| Logs dos consumidores do Azure e da AWS | Erros do laço, do processador, da confirmação e da renovação |

### Limites de tamanho

| Limite | Valor | Onde vale |
|---|---|---|
| `MessagingOptions.MaxPayloadLength` | 256 KiB padrão; 1 KiB a 4 MiB | Gravação no Outbox (`InvalidOperationException`) e consumo dos três transportes (mensagem rejeitada antes do handler). O teto de 4 MiB garante que o envelope (até 3 bytes por caractere em UTF-8, mais metadados) caiba no `max_message_size` padrão do RabbitMQ |
| Corpo AMQP | 16 MiB | Só evita o parse; o limite real é o do broker |
| Profundidade do JSON do envelope | 64 | Leitura do envelope no RabbitMQ |
| `Type` · `CorrelationId` · `CausationId` · `TraceParent` | 200 · 128 · 128 · 128 caracteres | `MessageEnvelope.Validate` (gravação e consumo no Azure e na AWS) e leitura no RabbitMQ |
| Texto analisado pelo sanitizador | 4.096 caracteres; 250 ms por expressão | `SensitiveDataSanitizer` |
| `LastError` | 1000 caracteres | Outbox |
| Nome do consumidor do Inbox | 100 caracteres | `IInboxStore` |
| Lote de limpeza | 5.000 linhas por chamada, até 1 min | Relay e limpeza do Inbox |

---

## ⚠️ Riscos residuais

| Risco | Por que existe | Mitigação |
|---|---|---|
| Dados pessoais no payload | O sanitizador cobre erros e motivos, **não** o payload | Coloque ids e metadados no evento; o consumidor consulta o serviço dono |
| Padrões de PII não cobertos | A sanitização é por regex: tokens opacos sem pontos (`Bearer abc123`), documentos fora dos formatos (RG, passaporte) e a senha embutida numa URI sem ponto no host (`amqp://usuario:senha@localhost`) não são mascarados; telefone escrito só com dígitos, sem DDD entre parênteses, `+55` ou hífen, também não; qualquer número de 11 dígitos é mascarado como CPF e de 13 a 19, como cartão (mascara a mais, inclusive trechos de GUID com hífens); texto além de 4.096 caracteres é descartado | Não confie nela como única barreira; não coloque PII em mensagens de exceção |
| Exceções nos logs | O objeto de exceção entregue ao `ILogger` (ex.: evento 4204 do consumidor, 4004 do relay) não passa pelo sanitizador | Não inclua dados sensíveis em mensagens de exceção; configure o nível e o destino dos logs |
| Produtor com acesso à exchange/entidade | Não há assinatura nem cifra de mensagem; o `x-tec-attempt` vem da mensagem e é confiado | Permissões mínimas no broker; produtores confiáveis |
| TLS desligável | `RequireTls = false` (ou o ambiente Development) aceita `amqp://`, o emulador do Service Bus e `http://` | Não use `RequireTls = false` fora de redes já protegidas; confira `DOTNET_ENVIRONMENT` em produção |
| Recusa de TLS no RabbitMQ só na conexão | A URI vem do cofre, fora das opções validadas na subida: a recusa acontece ao abrir a conexão (subida dos consumidores, primeira publicação, health check) | Monitore o health check e o log 4203 |
| Quem escreve na tabela do Outbox publica no broker | O relay publica o que está no banco | Escrita só para o login da aplicação |
| Entrega "pelo menos uma vez" e sem ordem | Relays concorrentes, retry, reentrega | Consumidores idempotentes ([📥 Inbox](inbox.md)) |

---

## ⚙️ Opções

Ver [Contratos](contratos-de-integracao.md#️-opções), [Outbox](outbox.md#️-opções), [Inbox](inbox.md#️-opções) e [RabbitMQ](rabbitmq.md#️-opções). As opções com efeito direto na segurança:

| Opção | Efeito |
|---|---|
| `MessagingOptions.MaxPayloadLength` | Teto do payload |
| `*.ConnectionSecretName` | Nome do segredo no cofre |
| `RabbitMqOptions.CircuitBreaker` | Evita tempestade de reconexões (e de leituras no cofre) com o broker fora |
| `RabbitMqOptions.Heartbeat` · `PublishTimeout` | Detecção de conexão morta e espera máxima por confirmação |
| `RabbitMqOptions.RequireTls` · `AzureServiceBusOptions.RequireTls` · `AwsMessagingOptions.RequireTls` | `null` (padrão) exige TLS fora de Development; `true` exige sempre; `false` libera |

## ❌ Erros

Ver as tabelas de erros de cada tema. Os que indicam problema de segurança:

| Situação | Causa | O que fazer |
|---|---|---|
| `InvalidOperationException: O segredo ... não contém uma URI amqp:// ou amqps:// válida` | Segredo do RabbitMQ malformado | Corrija o segredo |
| `InvalidOperationException` citando TLS, `UseDevelopmentEmulator` ou `http://` | Conexão sem TLS com `RequireTls` exigido | Use `amqps://`, o namespace real ou `https://` |
| Mensagens na DLQ com `x-tec-error` "Envelope sem messageId válido" etc. | Produtor externo enviando fora do contrato | Revise as permissões de publicação na exchange |
| `ACCESS_REFUSED` (403) na administração da DLQ | Usuário do broker sem permissão na fila | Ajuste as permissões; o erro sobe como exceção |

## 🛡️ Segurança

> [!WARNING]
> Mensagens ficam em DLQs, logs de erro e no Outbox por dias: **não coloque dados pessoais nem segredos no payload**.

> [!WARNING]
> Conceda ao usuário do broker só o vhost e as permissões necessárias (`configure` só para as filas do serviço).

### Checklist

- [ ] Segredos do broker no cofre, nunca em `appsettings` nem em variável de ambiente do container.
- [ ] `amqps://` (ou TLS equivalente) fora da máquina local, e `RequireTls` sem `false` em produção.
- [ ] Usuário do banco da aplicação é o único com escrita nas tabelas do Outbox/Inbox.
- [ ] Usuário do broker com permissões mínimas por vhost.
- [ ] Contratos com ids e metadados, sem PII.
- [ ] Alertas em `dead_messages`, `dlq.messages` e `oldest_pending_age` ([📡 Telemetria](telemetria.md)).
- [ ] `PublishedRetention` e `Retention` do Inbox coerentes com a política de retenção de dados.

### Decisões de arquitetura

[ADR 0006](adr/0006-sanitizacao-de-pii-e-credenciais-em-erros-logs-e-cabecalhos.md) (sanitização) · [ADR 0001](adr/0001-outbox-transacional-com-publicacao-fora-da-transacao.md) (Outbox) · [ADR 0004](adr/0004-inbox-idempotente-na-mesma-transacao-do-efeito.md) (Inbox).

### Cadeia de suprimentos

O build trata vulnerabilidade conhecida (`NU1901` a `NU1904`) como erro, versiona os lock files, fixa dependências transitivas vulneráveis em `Directory.Packages.props` e mapeia as origens do NuGet (`TEC.*` só do feed interno, contra *dependency confusion*). As imagens de container do CI são fixadas por digest ([🛠️ Desenvolvimento](desenvolvimento.md)).

## ❓ Perguntas frequentes

<details>
<summary><b>O motivo da falha vai para o cabeçalho da DLQ ou no banco. Pode vazar algo?</b></summary>

O motivo da falha e as mensagens de erro gravadas no banco (`LastError`) e no cabeçalho AMQP `x-tec-error` passam pelo `SensitiveDataSanitizer`. Ocorrências de CPF, CNPJ, número de cartão de crédito, CVV, e-mail, telefone, senhas de connection strings, Bearer tokens JWT e campos de senha são substituídas por `***REDACTED***` antes da gravação. A sanitização é por padrões (regex): veja os [riscos residuais](#️-riscos-residuais).
</details>

<details>
<summary><b>O `SensitiveDataSanitizer` é uma API pública?</b></summary>

Não: é `internal` ao núcleo. Para mascarar dados na sua aplicação use as utilidades do TEC.Core.
</details>

---
⬅️ [📡 Telemetria](telemetria.md) · [📚 Índice](README.md) · [🧪 Testes](testes.md) ➡️
