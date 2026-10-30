<div align="center">
<img src="https://www.tudoemcodigo.com/marca/PNG/tudoemcodigo-icone-512.png" alt="Tudo em Código" width="90" />
</div>

[🏠 TEC.Messaging](../../README.md) › [📚 Documentação](../README.md) › 🏛️ Decisões Arquiteturais (ADRs)

# 🏛️ Registros de Decisões de Arquitetura (ADR) — TEC.Messaging

Este diretório contém os Registros de Decisões de Arquitetura (**Architecture Decision Records — ADRs**) do componente **TEC.Messaging**.

O objetivo destes registros é documentar de forma clara, contextualizada e justificada as escolhas técnicas e de engenharia adotadas na biblioteca, orientando a manutenção, a evolução e a auditoria contínua da plataforma.

> [!NOTE]
> Os ADRs 0001 a 0008 são **retroativos**: registram decisões que já estavam implementadas na versão `0.0.1`, reconstruídas a partir do código, dos testes e da [🔍 Revisão técnica](../revisao-tecnica.md). A data de cada um é a do registro.

---

## 📑 Índice de Decisões

| ADR | Título | Status | Data | Contexto Resumido |
|:---:|:---|:---:|:---:|:---|
| [0001](0001-outbox-transacional-com-publicacao-fora-da-transacao.md) | **Outbox Transacional com Publicação Fora da Transação** | Aprovado | 2026-10-24 | O evento é gravado na mesma transação do agregado e publicado depois pelo relay, sem *dual write* e sem prender o banco. |
| [0002](0002-reserva-concorrente-do-outbox-com-token-e-relogio-do-banco.md) | **Reserva Concorrente do Outbox com Token e Relógio do Banco** | Aprovado | 2026-10-24 | Lease persistido com token e validade pelo relógio do banco (`UPDLOCK, READPAST` e `SKIP LOCKED`) para vários relays sem duplicar nem esperar. |
| [0003](0003-falhas-isoladas-backoff-e-transporte-indisponivel-sem-contar-tentativa.md) | **Falhas Isoladas, Backoff e Transporte Indisponível Sem Contar Tentativa** | Aprovado | 2026-10-24 | Mensagem defeituosa não bloqueia as demais (`Dead`); broker fora pausa o relay sem matar mensagens (`ITransportUnavailableException`). |
| [0004](0004-inbox-idempotente-na-mesma-transacao-do-efeito.md) | **Inbox Idempotente na Mesma Transação do Efeito** | Aprovado | 2026-10-24 | Registro `(MessageId, Consumer)` gravado junto com o efeito: entrega "pelo menos uma vez" vira processamento uma única vez. |
| [0005](0005-contratos-de-integracao-versionados-separados-do-dominio.md) | **Contratos de Integração Versionados e Separados do Domínio** | Aprovado | 2026-10-24 | `[IntegrationEvent]` com nome e versão estáveis, mapeadores e catálogo imutável; o domínio muda sem quebrar consumidores. |
| [0006](0006-sanitizacao-de-pii-e-credenciais-em-erros-logs-e-cabecalhos.md) | **Sanitização de PII e Credenciais em Erros, Logs e Cabeçalhos** | Aprovado | 2026-10-24 | `SensitiveDataSanitizer` mascara CPF, CNPJ, cartão, senhas e tokens no que é gravado e registrado (LGPD); entradas externas validadas. |
| [0007](0007-retry-escalonado-e-dlq-no-rabbitmq-com-filas-de-espera.md) | **Retry Escalonado e DLQ no RabbitMQ com Filas de Espera** | Aprovado | 2026-10-24 | Filas quorum, filas de espera por TTL e DLQ na topologia do broker: sem espera em memória e sem laço de reentrega. |
| [0008](0008-nucleo-agnostico-com-bancos-e-brokers-em-pacotes-satelites.md) | **Núcleo Agnóstico com Bancos e Brokers em Pacotes Satélites** | Aprovado | 2026-10-24 | Núcleo sem banco nem broker, compatível com Native AOT; SQL Server, PostgreSQL, RabbitMQ, Azure Service Bus e AWS em pacotes à parte. |
| [0009](0009-reenvio-agendado-no-azure-service-bus-e-rotacao-de-credenciais.md) | **Reenvio Agendado no Azure Service Bus, Rotação de Credenciais e TLS Obrigatório** | Aprovado | 2026-10-29 | Retry com `ScheduledEnqueueTime` (agenda antes de concluir; duplicidade em vez de perda), clientes recriados com single-flight quando a credencial é recusada, `RequireTls` fora de Development. |

---

## 📐 Formato

Cada ADR segue o molde dos demais componentes TEC: **Status**, **Data**, **Contexto**, **Decisão** e **Consequências** (positivas, neutras e negativas), com links relativos para o código que implementa a decisão.

| Campo | Significado |
|---|---|
| Status | `Aprovado` (em vigor), `Substituído` (aponta o ADR que o substitui) ou `Rejeitado` |
| Contexto | O problema e as forças em jogo |
| Decisão | O que foi decidido e onde está no código |
| Consequências | O que melhora, o que piora e o que passa a ser responsabilidade da aplicação |

Para propor uma decisão nova, crie `NNNN-slug-em-portugues.md` com o próximo número, preencha o molde e acrescente a linha no índice acima e em [📚 Documentação](../README.md).

---
[🏠 README](../../README.md) · [📚 Documentação](../README.md)
