[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › Segurança

# 🛡️ Segurança

> O que o componente garante e o que é responsabilidade da aplicação.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança-1)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

| Ameaça | Controle |
|---|---|
| Vazamento de credencial do broker | URI só pelo TEC.Vault (`ConnectionSecretName`); nunca em configuração nem em log |
| Mensagem gigante (DoS) | `MaxPayloadLength` na gravação; corpo AMQP limitado na leitura; JSON com profundidade máxima |
| Entrada não confiável na correlação | `MessageContext` descarta valores longos ou com caracteres de controle |
| Mensagem forjada ilegível | Rejeitada para a DLQ, sem exceção no consumidor |
| SQL injection | Nomes de tabela/coluna vêm do modelo e são delimitados; valores sempre parametrizados |
| Desserialização insegura | `System.Text.Json` com tipos registrados (sem polimorfismo aberto) |

## 🚀 Uso

```bash
# Segredo do broker no cofre (exemplo com HashiCorp Vault)
vault kv put secret/minha-app rabbitmq="amqps://app:<senha>@rabbit.interno:5671/producao"
```

## ⚙️ Opções

Ver [Contratos](contratos-de-integracao.md#️-opções), [Outbox](outbox.md#️-opções) e [RabbitMQ](rabbitmq.md#️-opções).

## ❌ Erros

Ver as tabelas de erros de cada tema.

## 🛡️ Segurança

> [!WARNING]
> Mensagens ficam em DLQs, logs de erro e no Outbox por dias: **não coloque dados pessoais nem segredos no payload**.

> [!WARNING]
> Conceda ao usuário do broker só o vhost e as permissões necessárias (`configure` só para as filas do serviço).

## ❓ Perguntas frequentes

<details>
<summary><b>O motivo da falha vai para o cabeçalho da DLQ. Pode vazar algo?</b></summary>

O motivo é a mensagem da exceção (até 1000 caracteres). Não coloque dados sensíveis em mensagens de exceção.
</details>

---
⬅️ [Telemetria](telemetria.md) · [📚 Índice](README.md) · [Testes](testes.md) ➡️
