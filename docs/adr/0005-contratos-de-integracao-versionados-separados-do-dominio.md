# ADR 0005: Contratos de Integração Versionados e Separados do Domínio

## Status
Aprovado

## Data
2026-10-24 (registro retroativo de uma decisão já implementada na versão 0.0.1)

## Contexto
Serializar o evento de domínio direto para outros serviços acopla o modelo interno ao contrato público: qualquer refatoração (renomear classe ou propriedade, mover namespace) vira uma mudança incompatível para quem consome. Além disso, o consumidor recebe o tipo como **texto no envelope** (entrada que vem de fora do processo) e não deve escolher livremente o tipo a desserializar. O componente também precisa funcionar em Native AOT, onde a serialização por reflexão não está disponível.

## Decisão
Separar o **evento de domínio** (interno, `IDomainEvent`, livre para mudar) do **evento de integração** (contrato público, nome e versão estáveis), ligando-os por mapeadores explícitos:

- **Contrato**: [`[IntegrationEvent("vendas.pedido-aprovado", Version = 1)]`](../../TEC.Messaging/Contracts/IntegrationEventAttribute.cs). O nome (minúsculas, dígitos, `.` e `-`, até 200 caracteres) é o que trafega em `MessageEnvelope.Type`; a versão começa em 1. Mudança incompatível = novo tipo com versão maior; as duas versões coexistem até os consumidores migrarem.
- **Mapeamento**: [`IIntegrationEventMapper`](../../TEC.Messaging/Mapping/IIntegrationEventMapper.cs) (sem estado, singleton, rápido, porque roda dentro do `SaveChanges`) converte um evento de domínio em zero ou mais eventos de integração; `Map<TDomainEvent>(...)` cobre o caso simples.
- **Catálogo**: [`IMessageTypeRegistry`](../../TEC.Messaging/Contracts/IMessageTypeRegistry.cs) é imutável (`FrozenDictionary`), montado uma vez na primeira resolução, e falha ao montar se o mesmo nome+versão aponta para dois tipos ou um tipo recebe dois nomes. O nome é validado no registro.
- **Serialização**: cada contrato carrega o seu `JsonTypeInfo` ([`MessageTypeInfo`](../../TEC.Messaging/Contracts/MessageTypeInfo.cs)). O registro por reflexão (`AddEventType<T>()`, `AddEventTypesFromAssembly`) é marcado com `RequiresUnreferencedCode`/`RequiresDynamicCode`; para Native AOT, `AddEventType<T>(JsonTypeInfo<T>)` usa a metadata gerada. O padrão de JSON é o do TEC.Core (`JsonDefaults.Options`).
- **Envelope**: a [`IntegrationEnvelopeFactory`](../../TEC.Messaging/Outbox/IntegrationEnvelopeFactory.cs) monta o [`MessageEnvelope`](../../TEC.Messaging/MessageEnvelope.cs) (UUIDv7, correlação, causa e `traceparent` do fluxo atual) e o valida contra os limites; tipo não registrado ou envelope inválido falha na gravação, não na publicação.

## Consequências
### Positivas
- O domínio pode ser refatorado sem quebrar consumidores; a evolução do contrato é explícita e versionada.
- O consumidor só desserializa tipos **registrados** (sem polimorfismo aberto): o `Type` da mensagem apenas escolhe entre contratos conhecidos.
- Funciona em Native AOT quando os contratos são registrados com `JsonTypeInfo`.
- Erros de contrato (nome inválido, duplicidade, tipo não registrado) aparecem cedo: no registro, na montagem do catálogo ou no `SaveChanges`.

### Neutras / Negativas
- Mais tipos e código de mapeamento (um contrato por evento publicado).
- O nome do contrato não pode mudar depois de publicado; mudança incompatível exige manter duas versões em paralelo.
- O registro por reflexão não é compatível com trimming/AOT; é preciso gerar um `JsonSerializerContext`.
- Publicar o próprio evento de domínio é possível (marcá-lo com o atributo), mas anula a proteção.
