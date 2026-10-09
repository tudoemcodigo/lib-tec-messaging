using System.Diagnostics;
using Microsoft.Extensions.Options;
using TEC.Core.Domain;
using TEC.Messaging.Context;
using TEC.Messaging.Contracts;
using TEC.Messaging.Internal;
using TEC.Messaging.Mapping;

namespace TEC.Messaging.Outbox;

/// <summary>
/// Monta os envelopes do Outbox: aplica os mapeadores aos eventos de domínio, serializa pelo catálogo de contratos e
/// carimba correlação, causa e trace do fluxo atual (<see cref="MessageContext"/>, <see cref="Activity.Current"/>).
/// Usado pelos armazenamentos (ex.: o interceptor do TEC.Messaging.SqlServer).
/// </summary>
public sealed class IntegrationEnvelopeFactory
{
    private readonly IMessageTypeRegistry _registry;
    private readonly IIntegrationEventMapper[] _mappers;
    private readonly int _maxPayloadLength;

    /// <summary>Cria a fábrica (resolvida pelo container).</summary>
    /// <param name="registry">Catálogo de contratos.</param>
    /// <param name="mappers">Mapeadores de eventos de domínio.</param>
    /// <param name="options">Opções gerais.</param>
    public IntegrationEnvelopeFactory(IMessageTypeRegistry registry, IEnumerable<IIntegrationEventMapper> mappers, IOptions<MessagingOptions> options)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(mappers);
        ArgumentNullException.ThrowIfNull(options);
        _registry = registry;
        _mappers = [.. mappers];
        _maxPayloadLength = options.Value.MaxPayloadLength;
    }

    /// <summary>Indica se há mapeadores registrados (sem eles, eventos de domínio não geram mensagens).</summary>
    public bool HasMappers => _mappers.Length > 0;

    /// <summary>Envelopes dos eventos de integração gerados por um evento de domínio.</summary>
    /// <param name="domainEvent">Evento de domínio.</param>
    /// <returns>Zero ou mais envelopes, com o <see cref="IDomainEvent.OccurredAt"/> do evento.</returns>
    /// <exception cref="InvalidOperationException">Um mapeador devolveu um tipo não registrado ou o envelope é inválido.</exception>
    public IReadOnlyList<MessageEnvelope> FromDomainEvent(IDomainEvent domainEvent)
    {
        ArgumentNullException.ThrowIfNull(domainEvent);
        List<MessageEnvelope>? envelopes = null;
        foreach (var mapper in _mappers)
        {
            foreach (var integrationEvent in mapper.Map(domainEvent))
                (envelopes ??= []).Add(Create(integrationEvent, domainEvent.OccurredAt));
        }

        return envelopes ?? (IReadOnlyList<MessageEnvelope>)[];
    }

    /// <summary>Envelope de um evento de integração.</summary>
    /// <param name="integrationEvent">Evento de um contrato registrado.</param>
    /// <param name="occurredAt">Quando ocorreu.</param>
    /// <returns>O envelope, com id UUIDv7, correlação, causa e trace do fluxo atual.</returns>
    /// <exception cref="InvalidOperationException">O tipo não está registrado ou o envelope é inválido (ex.: payload grande demais).</exception>
    public MessageEnvelope Create(object integrationEvent, DateTimeOffset occurredAt)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);
        if (!_registry.TryGet(integrationEvent.GetType(), out var info))
        {
            throw new InvalidOperationException(
                $"O tipo {integrationEvent.GetType().FullName} não está registrado como contrato de integração (use AddEventType ou [IntegrationEvent] + AddEventTypesFromAssembly).");
        }

        var envelope = new MessageEnvelope(
            GuidV7.Create(occurredAt), info.Name, info.Version, occurredAt, MessageContext.CorrelationId, MessageContext.CausationId,
            Activity.Current?.Id is { Length: <= MessageEnvelope.MaxTraceParentLength } traceParent ? traceParent : null,
            info.Serialize(integrationEvent));

        var valid = envelope.Validate(_maxPayloadLength);
        if (valid.IsFailure)
            throw new InvalidOperationException($"Mensagem '{info.Name}' inválida: {valid.Error!.Message}");
        return envelope;
    }
}
