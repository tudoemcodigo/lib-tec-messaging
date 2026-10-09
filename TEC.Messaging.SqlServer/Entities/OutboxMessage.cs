using TEC.Messaging.Outbox;

namespace TEC.Messaging.SqlServer.Entities;

/// <summary>
/// Linha do Outbox transacional: gravada na mesma transação do agregado e publicada depois pelo relay. Mapeada por
/// <see cref="ModelBuilderExtensions.AddTecMessaging"/>.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>Identificador (o <see cref="MessageEnvelope.MessageId"/>, UUIDv7).</summary>
    public Guid Id { get; set; }

    /// <summary>Contrato.</summary>
    public string Type { get; set; } = "";

    /// <summary>Versão do contrato.</summary>
    public int Version { get; set; }

    /// <summary>Quando o fato ocorreu.</summary>
    public DateTimeOffset OccurredAt { get; set; }

    /// <summary>Correlação.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>Causa.</summary>
    public string? CausationId { get; set; }

    /// <summary>Contexto W3C do trace.</summary>
    public string? TraceParent { get; set; }

    /// <summary>Payload JSON.</summary>
    public string Payload { get; set; } = "";

    /// <summary>Situação.</summary>
    public OutboxMessageStatus Status { get; set; }

    /// <summary>Falhas de publicação.</summary>
    public int Attempts { get; set; }

    /// <summary>Quando pode ser publicada (agora, ou depois da espera de uma falha).</summary>
    public DateTimeOffset NextAttemptAt { get; set; }

    /// <summary>Reserva vigente (relay que está publicando).</summary>
    public Guid? LeaseToken { get; set; }

    /// <summary>Fim da reserva vigente.</summary>
    public DateTimeOffset? LeasedUntil { get; set; }

    /// <summary>Quando foi publicada.</summary>
    public DateTimeOffset? PublishedAt { get; set; }

    /// <summary>Último erro de publicação.</summary>
    public string? LastError { get; set; }

    /// <summary>Linha pendente a partir de um envelope.</summary>
    /// <param name="envelope">Mensagem.</param>
    /// <param name="now">Agora (primeira tentativa imediata).</param>
    /// <returns>A linha.</returns>
    public static OutboxMessage Create(MessageEnvelope envelope, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        return new OutboxMessage
        {
            Id = envelope.MessageId,
            Type = envelope.Type,
            Version = envelope.Version,
            OccurredAt = envelope.OccurredAt,
            CorrelationId = envelope.CorrelationId,
            CausationId = envelope.CausationId,
            TraceParent = envelope.TraceParent,
            Payload = envelope.Payload,
            Status = OutboxMessageStatus.Pending,
            NextAttemptAt = now,
        };
    }

    internal MessageEnvelope ToEnvelope() => new(Id, Type, Version, OccurredAt, CorrelationId, CausationId, TraceParent, Payload);
}

/// <summary>Registro do Inbox: mensagem já processada por um consumidor (consumidor idempotente).</summary>
public sealed class InboxMessage
{
    /// <summary>Mensagem.</summary>
    public Guid MessageId { get; set; }

    /// <summary>Consumidor.</summary>
    public string Consumer { get; set; } = "";

    /// <summary>Quando foi processada.</summary>
    public DateTimeOffset ProcessedAt { get; set; }
}
