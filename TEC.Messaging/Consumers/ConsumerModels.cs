namespace TEC.Messaging;

/// <summary>O que fazer com a mensagem após o processamento no consumidor.</summary>
public enum ConsumeAction
{
    /// <summary>Processada com sucesso (ack).</summary>
    Complete = 1,

    /// <summary>Falha que consome tentativa: encaminha para espera com backoff ou DLQ.</summary>
    Retry = 2,

    /// <summary>Falha irrecuperável (mensagem inválida): vai direto para a DLQ.</summary>
    Reject = 3,

    /// <summary>Devolve à fila sem consumir tentativa (ex.: dependência temporariamente indisponível).</summary>
    Requeue = 4,
}

/// <summary>Desfecho do processamento de uma mensagem.</summary>
/// <param name="Action">Ação a ser executada pelo transporte.</param>
/// <param name="Reason">Motivo da falha ou rejeição.</param>
/// <param name="Attempt">Número da tentativa controlada.</param>
public readonly record struct ConsumeResult(ConsumeAction Action, string? Reason = null, int? Attempt = null)
{
    /// <summary>Processada com sucesso.</summary>
    public static ConsumeResult Completed => new(ConsumeAction.Complete);

    /// <summary>Nova tentativa com backoff.</summary>
    public static ConsumeResult Retry(string reason, int? attempt = null) => new(ConsumeAction.Retry, reason, attempt);

    /// <summary>Direto para a DLQ.</summary>
    public static ConsumeResult Reject(string reason) => new(ConsumeAction.Reject, reason);

    /// <summary>Devolve à fila sem consumir tentativa.</summary>
    public static ConsumeResult Requeue(string reason) => new(ConsumeAction.Requeue, reason);
}

/// <summary>Mensagem entregue ao consumidor.</summary>
/// <param name="Envelope">Envelope da mensagem.</param>
/// <param name="PreviousFailures">Falhas anteriores registradas.</param>
/// <param name="Redelivered">Se foi reentregue pelo broker.</param>
public sealed record ReceivedMessage(MessageEnvelope Envelope, int PreviousFailures, bool Redelivered);
