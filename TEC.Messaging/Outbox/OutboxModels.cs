namespace TEC.Messaging.Outbox;

/// <summary>Situação de uma mensagem no Outbox.</summary>
public enum OutboxMessageStatus : byte
{
    /// <summary>Aguardando publicação (inclusive em espera depois de falha).</summary>
    Pending = 0,

    /// <summary>Publicada com confirmação do broker.</summary>
    Published = 1,

    /// <summary>Esgotou as tentativas: precisa de ação do administrador (reprocessar ou descartar). Não bloqueia as demais.</summary>
    Dead = 2,
}

/// <summary>Mensagem reservada (lease) por um relay para publicação.</summary>
/// <param name="Envelope">Mensagem.</param>
/// <param name="Attempts">Falhas anteriores.</param>
/// <param name="LeaseToken">Token da reserva: só quem o tem pode concluir a mensagem.</param>
public sealed record LeasedOutboxMessage(MessageEnvelope Envelope, int Attempts, Guid LeaseToken);

/// <summary>Desfecho da tentativa de publicação de uma mensagem reservada.</summary>
public enum OutboxOutcomeKind
{
    /// <summary>Publicada.</summary>
    Published = 1,

    /// <summary>Falhou; nova tentativa em <see cref="OutboxOutcome.NextAttemptAt"/>.</summary>
    Failed = 2,

    /// <summary>Falhou e esgotou as tentativas.</summary>
    Dead = 3,

    /// <summary>Não foi tentada (broker indisponível ou fim da reserva): volta a pendente sem contar tentativa.</summary>
    Released = 4,
}

/// <summary>Resultado de uma mensagem reservada, aplicado pelo <see cref="IOutboxStore.CompleteAsync"/>.</summary>
/// <param name="MessageId">Mensagem.</param>
/// <param name="LeaseToken">Token da reserva (a conclusão é ignorada se outra reserva assumiu a mensagem).</param>
/// <param name="Kind">Desfecho.</param>
/// <param name="Attempts">Total de falhas depois desta tentativa.</param>
/// <param name="NextAttemptAt">Próxima tentativa (<see cref="OutboxOutcomeKind.Failed"/> e <see cref="OutboxOutcomeKind.Released"/>).</param>
/// <param name="Error">Erro resumido (até <see cref="MaxErrorLength"/> caracteres).</param>
public sealed record OutboxOutcome(Guid MessageId, Guid LeaseToken, OutboxOutcomeKind Kind, int Attempts, DateTimeOffset? NextAttemptAt, string? Error)
{
    /// <summary>Tamanho máximo do erro gravado.</summary>
    public const int MaxErrorLength = 1000;

    internal static OutboxOutcome Published(LeasedOutboxMessage message) =>
        new(message.Envelope.MessageId, message.LeaseToken, OutboxOutcomeKind.Published, message.Attempts, null, null);

    internal static OutboxOutcome Failed(LeasedOutboxMessage message, int attempts, DateTimeOffset nextAttemptAt, string error) =>
        new(message.Envelope.MessageId, message.LeaseToken, OutboxOutcomeKind.Failed, attempts, nextAttemptAt, Truncate(error));

    internal static OutboxOutcome Dead(LeasedOutboxMessage message, int attempts, string error) =>
        new(message.Envelope.MessageId, message.LeaseToken, OutboxOutcomeKind.Dead, attempts, null, Truncate(error));

    internal static OutboxOutcome Released(LeasedOutboxMessage message, DateTimeOffset nextAttemptAt) =>
        new(message.Envelope.MessageId, message.LeaseToken, OutboxOutcomeKind.Released, message.Attempts, nextAttemptAt, null);

    private static string Truncate(string error) => error.Length <= MaxErrorLength ? error : error[..MaxErrorLength];
}

/// <summary>Situação do Outbox, para painéis e métricas.</summary>
/// <param name="Pending">Pendentes (inclusive em espera).</param>
/// <param name="PendingWithErrors">Pendentes que já falharam ao menos uma vez.</param>
/// <param name="Dead">Esgotaram as tentativas.</param>
/// <param name="Published">Publicadas ainda retidas (antes da limpeza).</param>
/// <param name="OldestPendingOccurredAt">Quando ocorreu a pendente mais antiga (<c>null</c> sem pendentes).</param>
public sealed record OutboxStatistics(int Pending, int PendingWithErrors, int Dead, int Published, DateTimeOffset? OldestPendingOccurredAt);

/// <summary>Dados de uma mensagem do Outbox para a administração (sem o payload).</summary>
/// <param name="Id">Identificador (o <c>MessageId</c>).</param>
/// <param name="Type">Contrato.</param>
/// <param name="Version">Versão do contrato.</param>
/// <param name="Status">Situação.</param>
/// <param name="Attempts">Falhas.</param>
/// <param name="OccurredAt">Quando ocorreu.</param>
/// <param name="NextAttemptAt">Próxima tentativa.</param>
/// <param name="PublishedAt">Quando foi publicada.</param>
/// <param name="LastError">Último erro.</param>
public sealed record OutboxMessageInfo(
    Guid Id, string Type, int Version, OutboxMessageStatus Status, int Attempts, DateTimeOffset OccurredAt, DateTimeOffset NextAttemptAt,
    DateTimeOffset? PublishedAt, string? LastError);
