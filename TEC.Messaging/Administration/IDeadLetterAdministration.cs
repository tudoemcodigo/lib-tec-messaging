using TEC.Core.Common.Results;

namespace TEC.Messaging.Administration;

/// <summary>Mensagem parada numa fila de mensagens mortas (DLQ).</summary>
/// <param name="MessageId">Identificador (ou <see cref="Guid.Empty"/> se ilegível).</param>
/// <param name="Type">Contrato (ou <c>?</c>).</param>
/// <param name="Version">Versão do contrato.</param>
/// <param name="OccurredAt">Quando ocorreu.</param>
/// <param name="CorrelationId">Correlação.</param>
/// <param name="Attempts">Falhas registradas.</param>
/// <param name="LastError">Último erro (motivo de ter ido para a DLQ).</param>
/// <param name="Payload">Payload JSON (ou o corpo cru, se o envelope é ilegível).</param>
public sealed record DeadLetterMessage(
    Guid MessageId, string Type, int Version, DateTimeOffset? OccurredAt, string? CorrelationId, int Attempts, string? LastError, string Payload);

/// <summary>
/// Operação das filas de mensagens mortas pelo administrador, independente do broker. Fila inexistente devolve falha
/// <c>NotFound</c> (código <see cref="QueueNotFoundCode"/>).
/// </summary>
public interface IDeadLetterAdministration
{
    /// <summary>Código do erro de fila (ou DLQ) inexistente.</summary>
    public const string QueueNotFoundCode = "FILA_NAO_ENCONTRADA";

    /// <summary>Quantidade de mensagens na DLQ da fila.</summary>
    /// <param name="queue">Fila principal (a DLQ é derivada dela).</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Quantidade.</returns>
    Task<Result<long>> CountAsync(string queue, CancellationToken cancellationToken);

    /// <summary>Lê até <paramref name="limit"/> mensagens da DLQ sem removê-las.</summary>
    /// <param name="queue">Fila principal.</param>
    /// <param name="limit">Máximo (1 a 500).</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Mensagens.</returns>
    Task<Result<IReadOnlyList<DeadLetterMessage>>> ListAsync(string queue, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Devolve mensagens da DLQ à fila principal (todas ou só <paramref name="messageIds"/>), com as tentativas zeradas.
    /// <paramref name="handler"/>, se informado, pode tratar a mensagem por conta própria: retornando <c>true</c>, ela só
    /// sai da DLQ (sem republicar).
    /// </summary>
    /// <param name="queue">Fila principal.</param>
    /// <param name="messageIds">Mensagens (até 1000); <c>null</c> = todas.</param>
    /// <param name="handler">Tratamento próprio opcional.</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Quantas saíram da DLQ.</returns>
    Task<Result<int>> RequeueAsync(
        string queue, IReadOnlyCollection<Guid>? messageIds, Func<DeadLetterMessage, CancellationToken, Task<bool>>? handler,
        CancellationToken cancellationToken);

    /// <summary>Remove definitivamente mensagens da DLQ.</summary>
    /// <param name="queue">Fila principal.</param>
    /// <param name="messageIds">Mensagens (1 a 1000).</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Quantas foram removidas.</returns>
    Task<Result<int>> DiscardAsync(string queue, IReadOnlyCollection<Guid> messageIds, CancellationToken cancellationToken);
}
