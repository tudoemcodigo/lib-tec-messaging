namespace TEC.Messaging.Outbox;

/// <summary>
/// Armazenamento do Outbox usado pelo relay. Cada método é uma operação curta e independente: a reserva (lease) e a
/// conclusão acontecem em transações separadas e a publicação no broker fica <b>fora</b> de qualquer transação.
/// </summary>
/// <remarks>
/// Implementações devem garantir que dois relays nunca reservem a mesma mensagem enquanto a reserva estiver válida, e que
/// a conclusão só tenha efeito com o <see cref="LeasedOutboxMessage.LeaseToken"/> vigente.
/// </remarks>
public interface IOutboxStore
{
    /// <summary>
    /// Reserva até <paramref name="batchSize"/> mensagens pendentes e vencidas (próxima tentativa já chegou, sem reserva
    /// válida), na ordem em que ocorreram.
    /// </summary>
    /// <param name="batchSize">Máximo de mensagens.</param>
    /// <param name="leaseDuration">Duração da reserva.</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Mensagens reservadas.</returns>
    Task<IReadOnlyList<LeasedOutboxMessage>> LeaseAsync(int batchSize, TimeSpan leaseDuration, CancellationToken cancellationToken);

    /// <summary>Aplica os desfechos e libera as reservas.</summary>
    /// <param name="outcomes">Desfechos.</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Tarefa concluída quando gravado.</returns>
    Task CompleteAsync(IReadOnlyList<OutboxOutcome> outcomes, CancellationToken cancellationToken);

    /// <summary>Apaga publicadas antes de <paramref name="publishedBefore"/> (no máximo <paramref name="maxRows"/>).</summary>
    /// <param name="publishedBefore">Limite da retenção.</param>
    /// <param name="maxRows">Máximo de linhas por chamada.</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Quantas foram apagadas.</returns>
    Task<int> PurgePublishedAsync(DateTimeOffset publishedBefore, int maxRows, CancellationToken cancellationToken);
}

/// <summary>Operação do Outbox pelo administrador: situação, falhas, reprocessamento e descarte.</summary>
public interface IOutboxAdministration
{
    /// <summary>Contagens por situação e a pendente mais antiga.</summary>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Estatísticas.</returns>
    Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken);

    /// <summary>Mensagens com problema (mortas e pendentes que já falharam), das mais antigas para as mais novas.</summary>
    /// <param name="limit">Máximo (1 a 1000).</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Mensagens.</returns>
    Task<IReadOnlyList<OutboxMessageInfo>> ListFailedAsync(int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Devolve à fila de publicação as mensagens mortas e as pendentes com erro (todas, ou só <paramref name="ids"/>), com
    /// as tentativas zeradas e a publicação imediata.
    /// </summary>
    /// <param name="ids">Mensagens (até 1000); <c>null</c> = todas.</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Quantas voltaram.</returns>
    Task<int> RequeueAsync(IReadOnlyCollection<Guid>? ids, CancellationToken cancellationToken);

    /// <summary>Apaga mensagens mortas (só as informadas; pendentes e publicadas não são afetadas).</summary>
    /// <param name="ids">Mensagens (até 1000).</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Quantas foram apagadas.</returns>
    Task<int> DiscardAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken);
}

/// <summary>
/// Grava eventos de integração no Outbox dentro da unidade de trabalho atual, para casos sem agregado (ex.: alteração de
/// configuração). A mensagem só existe se a transação da aplicação fizer commit.
/// </summary>
public interface IOutbox
{
    /// <summary>Adiciona o evento ao Outbox da unidade de trabalho atual (gravado no próximo <c>SaveChanges</c>).</summary>
    /// <param name="integrationEvent">Evento de um contrato registrado.</param>
    /// <param name="occurredAt">Quando ocorreu (<c>null</c> = agora, pelo <see cref="TimeProvider"/>).</param>
    /// <returns>O envelope gravado.</returns>
    /// <exception cref="InvalidOperationException">O tipo não está registrado como contrato.</exception>
    MessageEnvelope Enqueue(object integrationEvent, DateTimeOffset? occurredAt = null);
}
