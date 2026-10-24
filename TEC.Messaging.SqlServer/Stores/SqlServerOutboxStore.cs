using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using TEC.Messaging.Outbox;
using TEC.Messaging.SqlServer.Entities;

namespace TEC.Messaging.SqlServer.Stores;

/// <summary>
/// Outbox no SQL Server. A reserva é um único <c>UPDATE</c> sobre os primeiros pendentes vencidos com
/// <c>UPDLOCK, READPAST, ROWLOCK</c>: relays concorrentes pegam lotes diferentes sem esperar uns pelos outros, e a
/// reserva (token + validade) sobrevive à transação, de modo que a publicação acontece fora dela.
/// </summary>
/// <typeparam name="TContext">Contexto com <see cref="ModelBuilderExtensions.AddTecMessaging"/> no modelo.</typeparam>
internal sealed class SqlServerOutboxStore<TContext>(TContext db, TimeProvider time) : IOutboxStore, IOutboxAdministration
    where TContext : DbContext
{
    private const int MaxIds = 1000;

    public async Task<IReadOnlyList<LeasedOutboxMessage>> LeaseAsync(int batchSize, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(batchSize, 1000);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(leaseDuration, TimeSpan.FromDays(1));

        var n = ModelBuilderExtensions.NamesOf<OutboxMessage>(db);
        var now = time.GetUtcNow();
        var token = Guid.NewGuid();

        // A validade da reserva é gravada e comparada pelo relógio do banco (SYSUTCDATETIME), único para todas as
        // instâncias: relógios de servidores divergentes não fazem outra instância assumir um lote ainda em publicação
        var sql = $"""
            WITH batch AS (
                SELECT TOP (@batchSize) * FROM {n.Table} WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE {n["Status"]} = 0 AND {n["NextAttemptAt"]} <= @now
                    AND ({n["LeasedUntil"]} IS NULL OR {n["LeasedUntil"]} < TODATETIMEOFFSET(SYSUTCDATETIME(), 0))
                ORDER BY {n["OccurredAt"]}, {n["Id"]})
            UPDATE batch SET {n["LeaseToken"]} = @token, {n["LeasedUntil"]} = DATEADD(millisecond, @leaseMs, TODATETIMEOFFSET(SYSUTCDATETIME(), 0))
            OUTPUT inserted.*;
            """;
#pragma warning disable EF1002 // Nomes vêm do modelo (delimitados); valores são parâmetros
        var rows = await db.Set<OutboxMessage>()
            .FromSqlRaw(
                sql,
                new SqlParameter("@batchSize", batchSize),
                new SqlParameter("@now", now),
                new SqlParameter("@token", token),
                new SqlParameter("@leaseMs", (int)Math.Ceiling(leaseDuration.TotalMilliseconds)))
            .AsNoTracking()
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);
#pragma warning restore EF1002

        return [.. rows.OrderBy(r => r.OccurredAt).ThenBy(r => r.Id).Select(r => new LeasedOutboxMessage(r.ToEnvelope(), r.Attempts, token))];
    }

    public async Task CompleteAsync(IReadOnlyList<OutboxOutcome> outcomes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(outcomes);
        if (outcomes.Count == 0)
            return;

        var now = time.GetUtcNow();
        foreach (var group in outcomes.Where(o => o.Kind == OutboxOutcomeKind.Published).GroupBy(o => o.LeaseToken))
        {
            var token = group.Key;
            var ids = group.Select(o => o.MessageId).ToList();
            await db.Set<OutboxMessage>()
                .Where(m => ids.Contains(m.Id) && m.LeaseToken == token)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(m => m.Status, OutboxMessageStatus.Published)
                    .SetProperty(m => m.PublishedAt, now)
                    .SetProperty(m => m.LeaseToken, (Guid?)null)
                    .SetProperty(m => m.LeasedUntil, (DateTimeOffset?)null)
                    .SetProperty(m => m.LastError, (string?)null), cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (var outcome in outcomes.Where(o => o.Kind != OutboxOutcomeKind.Published))
        {
            var id = outcome.MessageId;
            var token = outcome.LeaseToken;
            var status = outcome.Kind == OutboxOutcomeKind.Dead ? OutboxMessageStatus.Dead : OutboxMessageStatus.Pending;
            var next = outcome.NextAttemptAt ?? now;
            var query = db.Set<OutboxMessage>().Where(m => m.Id == id && m.LeaseToken == token);
            if (outcome.Kind == OutboxOutcomeKind.Released)
            {
                await query.ExecuteUpdateAsync(s => s
                        .SetProperty(m => m.NextAttemptAt, next)
                        .SetProperty(m => m.LeaseToken, (Guid?)null)
                        .SetProperty(m => m.LeasedUntil, (DateTimeOffset?)null), cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                var attempts = outcome.Attempts;
                var error = outcome.Error;
                await query.ExecuteUpdateAsync(s => s
                        .SetProperty(m => m.Status, status)
                        .SetProperty(m => m.Attempts, attempts)
                        .SetProperty(m => m.NextAttemptAt, next)
                        .SetProperty(m => m.LastError, error)
                        .SetProperty(m => m.LeaseToken, (Guid?)null)
                        .SetProperty(m => m.LeasedUntil, (DateTimeOffset?)null), cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    public Task<int> PurgePublishedAsync(DateTimeOffset publishedBefore, int maxRows, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRows, 1);
        var n = ModelBuilderExtensions.NamesOf<OutboxMessage>(db);
#pragma warning disable EF1002 // Nomes vêm do modelo (delimitados); valores são parâmetros
        return db.Database.ExecuteSqlRawAsync(
            $"DELETE TOP (@max) FROM {n.Table} WHERE {n["Status"]} = 1 AND {n["PublishedAt"]} < @before",
            [new SqlParameter("@max", maxRows), new SqlParameter("@before", publishedBefore)],
            cancellationToken);
#pragma warning restore EF1002
    }

    public async Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        var pending = await GetPendingStatisticsAsync(cancellationToken).ConfigureAwait(false);
        var published = await db.Set<OutboxMessage>().AsNoTracking()
            .CountAsync(m => m.Status == OutboxMessageStatus.Published, cancellationToken)
            .ConfigureAwait(false);
        return pending with { Published = published };
    }

    public async Task<OutboxStatistics> GetPendingStatisticsAsync(CancellationToken cancellationToken)
    {
        // Cada consulta tem o predicado do filtro de um índice (IX_<tabela>_Pending, IX_<tabela>_Dead): o custo acompanha as
        // pendentes e as mortas, não as publicadas retidas (o relay chama isto a cada StatisticsInterval, em cada instância)
        var messages = db.Set<OutboxMessage>().AsNoTracking();
        var pending = await messages.CountAsync(m => m.Status == OutboxMessageStatus.Pending, cancellationToken).ConfigureAwait(false);
        var withErrors = await messages.CountAsync(m => m.Status == OutboxMessageStatus.Pending && m.Attempts > 0, cancellationToken).ConfigureAwait(false);
        var dead = await messages.CountAsync(m => m.Status == OutboxMessageStatus.Dead, cancellationToken).ConfigureAwait(false);
        var oldest = await messages
            .Where(m => m.Status == OutboxMessageStatus.Pending)
            .OrderBy(m => m.OccurredAt)
            .Select(m => (DateTimeOffset?)m.OccurredAt)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
        return new OutboxStatistics(pending, withErrors, dead, 0, oldest);
    }

    public async Task<IReadOnlyList<OutboxMessageInfo>> ListFailedAsync(int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, 1000);
        return await db.Set<OutboxMessage>()
            .AsNoTracking()
            .Where(m => m.Status == OutboxMessageStatus.Dead || (m.Status == OutboxMessageStatus.Pending && m.Attempts > 0))
            .OrderBy(m => m.OccurredAt)
            .Take(limit)
            .Select(m => new OutboxMessageInfo(m.Id, m.Type, m.Version, m.Status, m.Attempts, m.OccurredAt, m.NextAttemptAt, m.PublishedAt, m.LastError))
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public Task<int> RequeueAsync(IReadOnlyCollection<Guid>? ids, CancellationToken cancellationToken)
    {
        // Coleção vazia não é "todas" (só null é): uma seleção vazia de uma tela não pode reprocessar o Outbox inteiro
        if (ids is { Count: 0 or > MaxIds })
            throw new ArgumentOutOfRangeException(nameof(ids), $"Informe de 1 a {MaxIds} mensagens, ou null para todas.");

        var now = time.GetUtcNow();

        // Pendentes com reserva vigente estão em publicação: zerar a reserva deixaria outro relay publicá-las de novo. A
        // validade é comparada pelo relógio do banco, o mesmo da reserva (DateTimeOffset.UtcNow vira SYSUTCDATETIME no SQL)
        var target = db.Set<OutboxMessage>()
            .Where(m => m.Status == OutboxMessageStatus.Dead
                || (m.Status == OutboxMessageStatus.Pending && m.Attempts > 0 && (m.LeasedUntil == null || m.LeasedUntil < DateTimeOffset.UtcNow)));
        if (ids is not null)
        {
            var list = ids.ToList();
            target = target.Where(m => list.Contains(m.Id));
        }

        return target.ExecuteUpdateAsync(s => s
            .SetProperty(m => m.Status, OutboxMessageStatus.Pending)
            .SetProperty(m => m.Attempts, 0)
            .SetProperty(m => m.NextAttemptAt, now)
            .SetProperty(m => m.LeaseToken, (Guid?)null)
            .SetProperty(m => m.LeasedUntil, (DateTimeOffset?)null), cancellationToken);
    }

    public Task<int> DiscardAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(ids);
        if (ids.Count is 0 or > MaxIds)
            throw new ArgumentOutOfRangeException(nameof(ids), $"Informe de 1 a {MaxIds} mensagens.");
        var list = ids.ToList();
        return db.Set<OutboxMessage>()
            .Where(m => m.Status == OutboxMessageStatus.Dead && list.Contains(m.Id))
            .ExecuteDeleteAsync(cancellationToken);
    }
}

/// <summary>Grava eventos de integração no Outbox do contexto atual (<see cref="IOutbox"/>).</summary>
internal sealed class SqlServerOutbox<TContext>(TContext db, IntegrationEnvelopeFactory factory, TimeProvider time) : IOutbox
    where TContext : DbContext
{
    public MessageEnvelope Enqueue(object integrationEvent, DateTimeOffset? occurredAt = null)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);
        var now = time.GetUtcNow();
        var envelope = factory.Create(integrationEvent, occurredAt ?? now);
        // A métrica tec.messaging.outbox.enqueued é contada pelo interceptor, só depois do SaveChanges bem-sucedido
        db.Set<OutboxMessage>().Add(OutboxMessage.Create(envelope, now));
        return envelope;
    }
}
