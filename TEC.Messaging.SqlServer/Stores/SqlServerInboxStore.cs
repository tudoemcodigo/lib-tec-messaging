using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using TEC.Messaging.Inbox;
using TEC.Messaging.SqlServer.Entities;

namespace TEC.Messaging.SqlServer.Stores;

/// <summary>
/// Inbox no SQL Server: o registro entra na unidade de trabalho do contexto e é gravado junto com o efeito do
/// processamento. A chave primária (mensagem + consumidor) impede o processamento duplo concorrente.
/// </summary>
/// <typeparam name="TContext">Contexto com <see cref="ModelBuilderExtensions.AddTecMessaging"/> no modelo.</typeparam>
internal sealed class SqlServerInboxStore<TContext>(TContext db, TimeProvider time) : IInboxStore
    where TContext : DbContext
{
    public async Task<bool> TryBeginAsync(Guid messageId, string consumer, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        if (consumer.Length > InboxOptions.MaxConsumerLength)
            throw new ArgumentOutOfRangeException(nameof(consumer), $"O nome do consumidor tem até {InboxOptions.MaxConsumerLength} caracteres.");

        var set = db.Set<InboxMessage>();
        if (set.Local.Any(m => m.MessageId == messageId && m.Consumer == consumer))
            return false;
        if (await set.AsNoTracking().AnyAsync(m => m.MessageId == messageId && m.Consumer == consumer, cancellationToken).ConfigureAwait(false))
            return false;

        set.Add(new InboxMessage { MessageId = messageId, Consumer = consumer, ProcessedAt = time.GetUtcNow() });
        return true;
    }

    public Task<int> PurgeAsync(DateTimeOffset processedBefore, int maxRows, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRows, 1);
        var n = ModelBuilderExtensions.NamesOf<InboxMessage>(db);
#pragma warning disable EF1002 // Nomes vêm do modelo (delimitados); valores são parâmetros
        return db.Database.ExecuteSqlRawAsync(
            $"DELETE TOP (@max) FROM {n.Table} WHERE {n["ProcessedAt"]} < @before",
            [new SqlParameter("@max", maxRows), new SqlParameter("@before", processedBefore)],
            cancellationToken);
#pragma warning restore EF1002
    }
}
