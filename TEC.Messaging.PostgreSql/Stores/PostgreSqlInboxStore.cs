using Microsoft.EntityFrameworkCore;
using Npgsql;
using TEC.Messaging.Inbox;
using TEC.Messaging.PostgreSql.Entities;

namespace TEC.Messaging.PostgreSql.Stores;

/// <summary>
/// Inbox no PostgreSQL: o registro entra na unidade de trabalho do contexto e é gravado junto com o efeito do processamento.
/// A chave primária (mensagem + consumidor) impede o processamento duplo concorrente.
/// </summary>
/// <typeparam name="TContext">Contexto com <see cref="ModelBuilderExtensions.AddTecMessagingPostgreSql"/> no modelo.</typeparam>
internal sealed class PostgreSqlInboxStore<TContext>(TContext db, TimeProvider time) : IInboxStore
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

    public bool IsDuplicate(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        // Só a chave primária do Inbox (23505 = unique_violation): duplicidade em tabela da aplicação não pode virar "já
        // processada". Sem a chave no modelo, nada é reconhecido (mesma regra do SQL Server)
        var key = db.Model.FindEntityType(typeof(InboxMessage))?.FindPrimaryKey()?.GetName();
        if (key is null)
            return false;
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation } pg
                && string.Equals(pg.ConstraintName, key, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    public Task<int> PurgeAsync(DateTimeOffset processedBefore, int maxRows, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRows, 1);
        return db.Database.ExecuteSqlRawAsync(
            PurgeSql(ModelBuilderExtensions.NamesOf<InboxMessage>(db)),
            [new NpgsqlParameter("@max", maxRows), new NpgsqlParameter("@before", processedBefore.ToUniversalTime())],
            cancellationToken);
    }

    /// <summary>
    /// Limpeza pela chave inteira (mensagem + consumidor): a mesma mensagem processada por vários consumidores tem um registro
    /// por consumidor, e apagar só pela mensagem levaria junto registros ainda dentro da retenção (reprocessamento na
    /// reentrega). Exposto para os testes.
    /// </summary>
    internal static string PurgeSql(PostgreSqlNames n) => $"""
        DELETE FROM {n.Table}
        WHERE ({n["MessageId"]}, {n["Consumer"]}) IN (
            SELECT {n["MessageId"]}, {n["Consumer"]} FROM {n.Table}
            WHERE {n["ProcessedAt"]} < @before
            LIMIT @max)
        """;
}
