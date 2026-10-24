using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using TEC.Messaging.Inbox;
using TEC.Messaging.Outbox;
using TEC.Messaging.PostgreSql.Entities;

namespace TEC.Messaging.PostgreSql;

/// <summary>Mapeamento das tabelas do Outbox e do Inbox no modelo do EF Core para PostgreSQL.</summary>
public static class ModelBuilderExtensions
{
    /// <summary>
    /// Mapeia <see cref="OutboxMessage"/> e <see cref="InboxMessage"/> para PostgreSQL (chame no <c>OnModelCreating</c> e gere a migration).
    /// </summary>
    /// <param name="modelBuilder">Modelo.</param>
    /// <param name="schema">Esquema (<c>null</c> = o padrão do modelo, normalmente <c>public</c>).</param>
    /// <param name="outboxTable">Tabela do Outbox.</param>
    /// <param name="inboxTable">Tabela do Inbox.</param>
    /// <returns>O modelo.</returns>
    public static ModelBuilder AddTecMessagingPostgreSql(
        this ModelBuilder modelBuilder, string? schema = null, string outboxTable = "outbox_messages", string inboxTable = "inbox_messages")
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(outboxTable);
        ArgumentException.ThrowIfNullOrWhiteSpace(inboxTable);

        modelBuilder.Entity<OutboxMessage>(b =>
        {
            b.ToTable(outboxTable, schema);
            b.HasKey(m => m.Id);
            b.Property(m => m.Id).ValueGeneratedNever();
            b.Property(m => m.Type).HasMaxLength(MessageEnvelope.MaxTypeLength).IsRequired();
            b.Property(m => m.CorrelationId).HasMaxLength(MessageEnvelope.MaxIdLength);
            b.Property(m => m.CausationId).HasMaxLength(MessageEnvelope.MaxIdLength);
            b.Property(m => m.TraceParent).HasMaxLength(MessageEnvelope.MaxTraceParentLength);
            b.Property(m => m.Payload).HasColumnType("text").IsRequired();
            b.Property(m => m.Status).HasConversion<short>();
            b.Property(m => m.LastError).HasMaxLength(OutboxOutcome.MaxErrorLength);

            var status = b.Property(m => m.Status).Metadata.GetColumnName();

            b.HasIndex(m => new { m.OccurredAt, m.Id })
                .HasFilter(StatusFilter(status, OutboxMessageStatus.Pending))
                .IncludeProperties(m => new { m.NextAttemptAt, m.LeasedUntil, m.Attempts })
                .HasDatabaseName($"ix_{outboxTable.ToLowerInvariant()}_pending");

            b.HasIndex(m => m.PublishedAt)
                .HasFilter(StatusFilter(status, OutboxMessageStatus.Published))
                .HasDatabaseName($"ix_{outboxTable.ToLowerInvariant()}_published");

            b.HasIndex(m => m.OccurredAt)
                .HasFilter(StatusFilter(status, OutboxMessageStatus.Dead))
                .HasDatabaseName($"ix_{outboxTable.ToLowerInvariant()}_dead");
        });

        modelBuilder.Entity<InboxMessage>(b =>
        {
            b.ToTable(inboxTable, schema);
            b.HasKey(m => new { m.MessageId, m.Consumer });
            b.Property(m => m.Consumer).HasMaxLength(InboxOptions.MaxConsumerLength);
            b.HasIndex(m => m.ProcessedAt).HasDatabaseName($"ix_{inboxTable.ToLowerInvariant()}_processed_at");
        });

        return modelBuilder;
    }

    /// <summary>Nomes reais (tabela e colunas) de uma entidade mapeada, para o SQL escrito à mão.</summary>
    internal static PostgreSqlNames NamesOf<TEntity>(DbContext context)
    {
        var entity = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException($"{typeof(TEntity).Name} não está no modelo de {context.GetType().Name}: chame modelBuilder.AddTecMessagingPostgreSql() no OnModelCreating.");
        var table = entity.GetTableName()!;
        var schema = entity.GetSchema();
        var store = StoreObjectIdentifier.Table(table, schema);
        var columns = entity.GetProperties().ToDictionary(p => p.Name, p => Quote(p.GetColumnName(store)!), StringComparer.Ordinal);
        return new PostgreSqlNames(schema is null ? Quote(table) : $"{Quote(schema)}.{Quote(table)}", columns);
    }

    private static string StatusFilter(string column, OutboxMessageStatus status) =>
        string.Create(CultureInfo.InvariantCulture, $"{Quote(column)} = {(short)status}");

    private static string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
}

/// <summary>Tabela e colunas já delimitadas (<c>"nome"</c>).</summary>
internal sealed record PostgreSqlNames(string Table, IReadOnlyDictionary<string, string> Columns)
{
    public string this[string property] => Columns[property];
}
