using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using TEC.Messaging.Inbox;
using TEC.Messaging.Outbox;
using TEC.Messaging.SqlServer.Entities;

namespace TEC.Messaging.SqlServer;

/// <summary>Mapeamento das tabelas do Outbox e do Inbox no modelo do EF Core.</summary>
public static class ModelBuilderExtensions
{
    /// <summary>
    /// Mapeia <see cref="OutboxMessage"/> e <see cref="InboxMessage"/> (chame no <c>OnModelCreating</c> e gere a migration).
    /// </summary>
    /// <param name="modelBuilder">Modelo.</param>
    /// <param name="schema">Esquema (<c>null</c> = o padrão do modelo).</param>
    /// <param name="outboxTable">Tabela do Outbox.</param>
    /// <param name="inboxTable">Tabela do Inbox.</param>
    /// <returns>O modelo.</returns>
    public static ModelBuilder AddTecMessaging(
        this ModelBuilder modelBuilder, string? schema = null, string outboxTable = "OutboxMessages", string inboxTable = "InboxMessages")
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(outboxTable);
        ArgumentException.ThrowIfNullOrWhiteSpace(inboxTable);

        modelBuilder.Entity<OutboxMessage>(b =>
        {
            b.ToTable(outboxTable, schema);
            b.HasKey(m => m.Id);
            b.Property(m => m.Id).ValueGeneratedNever();
            b.Property(m => m.Type).HasMaxLength(MessageEnvelope.MaxTypeLength).IsUnicode(false).IsRequired();
            b.Property(m => m.CorrelationId).HasMaxLength(MessageEnvelope.MaxIdLength).IsUnicode(false);
            b.Property(m => m.CausationId).HasMaxLength(MessageEnvelope.MaxIdLength).IsUnicode(false);
            b.Property(m => m.TraceParent).HasMaxLength(MessageEnvelope.MaxTraceParentLength).IsUnicode(false);
            b.Property(m => m.Payload).HasColumnType("nvarchar(max)").IsRequired();
            b.Property(m => m.Status).HasConversion<byte>();
            b.Property(m => m.LastError).HasMaxLength(OutboxOutcome.MaxErrorLength);
            // Na ordem da reserva (OccurredAt, Id): o TOP com UPDLOCK/READPAST lê as pendentes em ordem e para no tamanho do lote,
            // travando só as linhas que reserva. Com outra ordem no índice o SQL Server leria e ordenaria todas as pendentes,
            // travando-as de passagem, e reservas simultâneas pulariam (READPAST) mensagens que nenhuma delas pegaria
            b.HasIndex(m => new { m.OccurredAt, m.Id })
                .HasFilter("[Status] = 0")
                .IncludeProperties(m => new { m.NextAttemptAt, m.LeasedUntil })
                .HasDatabaseName($"IX_{outboxTable}_Pending");
            b.HasIndex(m => m.PublishedAt).HasFilter("[Status] = 1").HasDatabaseName($"IX_{outboxTable}_Published");
        });

        modelBuilder.Entity<InboxMessage>(b =>
        {
            b.ToTable(inboxTable, schema);
            b.HasKey(m => new { m.MessageId, m.Consumer });
            b.Property(m => m.Consumer).HasMaxLength(InboxOptions.MaxConsumerLength).IsUnicode(false);
            b.HasIndex(m => m.ProcessedAt);
        });

        return modelBuilder;
    }

    /// <summary>Nomes reais (tabela e colunas) de uma entidade mapeada, para o SQL escrito à mão.</summary>
    internal static SqlNames NamesOf<TEntity>(DbContext context)
    {
        var entity = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException($"{typeof(TEntity).Name} não está no modelo de {context.GetType().Name}: chame modelBuilder.AddTecMessaging() no OnModelCreating.");
        var table = entity.GetTableName()!;
        var schema = entity.GetSchema();
        var store = StoreObjectIdentifier.Table(table, schema);
        var columns = entity.GetProperties().ToDictionary(p => p.Name, p => Quote(p.GetColumnName(store)!), StringComparer.Ordinal);
        return new SqlNames(schema is null ? Quote(table) : $"{Quote(schema)}.{Quote(table)}", columns);
    }

    private static string Quote(string identifier) => $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";
}

/// <summary>Tabela e colunas já delimitadas (<c>[nome]</c>).</summary>
internal sealed record SqlNames(string Table, IReadOnlyDictionary<string, string> Columns)
{
    public string this[string property] => Columns[property];
}
