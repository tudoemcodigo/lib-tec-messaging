using Microsoft.EntityFrameworkCore;
using Npgsql;
using TEC.Messaging.PostgreSql;
using TEC.Messaging.PostgreSql.Entities;
using TEC.Messaging.PostgreSql.Stores;

namespace TEC.Messaging.Tests.PostgreSql;

public class TestPostgreSqlDbContext(DbContextOptions<TestPostgreSqlDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.AddTecMessagingPostgreSql("custom_schema", "tec_outbox", "tec_inbox");
    }
}

public class PostgreSqlUnitTests
{
    [Test]
    public async Task Model_configures_outbox_and_inbox_tables_and_indexes()
    {
        var builder = new DbContextOptionsBuilder<TestPostgreSqlDbContext>();
        builder.UseInMemoryDatabase(Guid.NewGuid().ToString());

        await using var db = new TestPostgreSqlDbContext(builder.Options);
        var model = db.Model;

        var outboxEntity = model.FindEntityType(typeof(OutboxMessage));
        await Assert.That(outboxEntity).IsNotNull();
        await Assert.That(outboxEntity!.GetTableName()).IsEqualTo("tec_outbox");
        await Assert.That(outboxEntity.GetSchema()).IsEqualTo("custom_schema");

        var inboxEntity = model.FindEntityType(typeof(InboxMessage));
        await Assert.That(inboxEntity).IsNotNull();
        await Assert.That(inboxEntity!.GetTableName()).IsEqualTo("tec_inbox");
        await Assert.That(inboxEntity.GetSchema()).IsEqualTo("custom_schema");

        var names = ModelBuilderExtensions.NamesOf<OutboxMessage>(db);
        await Assert.That(names.Table).IsEqualTo("\"custom_schema\".\"tec_outbox\"");
        await Assert.That(names["Id"]).Contains("\"");
    }

    private static TestPostgreSqlDbContext Context()
    {
        var builder = new DbContextOptionsBuilder<TestPostgreSqlDbContext>();
        builder.UseInMemoryDatabase(Guid.NewGuid().ToString());
        return new TestPostgreSqlDbContext(builder.Options);
    }

    private static PostgresException UniqueViolation(string constraint) =>
        new("duplicate key value violates unique constraint", "ERROR", "ERROR", PostgresErrorCodes.UniqueViolation, constraintName: constraint);

    [Test]
    public async Task Inbox_purge_deletes_by_message_and_consumer()
    {
        // Regressão: a limpeza apagava pelo MessageId, levando registros de outros consumidores ainda dentro da retenção
        await using var db = Context();
        var names = ModelBuilderExtensions.NamesOf<InboxMessage>(db);

        var sql = PostgreSqlInboxStore<TestPostgreSqlDbContext>.PurgeSql(names);

        await Assert.That(sql).Contains($"({names["MessageId"]}, {names["Consumer"]}) IN (");
        await Assert.That(sql).Contains($"SELECT {names["MessageId"]}, {names["Consumer"]} FROM {names.Table}");
    }

    [Test]
    public async Task Only_the_inbox_primary_key_counts_as_duplicate()
    {
        await using var db = Context();
        var store = new PostgreSqlInboxStore<TestPostgreSqlDbContext>(db, TimeProvider.System);
        var key = db.Model.FindEntityType(typeof(InboxMessage))!.FindPrimaryKey()!.GetName()!;

        await Assert.That(store.IsDuplicate(new DbUpdateException("x", UniqueViolation(key)))).IsTrue();
        await Assert.That(store.IsDuplicate(new DbUpdateException("x", UniqueViolation("ix_pedidos_numero")))).IsFalse();
        await Assert.That(store.IsDuplicate(new DbUpdateException("x", new PostgresException("fk", "ERROR", "ERROR", PostgresErrorCodes.ForeignKeyViolation)))).IsFalse();
    }

    [Test]
    public async Task Without_the_inbox_in_the_model_no_unique_violation_counts_as_duplicate()
    {
        // Regressão: sem a chave no modelo, qualquer violação de unicidade (de tabela da aplicação) virava "já processada"
        var builder = new DbContextOptionsBuilder<SemInboxDbContext>();
        builder.UseInMemoryDatabase(Guid.NewGuid().ToString());
        await using var db = new SemInboxDbContext(builder.Options);
        var store = new PostgreSqlInboxStore<SemInboxDbContext>(db, TimeProvider.System);

        await Assert.That(store.IsDuplicate(new DbUpdateException("x", UniqueViolation("ix_pedidos_numero")))).IsFalse();
    }

    [Test]
    public async Task Outbox_rows_are_written_in_utc()
    {
        var envelope = new MessageEnvelope(Guid.NewGuid(), "testes.x", 1, new DateTimeOffset(2026, 10, 29, 9, 0, 0, TimeSpan.FromHours(-3)), null, null, null, "{}");

        var row = OutboxMessage.Create(envelope, new DateTimeOffset(2026, 10, 29, 10, 0, 0, TimeSpan.FromHours(-3)));

        await Assert.That(row.OccurredAt.Offset).IsEqualTo(TimeSpan.Zero);
        await Assert.That(row.NextAttemptAt.Offset).IsEqualTo(TimeSpan.Zero);
        await Assert.That(row.OccurredAt).IsEqualTo(envelope.OccurredAt);
    }
}

public class SemInboxDbContext(DbContextOptions<SemInboxDbContext> options) : DbContext(options);
