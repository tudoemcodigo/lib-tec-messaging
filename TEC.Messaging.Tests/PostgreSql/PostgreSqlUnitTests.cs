using Microsoft.EntityFrameworkCore;
using TEC.Messaging.PostgreSql;
using TEC.Messaging.PostgreSql.Entities;

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
}
