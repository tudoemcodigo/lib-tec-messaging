using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Inbox;
using TEC.Messaging.Outbox;
using TEC.Messaging.SqlServer;
using TEC.Messaging.SqlServer.DependencyInjection;
using TEC.Messaging.SqlServer.Entities;
using TEC.Messaging.SqlServer.Stores;
using TEC.Messaging.Tests.Core;
using TEC.Messaging.Tests.Shared;

namespace TEC.Messaging.Tests.SqlServer;

internal sealed class IntegracaoDbContext(DbContextOptions<IntegracaoDbContext> options) : DbContext(options)
{
    public const string Schema = "tecmsg_testes";

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Pedido>(b =>
        {
            b.ToTable("Pedidos", Schema);
            b.HasKey(p => p.Id);
            b.Property(p => p.Total).HasPrecision(18, 2);
        });
        modelBuilder.AddTecMessaging(Schema);
    }
}

/// <summary>Outbox e Inbox no SQL Server real (lease concorrente, conclusão, administração, limpeza).</summary>
[Category(TestCategories.Integration)]
[NotInParallel("tec-messaging-sql")]
public class SqlServerStoreIntegrationTests
{
    private static readonly SemaphoreSlim SchemaLock = new(1, 1);
    private static bool _schemaReady;

    private static async Task<ServiceProvider> BuildAsync()
    {
        var connection = TestSettings.SqlConnection;
        Skip.When(connection is null, $"Sem SQL Server de testes: defina {TestSettings.SqlVariable} (ou TecTestes:MessagingSqlConexao no user-secrets).");
        Skip.When(AppContext.TryGetSwitch("System.Globalization.Invariant", out var invariant) && invariant, "Microsoft.Data.SqlClient exige ICU.");

        var services = new ServiceCollection();
        services.AddTecMessaging()
            .AddEventType<PedidoAprovadoV2>()
            .AddMapper<PedidoMapper>()
            .UseSqlServer<IntegracaoDbContext>();
        services.AddDbContext<IntegracaoDbContext>((sp, o) => o.UseSqlServer(connection).UseTecMessaging(sp));
        var provider = services.BuildServiceProvider();

        await SchemaLock.WaitAsync();
        try
        {
            await using var scope = provider.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<IntegracaoDbContext>();
            if (!_schemaReady)
            {
                await db.Database.ExecuteSqlRawAsync($"""
                    IF OBJECT_ID('{IntegracaoDbContext.Schema}.OutboxMessages') IS NOT NULL DROP TABLE {IntegracaoDbContext.Schema}.OutboxMessages;
                    IF OBJECT_ID('{IntegracaoDbContext.Schema}.InboxMessages') IS NOT NULL DROP TABLE {IntegracaoDbContext.Schema}.InboxMessages;
                    IF OBJECT_ID('{IntegracaoDbContext.Schema}.Pedidos') IS NOT NULL DROP TABLE {IntegracaoDbContext.Schema}.Pedidos;
                    """);
                await db.GetService<IRelationalDatabaseCreator>().CreateTablesAsync();
                _schemaReady = true;
            }
            else
            {
                await db.Database.ExecuteSqlRawAsync($"DELETE FROM {IntegracaoDbContext.Schema}.OutboxMessages; DELETE FROM {IntegracaoDbContext.Schema}.InboxMessages; DELETE FROM {IntegracaoDbContext.Schema}.Pedidos;");
            }
        }
        finally
        {
            SchemaLock.Release();
        }

        return provider;
    }

    private static async Task SeedAsync(ServiceProvider sp, int count)
    {
        await using var scope = sp.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<IntegracaoDbContext>();
        for (var i = 0; i < count; i++)
        {
            var pedido = new Pedido(Guid.NewGuid());
            pedido.Aprovar(i, DateTimeOffset.UtcNow.AddMilliseconds(-count + i));
            db.Add(pedido);
        }

        await db.SaveChangesAsync();
    }

    [Test]
    public async Task Aggregate_events_are_written_with_the_aggregate()
    {
        await using var sp = await BuildAsync();
        await SeedAsync(sp, 3);

        await using var scope = sp.CreateAsyncScope();
        var stats = await scope.ServiceProvider.GetRequiredService<IOutboxAdministration>().GetStatisticsAsync(CancellationToken.None);
        await Assert.That(stats.Pending).IsEqualTo(3);
        await Assert.That(stats.OldestPendingOccurredAt).IsNotNull();
    }

    [Test]
    public async Task Concurrent_leases_never_overlap()
    {
        await using var sp = await BuildAsync();
        await SeedAsync(sp, 60);

        async Task<IReadOnlyList<LeasedOutboxMessage>> LeaseAsync()
        {
            await using var scope = sp.CreateAsyncScope();
            return await scope.ServiceProvider.GetRequiredService<IOutboxStore>().LeaseAsync(25, TimeSpan.FromMinutes(1), CancellationToken.None);
        }

        var batches = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => LeaseAsync()));
        var ids = batches.SelectMany(b => b.Select(m => m.Envelope.MessageId)).ToList();

        await Assert.That(ids.Count).IsEqualTo(60);
        await Assert.That(ids.Distinct().Count()).IsEqualTo(60);
        await Assert.That((await LeaseAsync()).Count).IsEqualTo(0);
    }

    [Test]
    public async Task Complete_applies_outcomes_only_with_the_current_lease()
    {
        await using var sp = await BuildAsync();
        await SeedAsync(sp, 3);
        await using var scope = sp.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        var admin = scope.ServiceProvider.GetRequiredService<IOutboxAdministration>();

        var leased = await store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None);
        await store.CompleteAsync(
        [
            new OutboxOutcome(leased[0].Envelope.MessageId, leased[0].LeaseToken, OutboxOutcomeKind.Published, 0, null, null),
            new OutboxOutcome(leased[1].Envelope.MessageId, leased[1].LeaseToken, OutboxOutcomeKind.Dead, 10, null, "falhou"),
            new OutboxOutcome(leased[2].Envelope.MessageId, Guid.NewGuid(), OutboxOutcomeKind.Published, 0, null, null),
        ], CancellationToken.None);

        var stats = await admin.GetStatisticsAsync(CancellationToken.None);
        await Assert.That(stats.Published).IsEqualTo(1);
        await Assert.That(stats.Dead).IsEqualTo(1);
        await Assert.That(stats.Pending).IsEqualTo(1);

        var failed = await admin.ListFailedAsync(10, CancellationToken.None);
        await Assert.That(failed.Single().LastError).IsEqualTo("falhou");

        await Assert.That(await admin.RequeueAsync(null, CancellationToken.None)).IsEqualTo(1);
        await Assert.That((await admin.GetStatisticsAsync(CancellationToken.None)).Dead).IsEqualTo(0);
    }

    [Test]
    public async Task Discard_removes_only_dead_messages_and_purge_removes_old_published()
    {
        await using var sp = await BuildAsync();
        await SeedAsync(sp, 2);
        await using var scope = sp.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        var admin = scope.ServiceProvider.GetRequiredService<IOutboxAdministration>();
        var leased = await store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None);
        await store.CompleteAsync(
        [
            new OutboxOutcome(leased[0].Envelope.MessageId, leased[0].LeaseToken, OutboxOutcomeKind.Dead, 3, null, "x"),
            new OutboxOutcome(leased[1].Envelope.MessageId, leased[1].LeaseToken, OutboxOutcomeKind.Published, 0, null, null),
        ], CancellationToken.None);

        await Assert.That(await admin.DiscardAsync([leased[0].Envelope.MessageId, leased[1].Envelope.MessageId], CancellationToken.None)).IsEqualTo(1);
        await Assert.That(await store.PurgePublishedAsync(DateTimeOffset.UtcNow.AddMinutes(1), 100, CancellationToken.None)).IsEqualTo(1);
        var stats = await admin.GetStatisticsAsync(CancellationToken.None);
        await Assert.That(stats.Published + stats.Dead + stats.Pending).IsEqualTo(0);
    }

    [Test]
    public async Task Inbox_detects_duplicates_across_units_of_work()
    {
        await using var sp = await BuildAsync();
        var messageId = Guid.NewGuid();

        await using (var scope = sp.CreateAsyncScope())
        {
            var inbox = scope.ServiceProvider.GetRequiredService<IInboxStore>();
            await Assert.That(await inbox.TryBeginAsync(messageId, "faturamento", CancellationToken.None)).IsTrue();
            await Assert.That(await inbox.TryBeginAsync(messageId, "faturamento", CancellationToken.None)).IsFalse();
            await scope.ServiceProvider.GetRequiredService<IntegracaoDbContext>().SaveChangesAsync();
        }

        await using (var scope = sp.CreateAsyncScope())
        {
            var inbox = scope.ServiceProvider.GetRequiredService<IInboxStore>();
            await Assert.That(await inbox.TryBeginAsync(messageId, "faturamento", CancellationToken.None)).IsFalse();
            await Assert.That(await inbox.TryBeginAsync(messageId, "estoque", CancellationToken.None)).IsTrue();
            await Assert.That(await inbox.PurgeAsync(DateTimeOffset.UtcNow.AddMinutes(1), 100, CancellationToken.None)).IsEqualTo(1);
        }
    }

    [Test]
    public async Task Requeue_does_not_touch_messages_being_published()
    {
        // Regressão B2: RequeueAsync zerava a reserva vigente e outro relay publicava a mensagem de novo
        await using var sp = await BuildAsync();
        await SeedAsync(sp, 1);
        await using var scope = sp.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        var admin = scope.ServiceProvider.GetRequiredService<IOutboxAdministration>();
        var first = (await store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None)).Single();
        await store.CompleteAsync(
            [new OutboxOutcome(first.Envelope.MessageId, first.LeaseToken, OutboxOutcomeKind.Failed, 1, DateTimeOffset.UtcNow.AddMinutes(-1), "falhou")],
            CancellationToken.None);

        // Em publicação de novo (reserva vigente) quando o administrador pede o reprocessamento
        var publishing = (await store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None)).Single();
        await Assert.That(await admin.RequeueAsync(null, CancellationToken.None)).IsEqualTo(0);
        await Assert.That((await store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None)).Count).IsEqualTo(0);

        await store.CompleteAsync([new OutboxOutcome(publishing.Envelope.MessageId, publishing.LeaseToken, OutboxOutcomeKind.Published, 1, null, null)], CancellationToken.None);
        await Assert.That((await admin.GetStatisticsAsync(CancellationToken.None)).Published).IsEqualTo(1);
    }

    [Test]
    public async Task Lease_validity_uses_the_database_clock()
    {
        // Regressão B1: uma instância com o relógio adiantado assumia o lote ainda reservado por outra
        await using var sp = await BuildAsync();
        await SeedAsync(sp, 3);
        await using var scope = sp.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<IntegracaoDbContext>();
        var leased = await scope.ServiceProvider.GetRequiredService<IOutboxStore>().LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None);
        await Assert.That(leased.Count).IsEqualTo(3);

        var ahead = new SqlServerOutboxStore<IntegracaoDbContext>(db, new ShiftedClock(TimeSpan.FromMinutes(10)));
        await Assert.That((await ahead.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None)).Count).IsEqualTo(0);
    }

    [Test]
    public async Task Relay_statistics_skip_the_published_messages()
    {
        // M2: as estatísticas do relay não contam (nem varrem) as publicadas retidas
        await using var sp = await BuildAsync();
        await SeedAsync(sp, 4);
        await using var scope = sp.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
        var admin = scope.ServiceProvider.GetRequiredService<IOutboxAdministration>();
        var leased = await store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None);
        await store.CompleteAsync(
        [
            new OutboxOutcome(leased[0].Envelope.MessageId, leased[0].LeaseToken, OutboxOutcomeKind.Published, 0, null, null),
            new OutboxOutcome(leased[1].Envelope.MessageId, leased[1].LeaseToken, OutboxOutcomeKind.Dead, 10, null, "x"),
            new OutboxOutcome(leased[2].Envelope.MessageId, leased[2].LeaseToken, OutboxOutcomeKind.Failed, 1, DateTimeOffset.UtcNow, "y"),
            new OutboxOutcome(leased[3].Envelope.MessageId, leased[3].LeaseToken, OutboxOutcomeKind.Released, 0, DateTimeOffset.UtcNow, null),
        ], CancellationToken.None);

        var relay = await admin.GetPendingStatisticsAsync(CancellationToken.None);
        var full = await admin.GetStatisticsAsync(CancellationToken.None);

        await Assert.That(relay).IsEqualTo(full with { Published = 0 });
        await Assert.That((relay.Pending, relay.PendingWithErrors, relay.Dead, full.Published)).IsEqualTo((2, 1, 1, 1));
    }

    [Test]
    public async Task Inbox_recognizes_only_its_own_duplicate_key()
    {
        // B3: a entrega concorrente que perde a corrida pode terminar como processada, sem nova tentativa
        await using var sp = await BuildAsync();
        var messageId = Guid.NewGuid();
        await using var first = sp.CreateAsyncScope();
        await using var second = sp.CreateAsyncScope();
        var inbox = second.ServiceProvider.GetRequiredService<IInboxStore>();
        await Assert.That(await first.ServiceProvider.GetRequiredService<IInboxStore>().TryBeginAsync(messageId, "faturamento", CancellationToken.None)).IsTrue();
        await Assert.That(await inbox.TryBeginAsync(messageId, "faturamento", CancellationToken.None)).IsTrue();
        await first.ServiceProvider.GetRequiredService<IntegracaoDbContext>().SaveChangesAsync();

        var duplicate = await Assert.That(async () => await second.ServiceProvider.GetRequiredService<IntegracaoDbContext>().SaveChangesAsync())
            .Throws<DbUpdateException>();
        await Assert.That(inbox.IsDuplicate(duplicate!)).IsTrue();

        // Chave duplicada de uma tabela da aplicação não é "já processada"
        var id = Guid.NewGuid();
        await using (var scope = sp.CreateAsyncScope())
        {
            scope.ServiceProvider.GetRequiredService<IntegracaoDbContext>().Add(new Pedido(id));
            await scope.ServiceProvider.GetRequiredService<IntegracaoDbContext>().SaveChangesAsync();
        }

        await using (var scope = sp.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<IntegracaoDbContext>();
            db.Add(new Pedido(id));
            var other = await Assert.That(async () => await db.SaveChangesAsync()).Throws<DbUpdateException>();
            await Assert.That(scope.ServiceProvider.GetRequiredService<IInboxStore>().IsDuplicate(other!)).IsFalse();
        }
    }

    private sealed class ShiftedClock(TimeSpan offset) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UtcNow + offset;
    }
}
