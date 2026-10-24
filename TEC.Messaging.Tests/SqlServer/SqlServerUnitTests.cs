using System.Diagnostics.Metrics;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Inbox;
using TEC.Messaging.Outbox;
using TEC.Messaging.SqlServer;
using TEC.Messaging.SqlServer.DependencyInjection;
using TEC.Messaging.SqlServer.Entities;
using TEC.Messaging.Tests.Core;
using TEC.Messaging.Tests.Shared;

namespace TEC.Messaging.Tests.SqlServer;

/// <summary>Contexto com a coluna Status renomeada antes do AddTecMessaging (como faria uma convenção de nomes).</summary>
internal sealed class SnakeCaseDbContext(DbContextOptions<SnakeCaseDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessage>().Property(m => m.Status).HasColumnName("status");
        modelBuilder.AddTecMessaging("mensageria");
    }
}

/// <summary>Modelo, validações e métricas do satélite SQL Server sem banco (o provedor monta o modelo sem conectar).</summary>
public class SqlServerUnitTests
{
    private const string NoServer = "Server=127.0.0.1,1;Database=nenhum;User ID=x;Password=y;Encrypt=False;Connect Timeout=1";

    private static ServiceProvider BuildSqlServer()
    {
        var services = new ServiceCollection();
        services.AddTecMessaging().AddEventType<PedidoAprovadoV2>().UseSqlServer<VendasDbContext>();
        services.AddDbContext<VendasDbContext>((sp, o) => o.UseSqlServer(NoServer).UseTecMessaging(sp));
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Index_filters_use_the_real_column_name()
    {
        // Regressão B9: filtros fixos em [Status] quebravam com convenção de nomes (ex.: snake_case)
        var options = new DbContextOptionsBuilder<SnakeCaseDbContext>().UseSqlServer(NoServer).Options;
        await using var db = new SnakeCaseDbContext(options);
        var entity = db.Model.FindEntityType(typeof(OutboxMessage))!;
        var filters = entity.GetIndexes().ToDictionary(i => i.GetDatabaseName()!, i => i.GetFilter());

        await Assert.That(filters["IX_OutboxMessages_Pending"]).IsEqualTo("[status] = 0");
        await Assert.That(filters["IX_OutboxMessages_Published"]).IsEqualTo("[status] = 1");
        await Assert.That(filters["IX_OutboxMessages_Dead"]).IsEqualTo("[status] = 2");
    }

    [Test]
    public async Task Statistics_indexes_cover_pending_and_dead_messages()
    {
        // M2: as estatísticas do relay só leem índices das pendentes e das mortas
        await using var sp = BuildSqlServer();
        await using var scope = sp.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<VendasDbContext>();
        var indexes = db.GetService<IDesignTimeModel>().Model.FindEntityType(typeof(OutboxMessage))!.GetIndexes().ToDictionary(i => i.GetDatabaseName()!);

        await Assert.That(SqlServerIndexExtensions.GetIncludeProperties(indexes["IX_OutboxMessages_Pending"])!).Contains(nameof(OutboxMessage.Attempts));
        await Assert.That(indexes["IX_OutboxMessages_Dead"].Properties.Select(p => p.Name)).IsEquivalentTo(new[] { nameof(OutboxMessage.OccurredAt) });
    }

    [Test]
    public async Task Requeue_with_an_empty_selection_is_rejected()
    {
        // Regressão M3: coleção vazia reprocessava todas as mortas (mesmo efeito de null)
        await using var sp = BuildSqlServer();
        await using var scope = sp.CreateAsyncScope();
        var admin = scope.ServiceProvider.GetRequiredService<IOutboxAdministration>();

        await Assert.That(async () => await admin.RequeueAsync([], CancellationToken.None)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Enqueued_metric_counts_only_after_a_successful_save()
    {
        // Regressão B8: IOutbox.Enqueue contava tec.messaging.outbox.enqueued antes do SaveChanges
        var services = new ServiceCollection();
        services.AddTecMessaging().AddEventType<PedidoAprovadoV2>().UseSqlServer<VendasDbContext>();
        var database = Guid.NewGuid().ToString();
        services.AddDbContext<VendasDbContext>((sp, o) => o.UseInMemoryDatabase(database).UseTecMessaging(sp));
        await using var sp = services.BuildServiceProvider();
        var meters = sp.GetRequiredService<IMeterFactory>();
        long enqueued = 0;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Scope == meters && instrument.Name == "tec.messaging.outbox.enqueued")
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref enqueued, value));
        listener.Start();
        _ = sp.GetRequiredService<MessagingMetrics>();

        await using (var scope = sp.CreateAsyncScope())
        {
            scope.ServiceProvider.GetRequiredService<IOutbox>().Enqueue(new PedidoAprovadoV2(Guid.NewGuid(), 1, StatusDoPedido.Aprovado));
            await Assert.That(Interlocked.Read(ref enqueued)).IsEqualTo(0);
        }

        await using (var scope = sp.CreateAsyncScope())
        {
            scope.ServiceProvider.GetRequiredService<IOutbox>().Enqueue(new PedidoAprovadoV2(Guid.NewGuid(), 1, StatusDoPedido.Aprovado));
            await scope.ServiceProvider.GetRequiredService<VendasDbContext>().SaveChangesAsync();
            await Assert.That(Interlocked.Read(ref enqueued)).IsEqualTo(1);
        }
    }

    [Test]
    public async Task Inbox_cleanup_purges_in_batches_until_the_backlog_is_gone()
    {
        // Regressão M1: uma limpeza apagava no máximo 5000 registros por CleanupInterval
        var store = new CountingInboxStore([5000, 5000, 37]);
        var services = new ServiceCollection();
        services.AddSingleton<IInboxStore>(store);
        await using var sp = services.BuildServiceProvider();
        using var cleanup = new InboxCleanupService(
            sp.GetRequiredService<IServiceScopeFactory>(), Options.Create(new InboxOptions()), TimeProvider.System, NullLogger<InboxCleanupService>.Instance);

        var removed = await cleanup.CleanupAsync(new InboxOptions(), CancellationToken.None);

        await Assert.That(removed).IsEqualTo(10_037);
        await Assert.That(store.Calls).IsEqualTo(3);
    }

    private sealed class CountingInboxStore(int[] results) : IInboxStore
    {
        public int Calls { get; private set; }

        public Task<bool> TryBeginAsync(Guid messageId, string consumer, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<int> PurgeAsync(DateTimeOffset processedBefore, int maxRows, CancellationToken cancellationToken) =>
            Task.FromResult(Calls < results.Length ? results[Calls++] : 0);
    }
}
