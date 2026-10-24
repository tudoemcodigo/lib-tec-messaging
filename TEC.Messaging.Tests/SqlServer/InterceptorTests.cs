using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using TEC.Core.Domain;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Outbox;
using TEC.Messaging.SqlServer;
using TEC.Messaging.SqlServer.DependencyInjection;
using TEC.Messaging.SqlServer.Entities;
using TEC.Messaging.SqlServer.Interception;
using TEC.Messaging.Tests.Core;

namespace TEC.Messaging.Tests.SqlServer;

public sealed class Pedido : AggregateRoot<Guid>
{
    private Pedido()
    {
    }

    public Pedido(Guid id) => Id = id;

    public decimal Total { get; private set; }

    public void Aprovar(decimal total, DateTimeOffset agora)
    {
        Total = total;
        RaiseDomainEvent(new PedidoAprovado(Id, total, agora));
    }
}

public sealed class Historico
{
    public int Id { get; set; }

    public string Evento { get; set; } = "";
}

internal sealed class VendasDbContext(DbContextOptions<VendasDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Pedido>().HasKey(p => p.Id);
        modelBuilder.Entity<Historico>();
        modelBuilder.AddTecMessaging("vendas");
    }
}

internal sealed class HistoricoObserver : IDomainEventsSavingObserver
{
    public void OnSaving(DbContext context, IReadOnlyList<IDomainEvent> domainEvents)
    {
        foreach (var domainEvent in domainEvents)
            context.Set<Historico>().Add(new Historico { Evento = domainEvent.GetType().Name });
    }
}

/// <summary>Simula a falha do banco no primeiro SaveChanges (depois do interceptor do Outbox).</summary>
internal sealed class FailOnceInterceptor : SaveChangesInterceptor
{
    private int _calls;

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default) =>
        Interlocked.Increment(ref _calls) == 1 ? throw new InvalidOperationException("Banco indisponível.") : ValueTask.FromResult(result);
}

public class InterceptorTests
{
    private static ServiceProvider Build(IInterceptor? extra = null)
    {
        var services = new ServiceCollection();
        services.AddTecMessaging()
            .AddEventType<PedidoAprovadoV2>()
            .AddMapper<PedidoMapper>()
            .UseSqlServer<VendasDbContext>()
            .AddDomainEventsObserver<HistoricoObserver>();
        var database = Guid.NewGuid().ToString();
        services.AddDbContext<VendasDbContext>((sp, o) => 
        {
            o.UseInMemoryDatabase(database).UseTecMessaging(sp);
            if (extra is not null)
                o.AddInterceptors(extra);
        });
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Domain_events_become_outbox_messages_in_the_same_save()
    {
        await using var sp = Build();
        var id = Guid.NewGuid();
        await using (var scope = sp.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<VendasDbContext>();
            var pedido = new Pedido(id);
            pedido.Aprovar(42, DateTimeOffset.UnixEpoch);
            db.Add(pedido);
            await db.SaveChangesAsync();

            await Assert.That(pedido.DomainEvents.Count).IsEqualTo(0);
        }

        await using (var scope = sp.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<VendasDbContext>();
            var message = await db.Set<OutboxMessage>().SingleAsync();
            await Assert.That(message.Type).IsEqualTo("vendas.pedido-aprovado");
            await Assert.That(message.Version).IsEqualTo(2);
            await Assert.That(message.Status).IsEqualTo(OutboxMessageStatus.Pending);
            await Assert.That(message.Payload).Contains(id.ToString());
            await Assert.That(await db.Set<Historico>().CountAsync()).IsEqualTo(1);
        }
    }

    [Test]
    public async Task Failed_save_keeps_events_and_does_not_duplicate_messages_on_retry()
    {
        var failOnce = new FailOnceInterceptor();
        await using var sp = Build(failOnce);
        await using (var scope = sp.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<VendasDbContext>();
            var pedido = new Pedido(Guid.NewGuid());
            pedido.Aprovar(10, DateTimeOffset.UnixEpoch);
            db.Add(pedido);

            await Assert.That(() => db.SaveChangesAsync()).Throws<InvalidOperationException>();
            await Assert.That(pedido.DomainEvents.Count).IsEqualTo(1);

            await db.SaveChangesAsync();
            await Assert.That(pedido.DomainEvents.Count).IsEqualTo(0);
        }

        await using (var scope = sp.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<VendasDbContext>();
            await Assert.That(await db.Set<OutboxMessage>().CountAsync()).IsEqualTo(1);
            await Assert.That(await db.Set<Historico>().CountAsync()).IsEqualTo(1);
        }
    }

    [Test]
    public async Task Outbox_enqueue_writes_with_the_unit_of_work()
    {
        await using var sp = Build();
        await using (var scope = sp.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            var db = scope.ServiceProvider.GetRequiredService<VendasDbContext>();
            var envelope = outbox.Enqueue(new PedidoAprovadoV2(Guid.NewGuid(), 1, StatusDoPedido.Aprovado));
            await Assert.That(await db.Set<OutboxMessage>().CountAsync()).IsEqualTo(0);
            await db.SaveChangesAsync();
            await Assert.That((await db.Set<OutboxMessage>().SingleAsync()).Id).IsEqualTo(envelope.MessageId);
        }
    }

    [Test]
    public async Task Model_maps_tables_in_the_given_schema()
    {
        await using var sp = Build();
        await using var scope = sp.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<VendasDbContext>();
        var names = ModelBuilderExtensions.NamesOf<OutboxMessage>(db);

        await Assert.That(names.Table).IsEqualTo("[vendas].[OutboxMessages]");
        await Assert.That(names["LeaseToken"]).IsEqualTo("[LeaseToken]");
    }
}
