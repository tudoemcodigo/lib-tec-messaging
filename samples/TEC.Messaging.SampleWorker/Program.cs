using Microsoft.EntityFrameworkCore;
using TEC.Core.Common.Results;
using TEC.Core.Domain;
using TEC.Messaging.Contracts;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Inbox;
using TEC.Messaging.RabbitMQ;
using TEC.Messaging.RabbitMQ.Consumers;
using TEC.Messaging.RabbitMQ.DependencyInjection;
using TEC.Messaging.SqlServer;
using TEC.Messaging.SqlServer.DependencyInjection;
using TEC.Vault.DependencyInjection;
using TEC.Vault.InMemory;

// Exemplo: o pedido aprovado vira evento de domínio → Outbox (mesma transação) → relay → RabbitMQ → consumidor idempotente.
var builder = Host.CreateApplicationBuilder(args);
var sql = builder.Configuration["TecTestes:MessagingSqlConexao"] ?? throw new InvalidOperationException("Configure TecTestes:MessagingSqlConexao.");
var rabbit = builder.Configuration["TecTestes:MessagingRabbitUri"] ?? throw new InvalidOperationException("Configure TecTestes:MessagingRabbitUri.");

builder.Services.AddTecVault(v => v.UseInMemory(o => o.InitialSecrets["rabbitmq"] = rabbit));
builder.Services.AddTecMessaging()
    .AddEventType<PedidoAprovadoV1>()
    .Map<PedidoAprovado>(e => new PedidoAprovadoV1(e.PedidoId, e.Total))
    .UseSqlServer<VendasDbContext>()
    .UseRabbitMq(o =>
    {
        o.Exchange = "exemplo.vendas";
        o.RoutingKeyPrefixToRemove = "vendas.";
    })
    .AddOutboxRelay()
    .AddInboxCleanup()
    .AddConsumer<FaturamentoConsumer>();
builder.Services.AddDbContext<VendasDbContext>((sp, o) => o.UseSqlServer(sql).UseTecMessaging(sp));

var host = builder.Build();
await using (var scope = host.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<VendasDbContext>();
    await db.Database.EnsureCreatedAsync();
    var pedido = new Pedido(Guid.NewGuid());
    pedido.Aprovar(199.90m, DateTimeOffset.UtcNow);
    db.Add(pedido);
    await db.SaveChangesAsync();
}

await host.RunAsync();

[IntegrationEvent("vendas.pedido-aprovado")]
public sealed record PedidoAprovadoV1(Guid PedidoId, decimal Total);

public sealed record PedidoAprovado(Guid PedidoId, decimal Total, DateTimeOffset OccurredAt) : IDomainEvent;

public sealed class Pedido : AggregateRoot<Guid>
{
    private Pedido()
    {
    }

    public Pedido(Guid id) => Id = id;

    public decimal Total { get; private set; }

    public Result Aprovar(decimal total, DateTimeOffset agora)
    {
        Total = total;
        RaiseDomainEvent(new PedidoAprovado(Id, total, agora));
        return Result.Success();
    }
}

public sealed class VendasDbContext(DbContextOptions<VendasDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Pedido>(b =>
        {
            b.ToTable("Pedidos", "exemplo");
            b.Property(p => p.Total).HasPrecision(18, 2);
        });
        modelBuilder.AddTecMessaging("exemplo");
    }
}

/// <summary>Consumidor idempotente: o Inbox e o efeito são gravados na mesma transação.</summary>
public sealed partial class FaturamentoConsumer(
    RabbitMqConsumerDependencies dependencies, IServiceScopeFactory scopes, ILogger<FaturamentoConsumer> logger) : RabbitMqConsumer(dependencies)
{
    protected override QueueDefinition Queue { get; } = new("exemplo.faturamento", ["pedido-aprovado"]);

    protected override async Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken)
    {
        await using var scope = scopes.CreateAsyncScope();
        var inbox = scope.ServiceProvider.GetRequiredService<IInboxStore>();
        if (!await inbox.TryBeginAsync(message.Envelope.MessageId, "faturamento", cancellationToken))
            return ConsumeResult.Completed;

        LogFaturado(message.Envelope.MessageId);
        await scope.ServiceProvider.GetRequiredService<VendasDbContext>().SaveChangesAsync(cancellationToken);
        return ConsumeResult.Completed;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Pedido da mensagem {MessageId} faturado")]
    private partial void LogFaturado(Guid messageId);
}
