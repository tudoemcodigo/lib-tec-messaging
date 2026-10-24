using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Extensions.DependencyInjection;
using TEC.Core.Domain;
using TEC.Messaging;
using TEC.Messaging.Contracts;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Outbox;

BenchmarkSwitcher.FromAssembly(typeof(EnvelopeBenchmarks).Assembly).Run(args);

[IntegrationEvent("bench.pedido-aprovado")]
public sealed record PedidoAprovadoV1(Guid PedidoId, decimal Total, string Cliente);

public sealed record PedidoAprovado(Guid PedidoId, decimal Total, DateTimeOffset OccurredAt) : IDomainEvent;

[MemoryDiagnoser]
public class EnvelopeBenchmarks
{
    private readonly PedidoAprovado _evento = new(Guid.NewGuid(), 123.45m, DateTimeOffset.UtcNow);
    private ServiceProvider _provider = null!;
    private IntegrationEnvelopeFactory _factory = null!;
    private MessageEnvelope _envelope = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddTecMessaging()
            .AddEventType<PedidoAprovadoV1>()
            .Map<PedidoAprovado>(e => new PedidoAprovadoV1(e.PedidoId, e.Total, "Cliente de teste"));
        _provider = services.BuildServiceProvider();
        _factory = _provider.GetRequiredService<IntegrationEnvelopeFactory>();
        _envelope = _factory.FromDomainEvent(_evento)[0];
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    [Benchmark]
    public int FromDomainEvent() => _factory.FromDomainEvent(_evento).Count;

    [Benchmark]
    public bool Validate() => _envelope.Validate(256 * 1024).IsSuccess;
}
