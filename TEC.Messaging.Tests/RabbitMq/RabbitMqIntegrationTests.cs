using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using TEC.Messaging.Administration;
using TEC.Messaging.Context;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.RabbitMQ;
using TEC.Messaging.RabbitMQ.Consumers;
using TEC.Messaging.RabbitMQ.DependencyInjection;
using TEC.Messaging.Tests.Shared;
using TEC.Messaging.Transport;
using TEC.Vault.DependencyInjection;
using TEC.Vault.InMemory;

namespace TEC.Messaging.Tests.RabbitMq;

/// <summary>Comportamento configurável dos consumidores de teste e o que eles receberam.</summary>
internal sealed class Probe
{
    public ConcurrentQueue<(ReceivedMessage Message, string? Correlation, string? Causation)> Received { get; } = new();

    public Func<ReceivedMessage, ConsumeResult> Behavior { get; set; } = _ => ConsumeResult.Completed;

    public async Task<bool> WaitAsync(Func<int, bool> condition, TimeSpan timeout)
    {
        var until = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < until)
        {
            if (condition(Received.Count))
                return true;
            await Task.Delay(100);
        }

        return condition(Received.Count);
    }
}

internal sealed class ProbeConsumer(RabbitMqConsumerDependencies dependencies, Probe probe, QueueDefinition queue) : RabbitMqConsumer(dependencies)
{
    protected override QueueDefinition Queue { get; } = queue;

    protected override Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken)
    {
        probe.Received.Enqueue((message, MessageContext.CorrelationId, MessageContext.CausationId));
        return Task.FromResult(probe.Behavior(message));
    }
}

/// <summary>Publicação, consumo, nova tentativa e DLQ num RabbitMQ real.</summary>
[Category(TestCategories.Integration)]
public class RabbitMqIntegrationTests
{
    private sealed class Broker : IAsyncDisposable
    {
        private readonly IHost _host;
        private readonly string _uri;

        private Broker(IHost host, string uri, string exchange, QueueDefinition queue, Probe probe)
        {
            _host = host;
            _uri = uri;
            Exchange = exchange;
            Queue = queue;
            Probe = probe;
        }

        public string Exchange { get; }

        public QueueDefinition Queue { get; }

        public Probe Probe { get; }

        public IServiceProvider Services => _host.Services;

        public static async Task<Broker> StartAsync(Func<string, QueueDefinition> queueFactory)
        {
            var uri = TestSettings.RabbitUri;
            Skip.When(uri is null, $"Sem RabbitMQ de testes: defina {TestSettings.RabbitVariable} (ou TecTestes:MessagingRabbitUri no user-secrets).");

            var suffix = Guid.NewGuid().ToString("N")[..8];
            var exchange = $"tec-testes.{suffix}";
            var queue = queueFactory($"tec-testes.{suffix}");
            var probe = new Probe();

            var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = Environments.Development });
            builder.Logging.SetMinimumLevel(LogLevel.Warning);
            builder.Services.AddTecVault(v => v.UseInMemory(o => o.InitialSecrets["rabbitmq"] = uri!));
            builder.Services.AddSingleton(probe);
            builder.Services.AddSingleton(sp => new ProbeConsumer(sp.GetRequiredService<RabbitMqConsumerDependencies>(), probe, queue));
            builder.Services.AddTecMessaging()
                .UseRabbitMq(o =>
                {
                    o.Exchange = exchange;
                    o.RoutingKeyPrefixToRemove = "testes.";
                    o.DeadLetterMonitorInterval = TimeSpan.Zero;
                })
                .AddConsumer<ProbeConsumer>();
            var host = builder.Build();
            await host.StartAsync();
            return new Broker(host, uri!, exchange, queue, probe);
        }

        public Task PublishAsync(MessageEnvelope envelope) => Services.GetRequiredService<IMessagePublisher>().PublishAsync(envelope, CancellationToken.None);

        public async ValueTask DisposeAsync()
        {
            await _host.StopAsync();
            _host.Dispose();

            // Limpeza da topologia descartável do teste
            var factory = new ConnectionFactory { Uri = new Uri(_uri) };
            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();
            await channel.QueueDeleteAsync(Queue.Name);
            await channel.QueueDeleteAsync(Queue.DeadLetterQueue);
            foreach (var delay in Queue.RetryDelays.Distinct())
                await channel.QueueDeleteAsync(Queue.RetryQueue(delay));
            await channel.ExchangeDeleteAsync(Exchange);
        }
    }

    private static MessageEnvelope Envelope() =>
        new(Guid.NewGuid(), "testes.pedido-aprovado", 1, DateTimeOffset.UtcNow, "corr-integracao", null, null, "{\"pedidoId\":1}");

    [Test]
    public async Task Published_message_is_consumed_with_correlation_and_causation()
    {
        await using var broker = await Broker.StartAsync(name => new QueueDefinition(name, ["pedido-aprovado"]));
        var envelope = Envelope();

        await broker.PublishAsync(envelope);

        await Assert.That(await broker.Probe.WaitAsync(n => n >= 1, TimeSpan.FromSeconds(20))).IsTrue();
        var (message, correlation, causation) = broker.Probe.Received.Single();
        await Assert.That(message.Envelope.MessageId).IsEqualTo(envelope.MessageId);
        await Assert.That(message.Envelope.Payload).IsEqualTo("{\"pedidoId\":1}");
        await Assert.That(correlation).IsEqualTo("corr-integracao");
        await Assert.That(causation).IsEqualTo(envelope.MessageId.ToString());
        await Assert.That(message.PreviousFailures).IsEqualTo(0);
    }

    [Test]
    public async Task Retry_goes_through_the_wait_queue_and_comes_back()
    {
        await using var broker = await Broker.StartAsync(name => new QueueDefinition(name, ["pedido-aprovado"]) { RetryDelays = [TimeSpan.FromSeconds(1)] });
        broker.Probe.Behavior = m => m.PreviousFailures == 0 ? ConsumeResult.Retry("falha temporária") : ConsumeResult.Completed;

        await broker.PublishAsync(Envelope());

        await Assert.That(await broker.Probe.WaitAsync(n => n >= 2, TimeSpan.FromSeconds(30))).IsTrue();
        await Assert.That(broker.Probe.Received.Last().Message.PreviousFailures).IsEqualTo(1);
    }

    [Test]
    public async Task Rejected_message_goes_to_the_dlq_and_can_be_requeued()
    {
        await using var broker = await Broker.StartAsync(name => new QueueDefinition(name, ["pedido-aprovado"]));
        var rejectFirst = 1;
        broker.Probe.Behavior = _ => Interlocked.Exchange(ref rejectFirst, 0) == 1 ? ConsumeResult.Reject("inválida") : ConsumeResult.Completed;
        var envelope = Envelope();
        var admin = broker.Services.GetRequiredService<IDeadLetterAdministration>();

        await broker.PublishAsync(envelope);
        await Assert.That(await broker.Probe.WaitAsync(n => n >= 1, TimeSpan.FromSeconds(20))).IsTrue();

        IReadOnlyList<DeadLetterMessage> dead = [];
        for (var i = 0; i < 50 && dead.Count == 0; i++)
        {
            dead = (await admin.ListAsync(broker.Queue.Name, 10, CancellationToken.None)).Value;
            await Task.Delay(100);
        }

        await Assert.That(dead.Single().MessageId).IsEqualTo(envelope.MessageId);
        await Assert.That(dead.Single().LastError).IsEqualTo("inválida");
        await Assert.That((await admin.CountAsync(broker.Queue.Name, CancellationToken.None)).Value).IsEqualTo(1);

        var moved = await admin.RequeueAsync(broker.Queue.Name, [envelope.MessageId], null, CancellationToken.None);
        await Assert.That(moved.Value).IsEqualTo(1);
        await Assert.That(await broker.Probe.WaitAsync(n => n >= 2, TimeSpan.FromSeconds(20))).IsTrue();
        await Assert.That(broker.Probe.Received.Last().Message.PreviousFailures).IsEqualTo(0);
    }

    [Test]
    public async Task Missing_queue_is_reported_as_not_found()
    {
        await using var broker = await Broker.StartAsync(name => new QueueDefinition(name, ["pedido-aprovado"]));
        var admin = broker.Services.GetRequiredService<IDeadLetterAdministration>();

        var result = await admin.ListAsync("tec-testes.nao-existe", 10, CancellationToken.None);

        await Assert.That(result.Error!.Code).IsEqualTo(IDeadLetterAdministration.QueueNotFoundCode);
    }
}
