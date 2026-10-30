using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using TEC.Messaging.Administration;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.RabbitMQ;
using TEC.Messaging.RabbitMQ.Administration;
using TEC.Messaging.RabbitMQ.DependencyInjection;
using TEC.Messaging.RabbitMQ.Internal;
using TEC.Messaging.Transport;
using TEC.Vault.DependencyInjection;
using TEC.Vault.InMemory;

namespace TEC.Messaging.Tests.RabbitMq;

/// <summary>Host com o transporte RabbitMQ desligado (Enabled = false), sem subir os serviços.</summary>
internal sealed class DisabledTransportHost : IAsyncDisposable
{
    private readonly IHost _host = RabbitMqUnitTests.Build("amqp://guest:guest@127.0.0.1:1/", o => o.Enabled = false);

    public IMessagePublisher Publisher => _host.Services.GetRequiredService<IMessagePublisher>();

    public ValueTask DisposeAsync()
    {
        _host.Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>Publicador, administração da DLQ, envelope AMQP e topologia sem broker.</summary>
public class RabbitMqUnitTests
{
    // Porta 1 no loopback: conexão recusada na hora
    private const string UnreachableUri = "amqp://guest:guest@127.0.0.1:1/";

    // Endereço não roteável: a conexão fica pendurada (broker que não responde)
    private const string BlackholeUri = "amqp://guest:guest@10.255.255.1:5672/";

    internal static IHost Build(string uri, Action<RabbitMqOptions>? configure = null, string environment = "Development")
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = environment });
        builder.Logging.ClearProviders();
        builder.Services.AddTecVault(v => v.UseInMemory(o =>
        {
            o.AllowOutsideDevelopment = true;
            o.InitialSecrets["rabbitmq"] = uri;
        }));
        builder.Services.AddTecMessaging().UseRabbitMq(o =>
        {
            o.Exchange = "tec-testes.unitarios";
            o.DeadLetterMonitorInterval = TimeSpan.Zero;
            o.CircuitBreaker.MinimumThroughput = 2;
            o.CircuitBreaker.BreakDuration = TimeSpan.FromMinutes(5);
            configure?.Invoke(o);
        });
        return builder.Build();
    }

    private static MessageEnvelope Envelope(string type = "testes.pedido-aprovado", string payload = "{\"pedidoId\":1}") =>
        new(Guid.NewGuid(), type, 2, new DateTimeOffset(2026, 10, 19, 12, 0, 0, TimeSpan.Zero), "corr", "causa", null, payload);

    [Test]
    public async Task Disabled_transport_fails_as_transport_unavailable()
    {
        using var host = Build(UnreachableUri, o => o.Enabled = false);
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        var failure = await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<RabbitMqUnavailableException>();

        await Assert.That(failure).IsAssignableTo<ITransportUnavailableException>();
    }

    [Test]
    public async Task Broker_down_fails_as_transport_unavailable_also_with_the_circuit_open()
    {
        using var host = Build(UnreachableUri);
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        for (var i = 0; i < 2; i++)
        {
            var failure = await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<RabbitMqUnavailableException>();
            await Assert.That(failure!.InnerException).IsNotNull();
        }

        var open = await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<RabbitMqCircuitOpenException>();
        await Assert.That(open).IsAssignableTo<ITransportUnavailableException>();
    }

    [Test]
    public async Task Plain_amqp_is_refused_outside_development_without_touching_the_broker()
    {
        using var host = Build(UnreachableUri, environment: Environments.Production);
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        var failure = await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<RabbitMqUnavailableException>();

        // A recusa vem da URI, antes de qualquer conexão; a mensagem não repete a URI (com usuário e senha)
        await Assert.That(failure!.InnerException).IsTypeOf<InvalidOperationException>();
        await Assert.That(failure.InnerException!.Message).Contains("TLS");
        await Assert.That(failure.InnerException.Message).DoesNotContain("guest");
    }

    [Test]
    public async Task Explicit_require_tls_refuses_plain_amqp_even_in_development()
    {
        using var host = Build(UnreachableUri, o => o.RequireTls = true);
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        var failure = await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<RabbitMqUnavailableException>();

        await Assert.That(failure!.InnerException!.Message).Contains("TLS");
    }

    [Test]
    public async Task Tls_requirement_can_be_disabled_explicitly()
    {
        // Com RequireTls = false a URI passa pela checagem e a falha é a do broker inalcançável (conexão recusada)
        using var host = Build(UnreachableUri, o => o.RequireTls = false, Environments.Production);
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        var failure = await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<RabbitMqUnavailableException>();

        await Assert.That(failure!.InnerException!.Message).DoesNotContain("TLS");
    }

    [Test]
    public async Task Dispose_waits_for_the_publication_in_progress()
    {
        // Regressão B5: o descarte liberava a trava na hora e a publicação em andamento terminava com ObjectDisposedException
        using var host = Build(BlackholeUri, o => o.PublishTimeout = TimeSpan.FromSeconds(1));
        var publisher = host.Services.GetRequiredService<RabbitMqPublisher>();
        using var cts = new CancellationTokenSource();

        var publishing = publisher.PublishAsync(Envelope(), cts.Token);
        await Task.Delay(200);
        var disposing = publisher.DisposeAsync().AsTask();
        await Task.Delay(300);
        await Assert.That(disposing.IsCompleted).IsFalse();

        await cts.CancelAsync();
        await Assert.That(async () => await publishing).Throws<OperationCanceledException>();
        await disposing.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Requeue_with_an_empty_selection_is_rejected()
    {
        // Regressão M3: coleção vazia republicava a DLQ inteira (mesmo efeito de null)
        using var host = Build(UnreachableUri);
        var admin = host.Services.GetRequiredService<IDeadLetterAdministration>();

        var result = await admin.RequeueAsync("tec-testes.fila", [], null, CancellationToken.None);

        await Assert.That(result.IsFailure).IsTrue();
        await Assert.That(result.Error!.Code).IsEqualTo("IDS_INVALIDOS");
    }

    [Test]
    public async Task Only_not_found_is_reported_as_missing_queue()
    {
        // Regressão B12: ACCESS_REFUSED (403) aparecia como fila não encontrada
        var notFound = new OperationInterruptedException(new ShutdownEventArgs(ShutdownInitiator.Peer, 404, "NOT_FOUND - no queue"));
        var refused = new OperationInterruptedException(new ShutdownEventArgs(ShutdownInitiator.Peer, 403, "ACCESS_REFUSED"));

        await Assert.That(RabbitMqDeadLetterAdministration.IsNotFound(notFound)).IsTrue();
        await Assert.That(RabbitMqDeadLetterAdministration.IsNotFound(refused)).IsFalse();
    }

    [Test]
    [Arguments(30_000, "pedidos.retry.30s")]
    [Arguments(60_000, "pedidos.retry.1m")]
    [Arguments(60_500, "pedidos.retry.60500ms")]
    [Arguments(90_000, "pedidos.retry.90s")]
    [Arguments(7_200_000, "pedidos.retry.120m")]
    public async Task Wait_queue_names_never_collide(int milliseconds, string expected)
    {
        // Regressão B6: 60 s e 60,5 s geravam o mesmo nome (pedidos.retry.1m) com TTLs diferentes
        var queue = new QueueDefinition("pedidos", ["x"]);

        await Assert.That(queue.RetryQueue(TimeSpan.FromMilliseconds(milliseconds))).IsEqualTo(expected);
    }

    [Test]
    public async Task Envelope_round_trips_through_the_wire_format()
    {
        var envelope = Envelope();

        var read = AmqpEnvelope.Read(AmqpEnvelope.Serialize(envelope), out var error);

        await Assert.That(error).IsNull();
        await Assert.That(read).IsEqualTo(envelope);
    }

    [Test]
    [Arguments("[1,2]", "objeto")]
    [Arguments("{\"type\":\"testes.x\"}", "messageId")]
    [Arguments("{\"messageId\":\"0198f3a2-0000-7000-8000-000000000001\",\"type\":\"\"}", "type")]
    [Arguments("{\"messageId\":", "JSON")]
    public async Task Malformed_envelopes_are_rejected_with_a_reason(string body, string reason)
    {
        var read = AmqpEnvelope.Read(Encoding.UTF8.GetBytes(body), out var error);

        await Assert.That(read).IsNull();
        await Assert.That(error).Contains(reason);
    }

    [Test]
    public async Task Consumption_applies_the_payload_limit()
    {
        // Regressão B7: o consumo não aplicava MaxPayloadLength
        var body = AmqpEnvelope.Serialize(Envelope(payload: $"{{\"texto\":\"{new string('a', 2000)}\"}}"));

        var read = AmqpEnvelope.Read(body, out var error, maxPayloadLength: 1024);

        await Assert.That(read).IsNull();
        await Assert.That(error).Contains("1024");
        await Assert.That(AmqpEnvelope.Read(new byte[AmqpEnvelope.MaxBodyBytes + 1], out _)).IsNull();
    }

    [Test]
    public async Task Process_span_name_ignores_an_invalid_message_type()
    {
        // Regressão B11: o nome do span vinha do type da mensagem, sem validação
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == MessagingDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(listener);

        using var invalid = MessagingDiagnostics.StartProcess(Envelope(type: "Tipo Inventado!!"), null, "rabbitmq", "vendas.faturamento");
        using var valid = MessagingDiagnostics.StartProcess(Envelope(), null, "rabbitmq", "vendas.faturamento");

        await Assert.That(invalid!.DisplayName).IsEqualTo("vendas.faturamento process");
        await Assert.That(valid!.DisplayName).IsEqualTo("testes.pedido-aprovado process");
    }
}
