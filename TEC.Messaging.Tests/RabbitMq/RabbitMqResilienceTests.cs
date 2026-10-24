using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.RabbitMQ;
using TEC.Messaging.RabbitMQ.Consumers;
using TEC.Messaging.RabbitMQ.DependencyInjection;
using TEC.Messaging.RabbitMQ.Internal;
using TEC.Vault.DependencyInjection;
using TEC.Vault.InMemory;

namespace TEC.Messaging.Tests.RabbitMq;

/// <summary>Circuit breaker da conexão e espera de reconexão, sem broker (porta fechada no loopback).</summary>
public class RabbitMqResilienceTests
{
    // Porta 1 no loopback: conexão recusada na hora, sem depender de rede nem de broker
    private const string UnreachableUri = "amqp://guest:guest@127.0.0.1:1/";

    // Endereço não roteável: a conexão fica pendurada até o tempo limite (simula broker que não responde)
    private const string BlackholeUri = "amqp://guest:guest@10.255.255.1:5672/";

    private sealed class Clock(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = now;

        public override DateTimeOffset GetUtcNow() => Now;
    }

    private static IHost Build(Action<RabbitMqOptions>? configure = null, Action<IServiceCollection>? services = null)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { EnvironmentName = Environments.Development });
        builder.Logging.ClearProviders();
        services?.Invoke(builder.Services);
        builder.Services.AddTecVault(v => v.UseInMemory(o => o.InitialSecrets["rabbitmq"] = UnreachableUri));
        builder.Services.AddHealthChecks().AddTecRabbitMq();
        builder.Services.AddTecMessaging().UseRabbitMq(o =>
        {
            o.Exchange = "tec-testes.resiliencia";
            o.DeadLetterMonitorInterval = TimeSpan.Zero;
            o.CircuitBreaker.MinimumThroughput = 2;
            o.CircuitBreaker.BreakDuration = TimeSpan.FromMinutes(5);
            configure?.Invoke(o);
        });
        return builder.Build();
    }

    [Test]
    public async Task Repeated_connection_failures_open_circuit_and_fail_fast()
    {
        using var host = Build();
        var connection = host.Services.GetRequiredService<RabbitMqConnection>();

        for (int i = 0; i < 2; i++)
        {
            var failure = await Assert.That(async () => await connection.GetAsync(CancellationToken.None)).Throws<Exception>();
            await Assert.That(failure).IsNotTypeOf<RabbitMqCircuitOpenException>();
        }

        await Assert.That(connection.IsCircuitOpen).IsTrue();
        var watch = Stopwatch.StartNew();
        await Assert.That(async () => await connection.GetAsync(CancellationToken.None)).Throws<RabbitMqCircuitOpenException>();
        await Assert.That(watch.Elapsed).IsLessThan(TimeSpan.FromMilliseconds(500));

        // Health check responde na hora, sem mais uma tentativa de conexão
        var report = await host.Services.GetRequiredService<HealthCheckService>().CheckHealthAsync();
        await Assert.That(report.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(report.Entries["rabbitmq"].Description).Contains("circuito aberto");
    }

    [Test]
    public async Task Cancelled_probe_keeps_circuit_open()
    {
        var clock = new Clock(DateTimeOffset.UtcNow);
        using var host = Build(services: s => s.AddSingleton<TimeProvider>(clock));
        var connection = host.Services.GetRequiredService<RabbitMqConnection>();
        for (int i = 0; i < 2; i++)
            await Assert.That(async () => await connection.GetAsync(CancellationToken.None)).Throws<Exception>();
        await Assert.That(connection.IsCircuitOpen).IsTrue();

        // Fim da pausa: a tentativa de teste é cancelada pelo chamador (ex.: desligamento) antes de o broker responder
        clock.Now += TimeSpan.FromMinutes(6);
        var vault = host.Services.GetRequiredService<TEC.Vault.Abstractions.ISecretStore>();
        await vault.SetSecretAsync("rabbitmq", BlackholeUri);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        await Assert.That(async () => await connection.GetAsync(cts.Token)).Throws<OperationCanceledException>();

        // Sem resposta do broker, o circuito não pode fechar
        await Assert.That(connection.IsCircuitOpen).IsTrue();
        await Assert.That(async () => await connection.GetAsync(CancellationToken.None)).Throws<RabbitMqCircuitOpenException>();
    }

    [Test]
    public async Task Caller_cancellation_does_not_open_circuit()
    {
        using var host = Build();
        var connection = host.Services.GetRequiredService<RabbitMqConnection>();
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        for (int i = 0; i < 5; i++)
            await Assert.That(async () => await connection.GetAsync(canceled.Token)).Throws<OperationCanceledException>();

        await Assert.That(connection.IsCircuitOpen).IsFalse();
    }

    [Test]
    public async Task Disabled_circuit_keeps_trying()
    {
        using var host = Build(o => o.CircuitBreaker.Enabled = false);
        var connection = host.Services.GetRequiredService<RabbitMqConnection>();

        for (int i = 0; i < 4; i++)
        {
            var failure = await Assert.That(async () => await connection.GetAsync(CancellationToken.None)).Throws<Exception>();
            await Assert.That(failure).IsNotTypeOf<RabbitMqCircuitOpenException>();
        }
    }

    [Test]
    public async Task Reconnect_delay_grows_with_jitter_up_to_the_limit()
    {
        using var host = Build(o =>
        {
            o.ReconnectDelay = TimeSpan.FromSeconds(1);
            o.MaxReconnectDelay = TimeSpan.FromSeconds(10);
        });
        using var consumer = new ProbeConsumer(host.Services.GetRequiredService<RabbitMqConsumerDependencies>(), new Probe(),
            new QueueDefinition("tec-testes.resiliencia", ["x"]));

        var first = consumer.ReconnectDelay(1);
        var third = consumer.ReconnectDelay(3);
        await Assert.That(first).IsBetween(TimeSpan.FromMilliseconds(800), TimeSpan.FromMilliseconds(1200));
        await Assert.That(third).IsBetween(TimeSpan.FromMilliseconds(3200), TimeSpan.FromMilliseconds(4800));
        for (int failures = 5; failures < 100; failures += 7)
            await Assert.That(consumer.ReconnectDelay(failures)).IsLessThanOrEqualTo(TimeSpan.FromSeconds(10));
    }

    [Test]
    [Arguments("ratio")]
    [Arguments("throughput")]
    [Arguments("reconnect")]
    public async Task Invalid_options_fail_validation(string invalid)
    {
        var options = new RabbitMqOptions { Exchange = "tec-testes.x" };
        switch (invalid)
        {
            case "ratio": options.CircuitBreaker.FailureRatio = 2; break;
            case "throughput": options.CircuitBreaker.MinimumThroughput = 1; break;
            default: options.MaxReconnectDelay = TimeSpan.FromMilliseconds(500); break;
        }

        var services = new ServiceCollection();
        services.AddTecMessaging().UseRabbitMq();
        using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<IValidateOptions<RabbitMqOptions>>().Single();

        await Assert.That(validator.Validate(null, options).Failed).IsTrue();
    }
}
