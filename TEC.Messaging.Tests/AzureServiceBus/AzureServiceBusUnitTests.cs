using System.Globalization;
using System.Net.Sockets;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TEC.Core.Common.Results;
using TEC.Messaging.AzureServiceBus;
using TEC.Messaging.AzureServiceBus.Consumers;
using TEC.Messaging.AzureServiceBus.DependencyInjection;
using TEC.Messaging.AzureServiceBus.Internal;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Transport;
using TEC.Vault;
using TEC.Vault.Abstractions;
using TEC.Vault.DependencyInjection;
using TEC.Vault.InMemory;
using TEC.Vault.Secrets;
using TEC.Messaging.Tests.Shared;

namespace TEC.Messaging.Tests.AzureServiceBus;

/// <summary>Consumidor mínimo para os testes de registro.</summary>
internal sealed class FilaDeTesteConsumer(AzureServiceBusConsumerDependencies dependencies) : AzureServiceBusConsumer(dependencies)
{
    protected override ServiceBusConsumerDefinition Definition { get; } = new("tec-testes.fila");

    protected override Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken) =>
        Task.FromResult(ConsumeResult.Completed);
}

/// <summary>Leitor de segredos que conta as leituras (rotação e single-flight).</summary>
internal sealed class CountingSecretReader(ISecretReader inner) : ISecretReader
{
    private int _reads;

    public int Reads => Volatile.Read(ref _reads);

    public string ProviderName => inner.ProviderName;

    public async Task<Result<VaultSecret>> GetSecretAsync(string name, string? version = null, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _reads);
        await Task.Yield();
        return await inner.GetSecretAsync(name, version, cancellationToken);
    }

    public Task<Result<bool>> ExistsAsync(string name, CancellationToken cancellationToken = default) => inner.ExistsAsync(name, cancellationToken);

    public Task<Result<IReadOnlyList<SecretProperties>>> ListSecretsAsync(CancellationToken cancellationToken = default) => inner.ListSecretsAsync(cancellationToken);

    public Task<Result<IReadOnlyList<SecretProperties>>> ListSecretVersionsAsync(string name, CancellationToken cancellationToken = default) =>
        inner.ListSecretVersionsAsync(name, cancellationToken);
}

public class AzureServiceBusUnitTests
{
    private const string ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dGVzdA==";
    private const string EmulatorConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private static ServiceProvider Build(string? secret, Action<AzureServiceBusOptions>? configure = null, string environment = "Production")
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IHostEnvironment>(new TestHostEnvironment(environment));
        services.AddTecVault(v => v.UseInMemory(o =>
        {
            o.AllowOutsideDevelopment = true;
            if (secret is not null)
                o.InitialSecrets["azure-servicebus"] = secret;
        }));
        services.AddTecMessaging().UseAzureServiceBus(o =>
        {
            o.DefaultTopic = "tec-testes.eventos";
            o.PublishTimeout = TimeSpan.FromSeconds(2);
            configure?.Invoke(o);
        });
        return services.BuildServiceProvider();
    }

    private static MessageEnvelope Envelope() =>
        new(Guid.NewGuid(), "testes.pedido-aprovado", 1, DateTimeOffset.UtcNow, "corr", null, null, "{\"pedidoId\":1}");

    private static ServiceBusReceivedMessage Received(
        string? messageId, string body = "{\"pedidoId\":1}", string? subject = "testes.pedido-aprovado", IDictionary<string, object>? properties = null,
        int deliveryCount = 1) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(body), messageId: messageId, subject: subject, properties: properties, deliveryCount: deliveryCount,
            enqueuedTime: new DateTimeOffset(2026, 10, 29, 12, 0, 0, TimeSpan.Zero));

    [Test]
    public async Task Options_validator_rejects_missing_secret()
    {
        var validator = new AzureServiceBusOptionsValidator();
        var result = validator.Validate(null, new AzureServiceBusOptions { ConnectionSecretName = "" });
        await Assert.That(result.Failed).IsTrue();
    }

    [Test]
    public async Task Options_validator_accepts_valid_settings()
    {
        var validator = new AzureServiceBusOptionsValidator();
        var result = validator.Validate(null, new AzureServiceBusOptions
        {
            ConnectionSecretName = "asb-secret",
            DefaultTopic = "eventos",
            PublishTimeout = TimeSpan.FromSeconds(5),
        });
        await Assert.That(result.Succeeded).IsTrue();
    }

    [Test]
    public async Task UseAzureServiceBus_registers_core_services()
    {
        await using var sp = Build(ConnectionString, o => o.DefaultTopic = "meu-topico");
        var options = sp.GetRequiredService<IOptions<AzureServiceBusOptions>>().Value;

        await Assert.That(options.DefaultTopic).IsEqualTo("meu-topico");
        await Assert.That(sp.GetRequiredService<AzureServiceBusConsumerDependencies>()).IsNotNull();
    }

    // Classificação das falhas de publicação: só transitória deixa de contar tentativa no Outbox

    [Test]
    [Arguments(ServiceBusFailureReason.ServiceBusy)]
    [Arguments(ServiceBusFailureReason.ServiceTimeout)]
    [Arguments(ServiceBusFailureReason.ServiceCommunicationProblem)]
    public async Task Transient_service_bus_failures_are_transport_unavailable(ServiceBusFailureReason reason)
    {
        await Assert.That(AzureServiceBusPublisher.IsTransient(new ServiceBusException("falha", reason))).IsTrue();
    }

    [Test]
    [Arguments(ServiceBusFailureReason.MessageSizeExceeded)]
    [Arguments(ServiceBusFailureReason.MessagingEntityNotFound)]
    [Arguments(ServiceBusFailureReason.MessagingEntityDisabled)]
    [Arguments(ServiceBusFailureReason.GeneralError)]
    public async Task Definitive_service_bus_failures_count_an_attempt(ServiceBusFailureReason reason)
    {
        await Assert.That(AzureServiceBusPublisher.IsTransient(new ServiceBusException(false, "falha", null, reason, null))).IsFalse();
    }

    [Test]
    public async Task Service_bus_transient_flag_wins_over_a_generic_reason()
    {
        await Assert.That(AzureServiceBusPublisher.IsTransient(new ServiceBusException(true, "falha", null, ServiceBusFailureReason.GeneralError, null))).IsTrue();
    }

    [Test]
    public async Task Network_and_timeout_failures_are_transient_and_permission_or_payload_are_not()
    {
        await Assert.That(AzureServiceBusPublisher.IsTransient(new SocketException())).IsTrue();
        await Assert.That(AzureServiceBusPublisher.IsTransient(new TimeoutException())).IsTrue();
        await Assert.That(AzureServiceBusPublisher.IsTransient(new InvalidOperationException("embrulho", new IOException("rede")))).IsTrue();
        await Assert.That(AzureServiceBusPublisher.IsTransient(new UnauthorizedAccessException("sem permissão Send"))).IsFalse();
        await Assert.That(AzureServiceBusPublisher.IsTransient(new ArgumentException("payload inválido"))).IsFalse();
    }

    [Test]
    public async Task Vault_down_fails_as_transport_unavailable_without_leaking_the_secret()
    {
        await using var sp = Build(secret: null);
        var publisher = sp.GetRequiredService<IMessagePublisher>();

        var failure = await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<AzureServiceBusUnavailableException>();

        await Assert.That(failure).IsAssignableTo<ITransportUnavailableException>();
        await Assert.That(failure!.Message).Contains("azure-servicebus");
    }

    // TLS: o emulador local (AMQP sem TLS) só em Development ou com RequireTls = false

    [Test]
    public async Task Emulator_connection_string_is_refused_outside_development()
    {
        await using var sp = Build(EmulatorConnectionString);
        var connection = sp.GetRequiredService<AzureServiceBusConnection>();

        var failure = await Assert.That(async () => await connection.GetClientAsync(CancellationToken.None)).Throws<InvalidOperationException>();

        await Assert.That(failure!.Message).Contains("UseDevelopmentEmulator");
        await Assert.That(failure.Message).DoesNotContain("SAS_KEY_VALUE");
    }

    [Test]
    public async Task Emulator_connection_string_is_accepted_in_development_or_with_tls_disabled()
    {
        await using var development = Build(EmulatorConnectionString, environment: Environments.Development);
        await using var optOut = Build(EmulatorConnectionString, o => o.RequireTls = false);

        await Assert.That(await development.GetRequiredService<AzureServiceBusConnection>().GetClientAsync(CancellationToken.None)).IsNotNull();
        await Assert.That(await optOut.GetRequiredService<AzureServiceBusConnection>().GetClientAsync(CancellationToken.None)).IsNotNull();
    }

    [Test]
    public async Task Explicit_require_tls_refuses_the_emulator_even_in_development()
    {
        await using var sp = Build(EmulatorConnectionString, o => o.RequireTls = true, Environments.Development);

        await Assert.That(async () => await sp.GetRequiredService<AzureServiceBusConnection>().GetClientAsync(CancellationToken.None))
            .Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments("Endpoint=sb://x;UseDevelopmentEmulator=true", true)]
    [Arguments("Endpoint=sb://x; usedevelopmentemulator = True ;", true)]
    [Arguments("Endpoint=sb://x;UseDevelopmentEmulator=false", false)]
    [Arguments("Endpoint=sb://x;SharedAccessKey=UseDevelopmentEmulator=true", false)]
    [Arguments("Endpoint=sb://x", false)]
    public async Task Emulator_flag_is_read_from_its_own_key_only(string connectionString, bool expected)
    {
        await Assert.That(AzureServiceBusConnection.UsesDevelopmentEmulator(connectionString)).IsEqualTo(expected);
    }

    // Rotação de credencial: cliente descartado e recriado uma única vez, relendo o segredo

    [Test]
    public async Task Invalidated_client_is_recreated_once_rereading_the_secret()
    {
        await using var sp = Build(ConnectionString);
        var reader = new CountingSecretReader(sp.GetRequiredService<ISecretReader>());
        await using var connection = new AzureServiceBusConnection(reader, sp.GetRequiredService<IOptions<AzureServiceBusOptions>>());

        var first = await connection.GetClientAsync(CancellationToken.None);
        await Assert.That(await connection.GetClientAsync(CancellationToken.None)).IsSameReferenceAs(first);

        // Várias falhas simultâneas com o mesmo cliente: um descarte e uma recriação
        await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => connection.InvalidateAsync(first)));
        var clients = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => connection.GetClientAsync(CancellationToken.None)));

        await Assert.That(clients.Distinct().Count()).IsEqualTo(1);
        await Assert.That(clients[0]).IsNotSameReferenceAs(first);
        await Assert.That(first.IsClosed).IsTrue();
        await Assert.That(reader.Reads).IsEqualTo(2);

        // Invalidação atrasada com o cliente antigo não derruba o novo
        await connection.InvalidateAsync(first);
        await Assert.That(await connection.GetClientAsync(CancellationToken.None)).IsSameReferenceAs(clients[0]);
        await Assert.That(reader.Reads).IsEqualTo(2);
    }

    // Consumo: validação do envelope, limite de payload e identificador

    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments("   ")]
    public async Task Message_without_id_is_rejected_instead_of_becoming_guid_empty(string? messageId)
    {
        var envelope = AzureServiceBusConsumer.ReadEnvelope(Received(messageId), 256 * 1024, out var error);

        await Assert.That(envelope).IsNull();
        await Assert.That(error).Contains("MessageId");
    }

    [Test]
    public async Task Empty_guid_message_id_is_rejected()
    {
        var envelope = AzureServiceBusConsumer.ReadEnvelope(Received(Guid.Empty.ToString()), 256 * 1024, out var error);

        await Assert.That(envelope).IsNull();
        await Assert.That(error).IsNotNull();
    }

    [Test]
    public async Task Payload_above_the_limit_is_rejected_before_the_handler()
    {
        var big = $"{{\"texto\":\"{new string('a', 5000)}\"}}";

        await Assert.That(AzureServiceBusConsumer.ReadEnvelope(Received(Guid.NewGuid().ToString(), big), 1024, out var error)).IsNull();
        await Assert.That(error).Contains("1024");
        await Assert.That(AzureServiceBusConsumer.ReadEnvelope(Received(Guid.NewGuid().ToString(), new string('a', 10_000)), 1024, out _)).IsNull();
    }

    [Test]
    public async Task Message_without_type_is_rejected()
    {
        await Assert.That(AzureServiceBusConsumer.ReadEnvelope(Received(Guid.NewGuid().ToString(), subject: null), 256 * 1024, out var error)).IsNull();
        await Assert.That(error).IsNotNull();
    }

    [Test]
    public async Task Valid_message_is_read_with_invariant_parsing_and_utc_instant()
    {
        var id = Guid.NewGuid();
        // Sem ICU (DOTNET_SYSTEM_GLOBALIZATION_INVARIANT) só existe a cultura invariante: o cenário não se aplica
        Skip.When(CultureInfo.GetCultures(CultureTypes.SpecificCultures).Length <= 1, "Sem ICU: só a cultura invariante.");
        var previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("ar-SA");
        try
        {
            var envelope = AzureServiceBusConsumer.ReadEnvelope(
                Received(id.ToString(), properties: new Dictionary<string, object>
                {
                    ["version"] = "2",
                    ["occurred-at"] = "2026-10-29T09:00:00.0000000-03:00",
                    ["causation-id"] = "causa",
                }), 256 * 1024, out var error);

            await Assert.That(error).IsNull();
            await Assert.That(envelope!.MessageId).IsEqualTo(id);
            await Assert.That(envelope.Version).IsEqualTo(2);
            await Assert.That(envelope.OccurredAt.Offset).IsEqualTo(TimeSpan.Zero);
            await Assert.That(envelope.OccurredAt.UtcDateTime).IsEqualTo(new DateTime(2026, 10, 29, 12, 0, 0, DateTimeKind.Utc));
            await Assert.That(envelope.CausationId).IsEqualTo("causa");
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Test]
    public async Task Scheduled_copy_keeps_the_envelope_id_and_the_attempt_counter()
    {
        // A cópia agendada tem MessageId próprio (detecção de duplicatas) e carrega o id do envelope e as tentativas
        var id = Guid.NewGuid();
        var copy = Received($"{id}:3", properties: new Dictionary<string, object>
        {
            [AzureServiceBusConsumer.EnvelopeIdProperty] = id.ToString(),
            [AzureServiceBusConsumer.AttemptProperty] = 2,
        });

        var envelope = AzureServiceBusConsumer.ReadEnvelope(copy, 256 * 1024, out _);

        await Assert.That(envelope!.MessageId).IsEqualTo(id);
        await Assert.That(AzureServiceBusConsumer.PreviousFailures(copy)).IsEqualTo(2);
        await Assert.That(AzureServiceBusConsumer.PreviousFailures(Received(id.ToString(), deliveryCount: 4))).IsEqualTo(3);
        await Assert.That(AzureServiceBusConsumer.PreviousFailures(Received(id.ToString(), properties: new Dictionary<string, object>
        {
            [AzureServiceBusConsumer.AttemptProperty] = "nao-numero",
        }))).IsEqualTo(0);
    }

    [Test]
    public async Task Retry_delay_grows_and_is_capped()
    {
        await Assert.That(AzureServiceBusConsumer.RetryDelay(1)).IsEqualTo(TimeSpan.FromSeconds(5));
        await Assert.That(AzureServiceBusConsumer.RetryDelay(2)).IsEqualTo(TimeSpan.FromSeconds(10));
        await Assert.That(AzureServiceBusConsumer.RetryDelay(4)).IsEqualTo(TimeSpan.FromSeconds(40));
        await Assert.That(AzureServiceBusConsumer.RetryDelay(50)).IsEqualTo(TimeSpan.FromMinutes(15));
        await Assert.That(AzureServiceBusConsumer.RetryDelay(0)).IsEqualTo(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task Incoherent_definition_is_refused_on_startup()
    {
        await Assert.That(() => new ServiceBusConsumerDefinition("fila", MaxAttempts: 0).Validate()).Throws<InvalidOperationException>();
        await Assert.That(() => new ServiceBusConsumerDefinition("", MaxAttempts: 3).Validate()).Throws<InvalidOperationException>();
        await Assert.That(() => new ServiceBusConsumerDefinition("fila", MaxConcurrentCalls: 0).Validate()).Throws<InvalidOperationException>();
        await Assert.That(() => new ServiceBusConsumerDefinition("topico", Subscription: " ").Validate()).Throws<InvalidOperationException>();
        new ServiceBusConsumerDefinition("topico", "assinatura").Validate();
    }

    // Registro e health check

    [Test]
    public async Task Adding_the_same_consumer_twice_registers_one_hosted_service()
    {
        var services = new ServiceCollection();
        var builder = services.AddTecMessaging().UseAzureServiceBus();
        var before = services.Count(d => d.ServiceType == typeof(IHostedService));

        builder.AddAzureServiceBusConsumer<FilaDeTesteConsumer>().AddAzureServiceBusConsumer<FilaDeTesteConsumer>().AddConsumer<FilaDeTesteConsumer>();

        await Assert.That(services.Count(d => d.ServiceType == typeof(IHostedService))).IsEqualTo(before + 1);
    }

    [Test]
    public async Task Health_check_without_default_topic_is_degraded_not_healthy()
    {
        await using var sp = Build(ConnectionString, o => o.DefaultTopic = "");
        var check = new AzureServiceBusHealthCheck(sp.GetRequiredService<AzureServiceBusConnection>(), sp.GetRequiredService<IOptions<AzureServiceBusOptions>>());

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        await Assert.That(result.Status).IsEqualTo(HealthStatus.Degraded);
    }

    [Test]
    public async Task Health_check_fails_fast_when_the_vault_is_down()
    {
        await using var sp = Build(secret: null);
        var check = new AzureServiceBusHealthCheck(sp.GetRequiredService<AzureServiceBusConnection>(), sp.GetRequiredService<IOptions<AzureServiceBusOptions>>());

        var result = await check.CheckHealthAsync(new HealthCheckContext()).WaitAsync(TimeSpan.FromSeconds(10));

        await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
    }
}
