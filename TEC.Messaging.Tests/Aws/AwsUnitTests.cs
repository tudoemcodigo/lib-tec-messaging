using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Amazon.Runtime;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TEC.Messaging.Aws;
using TEC.Messaging.Aws.Consumers;
using TEC.Messaging.Aws.DependencyInjection;
using TEC.Messaging.Aws.Internal;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Tests.AzureServiceBus;
using TEC.Messaging.Tests.Shared;
using TEC.Messaging.Transport;
using TEC.Vault;
using TEC.Vault.Abstractions;
using TEC.Vault.DependencyInjection;
using TEC.Vault.InMemory;

namespace TEC.Messaging.Tests.Aws;

/// <summary>Consumidor mínimo para os testes de registro.</summary>
internal sealed class FilaSqsDeTesteConsumer(AwsSqsConsumerDependencies dependencies) : AwsSqsConsumer(dependencies)
{
    protected override SqsConsumerDefinition Definition { get; } = new("https://sqs.sa-east-1.amazonaws.com/123456789012/tec-testes");

    protected override Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken) =>
        Task.FromResult(ConsumeResult.Completed);
}

public class AwsUnitTests
{
    private const string QueueUrl = "https://sqs.sa-east-1.amazonaws.com/123456789012/tec-testes";
    private const string Credentials = "AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    private static ServiceProvider Build(string? secret, Action<AwsMessagingOptions>? configure = null, string environment = "Production")
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IHostEnvironment>(new TestHostEnvironment(environment));
        services.AddTecVault(v => v.UseInMemory(o =>
        {
            o.AllowOutsideDevelopment = true;
            if (secret is not null)
                o.InitialSecrets["aws-messaging"] = secret;
        }));
        services.AddTecMessaging().UseAws(o =>
        {
            o.Region = "sa-east-1";
            o.DefaultDestination = QueueUrl;
            o.PublishTimeout = TimeSpan.FromSeconds(2);
            configure?.Invoke(o);
        });
        return services.BuildServiceProvider();
    }

    private static MessageEnvelope Envelope() =>
        new(Guid.NewGuid(), "testes.pedido-aprovado", 1, DateTimeOffset.UtcNow, "corr", null, null, "{\"pedidoId\":1}");

    private static AmazonServiceException Service(HttpStatusCode status, string code) =>
        new($"falha {code}", ErrorType.Sender, code, "req-1", status);

    private static Message Received(string? messageId, string body = "{\"pedidoId\":1}", string? type = "testes.pedido-aprovado", Dictionary<string, string>? extra = null)
    {
        var attributes = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        if (messageId is not null)
            attributes["messageId"] = new() { DataType = "String", StringValue = messageId };
        if (type is not null)
            attributes["type"] = new() { DataType = "String", StringValue = type };
        foreach (var (key, value) in extra ?? [])
            attributes[key] = new() { DataType = "String", StringValue = value };
        return new Message { MessageId = "", Body = body, MessageAttributes = attributes, Attributes = new Dictionary<string, string>(StringComparer.Ordinal) };
    }

    [Test]
    public async Task Options_validator_rejects_empty_settings()
    {
        var validator = new AwsMessagingOptionsValidator();
        var result = validator.Validate(null, new AwsMessagingOptions { ConnectionSecretName = "", Region = "" });
        await Assert.That(result.Failed).IsTrue();
    }

    [Test]
    public async Task Options_validator_accepts_valid_settings()
    {
        var validator = new AwsMessagingOptionsValidator();
        var result = validator.Validate(null, new AwsMessagingOptions
        {
            ConnectionSecretName = "aws-secret",
            Region = "sa-east-1",
            DefaultDestination = "arn:aws:sns:sa-east-1:123456789012:meu-topico",
            PublishTimeout = TimeSpan.FromSeconds(5),
        });
        await Assert.That(result.Succeeded).IsTrue();
    }

    [Test]
    public async Task UseAws_registers_core_services()
    {
        await using var sp = Build(Credentials);
        var options = sp.GetRequiredService<IOptions<AwsMessagingOptions>>().Value;

        await Assert.That(options.Region).IsEqualTo("sa-east-1");
        await Assert.That(sp.GetRequiredService<AwsSqsConsumerDependencies>()).IsNotNull();
    }

    // DefaultDestination validado na subida

    [Test]
    [Arguments("")]
    [Arguments("   ")]
    [Arguments("vendas.pedido-aprovado")]
    [Arguments("arn:aws:sns:sa-east-1")]
    [Arguments("ftp://fila")]
    [Arguments("http://sqs.sa-east-1.amazonaws.com/123456789012/fila")]
    public async Task Invalid_default_destination_is_refused_on_startup(string destination)
    {
        await using var sp = Build(Credentials, o => o.DefaultDestination = destination);

        await Assert.That(() => sp.GetRequiredService<IOptions<AwsMessagingOptions>>().Value).Throws<OptionsValidationException>();
    }

    [Test]
    public async Task Disabled_transport_does_not_require_a_destination()
    {
        await using var sp = Build(Credentials, o =>
        {
            o.Enabled = false;
            o.DefaultDestination = "";
        });

        await Assert.That(sp.GetRequiredService<IOptions<AwsMessagingOptions>>().Value.Enabled).IsFalse();
    }

    [Test]
    public async Task Plain_http_queue_is_accepted_only_without_tls_requirement()
    {
        const string local = "http://localhost:4566/000000000000/fila";
        await using var development = Build(Credentials, o => o.DefaultDestination = local, Environments.Development);
        await using var optOut = Build(Credentials, o =>
        {
            o.DefaultDestination = local;
            o.RequireTls = false;
        });
        await using var forced = Build(Credentials, o =>
        {
            o.DefaultDestination = local;
            o.RequireTls = true;
        }, Environments.Development);

        await Assert.That(development.GetRequiredService<IOptions<AwsMessagingOptions>>().Value.DefaultDestination).IsEqualTo(local);
        await Assert.That(optOut.GetRequiredService<IOptions<AwsMessagingOptions>>().Value.DefaultDestination).IsEqualTo(local);
        await Assert.That(() => forced.GetRequiredService<IOptions<AwsMessagingOptions>>().Value).Throws<OptionsValidationException>();
    }

    // Classificação das falhas de publicação: só transitória deixa de contar tentativa no Outbox

    [Test]
    [Arguments(HttpStatusCode.InternalServerError, "InternalError")]
    [Arguments(HttpStatusCode.ServiceUnavailable, "ServiceUnavailable")]
    [Arguments(HttpStatusCode.TooManyRequests, "TooManyRequestsException")]
    [Arguments(HttpStatusCode.BadRequest, "ThrottlingException")]
    [Arguments(HttpStatusCode.BadRequest, "RequestThrottled")]
    public async Task Server_errors_and_throttling_are_transport_unavailable(HttpStatusCode status, string code)
    {
        await Assert.That(AwsMessagingPublisher.IsTransient(Service(status, code))).IsTrue();
    }

    [Test]
    [Arguments(HttpStatusCode.BadRequest, "AWS.SimpleQueueService.NonExistentQueue")]
    [Arguments(HttpStatusCode.NotFound, "NotFound")]
    [Arguments(HttpStatusCode.Forbidden, "AccessDenied")]
    [Arguments(HttpStatusCode.Forbidden, "InvalidClientTokenId")]
    [Arguments(HttpStatusCode.BadRequest, "InvalidParameterValue")]
    [Arguments(HttpStatusCode.BadRequest, "InvalidMessageContents")]
    public async Task Definitive_aws_failures_count_an_attempt(HttpStatusCode status, string code)
    {
        await Assert.That(AwsMessagingPublisher.IsTransient(Service(status, code))).IsFalse();
    }

    [Test]
    public async Task Network_failures_are_transient_and_programming_errors_are_not()
    {
        await Assert.That(AwsMessagingPublisher.IsTransient(new HttpRequestException("conexão recusada"))).IsTrue();
        await Assert.That(AwsMessagingPublisher.IsTransient(new AmazonClientException("Unable to connect", new SocketException()))).IsTrue();
        await Assert.That(AwsMessagingPublisher.IsTransient(new TimeoutException())).IsTrue();
        await Assert.That(AwsMessagingPublisher.IsTransient(new ArgumentException("mensagem inválida"))).IsFalse();
    }

    [Test]
    public async Task Only_rejected_credentials_trigger_rotation()
    {
        await Assert.That(AwsClientFactory.IsCredentialFailure(Service(HttpStatusCode.Forbidden, "InvalidClientTokenId"))).IsTrue();
        await Assert.That(AwsClientFactory.IsCredentialFailure(Service(HttpStatusCode.BadRequest, "ExpiredToken"))).IsTrue();
        await Assert.That(AwsClientFactory.IsCredentialFailure(Service(HttpStatusCode.Forbidden, "SignatureDoesNotMatch"))).IsTrue();
        await Assert.That(AwsClientFactory.IsCredentialFailure(Service(HttpStatusCode.Forbidden, "AccessDenied"))).IsFalse();
        await Assert.That(AwsClientFactory.IsCredentialFailure(new HttpRequestException("rede"))).IsFalse();
    }

    [Test]
    public async Task Vault_down_fails_as_transport_unavailable()
    {
        await using var sp = Build(secret: null);
        var publisher = sp.GetRequiredService<IMessagePublisher>();

        var failure = await Assert.That(async () => await publisher.PublishAsync(Envelope(), CancellationToken.None)).Throws<AwsMessagingUnavailableException>();

        await Assert.That(failure).IsAssignableTo<ITransportUnavailableException>();
    }

    // Credenciais e rotação

    [Test]
    public async Task Secret_formats_are_parsed_and_malformed_ones_refused()
    {
        await Assert.That(AwsClientFactory.TryParseCredentials("AKIA:segredo", out var basic)).IsTrue();
        await Assert.That(basic).IsTypeOf<BasicAWSCredentials>();
        await Assert.That(AwsClientFactory.TryParseCredentials("ASIA:segredo:token-de-sessao", out var session)).IsTrue();
        await Assert.That(session).IsTypeOf<SessionAWSCredentials>();
        await Assert.That(AwsClientFactory.TryParseCredentials("sem-separador", out _)).IsFalse();
        await Assert.That(AwsClientFactory.TryParseCredentials(":segredo", out _)).IsFalse();
        await Assert.That(AwsClientFactory.TryParseCredentials("AKIA:", out _)).IsFalse();
    }

    [Test]
    public async Task Invalidated_clients_are_recreated_once_rereading_the_secret()
    {
        await using var sp = Build(Credentials);
        var reader = new CountingSecretReader(sp.GetRequiredService<ISecretReader>());
        await using var factory = new AwsClientFactory(reader, sp.GetRequiredService<IOptions<AwsMessagingOptions>>());

        var first = await factory.GetSqsAsync(CancellationToken.None);
        var firstSns = await factory.GetSnsAsync(CancellationToken.None);
        await Assert.That(await factory.GetSqsAsync(CancellationToken.None)).IsSameReferenceAs(first);

        await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => factory.InvalidateAsync(first)));
        var clients = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => factory.GetSqsAsync(CancellationToken.None)));

        await Assert.That(clients.Distinct().Count()).IsEqualTo(1);
        await Assert.That(clients[0]).IsNotSameReferenceAs(first);
        await Assert.That(await factory.GetSnsAsync(CancellationToken.None)).IsNotSameReferenceAs(firstSns);
        await Assert.That(reader.Reads).IsEqualTo(2);

        // Invalidação atrasada com os clientes antigos não derruba os novos
        await factory.InvalidateAsync(firstSns);
        await Assert.That(await factory.GetSqsAsync(CancellationToken.None)).IsSameReferenceAs(clients[0]);
        await Assert.That(reader.Reads).IsEqualTo(2);
    }

    // Consumo: validação do envelope, limite de payload, definição

    [Test]
    public async Task Message_without_id_or_with_empty_guid_is_rejected()
    {
        await Assert.That(AwsSqsConsumer.ReadEnvelope(Received(null), 256 * 1024, out var missing)).IsNull();
        await Assert.That(missing).Contains("messageId");
        await Assert.That(AwsSqsConsumer.ReadEnvelope(Received(Guid.Empty.ToString()), 256 * 1024, out var empty)).IsNull();
        await Assert.That(empty).IsNotNull();
    }

    [Test]
    public async Task Payload_above_the_limit_and_missing_type_are_rejected()
    {
        await Assert.That(AwsSqsConsumer.ReadEnvelope(Received(Guid.NewGuid().ToString(), new string('a', 2000)), 1024, out var big)).IsNull();
        await Assert.That(big).Contains("1024");
        await Assert.That(AwsSqsConsumer.ReadEnvelope(Received(Guid.NewGuid().ToString(), type: null), 256 * 1024, out var untyped)).IsNull();
        await Assert.That(untyped).IsNotNull();
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
            var envelope = AwsSqsConsumer.ReadEnvelope(Received(id.ToString(), extra: new()
            {
                ["version"] = "3",
                ["occurredAt"] = "2026-10-29T09:00:00.0000000-03:00",
                ["correlationId"] = "corr",
            }), 256 * 1024, out var error);

            await Assert.That(error).IsNull();
            await Assert.That(envelope!.MessageId).IsEqualTo(id);
            await Assert.That(envelope.Version).IsEqualTo(3);
            await Assert.That(envelope.OccurredAt.Offset).IsEqualTo(TimeSpan.Zero);
            await Assert.That(envelope.OccurredAt.UtcDateTime).IsEqualTo(new DateTime(2026, 10, 29, 12, 0, 0, DateTimeKind.Utc));
            await Assert.That(envelope.CorrelationId).IsEqualTo("corr");
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Test]
    public async Task Incoherent_definition_is_refused_on_startup()
    {
        await Assert.That(() => new SqsConsumerDefinition("http://sqs.sa-east-1.amazonaws.com/1/fila").Validate(requireTls: true)).Throws<InvalidOperationException>();
        await Assert.That(() => new SqsConsumerDefinition("arn:aws:sqs:sa-east-1:1:fila").Validate(requireTls: false)).Throws<InvalidOperationException>();
        await Assert.That(() => new SqsConsumerDefinition(QueueUrl, MaxNumberOfMessages: 11).Validate(true)).Throws<InvalidOperationException>();
        await Assert.That(() => new SqsConsumerDefinition(QueueUrl, WaitTimeSeconds: 21).Validate(true)).Throws<InvalidOperationException>();
        await Assert.That(() => new SqsConsumerDefinition(QueueUrl, VisibilityTimeoutSeconds: 0).Validate(true)).Throws<InvalidOperationException>();
        await Assert.That(() => new SqsConsumerDefinition(QueueUrl, VisibilityTimeoutSeconds: 43_201).Validate(true)).Throws<InvalidOperationException>();
        await Assert.That(() => new SqsConsumerDefinition(QueueUrl, ConcurrencyLimit: 0).Validate(true)).Throws<InvalidOperationException>();
        new SqsConsumerDefinition(QueueUrl).Validate(true);
        new SqsConsumerDefinition("http://localhost:4566/000000000000/fila").Validate(requireTls: false);
    }

    [Test]
    public async Task Queue_name_in_metrics_does_not_expose_the_account()
    {
        await Assert.That(new SqsConsumerDefinition(QueueUrl).QueueName).IsEqualTo("tec-testes");
        await Assert.That(new SqsConsumerDefinition(QueueUrl + "/").QueueName).IsEqualTo("tec-testes");
    }

    [Test]
    public async Task Retry_delay_grows_and_is_capped()
    {
        await Assert.That(AwsSqsConsumer.RetryDelaySeconds(1)).IsEqualTo(2);
        await Assert.That(AwsSqsConsumer.RetryDelaySeconds(3)).IsEqualTo(8);
        await Assert.That(AwsSqsConsumer.RetryDelaySeconds(9)).IsEqualTo(300);
        await Assert.That(AwsSqsConsumer.RetryDelaySeconds(100)).IsEqualTo(300);
        await Assert.That(AwsSqsConsumer.RetryDelaySeconds(0)).IsEqualTo(2);
    }

    // Registro e health check

    [Test]
    public async Task Adding_the_same_consumer_twice_registers_one_hosted_service()
    {
        var services = new ServiceCollection();
        var builder = services.AddTecMessaging().UseAws(o => o.DefaultDestination = QueueUrl);
        var before = services.Count(d => d.ServiceType == typeof(IHostedService));

        builder.AddAwsSqsConsumer<FilaSqsDeTesteConsumer>().AddAwsSqsConsumer<FilaSqsDeTesteConsumer>().AddConsumer<FilaSqsDeTesteConsumer>();

        await Assert.That(services.Count(d => d.ServiceType == typeof(IHostedService))).IsEqualTo(before + 1);
    }

    [Test]
    public async Task Health_check_is_healthy_when_disabled_and_fails_fast_with_the_vault_down()
    {
        await using var disabled = Build(Credentials, o => o.Enabled = false);
        await using var vaultDown = Build(secret: null);

        var off = await new AwsMessagingHealthCheck(disabled.GetRequiredService<AwsClientFactory>(), disabled.GetRequiredService<IOptions<AwsMessagingOptions>>())
            .CheckHealthAsync(new HealthCheckContext());
        var down = await new AwsMessagingHealthCheck(vaultDown.GetRequiredService<AwsClientFactory>(), vaultDown.GetRequiredService<IOptions<AwsMessagingOptions>>())
            .CheckHealthAsync(new HealthCheckContext()).WaitAsync(TimeSpan.FromSeconds(10));

        await Assert.That(off.Status).IsEqualTo(HealthStatus.Healthy);
        await Assert.That(down.Status).IsEqualTo(HealthStatus.Unhealthy);
    }
}
