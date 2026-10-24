using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using TEC.Messaging.Aws;
using TEC.Messaging.Aws.DependencyInjection;
using TEC.Messaging.AzureServiceBus;
using TEC.Messaging.AzureServiceBus.DependencyInjection;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Internal;
using TEC.Messaging.PostgreSql.DependencyInjection;
using TEC.Messaging.PostgreSql.Interception;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Tests;

public class MessagingIssueRegressionTests
{
    private sealed class DummyDbContext(DbContextOptions<DummyDbContext> options) : DbContext(options);

    [Test]
    public async Task Issue1_PostgreSql_Interceptor_Resolves_Successfully()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddTecMessaging().UsePostgreSql<DummyDbContext>();

        using var sp = services.BuildServiceProvider();
        var interceptor = sp.GetService<TecMessagingPostgreSqlSaveChangesInterceptor>();
        await Assert.That(interceptor).IsNotNull();

        var optionsBuilder = new DbContextOptionsBuilder<DummyDbContext>();
        optionsBuilder.UseTecMessagingPostgreSql(sp);
    }

    [Test]
    public async Task Issue2_AzureServiceBus_Disabled_Throws_ITransportUnavailableException()
    {
        var options = Options.Create(new AzureServiceBusOptions { Enabled = false });
        var publisher = new TEC.Messaging.AzureServiceBus.Internal.AzureServiceBusPublisher(
            null!, options);

        var envelope = new MessageEnvelope(Guid.NewGuid(), "test.type", 1, DateTimeOffset.UtcNow, null, null, null, "{}");

        try
        {
            await publisher.PublishAsync(envelope, CancellationToken.None);
            Assert.Fail("Deveria ter lançado AzureServiceBusUnavailableException");
        }
        catch (Exception ex)
        {
            await Assert.That(ex is ITransportUnavailableException).IsTrue();
        }
    }

    [Test]
    public async Task Issue2_Aws_Disabled_Throws_ITransportUnavailableException()
    {
        var options = Options.Create(new AwsMessagingOptions { Enabled = false });
        var publisher = new TEC.Messaging.Aws.Internal.AwsMessagingPublisher(
            null!, options);

        var envelope = new MessageEnvelope(Guid.NewGuid(), "test.type", 1, DateTimeOffset.UtcNow, null, null, null, "{}");

        try
        {
            await publisher.PublishAsync(envelope, CancellationToken.None);
            Assert.Fail("Deveria ter lançado AwsMessagingUnavailableException");
        }
        catch (Exception ex)
        {
            await Assert.That(ex is ITransportUnavailableException).IsTrue();
        }
    }

    [Test]
    public async Task Issue4_SensitiveDataSanitizer_Masks_Emails_Phones_And_Cards()
    {
        var text = "Erro do cliente user.test@dominio.com.br, fone (11) 98765-4321, cartao 4111222233334444 e amex 378282246310005";
        var sanitized = SensitiveDataSanitizer.Sanitize(text);

        await Assert.That(sanitized.Contains("user.test@dominio.com.br")).IsFalse();
        await Assert.That(sanitized.Contains("98765-4321")).IsFalse();
        await Assert.That(sanitized.Contains("4111222233334444")).IsFalse();
        await Assert.That(sanitized.Contains("378282246310005")).IsFalse();
    }

    [Test]
    public async Task Issue8_DeterministicGuid_Produces_Identical_Guid_For_NonGuid_MessageId()
    {
        var nonGuid = "pedido-abc-12345";
        var id1 = DeterministicGuid.Create(nonGuid);
        var id2 = DeterministicGuid.Create(nonGuid);

        await Assert.That(id1).IsNotEqualTo(Guid.Empty);
        await Assert.That(id1).IsEqualTo(id2);
    }
}
