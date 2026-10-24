using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using TEC.Messaging.AzureServiceBus;
using TEC.Messaging.AzureServiceBus.DependencyInjection;
using TEC.Messaging.DependencyInjection;
using TEC.Vault;
using TEC.Vault.DependencyInjection;
using TEC.Vault.InMemory;

namespace TEC.Messaging.Tests.AzureServiceBus;

public class AzureServiceBusUnitTests
{
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
        var services = new ServiceCollection();
        services.AddTecVault(v => v.UseInMemory(o =>
        {
            o.AllowOutsideDevelopment = true;
            o.InitialSecrets["azure-servicebus"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dGVzdA==";
        }));
        services.AddTecMessaging().UseAzureServiceBus(o => o.DefaultTopic = "meu-topico");

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<AzureServiceBusOptions>>().Value;

        await Assert.That(options.DefaultTopic).IsEqualTo("meu-topico");
    }
}
