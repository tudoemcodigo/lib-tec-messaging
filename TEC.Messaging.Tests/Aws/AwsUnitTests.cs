using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using TEC.Messaging.Aws;
using TEC.Messaging.Aws.DependencyInjection;
using TEC.Messaging.DependencyInjection;
using TEC.Vault;
using TEC.Vault.DependencyInjection;
using TEC.Vault.InMemory;

namespace TEC.Messaging.Tests.Aws;

public class AwsUnitTests
{
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
        var services = new ServiceCollection();
        services.AddTecVault(v => v.UseInMemory(o =>
        {
            o.AllowOutsideDevelopment = true;
            o.InitialSecrets["aws-messaging"] = "AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        }));
        services.AddTecMessaging().UseAws(o => o.Region = "sa-east-1");

        var sp = services.BuildServiceProvider();
        var options = sp.GetRequiredService<IOptions<AwsMessagingOptions>>().Value;

        await Assert.That(options.Region).IsEqualTo("sa-east-1");
    }
}
