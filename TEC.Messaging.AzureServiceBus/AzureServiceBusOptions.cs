using Microsoft.Extensions.Options;

namespace TEC.Messaging.AzureServiceBus;

/// <summary>Opções do transporte Azure Service Bus (seção <c>Messaging:AzureServiceBus</c> por padrão).</summary>
public sealed class AzureServiceBusOptions
{
    /// <summary>Seção padrão da configuração.</summary>
    public const string SectionName = "Messaging:AzureServiceBus";

    /// <summary>Habilita o transporte do Azure Service Bus. Padrão <c>true</c>.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Nome do segredo no TEC.Vault com a connection string do Service Bus. Padrão <c>azure-servicebus</c>.</summary>
    public string ConnectionSecretName { get; set; } = "azure-servicebus";

    /// <summary>Tópico padrão de saída para os eventos de integração.</summary>
    public string DefaultTopic { get; set; } = "";

    /// <summary>Tempo limite para a confirmação de publicação. Padrão 10 s.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

internal sealed class AzureServiceBusOptionsValidator : IValidateOptions<AzureServiceBusOptions>
{
    public ValidateOptionsResult Validate(string? name, AzureServiceBusOptions options)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(options.ConnectionSecretName))
            errors.Add("ConnectionSecretName é obrigatório.");
        if (options.PublishTimeout < TimeSpan.FromSeconds(1) || options.PublishTimeout > TimeSpan.FromMinutes(5))
            errors.Add("PublishTimeout deve estar entre 1 s e 5 min.");

        return errors.Count > 0 ? ValidateOptionsResult.Fail(errors) : ValidateOptionsResult.Success;
    }
}
