using Microsoft.Extensions.Options;

namespace TEC.Messaging.Aws;

/// <summary>Opções do transporte AWS SQS/SNS (seção <c>Messaging:Aws</c> por padrão).</summary>
public sealed class AwsMessagingOptions
{
    /// <summary>Seção padrão da configuração.</summary>
    public const string SectionName = "Messaging:Aws";

    /// <summary>Habilita o transporte AWS. Padrão <c>true</c>.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Nome do segredo no TEC.Vault com credenciais ou URI AWS. Padrão <c>aws-messaging</c>.</summary>
    public string ConnectionSecretName { get; set; } = "aws-messaging";

    /// <summary>Região AWS (ex.: <c>us-east-1</c>, <c>sa-east-1</c>). Padrão <c>us-east-1</c>.</summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>ARN padrão do SNS Topic ou URL padrão da SQS Queue para publicação.</summary>
    public string DefaultDestination { get; set; } = "";

    /// <summary>Tempo limite para a confirmação de publicação. Padrão 10 s.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

internal sealed class AwsMessagingOptionsValidator : IValidateOptions<AwsMessagingOptions>
{
    public ValidateOptionsResult Validate(string? name, AwsMessagingOptions options)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(options.ConnectionSecretName))
            errors.Add("ConnectionSecretName é obrigatório.");
        if (string.IsNullOrWhiteSpace(options.Region))
            errors.Add("Region é obrigatória.");
        if (options.PublishTimeout < TimeSpan.FromSeconds(1) || options.PublishTimeout > TimeSpan.FromMinutes(5))
            errors.Add("PublishTimeout deve estar entre 1 s e 5 min.");

        return errors.Count > 0 ? ValidateOptionsResult.Fail(errors) : ValidateOptionsResult.Success;
    }
}
