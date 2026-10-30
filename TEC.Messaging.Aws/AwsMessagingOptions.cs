using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TEC.Messaging.Internal;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Aws;

/// <summary>Opções do transporte AWS SQS/SNS (seção <c>Messaging:Aws</c> por padrão).</summary>
public sealed class AwsMessagingOptions
{
    /// <summary>Seção padrão da configuração.</summary>
    public const string SectionName = "Messaging:Aws";

    /// <summary>Habilita o transporte AWS. Padrão <c>true</c>.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Permite usar a cadeia padrão de credenciais da AWS (ambiente, IAM role) caso o segredo do cofre não esteja configurado ou falhe. Padrão <c>false</c>.</summary>
    public bool UseAmbientCredentials { get; set; } = false;

    /// <summary>
    /// Nome do segredo no TEC.Vault com as credenciais AWS: <c>accessKey:secretKey</c> ou, para credenciais temporárias (STS),
    /// <c>accessKey:secretKey:sessionToken</c>. Padrão <c>aws-messaging</c>. O segredo é relido quando a AWS recusa a credencial.
    /// </summary>
    public string ConnectionSecretName { get; set; } = "aws-messaging";

    /// <summary>Região AWS (ex.: <c>us-east-1</c>, <c>sa-east-1</c>). Padrão <c>us-east-1</c>.</summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    /// ARN do tópico SNS (<c>arn:aws:sns:...</c>) ou URL da fila SQS (<c>https://sqs...</c>) para publicação. Obrigatório com o
    /// transporte habilitado: validado na subida.
    /// </summary>
    public string DefaultDestination { get; set; } = "";

    /// <summary>Tempo limite para a confirmação de publicação. Padrão 10 s.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Exige TLS: URL de fila com <c>http://</c> (em <see cref="DefaultDestination"/> ou na definição de um consumidor) é recusada
    /// na subida, e os clientes nunca usam HTTP. <c>null</c> (padrão) = exige fora de Development
    /// (<c>IHostEnvironment.IsDevelopment()</c>).
    /// </summary>
    public bool? RequireTls { get; set; }
}

/// <summary>
/// Publicação na AWS impossível por indisponibilidade do transporte: transporte desligado, cliente impossível de criar
/// (cofre fora, credencial ausente), erro 5xx, limitação de taxa (throttling), erro de rede ou confirmação fora do prazo. O
/// Outbox devolve as mensagens sem contar tentativa (<see cref="ITransportUnavailableException"/>). Falhas definitivas
/// (fila ou tópico inexistente, permissão negada, credencial recusada, mensagem inválida ou grande demais) saem como vieram e
/// contam tentativa.
/// </summary>
public sealed class AwsMessagingUnavailableException : Exception, ITransportUnavailableException
{
    /// <summary>Cria uma nova instância de <see cref="AwsMessagingUnavailableException"/>.</summary>
    public AwsMessagingUnavailableException() : base("AWS Messaging indisponível.") { }

    /// <summary>Cria uma nova instância com mensagem descritiva.</summary>
    public AwsMessagingUnavailableException(string message) : base(message) { }

    /// <summary>Cria uma nova instância com mensagem e causa interna.</summary>
    public AwsMessagingUnavailableException(string message, Exception innerException) : base(message, innerException) { }
}

internal sealed class AwsMessagingOptionsValidator(IHostEnvironment? environment = null) : IValidateOptions<AwsMessagingOptions>
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
        if (options.Enabled && CheckDestination(options.DefaultDestination, TransportSecurity.RequiresTls(options.RequireTls, environment)) is { } error)
            errors.Add($"DefaultDestination: {error}");

        return errors.Count > 0 ? ValidateOptionsResult.Fail(errors) : ValidateOptionsResult.Success;
    }

    /// <summary>
    /// Motivo da recusa de um destino (ARN SNS ou URL SQS), ou <c>null</c> se válido. URL <c>http://</c> só sem exigência de TLS.
    /// </summary>
    internal static string? CheckDestination(string? destination, bool requireTls)
    {
        if (string.IsNullOrWhiteSpace(destination))
            return "obrigatório: informe o ARN do tópico SNS ou a URL da fila SQS.";
        if (destination.StartsWith("arn:aws:sns:", StringComparison.OrdinalIgnoreCase))
            return destination.Split(':').Length >= 6 ? null : "ARN do SNS incompleto (arn:aws:sns:região:conta:tópico).";
        if (!Uri.TryCreate(destination, UriKind.Absolute, out var uri) || (uri.Scheme != Uri.UriSchemeHttps && uri.Scheme != Uri.UriSchemeHttp))
            return "não é um ARN do SNS nem uma URL de fila SQS.";
        if (uri.Scheme == Uri.UriSchemeHttp && requireTls)
            return "URL http:// (sem TLS) recusada porque o transporte exige TLS (RequireTls; padrão fora de Development).";
        return null;
    }
}
