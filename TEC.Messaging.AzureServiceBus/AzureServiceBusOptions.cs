using Microsoft.Extensions.Options;
using TEC.Messaging.Transport;

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

    /// <summary>
    /// Tópico (ou fila) padrão de saída para os eventos de integração. Vazio = a entidade tem o nome do tipo da mensagem. O
    /// health check abre um link de envio com esta entidade; vazio, ele responde <c>Degraded</c> sem testar a rede.
    /// </summary>
    public string DefaultTopic { get; set; } = "";

    /// <summary>
    /// Exige TLS: connection string do emulador local (<c>UseDevelopmentEmulator=true</c>, AMQP sem TLS) é recusada ao criar o
    /// cliente. <c>null</c> (padrão) = exige fora de Development (<c>IHostEnvironment.IsDevelopment()</c>). Fora do emulador o
    /// SDK sempre usa TLS.
    /// </summary>
    public bool? RequireTls { get; set; }

    /// <summary>Tempo limite para a confirmação de publicação. Padrão 10 s.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

/// <summary>
/// Publicação no Azure Service Bus impossível por indisponibilidade do transporte: transporte desligado, cliente impossível de
/// criar (cofre fora, connection string inválida), falha transitória do serviço (<c>ServiceBusException.IsTransient</c>, como
/// <c>ServiceBusy</c>, <c>ServiceTimeout</c> e <c>ServiceCommunicationProblem</c>), erro de rede ou confirmação fora do prazo. O
/// Outbox devolve as mensagens sem contar tentativa (<see cref="ITransportUnavailableException"/>). Falhas definitivas (tamanho,
/// entidade inexistente ou desabilitada, permissão negada) saem como vieram e contam tentativa.
/// </summary>
public sealed class AzureServiceBusUnavailableException : Exception, ITransportUnavailableException
{
    /// <summary>Cria uma nova instância de <see cref="AzureServiceBusUnavailableException"/>.</summary>
    public AzureServiceBusUnavailableException() : base("Azure Service Bus indisponível.") { }

    /// <summary>Cria uma nova instância com mensagem descritiva.</summary>
    public AzureServiceBusUnavailableException(string message) : base(message) { }

    /// <summary>Cria uma nova instância com mensagem e causa interna.</summary>
    public AzureServiceBusUnavailableException(string message, Exception innerException) : base(message, innerException) { }
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
