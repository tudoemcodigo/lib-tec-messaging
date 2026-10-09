using Microsoft.Extensions.Options;

namespace TEC.Messaging.RabbitMQ;

/// <summary>Opções do transporte RabbitMQ (seção <c>Messaging:RabbitMQ</c> por padrão), validadas na subida.</summary>
public sealed class RabbitMqOptions
{
    /// <summary>Seção padrão da configuração.</summary>
    public const string SectionName = "Messaging:RabbitMQ";

    /// <summary>
    /// Desliga o transporte (ex.: testes de integração da aplicação sem broker): consumidores não sobem e a publicação
    /// falha, então o Outbox retém as mensagens. Padrão <c>true</c>.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Nome do segredo (no TEC.Vault) com a URI AMQP (<c>amqps://usuario:senha@host:5671/vhost</c>). Padrão <c>rabbitmq</c>.</summary>
    public string ConnectionSecretName { get; set; } = "rabbitmq";

    /// <summary>Exchange topic dos eventos de integração (obrigatória).</summary>
    public string Exchange { get; set; } = "";

    /// <summary>
    /// Prefixo removido do tipo da mensagem para formar a routing key (ex.: <c>vendas.</c>: <c>vendas.pedido-aprovado</c> →
    /// <c>pedido-aprovado</c>). Vazio = a routing key é o tipo.
    /// </summary>
    public string RoutingKeyPrefixToRemove { get; set; } = "";

    /// <summary>Espera máxima pela confirmação do broker (1 s a 5 min). Padrão 10 s.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Heartbeat da conexão (5 s a 5 min). Padrão 30 s.</summary>
    public TimeSpan Heartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Intervalo da contagem das DLQs para a métrica <c>tec.messaging.dlq.messages</c>; zero desliga. Padrão 1 min.</summary>
    public TimeSpan DeadLetterMonitorInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Routing key de uma mensagem.</summary>
    internal string RoutingKeyOf(string type) =>
        RoutingKeyPrefixToRemove.Length > 0 && type.StartsWith(RoutingKeyPrefixToRemove, StringComparison.Ordinal)
            ? type[RoutingKeyPrefixToRemove.Length..]
            : type;
}

internal sealed class RabbitMqOptionsValidator : IValidateOptions<RabbitMqOptions>
{
    public ValidateOptionsResult Validate(string? name, RabbitMqOptions options)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(options.ConnectionSecretName))
            errors.Add("ConnectionSecretName é obrigatório.");
        if (!QueueDefinition.IsValidName(options.Exchange))
            errors.Add("Exchange é obrigatória: letras minúsculas, dígitos, '.', '-' e '_', até 200 caracteres.");
        if (options.RoutingKeyPrefixToRemove.Length > 100)
            errors.Add("RoutingKeyPrefixToRemove tem até 100 caracteres.");
        if (options.PublishTimeout < TimeSpan.FromSeconds(1) || options.PublishTimeout > TimeSpan.FromMinutes(5))
            errors.Add("PublishTimeout deve estar entre 1 s e 5 min.");
        if (options.Heartbeat < TimeSpan.FromSeconds(5) || options.Heartbeat > TimeSpan.FromMinutes(5))
            errors.Add("Heartbeat deve estar entre 5 s e 5 min.");
        if (options.DeadLetterMonitorInterval < TimeSpan.Zero || options.DeadLetterMonitorInterval > TimeSpan.FromHours(1))
            errors.Add("DeadLetterMonitorInterval deve estar entre 0 e 1 h.");
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}
