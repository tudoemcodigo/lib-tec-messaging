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

    /// <summary>
    /// Espera inicial antes de reabrir canal e conexão de um consumidor depois de uma queda (100 ms a 1 min). Dobra a cada falha
    /// seguida, com variação aleatória, até <see cref="MaxReconnectDelay"/>; volta ao início quando o consumo é retomado.
    /// Padrão 1 s.
    /// </summary>
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Maior espera entre tentativas de reabrir o consumo (1 s a 10 min). Padrão 1 min.</summary>
    public TimeSpan MaxReconnectDelay { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Circuit breaker da criação da conexão (leitura da URI no cofre + conexão com o broker), compartilhado por publicador,
    /// consumidores, monitor de DLQ e health check. Ligado por padrão.
    /// </summary>
    public RabbitMqCircuitBreakerOptions CircuitBreaker { get; set; } = new();

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
        if (options.ReconnectDelay < TimeSpan.FromMilliseconds(100) || options.ReconnectDelay > TimeSpan.FromMinutes(1))
            errors.Add("ReconnectDelay deve estar entre 100 ms e 1 min.");
        if (options.MaxReconnectDelay < TimeSpan.FromSeconds(1) || options.MaxReconnectDelay > TimeSpan.FromMinutes(10)
            || options.MaxReconnectDelay < options.ReconnectDelay)
            errors.Add("MaxReconnectDelay deve estar entre 1 s e 10 min, e não menor que ReconnectDelay.");
        if (options.CircuitBreaker is not { } circuit)
            errors.Add("CircuitBreaker é obrigatório.");
        else if (circuit.Enabled)
        {
            if (double.IsNaN(circuit.FailureRatio) || circuit.FailureRatio is <= 0 or > 1)
                errors.Add("CircuitBreaker:FailureRatio deve ser maior que 0 e no máximo 1.");
            if (circuit.MinimumThroughput is < 2 or > 10_000)
                errors.Add("CircuitBreaker:MinimumThroughput deve estar entre 2 e 10.000.");
            if (circuit.SamplingDuration < TimeSpan.FromMilliseconds(500) || circuit.SamplingDuration > TimeSpan.FromHours(1))
                errors.Add("CircuitBreaker:SamplingDuration deve estar entre 0,5 s e 1 h.");
            if (circuit.BreakDuration < TimeSpan.FromMilliseconds(500) || circuit.BreakDuration > TimeSpan.FromHours(1))
                errors.Add("CircuitBreaker:BreakDuration deve estar entre 0,5 s e 1 h.");
        }
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}

/// <summary>
/// Circuit breaker da criação da conexão com o RabbitMQ (<see cref="RabbitMqOptions.CircuitBreaker"/>).
/// </summary>
/// <remarks>
/// Toda falha ao criar a conexão conta (cofre, rede, autenticação no broker). O circuito abre quando, dentro de
/// <see cref="SamplingDuration"/>, houve pelo menos <see cref="MinimumThroughput"/> tentativas e a proporção de falhas chegou a
/// <see cref="FailureRatio"/>; aberto, quem pede a conexão recebe <see cref="RabbitMqCircuitOpenException"/> na hora, sem
/// tocar no cofre nem no broker. Depois de <see cref="BreakDuration"/>, uma tentativa de teste decide se fecha ou abre de novo.
/// </remarks>
public sealed class RabbitMqCircuitBreakerOptions
{
    /// <summary>Liga o circuit breaker. Padrão <c>true</c>.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Proporção de falhas que abre o circuito (maior que 0, até 1). Padrão 0,5.</summary>
    public double FailureRatio { get; set; } = 0.5;

    /// <summary>
    /// Mínimo de tentativas de conexão na janela (2 a 10.000). Padrão 5: conexões são criadas raramente (uma por processo),
    /// então a janela tem bem menos tentativas que requisições.
    /// </summary>
    public int MinimumThroughput { get; set; } = 5;

    /// <summary>Janela de amostragem (0,5 s a 1 h). Padrão 30 s.</summary>
    public TimeSpan SamplingDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Tempo com o circuito aberto antes da tentativa de teste (0,5 s a 1 h). Padrão 30 s.</summary>
    public TimeSpan BreakDuration { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// Conexão com o RabbitMQ recusada sem tentar: o circuito está aberto depois de falhas repetidas ao conectar
/// (<see cref="RabbitMqOptions.CircuitBreaker"/>). O Outbox mantém as mensagens e tenta de novo depois.
/// </summary>
public sealed class RabbitMqCircuitOpenException : Exception
{
    /// <summary>Cria a exceção.</summary>
    public RabbitMqCircuitOpenException() : base("Circuito aberto: o RabbitMQ falhou repetidas vezes; conexão não tentada.")
    {
    }

    /// <summary>Cria a exceção com mensagem.</summary>
    public RabbitMqCircuitOpenException(string message) : base(message)
    {
    }

    /// <summary>Cria a exceção com mensagem e causa.</summary>
    public RabbitMqCircuitOpenException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
