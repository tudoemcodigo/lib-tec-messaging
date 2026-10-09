using System.Globalization;

namespace TEC.Messaging.RabbitMQ;

/// <summary>
/// Fila de um consumidor e a topologia dela. Durável: fila principal <c>quorum</c> ligada à exchange, filas de espera
/// (TTL; ao expirar, a mensagem volta à principal) e DLQ <c>&lt;fila&gt;.dlq</c>. Temporária: fila exclusiva e
/// auto-delete, sem espera nem DLQ (ex.: notificação em tempo real de uma instância, em que perder um evento é aceitável).
/// </summary>
/// <param name="Name">Nome da fila (minúsculas, dígitos, <c>.</c>, <c>-</c>, <c>_</c>; até 200).</param>
/// <param name="RoutingKeys">Routing keys ligadas (aceitam os curingas <c>*</c> e <c>#</c> do topic).</param>
public sealed record QueueDefinition(string Name, IReadOnlyList<string> RoutingKeys)
{
    /// <summary>Esperas padrão entre falhas: 30 s → 2 min → 10 min → 30 min.</summary>
    public static readonly IReadOnlyList<TimeSpan> DefaultRetryDelays =
        [TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(30)];

    /// <summary>Mensagens entregues sem confirmação ao mesmo tempo (1 a 1000). Padrão 10.</summary>
    public ushort Prefetch { get; init; } = 10;

    /// <summary>Na N-ésima falha a mensagem vai para a DLQ (1 a 100). Padrão 5.</summary>
    public int MaxAttempts { get; init; } = 5;

    /// <summary>Esperas entre falhas (1 a 10 itens, cada um de 1 s a 1 dia); a última se repete.</summary>
    public IReadOnlyList<TimeSpan> RetryDelays { get; init; } = DefaultRetryDelays;

    /// <summary>
    /// Rede de segurança contra laços de reentrega (<c>x-delivery-limit</c> da fila quorum; devolução por indisponibilidade
    /// conta). Acima do limite a mensagem vai para a DLQ, sem se perder. Padrão 50.
    /// </summary>
    public int DeliveryLimit { get; init; } = 50;

    /// <summary>Fila temporária (exclusiva, auto-delete, sem retry nem DLQ).</summary>
    public bool Transient { get; init; }

    /// <summary>Nome da DLQ.</summary>
    public string DeadLetterQueue => $"{Name}.dlq";

    /// <summary>Nome da fila de espera de um atraso.</summary>
    /// <param name="delay">Atraso.</param>
    /// <returns>Nome (ex.: <c>pedidos.retry.2m</c>).</returns>
    public string RetryQueue(TimeSpan delay) => $"{Name}.retry.{Label(delay)}";

    /// <summary>Espera antes da tentativa seguinte à <paramref name="failures"/>-ésima falha.</summary>
    /// <param name="failures">Falhas até agora (1 ou mais).</param>
    /// <returns>Espera.</returns>
    public TimeSpan RetryDelayAfter(int failures) => RetryDelays[Math.Clamp(failures - 1, 0, RetryDelays.Count - 1)];

    /// <summary>Valida a definição; lança na subida do consumidor.</summary>
    /// <exception cref="InvalidOperationException">Definição inválida.</exception>
    internal void Validate()
    {
        if (!IsValidName(Name) || Name.EndsWith(".dlq", StringComparison.Ordinal))
            throw new InvalidOperationException($"Nome de fila inválido: '{Name}'.");
        if (RoutingKeys is null || RoutingKeys.Count > 100 || RoutingKeys.Any(k => string.IsNullOrWhiteSpace(k) || k.Length > 255))
            throw new InvalidOperationException($"Routing keys inválidas na fila '{Name}' (até 100, cada uma com até 255 caracteres).");
        if (Prefetch is 0 or > 1000)
            throw new InvalidOperationException($"Prefetch da fila '{Name}' deve estar entre 1 e 1000.");
        if (MaxAttempts is < 1 or > 100)
            throw new InvalidOperationException($"MaxAttempts da fila '{Name}' deve estar entre 1 e 100.");
        if (RetryDelays is null || RetryDelays.Count is < 1 or > 10 || RetryDelays.Any(d => d < TimeSpan.FromSeconds(1) || d > TimeSpan.FromDays(1)))
            throw new InvalidOperationException($"RetryDelays da fila '{Name}': de 1 a 10 esperas, cada uma entre 1 s e 1 dia.");
        if (DeliveryLimit is < 1 or > 1000)
            throw new InvalidOperationException($"DeliveryLimit da fila '{Name}' deve estar entre 1 e 1000.");
    }

    internal static bool IsValidName(string? name) =>
        !string.IsNullOrEmpty(name) && name.Length <= 200
        && name.All(c => c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '.' or '-' or '_');

    private static string Label(TimeSpan delay) =>
        delay.TotalMinutes >= 1 && delay.Seconds == 0
            ? string.Create(CultureInfo.InvariantCulture, $"{(int)delay.TotalMinutes}m")
            : string.Create(CultureInfo.InvariantCulture, $"{(int)delay.TotalSeconds}s");
}
