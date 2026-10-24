using System.Diagnostics;

namespace TEC.Messaging.Context;

/// <summary>
/// Correlação ponta a ponta do fluxo atual (fluxo assíncrono, via <see cref="AsyncLocal{T}"/>): definida pela borda
/// (middleware HTTP com o <c>X-Correlation-ID</c>, ou o consumidor com o envelope recebido) e copiada para todas as
/// mensagens gravadas no Outbox durante o fluxo.
/// </summary>
/// <remarks>
/// Sem escopo aberto, <see cref="CorrelationId"/> cai no <c>TraceId</c> da <see cref="Activity"/> atual (W3C). Valores
/// vazios, com caracteres de controle ou maiores que <see cref="MessageEnvelope.MaxIdLength"/> são ignorados: a
/// correlação costuma vir de cabeçalho HTTP, que é entrada não confiável.
/// </remarks>
public static class MessageContext
{
    private static readonly AsyncLocal<Scope?> CurrentScope = new();

    /// <summary>Correlação do fluxo, ou o <c>TraceId</c> da atividade atual, ou <c>null</c>.</summary>
    public static string? CorrelationId =>
        CurrentScope.Value?.CorrelationId
        ?? (Activity.Current is { IdFormat: ActivityIdFormat.W3C } activity ? activity.TraceId.ToHexString() : null);

    /// <summary>Mensagem que causou o fluxo atual (o <c>MessageId</c> consumido), ou <c>null</c>.</summary>
    public static string? CausationId => CurrentScope.Value?.CausationId;

    /// <summary>Abre um escopo com correlação e causa; o anterior volta no <c>Dispose</c>.</summary>
    /// <param name="correlationId">Correlação (ignorada se inválida).</param>
    /// <param name="causationId">Causa (ignorada se inválida).</param>
    /// <returns>Escopo a descartar no fim do fluxo.</returns>
    /// <example><code>using var _ = MessageContext.Begin(http.Request.Headers["X-Correlation-ID"]);</code></example>
    public static IDisposable Begin(string? correlationId, string? causationId = null)
    {
        var previous = CurrentScope.Value;
        CurrentScope.Value = new Scope(Sanitize(correlationId), Sanitize(causationId));
        return new Restore(previous);
    }

    internal static string? Sanitize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > MessageEnvelope.MaxIdLength)
            return null;
        foreach (var c in value)
        {
            if (char.IsControl(c))
                return null;
        }

        return value;
    }

    private sealed record Scope(string? CorrelationId, string? CausationId);

    private sealed class Restore(Scope? previous) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                CurrentScope.Value = previous;
        }
    }
}
