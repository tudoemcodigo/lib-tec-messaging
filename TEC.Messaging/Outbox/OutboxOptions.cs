using Microsoft.Extensions.Options;

namespace TEC.Messaging.Outbox;

/// <summary>Opções do relay do Outbox (<c>builder.AddOutboxRelay(o =&gt; ...)</c>), validadas na subida.</summary>
public sealed class OutboxOptions
{
    /// <summary>Mensagens reservadas por ciclo (1 a 1000). Padrão 50.</summary>
    public int BatchSize { get; set; } = 50;

    /// <summary>Espera entre ciclos quando o lote veio incompleto (10 ms a 1 min). Padrão 1 s.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Duração da reserva de um lote (5 s a 1 h). Padrão 2 min. O relay libera o resto do lote antes de 80% dela.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>Falhas até a mensagem virar <see cref="OutboxMessageStatus.Dead"/> (1 a 100). Padrão 10.</summary>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>Espera depois da primeira falha; dobra a cada falha (1 s a 1 h). Padrão 2 s.</summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Teto da espera entre tentativas (≥ <see cref="InitialRetryDelay"/>, até 1 dia). Padrão 5 min.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Falhas seguidas num lote que indicam broker indisponível (1 a 100). Padrão 3: o relay devolve o resto do lote sem
    /// contar tentativa e pausa por <see cref="PauseDuration"/>. Uma mensagem defeituosa isolada não pausa nada.
    /// </summary>
    public int ConsecutiveFailuresToPause { get; set; } = 3;

    /// <summary>Pausa depois de falhas seguidas (100 ms a 10 min). Padrão 10 s.</summary>
    public TimeSpan PauseDuration { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Retenção das publicadas antes da limpeza (1 h a 365 dias). Padrão 7 dias.</summary>
    public TimeSpan PublishedRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>Intervalo da limpeza (1 min a 1 dia). Padrão 1 h.</summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Intervalo de atualização das métricas de pendentes; <see cref="TimeSpan.Zero"/> desliga (até 1 h). Padrão 30 s.</summary>
    public TimeSpan StatisticsInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Espera antes da tentativa seguinte à <paramref name="attempts"/>-ésima falha.</summary>
    internal TimeSpan RetryDelay(int attempts)
    {
        var exponent = Math.Clamp(attempts - 1, 0, 30);
        var ticks = InitialRetryDelay.Ticks * Math.Pow(2, exponent);
        return ticks >= MaxRetryDelay.Ticks ? MaxRetryDelay : TimeSpan.FromTicks((long)ticks);
    }
}

internal sealed class OutboxOptionsValidator : IValidateOptions<OutboxOptions>
{
    public ValidateOptionsResult Validate(string? name, OutboxOptions options)
    {
        var errors = new List<string>();
        Check(errors, options.BatchSize is >= 1 and <= 1000, "BatchSize deve estar entre 1 e 1000.");
        Check(errors, InRange(options.PollInterval, TimeSpan.FromMilliseconds(10), TimeSpan.FromMinutes(1)), "PollInterval deve estar entre 10 ms e 1 min.");
        Check(errors, InRange(options.LeaseDuration, TimeSpan.FromSeconds(5), TimeSpan.FromHours(1)), "LeaseDuration deve estar entre 5 s e 1 h.");
        Check(errors, options.MaxAttempts is >= 1 and <= 100, "MaxAttempts deve estar entre 1 e 100.");
        Check(errors, InRange(options.InitialRetryDelay, TimeSpan.FromSeconds(1), TimeSpan.FromHours(1)), "InitialRetryDelay deve estar entre 1 s e 1 h.");
        Check(errors, InRange(options.MaxRetryDelay, options.InitialRetryDelay, TimeSpan.FromDays(1)), "MaxRetryDelay deve estar entre InitialRetryDelay e 1 dia.");
        Check(errors, options.ConsecutiveFailuresToPause is >= 1 and <= 100, "ConsecutiveFailuresToPause deve estar entre 1 e 100.");
        Check(errors, InRange(options.PauseDuration, TimeSpan.FromMilliseconds(100), TimeSpan.FromMinutes(10)), "PauseDuration deve estar entre 100 ms e 10 min.");
        Check(errors, InRange(options.PublishedRetention, TimeSpan.FromHours(1), TimeSpan.FromDays(365)), "PublishedRetention deve estar entre 1 h e 365 dias.");
        Check(errors, InRange(options.CleanupInterval, TimeSpan.FromMinutes(1), TimeSpan.FromDays(1)), "CleanupInterval deve estar entre 1 min e 1 dia.");
        Check(errors, InRange(options.StatisticsInterval, TimeSpan.Zero, TimeSpan.FromHours(1)), "StatisticsInterval deve estar entre 0 e 1 h.");
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }

    internal static bool InRange(TimeSpan value, TimeSpan min, TimeSpan max) => value >= min && value <= max;

    internal static void Check(List<string> errors, bool ok, string message)
    {
        if (!ok)
            errors.Add(message);
    }
}
