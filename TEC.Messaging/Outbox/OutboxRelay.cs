using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Outbox;

/// <summary>
/// Publica o Outbox em segundo plano. A cada ciclo: reserva um lote (transação curta), publica cada mensagem fora de
/// transação e grava os desfechos (outra transação curta). Vários relays (vários processos) podem rodar juntos: a
/// reserva impede a publicação simultânea da mesma mensagem.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>Falha de uma mensagem não bloqueia as outras: ela entra em espera com backoff exponencial e, depois de
/// <see cref="OutboxOptions.MaxAttempts"/>, vira <see cref="OutboxMessageStatus.Dead"/>.</description></item>
/// <item><description>Falhas seguidas (<see cref="OutboxOptions.ConsecutiveFailuresToPause"/>) indicam broker fora: o resto do lote
/// volta sem contar tentativa e o relay pausa.</description></item>
/// <item><description>A ordem é a de ocorrência dentro do lote, mas <b>não é garantida</b> entre lotes, relays e novas tentativas:
/// consumidores devem ser idempotentes e tolerar reordenação.</description></item>
/// </list>
/// </remarks>
internal sealed partial class OutboxRelay(
    IServiceScopeFactory scopes, IOptions<OutboxOptions> options, MessagingMetrics metrics, TimeProvider time, ILogger<OutboxRelay> logger)
    : BackgroundService
{
    private const int PurgeBatch = 5000;
    private static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        var nextCleanup = time.GetUtcNow();
        var nextStatistics = time.GetUtcNow();
        while (!stoppingToken.IsCancellationRequested)
        {
            var wait = settings.PollInterval;
            try
            {
                var cycle = await RunCycleAsync(settings, stoppingToken).ConfigureAwait(false);
                if (cycle.Paused)
                    wait = settings.PauseDuration;
                else if (cycle.Leased >= settings.BatchSize)
                    wait = TimeSpan.Zero;

                var now = time.GetUtcNow();
                if (now >= nextCleanup)
                {
                    await CleanupAsync(settings, now, stoppingToken).ConfigureAwait(false);
                    nextCleanup = now + settings.CleanupInterval;
                }

                if (settings.StatisticsInterval > TimeSpan.Zero && now >= nextStatistics)
                {
                    await UpdateStatisticsAsync(now, stoppingToken).ConfigureAwait(false);
                    nextStatistics = now + settings.StatisticsInterval;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogCycleFailed(ex);
                wait = settings.PauseDuration;
            }

            if (wait > TimeSpan.Zero)
            {
                try
                {
                    await Task.Delay(wait, time, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    /// <summary>Um ciclo: reserva, publica e conclui. Exposto para os testes.</summary>
    internal async Task<RelayCycle> RunCycleAsync(OutboxOptions settings, CancellationToken stoppingToken)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
            var publisher = scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

            var leased = await store.LeaseAsync(settings.BatchSize, settings.LeaseDuration, stoppingToken).ConfigureAwait(false);
            if (leased.Count == 0)
                return new RelayCycle(0, 0, false);

            var started = time.GetUtcNow();
            var deadline = started + (settings.LeaseDuration * 0.8);
            var outcomes = new List<OutboxOutcome>(leased.Count);
            var consecutiveFailures = 0;
            var paused = false;
            var published = 0;

            for (var i = 0; i < leased.Count; i++)
            {
                var message = leased[i];
                var now = time.GetUtcNow();
                if (stoppingToken.IsCancellationRequested || now >= deadline || consecutiveFailures >= settings.ConsecutiveFailuresToPause)
                {
                    paused = consecutiveFailures >= settings.ConsecutiveFailuresToPause;
                    var retryAt = paused ? now + settings.PauseDuration : now;
                    for (var j = i; j < leased.Count; j++)
                        outcomes.Add(OutboxOutcome.Released(leased[j], retryAt));
                    if (paused)
                        LogPaused(consecutiveFailures, leased.Count - i, settings.PauseDuration);
                    break;
                }

                try
                {
                    await publisher.PublishAsync(message.Envelope, stoppingToken).ConfigureAwait(false);
                    outcomes.Add(OutboxOutcome.Published(message));
                    metrics.Published(message.Envelope.Type);
                    consecutiveFailures = 0;
                    published++;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    for (var j = i; j < leased.Count; j++)
                        outcomes.Add(OutboxOutcome.Released(leased[j], now));
                    break;
                }
                catch (Exception ex)
                {
                    consecutiveFailures++;
                    var attempts = message.Attempts + 1;
                    var error = $"{ex.GetType().Name}: {ex.Message}";
                    metrics.PublishFailed(message.Envelope.Type);
                    if (attempts >= settings.MaxAttempts)
                    {
                        outcomes.Add(OutboxOutcome.Dead(message, attempts, error));
                        metrics.Dead(message.Envelope.Type);
                        LogDead(message.Envelope.MessageId, message.Envelope.Type, attempts, ex.GetType().Name);
                    }
                    else
                    {
                        var delay = settings.RetryDelay(attempts);
                        outcomes.Add(OutboxOutcome.Failed(message, attempts, time.GetUtcNow() + delay, error));
                        LogPublishFailed(message.Envelope.MessageId, message.Envelope.Type, attempts, delay, ex.GetType().Name);
                    }
                }
            }

            // A conclusão roda mesmo no desligamento: sem ela, as publicadas seriam reenviadas quando a reserva vencer.
            using var completion = new CancellationTokenSource(CompletionTimeout);
            await store.CompleteAsync(outcomes, completion.Token).ConfigureAwait(false);
            return new RelayCycle(leased.Count, published, paused);
        }
    }

    private async Task CleanupAsync(OutboxOptions settings, DateTimeOffset now, CancellationToken ct)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
            var removed = await store.PurgePublishedAsync(now - settings.PublishedRetention, PurgeBatch, ct).ConfigureAwait(false);
            if (removed > 0)
                LogPurged(removed);
        }
    }

    private async Task UpdateStatisticsAsync(DateTimeOffset now, CancellationToken ct)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            if (scope.ServiceProvider.GetService<IOutboxAdministration>() is not { } administration)
                return;
            var stats = await administration.GetStatisticsAsync(ct).ConfigureAwait(false);
            var oldest = stats.OldestPendingOccurredAt is { } occurred ? now - occurred : (TimeSpan?)null;
            metrics.UpdateOutbox(stats.Pending, stats.PendingWithErrors, stats.Dead, oldest);
        }
    }

    [LoggerMessage(EventId = 4001, Level = LogLevel.Warning, Message = "Falha ao publicar a mensagem {MessageId} ({Type}), tentativa {Attempt}; nova tentativa em {Delay}: {Error}")]
    private partial void LogPublishFailed(Guid messageId, string type, int attempt, TimeSpan delay, string error);

    [LoggerMessage(EventId = 4002, Level = LogLevel.Error, Message = "Mensagem {MessageId} ({Type}) esgotou as {Attempts} tentativas e foi marcada como morta: {Error}")]
    private partial void LogDead(Guid messageId, string type, int attempts, string error);

    [LoggerMessage(EventId = 4003, Level = LogLevel.Warning, Message = "{Failures} falhas seguidas ao publicar: {Released} mensagem(ns) devolvida(s) ao Outbox; relay pausado por {Pause}")]
    private partial void LogPaused(int failures, int released, TimeSpan pause);

    [LoggerMessage(EventId = 4004, Level = LogLevel.Error, Message = "Falha no ciclo do relay do Outbox")]
    private partial void LogCycleFailed(Exception exception);

    [LoggerMessage(EventId = 4005, Level = LogLevel.Information, Message = "Limpeza do Outbox: {Removed} mensagem(ns) publicada(s) removida(s)")]
    private partial void LogPurged(int removed);
}

/// <summary>Resumo de um ciclo do relay.</summary>
internal readonly record struct RelayCycle(int Leased, int Published, bool Paused);
