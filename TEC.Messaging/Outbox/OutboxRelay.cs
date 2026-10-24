using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;
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
/// <item><description>Transporte indisponível (exceção marcada com <see cref="ITransportUnavailableException"/>: broker fora,
/// circuito aberto, transporte desligado) não é defeito da mensagem: ela, as falhas seguidas que a precederam e o resto do lote
/// voltam sem contar tentativa e o relay pausa por <see cref="OutboxOptions.PauseDuration"/>. Uma indisponibilidade longa não
/// leva mensagens a <see cref="OutboxMessageStatus.Dead"/>.</description></item>
/// <item><description>Falhas seguidas sem essa marca (<see cref="OutboxOptions.ConsecutiveFailuresToPause"/>) também pausam o
/// relay e devolvem o resto do lote sem contar tentativa, mas as falhas em si contam (podem ser mensagens defeituosas; não
/// contar travaria o Outbox atrás delas).</description></item>
/// <item><description>A publicação de cada mensagem é cancelada ao fim de 80% da reserva: a mensagem e o resto do lote voltam
/// sem contar tentativa antes que outro relay possa assumi-las.</description></item>
/// <item><description>A ordem é a de ocorrência dentro do lote, mas <b>não é garantida</b> entre lotes, relays e novas tentativas:
/// consumidores devem ser idempotentes e tolerar reordenação.</description></item>
/// </list>
/// </remarks>
internal sealed partial class OutboxRelay(
    IServiceScopeFactory scopes, IOptions<OutboxOptions> options, MessagingMetrics metrics, TimeProvider time, ILogger<OutboxRelay> logger)
    : BackgroundService
{
    private const int PurgeBatch = 5000;

    // Teto de cada limpeza: apaga em lotes enquanto vierem cheios, sem prender o relay por muito tempo
    private static readonly TimeSpan MaxCleanupDuration = TimeSpan.FromMinutes(1);
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

            // Falhas da sequência atual: se ela terminar em transporte indisponível, nenhuma delas conta tentativa. Logs e
            // métricas das falhas saem só no fim, com o desfecho definitivo
            var streak = new List<int>();
            var failures = new List<(int Index, string ErrorType)>();

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

                // A publicação não pode passar do prazo da reserva: depois dele outro relay pode assumir a mensagem
                using var leaseEnd = new CancellationTokenSource(deadline - now, time);
                using var lease = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, leaseEnd.Token);
                try
                {
                    await publisher.PublishAsync(message.Envelope, lease.Token).ConfigureAwait(false);
                    outcomes.Add(OutboxOutcome.Published(message));
                    metrics.Published(message.Envelope.Type);
                    consecutiveFailures = 0;
                    streak.Clear();
                    published++;
                }
                catch (OperationCanceledException) when (lease.IsCancellationRequested)
                {
                    // Desligamento ou fim do prazo da reserva: a mensagem e o resto do lote voltam sem contar tentativa
                    for (var j = i; j < leased.Count; j++)
                        outcomes.Add(OutboxOutcome.Released(leased[j], time.GetUtcNow()));
                    break;
                }
                catch (Exception ex) when (IsTransportUnavailable(ex))
                {
                    // Broker fora, circuito aberto ou transporte desligado: não é defeito da mensagem. Ela, as falhas da sequência
                    // (provavelmente o começo da mesma queda) e o resto do lote voltam sem contar tentativa, e o relay pausa
                    metrics.PublishFailed(message.Envelope.Type);
                    paused = true;
                    var retryAt = time.GetUtcNow() + settings.PauseDuration;
                    foreach (var index in streak)
                        outcomes[index] = OutboxOutcome.Released(leased[index], retryAt);
                    for (var j = i; j < leased.Count; j++)
                        outcomes.Add(OutboxOutcome.Released(leased[j], retryAt));
                    LogTransportUnavailable(streak.Count + leased.Count - i, settings.PauseDuration, SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
                    break;
                }
                catch (Exception ex)
                {
                    consecutiveFailures++;
                    var attempts = message.Attempts + 1;
                    var error = SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}");
                    metrics.PublishFailed(message.Envelope.Type);
                    streak.Add(outcomes.Count);
                    failures.Add((outcomes.Count, ex.GetType().Name));
                    outcomes.Add(attempts >= settings.MaxAttempts
                        ? OutboxOutcome.Dead(message, attempts, error)
                        : OutboxOutcome.Failed(message, attempts, time.GetUtcNow() + settings.RetryDelay(attempts), error));
                }
            }

            foreach (var (index, errorType) in failures)
            {
                var outcome = outcomes[index];
                var envelope = leased[index].Envelope;
                if (outcome.Kind == OutboxOutcomeKind.Dead)
                {
                    metrics.Dead(envelope.Type);
                    LogDead(envelope.MessageId, envelope.Type, outcome.Attempts, errorType);
                }
                else if (outcome.Kind == OutboxOutcomeKind.Failed)
                {
                    LogPublishFailed(envelope.MessageId, envelope.Type, outcome.Attempts, settings.RetryDelay(outcome.Attempts), errorType);
                }
            }

            // A conclusão roda mesmo no desligamento: sem ela, as publicadas seriam reenviadas quando a reserva vencer.
            using var completion = new CancellationTokenSource(CompletionTimeout);
            await store.CompleteAsync(outcomes, completion.Token).ConfigureAwait(false);
            return new RelayCycle(leased.Count, published, paused);
        }
    }

    /// <summary>A exceção (ou uma interna) marca transporte indisponível (<see cref="ITransportUnavailableException"/>).</summary>
    private static bool IsTransportUnavailable(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is ITransportUnavailableException)
                return true;
        }

        return false;
    }

    /// <summary>Apaga as publicadas vencidas em lotes, enquanto vierem cheios (até <see cref="MaxCleanupDuration"/>). Exposto para os testes.</summary>
    internal async Task<int> CleanupAsync(OutboxOptions settings, DateTimeOffset now, CancellationToken ct)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
            var started = time.GetTimestamp();
            int removed, total = 0;
            do
            {
                removed = await store.PurgePublishedAsync(now - settings.PublishedRetention, PurgeBatch, ct).ConfigureAwait(false);
                total += removed;
            }
            while (removed >= PurgeBatch && !ct.IsCancellationRequested && time.GetElapsedTime(started) < MaxCleanupDuration);

            if (total > 0)
                LogPurged(total);
            return total;
        }
    }

    private async Task UpdateStatisticsAsync(DateTimeOffset now, CancellationToken ct)
    {
        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            if (scope.ServiceProvider.GetService<IOutboxAdministration>() is not { } administration)
                return;
            var stats = await administration.GetPendingStatisticsAsync(ct).ConfigureAwait(false);
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

    [LoggerMessage(EventId = 4006, Level = LogLevel.Warning,
        Message = "Transporte indisponível ao publicar: {Released} mensagem(ns) devolvida(s) ao Outbox sem contar tentativa; relay pausado por {Pause}: {Error}")]
    private partial void LogTransportUnavailable(int released, TimeSpan pause, string error);

    [LoggerMessage(EventId = 4004, Level = LogLevel.Error, Message = "Falha no ciclo do relay do Outbox")]
    private partial void LogCycleFailed(Exception exception);

    [LoggerMessage(EventId = 4005, Level = LogLevel.Information, Message = "Limpeza do Outbox: {Removed} mensagem(ns) publicada(s) removida(s)")]
    private partial void LogPurged(int removed);
}

/// <summary>Resumo de um ciclo do relay.</summary>
internal readonly record struct RelayCycle(int Leased, int Published, bool Paused);
