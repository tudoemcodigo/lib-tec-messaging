using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace TEC.Messaging.Diagnostics;

/// <summary>
/// Nomes da telemetria do TEC.Messaging, assinados automaticamente pelo TEC.Observability (prefixo <c>TEC.</c>) ou à mão
/// (<c>AddSource</c>/<c>AddMeter</c> do OpenTelemetry).
/// </summary>
public static class MessagingDiagnostics
{
    /// <summary>Nome do <see cref="System.Diagnostics.ActivitySource"/> (spans de publicação e consumo).</summary>
    public const string ActivitySourceName = "TEC.Messaging";

    /// <summary>Nome do <see cref="Meter"/>.</summary>
    public const string MeterName = "TEC.Messaging";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName, typeof(MessagingDiagnostics).Assembly.GetName().Version?.ToString());

    /// <summary>Span de produtor (publicação) ligado ao trace da mensagem.</summary>
    internal static Activity? StartPublish(MessageEnvelope envelope, string system, string destination)
    {
        var activity = ActivitySource.StartActivity($"{envelope.Type} publish", ActivityKind.Producer, envelope.TraceParent);
        Tag(activity, envelope, system, "publish", destination);
        return activity;
    }

    /// <summary>Span de consumidor, continuando o trace do produtor.</summary>
    internal static Activity? StartProcess(MessageEnvelope envelope, string? traceParent, string system, string destination)
    {
        var activity = ActivitySource.StartActivity($"{envelope.Type} process", ActivityKind.Consumer, traceParent ?? envelope.TraceParent);
        Tag(activity, envelope, system, "process", destination);
        return activity;
    }

    private static void Tag(Activity? activity, MessageEnvelope envelope, string system, string operation, string destination)
    {
        if (activity is null)
            return;
        activity.SetTag("messaging.system", system);
        activity.SetTag("messaging.operation.type", operation);
        activity.SetTag("messaging.destination.name", destination);
        activity.SetTag("messaging.message.id", envelope.MessageId.ToString());
        activity.SetTag("messaging.message.conversation_id", envelope.CorrelationId);
        activity.SetTag("tec.messaging.type", envelope.Type);
    }
}

/// <summary>Resultado do consumo, nas métricas.</summary>
internal enum ConsumeOutcomeTag
{
    Completed,
    Retried,
    Rejected,
    Requeued,
}

/// <summary>
/// Métricas do TEC.Messaging, criadas pelo <see cref="IMeterFactory"/> do container (sem estado estático): contadores do
/// Outbox e do consumo e gauges observáveis atualizados pelo relay (pendentes) e pelo monitor da DLQ.
/// </summary>
internal sealed class MessagingMetrics : IDisposable
{
    private readonly Meter _meter;
    private readonly Counter<long> _enqueued;
    private readonly Counter<long> _published;
    private readonly Counter<long> _publishFailures;
    private readonly Counter<long> _dead;
    private readonly Counter<long> _consumed;
    private readonly Histogram<double> _consumeDuration;
    private readonly ConcurrentDictionary<string, long> _deadLetters = new(StringComparer.Ordinal);
    private OutboxSnapshot? _outbox;

    public MessagingMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(MessagingDiagnostics.MeterName);
        _enqueued = _meter.CreateCounter<long>("tec.messaging.outbox.enqueued", "{message}", "Mensagens gravadas no Outbox.");
        _published = _meter.CreateCounter<long>("tec.messaging.outbox.published", "{message}", "Mensagens publicadas com confirmação do broker.");
        _publishFailures = _meter.CreateCounter<long>("tec.messaging.outbox.publish_failures", "{message}", "Falhas de publicação (cada tentativa).");
        _dead = _meter.CreateCounter<long>("tec.messaging.outbox.dead", "{message}", "Mensagens que esgotaram as tentativas.");
        _consumed = _meter.CreateCounter<long>("tec.messaging.consumer.messages", "{message}", "Mensagens consumidas, por desfecho.");
        _consumeDuration = _meter.CreateHistogram<double>("tec.messaging.consumer.duration", "s", "Duração do processamento de uma mensagem.");
        _meter.CreateObservableGauge("tec.messaging.outbox.pending", () => Observe(s => s.Pending), "{message}", "Mensagens pendentes no Outbox.");
        _meter.CreateObservableGauge("tec.messaging.outbox.pending_with_errors", () => Observe(s => s.PendingWithErrors), "{message}", "Pendentes que já falharam.");
        _meter.CreateObservableGauge("tec.messaging.outbox.dead_messages", () => Observe(s => s.Dead), "{message}", "Mensagens mortas aguardando o administrador.");
        _meter.CreateObservableGauge("tec.messaging.outbox.oldest_pending_age", ObserveOldestAge, "s", "Idade da mensagem pendente mais antiga.");
        _meter.CreateObservableGauge("tec.messaging.dlq.messages", ObserveDeadLetters, "{message}", "Mensagens nas filas de mensagens mortas (DLQ).");
    }

    public void Enqueued(string type) => _enqueued.Add(1, new KeyValuePair<string, object?>("tec.messaging.type", type));

    public void Published(string type) => _published.Add(1, new KeyValuePair<string, object?>("tec.messaging.type", type));

    public void PublishFailed(string type) => _publishFailures.Add(1, new KeyValuePair<string, object?>("tec.messaging.type", type));

    public void Dead(string type) => _dead.Add(1, new KeyValuePair<string, object?>("tec.messaging.type", type));

    public void Consumed(string queue, ConsumeOutcomeTag outcome, TimeSpan duration)
    {
        var tags = new TagList { { "messaging.destination.name", queue }, { "tec.messaging.outcome", Outcome(outcome) } };
        _consumed.Add(1, tags);
        _consumeDuration.Record(duration.TotalSeconds, tags);
    }

    /// <summary>Atualiza o retrato do Outbox (relay, a cada <c>StatisticsInterval</c>).</summary>
    public void UpdateOutbox(int pending, int pendingWithErrors, int dead, TimeSpan? oldestPendingAge) =>
        Volatile.Write(ref _outbox, new OutboxSnapshot(pending, pendingWithErrors, dead, oldestPendingAge));

    /// <summary>Atualiza a quantidade de mensagens na DLQ de uma fila.</summary>
    public void UpdateDeadLetters(string queue, long count) => _deadLetters[queue] = count;

    public void Dispose() => _meter.Dispose();

    private static string Outcome(ConsumeOutcomeTag outcome) => outcome switch
    {
        ConsumeOutcomeTag.Completed => "completed",
        ConsumeOutcomeTag.Retried => "retried",
        ConsumeOutcomeTag.Rejected => "rejected",
        _ => "requeued",
    };

    private IEnumerable<Measurement<int>> Observe(Func<OutboxSnapshot, int> value)
    {
        var snapshot = Volatile.Read(ref _outbox);
        return snapshot is null ? [] : [new Measurement<int>(value(snapshot))];
    }

    private IEnumerable<Measurement<double>> ObserveOldestAge()
    {
        var snapshot = Volatile.Read(ref _outbox);
        return snapshot is null ? [] : [new Measurement<double>(snapshot.OldestPendingAge?.TotalSeconds ?? 0)];
    }

    private IEnumerable<Measurement<long>> ObserveDeadLetters() =>
        [.. _deadLetters.Select(p => new Measurement<long>(p.Value, new KeyValuePair<string, object?>("messaging.destination.name", p.Key)))];

    private sealed record OutboxSnapshot(int Pending, int PendingWithErrors, int Dead, TimeSpan? OldestPendingAge);
}
