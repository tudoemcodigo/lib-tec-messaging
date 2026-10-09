using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using TEC.Messaging.Context;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.RabbitMQ.Internal;

namespace TEC.Messaging.RabbitMQ.Consumers;

/// <summary>O que fazer com a mensagem depois do processamento.</summary>
public enum ConsumeAction
{
    /// <summary>Processada (ou nada a fazer): ack.</summary>
    Complete = 1,

    /// <summary>Falha que consome tentativa: fila de espera com backoff; esgotadas as tentativas, DLQ.</summary>
    Retry = 2,

    /// <summary>Sem conserto por nova tentativa (mensagem inválida, desfecho final): DLQ.</summary>
    Reject = 3,

    /// <summary>Devolve à fila sem consumir tentativa (ex.: dependência fora do ar). Limitado pelo <see cref="QueueDefinition.DeliveryLimit"/>.</summary>
    Requeue = 4,
}

/// <summary>Desfecho do processamento (o ack só acontece depois dele, portanto depois do commit do handler).</summary>
/// <param name="Action">Ação.</param>
/// <param name="Reason">Motivo (falhas): vai para o cabeçalho de erro e os logs.</param>
/// <param name="Attempt">Número da falha, quando o consumidor o controla (ex.: contagem no banco); senão, cabeçalho + 1.</param>
public readonly record struct ConsumeResult(ConsumeAction Action, string? Reason = null, int? Attempt = null)
{
    /// <summary>Processada.</summary>
    public static ConsumeResult Completed => new(ConsumeAction.Complete);

    /// <summary>Nova tentativa com backoff.</summary>
    /// <param name="reason">Motivo.</param>
    /// <param name="attempt">Número da falha, se controlado pelo consumidor.</param>
    /// <returns>O desfecho.</returns>
    public static ConsumeResult Retry(string reason, int? attempt = null) => new(ConsumeAction.Retry, reason, attempt);

    /// <summary>Direto para a DLQ.</summary>
    /// <param name="reason">Motivo.</param>
    /// <returns>O desfecho.</returns>
    public static ConsumeResult Reject(string reason) => new(ConsumeAction.Reject, reason);

    /// <summary>Devolve à fila sem consumir tentativa.</summary>
    /// <param name="reason">Motivo.</param>
    /// <returns>O desfecho.</returns>
    public static ConsumeResult Requeue(string reason) => new(ConsumeAction.Requeue, reason);
}

/// <summary>Mensagem entregue ao consumidor.</summary>
/// <param name="Envelope">Envelope.</param>
/// <param name="PreviousFailures">Falhas anteriores registradas no cabeçalho.</param>
/// <param name="Redelivered">Reentrega do broker (o consumidor anterior caiu sem ack).</param>
public sealed record ReceivedMessage(MessageEnvelope Envelope, int PreviousFailures, bool Redelivered);

/// <summary>Dependências comuns dos consumidores (um único parâmetro no construtor das subclasses).</summary>
public sealed class RabbitMqConsumerDependencies
{
    internal RabbitMqConsumerDependencies(
        RabbitMqConnection connection, RabbitMqPublisher publisher, IOptions<RabbitMqOptions> options, MessagingMetrics metrics, TimeProvider time,
        ILoggerFactory loggers)
    {
        Connection = connection;
        Publisher = publisher;
        Options = options;
        Metrics = metrics;
        Time = time;
        Loggers = loggers;
    }

    internal RabbitMqConnection Connection { get; }

    internal RabbitMqPublisher Publisher { get; }

    internal IOptions<RabbitMqOptions> Options { get; }

    internal MessagingMetrics Metrics { get; }

    internal TimeProvider Time { get; }

    internal ILoggerFactory Loggers { get; }
}

/// <summary>
/// Base dos consumidores: declara a topologia, consome com prefetch e ack manual depois do handler, restaura correlação,
/// causa (<see cref="MessageContext"/>) e o trace W3C do envelope, encaminha falhas para as filas de espera (backoff por
/// tentativa) e para a DLQ, e permite pausar/retomar o consumo (ex.: circuit breaker de uma dependência). Reabre canal e
/// conexão sozinho em caso de queda.
/// </summary>
/// <example>
/// <code>
/// public sealed class ConsumidorDePedidos(RabbitMqConsumerDependencies deps, IServiceScopeFactory scopes) : RabbitMqConsumer(deps)
/// {
///     protected override QueueDefinition Queue { get; } = new("vendas.faturamento", ["pedido-aprovado"]);
///
///     protected override async Task&lt;ConsumeResult&gt; HandleAsync(ReceivedMessage message, CancellationToken ct) { ... }
/// }
/// </code>
/// </example>
public abstract partial class RabbitMqConsumer : BackgroundService
{
    private static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(5);
    private readonly RabbitMqConsumerDependencies _deps;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _signal = new(0);
    private IChannel? _channel;
    private string? _tag;
    private volatile bool _pauseRequested;
    private int _reconnectFailures;

    /// <summary>Cria o consumidor.</summary>
    /// <param name="dependencies">Dependências comuns (do container).</param>
    protected RabbitMqConsumer(RabbitMqConsumerDependencies dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        _deps = dependencies;
        _logger = dependencies.Loggers.CreateLogger(GetType());
    }

    /// <summary>Fila, ligações e parâmetros de retry deste consumidor.</summary>
    protected abstract QueueDefinition Queue { get; }

    /// <summary>Consumo suspenso (pedido de pausa).</summary>
    public bool IsPaused => _pauseRequested;

    internal QueueDefinition Definition => Queue;

    /// <summary>Suspende o consumo: as mensagens ficam na fila (duráveis), sem requeue em laço.</summary>
    public void Pause()
    {
        if (_pauseRequested)
            return;
        _pauseRequested = true;
        _signal.Release();
    }

    /// <summary>Retoma o consumo.</summary>
    public void Resume()
    {
        if (!_pauseRequested)
            return;
        _pauseRequested = false;
        _signal.Release();
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _signal.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>Processa a mensagem. Exceção não tratada = <see cref="ConsumeAction.Retry"/>.</summary>
    /// <param name="message">Mensagem.</param>
    /// <param name="cancellationToken">Cancelamento (desligamento do processo).</param>
    /// <returns>Desfecho.</returns>
    protected abstract Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = Queue;
        queue.Validate();
        if (!_deps.Options.Value.Enabled)
        {
            LogDisabled(queue.Name);
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (_channel is not { IsOpen: true })
                    await OpenChannelAsync(queue, stoppingToken).ConfigureAwait(false);

                var channel = _channel!;
                if (_pauseRequested && _tag is not null)
                {
                    await channel.BasicCancelAsync(_tag, cancellationToken: stoppingToken).ConfigureAwait(false);
                    _tag = null;
                    LogPaused(queue.Name);
                }
                else if (!_pauseRequested && _tag is null)
                {
                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.ReceivedAsync += (_, delivery) => ReceiveAsync(channel, queue, delivery, stoppingToken);
                    _tag = await channel.BasicConsumeAsync(queue.Name, autoAck: false, consumer, stoppingToken).ConfigureAwait(false);
                    LogConsuming(queue.Name, queue.Prefetch);
                }

                // Canal aberto e consumo no estado pedido: a próxima queda recomeça a espera do início
                _reconnectFailures = 0;

                await _signal.WaitAsync(CheckInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var delay = ReconnectDelay(++_reconnectFailures);
                if (ex is RabbitMqCircuitOpenException)
                    LogCircuitOpenWaiting(queue.Name, delay);
                else
                    LogChannelFailed(ex, queue.Name);
                await CloseChannelAsync().ConfigureAwait(false);
                try
                {
                    await Task.Delay(delay, _deps.Time, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        await CloseChannelAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Espera antes de reabrir o consumo depois da falha número <paramref name="failures"/>: exponencial a partir de
    /// <see cref="RabbitMqOptions.ReconnectDelay"/> até <see cref="RabbitMqOptions.MaxReconnectDelay"/>, com variação de ±20% para
    /// que várias instâncias não reconectem juntas.
    /// </summary>
    internal TimeSpan ReconnectDelay(int failures)
    {
        var settings = _deps.Options.Value;
        double baseMs = settings.ReconnectDelay.TotalMilliseconds * Math.Pow(2, Math.Min(failures - 1, 30));
        double capped = Math.Min(baseMs, settings.MaxReconnectDelay.TotalMilliseconds);
        double jitter = 0.8 + (System.Security.Cryptography.RandomNumberGenerator.GetInt32(0, 401) / 1000.0);
        return TimeSpan.FromMilliseconds(Math.Min(capped * jitter, settings.MaxReconnectDelay.TotalMilliseconds));
    }

    private async Task OpenChannelAsync(QueueDefinition queue, CancellationToken ct)
    {
        await CloseChannelAsync().ConfigureAwait(false);
        var connection = await _deps.Connection.GetAsync(ct).ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);
        try
        {
            await Topology.DeclareAsync(channel, _deps.Options.Value.Exchange, queue, ct).ConfigureAwait(false);
            await channel.BasicQosAsync(0, queue.Prefetch, global: false, ct).ConfigureAwait(false);
        }
        catch
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        _channel = channel;
        _tag = null;
    }

    private async Task CloseChannelAsync()
    {
        var channel = _channel;
        _channel = null;
        _tag = null;
        if (channel is null)
            return;
        try
        {
            await channel.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Já fechado (queda de conexão).
        }
    }

    private async Task ReceiveAsync(IChannel channel, QueueDefinition queue, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        var started = _deps.Time.GetTimestamp();
        var properties = delivery.BasicProperties;
        var failures = AmqpEnvelope.HeaderInt(properties.Headers, AmqpEnvelope.AttemptHeader);
        var envelope = AmqpEnvelope.Read(delivery.Body, out var readError);
        if (envelope is null)
        {
            LogInvalidMessage(queue.Name, Limit(properties.MessageId), readError);
            await FinishAsync(channel, queue, delivery, ConsumeResult.Reject(readError ?? "Mensagem ilegível."), failures, started, ct).ConfigureAwait(false);
            return;
        }

        var traceParent = AmqpEnvelope.HeaderText(properties.Headers, AmqpEnvelope.TraceParentHeader);
        using var activity = MessagingDiagnostics.StartProcess(envelope, traceParent is { Length: <= MessageEnvelope.MaxTraceParentLength } ? traceParent : null, "rabbitmq", queue.Name);

        // Mensagens gravadas no Outbox durante o processamento herdam a correlação; a causa é a mensagem recebida
        using var context = MessageContext.Begin(envelope.CorrelationId ?? activity?.TraceId.ToHexString(), envelope.MessageId.ToString());

        ConsumeResult result;
        try
        {
            result = await HandleAsync(new ReceivedMessage(envelope, failures, delivery.Redelivered), ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            result = ConsumeResult.Requeue("Processo encerrando.");
        }
        catch (Exception ex)
        {
            LogHandlerFailed(ex, queue.Name, envelope.MessageId, envelope.Type);
            result = ConsumeResult.Retry($"{ex.GetType().Name}: {ex.Message}");
        }

        if (result.Action != ConsumeAction.Complete)
            activity?.SetStatus(ActivityStatusCode.Error, result.Reason);
        await FinishAsync(channel, queue, delivery, result, failures, started, ct).ConfigureAwait(false);
    }

    private async Task FinishAsync(
        IChannel channel, QueueDefinition queue, BasicDeliverEventArgs delivery, ConsumeResult result, int failures, long started, CancellationToken ct)
    {
        var action = result.Action;
        var attempt = result.Attempt ?? failures + 1;
        if (action == ConsumeAction.Retry && attempt >= queue.MaxAttempts)
            action = ConsumeAction.Reject;
        var messageId = Limit(delivery.BasicProperties.MessageId);

        try
        {
            switch (action)
            {
                case ConsumeAction.Requeue:
                    await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, CancellationToken.None).ConfigureAwait(false);
                    Record(queue, ConsumeOutcomeTag.Requeued, started);
                    return;

                case ConsumeAction.Retry when !queue.Transient:
                    var delay = queue.RetryDelayAfter(attempt);
                    await RepublishAsync(delivery, queue.RetryQueue(delay), attempt, result.Reason, ct).ConfigureAwait(false);
                    LogRetry(queue.Name, messageId, attempt, delay, result.Reason);
                    break;

                case ConsumeAction.Reject when !queue.Transient:
                    await RepublishAsync(delivery, queue.DeadLetterQueue, attempt, result.Reason, ct).ConfigureAwait(false);
                    LogDeadLettered(queue.Name, messageId, result.Reason);
                    break;

                case ConsumeAction.Retry or ConsumeAction.Reject:
                    LogDiscarded(queue.Name, messageId, result.Reason);
                    break;
            }

            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, CancellationToken.None).ConfigureAwait(false);
            Record(queue, action switch
            {
                ConsumeAction.Complete => ConsumeOutcomeTag.Completed,
                ConsumeAction.Retry => ConsumeOutcomeTag.Retried,
                _ => ConsumeOutcomeTag.Rejected,
            }, started);
        }
        catch (Exception ex)
        {
            // Sem ack: a mensagem volta à fila (reentrega), nada se perde
            LogFinishFailed(ex, queue.Name, messageId);
            try
            {
                if (channel.IsOpen)
                    await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Canal caiu: o broker devolve as mensagens não confirmadas.
            }
        }
    }

    private void Record(QueueDefinition queue, ConsumeOutcomeTag outcome, long started) =>
        _deps.Metrics.Consumed(queue.Name, outcome, _deps.Time.GetElapsedTime(started));

    private Task RepublishAsync(BasicDeliverEventArgs delivery, string targetQueue, int attempt, string? reason, CancellationToken ct)
    {
        var properties = AmqpEnvelope.Copy(delivery.BasicProperties);
        properties.Headers![AmqpEnvelope.AttemptHeader] = attempt;
        properties.Headers[AmqpEnvelope.ErrorHeader] = reason is { Length: > 1000 } ? reason[..1000] : reason ?? "";
        return _deps.Publisher.PublishRawAsync("", targetQueue, true, properties, delivery.Body.ToArray(), ct);
    }

    private static string? Limit(string? value) => value is { Length: > 64 } ? value[..64] : value;

    [LoggerMessage(EventId = 4201, Level = LogLevel.Information, Message = "Consumindo a fila {Queue} (prefetch {Prefetch})")]
    private partial void LogConsuming(string queue, ushort prefetch);

    [LoggerMessage(EventId = 4202, Level = LogLevel.Warning, Message = "Consumo da fila {Queue} pausado")]
    private partial void LogPaused(string queue);

    [LoggerMessage(EventId = 4203, Level = LogLevel.Error, Message = "Falha no canal da fila {Queue}; reabrindo")]
    private partial void LogChannelFailed(Exception exception, string queue);

    [LoggerMessage(EventId = 4204, Level = LogLevel.Error, Message = "Falha ao processar {MessageId} ({Type}) da fila {Queue}")]
    private partial void LogHandlerFailed(Exception exception, string queue, Guid messageId, string type);

    [LoggerMessage(EventId = 4205, Level = LogLevel.Warning, Message = "Mensagem {MessageId} da fila {Queue} em espera: tentativa {Attempt}, nova em {Delay} ({Reason})")]
    private partial void LogRetry(string queue, string? messageId, int attempt, TimeSpan delay, string? reason);

    [LoggerMessage(EventId = 4206, Level = LogLevel.Warning, Message = "Mensagem {MessageId} da fila {Queue} enviada para a DLQ: {Reason}")]
    private partial void LogDeadLettered(string queue, string? messageId, string? reason);

    [LoggerMessage(EventId = 4207, Level = LogLevel.Warning, Message = "Mensagem {MessageId} inválida na fila {Queue}: {Error}")]
    private partial void LogInvalidMessage(string queue, string? messageId, string? error);

    [LoggerMessage(EventId = 4208, Level = LogLevel.Error, Message = "Falha ao confirmar a mensagem {MessageId} da fila {Queue}; devolvida à fila")]
    private partial void LogFinishFailed(Exception exception, string queue, string? messageId);

    [LoggerMessage(EventId = 4209, Level = LogLevel.Warning, Message = "Mensagem {MessageId} da fila temporária {Queue} descartada: {Reason}")]
    private partial void LogDiscarded(string queue, string? messageId, string? reason);

    [LoggerMessage(EventId = 4210, Level = LogLevel.Warning, Message = "Transporte RabbitMQ desligado: consumidor da fila {Queue} não iniciado")]
    private partial void LogDisabled(string queue);

    [LoggerMessage(EventId = 4211, Level = LogLevel.Debug, Message = "Fila {Queue}: circuito da conexão aberto; nova tentativa em {Delay}")]
    private partial void LogCircuitOpenWaiting(string queue, TimeSpan delay);
}
