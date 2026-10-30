using System.Diagnostics;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using TEC.Messaging.Context;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;
using TEC.Messaging.RabbitMQ.Internal;

namespace TEC.Messaging.RabbitMQ.Consumers;

/// <summary>Dependências comuns dos consumidores (um único parâmetro no construtor das subclasses).</summary>
public sealed class RabbitMqConsumerDependencies
{
    internal RabbitMqConsumerDependencies(
        RabbitMqConnection connection, RabbitMqPublisher publisher, IOptions<RabbitMqOptions> options, MessagingMetrics metrics, TimeProvider time,
        ILoggerFactory loggers, IOptions<MessagingOptions>? messaging = null)
    {
        Connection = connection;
        Publisher = publisher;
        Options = options;
        Metrics = metrics;
        Time = time;
        Loggers = loggers;
        MaxPayloadLength = messaging?.Value.MaxPayloadLength ?? MessagingOptions.MaxPayloadLengthLimit;
    }

    internal RabbitMqConnection Connection { get; }

    internal RabbitMqPublisher Publisher { get; }

    internal IOptions<RabbitMqOptions> Options { get; }

    /// <summary>Payload máximo aceito no consumo (<see cref="MessagingOptions.MaxPayloadLength"/>).</summary>
    internal int MaxPayloadLength { get; }

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
/// A ligação usa a routing key, que é o tipo da mensagem sem <see cref="RabbitMqOptions.RoutingKeyPrefixToRemove"/>. Com
/// <c>o.RoutingKeyPrefixToRemove = "vendas."</c> no <c>UseRabbitMq</c>, <c>vendas.pedido-aprovado</c> chega por
/// <c>pedido-aprovado</c>; sem o prefixo configurado, a ligação teria de ser <c>vendas.pedido-aprovado</c>.
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
    private static readonly TimeSpan DrainCheckInterval = TimeSpan.FromMilliseconds(50);
    private readonly RabbitMqConsumerDependencies _deps;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _signal = new(0);
    private SemaphoreSlim? _concurrencySemaphore;

    // Cancelamento dos handlers: separado do stoppingToken para o desligamento esperar os que estão em andamento (StopAsync)
    private readonly CancellationTokenSource _handlerStop = new();
    private IChannel? _channel;
    private string? _tag;
    private volatile bool _pauseRequested;
    private volatile bool _draining;
    private int _inFlight;
    private int _reconnectFailures;
    private int _disposed;

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

    /// <summary>
    /// Desligamento com drenagem: deixa de processar novas entregas (ficam sem ack e voltam à fila quando o canal fecha, sem
    /// NACK), espera os handlers em andamento até <see cref="RabbitMqOptions.DrainTimeout"/> (ou o fim do prazo do host) e só
    /// então cancela os que restarem e fecha o canal.
    /// </summary>
    /// <param name="cancellationToken">Prazo do desligamento do host.</param>
    /// <returns>Tarefa concluída com o consumo encerrado.</returns>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _draining = true;
        var started = _deps.Time.GetTimestamp();
        var limit = _deps.Options.Value.DrainTimeout;
        try
        {
            while (Volatile.Read(ref _inFlight) > 0 && _deps.Time.GetElapsedTime(started) < limit)
                await Task.Delay(DrainCheckInterval, _deps.Time, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Prazo do host esgotado: cancela os handlers que restam
        }

        if (Volatile.Read(ref _inFlight) > 0)
            LogDrainExpired(Queue.Name, Volatile.Read(ref _inFlight));
        await _handlerStop.CancelAsync().ConfigureAwait(false);
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _handlerStop.Cancel();
        _handlerStop.Dispose();
        _signal.Dispose();
        _concurrencySemaphore?.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>Processa a mensagem. Exceção não tratada = <see cref="ConsumeAction.Retry"/>.</summary>
    /// <param name="message">Mensagem.</param>
    /// <param name="cancellationToken">
    /// Cancelamento: no desligamento, depois de <see cref="RabbitMqOptions.DrainTimeout"/>. Cancelado, a mensagem volta à fila
    /// sem ack nem NACK.
    /// </param>
    /// <returns>Desfecho.</returns>
    protected abstract Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queue = Queue;
        queue.Validate();
        if (_concurrencySemaphore is null && queue.ConcurrencyLimit > 1)
            _concurrencySemaphore = new SemaphoreSlim(queue.ConcurrencyLimit, queue.ConcurrencyLimit);
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
                    consumer.ReceivedAsync += (_, delivery) => ReceiveAsync(channel, queue, delivery, _handlerStop.Token);
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
        // Desligando: sem ack nem NACK, a mensagem volta à fila quando o canal fechar
        if (_draining)
            return;

        Interlocked.Increment(ref _inFlight);
        if (_concurrencySemaphore is not { } semaphore)
        {
            try
            {
                await ProcessAsync(channel, queue, delivery, ct).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }

            return;
        }

        try
        {
            await semaphore.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
        {
            // Desligamento enquanto esperava vaga: sem ack nem NACK, a mensagem volta à fila com o canal. A contagem precisa
            // voltar, senão a drenagem esperaria até o fim do DrainTimeout por uma mensagem que nunca entrou em processamento
            Interlocked.Decrement(ref _inFlight);
            return;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await ProcessAsync(channel, queue, delivery, ct).ConfigureAwait(false);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
                try
                {
                    semaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                    // Consumidor já descartado: não há quem espere a vaga.
                }
            }
        }, CancellationToken.None);
    }

    private async Task ProcessAsync(IChannel channel, QueueDefinition queue, BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        var started = _deps.Time.GetTimestamp();
        var properties = delivery.BasicProperties;
        var failures = AmqpEnvelope.HeaderInt(properties.Headers, AmqpEnvelope.AttemptHeader);
        var envelope = AmqpEnvelope.Read(delivery.Body, out var readError, _deps.MaxPayloadLength);
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
            // Desligamento depois da drenagem: sem ack nem NACK (não conta como entrega devolvida); volta à fila com o canal
            activity?.SetStatus(ActivityStatusCode.Error, "Processo encerrando.");
            return;
        }
        catch (Exception ex)
        {
            var safeError = SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}");
            LogHandlerFailed(safeError, queue.Name, envelope.MessageId, envelope.Type);
            result = ConsumeResult.Retry(safeError);
        }

        if (result.Action != ConsumeAction.Complete)
            activity?.SetStatus(ActivityStatusCode.Error, SensitiveDataSanitizer.Sanitize(result.Reason));
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
        var safeReason = SensitiveDataSanitizer.Sanitize(result.Reason);

        try
        {
            switch (action)
            {
                case ConsumeAction.Requeue when !queue.Transient:
                    // Volta depois da primeira espera, sem consumir tentativa (o cabeçalho de falhas segue igual). Um NACK com
                    // requeue reentregaria na hora: com a dependência ainda fora, laço de reentregas até o x-delivery-limit
                    // mandar a mensagem para a DLQ
                    var wait = queue.RetryDelays[0];
                    await RepublishAsync(delivery, queue.RetryQueue(wait), failures, safeReason, ct).ConfigureAwait(false);
                    LogRequeued(queue.Name, messageId, wait, safeReason);
                    break;

                case ConsumeAction.Requeue:
                    // Fila temporária não tem fila de espera: volta na hora
                    await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true, CancellationToken.None).ConfigureAwait(false);
                    Record(queue, ConsumeOutcomeTag.Requeued, started);
                    return;

                case ConsumeAction.Retry when !queue.Transient:
                    var delay = queue.RetryDelayAfter(attempt);
                    await RepublishAsync(delivery, queue.RetryQueue(delay), attempt, safeReason, ct).ConfigureAwait(false);
                    LogRetry(queue.Name, messageId, attempt, delay, safeReason);
                    break;

                case ConsumeAction.Reject when !queue.Transient:
                    await RepublishAsync(delivery, queue.DeadLetterQueue, attempt, safeReason, ct).ConfigureAwait(false);
                    LogDeadLettered(queue.Name, messageId, safeReason);
                    break;

                case ConsumeAction.Retry or ConsumeAction.Reject:
                    LogDiscarded(queue.Name, messageId, safeReason);
                    break;
            }

            await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false, CancellationToken.None).ConfigureAwait(false);
            Record(queue, action switch
            {
                ConsumeAction.Complete => ConsumeOutcomeTag.Completed,
                ConsumeAction.Retry => ConsumeOutcomeTag.Retried,
                ConsumeAction.Requeue => ConsumeOutcomeTag.Requeued,
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
        var sanitized = SensitiveDataSanitizer.Sanitize(reason);
        var properties = AmqpEnvelope.Copy(delivery.BasicProperties);
        properties.Headers![AmqpEnvelope.AttemptHeader] = attempt;
        properties.Headers[AmqpEnvelope.ErrorHeader] = sanitized is { Length: > 1000 } ? sanitized[..1000] : sanitized ?? "";
        return _deps.Publisher.PublishRawAsync("", targetQueue, true, properties, delivery.Body.ToArray(), ct);
    }

    private static string? Limit(string? value) => value is { Length: > 64 } ? value[..64] : value;

    [LoggerMessage(EventId = 4201, Level = LogLevel.Information, Message = "Consumindo a fila {Queue} (prefetch {Prefetch})")]
    private partial void LogConsuming(string queue, ushort prefetch);

    [LoggerMessage(EventId = 4202, Level = LogLevel.Warning, Message = "Consumo da fila {Queue} pausado")]
    private partial void LogPaused(string queue);

    [LoggerMessage(EventId = 4203, Level = LogLevel.Error, Message = "Falha no canal da fila {Queue}; reabrindo")]
    private partial void LogChannelFailed(Exception exception, string queue);

    [LoggerMessage(EventId = 4204, Level = LogLevel.Error, Message = "Falha ao processar {MessageId} ({Type}) da fila {Queue}: {Error}")]
    private partial void LogHandlerFailed(string error, string queue, Guid messageId, string type);

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

    [LoggerMessage(EventId = 4213, Level = LogLevel.Warning, Message = "Mensagem {MessageId} da fila {Queue} devolvida sem consumir tentativa; volta em {Delay} ({Reason})")]
    private partial void LogRequeued(string queue, string? messageId, TimeSpan delay, string? reason);

    [LoggerMessage(EventId = 4214, Level = LogLevel.Warning,
        Message = "Desligamento do consumidor da fila {Queue}: {InFlight} mensagem(ns) ainda em processamento no fim da drenagem; handlers cancelados")]
    private partial void LogDrainExpired(string queue, int inFlight);
}
