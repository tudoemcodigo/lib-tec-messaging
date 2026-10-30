using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TEC.Messaging.Aws.Internal;
using TEC.Messaging.Context;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;

namespace TEC.Messaging.Aws.Consumers;

/// <summary>Dependências comuns dos consumidores AWS SQS.</summary>
public sealed class AwsSqsConsumerDependencies
{
    internal AwsSqsConsumerDependencies(
        AwsClientFactory clientFactory, ILoggerFactory loggers, MessagingMetrics metrics, TimeProvider time, int maxPayloadLength, bool requireTls)
    {
        ClientFactory = clientFactory;
        Loggers = loggers;
        Metrics = metrics;
        Time = time;
        MaxPayloadLength = maxPayloadLength;
        RequireTls = requireTls;
    }

    internal AwsClientFactory ClientFactory { get; }

    internal ILoggerFactory Loggers { get; }

    internal MessagingMetrics Metrics { get; }

    internal TimeProvider Time { get; }

    /// <summary>Payload máximo aceito no consumo (<see cref="MessagingOptions.MaxPayloadLength"/>).</summary>
    internal int MaxPayloadLength { get; }

    /// <summary>URL de fila <c>http://</c> recusada (<see cref="AwsMessagingOptions.RequireTls"/>).</summary>
    internal bool RequireTls { get; }
}

/// <summary>Definição do consumidor AWS SQS.</summary>
/// <param name="QueueUrl">URL da fila (<c>https://sqs...</c>).</param>
/// <param name="MaxNumberOfMessages">Mensagens por recebimento (1 a 10).</param>
/// <param name="WaitTimeSeconds">Long polling (0 a 20 s).</param>
/// <param name="VisibilityTimeoutSeconds">
/// Visibilidade pedida no recebimento (1 s a 12 h). Enquanto o handler roda, a visibilidade é renovada para este mesmo valor a
/// cada metade dele: um handler mais lento que a visibilidade não faz a mensagem reaparecer para outro consumidor.
/// </param>
/// <param name="ConcurrencyLimit">Handlers em paralelo (1 ou mais).</param>
public sealed record SqsConsumerDefinition(
    string QueueUrl,
    int MaxNumberOfMessages = 10,
    int WaitTimeSeconds = 20,
    int VisibilityTimeoutSeconds = 30,
    ushort ConcurrencyLimit = 5)
{
    /// <summary>Maior visibilidade aceita pelo SQS (12 h).</summary>
    internal const int MaxVisibilityTimeoutSeconds = 43_200;

    /// <summary>Nome da fila (último segmento da URL), usado em métricas e spans sem expor a conta.</summary>
    internal string QueueName
    {
        get
        {
            var url = QueueUrl.TrimEnd('/');
            var slash = url.LastIndexOf('/');
            return slash >= 0 ? url[(slash + 1)..] : url;
        }
    }

    /// <summary>Recusa definição incoerente na subida do consumidor.</summary>
    internal void Validate(bool requireTls)
    {
        if (QueueUrl?.StartsWith("arn:", StringComparison.OrdinalIgnoreCase) == true)
            throw new InvalidOperationException("SqsConsumerDefinition: QueueUrl inválida (o consumidor lê de uma fila SQS, não de um ARN).");
        if (AwsMessagingOptionsValidator.CheckDestination(QueueUrl, requireTls) is { } error)
            throw new InvalidOperationException($"SqsConsumerDefinition: QueueUrl {error}");
        if (MaxNumberOfMessages is < 1 or > 10)
            throw new InvalidOperationException("SqsConsumerDefinition: MaxNumberOfMessages deve estar entre 1 e 10.");
        if (WaitTimeSeconds is < 0 or > 20)
            throw new InvalidOperationException("SqsConsumerDefinition: WaitTimeSeconds deve estar entre 0 e 20.");
        if (VisibilityTimeoutSeconds is < 1 or > MaxVisibilityTimeoutSeconds)
            throw new InvalidOperationException("SqsConsumerDefinition: VisibilityTimeoutSeconds deve estar entre 1 e 43200 (12 h).");
        if (ConcurrencyLimit < 1)
            throw new InvalidOperationException("SqsConsumerDefinition: ConcurrencyLimit deve ser 1 ou mais.");
    }
}

/// <summary>
/// Base para consumidores de mensagens no AWS SQS com long polling e concorrência.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>O envelope é validado (<see cref="MessageEnvelope.Validate"/> com <see cref="MessagingOptions.MaxPayloadLength"/>):
/// mensagem inválida ou acima do limite não chega ao handler e é tratada como <see cref="ConsumeAction.Reject"/>.</description></item>
/// <item><description>A visibilidade é renovada enquanto o handler roda (<see cref="SqsConsumerDefinition.VisibilityTimeoutSeconds"/>).</description></item>
/// <item><description>Falha ao obter o cliente (cofre fora) ou ao receber não derruba o serviço: o laço espera e tenta de novo;
/// credencial recusada descarta os clientes e o segredo é relido.</description></item>
/// </list>
/// </remarks>
public abstract partial class AwsSqsConsumer : BackgroundService
{
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan SettlementTimeout = TimeSpan.FromSeconds(30);

    private readonly AwsSqsConsumerDependencies _deps;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<Task, byte> _runningTasks = new();
    private SemaphoreSlim? _semaphore;
    private int _disposed;

    /// <summary>Cria o consumidor AWS SQS.</summary>
    protected AwsSqsConsumer(AwsSqsConsumerDependencies dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        _deps = dependencies;
        _logger = dependencies.Loggers.CreateLogger(GetType());
    }

    /// <summary>Definição da fila SQS.</summary>
    protected abstract SqsConsumerDefinition Definition { get; }

    /// <summary>Executa a lógica de consumo da mensagem. Exceção não tratada = <see cref="ConsumeAction.Retry"/>.</summary>
    protected abstract Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken);

    /// <summary>Espera (visibilidade) antes da tentativa seguinte à falha número <paramref name="attempt"/>: 2 s, 4 s... até 300 s.</summary>
    internal static int RetryDelaySeconds(int attempt) => (int)Math.Clamp(Math.Pow(2, Math.Clamp(attempt, 1, 9)), 1, 300);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var def = Definition;
        def.Validate(_deps.RequireTls);
        var semaphore = _semaphore = new SemaphoreSlim(def.ConcurrencyLimit, def.ConcurrencyLimit);
        var failures = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            IAmazonSQS? sqs = null;
            try
            {
                // Dentro do try: cofre fora na subida não derruba o host, o laço espera e tenta de novo
                sqs = await _deps.ClientFactory.GetSqsAsync(stoppingToken).ConfigureAwait(false);
                var receiveRequest = new ReceiveMessageRequest
                {
                    QueueUrl = def.QueueUrl,
                    MaxNumberOfMessages = def.MaxNumberOfMessages,
                    WaitTimeSeconds = def.WaitTimeSeconds,
                    VisibilityTimeout = def.VisibilityTimeoutSeconds,
                    MessageSystemAttributeNames = ["All"],
                    MessageAttributeNames = ["All"],
                };

                var response = await sqs.ReceiveMessageAsync(receiveRequest, stoppingToken).ConfigureAwait(false);
                failures = 0;
                if (response.Messages is not { Count: > 0 } messages)
                    continue;

                foreach (var msg in messages)
                {
                    await semaphore.WaitAsync(stoppingToken).ConfigureAwait(false);
                    Start(sqs, def, msg, semaphore, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                if (sqs is not null && AwsClientFactory.IsCredentialFailure(ex))
                    await _deps.ClientFactory.InvalidateAsync(sqs).ConfigureAwait(false);

                LogLoopFailed(def.QueueName, SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
                var delay = TimeSpan.FromSeconds(Math.Min(60, 2 * Math.Pow(2, Math.Min(failures++, 5))));
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

        if (!_runningTasks.IsEmpty)
        {
            try
            {
                await Task.WhenAll(_runningTasks.Keys).WaitAsync(DrainTimeout, CancellationToken.None).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                LogDrainExpired(def.QueueName, _runningTasks.Count);
            }
        }
    }

    /// <summary>
    /// Processa em segundo plano. O rastreio usa uma tarefa própria, registrada antes do início: nunca há chave nula nem
    /// entrada órfã, e a vaga do semáforo volta sempre, antes de qualquer outra coisa no <c>finally</c>.
    /// </summary>
    private void Start(IAmazonSQS sqs, SqsConsumerDefinition def, Message msg, SemaphoreSlim semaphore, CancellationToken ct)
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _runningTasks.TryAdd(done.Task, 0);
        _ = Task.Run(async () =>
        {
            try
            {
                await ProcessSqsMessageAsync(sqs, def, msg, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogProcessFailed(def.QueueName, Limit(msg.MessageId), SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
            }
            finally
            {
                try
                {
                    semaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                    // Consumidor já descartado: não há quem espere a vaga.
                }

                _runningTasks.TryRemove(done.Task, out _);
                done.TrySetResult();
            }
        }, CancellationToken.None);
    }

    private async Task ProcessSqsMessageAsync(IAmazonSQS sqs, SqsConsumerDefinition def, Message raw, CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
            return;

        var started = _deps.Time.GetTimestamp();
        var failures = ReadInt(raw.Attributes, "ApproximateReceiveCount") is var count and > 0 ? count - 1 : 0;
        var envelope = ReadEnvelope(raw, _deps.MaxPayloadLength, out var invalidReason);
        if (envelope is null)
        {
            LogInvalidMessage(def.QueueName, Limit(raw.MessageId), invalidReason);
            await SettleAsync(sqs, def, raw, ConsumeResult.Reject(invalidReason ?? "Mensagem inválida."), failures, started).ConfigureAwait(false);
            return;
        }

        using var activity = MessagingDiagnostics.StartProcess(envelope, envelope.TraceParent, "aws-sqs", def.QueueName);
        using var context = MessageContext.Begin(envelope.CorrelationId ?? activity?.TraceId.ToHexString(), envelope.MessageId.ToString());

        ConsumeResult result;
        using (var renewal = CancellationTokenSource.CreateLinkedTokenSource(ct))
        {
            var renewing = RenewVisibilityAsync(sqs, def, raw, renewal.Token);
            try
            {
                result = await HandleAsync(new ReceivedMessage(envelope, failures, failures > 0), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Desligamento: a mensagem reaparece quando a visibilidade vencer
                activity?.SetStatus(ActivityStatusCode.Error, "Processo encerrando.");
                return;
            }
            catch (Exception ex)
            {
                result = ConsumeResult.Retry(SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
            }
            finally
            {
                await renewal.CancelAsync().ConfigureAwait(false);
                await renewing.ConfigureAwait(false);
            }
        }

        if (result.Action != ConsumeAction.Complete)
            activity?.SetStatus(ActivityStatusCode.Error, SensitiveDataSanitizer.Sanitize(result.Reason));

        await SettleAsync(sqs, def, raw, result, failures, started).ConfigureAwait(false);
    }

    private async Task SettleAsync(IAmazonSQS sqs, SqsConsumerDefinition def, Message raw, ConsumeResult result, int failures, long started)
    {
        using var timeout = new CancellationTokenSource(SettlementTimeout);
        var messageId = Limit(raw.MessageId);
        var reason = SensitiveDataSanitizer.Sanitize(result.Reason);
        try
        {
            switch (result.Action)
            {
                case ConsumeAction.Complete:
                    await sqs.DeleteMessageAsync(def.QueueUrl, raw.ReceiptHandle, timeout.Token).ConfigureAwait(false);
                    break;
                case ConsumeAction.Reject:
                    // Volta visível na hora: a redrive policy da fila (maxReceiveCount) a leva para a DLQ
                    await sqs.ChangeMessageVisibilityAsync(def.QueueUrl, raw.ReceiptHandle, 0, timeout.Token).ConfigureAwait(false);
                    LogRejected(def.QueueName, messageId, reason);
                    break;
                case ConsumeAction.Requeue:
                    await sqs.ChangeMessageVisibilityAsync(def.QueueUrl, raw.ReceiptHandle, 0, timeout.Token).ConfigureAwait(false);
                    break;
                default:
                    var delaySeconds = RetryDelaySeconds(result.Attempt ?? failures + 1);
                    await sqs.ChangeMessageVisibilityAsync(def.QueueUrl, raw.ReceiptHandle, delaySeconds, timeout.Token).ConfigureAwait(false);
                    LogRetry(def.QueueName, messageId, failures + 1, delaySeconds, reason);
                    break;
            }

            _deps.Metrics.Consumed(def.QueueName, result.Action switch
            {
                ConsumeAction.Complete => ConsumeOutcomeTag.Completed,
                ConsumeAction.Reject => ConsumeOutcomeTag.Rejected,
                ConsumeAction.Requeue => ConsumeOutcomeTag.Requeued,
                _ => ConsumeOutcomeTag.Retried,
            }, _deps.Time.GetElapsedTime(started));
        }
        catch (Exception ex)
        {
            // Sem confirmação, a mensagem reaparece quando a visibilidade vencer: nada se perde
            if (AwsClientFactory.IsCredentialFailure(ex))
                await _deps.ClientFactory.InvalidateAsync(sqs).ConfigureAwait(false);
            LogSettleFailed(def.QueueName, messageId, SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
        }
    }

    /// <summary>Renova a visibilidade a cada metade do prazo enquanto o handler roda. Falha encerra a renovação (com log).</summary>
    private async Task RenewVisibilityAsync(IAmazonSQS sqs, SqsConsumerDefinition def, Message raw, CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(1, def.VisibilityTimeoutSeconds / 2.0));
        var elapsed = TimeSpan.Zero;
        try
        {
            while (true)
            {
                await Task.Delay(interval, _deps.Time, ct).ConfigureAwait(false);
                elapsed += interval;
                if (elapsed.TotalSeconds + def.VisibilityTimeoutSeconds > SqsConsumerDefinition.MaxVisibilityTimeoutSeconds)
                {
                    // O SQS não estende a visibilidade além de 12 h do recebimento
                    LogRenewalLimit(def.QueueName, Limit(raw.MessageId));
                    return;
                }

                await sqs.ChangeMessageVisibilityAsync(def.QueueUrl, raw.ReceiptHandle, def.VisibilityTimeoutSeconds, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Handler terminou (ou desligamento)
        }
        catch (Exception ex)
        {
            LogRenewalFailed(def.QueueName, Limit(raw.MessageId), SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
        }
    }

    /// <summary>
    /// Monta e valida o envelope; <c>null</c> com o motivo quando faltam identificador ou tipo, os limites são excedidos ou o
    /// payload passa de <paramref name="maxPayloadLength"/>.
    /// </summary>
    internal static MessageEnvelope? ReadEnvelope(Message raw, int maxPayloadLength, out string? error)
    {
        var attributes = raw.MessageAttributes;
        var rawId = Text(attributes, "messageId") ?? raw.MessageId;
        if (string.IsNullOrWhiteSpace(rawId))
        {
            error = "Mensagem sem messageId.";
            return null;
        }

        var body = raw.Body ?? "";
        if (body.Length > maxPayloadLength)
        {
            error = $"Payload maior que {maxPayloadLength} caracteres.";
            return null;
        }

        var messageId = Guid.TryParse(rawId, out var parsedId) ? parsedId : DeterministicGuid.Create(rawId);
        var version = int.TryParse(Text(attributes, "version"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 1;
        var occurredAt = DateTimeOffset.TryParse(Text(attributes, "occurredAt"), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsedOa)
            ? parsedOa
            : (long.TryParse(raw.Attributes?.GetValueOrDefault("ApproximateFirstReceiveTimestamp"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var ms)
                ? DateTimeOffset.FromUnixTimeMilliseconds(ms)
                : DateTimeOffset.UtcNow);

        var envelope = new MessageEnvelope(
            messageId, Text(attributes, "type") ?? "", version, occurredAt, Text(attributes, "correlationId"), Text(attributes, "causationId"),
            Text(attributes, "traceparent"), body);

        var valid = envelope.Validate(maxPayloadLength);
        if (valid.IsFailure)
        {
            error = valid.Error!.Message;
            return null;
        }

        error = null;
        return envelope;
    }

    private static string? Text(Dictionary<string, MessageAttributeValue>? attributes, string name) =>
        attributes is not null && attributes.TryGetValue(name, out var value) ? value.StringValue : null;

    private static int ReadInt(Dictionary<string, string>? attributes, string name) =>
        attributes is not null && attributes.TryGetValue(name, out var value) && int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n)
            ? n
            : 0;

    private static string? Limit(string? value) => value is { Length: > 64 } ? value[..64] : value;

    /// <inheritdoc />
    public override void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _semaphore?.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }

    [LoggerMessage(EventId = 4501, Level = LogLevel.Error, Message = "Erro no laço de consumo do AWS SQS na fila {Queue}: {Error}")]
    private partial void LogLoopFailed(string queue, string error);

    [LoggerMessage(EventId = 4502, Level = LogLevel.Error, Message = "Falha ao processar a mensagem {MessageId} da fila {Queue}: {Error}")]
    private partial void LogProcessFailed(string queue, string? messageId, string error);

    [LoggerMessage(EventId = 4503, Level = LogLevel.Warning, Message = "Mensagem {MessageId} inválida na fila {Queue}: {Error}")]
    private partial void LogInvalidMessage(string queue, string? messageId, string? error);

    [LoggerMessage(EventId = 4504, Level = LogLevel.Warning, Message = "Mensagem {MessageId} da fila {Queue} rejeitada (DLQ pela redrive policy): {Reason}")]
    private partial void LogRejected(string queue, string? messageId, string reason);

    [LoggerMessage(EventId = 4505, Level = LogLevel.Warning, Message = "Mensagem {MessageId} da fila {Queue} em espera: tentativa {Attempt}, nova em {DelaySeconds} s ({Reason})")]
    private partial void LogRetry(string queue, string? messageId, int attempt, int delaySeconds, string reason);

    [LoggerMessage(EventId = 4506, Level = LogLevel.Error, Message = "Falha ao confirmar a mensagem {MessageId} da fila {Queue}; ela reaparece quando a visibilidade vencer: {Error}")]
    private partial void LogSettleFailed(string queue, string? messageId, string error);

    [LoggerMessage(EventId = 4507, Level = LogLevel.Warning, Message = "Falha ao renovar a visibilidade da mensagem {MessageId} da fila {Queue}; ela pode ser entregue de novo: {Error}")]
    private partial void LogRenewalFailed(string queue, string? messageId, string error);

    [LoggerMessage(EventId = 4508, Level = LogLevel.Warning, Message = "Mensagem {MessageId} da fila {Queue} chegou ao limite de 12 h de visibilidade do SQS; renovação encerrada")]
    private partial void LogRenewalLimit(string queue, string? messageId);

    [LoggerMessage(EventId = 4509, Level = LogLevel.Warning,
        Message = "Desligamento do consumidor da fila {Queue}: {Running} mensagem(ns) ainda em processamento no fim da espera")]
    private partial void LogDrainExpired(string queue, int running);
}
