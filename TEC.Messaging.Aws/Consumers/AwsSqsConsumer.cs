using System.Collections.Concurrent;
using System.Diagnostics;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TEC.Messaging.Aws.Internal;
using TEC.Messaging.Context;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;

namespace TEC.Messaging.Aws.Consumers;

/// <summary>Dependências comuns dos consumidores AWS SQS.</summary>
public sealed class AwsSqsConsumerDependencies
{
    internal AwsSqsConsumerDependencies(AwsClientFactory clientFactory, ILoggerFactory loggers)
    {
        ClientFactory = clientFactory;
        Loggers = loggers;
    }

    internal AwsClientFactory ClientFactory { get; }
    internal ILoggerFactory Loggers { get; }
}

/// <summary>Definição do consumidor AWS SQS.</summary>
public sealed record SqsConsumerDefinition(
    string QueueUrl,
    int MaxNumberOfMessages = 10,
    int WaitTimeSeconds = 20,
    int VisibilityTimeoutSeconds = 30,
    ushort ConcurrencyLimit = 5);

/// <summary>
/// Base para consumidores de mensagens no AWS SQS com long polling e concorrência.
/// </summary>
public abstract class AwsSqsConsumer : BackgroundService
{
    private readonly AwsSqsConsumerDependencies _deps;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<Task, bool> _runningTasks = new();
    private SemaphoreSlim? _semaphore;

    /// <summary>Cria o consumidor AWS SQS.</summary>
    protected AwsSqsConsumer(AwsSqsConsumerDependencies dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        _deps = dependencies;
        _logger = dependencies.Loggers.CreateLogger(GetType());
    }

    /// <summary>Definição da fila SQS.</summary>
    protected abstract SqsConsumerDefinition Definition { get; }

    /// <summary>Executa a lógica de consumo da mensagem.</summary>
    protected abstract Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sqs = await _deps.ClientFactory.GetSqsAsync(stoppingToken).ConfigureAwait(false);
        var def = Definition;
        _semaphore = new SemaphoreSlim(def.ConcurrencyLimit, def.ConcurrencyLimit);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
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
                if (response.Messages is null || response.Messages.Count == 0)
                    continue;

                foreach (var msg in response.Messages)
                {
                    await _semaphore.WaitAsync(stoppingToken).ConfigureAwait(false);
                    Task task = null!;
                    task = Task.Run(async () =>
                    {
                        try
                        {
                            await ProcessSqsMessageAsync(sqs, def.QueueUrl, msg, stoppingToken).ConfigureAwait(false);
                        }
                        finally
                        {
                            _runningTasks.TryRemove(task, out _);
                            _semaphore.Release();
                        }
                    }, CancellationToken.None);
                    _runningTasks.TryAdd(task, true);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var safeMsg = SensitiveDataSanitizer.Sanitize(ex.Message);
                _logger.LogError("Erro no loop de consumo do AWS SQS na fila {QueueUrl}: {Error}", def.QueueUrl, safeMsg);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken).ConfigureAwait(false);
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
                await Task.WhenAll(_runningTasks.Keys).WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Encerramento ordenado
            }
        }
    }

    private async Task ProcessSqsMessageAsync(IAmazonSQS sqs, string queueUrl, Message raw, CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
            return;

        var rawId = raw.MessageAttributes.TryGetValue("messageId", out var idAttr) ? idAttr.StringValue : raw.MessageId;
        var messageId = Guid.TryParse(rawId, out var parsedId) && parsedId != Guid.Empty
            ? parsedId
            : DeterministicGuid.Create(rawId);

        var type = raw.MessageAttributes.TryGetValue("type", out var tAttr) ? tAttr.StringValue : "unknown";
        var version = raw.MessageAttributes.TryGetValue("version", out var vAttr) && int.TryParse(vAttr.StringValue, out var v) ? v : 1;
        var traceParent = raw.MessageAttributes.TryGetValue("traceparent", out var tpAttr) ? tpAttr.StringValue : null;
        var causationId = raw.MessageAttributes.TryGetValue("causationId", out var cAttr) ? cAttr.StringValue : null;
        var correlationId = raw.MessageAttributes.TryGetValue("correlationId", out var crAttr) ? crAttr.StringValue : null;

        var failures = raw.Attributes.TryGetValue("ApproximateReceiveCount", out var rc) && int.TryParse(rc, out var count) ? Math.Max(0, count - 1) : 0;

        var occurredAt = raw.MessageAttributes.TryGetValue("occurredAt", out var oaAttr) && DateTimeOffset.TryParse(oaAttr.StringValue, out var parsedOa)
            ? parsedOa
            : (raw.Attributes.TryGetValue("ApproximateFirstReceiveTimestamp", out var ts) && long.TryParse(ts, out var ms)
                ? DateTimeOffset.FromUnixTimeMilliseconds(ms)
                : DateTimeOffset.UtcNow);

        var envelope = new MessageEnvelope(
            messageId,
            type,
            version,
            occurredAt,
            correlationId,
            causationId,
            traceParent,
            raw.Body);

        using var activity = MessagingDiagnostics.StartProcess(envelope, traceParent, "aws-sqs", queueUrl);
        using var context = MessageContext.Begin(envelope.CorrelationId ?? activity?.TraceId.ToHexString(), envelope.MessageId.ToString());

        ConsumeResult result;
        try
        {
            result = await HandleAsync(new ReceivedMessage(envelope, failures, failures > 0), ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            result = ConsumeResult.Retry(SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
        }

        if (result.Action is ConsumeAction.Reject or ConsumeAction.Retry)
        {
            var sanitizedReason = SensitiveDataSanitizer.Sanitize(result.Reason ?? string.Empty);
            if (!string.IsNullOrEmpty(sanitizedReason))
                activity?.SetStatus(ActivityStatusCode.Error, sanitizedReason);
        }

        switch (result.Action)
        {
            case ConsumeAction.Complete:
                await sqs.DeleteMessageAsync(queueUrl, raw.ReceiptHandle, CancellationToken.None).ConfigureAwait(false);
                break;
            case ConsumeAction.Reject:
                await sqs.ChangeMessageVisibilityAsync(queueUrl, raw.ReceiptHandle, 0, CancellationToken.None).ConfigureAwait(false);
                break;
            case ConsumeAction.Requeue:
                await sqs.ChangeMessageVisibilityAsync(queueUrl, raw.ReceiptHandle, 0, CancellationToken.None).ConfigureAwait(false);
                break;
            case ConsumeAction.Retry:
            default:
                var delaySeconds = Math.Clamp((int)Math.Pow(2, Math.Min(failures + 1, 8)), 1, 300);
                await sqs.ChangeMessageVisibilityAsync(queueUrl, raw.ReceiptHandle, delaySeconds, CancellationToken.None).ConfigureAwait(false);
                break;
        }
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _semaphore?.Dispose();
        base.Dispose();
        GC.SuppressFinalize(this);
    }
}
