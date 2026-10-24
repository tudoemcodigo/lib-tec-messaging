using System.Diagnostics;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TEC.Messaging.AzureServiceBus.Internal;
using TEC.Messaging.Context;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;

namespace TEC.Messaging.AzureServiceBus.Consumers;

/// <summary>Dependências comuns dos consumidores do Azure Service Bus.</summary>
public sealed class AzureServiceBusConsumerDependencies
{
    internal AzureServiceBusConsumerDependencies(AzureServiceBusConnection connection, ILoggerFactory loggers)
    {
        Connection = connection;
        Loggers = loggers;
    }

    internal AzureServiceBusConnection Connection { get; }
    internal ILoggerFactory Loggers { get; }
}

/// <summary>Definição do consumidor do Azure Service Bus (Fila ou Tópico + Assinatura).</summary>
public sealed record ServiceBusConsumerDefinition(
    string TopicOrQueue,
    string? Subscription = null,
    int MaxConcurrentCalls = 10,
    TimeSpan? AutoRenewLockDuration = null);

/// <summary>
/// Base para consumidores de mensagens no Azure Service Bus via ServiceBusProcessor.
/// </summary>
public abstract class AzureServiceBusConsumer : BackgroundService
{
    private readonly AzureServiceBusConsumerDependencies _deps;
    private readonly ILogger _logger;
    private ServiceBusProcessor? _processor;

    /// <summary>Cria o consumidor do Azure Service Bus.</summary>
    protected AzureServiceBusConsumer(AzureServiceBusConsumerDependencies dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        _deps = dependencies;
        _logger = dependencies.Loggers.CreateLogger(GetType());
    }

    /// <summary>Configuração do destino (tópico + assinatura ou fila) e concorrência.</summary>
    protected abstract ServiceBusConsumerDefinition Definition { get; }

    /// <summary>Executa a lógica de consumo da mensagem.</summary>
    protected abstract Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var def = Definition;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var client = await _deps.Connection.GetClientAsync(stoppingToken).ConfigureAwait(false);

                var options = new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages = false,
                    MaxConcurrentCalls = def.MaxConcurrentCalls,
                };

                if (def.AutoRenewLockDuration is { } lockDuration)
                    options.MaxAutoLockRenewalDuration = lockDuration;

                _processor = def.Subscription is not null
                    ? client.CreateProcessor(def.TopicOrQueue, def.Subscription, options)
                    : client.CreateProcessor(def.TopicOrQueue, options);

                _processor.ProcessMessageAsync += ProcessMessageAsync;
                _processor.ProcessErrorAsync += ProcessErrorAsync;

                await _processor.StartProcessingAsync(stoppingToken).ConfigureAwait(false);
                break;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                var safeMsg = SensitiveDataSanitizer.Sanitize(ex.Message);
                _logger.LogWarning("Falha ao iniciar processador do Azure Service Bus na entidade {Entity}: {Error}. Nova tentativa em 5 s...", def.TopicOrQueue, safeMsg);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Encerramento ordenado
        }
        finally
        {
            if (_processor is not null)
            {
                await _processor.StopProcessingAsync(CancellationToken.None).ConfigureAwait(false);
                await _processor.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        if (args.CancellationToken.IsCancellationRequested)
            return;

        var raw = args.Message;
        var body = Encoding.UTF8.GetString(raw.Body);

        var messageId = Guid.TryParse(raw.MessageId, out var parsedId) && parsedId != Guid.Empty
            ? parsedId
            : DeterministicGuid.Create(raw.MessageId);

        var type = raw.Subject ?? (raw.ApplicationProperties.TryGetValue("type", out var t) ? Convert.ToString(t) : "unknown") ?? "unknown";
        var version = raw.ApplicationProperties.TryGetValue("version", out var v) && int.TryParse(Convert.ToString(v), out var ver) ? ver : 1;
        var traceParent = raw.ApplicationProperties.TryGetValue("traceparent", out var tp) ? Convert.ToString(tp) : null;
        var causationId = raw.ApplicationProperties.TryGetValue("causation-id", out var c) ? Convert.ToString(c) : null;

        var occurredAt = raw.ApplicationProperties.TryGetValue("occurred-at", out var oa) && DateTimeOffset.TryParse(Convert.ToString(oa), out var parsedOa)
            ? parsedOa
            : (raw.ApplicationProperties.TryGetValue("occurredAt", out var oa2) && DateTimeOffset.TryParse(Convert.ToString(oa2), out var parsedOa2)
                ? parsedOa2
                : raw.EnqueuedTime);

        var envelope = new MessageEnvelope(
            messageId,
            type,
            version,
            occurredAt,
            raw.CorrelationId,
            causationId,
            traceParent,
            body);

        using var activity = MessagingDiagnostics.StartProcess(envelope, traceParent, "azure-servicebus", Definition.TopicOrQueue);
        using var context = MessageContext.Begin(envelope.CorrelationId ?? activity?.TraceId.ToHexString(), envelope.MessageId.ToString());

        ConsumeResult result;
        try
        {
            result = await HandleAsync(new ReceivedMessage(envelope, raw.DeliveryCount - 1, raw.DeliveryCount > 1), args.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            result = ConsumeResult.Retry(SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
        }

        if (args.CancellationToken.IsCancellationRequested)
            return;

        if (result.Action is ConsumeAction.Reject or ConsumeAction.Retry)
        {
            var sanitizedReason = SensitiveDataSanitizer.Sanitize(result.Reason ?? string.Empty);
            if (!string.IsNullOrEmpty(sanitizedReason))
                activity?.SetStatus(ActivityStatusCode.Error, sanitizedReason);
        }

        switch (result.Action)
        {
            case ConsumeAction.Complete:
                await args.CompleteMessageAsync(raw, args.CancellationToken).ConfigureAwait(false);
                break;
            case ConsumeAction.Reject:
                var reason = SensitiveDataSanitizer.Sanitize(result.Reason ?? "Rejeitado pelo consumidor.");
                await args.DeadLetterMessageAsync(raw, "ProcessingRejected", reason, args.CancellationToken).ConfigureAwait(false);
                break;
            case ConsumeAction.Requeue:
            case ConsumeAction.Retry:
            default:
                if (!args.CancellationToken.IsCancellationRequested)
                    await args.AbandonMessageAsync(raw, cancellationToken: args.CancellationToken).ConfigureAwait(false);
                break;
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        var safeMsg = SensitiveDataSanitizer.Sanitize(args.Exception?.Message ?? string.Empty);
        _logger.LogError("Erro no processamento do Azure Service Bus na entidade {EntityPath}: {Error}", args.EntityPath, safeMsg);
        return Task.CompletedTask;
    }
}
