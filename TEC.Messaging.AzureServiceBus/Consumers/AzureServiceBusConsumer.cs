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
        var client = await _deps.Connection.GetClientAsync(stoppingToken).ConfigureAwait(false);
        var def = Definition;

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
            await _processor.StopProcessingAsync(CancellationToken.None).ConfigureAwait(false);
            await _processor.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        var raw = args.Message;
        var body = Encoding.UTF8.GetString(raw.Body);

        _ = Guid.TryParse(raw.MessageId, out var messageId);
        var type = raw.Subject ?? (raw.ApplicationProperties.TryGetValue("type", out var t) ? Convert.ToString(t) : "unknown") ?? "unknown";
        var version = raw.ApplicationProperties.TryGetValue("version", out var v) && int.TryParse(Convert.ToString(v), out var ver) ? ver : 1;
        var traceParent = raw.ApplicationProperties.TryGetValue("traceparent", out var tp) ? Convert.ToString(tp) : null;
        var causationId = raw.ApplicationProperties.TryGetValue("causation-id", out var c) ? Convert.ToString(c) : null;

        var envelope = new MessageEnvelope(
            messageId == Guid.Empty ? Guid.NewGuid() : messageId,
            type,
            version,
            raw.EnqueuedTime,
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
                await args.AbandonMessageAsync(raw, cancellationToken: args.CancellationToken).ConfigureAwait(false);
                break;
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Erro no processamento do Azure Service Bus na entidade {EntityPath}", args.EntityPath);
        return Task.CompletedTask;
    }
}
