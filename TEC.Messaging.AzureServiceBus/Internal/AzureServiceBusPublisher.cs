using System.Collections.Concurrent;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Transport;

namespace TEC.Messaging.AzureServiceBus.Internal;

/// <summary>
/// Publicador de mensagens para tópicos/filas do Azure Service Bus.
/// </summary>
internal sealed class AzureServiceBusPublisher(
    AzureServiceBusConnection connection, IOptions<AzureServiceBusOptions> options)
    : IMessagePublisher, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);
    private int _disposed;

    public async Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        var settings = options.Value;
        if (!settings.Enabled)
            throw new AzureServiceBusUnavailableException("Transporte Azure Service Bus desabilitado.");

        var destination = !string.IsNullOrWhiteSpace(settings.DefaultTopic) ? settings.DefaultTopic : envelope.Type;
        using var activity = MessagingDiagnostics.StartPublish(envelope, "azure-servicebus", destination);

        var sentEnvelope = activity?.Id is { Length: <= MessageEnvelope.MaxTraceParentLength } id
            ? envelope with { TraceParent = id }
            : envelope;

        var message = new ServiceBusMessage(Encoding.UTF8.GetBytes(sentEnvelope.Payload))
        {
            MessageId = sentEnvelope.MessageId.ToString(),
            Subject = sentEnvelope.Type,
            CorrelationId = sentEnvelope.CorrelationId,
            ContentType = "application/json",
        };

        message.ApplicationProperties["type"] = sentEnvelope.Type;
        message.ApplicationProperties["version"] = sentEnvelope.Version;
        message.ApplicationProperties["occurred-at"] = sentEnvelope.OccurredAt.ToString("O");
        if (sentEnvelope.CausationId is not null)
            message.ApplicationProperties["causation-id"] = sentEnvelope.CausationId;
        if (sentEnvelope.TraceParent is not null)
            message.ApplicationProperties["traceparent"] = sentEnvelope.TraceParent;

        try
        {
            var sender = await GetSenderAsync(destination, cancellationToken).ConfigureAwait(false);
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(settings.PublishTimeout);

            await sender.SendMessageAsync(message, timeout.Token).ConfigureAwait(false);
        }
        catch (ServiceBusException ex)
        {
            throw new AzureServiceBusUnavailableException($"Falha no Azure Service Bus ({ex.Reason}): {ex.Message}", ex);
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            throw new AzureServiceBusUnavailableException("Timeout aguardando confirmação do Azure Service Bus.", ex);
        }
        catch (Exception ex) when (ex is not (OperationCanceledException or AzureServiceBusUnavailableException))
        {
            throw new AzureServiceBusUnavailableException($"Falha ao publicar no Azure Service Bus: {ex.Message}", ex);
        }
    }

    private async Task<ServiceBusSender> GetSenderAsync(string destination, CancellationToken ct)
    {
        if (_senders.TryGetValue(destination, out var existing))
            return existing;

        var client = await connection.GetClientAsync(ct).ConfigureAwait(false);
        return _senders.GetOrAdd(destination, name => client.CreateSender(name));
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var sender in _senders.Values)
            await sender.DisposeAsync().ConfigureAwait(false);
        _senders.Clear();
    }
}
