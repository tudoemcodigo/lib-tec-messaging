using System.Globalization;
using System.Net.Sockets;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;
using TEC.Messaging.Transport;

namespace TEC.Messaging.AzureServiceBus.Internal;

/// <summary>
/// Publicador de mensagens para tópicos/filas do Azure Service Bus.
/// </summary>
/// <remarks>
/// Só falha transitória vira <see cref="AzureServiceBusUnavailableException"/> (o Outbox não conta tentativa): cliente
/// impossível de criar, <see cref="ServiceBusException.IsTransient"/> (ou <c>ServiceBusy</c>, <c>ServiceTimeout</c>,
/// <c>ServiceCommunicationProblem</c>), erro de rede e confirmação fora de <see cref="AzureServiceBusOptions.PublishTimeout"/>.
/// O resto (mensagem grande demais, entidade inexistente ou desabilitada, permissão negada) sai como veio: conta tentativa e,
/// esgotadas as tentativas, a mensagem vira <c>Dead</c> no Outbox. Credencial recusada também descarta o cliente, para a
/// próxima tentativa reler o segredo.
/// </remarks>
internal sealed class AzureServiceBusPublisher(AzureServiceBusConnection connection, IOptions<AzureServiceBusOptions> options) : IMessagePublisher
{
    public async Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

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
        message.ApplicationProperties["occurred-at"] = sentEnvelope.OccurredAt.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
        if (sentEnvelope.CausationId is not null)
            message.ApplicationProperties["causation-id"] = sentEnvelope.CausationId;
        if (sentEnvelope.TraceParent is not null)
            message.ApplicationProperties["traceparent"] = sentEnvelope.TraceParent;

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(settings.PublishTimeout);

        ServiceBusSender sender;
        try
        {
            sender = await connection.GetSenderAsync(destination, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            // Cofre fora, connection string inválida, conexão descartada: problema do transporte, não da mensagem
            throw new AzureServiceBusUnavailableException($"Sem cliente do Azure Service Bus: {Describe(ex)}", ex);
        }

        try
        {
            await sender.SendMessageAsync(message, timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            throw new AzureServiceBusUnavailableException($"O Azure Service Bus não confirmou a publicação em {settings.PublishTimeout}.", ex);
        }
        catch (UnauthorizedAccessException)
        {
            // Credencial recusada (chave rotacionada, SAS vencida, sem permissão de envio): a próxima tentativa relê o segredo.
            // Conta tentativa: permissão negada de fato não se resolve sozinha
            await connection.InvalidateAsync(sender).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex) when (IsTransient(ex))
        {
            throw new AzureServiceBusUnavailableException($"Falha transitória no Azure Service Bus: {Describe(ex)}", ex);
        }
    }

    /// <summary>Falha transitória (rede, serviço ocupado, tempo esgotado, cliente trocado durante a rotação).</summary>
    internal static bool IsTransient(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            switch (current)
            {
                case ServiceBusException sb when sb.IsTransient
                    || sb.Reason is ServiceBusFailureReason.ServiceBusy or ServiceBusFailureReason.ServiceTimeout or ServiceBusFailureReason.ServiceCommunicationProblem:
                case TimeoutException:
                case SocketException:
                case IOException:
                case ObjectDisposedException:
                    return true;
                case ServiceBusException:
                case UnauthorizedAccessException:
                    // Classificação explícita do Service Bus: definitiva
                    return false;
            }
        }

        return false;
    }

    private static string Describe(Exception ex) => ex is ServiceBusException sb
        ? $"{sb.Reason}: {SensitiveDataSanitizer.Sanitize(sb.Message)}"
        : $"{ex.GetType().Name}: {SensitiveDataSanitizer.Sanitize(ex.Message)}";
}
