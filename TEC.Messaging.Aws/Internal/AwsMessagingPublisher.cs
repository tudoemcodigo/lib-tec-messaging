using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Amazon.Runtime;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
using Microsoft.Extensions.Options;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Aws.Internal;

/// <summary>
/// Publicador de mensagens para AWS SNS (tópicos) ou AWS SQS (filas).
/// </summary>
/// <remarks>
/// Só falha transitória vira <see cref="AwsMessagingUnavailableException"/> (o Outbox não conta tentativa): cliente impossível
/// de criar, HTTP 5xx, limitação de taxa (throttling, HTTP 429), erro de rede e confirmação fora de
/// <see cref="AwsMessagingOptions.PublishTimeout"/>. O resto (fila ou tópico inexistente, permissão negada, mensagem inválida
/// ou grande demais) sai como veio: conta tentativa e, esgotadas as tentativas, a mensagem vira <c>Dead</c> no Outbox.
/// Credencial recusada também descarta os clientes, para a próxima tentativa reler o segredo.
/// </remarks>
internal sealed class AwsMessagingPublisher(AwsClientFactory clientFactory, IOptions<AwsMessagingOptions> options) : IMessagePublisher
{
    private static readonly HashSet<string> ThrottlingErrorCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "Throttling", "ThrottlingException", "ThrottledException", "RequestThrottled", "RequestThrottledException",
        "TooManyRequestsException", "RequestLimitExceeded", "SlowDown", "KMS.ThrottlingException", "KMSThrottlingException",
        "AWS.SimpleQueueService.RequestThrottled",
    };

    public async Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var settings = options.Value;
        if (!settings.Enabled)
            throw new AwsMessagingUnavailableException("Transporte AWS desabilitado.");

        // DefaultDestination é validado na subida; o tipo da mensagem fica só como último recurso (nunca é ARN nem URL)
        var destination = !string.IsNullOrWhiteSpace(settings.DefaultDestination)
            ? settings.DefaultDestination
            : envelope.Type;

        using var activity = MessagingDiagnostics.StartPublish(envelope, "aws", destination);
        var sent = activity?.Id is { Length: <= MessageEnvelope.MaxTraceParentLength } id
            ? envelope with { TraceParent = id }
            : envelope;

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(settings.PublishTimeout);

        var toSns = destination.StartsWith("arn:aws:sns:", StringComparison.OrdinalIgnoreCase);
        object client;
        try
        {
            client = toSns
                ? await clientFactory.GetSnsAsync(timeout.Token).ConfigureAwait(false)
                : await clientFactory.GetSqsAsync(timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            // Cofre fora, segredo inválido, credencial do ambiente ausente: problema do transporte, não da mensagem
            throw new AwsMessagingUnavailableException($"Sem cliente AWS: {Describe(ex)}", ex);
        }

        try
        {
            if (client is Amazon.SimpleNotificationService.IAmazonSimpleNotificationService sns)
                await sns.PublishAsync(SnsRequest(destination, sent), timeout.Token).ConfigureAwait(false);
            else
                await ((Amazon.SQS.IAmazonSQS)client).SendMessageAsync(SqsRequest(destination, sent), timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            throw new AwsMessagingUnavailableException($"A AWS não confirmou a publicação em {settings.PublishTimeout}.", ex);
        }
        catch (Exception ex) when (AwsClientFactory.IsCredentialFailure(ex))
        {
            // Credencial recusada (chave rotacionada, token vencido): a próxima tentativa relê o segredo. Conta tentativa
            await clientFactory.InvalidateAsync(client).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex) when (IsTransient(ex))
        {
            throw new AwsMessagingUnavailableException($"Falha transitória na AWS: {Describe(ex)}", ex);
        }
    }

    /// <summary>Falha transitória: 5xx, throttling (429 ou código de limitação), rede, tempo esgotado, cliente trocado na rotação.</summary>
    internal static bool IsTransient(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            switch (current)
            {
                case AmazonServiceException service:
                    return (int)service.StatusCode >= 500
                        || service.StatusCode == HttpStatusCode.TooManyRequests
                        || service.Retryable?.Throttling == true
                        || (service.ErrorCode is { } code && ThrottlingErrorCodes.Contains(code));
                case HttpRequestException:
                case SocketException:
                case IOException:
                case TimeoutException:
                case WebException:
                case ObjectDisposedException:
                    return true;
                case AmazonClientException:
                    // Erro do lado do cliente sem resposta do serviço (conexão, DNS, credencial do ambiente indisponível)
                    return true;
            }
        }

        return false;
    }

    private static PublishRequest SnsRequest(string topicArn, MessageEnvelope sent)
    {
        var request = new PublishRequest
        {
            TopicArn = topicArn,
            Message = sent.Payload,
            MessageAttributes = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>(StringComparer.Ordinal)
            {
                ["messageId"] = new() { DataType = "String", StringValue = sent.MessageId.ToString() },
                ["type"] = new() { DataType = "String", StringValue = sent.Type },
                ["version"] = new() { DataType = "Number", StringValue = sent.Version.ToString(CultureInfo.InvariantCulture) },
                ["occurredAt"] = new() { DataType = "String", StringValue = Instant(sent.OccurredAt) },
            },
        };

        if (sent.CorrelationId is not null)
            request.MessageAttributes["correlationId"] = new() { DataType = "String", StringValue = sent.CorrelationId };
        if (sent.CausationId is not null)
            request.MessageAttributes["causationId"] = new() { DataType = "String", StringValue = sent.CausationId };
        if (sent.TraceParent is not null)
            request.MessageAttributes["traceparent"] = new() { DataType = "String", StringValue = sent.TraceParent };
        return request;
    }

    private static SendMessageRequest SqsRequest(string queueUrl, MessageEnvelope sent)
    {
        var request = new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = sent.Payload,
            MessageAttributes = new Dictionary<string, Amazon.SQS.Model.MessageAttributeValue>(StringComparer.Ordinal)
            {
                ["messageId"] = new() { DataType = "String", StringValue = sent.MessageId.ToString() },
                ["type"] = new() { DataType = "String", StringValue = sent.Type },
                ["version"] = new() { DataType = "Number", StringValue = sent.Version.ToString(CultureInfo.InvariantCulture) },
                ["occurredAt"] = new() { DataType = "String", StringValue = Instant(sent.OccurredAt) },
            },
        };

        if (sent.CorrelationId is not null)
            request.MessageAttributes["correlationId"] = new() { DataType = "String", StringValue = sent.CorrelationId };
        if (sent.CausationId is not null)
            request.MessageAttributes["causationId"] = new() { DataType = "String", StringValue = sent.CausationId };
        if (sent.TraceParent is not null)
            request.MessageAttributes["traceparent"] = new() { DataType = "String", StringValue = sent.TraceParent };
        return request;
    }

    private static string Instant(DateTimeOffset value) => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    private static string Describe(Exception ex) => ex is AmazonServiceException service
        ? $"{(int)service.StatusCode} {service.ErrorCode}: {SensitiveDataSanitizer.Sanitize(service.Message)}"
        : $"{ex.GetType().Name}: {SensitiveDataSanitizer.Sanitize(ex.Message)}";
}
