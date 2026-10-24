using System.Globalization;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
using Microsoft.Extensions.Options;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Aws.Internal;

/// <summary>
/// Publicador de mensagens para AWS SNS (tópicos) ou AWS SQS (filas).
/// </summary>
internal sealed class AwsMessagingPublisher(
    AwsClientFactory clientFactory, IOptions<AwsMessagingOptions> options)
    : IMessagePublisher
{
    public async Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var settings = options.Value;
        if (!settings.Enabled)
            throw new InvalidOperationException("Transporte AWS desabilitado.");

        var destination = !string.IsNullOrWhiteSpace(settings.DefaultDestination)
            ? settings.DefaultDestination
            : envelope.Type;

        using var activity = MessagingDiagnostics.StartPublish(envelope, "aws", destination);
        var sent = activity?.Id is { Length: <= MessageEnvelope.MaxTraceParentLength } id
            ? envelope with { TraceParent = id }
            : envelope;

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(settings.PublishTimeout);

        if (destination.StartsWith("arn:aws:sns:", StringComparison.OrdinalIgnoreCase))
        {
            var sns = await clientFactory.GetSnsAsync(timeout.Token).ConfigureAwait(false);
            var request = new PublishRequest
            {
                TopicArn = destination,
                Message = sent.Payload,
                MessageAttributes = new Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>
                {
                    ["messageId"] = new() { DataType = "String", StringValue = sent.MessageId.ToString() },
                    ["type"] = new() { DataType = "String", StringValue = sent.Type },
                    ["version"] = new() { DataType = "Number", StringValue = sent.Version.ToString(CultureInfo.InvariantCulture) },
                    ["occurredAt"] = new() { DataType = "String", StringValue = sent.OccurredAt.ToString("O") },
                },
            };

            if (sent.CorrelationId is not null)
                request.MessageAttributes["correlationId"] = new() { DataType = "String", StringValue = sent.CorrelationId };
            if (sent.CausationId is not null)
                request.MessageAttributes["causationId"] = new() { DataType = "String", StringValue = sent.CausationId };
            if (sent.TraceParent is not null)
                request.MessageAttributes["traceparent"] = new() { DataType = "String", StringValue = sent.TraceParent };

            await sns.PublishAsync(request, timeout.Token).ConfigureAwait(false);
        }
        else
        {
            var sqs = await clientFactory.GetSqsAsync(timeout.Token).ConfigureAwait(false);
            var request = new SendMessageRequest
            {
                QueueUrl = destination,
                MessageBody = sent.Payload,
                MessageAttributes = new Dictionary<string, Amazon.SQS.Model.MessageAttributeValue>
                {
                    ["messageId"] = new() { DataType = "String", StringValue = sent.MessageId.ToString() },
                    ["type"] = new() { DataType = "String", StringValue = sent.Type },
                    ["version"] = new() { DataType = "Number", StringValue = sent.Version.ToString(CultureInfo.InvariantCulture) },
                    ["occurredAt"] = new() { DataType = "String", StringValue = sent.OccurredAt.ToString("O") },
                },
            };

            if (sent.CorrelationId is not null)
                request.MessageAttributes["correlationId"] = new() { DataType = "String", StringValue = sent.CorrelationId };
            if (sent.CausationId is not null)
                request.MessageAttributes["causationId"] = new() { DataType = "String", StringValue = sent.CausationId };
            if (sent.TraceParent is not null)
                request.MessageAttributes["traceparent"] = new() { DataType = "String", StringValue = sent.TraceParent };

            await sqs.SendMessageAsync(request, timeout.Token).ConfigureAwait(false);
        }
    }
}
