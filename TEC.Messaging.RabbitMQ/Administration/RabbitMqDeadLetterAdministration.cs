using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using TEC.Core.Common.Results;
using TEC.Messaging.Administration;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.RabbitMQ.Consumers;
using TEC.Messaging.RabbitMQ.Internal;

namespace TEC.Messaging.RabbitMQ.Administration;

/// <summary>
/// DLQ pelo protocolo AMQP (sem a API de gerenciamento): <c>basic.get</c> sem ack e, no fim, NACK com requeue (listagem)
/// ou ack depois da republicação confirmada na fila principal (reprocessamento).
/// </summary>
internal sealed class RabbitMqDeadLetterAdministration(RabbitMqConnection connection, RabbitMqPublisher publisher) : IDeadLetterAdministration
{
    private const int MaxIds = 1000;
    private static readonly Error QueueNotFound = Error.NotFound(IDeadLetterAdministration.QueueNotFoundCode, "Fila (ou DLQ) não encontrada.");
    private static readonly Error InvalidQueue = Error.Validation("FILA_INVALIDA", "Nome de fila inválido.", "queue");

    public async Task<Result<long>> CountAsync(string queue, CancellationToken cancellationToken)
    {
        if (!QueueDefinition.IsValidName(queue))
            return InvalidQueue;
        var channel = await (await connection.GetAsync(cancellationToken).ConfigureAwait(false)).CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        await using (channel.ConfigureAwait(false))
        {
            return await CountMessagesAsync(channel, $"{queue}.dlq", cancellationToken).ConfigureAwait(false) is { } count ? count : QueueNotFound;
        }
    }

    public async Task<Result<IReadOnlyList<DeadLetterMessage>>> ListAsync(string queue, int limit, CancellationToken cancellationToken)
    {
        if (!QueueDefinition.IsValidName(queue))
            return InvalidQueue;
        if (limit is < 1 or > 500)
            return Error.Validation("LIMITE_INVALIDO", "O limite deve estar entre 1 e 500.", "limit");

        var channel = await (await connection.GetAsync(cancellationToken).ConfigureAwait(false)).CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        await using (channel.ConfigureAwait(false))
        {
            var dlq = $"{queue}.dlq";
            if (await CountMessagesAsync(channel, dlq, cancellationToken).ConfigureAwait(false) is not { } total)
                return QueueNotFound;

            var read = new List<DeadLetterMessage>();
            ulong last = 0;
            for (var i = 0; i < Math.Min(limit, total); i++)
            {
                var item = await channel.BasicGetAsync(dlq, autoAck: false, cancellationToken).ConfigureAwait(false);
                if (item is null)
                    break;
                last = item.DeliveryTag;
                read.Add(ToMessage(item));
            }

            if (last > 0)
                await channel.BasicNackAsync(last, multiple: true, requeue: true, cancellationToken).ConfigureAwait(false);
            return read;
        }
    }

    public async Task<Result<int>> RequeueAsync(
        string queue, IReadOnlyCollection<Guid>? messageIds, Func<DeadLetterMessage, CancellationToken, Task<bool>>? handler,
        CancellationToken cancellationToken)
    {
        if (!QueueDefinition.IsValidName(queue))
            return InvalidQueue;
        if (messageIds is { Count: > MaxIds })
            return Error.Validation("IDS_INVALIDOS", $"Informe até {MaxIds} mensagens.", "messageIds");
        return await MoveAsync(queue, messageIds is { Count: > 0 } ? [.. messageIds] : null, republish: true, handler, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Result<int>> DiscardAsync(string queue, IReadOnlyCollection<Guid> messageIds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messageIds);
        if (!QueueDefinition.IsValidName(queue))
            return InvalidQueue;
        if (messageIds.Count is 0 or > MaxIds)
            return Error.Validation("IDS_INVALIDOS", $"Informe de 1 a {MaxIds} mensagens.", "messageIds");
        return await MoveAsync(queue, [.. messageIds], republish: false, null, cancellationToken).ConfigureAwait(false);
    }

    private async Task<Result<int>> MoveAsync(
        string queue, HashSet<Guid>? filter, bool republish, Func<DeadLetterMessage, CancellationToken, Task<bool>>? handler, CancellationToken ct)
    {
        var dlq = $"{queue}.dlq";
        var channel = await (await connection.GetAsync(ct).ConfigureAwait(false)).CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);
        await using (channel.ConfigureAwait(false))
        {
            if (await CountMessagesAsync(channel, dlq, ct).ConfigureAwait(false) is not { } total)
                return QueueNotFound;
            if (republish && await CountMessagesAsync(channel, queue, ct).ConfigureAwait(false) is null)
                return QueueNotFound;

            var moved = 0;
            ulong lastKept = 0;
            for (var i = 0; i < total; i++)
            {
                var item = await channel.BasicGetAsync(dlq, autoAck: false, ct).ConfigureAwait(false);
                if (item is null)
                    break;

                var message = ToMessage(item);
                if (filter is not null && !filter.Contains(message.MessageId))
                {
                    lastKept = item.DeliveryTag;
                    continue;
                }

                if (republish)
                {
                    var handled = handler is not null && await handler(message, ct).ConfigureAwait(false);
                    if (!handled)
                    {
                        var properties = AmqpEnvelope.Copy(item.BasicProperties);
                        properties.Headers!.Remove(AmqpEnvelope.AttemptHeader);
                        properties.Headers.Remove(AmqpEnvelope.ErrorHeader);
                        properties.Headers[AmqpEnvelope.RequeuedHeader] = true;
                        await publisher.PublishRawAsync("", queue, true, properties, item.Body.ToArray(), ct).ConfigureAwait(false);
                    }
                }

                await channel.BasicAckAsync(item.DeliveryTag, multiple: false, ct).ConfigureAwait(false);
                moved++;
            }

            // Devolve as não selecionadas (multiple: só as ainda sem ack até esta entrega)
            if (lastKept > 0)
                await channel.BasicNackAsync(lastKept, multiple: true, requeue: true, ct).ConfigureAwait(false);
            return moved;
        }
    }

    /// <summary>Mensagens prontas na fila; <c>null</c> se ela não existe (a declaração passiva falha e fecha o canal).</summary>
    private static async Task<uint?> CountMessagesAsync(IChannel channel, string queue, CancellationToken ct)
    {
        try
        {
            var declared = await channel.QueueDeclarePassiveAsync(queue, ct).ConfigureAwait(false);
            return declared.MessageCount;
        }
        catch (OperationInterruptedException)
        {
            return null;
        }
    }

    private static DeadLetterMessage ToMessage(BasicGetResult item)
    {
        var properties = item.BasicProperties;
        var attempts = AmqpEnvelope.HeaderInt(properties.Headers, AmqpEnvelope.AttemptHeader);
        var error = AmqpEnvelope.HeaderText(properties.Headers, AmqpEnvelope.ErrorHeader);
        var envelope = AmqpEnvelope.Read(item.Body, out var readError);
        if (envelope is not null)
        {
            return new DeadLetterMessage(
                envelope.MessageId, envelope.Type, envelope.Version, envelope.OccurredAt, envelope.CorrelationId, attempts, error, envelope.Payload);
        }

        return new DeadLetterMessage(
            Guid.TryParse(properties.MessageId, out var id) ? id : Guid.Empty, properties.Type ?? "?", 1, null, properties.CorrelationId,
            attempts, error ?? readError, Encoding.UTF8.GetString(item.Body.Span[..Math.Min(item.Body.Length, 64 * 1024)]));
    }
}

/// <summary>Conta as DLQs dos consumidores duráveis deste processo para a métrica <c>tec.messaging.dlq.messages</c>.</summary>
internal sealed partial class DeadLetterMonitor(
    IServiceProvider services, IOptions<RabbitMqOptions> options, MessagingMetrics metrics, TimeProvider time, ILogger<DeadLetterMonitor> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        if (!settings.Enabled || settings.DeadLetterMonitorInterval <= TimeSpan.Zero)
            return;

        var administration = services.GetRequiredService<IDeadLetterAdministration>();
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var consumer in services.GetServices<RabbitMqConsumer>().Where(c => !c.Definition.Transient))
            {
                try
                {
                    var count = await administration.CountAsync(consumer.Definition.Name, stoppingToken).ConfigureAwait(false);
                    if (count.IsSuccess)
                        metrics.UpdateDeadLetters(consumer.Definition.Name, count.Value);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    LogCountFailed(ex, consumer.Definition.Name);
                }
            }

            try
            {
                await Task.Delay(settings.DeadLetterMonitorInterval, time, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    [LoggerMessage(EventId = 4301, Level = LogLevel.Warning, Message = "Falha ao contar a DLQ da fila {Queue}")]
    private partial void LogCountFailed(Exception exception, string queue);
}
