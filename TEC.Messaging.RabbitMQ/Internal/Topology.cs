using RabbitMQ.Client;

namespace TEC.Messaging.RabbitMQ.Internal;

/// <summary>Declaração (idempotente) da exchange e das filas de um consumidor.</summary>
internal static class Topology
{
    public static Task DeclareExchangeAsync(IChannel channel, string exchange, CancellationToken ct) =>
        channel.ExchangeDeclareAsync(exchange, ExchangeType.Topic, durable: true, autoDelete: false, cancellationToken: ct);

    public static async Task DeclareAsync(IChannel channel, string exchange, QueueDefinition queue, CancellationToken ct)
    {
        await DeclareExchangeAsync(channel, exchange, ct).ConfigureAwait(false);

        if (queue.Transient)
        {
            await channel.QueueDeclareAsync(queue.Name, durable: false, exclusive: true, autoDelete: true, cancellationToken: ct).ConfigureAwait(false);
        }
        else
        {
            // DLQ: sem TTL e sem limite de entregas (a listagem pelo administrador devolve as mensagens à fila)
            await channel.QueueDeclareAsync(queue.DeadLetterQueue, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum", ["x-delivery-limit"] = -1 }, cancellationToken: ct)
                .ConfigureAwait(false);

            await channel.QueueDeclareAsync(queue.Name, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    ["x-queue-type"] = "quorum",
                    ["x-delivery-limit"] = queue.DeliveryLimit,
                    ["x-dead-letter-exchange"] = "",
                    ["x-dead-letter-routing-key"] = queue.DeadLetterQueue,
                    ["x-dead-letter-strategy"] = "at-least-once",
                    ["x-overflow"] = "reject-publish",
                },
                cancellationToken: ct).ConfigureAwait(false);

            foreach (var delay in queue.RetryDelays.Distinct())
            {
                await channel.QueueDeclareAsync(queue.RetryQueue(delay), durable: true, exclusive: false, autoDelete: false,
                    arguments: new Dictionary<string, object?>
                    {
                        ["x-queue-type"] = "quorum",
                        ["x-message-ttl"] = (long)delay.TotalMilliseconds,
                        ["x-dead-letter-exchange"] = "",
                        ["x-dead-letter-routing-key"] = queue.Name,
                        ["x-dead-letter-strategy"] = "at-least-once",
                        ["x-overflow"] = "reject-publish",
                    },
                    cancellationToken: ct).ConfigureAwait(false);
            }
        }

        foreach (var key in queue.RoutingKeys)
            await channel.QueueBindAsync(queue.Name, exchange, key, cancellationToken: ct).ConfigureAwait(false);
    }
}
