using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.RabbitMQ.Consumers;
using TEC.Messaging.Transport;

namespace TEC.Messaging.RabbitMQ.Internal;

/// <summary>
/// Publica com publisher confirms (o <c>await</c> só termina com o ack do broker; nack, timeout ou queda lançam exceção
/// e o Outbox tenta de novo) e mensagens persistentes. Um canal compartilhado, serializado por trava.
/// </summary>
internal sealed class RabbitMqPublisher(RabbitMqConnection connection, IOptions<RabbitMqOptions> options, IServiceProvider services)
    : IMessagePublisher, IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IChannel? _channel;
    private int _disposed;

    public async Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var settings = options.Value;
        var routingKey = settings.RoutingKeyOf(envelope.Type);
        using var activity = MessagingDiagnostics.StartPublish(envelope, "rabbitmq", settings.Exchange);
        activity?.SetTag("messaging.rabbitmq.destination.routing_key", routingKey);

        // O consumidor continua o trace a partir do span de publicação
        var sent = activity?.Id is { Length: <= MessageEnvelope.MaxTraceParentLength } id ? envelope with { TraceParent = id } : envelope;

        // Sem "mandatory": evento sem fila ligada não é erro e não pode travar o Outbox
        await PublishRawAsync(settings.Exchange, routingKey, false, AmqpEnvelope.Properties(sent), AmqpEnvelope.Serialize(sent), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Publicação bruta (retry, DLQ e reprocessamento), aguardando a confirmação.</summary>
    internal async Task PublishRawAsync(
        string exchange, string routingKey, bool mandatory, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var settings = options.Value;
        if (!settings.Enabled)
            throw new InvalidOperationException("Transporte RabbitMQ desligado (Messaging:RabbitMQ:Enabled = false).");

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var channel = await ChannelAsync(ct).ConfigureAwait(false);
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(settings.PublishTimeout);
            await channel.BasicPublishAsync(exchange, routingKey, mandatory, properties, body, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // Canal em estado incerto (nack, retorno, timeout, queda): descarta e recria na próxima publicação
            await DisposeChannelAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        await DisposeChannelAsync().ConfigureAwait(false);
        _lock.Dispose();
    }

    private async Task<IChannel> ChannelAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true } open)
            return open;

        await DisposeChannelAsync().ConfigureAwait(false);
        var exchange = options.Value.Exchange;
        var active = await connection.GetAsync(ct).ConfigureAwait(false);
        var channel = await active.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true), ct).ConfigureAwait(false);
        try
        {
            await Topology.DeclareExchangeAsync(channel, exchange, ct).ConfigureAwait(false);

            // Filas duráveis dos consumidores deste processo antes da primeira publicação: evento publicado antes de a fila
            // existir não teria para onde ir (relay e consumidores sobem juntos)
            foreach (var consumer in services.GetServices<RabbitMqConsumer>().Where(c => !c.Definition.Transient))
                await Topology.DeclareAsync(channel, exchange, consumer.Definition, ct).ConfigureAwait(false);
        }
        catch
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        _channel = channel;
        return channel;
    }

    private async Task DisposeChannelAsync()
    {
        var channel = _channel;
        _channel = null;
        if (channel is null)
            return;
        try
        {
            await channel.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Canal já fechado pelo broker.
        }
    }
}
