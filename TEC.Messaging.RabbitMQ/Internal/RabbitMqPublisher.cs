using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.RabbitMQ.Consumers;
using TEC.Messaging.Transport;

namespace TEC.Messaging.RabbitMQ.Internal;

/// <summary>
/// Publica com publisher confirms (o <c>await</c> só termina com o ack do broker; nack, timeout ou queda lançam exceção
/// e o Outbox tenta de novo) e mensagens persistentes. Um canal compartilhado, serializado por trava.
/// </summary>
/// <remarks>
/// Falhas do transporte (desligado, sem conexão ou canal, conexão perdida, confirmação fora do prazo) saem como
/// <see cref="RabbitMqUnavailableException"/> (ou <see cref="RabbitMqCircuitOpenException"/>): o Outbox não conta tentativa.
/// Recusa da mensagem pelo broker (nack, canal fechado com a conexão aberta) sai como veio e conta tentativa. A publicação é
/// <c>mandatory</c>: mensagem sem fila ligada volta do broker (<c>basic.return</c>) e sai como
/// <see cref="RabbitMqUnroutableException"/>, que também conta tentativa.
/// </remarks>
internal sealed partial class RabbitMqPublisher(
    RabbitMqConnection connection, IOptions<RabbitMqOptions> options, IServiceProvider services, ILogger<RabbitMqPublisher>? logger = null)
    : IMessagePublisher, IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly ILogger _logger = (ILogger?)logger ?? NullLogger.Instance;
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

        // Com "mandatory": evento sem fila ligada (binding esquecido, routing key errada) volta do broker e conta tentativa, em
        // vez de ser descartado em silêncio com a confirmação positiva
        await PublishRawAsync(settings.Exchange, routingKey, true, AmqpEnvelope.Properties(sent), AmqpEnvelope.Serialize(sent), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Publicação bruta (retry, DLQ e reprocessamento), aguardando a confirmação.</summary>
    internal async Task PublishRawAsync(
        string exchange, string routingKey, bool mandatory, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var settings = options.Value;
        if (!settings.Enabled)
            throw new RabbitMqUnavailableException("Transporte RabbitMQ desligado (Messaging:RabbitMQ:Enabled = false).");

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        var publishing = false;
        try
        {
            // DisposeAsync pode ter rodado enquanto esperávamos a trava
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            var channel = await ChannelAsync(ct).ConfigureAwait(false);
            publishing = true;
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(settings.PublishTimeout);
            await channel.BasicPublishAsync(exchange, routingKey, mandatory, properties, body, timeout.Token).ConfigureAwait(false);
        }
        catch (PublishException ex) when (ex.IsReturn)
        {
            // basic.return: o canal segue íntegro (não é descartado). Falha de roteamento, não do transporte: conta tentativa
            throw new RabbitMqUnroutableException(
                $"O RabbitMQ devolveu a mensagem: nenhuma fila ligada à exchange '{Limit(exchange)}' com a routing key '{Limit(routingKey)}'.", ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // Canal em estado incerto (nack, retorno, timeout, queda): descarta e recria na próxima publicação
            await DisposeChannelAsync().ConfigureAwait(false);
            if (ex is ITransportUnavailableException or ObjectDisposedException)
                throw;
            if (!publishing)
                throw new RabbitMqUnavailableException("Sem conexão ou canal com o RabbitMQ.", ex);
            if (ex is OperationCanceledException)
                throw new RabbitMqUnavailableException($"O RabbitMQ não confirmou a publicação em {settings.PublishTimeout}.", ex);
            if (!connection.IsOpen)
                throw new RabbitMqUnavailableException("Conexão com o RabbitMQ perdida durante a publicação.", ex);
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

        // Espera a publicação em andamento terminar (no máximo o prazo de confirmação) antes de fechar o canal. A trava não é
        // descartada: uma publicação que ainda espera por ela chama Release (SemaphoreSlim sem AvailableWaitHandle não guarda
        // recurso nativo) e encontra o publicador descartado
        var acquired = await _lock.WaitAsync(options.Value.PublishTimeout + TimeSpan.FromSeconds(1)).ConfigureAwait(false);
        try
        {
            await DisposeChannelAsync().ConfigureAwait(false);
        }
        finally
        {
            if (acquired)
                _lock.Release();
        }
    }

    private static string Limit(string value) => value.Length > 200 ? value[..200] : value;

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
        }
        catch
        {
            await channel.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        // Filas duráveis dos consumidores deste processo antes da primeira publicação: evento publicado antes de a fila
        // existir não teria para onde ir (relay e consumidores sobem juntos). Cada fila num canal próprio e sem interromper a
        // publicação: uma declaração recusada (ex.: PRECONDITION_FAILED porque RetryDelays ou DeliveryLimit mudou numa fila
        // existente) fecha só aquele canal e é problema daquele consumidor, que a reporta ao subir
        foreach (var consumer in services.GetServices<RabbitMqConsumer>().Where(c => !c.Definition.Transient))
            await DeclareConsumerQueueAsync(active, exchange, consumer.Definition, ct).ConfigureAwait(false);

        _channel = channel;
        return channel;
    }

    private async Task DeclareConsumerQueueAsync(IConnection active, string exchange, QueueDefinition queue, CancellationToken ct)
    {
        try
        {
            var declaring = await active.CreateChannelAsync(cancellationToken: ct).ConfigureAwait(false);
            await using (declaring.ConfigureAwait(false))
                await Topology.DeclareAsync(declaring, exchange, queue, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            LogQueueDeclarationFailed(ex, queue.Name);
        }
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

    [LoggerMessage(EventId = 4212, Level = LogLevel.Warning,
        Message = "Publicador: falha ao declarar a fila {Queue} de um consumidor deste processo; a publicação segue sem ela")]
    private partial void LogQueueDeclarationFailed(Exception exception, string queue);
}
