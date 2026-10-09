using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using TEC.Vault.Abstractions;

namespace TEC.Messaging.RabbitMQ.Internal;

/// <summary>
/// Conexão única do processo com o broker. A URI vem do cofre (nunca da configuração). Sem recuperação automática do
/// cliente: quem usa a conexão detecta canal fechado e pede de novo, o que recria a conexão e relê o segredo
/// (acompanhando rotação de senha).
/// </summary>
internal sealed class RabbitMqConnection(ISecretReader vault, IOptions<RabbitMqOptions> options, IHostEnvironment environment) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IConnection? _connection;
    private int _disposed;

    public bool IsOpen => _connection is { IsOpen: true };

    public async Task<IConnection> GetAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_connection is { IsOpen: true } open)
            return open;

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_connection is { IsOpen: true } current)
                return current;
            if (_connection is not null)
                await DisposeConnectionAsync(_connection).ConfigureAwait(false);

            var settings = options.Value;
            var secret = await vault.GetSecretAsync(settings.ConnectionSecretName, cancellationToken: ct).ConfigureAwait(false);
            if (secret.IsFailure)
                throw new InvalidOperationException($"Segredo '{settings.ConnectionSecretName}' da mensageria indisponível no cofre ({secret.Error?.Code}).");
            if (!Uri.TryCreate(secret.Value.Value, UriKind.Absolute, out var uri) || uri.Scheme is not ("amqp" or "amqps"))
                throw new InvalidOperationException($"O segredo '{settings.ConnectionSecretName}' não contém uma URI amqp:// ou amqps:// válida.");

            var factory = new ConnectionFactory
            {
                Uri = uri,
                AutomaticRecoveryEnabled = false,
                TopologyRecoveryEnabled = false,
                RequestedHeartbeat = settings.Heartbeat,
                ClientProvidedName = $"{environment.ApplicationName}@{Environment.MachineName}",
            };
            _connection = await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
            return _connection;
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
        if (_connection is not null)
            await DisposeConnectionAsync(_connection).ConfigureAwait(false);
        _lock.Dispose();
    }

    private static async Task DisposeConnectionAsync(IConnection connection)
    {
        try
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Conexão já quebrada: nada a liberar.
        }
    }
}
