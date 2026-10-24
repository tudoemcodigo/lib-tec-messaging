using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using TEC.Vault.Abstractions;

namespace TEC.Messaging.AzureServiceBus.Internal;

/// <summary>
/// Conexão gerenciada com o Azure Service Bus. A connection string é resolvida com segurança pelo cofre (TEC.Vault).
/// </summary>
internal sealed class AzureServiceBusConnection(ISecretReader vault, IOptions<AzureServiceBusOptions> options) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private ServiceBusClient? _client;
    private int _disposed;

    public bool IsOpen => _client is { IsClosed: false };

    public async Task<ServiceBusClient> GetClientAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_client is { IsClosed: false } existing)
            return existing;

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_client is { IsClosed: false } current)
                return current;

            if (_client is not null)
            {
                await _client.DisposeAsync().ConfigureAwait(false);
                _client = null;
            }

            var settings = options.Value;
            var secret = await vault.GetSecretAsync(settings.ConnectionSecretName, cancellationToken: ct).ConfigureAwait(false);
            if (secret.IsFailure)
                throw new InvalidOperationException($"Segredo '{settings.ConnectionSecretName}' do Azure Service Bus indisponível no cofre ({secret.Error?.Code}).");

            _client = new ServiceBusClient(secret.Value.Value);
            return _client;
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

        if (_client is not null)
            await _client.DisposeAsync().ConfigureAwait(false);
    }
}
