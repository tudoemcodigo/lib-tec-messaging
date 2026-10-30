using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TEC.Messaging.Internal;
using TEC.Vault.Abstractions;

namespace TEC.Messaging.AzureServiceBus.Internal;

/// <summary>
/// Conexão gerenciada com o Azure Service Bus. A connection string é resolvida pelo cofre (TEC.Vault) a cada criação do
/// cliente: quando o Service Bus recusa a credencial (<see cref="UnauthorizedAccessException"/>, ex.: chave rotacionada ou SAS
/// vencida), quem percebeu chama <see cref="InvalidateAsync(ServiceBusClient)"/> e a próxima operação relê o segredo, como o RabbitMQ faz ao
/// recriar a conexão.
/// </summary>
/// <remarks>
/// Criação e descarte passam pela mesma trava (single-flight): várias falhas simultâneas com o mesmo cliente descartam e
/// recriam uma única vez. Os <see cref="ServiceBusSender"/> pertencem ao cliente que os criou e são descartados com ele.
/// </remarks>
internal sealed class AzureServiceBusConnection(
    ISecretReader vault, IOptions<AzureServiceBusOptions> options, IHostEnvironment? environment = null) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private ClientState? _state;
    private int _disposed;

    public bool IsOpen => Volatile.Read(ref _state) is { Client.IsClosed: false };

    public async Task<ServiceBusClient> GetClientAsync(CancellationToken ct) => (await GetStateAsync(ct).ConfigureAwait(false)).Client;

    /// <summary>Sender da entidade (fila ou tópico), reaproveitado enquanto o cliente atual valer.</summary>
    public async Task<ServiceBusSender> GetSenderAsync(string entity, CancellationToken ct)
    {
        var state = await GetStateAsync(ct).ConfigureAwait(false);
        return state.Senders.GetOrAdd(entity, static (name, client) => client.CreateSender(name), state.Client);
    }

    /// <summary>
    /// Descarta o cliente que recusou a credencial, se ainda for o atual (chamadas repetidas com o mesmo cliente são
    /// ignoradas). A próxima operação cria outro, relendo o segredo do cofre.
    /// </summary>
    public Task InvalidateAsync(ServiceBusClient failed) => InvalidateAsync(s => ReferenceEquals(s.Client, failed));

    /// <summary>Como <see cref="InvalidateAsync(ServiceBusClient)"/>, a partir de um sender criado por esta conexão.</summary>
    public Task InvalidateAsync(ServiceBusSender failed) => InvalidateAsync(s => s.Senders.Values.Any(v => ReferenceEquals(v, failed)));

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        if (Interlocked.Exchange(ref _state, null) is { } state)
            await state.DisposeAsync().ConfigureAwait(false);

        // _lock não é descartado: uma criação em andamento ainda chama Release e descarta o próprio cliente ao ver _disposed
    }

    private async Task<ClientState> GetStateAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _state) is { Client.IsClosed: false } existing)
            return existing;

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_state is { Client.IsClosed: false } current)
                return current;

            if (Interlocked.Exchange(ref _state, null) is { } closed)
                await closed.DisposeAsync().ConfigureAwait(false);

            var created = new ClientState(await CreateClientAsync(ct).ConfigureAwait(false));
            if (Volatile.Read(ref _disposed) != 0)
            {
                await created.DisposeAsync().ConfigureAwait(false);
                throw new ObjectDisposedException(nameof(AzureServiceBusConnection));
            }

            Volatile.Write(ref _state, created);
            return created;
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task InvalidateAsync(Func<ClientState, bool> owns)
    {
        if (Volatile.Read(ref _disposed) != 0)
            return;

        ClientState? stale = null;
        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (_state is { } current && owns(current))
            {
                stale = current;
                Volatile.Write(ref _state, null);
            }
        }
        finally
        {
            _lock.Release();
        }

        if (stale is not null)
            await stale.DisposeAsync().ConfigureAwait(false);
    }

    private async Task<ServiceBusClient> CreateClientAsync(CancellationToken ct)
    {
        var settings = options.Value;
        var secret = await vault.GetSecretAsync(settings.ConnectionSecretName, cancellationToken: ct).ConfigureAwait(false);
        if (secret.IsFailure)
            throw new InvalidOperationException($"Segredo '{settings.ConnectionSecretName}' do Azure Service Bus indisponível no cofre ({secret.Error?.Code}).");

        var connectionString = secret.Value.Value;
        try
        {
            _ = ServiceBusConnectionStringProperties.Parse(connectionString);
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException)
        {
            // A mensagem da exceção original pode repetir trechos da connection string: não é propagada
            throw new InvalidOperationException($"O segredo '{settings.ConnectionSecretName}' não contém uma connection string válida do Azure Service Bus.");
        }

        // Fora do emulador, o cliente sempre usa TLS (AMQP sobre TCP na 5671, o transporte padrão; o pacote não expõe opção para WebSockets); o emulador local fala AMQP sem TLS
        if (UsesDevelopmentEmulator(connectionString) && TransportSecurity.RequiresTls(settings.RequireTls, environment))
        {
            throw new InvalidOperationException(
                $"O segredo '{settings.ConnectionSecretName}' aponta para o emulador do Service Bus (UseDevelopmentEmulator, sem TLS), recusado porque a conexão exige TLS (RequireTls; padrão fora de Development).");
        }

        return new ServiceBusClient(connectionString);
    }

    /// <summary>A connection string liga o modo do emulador local (<c>UseDevelopmentEmulator=true</c>, AMQP sem TLS).</summary>
    internal static bool UsesDevelopmentEmulator(string connectionString)
    {
        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var separator = part.IndexOf('=', StringComparison.Ordinal);
            if (separator > 0
                && part[..separator].Trim().Equals("UseDevelopmentEmulator", StringComparison.OrdinalIgnoreCase)
                && bool.TryParse(part[(separator + 1)..].Trim(), out var enabled))
            {
                return enabled;
            }
        }

        return false;
    }

    private sealed class ClientState(ServiceBusClient client) : IAsyncDisposable
    {
        public ServiceBusClient Client { get; } = client;

        public ConcurrentDictionary<string, ServiceBusSender> Senders { get; } = new(StringComparer.Ordinal);

        public async ValueTask DisposeAsync()
        {
            foreach (var sender in Senders.Values)
            {
                try
                {
                    await sender.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Link já fechado: o descarte do cliente encerra a conexão de qualquer forma.
                }
            }

            Senders.Clear();
            try
            {
                await Client.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Conexão já quebrada: nada a liberar.
            }
        }
    }
}
