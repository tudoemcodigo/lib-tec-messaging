using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Polly;
using Polly.CircuitBreaker;
using RabbitMQ.Client;
using TEC.Messaging.Diagnostics;
using TEC.Vault.Abstractions;

namespace TEC.Messaging.RabbitMQ.Internal;

/// <summary>
/// Conexão única do processo com o broker. A URI vem do cofre (nunca da configuração). Sem recuperação automática do
/// cliente: quem usa a conexão detecta canal fechado e pede de novo, o que recria a conexão e relê o segredo
/// (acompanhando rotação de senha).
/// </summary>
/// <remarks>
/// A criação da conexão passa por um circuit breaker (<see cref="RabbitMqOptions.CircuitBreaker"/>): com o broker ou o cofre
/// fora do ar, publicador, consumidores, monitor de DLQ e health check deixam de tentar todos ao mesmo tempo e recebem
/// <see cref="RabbitMqCircuitOpenException"/> na hora, até a pausa acabar.
/// </remarks>
internal sealed partial class RabbitMqConnection(
    ISecretReader vault,
    IOptions<RabbitMqOptions> options,
    IHostEnvironment environment,
    MessagingMetrics? metrics = null,
    ILogger<RabbitMqConnection>? logger = null,
    TimeProvider? time = null) : IAsyncDisposable
{
    private const string System = "rabbitmq";

    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly CircuitBreakerStateProvider _state = new();
    private readonly ILogger _logger = (ILogger?)logger ?? NullLogger.Instance;
    private ResiliencePipeline? _circuit;
    private IConnection? _connection;
    private int _disposed;

    public bool IsOpen => _connection is { IsOpen: true };

    /// <summary>Circuito aberto: novas conexões são recusadas sem tentar.</summary>
    public bool IsCircuitOpen => _state.CircuitState is CircuitState.Open or CircuitState.Isolated;

    public async Task<IConnection> GetAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_connection is { IsOpen: true } open)
            return open;

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            // DisposeAsync pode ter rodado enquanto esperávamos a trava: não criar conexão que ninguém descartaria
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_connection is { IsOpen: true } current)
                return current;
            if (_connection is not null)
            {
                await DisposeConnectionAsync(_connection).ConfigureAwait(false);
                _connection = null;
            }

            var circuit = _circuit ??= BuildCircuit(options.Value.CircuitBreaker);
            try
            {
                _connection = await circuit.ExecuteAsync(static (self, token) => new ValueTask<IConnection>(self.CreateAsync(token)), this, ct)
                    .ConfigureAwait(false);
            }
            catch (BrokenCircuitException exception)
            {
                LogCircuitRejected();
                throw new RabbitMqCircuitOpenException("Circuito aberto: o RabbitMQ (ou o cofre) falhou repetidas vezes; conexão não tentada.", exception);
            }

            if (Volatile.Read(ref _disposed) != 0)
            {
                // Descartado durante a criação: a conexão nova não pode sobrar aberta
                await DisposeConnectionAsync(_connection).ConfigureAwait(false);
                _connection = null;
                throw new ObjectDisposedException(nameof(RabbitMqConnection));
            }

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
        // _lock não é descartado: um GetAsync em andamento ainda chama Release (SemaphoreSlim sem AvailableWaitHandle não
        // guarda recurso nativo); a conexão criada por ele depois do descarte é fechada lá
    }

    private async Task<IConnection> CreateAsync(CancellationToken ct)
    {
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
        return await factory.CreateConnectionAsync(ct).ConfigureAwait(false);
    }

    private ResiliencePipeline BuildCircuit(RabbitMqCircuitBreakerOptions? settings)
    {
        var builder = new ResiliencePipelineBuilder { TimeProvider = time ?? TimeProvider.System };
        if (settings is not { Enabled: true })
            return builder.Build();

        return builder.AddCircuitBreaker(new CircuitBreakerStrategyOptions
        {
            FailureRatio = settings.FailureRatio,
            MinimumThroughput = settings.MinimumThroughput,
            SamplingDuration = settings.SamplingDuration,
            BreakDuration = settings.BreakDuration,
            StateProvider = _state,
            // Cancelamento de quem pediu (desligamento) não diz nada sobre o broker e não abre o circuito. Mas, para o Polly, o que
            // não é falha conta como sucesso: na tentativa de teste (meia-abertura) o cancelamento conta como falha, senão o
            // circuito fecharia sem o broker ter respondido
            ShouldHandle = args => ValueTask.FromResult(args.Outcome.Exception is { } ex
                && (ex is not OperationCanceledException || _state.CircuitState == CircuitState.HalfOpen)),
            OnOpened = args =>
            {
                metrics?.CircuitStateChanged(System, "open");
                LogCircuitOpened(args.BreakDuration.TotalSeconds);
                return default;
            },
            OnHalfOpened = _ =>
            {
                metrics?.CircuitStateChanged(System, "half_open");
                LogCircuitHalfOpened();
                return default;
            },
            OnClosed = _ =>
            {
                metrics?.CircuitStateChanged(System, "closed");
                LogCircuitClosed();
                return default;
            }
        }).Build();
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

    [LoggerMessage(EventId = 4401, Level = LogLevel.Warning,
        Message = "Circuito da conexão com o RabbitMQ aberto após falhas repetidas; novas conexões recusadas por {BreakSeconds} s")]
    private partial void LogCircuitOpened(double breakSeconds);

    [LoggerMessage(EventId = 4402, Level = LogLevel.Information, Message = "Circuito da conexão com o RabbitMQ meio-aberto; testando com uma conexão")]
    private partial void LogCircuitHalfOpened();

    [LoggerMessage(EventId = 4403, Level = LogLevel.Information, Message = "Circuito da conexão com o RabbitMQ fechado; o broker voltou a aceitar conexões")]
    private partial void LogCircuitClosed();

    [LoggerMessage(EventId = 4404, Level = LogLevel.Debug, Message = "Conexão com o RabbitMQ recusada com o circuito aberto")]
    private partial void LogCircuitRejected();
}
