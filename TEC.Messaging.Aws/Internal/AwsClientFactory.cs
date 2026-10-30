using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Microsoft.Extensions.Options;
using TEC.Vault.Abstractions;

namespace TEC.Messaging.Aws.Internal;

/// <summary>
/// Fábrica de clientes AWS (SQS e SNS) com credenciais lidas do TEC.Vault. Quando a AWS recusa a credencial (chave
/// rotacionada ou desativada, token de sessão vencido), quem percebeu chama <see cref="InvalidateAsync"/> e a próxima operação
/// recria os clientes relendo o segredo, como o RabbitMQ faz ao recriar a conexão.
/// </summary>
/// <remarks>
/// Criação e descarte passam pela mesma trava (single-flight): várias falhas simultâneas com o mesmo cliente descartam e
/// recriam uma única vez. Os clientes nunca usam HTTP (<c>UseHttp = false</c>).
/// </remarks>
internal sealed class AwsClientFactory(ISecretReader vault, IOptions<AwsMessagingOptions> options) : IAsyncDisposable
{
    /// <summary>Códigos de erro da AWS que indicam credencial inválida, desativada ou vencida (e não falta de permissão).</summary>
    private static readonly HashSet<string> CredentialErrorCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "InvalidClientTokenId", "UnrecognizedClientException", "SignatureDoesNotMatch", "IncompleteSignature",
        "InvalidAccessKeyId", "ExpiredToken", "ExpiredTokenException", "InvalidToken", "AuthFailure", "InvalidSecurity",
    };

    private readonly SemaphoreSlim _lock = new(1, 1);
    private Clients? _clients;
    private int _disposed;

    public async Task<IAmazonSQS> GetSqsAsync(CancellationToken ct) => (await GetClientsAsync(ct).ConfigureAwait(false)).Sqs;

    public async Task<IAmazonSimpleNotificationService> GetSnsAsync(CancellationToken ct) => (await GetClientsAsync(ct).ConfigureAwait(false)).Sns;

    /// <summary>A exceção (ou uma interna) é recusa da credencial pela AWS.</summary>
    public static bool IsCredentialFailure(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is AmazonServiceException { ErrorCode: { } code } && CredentialErrorCodes.Contains(code))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Descarta os clientes se <paramref name="failed"/> (SQS ou SNS) ainda for um dos atuais; chamadas repetidas com o mesmo
    /// cliente são ignoradas. A próxima operação recria os dois, relendo o segredo.
    /// </summary>
    public async Task InvalidateAsync(object failed)
    {
        if (Volatile.Read(ref _disposed) != 0)
            return;

        Clients? stale = null;
        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (_clients is { } current && (ReferenceEquals(current.Sqs, failed) || ReferenceEquals(current.Sns, failed)))
            {
                stale = current;
                Volatile.Write(ref _clients, null);
            }
        }
        finally
        {
            _lock.Release();
        }

        stale?.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;

        Interlocked.Exchange(ref _clients, null)?.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<Clients> GetClientsAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _clients) is { } existing)
            return existing;

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_clients is { } current)
                return current;

            var created = await CreateClientsAsync(ct).ConfigureAwait(false);
            if (Volatile.Read(ref _disposed) != 0)
            {
                created.Dispose();
                throw new ObjectDisposedException(nameof(AwsClientFactory));
            }

            Volatile.Write(ref _clients, created);
            return created;
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<Clients> CreateClientsAsync(CancellationToken ct)
    {
        var settings = options.Value;
        var region = RegionEndpoint.GetBySystemName(settings.Region);
        var credentials = await ReadCredentialsAsync(settings, ct).ConfigureAwait(false);

        var sqs = new AmazonSQSClient(credentials, new AmazonSQSConfig { RegionEndpoint = region, UseHttp = false });
        var sns = new AmazonSimpleNotificationServiceClient(credentials, new AmazonSimpleNotificationServiceConfig { RegionEndpoint = region, UseHttp = false });
        return new Clients(sqs, sns);
    }

    private async Task<AWSCredentials> ReadCredentialsAsync(AwsMessagingOptions settings, CancellationToken ct)
    {
        var secret = await vault.GetSecretAsync(settings.ConnectionSecretName, cancellationToken: ct).ConfigureAwait(false);
        if (secret.IsSuccess && !string.IsNullOrWhiteSpace(secret.Value.Value))
        {
            if (TryParseCredentials(secret.Value.Value, out var parsed))
                return parsed;
            if (settings.UseAmbientCredentials)
                return FallbackCredentialsFactory.GetCredentials();
            throw new InvalidOperationException(
                $"O segredo '{settings.ConnectionSecretName}' do cofre não está no formato 'accessKey:secretKey' (ou 'accessKey:secretKey:sessionToken'). Para usar credenciais do ambiente, configure UseAmbientCredentials = true.");
        }

        if (settings.UseAmbientCredentials)
            return FallbackCredentialsFactory.GetCredentials();

        var code = secret.IsFailure ? secret.Error?.Code : "SEGREDO_VAZIO";
        throw new InvalidOperationException(
            $"Não foi possível obter o segredo de credenciais '{settings.ConnectionSecretName}' no cofre ({code}). Para usar credenciais do ambiente, configure UseAmbientCredentials = true.");
    }

    /// <summary>
    /// <c>accessKey:secretKey</c> (chave de longo prazo) ou <c>accessKey:secretKey:sessionToken</c> (credencial temporária do STS).
    /// Chaves secretas da AWS não contêm <c>:</c>.
    /// </summary>
    internal static bool TryParseCredentials(string raw, out AWSCredentials credentials)
    {
        var parts = raw.Trim().Split(':', 3, StringSplitOptions.TrimEntries);
        credentials = null!;
        if (parts.Length < 2 || parts.Any(string.IsNullOrEmpty))
            return false;

        credentials = parts.Length == 3
            ? new SessionAWSCredentials(parts[0], parts[1], parts[2])
            : new BasicAWSCredentials(parts[0], parts[1]);
        return true;
    }

    private sealed record Clients(IAmazonSQS Sqs, IAmazonSimpleNotificationService Sns) : IDisposable
    {
        public void Dispose()
        {
            Sqs.Dispose();
            Sns.Dispose();
        }
    }
}
