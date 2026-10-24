using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Microsoft.Extensions.Options;
using TEC.Vault.Abstractions;

namespace TEC.Messaging.Aws.Internal;

/// <summary>
/// Fábrica de clientes AWS (SQS e SNS) com credenciais e endpoints gerenciados pelo TEC.Vault.
/// </summary>
internal sealed class AwsClientFactory(ISecretReader vault, IOptions<AwsMessagingOptions> options) : IAsyncDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private IAmazonSQS? _sqs;
    private IAmazonSimpleNotificationService? _sns;
    private int _disposed;

    public async Task<IAmazonSQS> GetSqsAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_sqs is not null)
            return _sqs;

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_sqs is not null)
                return _sqs;

            await EnsureClientsAsync(ct).ConfigureAwait(false);
            return _sqs!;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<IAmazonSimpleNotificationService> GetSnsAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_sns is not null)
            return _sns;

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_sns is not null)
                return _sns;

            await EnsureClientsAsync(ct).ConfigureAwait(false);
            return _sns!;
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task EnsureClientsAsync(CancellationToken ct)
    {
        var settings = options.Value;
        var region = RegionEndpoint.GetBySystemName(settings.Region);

        var secret = await vault.GetSecretAsync(settings.ConnectionSecretName, cancellationToken: ct).ConfigureAwait(false);
        AWSCredentials credentials;

        if (secret.IsSuccess && !string.IsNullOrWhiteSpace(secret.Value.Value))
        {
            var raw = secret.Value.Value.Trim();
            if (raw.Contains(':'))
            {
                var parts = raw.Split(':', 2);
                credentials = new BasicAWSCredentials(parts[0], parts[1]);
            }
            else
            {
                credentials = FallbackCredentialsFactory.GetCredentials();
            }
        }
        else
        {
            credentials = FallbackCredentialsFactory.GetCredentials();
        }

        var sqsConfig = new AmazonSQSConfig { RegionEndpoint = region };
        var snsConfig = new AmazonSimpleNotificationServiceConfig { RegionEndpoint = region };

        _sqs = new AmazonSQSClient(credentials, sqsConfig);
        _sns = new AmazonSimpleNotificationServiceClient(credentials, snsConfig);
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;

        _sqs?.Dispose();
        _sns?.Dispose();
        return ValueTask.CompletedTask;
    }
}
