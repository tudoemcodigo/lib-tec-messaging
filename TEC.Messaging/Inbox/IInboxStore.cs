using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TEC.Messaging.Outbox;

namespace TEC.Messaging.Inbox;

/// <summary>
/// Inbox do consumidor idempotente: registra que uma mensagem foi processada por um consumidor <b>na mesma transação</b>
/// do efeito do processamento. Reentregas da mesma mensagem (at-least-once) são reconhecidas e ignoradas.
/// </summary>
/// <example>
/// <code>
/// if (!await inbox.TryBeginAsync(envelope.MessageId, "notificacoes", ct))
///     return ConsumeResult.Completed;          // já processada
/// ... efeito no banco ...
/// await db.SaveChangesAsync(ct);              // grava o efeito e o registro do Inbox juntos
/// </code>
/// </example>
public interface IInboxStore
{
    /// <summary>
    /// Inicia o processamento: <c>false</c> se a mensagem já foi processada pelo consumidor; senão registra a mensagem na
    /// unidade de trabalho atual (gravada no próximo <c>SaveChanges</c>) e devolve <c>true</c>. Duas entregas simultâneas
    /// da mesma mensagem: uma delas falha no commit (chave duplicada) e, na reentrega, recebe <c>false</c>.
    /// </summary>
    /// <param name="messageId">Mensagem.</param>
    /// <param name="consumer">Nome estável do consumidor (até <see cref="InboxOptions.MaxConsumerLength"/> caracteres).</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns><c>true</c> para processar.</returns>
    Task<bool> TryBeginAsync(Guid messageId, string consumer, CancellationToken cancellationToken);

    /// <summary>Apaga registros processados antes de <paramref name="processedBefore"/> (no máximo <paramref name="maxRows"/>).</summary>
    /// <param name="processedBefore">Limite da retenção.</param>
    /// <param name="maxRows">Máximo de linhas.</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Quantos foram apagados.</returns>
    Task<int> PurgeAsync(DateTimeOffset processedBefore, int maxRows, CancellationToken cancellationToken);
}

/// <summary>Opções da limpeza do Inbox (<c>builder.AddInboxCleanup(o =&gt; ...)</c>).</summary>
public sealed class InboxOptions
{
    /// <summary>Tamanho máximo do nome do consumidor.</summary>
    public const int MaxConsumerLength = 100;

    /// <summary>
    /// Retenção dos registros (1 dia a 365 dias). Padrão 30 dias. Deve ser maior que o tempo máximo de reentrega
    /// (filas de espera, DLQ reprocessada): uma mensagem reentregue depois da limpeza seria processada de novo.
    /// </summary>
    public TimeSpan Retention { get; set; } = TimeSpan.FromDays(30);

    /// <summary>Intervalo da limpeza (1 min a 1 dia). Padrão 1 h.</summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);
}

internal sealed class InboxOptionsValidator : IValidateOptions<InboxOptions>
{
    public ValidateOptionsResult Validate(string? name, InboxOptions options)
    {
        var errors = new List<string>();
        OutboxOptionsValidator.Check(errors, OutboxOptionsValidator.InRange(options.Retention, TimeSpan.FromDays(1), TimeSpan.FromDays(365)), "Retention deve estar entre 1 e 365 dias.");
        OutboxOptionsValidator.Check(errors, OutboxOptionsValidator.InRange(options.CleanupInterval, TimeSpan.FromMinutes(1), TimeSpan.FromDays(1)), "CleanupInterval deve estar entre 1 min e 1 dia.");
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }
}

/// <summary>Limpeza periódica do Inbox.</summary>
internal sealed partial class InboxCleanupService(
    IServiceScopeFactory scopes, IOptions<InboxOptions> options, TimeProvider time, ILogger<InboxCleanupService> logger) : BackgroundService
{
    private const int PurgeBatch = 5000;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var scope = scopes.CreateAsyncScope();
                await using (scope.ConfigureAwait(false))
                {
                    var store = scope.ServiceProvider.GetRequiredService<IInboxStore>();
                    var removed = await store.PurgeAsync(time.GetUtcNow() - settings.Retention, PurgeBatch, stoppingToken).ConfigureAwait(false);
                    if (removed > 0)
                        LogPurged(removed);
                }

                await Task.Delay(settings.CleanupInterval, time, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogFailed(ex);
                try
                {
                    await Task.Delay(settings.CleanupInterval, time, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    [LoggerMessage(EventId = 4101, Level = LogLevel.Information, Message = "Limpeza do Inbox: {Removed} registro(s) removido(s)")]
    private partial void LogPurged(int removed);

    [LoggerMessage(EventId = 4102, Level = LogLevel.Error, Message = "Falha na limpeza do Inbox")]
    private partial void LogFailed(Exception exception);
}
