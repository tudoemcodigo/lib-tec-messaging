using System.Diagnostics;
using System.Globalization;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TEC.Messaging.AzureServiceBus.Internal;
using TEC.Messaging.Context;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;

namespace TEC.Messaging.AzureServiceBus.Consumers;

/// <summary>Dependências comuns dos consumidores do Azure Service Bus.</summary>
public sealed class AzureServiceBusConsumerDependencies
{
    internal AzureServiceBusConsumerDependencies(
        AzureServiceBusConnection connection, ILoggerFactory loggers, MessagingMetrics metrics, TimeProvider time, int maxPayloadLength)
    {
        Connection = connection;
        Loggers = loggers;
        Metrics = metrics;
        Time = time;
        MaxPayloadLength = maxPayloadLength;
    }

    internal AzureServiceBusConnection Connection { get; }

    internal ILoggerFactory Loggers { get; }

    internal MessagingMetrics Metrics { get; }

    internal TimeProvider Time { get; }

    /// <summary>Payload máximo aceito no consumo (<see cref="MessagingOptions.MaxPayloadLength"/>).</summary>
    internal int MaxPayloadLength { get; }
}

/// <summary>Definição do consumidor do Azure Service Bus (Fila ou Tópico + Assinatura).</summary>
/// <param name="TopicOrQueue">Fila, ou tópico quando <paramref name="Subscription"/> é informada.</param>
/// <param name="Subscription">Assinatura do tópico; <c>null</c> para fila.</param>
/// <param name="MaxConcurrentCalls">Mensagens processadas em paralelo (1 a 1000).</param>
/// <param name="AutoRenewLockDuration">Renovação automática do lock enquanto o handler roda; <c>null</c> = padrão do SDK (5 min).</param>
/// <param name="MaxAttempts">
/// Tentativas antes da DLQ (1 a 100). Contadas na propriedade <c>tec-attempt</c> da mensagem, que acompanha o reenvio agendado
/// (o <c>DeliveryCount</c> do broker recomeça a cada reenvio).
/// </param>
public sealed record ServiceBusConsumerDefinition(
    string TopicOrQueue,
    string? Subscription = null,
    int MaxConcurrentCalls = 10,
    TimeSpan? AutoRenewLockDuration = null,
    int MaxAttempts = 5)
{
    /// <summary>Destino nas métricas e nos spans (<c>fila</c> ou <c>tópico/assinatura</c>).</summary>
    internal string Destination => Subscription is null ? TopicOrQueue : $"{TopicOrQueue}/{Subscription}";

    /// <summary>Recusa definição incoerente na subida do consumidor.</summary>
    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(TopicOrQueue) || TopicOrQueue.Length > 260)
            throw new InvalidOperationException("ServiceBusConsumerDefinition: TopicOrQueue é obrigatório (até 260 caracteres).");
        if (Subscription is { } sub && (string.IsNullOrWhiteSpace(sub) || sub.Length > 50))
            throw new InvalidOperationException("ServiceBusConsumerDefinition: Subscription tem de 1 a 50 caracteres.");
        if (MaxConcurrentCalls is < 1 or > 1000)
            throw new InvalidOperationException("ServiceBusConsumerDefinition: MaxConcurrentCalls deve estar entre 1 e 1000.");
        if (MaxAttempts is < 1 or > 100)
            throw new InvalidOperationException("ServiceBusConsumerDefinition: MaxAttempts deve estar entre 1 e 100.");
        if (AutoRenewLockDuration is { } renew && (renew < TimeSpan.Zero || renew > TimeSpan.FromDays(1)))
            throw new InvalidOperationException("ServiceBusConsumerDefinition: AutoRenewLockDuration deve estar entre 0 e 1 dia.");
    }
}

/// <summary>
/// Base para consumidores de mensagens no Azure Service Bus via <see cref="ServiceBusProcessor"/>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description>O envelope é validado (<see cref="MessageEnvelope.Validate"/> com <see cref="MessagingOptions.MaxPayloadLength"/>):
/// mensagem inválida, sem <c>MessageId</c> ou com payload acima do limite vai direto para a DLQ, sem chegar ao handler.</description></item>
/// <item><description>Em fila, <see cref="ConsumeAction.Retry"/> e <see cref="ConsumeAction.Requeue"/> agendam uma cópia na mesma
/// fila (<c>ScheduledEnqueueTime</c>) com espera crescente e só então concluem a original. Ver
/// <see cref="RetryDelay"/> e a documentação do pacote para o trade-off de duplicidade.</description></item>
/// <item><description>Em assinatura de tópico, a cópia agendada iria para o tópico e chegaria a todas as assinaturas: a espera
/// acontece no próprio handler (com o lock renovado) e a mensagem volta com <c>Abandon</c>.</description></item>
/// <item><description>Credencial recusada no processamento derruba o processador, descarta o cliente e reabre com o segredo relido.</description></item>
/// </list>
/// </remarks>
public abstract partial class AzureServiceBusConsumer : BackgroundService
{
    /// <summary>Tentativas já consumidas (propriedade da mensagem).</summary>
    internal const string AttemptProperty = "tec-attempt";

    /// <summary>Reenvios agendados da mensagem (retry e requeue), para dar a cada cópia um <c>MessageId</c> próprio.</summary>
    internal const string RescheduleProperty = "tec-reschedule";

    /// <summary>Identificador do envelope na cópia agendada (o <c>MessageId</c> da cópia é outro).</summary>
    internal const string EnvelopeIdProperty = "tec-message-id";

    /// <summary>Último erro registrado.</summary>
    internal const string ErrorProperty = "tec-error";

    private static readonly TimeSpan BaseRetryDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan SettlementTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan LockMargin = TimeSpan.FromSeconds(30);

    private readonly AzureServiceBusConsumerDependencies _deps;
    private readonly ILogger _logger;
    private TaskCompletionSource? _credentialFailure;

    /// <summary>Cria o consumidor do Azure Service Bus.</summary>
    protected AzureServiceBusConsumer(AzureServiceBusConsumerDependencies dependencies)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        _deps = dependencies;
        _logger = dependencies.Loggers.CreateLogger(GetType());
    }

    /// <summary>Configuração do destino (tópico + assinatura ou fila) e concorrência.</summary>
    protected abstract ServiceBusConsumerDefinition Definition { get; }

    /// <summary>Executa a lógica de consumo da mensagem. Exceção não tratada = <see cref="ConsumeAction.Retry"/>.</summary>
    protected abstract Task<ConsumeResult> HandleAsync(ReceivedMessage message, CancellationToken cancellationToken);

    /// <summary>
    /// Espera antes da tentativa seguinte à falha número <paramref name="attempt"/>: 5 s, 10 s, 20 s... até 15 min.
    /// </summary>
    internal static TimeSpan RetryDelay(int attempt) =>
        TimeSpan.FromTicks(Math.Min(BaseRetryDelay.Ticks * (1L << Math.Clamp(attempt - 1, 0, 20)), MaxRetryDelay.Ticks));

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var def = Definition;
        def.Validate();
        var failures = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            ServiceBusClient? client = null;
            ServiceBusProcessor? processor = null;
            var invalidate = false;
            try
            {
                client = await _deps.Connection.GetClientAsync(stoppingToken).ConfigureAwait(false);

                var options = new ServiceBusProcessorOptions
                {
                    AutoCompleteMessages = false,
                    MaxConcurrentCalls = def.MaxConcurrentCalls,
                };
                if (def.AutoRenewLockDuration is { } lockDuration)
                    options.MaxAutoLockRenewalDuration = lockDuration;

                processor = def.Subscription is not null
                    ? client.CreateProcessor(def.TopicOrQueue, def.Subscription, options)
                    : client.CreateProcessor(def.TopicOrQueue, options);

                var credentialFailure = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                Volatile.Write(ref _credentialFailure, credentialFailure);
                processor.ProcessMessageAsync += ProcessMessageAsync;
                processor.ProcessErrorAsync += ProcessErrorAsync;

                await processor.StartProcessingAsync(stoppingToken).ConfigureAwait(false);
                failures = 0;
                LogStarted(def.Destination);

                // Processa até o desligamento ou até o Service Bus recusar a credencial (rotação)
                await credentialFailure.Task.WaitAsync(stoppingToken).ConfigureAwait(false);
                invalidate = true;
                LogCredentialRejected(def.Destination);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                invalidate = ex is UnauthorizedAccessException;
                LogStartFailed(def.Destination, SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
            }
            finally
            {
                // Sempre descartado: um processador criado e não iniciado (falha no Start) também segura links
                if (processor is not null)
                    await StopProcessorAsync(processor).ConfigureAwait(false);
            }

            if (invalidate && client is not null)
                await _deps.Connection.InvalidateAsync(client).ConfigureAwait(false);

            var delay = TimeSpan.FromSeconds(Math.Min(60, 5 * Math.Pow(2, Math.Min(failures++, 4))));
            try
            {
                await Task.Delay(delay, _deps.Time, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task StopProcessorAsync(ServiceBusProcessor processor)
    {
        try
        {
            if (processor.IsProcessing)
                await processor.StopProcessingAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogStopFailed(Definition.Destination, SensitiveDataSanitizer.Sanitize(ex.Message));
        }
        finally
        {
            processor.ProcessMessageAsync -= ProcessMessageAsync;
            processor.ProcessErrorAsync -= ProcessErrorAsync;
            try
            {
                await processor.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Links já fechados.
            }
        }
    }

    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        if (args.CancellationToken.IsCancellationRequested)
            return;

        var def = Definition;
        var started = _deps.Time.GetTimestamp();
        var raw = args.Message;
        var failures = PreviousFailures(raw);

        var envelope = ReadEnvelope(raw, _deps.MaxPayloadLength, out var invalidReason);
        if (envelope is null)
        {
            LogInvalidMessage(def.Destination, Limit(raw.MessageId), invalidReason);
            await SettleAsync(args, raw, null, ConsumeResult.Reject(invalidReason ?? "Mensagem inválida."), failures, started, invalid: true).ConfigureAwait(false);
            return;
        }

        var traceParent = envelope.TraceParent;
        using var activity = MessagingDiagnostics.StartProcess(envelope, traceParent, "azure-servicebus", def.Destination);
        using var context = MessageContext.Begin(envelope.CorrelationId ?? activity?.TraceId.ToHexString(), envelope.MessageId.ToString());

        ConsumeResult result;
        try
        {
            result = await HandleAsync(new ReceivedMessage(envelope, failures, raw.DeliveryCount > 1 || failures > 0), args.CancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (args.CancellationToken.IsCancellationRequested)
        {
            // Desligamento: sem settle, o lock vence e a mensagem volta
            activity?.SetStatus(ActivityStatusCode.Error, "Processo encerrando.");
            return;
        }
        catch (Exception ex)
        {
            result = ConsumeResult.Retry(SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
        }

        if (args.CancellationToken.IsCancellationRequested)
            return;

        if (result.Action != ConsumeAction.Complete)
            activity?.SetStatus(ActivityStatusCode.Error, SensitiveDataSanitizer.Sanitize(result.Reason));

        await SettleAsync(args, raw, envelope, result, failures, started, invalid: false).ConfigureAwait(false);
    }

    private async Task SettleAsync(
        ProcessMessageEventArgs args, ServiceBusReceivedMessage raw, MessageEnvelope? envelope, ConsumeResult result, int failures, long started, bool invalid)
    {
        var def = Definition;
        var action = result.Action;
        var attempt = result.Attempt ?? failures + 1;
        if (action == ConsumeAction.Retry && attempt >= def.MaxAttempts)
            action = ConsumeAction.Reject;
        var reason = Truncate(SensitiveDataSanitizer.Sanitize(result.Reason));
        var messageId = Limit(raw.MessageId);

        try
        {
            switch (action)
            {
                case ConsumeAction.Complete:
                    await args.CompleteMessageAsync(raw, args.CancellationToken).ConfigureAwait(false);
                    break;

                case ConsumeAction.Reject:
                    await args.DeadLetterMessageAsync(raw, invalid ? "InvalidMessage" : "ProcessingRejected", reason, args.CancellationToken).ConfigureAwait(false);
                    LogDeadLettered(def.Destination, messageId, reason);
                    break;

                case ConsumeAction.Retry:
                    await RescheduleAsync(args, raw, envelope!, attempt, RetryDelay(attempt), reason).ConfigureAwait(false);
                    LogRetry(def.Destination, messageId, attempt, RetryDelay(attempt), reason);
                    break;

                default:
                    // Requeue: volta depois da primeira espera, sem consumir tentativa
                    await RescheduleAsync(args, raw, envelope!, failures, RetryDelay(1), reason).ConfigureAwait(false);
                    LogRequeued(def.Destination, messageId, RetryDelay(1), reason);
                    break;
            }

            _deps.Metrics.Consumed(def.Destination, action switch
            {
                ConsumeAction.Complete => ConsumeOutcomeTag.Completed,
                ConsumeAction.Retry => ConsumeOutcomeTag.Retried,
                ConsumeAction.Requeue => ConsumeOutcomeTag.Requeued,
                _ => ConsumeOutcomeTag.Rejected,
            }, _deps.Time.GetElapsedTime(started));
        }
        catch (OperationCanceledException) when (args.CancellationToken.IsCancellationRequested)
        {
            // Desligamento durante o settle: o lock vence e a mensagem volta
        }
        catch (Exception ex)
        {
            // Sem settle, o lock vence e o Service Bus reentrega: nada se perde (no máximo uma cópia agendada duplicada)
            LogSettleFailed(def.Destination, messageId, SensitiveDataSanitizer.Sanitize($"{ex.GetType().Name}: {ex.Message}"));
        }
    }

    /// <summary>
    /// Nova tentativa com espera, sem <c>Abandon</c> imediato. Fila: agenda uma cópia na própria fila e conclui a original.
    /// Assinatura: espera no handler (lock renovado pelo processador) e devolve com <c>Abandon</c>.
    /// </summary>
    /// <remarks>
    /// Agendar e concluir não são atômicos (transação do Service Bus exigiria camada Standard/Premium e o mesmo link). A ordem é
    /// agendar primeiro: se a conclusão falhar com o lock perdido (a original com certeza volta), a cópia é cancelada; em
    /// qualquer outra falha a cópia fica, e a mensagem pode ser processada duas vezes. Duplicidade é preferível a perda; o
    /// Inbox do consumidor absorve a repetição. A cópia leva <c>MessageId</c> próprio (<c>{id}:{reenvio}</c>) para não ser
    /// descartada pela detecção de duplicatas da fila; o id do envelope segue em <see cref="EnvelopeIdProperty"/>.
    /// </remarks>
    private async Task RescheduleAsync(
        ProcessMessageEventArgs args, ServiceBusReceivedMessage raw, MessageEnvelope envelope, int attempt, TimeSpan delay, string reason)
    {
        var def = Definition;
        var ct = args.CancellationToken;
        if (def.Subscription is not null)
        {
            var budget = (def.AutoRenewLockDuration ?? TimeSpan.FromMinutes(5)) - LockMargin;
            var wait = delay < budget ? delay : budget;
            if (wait > TimeSpan.Zero)
                await Task.Delay(wait, _deps.Time, ct).ConfigureAwait(false);

            await args.AbandonMessageAsync(raw, new Dictionary<string, object>(StringComparer.Ordinal)
            {
                [AttemptProperty] = attempt,
                [ErrorProperty] = reason,
            }, ct).ConfigureAwait(false);
            return;
        }

        var reschedules = ReadInt(raw.ApplicationProperties, RescheduleProperty) + 1;
        var copy = new ServiceBusMessage(raw)
        {
            MessageId = string.Create(CultureInfo.InvariantCulture, $"{envelope.MessageId}:{reschedules}"),
        };
        copy.ApplicationProperties[AttemptProperty] = attempt;
        copy.ApplicationProperties[RescheduleProperty] = reschedules;
        copy.ApplicationProperties[EnvelopeIdProperty] = envelope.MessageId.ToString();
        copy.ApplicationProperties[ErrorProperty] = reason;

        var sender = await _deps.Connection.GetSenderAsync(def.TopicOrQueue, ct).ConfigureAwait(false);
        var sequence = await sender.ScheduleMessageAsync(copy, _deps.Time.GetUtcNow() + delay, ct).ConfigureAwait(false);
        try
        {
            await args.CompleteMessageAsync(raw, ct).ConfigureAwait(false);
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageLockLost)
        {
            // A original com certeza volta: a cópia sobraria como duplicata
            try
            {
                using var cancel = new CancellationTokenSource(SettlementTimeout);
                await sender.CancelScheduledMessageAsync(sequence, cancel.Token).ConfigureAwait(false);
            }
            catch (Exception cancelFailure)
            {
                LogCancelScheduledFailed(def.Destination, Limit(raw.MessageId), SensitiveDataSanitizer.Sanitize(cancelFailure.Message));
            }

            throw;
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        var safeMsg = SensitiveDataSanitizer.Sanitize(args.Exception?.Message);
        LogProcessorError(args.EntityPath, args.ErrorSource, safeMsg);

        // Credencial recusada (rotação, SAS vencida): o laço principal derruba o processador e recria o cliente
        if (args.Exception is UnauthorizedAccessException)
            Volatile.Read(ref _credentialFailure)?.TrySetResult();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Falhas anteriores: a propriedade <see cref="AttemptProperty"/> (que sobrevive ao reenvio agendado) ou as entregas
    /// devolvidas desta cópia (lock perdido, <c>Abandon</c>), o que for maior.
    /// </summary>
    internal static int PreviousFailures(ServiceBusReceivedMessage raw) =>
        Math.Max(ReadInt(raw.ApplicationProperties, AttemptProperty), Math.Max(0, raw.DeliveryCount - 1));

    /// <summary>
    /// Monta e valida o envelope; <c>null</c> com o motivo quando a mensagem não tem identificador (vazio nunca vira
    /// <see cref="Guid.Empty"/>), tem tipo, versão ou ids fora dos limites, ou payload acima de <paramref name="maxPayloadLength"/>.
    /// </summary>
    internal static MessageEnvelope? ReadEnvelope(ServiceBusReceivedMessage raw, int maxPayloadLength, out string? error)
    {
        var props = raw.ApplicationProperties;
        var rawId = Text(props, EnvelopeIdProperty) ?? raw.MessageId;
        if (string.IsNullOrWhiteSpace(rawId))
        {
            error = "Mensagem sem MessageId.";
            return null;
        }

        // Bytes antes da decodificação (até 3 bytes UTF-8 por caractere): um corpo gigante não chega a virar string
        var body = raw.Body.ToMemory();
        if (body.Length > (long)maxPayloadLength * 3)
        {
            error = $"Payload maior que {maxPayloadLength} caracteres.";
            return null;
        }

        // MessageId que não é GUID (produtor de fora do TEC.Messaging) vira um GUID determinístico; GUID vazio é recusado pela validação
        var messageId = Guid.TryParse(rawId, out var parsedId) ? parsedId : DeterministicGuid.Create(rawId);
        var type = raw.Subject ?? Text(props, "type") ?? "";
        var version = int.TryParse(Text(props, "version"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var ver) ? ver : 1;
        var occurredAt = ParseInstant(Text(props, "occurred-at")) ?? ParseInstant(Text(props, "occurredAt")) ?? raw.EnqueuedTime.ToUniversalTime();

        var envelope = new MessageEnvelope(
            messageId, type, version, occurredAt, raw.CorrelationId, Text(props, "causation-id"), Text(props, "traceparent"), Encoding.UTF8.GetString(body.Span));

        var valid = envelope.Validate(maxPayloadLength);
        if (valid.IsFailure)
        {
            error = valid.Error!.Message;
            return null;
        }

        error = null;
        return envelope;
    }

    private static DateTimeOffset? ParseInstant(string? value) =>
        DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
            ? parsed
            : null;

    private static string? Text(IReadOnlyDictionary<string, object> props, string name) =>
        props.TryGetValue(name, out var value) && value is not null ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;

    private static int ReadInt(IReadOnlyDictionary<string, object> props, string name) =>
        props.TryGetValue(name, out var value) ? value switch
        {
            int i => Math.Max(0, i),
            long l => (int)Math.Clamp(l, 0, int.MaxValue),
            string s when int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) => Math.Max(0, n),
            _ => 0,
        } : 0;

    private static string Truncate(string value) => value.Length > 1000 ? value[..1000] : value;

    private static string? Limit(string? value) => value is { Length: > 64 } ? value[..64] : value;

    [LoggerMessage(EventId = 4601, Level = LogLevel.Information, Message = "Consumindo {Destination} no Azure Service Bus")]
    private partial void LogStarted(string destination);

    [LoggerMessage(EventId = 4602, Level = LogLevel.Warning, Message = "Falha ao iniciar o processador do Azure Service Bus em {Destination}: {Error}")]
    private partial void LogStartFailed(string destination, string error);

    [LoggerMessage(EventId = 4603, Level = LogLevel.Warning, Message = "Credencial recusada pelo Azure Service Bus em {Destination}: processador reiniciado com o segredo relido")]
    private partial void LogCredentialRejected(string destination);

    [LoggerMessage(EventId = 4604, Level = LogLevel.Error, Message = "Erro no processador do Azure Service Bus em {EntityPath} ({Source}): {Error}")]
    private partial void LogProcessorError(string entityPath, ServiceBusErrorSource source, string error);

    [LoggerMessage(EventId = 4605, Level = LogLevel.Warning, Message = "Mensagem {MessageId} inválida em {Destination}: {Error}")]
    private partial void LogInvalidMessage(string destination, string? messageId, string? error);

    [LoggerMessage(EventId = 4606, Level = LogLevel.Warning, Message = "Mensagem {MessageId} de {Destination} em espera: tentativa {Attempt}, nova em {Delay} ({Reason})")]
    private partial void LogRetry(string destination, string? messageId, int attempt, TimeSpan delay, string reason);

    [LoggerMessage(EventId = 4607, Level = LogLevel.Warning, Message = "Mensagem {MessageId} de {Destination} devolvida sem consumir tentativa; volta em {Delay} ({Reason})")]
    private partial void LogRequeued(string destination, string? messageId, TimeSpan delay, string reason);

    [LoggerMessage(EventId = 4608, Level = LogLevel.Warning, Message = "Mensagem {MessageId} de {Destination} enviada para a DLQ: {Reason}")]
    private partial void LogDeadLettered(string destination, string? messageId, string reason);

    [LoggerMessage(EventId = 4609, Level = LogLevel.Error, Message = "Falha ao concluir a mensagem {MessageId} de {Destination}; o Service Bus a reentrega quando o lock vencer: {Error}")]
    private partial void LogSettleFailed(string destination, string? messageId, string error);

    [LoggerMessage(EventId = 4610, Level = LogLevel.Warning, Message = "Cópia agendada da mensagem {MessageId} de {Destination} não foi cancelada; pode haver uma entrega duplicada: {Error}")]
    private partial void LogCancelScheduledFailed(string destination, string? messageId, string error);

    [LoggerMessage(EventId = 4611, Level = LogLevel.Warning, Message = "Falha ao parar o processador do Azure Service Bus em {Destination}: {Error}")]
    private partial void LogStopFailed(string destination, string error);
}
