using Microsoft.Extensions.Configuration;
using TEC.Messaging.Outbox;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Tests.Shared;

/// <summary>Categorias dos testes (seleção no CI sempre por categoria).</summary>
internal static class TestCategories
{
    /// <summary>Dependências reais (SQL Server, RabbitMQ): job de integração, com os serviços em container.</summary>
    public const string Integration = "Integracao";
}

/// <summary>Relógio manual: o tempo só anda quando o teste manda.</summary>
internal sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan delta) => _now += delta;
}

/// <summary>
/// Configuração dos serviços de teste: variável de ambiente, user-secrets compartilhado (<c>tudoemcodigo-tec-testes</c>) ou
/// <c>appsettings.Local.json</c> da saída (seção <c>TecTestes</c>).
/// </summary>
internal static class TestSettings
{
    public const string SqlVariable = "TEC_TESTES_MESSAGING_SQL_CONEXAO";
    public const string RabbitVariable = "TEC_TESTES_MESSAGING_RABBITMQ_URI";

    private static readonly IConfiguration Configuration = new ConfigurationBuilder()
        .AddUserSecrets("tudoemcodigo-tec-testes")
        .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "appsettings.Local.json"), optional: true)
        .Build();

    public static string? SqlConnection => Read(SqlVariable, "MessagingSqlConexao");

    public static string? RabbitUri => Read(RabbitVariable, "MessagingRabbitUri");

    private static string? Read(string variable, string key) =>
        Environment.GetEnvironmentVariable(variable) is { Length: > 0 } value ? value : Configuration[$"TecTestes:{key}"] is { Length: > 0 } v ? v : null;
}

/// <summary>Outbox em memória com a mesma semântica de reserva do SQL Server (para os testes do relay).</summary>
internal sealed class InMemoryOutboxStore(TimeProvider time) : IOutboxStore, IOutboxAdministration
{
    private readonly Lock _sync = new();
    private readonly List<Row> _rows = [];

    public IReadOnlyList<Row> Rows
    {
        get
        {
            lock (_sync)
                return [.. _rows];
        }
    }

    public void Add(MessageEnvelope envelope)
    {
        lock (_sync)
            _rows.Add(new Row(envelope) { NextAttemptAt = time.GetUtcNow() });
    }

    public Task<IReadOnlyList<LeasedOutboxMessage>> LeaseAsync(int batchSize, TimeSpan leaseDuration, CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            var now = time.GetUtcNow();
            var token = Guid.NewGuid();
            var batch = _rows
                .Where(r => r.Status == OutboxMessageStatus.Pending && r.NextAttemptAt <= now && (r.LeasedUntil is null || r.LeasedUntil < now))
                .OrderBy(r => r.Envelope.OccurredAt)
                .Take(batchSize)
                .ToList();
            foreach (var row in batch)
            {
                row.LeaseToken = token;
                row.LeasedUntil = now + leaseDuration;
            }

            return Task.FromResult<IReadOnlyList<LeasedOutboxMessage>>([.. batch.Select(r => new LeasedOutboxMessage(r.Envelope, r.Attempts, token))]);
        }
    }

    public Task CompleteAsync(IReadOnlyList<OutboxOutcome> outcomes, CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            foreach (var outcome in outcomes)
            {
                var row = _rows.Single(r => r.Envelope.MessageId == outcome.MessageId);
                if (row.LeaseToken != outcome.LeaseToken)
                    continue;
                row.LeaseToken = null;
                row.LeasedUntil = null;
                switch (outcome.Kind)
                {
                    case OutboxOutcomeKind.Published:
                        row.Status = OutboxMessageStatus.Published;
                        row.PublishedAt = time.GetUtcNow();
                        break;
                    case OutboxOutcomeKind.Released:
                        row.NextAttemptAt = outcome.NextAttemptAt!.Value;
                        break;
                    default:
                        row.Status = outcome.Kind == OutboxOutcomeKind.Dead ? OutboxMessageStatus.Dead : OutboxMessageStatus.Pending;
                        row.Attempts = outcome.Attempts;
                        row.NextAttemptAt = outcome.NextAttemptAt ?? time.GetUtcNow();
                        row.LastError = outcome.Error;
                        break;
                }
            }
        }

        return Task.CompletedTask;
    }

    public Task<int> PurgePublishedAsync(DateTimeOffset publishedBefore, int maxRows, CancellationToken cancellationToken)
    {
        lock (_sync)
            return Task.FromResult(_rows.RemoveAll(r => r.Status == OutboxMessageStatus.Published && r.PublishedAt < publishedBefore));
    }

    public Task<OutboxStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            var pending = _rows.Where(r => r.Status == OutboxMessageStatus.Pending).ToList();
            return Task.FromResult(new OutboxStatistics(
                pending.Count, pending.Count(r => r.Attempts > 0), _rows.Count(r => r.Status == OutboxMessageStatus.Dead),
                _rows.Count(r => r.Status == OutboxMessageStatus.Published), pending.Count == 0 ? null : pending.Min(r => r.Envelope.OccurredAt)));
        }
    }

    public Task<IReadOnlyList<OutboxMessageInfo>> ListFailedAsync(int limit, CancellationToken cancellationToken) => throw new NotSupportedException();

    public Task<int> RequeueAsync(IReadOnlyCollection<Guid>? ids, CancellationToken cancellationToken) => throw new NotSupportedException();

    public Task<int> DiscardAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken) => throw new NotSupportedException();

    internal sealed class Row(MessageEnvelope envelope)
    {
        public MessageEnvelope Envelope { get; } = envelope;

        public OutboxMessageStatus Status { get; set; }

        public int Attempts { get; set; }

        public DateTimeOffset NextAttemptAt { get; set; }

        public Guid? LeaseToken { get; set; }

        public DateTimeOffset? LeasedUntil { get; set; }

        public DateTimeOffset? PublishedAt { get; set; }

        public string? LastError { get; set; }
    }
}

/// <summary>Publicador em memória: falha para os tipos (ou todas as mensagens) configurados.</summary>
internal sealed class FakePublisher : IMessagePublisher
{
    private readonly Lock _sync = new();
    private readonly List<MessageEnvelope> _published = [];

    public Func<MessageEnvelope, bool> ShouldFail { get; set; } = _ => false;

    public IReadOnlyList<MessageEnvelope> Published
    {
        get
        {
            lock (_sync)
                return [.. _published];
        }
    }

    public int Calls { get; private set; }

    public Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            Calls++;
            if (ShouldFail(envelope))
                throw new InvalidOperationException("Broker recusou a mensagem.");
            _published.Add(envelope);
        }

        return Task.CompletedTask;
    }
}

internal static class Envelopes
{
    public static MessageEnvelope Create(string type = "testes.algo-ocorreu", DateTimeOffset? at = null) =>
        new(Guid.NewGuid(), type, 1, at ?? DateTimeOffset.UnixEpoch, "corr", null, null, "{}");
}
