using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Outbox;
using TEC.Messaging.Tests.Shared;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Tests.Outbox;

public class OutboxRelayTests
{
    private static readonly DateTimeOffset Start = new(2026, 10, 9, 12, 0, 0, TimeSpan.Zero);

    private sealed class Harness : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;

        public Harness(OutboxOptions? options = null, IMessagePublisher? publisher = null)
        {
            Time = new ManualTimeProvider(Start);
            Store = new InMemoryOutboxStore(Time);
            Options = options ?? new OutboxOptions();
            var services = new ServiceCollection();
            services.AddSingleton<TimeProvider>(Time);
            services.AddTecMessaging();
            services.AddSingleton<IOutboxStore>(Store);
            services.AddSingleton<IOutboxAdministration>(Store);
            services.AddSingleton(publisher ?? Publisher);
            _provider = services.BuildServiceProvider();
            Relay = new OutboxRelay(
                _provider.GetRequiredService<IServiceScopeFactory>(), Microsoft.Extensions.Options.Options.Create(Options),
                _provider.GetRequiredService<MessagingMetrics>(), Time, NullLogger<OutboxRelay>.Instance);
        }

        public ManualTimeProvider Time { get; }

        public InMemoryOutboxStore Store { get; }

        public FakePublisher Publisher { get; } = new();

        public OutboxOptions Options { get; }

        public OutboxRelay Relay { get; }

        public Task<RelayCycle> CycleAsync() => Relay.RunCycleAsync(Options, CancellationToken.None);

        public async ValueTask DisposeAsync()
        {
            Relay.Dispose();
            await _provider.DisposeAsync();
        }
    }

    [Test]
    public async Task Publishes_pending_messages_in_order_and_marks_them()
    {
        await using var h = new Harness();
        var second = Envelopes.Create(at: Start.AddSeconds(-1));
        var first = Envelopes.Create(at: Start.AddSeconds(-2));
        h.Store.Add(second);
        h.Store.Add(first);

        var cycle = await h.CycleAsync();

        await Assert.That(cycle.Published).IsEqualTo(2);
        await Assert.That(h.Publisher.Published.Select(e => e.MessageId)).IsEquivalentTo([first.MessageId, second.MessageId]);
        await Assert.That(h.Publisher.Published[0].MessageId).IsEqualTo(first.MessageId);
        await Assert.That(h.Store.Rows.All(r => r.Status == OutboxMessageStatus.Published && r.LeaseToken is null)).IsTrue();
        await Assert.That((await h.CycleAsync()).Leased).IsEqualTo(0);
    }

    [Test]
    public async Task A_failing_message_does_not_block_the_others()
    {
        await using var h = new Harness();
        var poison = Envelopes.Create("testes.veneno", Start.AddSeconds(-3));
        h.Store.Add(poison);
        h.Store.Add(Envelopes.Create(at: Start.AddSeconds(-2)));
        h.Store.Add(Envelopes.Create(at: Start.AddSeconds(-1)));
        h.Publisher.ShouldFail = e => e.Type == "testes.veneno";

        var cycle = await h.CycleAsync();

        await Assert.That(cycle.Published).IsEqualTo(2);
        await Assert.That(cycle.Paused).IsFalse();
        var row = h.Store.Rows.Single(r => r.Envelope.MessageId == poison.MessageId);
        await Assert.That(row.Status).IsEqualTo(OutboxMessageStatus.Pending);
        await Assert.That(row.Attempts).IsEqualTo(1);
        await Assert.That(row.NextAttemptAt).IsEqualTo(Start + h.Options.InitialRetryDelay);
        await Assert.That(row.LastError).Contains("Broker recusou");
    }

    [Test]
    public async Task Message_becomes_dead_after_max_attempts()
    {
        await using var h = new Harness(new OutboxOptions { MaxAttempts = 3, InitialRetryDelay = TimeSpan.FromSeconds(1) });
        h.Store.Add(Envelopes.Create("testes.veneno", Start.AddSeconds(-1)));
        h.Publisher.ShouldFail = _ => true;

        for (var i = 0; i < 3; i++)
        {
            await h.CycleAsync();
            h.Time.Advance(TimeSpan.FromMinutes(10));
        }

        var row = h.Store.Rows.Single();
        await Assert.That(row.Status).IsEqualTo(OutboxMessageStatus.Dead);
        await Assert.That(row.Attempts).IsEqualTo(3);
        await Assert.That((await h.CycleAsync()).Leased).IsEqualTo(0);
        await Assert.That(h.Publisher.Calls).IsEqualTo(3);
    }

    [Test]
    public async Task Consecutive_failures_release_the_rest_without_counting_attempts()
    {
        await using var h = new Harness(new OutboxOptions { ConsecutiveFailuresToPause = 2, PauseDuration = TimeSpan.FromSeconds(30) });
        for (var i = 0; i < 5; i++)
            h.Store.Add(Envelopes.Create(at: Start.AddSeconds(-10 + i)));
        h.Publisher.ShouldFail = _ => true;

        var cycle = await h.CycleAsync();

        await Assert.That(cycle.Paused).IsTrue();
        await Assert.That(h.Publisher.Calls).IsEqualTo(2);
        var rows = h.Store.Rows.OrderBy(r => r.Envelope.OccurredAt).ToList();
        await Assert.That(rows.Take(2).All(r => r.Attempts == 1)).IsTrue();
        await Assert.That(rows.Skip(2).All(r => r.Attempts == 0 && r.NextAttemptAt == Start.AddSeconds(30) && r.LeaseToken is null)).IsTrue();
    }

    [Test]
    public async Task Leased_messages_are_not_leased_again_until_the_lease_expires()
    {
        await using var h = new Harness();
        h.Store.Add(Envelopes.Create(at: Start.AddSeconds(-1)));

        var first = await h.Store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None);
        var second = await h.Store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None);
        h.Time.Advance(TimeSpan.FromMinutes(2));
        var third = await h.Store.LeaseAsync(10, TimeSpan.FromMinutes(1), CancellationToken.None);

        await Assert.That(first.Count).IsEqualTo(1);
        await Assert.That(second.Count).IsEqualTo(0);
        await Assert.That(third.Count).IsEqualTo(1);

        // Conclusão com a reserva vencida é ignorada (outro relay assumiu)
        await h.Store.CompleteAsync([OutboxOutcome.Published(first[0])], CancellationToken.None);
        await Assert.That(h.Store.Rows.Single().Status).IsEqualTo(OutboxMessageStatus.Pending);
    }

    [Test]
    public async Task Retry_delay_doubles_up_to_the_ceiling()
    {
        var options = new OutboxOptions { InitialRetryDelay = TimeSpan.FromSeconds(2), MaxRetryDelay = TimeSpan.FromSeconds(30) };

        await Assert.That(options.RetryDelay(1)).IsEqualTo(TimeSpan.FromSeconds(2));
        await Assert.That(options.RetryDelay(2)).IsEqualTo(TimeSpan.FromSeconds(4));
        await Assert.That(options.RetryDelay(4)).IsEqualTo(TimeSpan.FromSeconds(16));
        await Assert.That(options.RetryDelay(5)).IsEqualTo(TimeSpan.FromSeconds(30));
        await Assert.That(options.RetryDelay(1000)).IsEqualTo(TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task Invalid_options_fail_at_startup()
    {
        var services = new ServiceCollection();
        services.AddTecMessaging().AddOutboxRelay(o =>
        {
            o.BatchSize = 0;
            o.MaxRetryDelay = TimeSpan.FromMilliseconds(1);
        });
        await using var sp = services.BuildServiceProvider();

        var ex = await Assert.That(() => sp.GetRequiredService<IOptions<OutboxOptions>>().Value).ThrowsExactly<OptionsValidationException>();
        await Assert.That(ex!.Failures.Count()).IsEqualTo(2);
    }

    [Test]
    public async Task Statistics_feed_the_outbox_gauges()
    {
        await using var h = new Harness();
        h.Store.Add(Envelopes.Create(at: Start.AddMinutes(-5)));
        var stats = await h.Store.GetStatisticsAsync(CancellationToken.None);

        await Assert.That(stats.Pending).IsEqualTo(1);
        await Assert.That(stats.OldestPendingOccurredAt).IsEqualTo(Start.AddMinutes(-5));
    }

    [Test]
    public async Task Transport_unavailable_never_counts_attempts_nor_kills_messages()
    {
        // Regressão A1: com os padrões, ~14 min de broker fora levavam as mensagens mais antigas a Dead
        await using var h = new Harness();
        for (var i = 0; i < 5; i++)
            h.Store.Add(Envelopes.Create(at: Start.AddSeconds(-10 + i)));
        h.Publisher.ShouldFail = _ => true;
        h.Publisher.Failure = _ => new TransportDownException();

        for (var cycle = 0; cycle < 3 * h.Options.MaxAttempts; cycle++)
        {
            var result = await h.CycleAsync();
            await Assert.That(result.Paused).IsTrue();
            h.Time.Advance(h.Options.PauseDuration);
        }

        await Assert.That(h.Store.Rows.All(r => r.Status == OutboxMessageStatus.Pending && r.Attempts == 0 && r.LeaseToken is null)).IsTrue();
        await Assert.That(h.Publisher.Calls).IsEqualTo(3 * h.Options.MaxAttempts);

        // Broker de volta: tudo publicado
        h.Publisher.ShouldFail = _ => false;
        await Assert.That((await h.CycleAsync()).Published).IsEqualTo(5);
    }

    [Test]
    public async Task Failures_that_precede_transport_unavailable_are_released_without_counting()
    {
        await using var h = new Harness(new OutboxOptions { ConsecutiveFailuresToPause = 5 });
        var first = Envelopes.Create(at: Start.AddSeconds(-3));
        var second = Envelopes.Create(at: Start.AddSeconds(-2));
        var third = Envelopes.Create(at: Start.AddSeconds(-1));
        h.Store.Add(first);
        h.Store.Add(second);
        h.Store.Add(third);
        h.Publisher.ShouldFail = _ => true;

        // A queda começa com um erro genérico e depois se mostra como indisponibilidade (marca numa exceção interna)
        h.Publisher.Failure = e => e.MessageId == first.MessageId
            ? new IOException("Conexão reiniciada.")
            : new InvalidOperationException("Publicação falhou.", new TransportDownException());

        var cycle = await h.CycleAsync();

        await Assert.That(cycle.Paused).IsTrue();
        await Assert.That(h.Publisher.Calls).IsEqualTo(2);
        await Assert.That(h.Store.Rows.All(r => r.Attempts == 0 && r.NextAttemptAt == Start + h.Options.PauseDuration)).IsTrue();
    }

    [Test]
    public async Task Disabled_transport_keeps_messages_without_counting_attempts()
    {
        // Regressão A1: Enabled = false lançava InvalidOperationException e as mensagens chegavam a Dead
        await using var host = new RabbitMq.DisabledTransportHost();
        await using var h = new Harness(new OutboxOptions { MaxAttempts = 1 }, host.Publisher);
        h.Store.Add(Envelopes.Create(at: Start.AddSeconds(-1)));

        var cycle = await h.CycleAsync();

        await Assert.That(cycle.Paused).IsTrue();
        var row = h.Store.Rows.Single();
        await Assert.That(row.Status).IsEqualTo(OutboxMessageStatus.Pending);
        await Assert.That(row.Attempts).IsEqualTo(0);
    }

    [Test]
    public async Task Publication_is_cancelled_at_the_lease_deadline_and_released_without_counting()
    {
        // Regressão B1: publicação lenta passava do prazo da reserva e outro relay podia assumir a mensagem
        await using var h = new Harness(new OutboxOptions { LeaseDuration = TimeSpan.FromMilliseconds(500) });
        h.Store.Add(Envelopes.Create(at: Start.AddSeconds(-2)));
        h.Store.Add(Envelopes.Create(at: Start.AddSeconds(-1)));
        h.Publisher.Hang = true;

        var cycle = await h.CycleAsync().WaitAsync(TimeSpan.FromSeconds(10));

        await Assert.That(cycle.Published).IsEqualTo(0);
        await Assert.That(h.Publisher.Calls).IsEqualTo(1);
        await Assert.That(h.Store.Rows.All(r => r.Status == OutboxMessageStatus.Pending && r.Attempts == 0 && r.LeaseToken is null)).IsTrue();
    }

    [Test]
    public async Task Cleanup_purges_in_batches_until_the_backlog_is_gone()
    {
        // Regressão M1: uma limpeza apagava no máximo 5000 linhas por CleanupInterval
        await using var h = new Harness();
        for (var i = 0; i < 12_001; i++)
            h.Store.AddPublished(Envelopes.Create(at: Start.AddDays(-30)), Start.AddDays(-30));
        h.Store.AddPublished(Envelopes.Create(at: Start), Start);

        var removed = await h.Relay.CleanupAsync(h.Options, Start, CancellationToken.None);

        await Assert.That(removed).IsEqualTo(12_001);
        await Assert.That(h.Store.PurgeCalls).IsEqualTo(3);
        await Assert.That(h.Store.Rows.Count).IsEqualTo(1);
    }
}
