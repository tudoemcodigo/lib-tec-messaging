using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Outbox;
using TEC.Messaging.Tests.Shared;
using TEC.Messaging.Transport;

namespace TEC.Messaging.LoadTests;

/// <summary>Categorias dos testes de carga.</summary>
internal static class LoadCategories
{
    public const string LoadCi = "Carga-CI";
    public const string Heavy = "Carga-Pesada";
}

/// <summary>Vários relays concorrentes sobre o mesmo Outbox: cada mensagem é publicada uma única vez.</summary>
[NotInParallel]
public class RelayLoadTests
{
    private static int Volume(int padrao) =>
        int.TryParse(Environment.GetEnvironmentVariable("TEC_CARGA_MENSAGENS"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) && n > 0 ? n : padrao;

    [Test]
    [Category(LoadCategories.LoadCi)]
    public Task Four_relays_publish_each_message_once() => RunAsync(Volume(5_000), relays: 4);

    [Test]
    [Category(LoadCategories.Heavy)]
    [Explicit]
    public Task Sixteen_relays_under_volume() => RunAsync(Volume(200_000), relays: 16);

    private static async Task RunAsync(int messages, int relays)
    {
        var time = TimeProvider.System;
        var store = new InMemoryOutboxStore(time);
        for (var i = 0; i < messages; i++)
            store.Add(Envelopes.Create(at: DateTimeOffset.UtcNow.AddSeconds(-1)));
        var publisher = new FakePublisher();

        var services = new ServiceCollection();
        services.AddTecMessaging();
        services.AddSingleton<IOutboxStore>(store);
        services.AddSingleton<IMessagePublisher>(publisher);
        await using var sp = services.BuildServiceProvider();
        var options = new OutboxOptions { BatchSize = 200 };

        var watch = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, relays).Select(_ => Task.Run(async () =>
        {
            using var relay = new OutboxRelay(sp.GetRequiredService<IServiceScopeFactory>(), Options.Create(options), sp.GetRequiredService<MessagingMetrics>(),
                time, NullLogger<OutboxRelay>.Instance);
            while ((await relay.RunCycleAsync(options, CancellationToken.None)).Leased > 0)
            {
            }
        })));
        watch.Stop();

        await Assert.That(publisher.Published.Count).IsEqualTo(messages);
        await Assert.That(publisher.Published.Select(e => e.MessageId).Distinct().Count()).IsEqualTo(messages);
        await Report($"relay-{relays}", $"| Relays | Mensagens | Tempo | Vazão |\n|---|---|---|---|\n| {relays} | {messages} | {watch.Elapsed.TotalSeconds:0.00} s | {messages / watch.Elapsed.TotalSeconds:0} msg/s |\n");
    }

    private static async Task Report(string name, string markdown)
    {
        if (Environment.GetEnvironmentVariable("TEC_CARGA_RELATORIOS") is { Length: > 0 } folder)
        {
            Directory.CreateDirectory(folder);
            await File.WriteAllTextAsync(Path.Combine(folder, $"tec-messaging-{name}.md"), $"## TEC.Messaging: {name}\n\n{markdown}");
        }
    }
}
