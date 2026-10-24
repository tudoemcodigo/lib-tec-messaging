using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using TEC.Core.Domain;
using TEC.Messaging.Context;
using TEC.Messaging.Contracts;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Internal;
using TEC.Messaging.Mapping;
using TEC.Messaging.Outbox;

namespace TEC.Messaging.Tests.Core;

[IntegrationEvent("vendas.pedido-aprovado", Version = 2)]
public sealed record PedidoAprovadoV2(Guid PedidoId, decimal Total, StatusDoPedido Status);

[IntegrationEvent("vendas.pedido-cancelado")]
public sealed record PedidoCanceladoV1(Guid PedidoId);

public enum StatusDoPedido
{
    Aprovado,
    Cancelado,
}

public sealed record SemAtributo(int Valor);

[IntegrationEvent("testes.texto")]
public sealed record TextoV1(string Texto);

public sealed record PedidoAprovado(Guid PedidoId, decimal Total, DateTimeOffset OccurredAt) : IDomainEvent;

public sealed record PedidoArquivado(Guid PedidoId, DateTimeOffset OccurredAt) : IDomainEvent;

[JsonSerializable(typeof(SemAtributo))]
internal sealed partial class ContratosJsonContext : JsonSerializerContext;

internal sealed class PedidoMapper : IntegrationEventMapper<PedidoAprovado>
{
    protected override IEnumerable<object> Map(PedidoAprovado domainEvent)
    {
        yield return new PedidoAprovadoV2(domainEvent.PedidoId, domainEvent.Total, StatusDoPedido.Aprovado);
    }
}

public class ContractsTests
{
    private static ServiceProvider Build(Action<MessagingBuilder> configure)
    {
        var services = new ServiceCollection();
        configure(services.AddTecMessaging());
        return services.BuildServiceProvider();
    }

    [Test]
    public async Task Registry_resolves_by_name_version_and_type()
    {
        await using var sp = Build(b => b.AddEventType<PedidoAprovadoV2>().AddEventType<PedidoCanceladoV1>());
        var registry = sp.GetRequiredService<IMessageTypeRegistry>();

        await Assert.That(registry.TryGet("vendas.pedido-aprovado", 2, out var info)).IsTrue();
        await Assert.That(info!.ClrType).IsEqualTo(typeof(PedidoAprovadoV2));
        await Assert.That(registry.TryGet("vendas.pedido-aprovado", 1, out _)).IsFalse();
        await Assert.That(registry.TryGet(typeof(PedidoCanceladoV1), out var cancelado)).IsTrue();
        await Assert.That(cancelado!.Version).IsEqualTo(1);
    }

    [Test]
    public async Task Assembly_scan_registers_annotated_types()
    {
        await using var sp = Build(b => b.AddEventTypesFromAssembly(typeof(ContractsTests).Assembly));
        var registry = sp.GetRequiredService<IMessageTypeRegistry>();

        await Assert.That(registry.TryGet(typeof(PedidoAprovadoV2), out _)).IsTrue();
        await Assert.That(registry.TryGet(typeof(SemAtributo), out _)).IsFalse();
    }

    [Test]
    public async Task Json_type_info_registration_is_used()
    {
        await using var sp = Build(b => b.AddEventType(ContratosJsonContext.Default.SemAtributo, "testes.sem-atributo", 3));
        var registry = sp.GetRequiredService<IMessageTypeRegistry>();

        await Assert.That(registry.TryGet("testes.sem-atributo", 3, out var info)).IsTrue();
        var json = info!.Serialize(new SemAtributo(7));
        await Assert.That(json).IsEqualTo("{\"Valor\":7}");
        await Assert.That(info.Deserialize(json)).IsEqualTo(new SemAtributo(7));
    }

    [Test]
    public async Task Default_json_uses_tec_core_conventions()
    {
        await using var sp = Build(b => b.AddEventType<PedidoAprovadoV2>());
        var info = sp.GetRequiredService<IMessageTypeRegistry>().Types.Single();
        var id = Guid.Parse("0190c6a2-0000-7000-8000-000000000001");

        var json = info.Serialize(new PedidoAprovadoV2(id, 10.5m, StatusDoPedido.Cancelado));

        await Assert.That(json).IsEqualTo($"{{\"pedidoId\":\"{id}\",\"total\":10.5,\"status\":\"cancelado\"}}");
        await Assert.That(() => info.Serialize(new PedidoCanceladoV1(id))).ThrowsExactly<ArgumentException>();
    }

    [Test]
    [Arguments("Vendas.Pedido")]
    [Arguments("vendas..pedido")]
    [Arguments(".vendas")]
    [Arguments("vendas-")]
    [Arguments("vendas pedido")]
    [Arguments("")]
    public async Task Invalid_contract_names_are_rejected(string name)
    {
        var builder = new ServiceCollection().AddTecMessaging();

        await Assert.That(() => builder.AddEventType(ContratosJsonContext.Default.SemAtributo, name)).ThrowsExactly<InvalidOperationException>();
    }

    [Test]
    public async Task Same_name_and_version_for_two_types_fails_at_resolution()
    {
        await using var sp = Build(b => b
            .AddEventType<PedidoCanceladoV1>()
            .AddEventType(ContratosJsonContext.Default.SemAtributo, "vendas.pedido-cancelado", 1));

        await Assert.That(() => sp.GetRequiredService<IMessageTypeRegistry>()).ThrowsExactly<InvalidOperationException>();
    }

    [Test]
    public async Task Type_without_attribute_needs_explicit_name()
    {
        var builder = new ServiceCollection().AddTecMessaging();

        await Assert.That(() => builder.AddEventType<SemAtributo>()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => builder.AddEventType(ContratosJsonContext.Default.SemAtributo)).ThrowsExactly<InvalidOperationException>();
    }

    [Test]
    public async Task Factory_maps_domain_events_and_stamps_context()
    {
        await using var sp = Build(b => b.AddEventType<PedidoAprovadoV2>().AddEventType<PedidoCanceladoV1>()
            .AddMapper<PedidoMapper>()
            .Map<PedidoAprovado>(e => e.Total > 100 ? new PedidoCanceladoV1(e.PedidoId) : null));
        var factory = sp.GetRequiredService<IntegrationEnvelopeFactory>();
        var at = new DateTimeOffset(2026, 10, 9, 12, 0, 0, TimeSpan.Zero);

        using (MessageContext.Begin("corr-1", "causa-1"))
        {
            var envelopes = factory.FromDomainEvent(new PedidoAprovado(Guid.NewGuid(), 50, at));
            await Assert.That(envelopes.Count).IsEqualTo(1);
            var envelope = envelopes[0];
            await Assert.That(envelope.Type).IsEqualTo("vendas.pedido-aprovado");
            await Assert.That(envelope.Version).IsEqualTo(2);
            await Assert.That(envelope.OccurredAt).IsEqualTo(at);
            await Assert.That(envelope.CorrelationId).IsEqualTo("corr-1");
            await Assert.That(envelope.CausationId).IsEqualTo("causa-1");
            await Assert.That(envelope.MessageId.ToString()[14]).IsEqualTo('7');

            await Assert.That(factory.FromDomainEvent(new PedidoAprovado(Guid.NewGuid(), 500, at)).Count).IsEqualTo(2);
            await Assert.That(factory.FromDomainEvent(new PedidoArquivado(Guid.NewGuid(), at)).Count).IsEqualTo(0);
        }
    }

    [Test]
    public async Task Factory_rejects_unregistered_types_and_oversized_payloads()
    {
        await using var sp = Build(b => b.AddEventType<PedidoCanceladoV1>());
        var factory = sp.GetRequiredService<IntegrationEnvelopeFactory>();

        await Assert.That(() => factory.Create(new SemAtributo(1), DateTimeOffset.UnixEpoch)).ThrowsExactly<InvalidOperationException>();

        var services = new ServiceCollection();
        services.AddTecMessaging(o => o.MaxPayloadLength = 1024).AddEventType<TextoV1>();
        await using var small = services.BuildServiceProvider();
        var smallFactory = small.GetRequiredService<IntegrationEnvelopeFactory>();

        await Assert.That(smallFactory.Create(new TextoV1("curto"), DateTimeOffset.UnixEpoch).Payload).IsEqualTo("{\"texto\":\"curto\"}");
        await Assert.That(() => smallFactory.Create(new TextoV1(new string('x', 2000)), DateTimeOffset.UnixEpoch)).ThrowsExactly<InvalidOperationException>();
    }

    [Test]
    public async Task Add_tec_messaging_is_idempotent()
    {
        var services = new ServiceCollection();
        services.AddTecMessaging().AddEventType<PedidoCanceladoV1>();
        services.AddTecMessaging().AddEventType<PedidoAprovadoV2>();
        await using var sp = services.BuildServiceProvider();

        await Assert.That(sp.GetRequiredService<IMessageTypeRegistry>().Types.Count).IsEqualTo(2);
        await Assert.That(services.Count(d => d.ServiceType == typeof(IMessageTypeRegistry))).IsEqualTo(1);
    }

    [Test]
    public async Task Envelope_validation_enforces_limits()
    {
        var valid = new MessageEnvelope(Guid.NewGuid(), "a.b", 1, DateTimeOffset.UnixEpoch, null, null, null, "{}");

        await Assert.That(valid.Validate(1024).IsSuccess).IsTrue();
        await Assert.That((valid with { MessageId = Guid.Empty }).Validate(1024).IsFailure).IsTrue();
        await Assert.That((valid with { Version = 0 }).Validate(1024).IsFailure).IsTrue();
        await Assert.That((valid with { Type = new string('a', 201) }).Validate(1024).IsFailure).IsTrue();
        await Assert.That((valid with { CorrelationId = new string('a', 129) }).Validate(1024).IsFailure).IsTrue();
        await Assert.That((valid with { Payload = new string('a', 1025) }).Validate(1024).Error!.Field).IsEqualTo("Payload");
    }

    [Test]
    public async Task Message_context_restores_previous_scope_and_ignores_invalid_values()
    {
        // Sem atividade: o fallback para o TraceId não interfere nas asserções
        System.Diagnostics.Activity.Current = null;
        using (MessageContext.Begin("externo"))
        {
            using (MessageContext.Begin("interno", "causa"))
            {
                await Assert.That(MessageContext.CorrelationId).IsEqualTo("interno");
                await Assert.That(MessageContext.CausationId).IsEqualTo("causa");
            }

            await Assert.That(MessageContext.CorrelationId).IsEqualTo("externo");
            await Assert.That(MessageContext.CausationId).IsNull();

            using (MessageContext.Begin("linha\nquebrada", new string('x', 500)))
            {
                await Assert.That(MessageContext.CorrelationId).IsNull();
                await Assert.That(MessageContext.CausationId).IsNull();
            }
        }
    }

    [Test]
    public async Task Message_context_falls_back_to_the_current_trace()
    {
        using var activity = new System.Diagnostics.Activity("teste").SetIdFormat(System.Diagnostics.ActivityIdFormat.W3C).Start();

        await Assert.That(MessageContext.CorrelationId).IsEqualTo(activity.TraceId.ToHexString());
        using (MessageContext.Begin("explicita"))
            await Assert.That(MessageContext.CorrelationId).IsEqualTo("explicita");
    }

    [Test]
    public async Task Guid_v7_has_version_and_variant_and_sorts_by_time()
    {
        var first = GuidV7.Create(DateTimeOffset.FromUnixTimeMilliseconds(1_000));
        var second = GuidV7.Create(DateTimeOffset.FromUnixTimeMilliseconds(2_000));
        var text = first.ToString();

        await Assert.That(text[14]).IsEqualTo('7');
        await Assert.That("89ab".Contains(text[19], StringComparison.Ordinal)).IsTrue();
        await Assert.That(string.CompareOrdinal(first.ToString(), second.ToString())).IsLessThan(0);
    }
}
