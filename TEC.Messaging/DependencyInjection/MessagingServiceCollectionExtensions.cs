using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using TEC.Messaging.Contracts;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Outbox;

namespace TEC.Messaging.DependencyInjection;

/// <summary>Registro do TEC.Messaging no container.</summary>
public static class MessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registra o núcleo do TEC.Messaging: catálogo de contratos, fábrica de envelopes, métricas e opções (validadas na
    /// subida). Chamar de novo é idempotente e devolve um builder sobre os mesmos registros.
    /// </summary>
    /// <param name="services">Container.</param>
    /// <param name="configure">Opções gerais.</param>
    /// <returns>Builder para contratos, mapeadores, armazenamento e transporte.</returns>
    /// <example>
    /// <code>
    /// services.AddTecMessaging()
    ///     .AddEventTypesFromAssembly(typeof(PedidoAprovadoV1).Assembly)
    ///     .Map&lt;PedidoAprovado&gt;(e => new PedidoAprovadoV1(e.PedidoId, e.Total))
    ///     .UseSqlServer&lt;VendasDbContext&gt;()
    ///     .UseRabbitMq(o => o.Exchange = "vendas.eventos")
    ///     .AddOutboxRelay();
    /// </code>
    /// </example>
    public static MessagingBuilder AddTecMessaging(this IServiceCollection services, Action<MessagingOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = services.AddOptions<MessagingOptions>();
        if (configure is not null)
            options.Configure(configure);

        var existing = services.FirstOrDefault(d => d.ServiceType == typeof(MessageTypeRegistrations))?.ImplementationInstance as MessageTypeRegistrations;
        if (existing is not null)
            return new MessagingBuilder(services, existing);

        var registrations = new MessageTypeRegistrations();
        services.AddSingleton(registrations);
        options.ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<MessagingOptions>, MessagingOptionsValidator>());
        services.TryAddSingleton(TimeProvider.System);
        services.AddMetrics();
        services.TryAddSingleton<MessagingMetrics>();
        services.TryAddSingleton<IMessageTypeRegistry>(sp =>
            sp.GetRequiredService<MessageTypeRegistrations>().Build(sp.GetRequiredService<IOptions<MessagingOptions>>().Value.JsonSerializerOptions));
        services.TryAddSingleton<IntegrationEnvelopeFactory>();
        return new MessagingBuilder(services, registrations);
    }
}
