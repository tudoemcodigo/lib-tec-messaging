using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TEC.Messaging.AzureServiceBus.Consumers;
using TEC.Messaging.AzureServiceBus.Internal;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Transport;

namespace TEC.Messaging.AzureServiceBus.DependencyInjection;

/// <summary>Extensões para registro do transporte Azure Service Bus.</summary>
public static class MessagingBuilderAzureServiceBusExtensions
{
    /// <summary>
    /// Registra o Azure Service Bus como transporte do TEC.Messaging.
    /// </summary>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <param name="configure">Configuração de opções.</param>
    /// <param name="configuration">Seção de configuração opcional.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder UseAzureServiceBus(
        this MessagingBuilder builder, Action<AzureServiceBusOptions>? configure = null, IConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        var options = services.AddOptions<AzureServiceBusOptions>();
        if (configuration is not null)
            options.Bind(configuration);
        if (configure is not null)
            options.Configure(configure);

        options.ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<AzureServiceBusOptions>, AzureServiceBusOptionsValidator>());

        services.TryAddSingleton<AzureServiceBusConnection>();
        services.TryAddSingleton<AzureServiceBusPublisher>();
        services.TryAddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<AzureServiceBusPublisher>());
        services.TryAddSingleton(sp => new AzureServiceBusConsumerDependencies(
            sp.GetRequiredService<AzureServiceBusConnection>(),
            sp.GetRequiredService<ILoggerFactory>()));

        return builder;
    }

    /// <summary>Alias para <see cref="AddAzureServiceBusConsumer{TConsumer}"/>.</summary>
    public static MessagingBuilder AddConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(this MessagingBuilder builder)
        where TConsumer : AzureServiceBusConsumer
        => builder.AddAzureServiceBusConsumer<TConsumer>();

    /// <summary>
    /// Registra um consumidor do Azure Service Bus no container como serviço hospedado.
    /// </summary>
    /// <typeparam name="TConsumer">Tipo do consumidor.</typeparam>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder AddAzureServiceBusConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(this MessagingBuilder builder)
        where TConsumer : AzureServiceBusConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        services.TryAddSingleton<TConsumer>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<TConsumer>());
        return builder;
    }

    /// <summary>Adiciona health check do Azure Service Bus.</summary>
    public static IHealthChecksBuilder AddTecAzureServiceBus(
        this IHealthChecksBuilder builder, string name = "azure-servicebus", IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<AzureServiceBusHealthCheck>(name, HealthStatus.Unhealthy, tags ?? ["ready"]);
    }
}

internal sealed class AzureServiceBusHealthCheck(AzureServiceBusConnection connection, IOptions<AzureServiceBusOptions> options) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!options.Value.Enabled)
            return HealthCheckResult.Healthy("Transporte Azure Service Bus desabilitado.");

        try
        {
            var client = await connection.GetClientAsync(cancellationToken).ConfigureAwait(false);
            return client.IsClosed ? HealthCheckResult.Unhealthy("Cliente Service Bus fechado.") : HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Azure Service Bus indisponível.", ex);
        }
    }
}
