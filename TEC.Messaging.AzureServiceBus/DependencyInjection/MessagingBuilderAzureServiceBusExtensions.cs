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
using TEC.Messaging.Diagnostics;
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
            sp.GetRequiredService<ILoggerFactory>(),
            sp.GetRequiredService<MessagingMetrics>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<IOptions<MessagingOptions>>().Value.MaxPayloadLength));

        return builder;
    }

    /// <summary>Alias para <see cref="AddAzureServiceBusConsumer{TConsumer}"/>.</summary>
    public static MessagingBuilder AddConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(this MessagingBuilder builder)
        where TConsumer : AzureServiceBusConsumer
        => builder.AddAzureServiceBusConsumer<TConsumer>();

    /// <summary>
    /// Registra um consumidor do Azure Service Bus no container como singleton e serviço hospedado. Idempotente por tipo:
    /// registrar duas vezes não sobe dois processadores.
    /// </summary>
    /// <typeparam name="TConsumer">Tipo do consumidor.</typeparam>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder AddAzureServiceBusConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(this MessagingBuilder builder)
        where TConsumer : AzureServiceBusConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        if (services.Any(d => d.ServiceType == typeof(AzureServiceBusConsumerRegistration<TConsumer>)))
            return builder;
        services.AddSingleton<AzureServiceBusConsumerRegistration<TConsumer>>();

        // Registro próprio do consumidor (ex.: fábrica com parâmetros) é respeitado
        services.TryAddSingleton<TConsumer>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<TConsumer>());
        return builder;
    }

    /// <summary>
    /// Health check do Azure Service Bus: abre (ou reaproveita) o link de envio de <see cref="AzureServiceBusOptions.DefaultTopic"/>
    /// e pede um lote vazio, o que toca a rede e valida a credencial sem publicar nada. Responde em até 5 s.
    /// </summary>
    public static IHealthChecksBuilder AddTecAzureServiceBus(
        this IHealthChecksBuilder builder, string name = "azure-servicebus", IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<AzureServiceBusHealthCheck>(name, HealthStatus.Unhealthy, tags ?? ["ready"]);
    }
}

/// <summary>Marca de registro de um consumidor (torna <c>AddAzureServiceBusConsumer</c> idempotente).</summary>
internal sealed class AzureServiceBusConsumerRegistration<TConsumer>;

internal sealed class AzureServiceBusHealthCheck(AzureServiceBusConnection connection, IOptions<AzureServiceBusOptions> options) : IHealthCheck
{
    internal static readonly TimeSpan CheckTimeout = TimeSpan.FromSeconds(5);

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var settings = options.Value;
        if (!settings.Enabled)
            return HealthCheckResult.Healthy("Transporte Azure Service Bus desabilitado.");
        if (string.IsNullOrWhiteSpace(settings.DefaultTopic))
            return HealthCheckResult.Degraded("DefaultTopic vazio: sem entidade conhecida, a conectividade com o Azure Service Bus não foi testada.");

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(CheckTimeout);
        Azure.Messaging.ServiceBus.ServiceBusSender? sender = null;
        try
        {
            sender = await connection.GetSenderAsync(settings.DefaultTopic, timeout.Token).ConfigureAwait(false);

            // O lote depende do tamanho máximo negociado no link: criá-lo abre o link (rede + autorização de envio)
            using var batch = await sender.CreateMessageBatchAsync(timeout.Token).AsTask().WaitAsync(CheckTimeout, cancellationToken).ConfigureAwait(false);
            return HealthCheckResult.Healthy();
        }
        catch (UnauthorizedAccessException ex)
        {
            if (sender is not null)
                await connection.InvalidateAsync(sender).ConfigureAwait(false);
            return HealthCheckResult.Unhealthy("Credencial recusada pelo Azure Service Bus.", ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return HealthCheckResult.Unhealthy(ex is OperationCanceledException or TimeoutException
                ? "Azure Service Bus sem resposta em 5 s."
                : "Azure Service Bus indisponível.", ex);
        }
    }
}
