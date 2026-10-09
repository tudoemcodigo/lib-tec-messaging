using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TEC.Messaging.Administration;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.RabbitMQ.Administration;
using TEC.Messaging.RabbitMQ.Consumers;
using TEC.Messaging.RabbitMQ.Internal;
using TEC.Messaging.Transport;

namespace TEC.Messaging.RabbitMQ.DependencyInjection;

/// <summary>Registro do transporte RabbitMQ do TEC.Messaging.</summary>
public static class MessagingBuilderRabbitMqExtensions
{
    /// <summary>
    /// Usa o RabbitMQ como transporte: publicador com confirmação (<see cref="IMessagePublisher"/>), administração das DLQs
    /// (<see cref="IDeadLetterAdministration"/>) e monitor da métrica das DLQs. A URI vem do segredo
    /// <see cref="RabbitMqOptions.ConnectionSecretName"/> do TEC.Vault (requer <c>AddTecVault</c>).
    /// </summary>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <param name="configure">Opções (aplicadas depois da configuração).</param>
    /// <param name="configuration">Seção de configuração (ex.: <c>Messaging:RabbitMQ</c>); <c>null</c> = só o código.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder UseRabbitMq(this MessagingBuilder builder, Action<RabbitMqOptions>? configure = null, IConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        var options = services.AddOptions<RabbitMqOptions>();
        if (configuration is not null)
            options.Bind(configuration);
        if (configure is not null)
            options.Configure(configure);
        options.ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<RabbitMqOptions>, RabbitMqOptionsValidator>());

        services.TryAddSingleton<RabbitMqConnection>();
        services.TryAddSingleton<RabbitMqPublisher>();
        services.TryAddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
        services.TryAddSingleton<IDeadLetterAdministration, RabbitMqDeadLetterAdministration>();
        services.TryAddSingleton(sp => new RabbitMqConsumerDependencies(
            sp.GetRequiredService<RabbitMqConnection>(), sp.GetRequiredService<RabbitMqPublisher>(), sp.GetRequiredService<IOptions<RabbitMqOptions>>(),
            sp.GetRequiredService<MessagingMetrics>(), sp.GetRequiredService<TimeProvider>(), sp.GetRequiredService<ILoggerFactory>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, DeadLetterMonitor>());
        return builder;
    }

    /// <summary>
    /// Adiciona um consumidor: singleton (para pausar/retomar de fora, ex.: circuit breaker) e serviço em segundo plano.
    /// Idempotente por tipo.
    /// </summary>
    /// <typeparam name="TConsumer">Consumidor.</typeparam>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder AddConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(this MessagingBuilder builder)
        where TConsumer : RabbitMqConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        if (services.Any(d => d.ServiceType == typeof(ConsumerRegistration<TConsumer>)))
            return builder;
        services.AddSingleton<ConsumerRegistration<TConsumer>>();

        // Registro próprio do consumidor (ex.: fábrica com parâmetros) é respeitado
        services.TryAddSingleton<TConsumer>();
        services.AddSingleton<RabbitMqConsumer>(sp => sp.GetRequiredService<TConsumer>());
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<TConsumer>());
        return builder;
    }

    /// <summary>Health check da conexão com o RabbitMQ (pronto quando a conexão abre em até 5 s).</summary>
    /// <param name="builder">Health checks.</param>
    /// <param name="name">Nome do check.</param>
    /// <param name="tags">Tags (padrão <c>ready</c>).</param>
    /// <returns>O builder.</returns>
    public static IHealthChecksBuilder AddTecRabbitMq(this IHealthChecksBuilder builder, string name = "rabbitmq", IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<RabbitMqHealthCheck>(name, HealthStatus.Unhealthy, tags ?? ["ready"]);
    }
}

/// <summary>Marca de registro de um consumidor (torna <c>AddConsumer</c> idempotente).</summary>
internal sealed class ConsumerRegistration<TConsumer>;

/// <summary>Pronto quando a conexão com o broker abre.</summary>
internal sealed class RabbitMqHealthCheck(RabbitMqConnection connection, IOptions<RabbitMqOptions> options) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!options.Value.Enabled)
            return HealthCheckResult.Healthy("Transporte RabbitMQ desligado.");
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            var open = await connection.GetAsync(timeout.Token).ConfigureAwait(false);
            return open.IsOpen ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy("Conexão com o RabbitMQ fechada.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("RabbitMQ indisponível.", ex);
        }
    }
}
