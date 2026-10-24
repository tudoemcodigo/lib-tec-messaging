using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TEC.Messaging.Aws.Consumers;
using TEC.Messaging.Aws.Internal;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Aws.DependencyInjection;

/// <summary>Extensões para registro do transporte AWS (SQS e SNS).</summary>
public static class MessagingBuilderAwsExtensions
{
    /// <summary>
    /// Registra o AWS (SQS e SNS) como transporte do TEC.Messaging.
    /// </summary>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <param name="configure">Configuração de opções.</param>
    /// <param name="configuration">Seção de configuração opcional.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder UseAws(
        this MessagingBuilder builder, Action<AwsMessagingOptions>? configure = null, IConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        var options = services.AddOptions<AwsMessagingOptions>();
        if (configuration is not null)
            options.Bind(configuration);
        if (configure is not null)
            options.Configure(configure);

        options.ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<AwsMessagingOptions>, AwsMessagingOptionsValidator>());

        services.TryAddSingleton<AwsClientFactory>();
        services.TryAddSingleton<AwsMessagingPublisher>();
        services.TryAddSingleton<IMessagePublisher>(sp => sp.GetRequiredService<AwsMessagingPublisher>());
        services.TryAddSingleton(sp => new AwsSqsConsumerDependencies(
            sp.GetRequiredService<AwsClientFactory>(),
            sp.GetRequiredService<ILoggerFactory>()));

        return builder;
    }

    /// <summary>
    /// Registra um consumidor AWS SQS no container como serviço hospedado.
    /// </summary>
    /// <typeparam name="TConsumer">Tipo do consumidor.</typeparam>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder AddAwsSqsConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(this MessagingBuilder builder)
        where TConsumer : AwsSqsConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        services.TryAddSingleton<TConsumer>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<TConsumer>());
        return builder;
    }

    /// <summary>Adiciona health check do AWS SQS.</summary>
    public static IHealthChecksBuilder AddTecAws(
        this IHealthChecksBuilder builder, string name = "aws-messaging", IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<AwsMessagingHealthCheck>(name, HealthStatus.Unhealthy, tags ?? ["ready"]);
    }
}

internal sealed class AwsMessagingHealthCheck(AwsClientFactory factory, IOptions<AwsMessagingOptions> options) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!options.Value.Enabled)
            return HealthCheckResult.Healthy("Transporte AWS desabilitado.");

        try
        {
            var sqs = await factory.GetSqsAsync(cancellationToken).ConfigureAwait(false);
            return sqs is not null ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy("Cliente SQS não inicializado.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("AWS SQS indisponível.", ex);
        }
    }
}
