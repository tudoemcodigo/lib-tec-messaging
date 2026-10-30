using System.Diagnostics.CodeAnalysis;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
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
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Internal;
using TEC.Messaging.Transport;

namespace TEC.Messaging.Aws.DependencyInjection;

/// <summary>Extensões para registro do transporte AWS (SQS e SNS).</summary>
public static class MessagingBuilderAwsExtensions
{
    /// <summary>
    /// Registra o AWS (SQS e SNS) como transporte do TEC.Messaging. <see cref="AwsMessagingOptions.DefaultDestination"/> é
    /// obrigatório com o transporte habilitado (validado na subida).
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
            sp.GetRequiredService<ILoggerFactory>(),
            sp.GetRequiredService<MessagingMetrics>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<IOptions<MessagingOptions>>().Value.MaxPayloadLength,
            TransportSecurity.RequiresTls(sp.GetRequiredService<IOptions<AwsMessagingOptions>>().Value.RequireTls, sp.GetService<IHostEnvironment>())));

        return builder;
    }

    /// <summary>Alias para <see cref="UseAws"/>.</summary>
    public static MessagingBuilder UseAwsMessaging(
        this MessagingBuilder builder, Action<AwsMessagingOptions>? configure = null, IConfiguration? configuration = null)
        => builder.UseAws(configure, configuration);

    /// <summary>Alias para <see cref="AddAwsSqsConsumer{TConsumer}"/>.</summary>
    public static MessagingBuilder AddConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(this MessagingBuilder builder)
        where TConsumer : AwsSqsConsumer
        => builder.AddAwsSqsConsumer<TConsumer>();

    /// <summary>
    /// Registra um consumidor AWS SQS no container como singleton e serviço hospedado. Idempotente por tipo: registrar duas
    /// vezes não sobe dois laços de recebimento.
    /// </summary>
    /// <typeparam name="TConsumer">Tipo do consumidor.</typeparam>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder AddAwsSqsConsumer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TConsumer>(this MessagingBuilder builder)
        where TConsumer : AwsSqsConsumer
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        if (services.Any(d => d.ServiceType == typeof(AwsSqsConsumerRegistration<TConsumer>)))
            return builder;
        services.AddSingleton<AwsSqsConsumerRegistration<TConsumer>>();

        // Registro próprio do consumidor (ex.: fábrica com parâmetros) é respeitado
        services.TryAddSingleton<TConsumer>();
        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<TConsumer>());
        return builder;
    }

    /// <summary>
    /// Health check do AWS: lê um atributo do destino padrão (<c>GetQueueAttributes</c> da fila SQS ou <c>GetTopicAttributes</c>
    /// do tópico SNS), o que toca a rede e valida a credencial. Responde em até 5 s. Exige a permissão de leitura correspondente
    /// (<c>sqs:GetQueueAttributes</c> ou <c>sns:GetTopicAttributes</c>).
    /// </summary>
    public static IHealthChecksBuilder AddTecAws(
        this IHealthChecksBuilder builder, string name = "aws-messaging", IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<AwsMessagingHealthCheck>(name, HealthStatus.Unhealthy, tags ?? ["ready"]);
    }

    /// <summary>Alias para <see cref="AddTecAws"/>.</summary>
    public static IHealthChecksBuilder AddTecAwsMessaging(
        this IHealthChecksBuilder builder, string name = "aws-messaging", IEnumerable<string>? tags = null)
        => builder.AddTecAws(name, tags);
}

/// <summary>Marca de registro de um consumidor (torna <c>AddAwsSqsConsumer</c> idempotente).</summary>
internal sealed class AwsSqsConsumerRegistration<TConsumer>;

internal sealed class AwsMessagingHealthCheck(AwsClientFactory factory, IOptions<AwsMessagingOptions> options) : IHealthCheck
{
    internal static readonly TimeSpan CheckTimeout = TimeSpan.FromSeconds(5);

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var settings = options.Value;
        if (!settings.Enabled)
            return HealthCheckResult.Healthy("Transporte AWS desabilitado.");

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(CheckTimeout);
        object? client = null;
        try
        {
            var destination = settings.DefaultDestination;
            if (destination.StartsWith("arn:aws:sns:", StringComparison.OrdinalIgnoreCase))
            {
                var sns = await factory.GetSnsAsync(timeout.Token).ConfigureAwait(false);
                client = sns;
                await sns.GetTopicAttributesAsync(new GetTopicAttributesRequest { TopicArn = destination }, timeout.Token)
                    .WaitAsync(CheckTimeout, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var sqs = await factory.GetSqsAsync(timeout.Token).ConfigureAwait(false);
                client = sqs;
                await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest { QueueUrl = destination, AttributeNames = ["QueueArn"] }, timeout.Token)
                    .WaitAsync(CheckTimeout, cancellationToken).ConfigureAwait(false);
            }

            return HealthCheckResult.Healthy();
        }
        catch (Exception ex) when (AwsClientFactory.IsCredentialFailure(ex))
        {
            if (client is not null)
                await factory.InvalidateAsync(client).ConfigureAwait(false);
            return HealthCheckResult.Unhealthy("Credencial recusada pela AWS.", ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            return HealthCheckResult.Unhealthy(ex is OperationCanceledException or TimeoutException
                ? "AWS sem resposta em 5 s."
                : "AWS indisponível.", ex);
        }
    }
}
