using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Inbox;
using TEC.Messaging.Outbox;
using TEC.Messaging.PostgreSql.Interception;
using TEC.Messaging.PostgreSql.Stores;

namespace TEC.Messaging.PostgreSql.DependencyInjection;

/// <summary>Registro do armazenamento Outbox/Inbox no PostgreSQL.</summary>
public static class MessagingBuilderPostgreSqlExtensions
{
    /// <summary>
    /// Usa o PostgreSQL como armazenamento do Outbox e do Inbox no contexto da aplicação (<typeparamref name="TContext"/>).
    /// </summary>
    /// <typeparam name="TContext">Contexto com <see cref="ModelBuilderExtensions.AddTecMessagingPostgreSql"/> no modelo.</typeparam>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder UsePostgreSql<TContext>(this MessagingBuilder builder)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        services.TryAddScoped<PostgreSqlOutboxStore<TContext>>();
        services.TryAddScoped<IOutboxStore>(sp => sp.GetRequiredService<PostgreSqlOutboxStore<TContext>>());
        services.TryAddScoped<IOutboxAdministration>(sp => sp.GetRequiredService<PostgreSqlOutboxStore<TContext>>());
        services.TryAddScoped<IInboxStore, PostgreSqlInboxStore<TContext>>();
        services.TryAddScoped<IOutbox, PostgreSqlOutbox<TContext>>();

        services.TryAddSingleton(sp => new TecMessagingPostgreSqlSaveChangesInterceptor(
            sp.GetRequiredService<IntegrationEnvelopeFactory>(),
            sp.GetServices<IPostgreSqlDomainEventsSavingObserver>(),
            sp.GetRequiredService<MessagingMetrics>(),
            sp.GetRequiredService<TimeProvider>()));
        return builder;
    }

    /// <summary>
    /// Liga o interceptor do TEC.Messaging ao contexto no PostgreSQL: grava os eventos de domínio dos agregados rastreados
    /// no Outbox, na mesma transação do <c>SaveChangesAsync</c>.
    /// </summary>
    /// <param name="optionsBuilder">Opções do contexto.</param>
    /// <param name="serviceProvider">Container de serviços.</param>
    /// <returns>O builder de opções.</returns>
    public static DbContextOptionsBuilder UseTecMessagingPostgreSql(this DbContextOptionsBuilder optionsBuilder, IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(serviceProvider);
        return optionsBuilder.AddInterceptors(serviceProvider.GetRequiredService<TecMessagingPostgreSqlSaveChangesInterceptor>());
    }
}
