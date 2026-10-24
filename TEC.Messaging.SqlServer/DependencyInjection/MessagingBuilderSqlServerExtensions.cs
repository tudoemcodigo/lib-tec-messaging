using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using TEC.Messaging.DependencyInjection;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Inbox;
using TEC.Messaging.Outbox;
using TEC.Messaging.SqlServer.Interception;
using TEC.Messaging.SqlServer.Stores;

namespace TEC.Messaging.SqlServer.DependencyInjection;

/// <summary>Registro do armazenamento SQL Server do TEC.Messaging.</summary>
public static class MessagingBuilderSqlServerExtensions
{
    /// <summary>
    /// Usa o contexto <typeparamref name="TContext"/> (SQL Server) como armazenamento do Outbox e do Inbox: registra
    /// <see cref="IOutboxStore"/>, <see cref="IOutboxAdministration"/>, <see cref="IOutbox"/>, <see cref="IInboxStore"/> e o
    /// <see cref="TecMessagingSaveChangesInterceptor"/>. Ligue o interceptor ao contexto (<see cref="UseTecMessaging"/>) e
    /// chame <see cref="ModelBuilderExtensions.AddTecMessaging"/> no modelo.
    /// </summary>
    /// <typeparam name="TContext">Contexto da aplicação.</typeparam>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder UseSqlServer<TContext>(this MessagingBuilder builder) where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;
        services.TryAddScoped<SqlServerOutboxStore<TContext>>();
        services.TryAddScoped<IOutboxStore>(sp => sp.GetRequiredService<SqlServerOutboxStore<TContext>>());
        services.TryAddScoped<IOutboxAdministration>(sp => sp.GetRequiredService<SqlServerOutboxStore<TContext>>());
        services.TryAddScoped<IOutbox, SqlServerOutbox<TContext>>();
        services.TryAddScoped<IInboxStore, SqlServerInboxStore<TContext>>();
        services.TryAddSingleton(sp => new TecMessagingSaveChangesInterceptor(
            sp.GetRequiredService<IntegrationEnvelopeFactory>(), sp.GetServices<IDomainEventsSavingObserver>(),
            sp.GetRequiredService<MessagingMetrics>(), sp.GetRequiredService<TimeProvider>()));
        return builder;
    }

    /// <summary>Registra um observador dos eventos de domínio no <c>SaveChanges</c> (singleton).</summary>
    /// <typeparam name="TObserver">Observador.</typeparam>
    /// <param name="builder">Builder do TEC.Messaging.</param>
    /// <returns>O builder.</returns>
    public static MessagingBuilder AddDomainEventsObserver<TObserver>(this MessagingBuilder builder)
        where TObserver : class, IDomainEventsSavingObserver
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IDomainEventsSavingObserver, TObserver>());
        return builder;
    }

    /// <summary>Liga o interceptor do Outbox ao contexto (no <c>AddDbContext</c> ou no <c>OnConfiguring</c>).</summary>
    /// <param name="optionsBuilder">Opções do contexto.</param>
    /// <param name="services">Container (com <see cref="UseSqlServer{TContext}"/> registrado).</param>
    /// <returns>As opções.</returns>
    /// <example><code>services.AddDbContext&lt;VendasDbContext&gt;((sp, o) => o.UseSqlServer(conexao).UseTecMessaging(sp));</code></example>
    public static DbContextOptionsBuilder UseTecMessaging(this DbContextOptionsBuilder optionsBuilder, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(services);
        var interceptor = services.GetService<TecMessagingSaveChangesInterceptor>()
            ?? throw new InvalidOperationException("Registre o armazenamento com services.AddTecMessaging().UseSqlServer<TContext>().");
        return optionsBuilder.AddInterceptors(interceptor);
    }
}
