using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using TEC.Core.Domain;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Outbox;
using TEC.Messaging.PostgreSql.Entities;

namespace TEC.Messaging.PostgreSql.Interception;

/// <summary>
/// Observa os eventos de domínio coletados no <c>SaveChanges</c> do PostgreSQL, antes de virarem mensagens:
/// permite à aplicação gravar dados próprios na mesma transação.
/// </summary>
public interface IPostgreSqlDomainEventsSavingObserver
{
    /// <summary>Chamado uma vez por <c>SaveChanges</c> que tem eventos de domínio pendentes.</summary>
    void OnSaving(DbContext context, IReadOnlyList<IDomainEvent> domainEvents);
}

/// <summary>
/// Interceptor do Outbox transacional no PostgreSQL: no <c>SaveChanges</c>, coleta os eventos de domínio dos agregados
/// rastreados (<see cref="IHasDomainEvents"/>), aplica os mapeadores e grava as mensagens de integração no Outbox, na mesma transação.
/// </summary>
public sealed class TecMessagingPostgreSqlSaveChangesInterceptor : SaveChangesInterceptor
{
    private readonly IntegrationEnvelopeFactory _factory;
    private readonly IPostgreSqlDomainEventsSavingObserver[] _observers;
    private readonly MessagingMetrics _metrics;
    private readonly TimeProvider _time;
    private readonly ConditionalWeakTable<DbContext, PendingSave> _pending = new();

    internal TecMessagingPostgreSqlSaveChangesInterceptor(
        IntegrationEnvelopeFactory factory, IEnumerable<IPostgreSqlDomainEventsSavingObserver> observers, MessagingMetrics metrics, TimeProvider time)
    {
        _factory = factory;
        _observers = [.. observers];
        _metrics = metrics;
        _time = time;
    }

    /// <inheritdoc />
    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Collect(eventData.Context);
        return result;
    }

    /// <inheritdoc />
    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Collect(eventData.Context);
        return ValueTask.FromResult(result);
    }

    /// <inheritdoc />
    public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Commit(eventData.Context);
        return result;
    }

    /// <inheritdoc />
    public override ValueTask<int> SavedChangesAsync(SaveChangesCompletedEventData eventData, int result, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Commit(eventData.Context);
        return ValueTask.FromResult(result);
    }

    /// <inheritdoc />
    public override void SaveChangesFailed(DbContextErrorEventData eventData)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Rollback(eventData.Context);
    }

    /// <inheritdoc />
    public override Task SaveChangesFailedAsync(DbContextErrorEventData eventData, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Rollback(eventData.Context);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override void SaveChangesCanceled(DbContextEventData eventData)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Rollback(eventData.Context);
    }

    /// <inheritdoc />
    public override Task SaveChangesCanceledAsync(DbContextEventData eventData, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(eventData);
        Rollback(eventData.Context);
        return Task.CompletedTask;
    }

    private void Collect(DbContext? context)
    {
        if (context is null)
            return;

        Rollback(context);

        var enqueued = context.ChangeTracker.Entries<OutboxMessage>()
            .Where(e => e.State == EntityState.Added)
            .Select(e => e.Entity)
            .ToList();

        var sources = context.ChangeTracker.Entries<IHasDomainEvents>()
            .Select(e => e.Entity)
            .Where(e => e.DomainEvents.Count > 0)
            .ToList();
        if (sources.Count == 0)
        {
            if (enqueued.Count > 0)
                _pending.AddOrUpdate(context, new PendingSave([], [], [], enqueued));
            return;
        }

        var domainEvents = sources.SelectMany(s => s.DomainEvents).ToList();

        var observed = new List<object>();
        if (_observers.Length > 0)
        {
            var before = AddedEntities(context).ToHashSet(ReferenceEqualityComparer.Instance);
            foreach (var observer in _observers)
                observer.OnSaving(context, domainEvents);
            observed.AddRange(AddedEntities(context).Where(e => !before.Contains(e)));
        }

        var now = _time.GetUtcNow();
        var added = new List<OutboxMessage>();
        foreach (var domainEvent in domainEvents)
        {
            foreach (var envelope in _factory.FromDomainEvent(domainEvent))
            {
                var message = OutboxMessage.Create(envelope, now);
                context.Set<OutboxMessage>().Add(message);
                added.Add(message);
            }
        }

        _pending.AddOrUpdate(context, new PendingSave(sources, added, observed, enqueued));
    }

    private void Commit(DbContext? context)
    {
        if (context is null || !_pending.TryGetValue(context, out var pending))
            return;
        _pending.Remove(context);
        foreach (var source in pending.Sources)
            source.ClearDomainEvents();
        foreach (var message in pending.Messages.Concat(pending.Enqueued))
            _metrics.Enqueued(message.Type);
    }

    private void Rollback(DbContext? context)
    {
        if (context is null || !_pending.TryGetValue(context, out var pending))
            return;
        _pending.Remove(context);
        foreach (var entity in pending.Messages.Concat(pending.Observed))
        {
            var entry = context.Entry(entity);
            if (entry.State == EntityState.Added)
                entry.State = EntityState.Detached;
        }
    }

    private static IEnumerable<object> AddedEntities(DbContext context) =>
        context.ChangeTracker.Entries().Where(e => e.State == EntityState.Added).Select(e => e.Entity);

    private sealed record PendingSave(
        IReadOnlyList<IHasDomainEvents> Sources, IReadOnlyList<OutboxMessage> Messages, IReadOnlyList<object> Observed, IReadOnlyList<OutboxMessage> Enqueued);
}
