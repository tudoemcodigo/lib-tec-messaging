using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using TEC.Core.Domain;
using TEC.Messaging.Diagnostics;
using TEC.Messaging.Outbox;
using TEC.Messaging.SqlServer.Entities;

namespace TEC.Messaging.SqlServer.Interception;

/// <summary>
/// Observa os eventos de domínio coletados no <c>SaveChanges</c>, antes de virarem mensagens: permite à aplicação gravar
/// dados próprios na <b>mesma transação</b> (ex.: histórico imutável do agregado).
/// </summary>
/// <remarks>Registre como singleton (sem estado, thread-safe). Roda dentro do <c>SaveChanges</c>: mantenha-o rápido e síncrono.</remarks>
public interface IDomainEventsSavingObserver
{
    /// <summary>Chamado uma vez por <c>SaveChanges</c> que tem eventos de domínio pendentes.</summary>
    /// <param name="context">Contexto sendo salvo (use-o para adicionar entidades).</param>
    /// <param name="domainEvents">Eventos, na ordem dos agregados e da ocorrência.</param>
    void OnSaving(DbContext context, IReadOnlyList<IDomainEvent> domainEvents);
}

/// <summary>
/// Interceptor do Outbox transacional: no <c>SaveChanges</c>, coleta os eventos de domínio dos agregados rastreados
/// (<see cref="IHasDomainEvents"/>), aplica os mapeadores e grava as mensagens de integração no Outbox, na mesma
/// transação. Os eventos só são descartados dos agregados depois do commit; se o <c>SaveChanges</c> falhar, as linhas
/// do Outbox adicionadas são retiradas do rastreamento, e uma nova tentativa gera tudo de novo, sem duplicar.
/// </summary>
/// <remarks>
/// Ligue-o ao contexto com <c>optionsBuilder.UseTecMessaging(serviceProvider)</c> ou, quando o registro do contexto não
/// expõe as opções (ex.: <c>AddTecOrm</c>), no <c>OnConfiguring</c> do contexto, recebendo o interceptor pelo construtor.
/// </remarks>
public sealed class TecMessagingSaveChangesInterceptor : SaveChangesInterceptor
{
    private readonly IntegrationEnvelopeFactory _factory;
    private readonly IDomainEventsSavingObserver[] _observers;
    private readonly MessagingMetrics _metrics;
    private readonly TimeProvider _time;
    private readonly ConditionalWeakTable<DbContext, PendingSave> _pending = new();

    internal TecMessagingSaveChangesInterceptor(
        IntegrationEnvelopeFactory factory, IEnumerable<IDomainEventsSavingObserver> observers, MessagingMetrics metrics, TimeProvider time)
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

        // Tentativa anterior que falhou sem passar pelo SaveChangesFailed (ex.: exceção antes do banco): começa do zero.
        Rollback(context);

        var sources = context.ChangeTracker.Entries<IHasDomainEvents>()
            .Select(e => e.Entity)
            .Where(e => e.DomainEvents.Count > 0)
            .ToList();
        if (sources.Count == 0)
            return;

        var domainEvents = sources.SelectMany(s => s.DomainEvents).ToList();

        // O que os observadores adicionarem também é desfeito se o SaveChanges falhar (sem duplicar numa nova tentativa)
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

        _pending.AddOrUpdate(context, new PendingSave(sources, added, observed));
    }

    private void Commit(DbContext? context)
    {
        if (context is null || !_pending.TryGetValue(context, out var pending))
            return;
        _pending.Remove(context);
        foreach (var source in pending.Sources)
            source.ClearDomainEvents();
        foreach (var message in pending.Messages)
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

    private sealed record PendingSave(IReadOnlyList<IHasDomainEvents> Sources, IReadOnlyList<OutboxMessage> Messages, IReadOnlyList<object> Observed);
}
