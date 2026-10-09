using TEC.Core.Domain;

namespace TEC.Messaging.Mapping;

/// <summary>
/// Converte eventos de domínio em eventos de integração (contratos públicos e versionados). O Outbox chama todos os
/// mapeadores registrados para cada evento de domínio coletado no commit; cada um devolve zero ou mais eventos.
/// </summary>
/// <remarks>
/// Implementações devem ser <b>sem estado e thread-safe</b> (singleton) e rápidas: rodam dentro do <c>SaveChanges</c>.
/// </remarks>
public interface IIntegrationEventMapper
{
    /// <summary>Eventos de integração gerados pelo evento de domínio (vazio quando não se aplica).</summary>
    /// <param name="domainEvent">Evento de domínio coletado.</param>
    /// <returns>Eventos de integração, cada um de um tipo registrado no catálogo.</returns>
    IEnumerable<object> Map(IDomainEvent domainEvent);
}

/// <summary>Base tipada: só recebe eventos do tipo <typeparamref name="TDomainEvent"/> (e derivados).</summary>
/// <typeparam name="TDomainEvent">Evento de domínio tratado.</typeparam>
/// <example>
/// <code>
/// internal sealed class PedidoMapper : IntegrationEventMapper&lt;PedidoAprovado&gt;
/// {
///     protected override IEnumerable&lt;object&gt; Map(PedidoAprovado e) { yield return new PedidoAprovadoV1(e.PedidoId, e.Total); }
/// }
/// </code>
/// </example>
public abstract class IntegrationEventMapper<TDomainEvent> : IIntegrationEventMapper
    where TDomainEvent : IDomainEvent
{
    IEnumerable<object> IIntegrationEventMapper.Map(IDomainEvent domainEvent) =>
        domainEvent is TDomainEvent typed ? Map(typed) : [];

    /// <summary>Eventos de integração gerados pelo evento de domínio.</summary>
    /// <param name="domainEvent">Evento de domínio.</param>
    /// <returns>Zero ou mais eventos de integração.</returns>
    protected abstract IEnumerable<object> Map(TDomainEvent domainEvent);
}

/// <summary>Mapeador a partir de uma função (<c>builder.Map&lt;TDomainEvent&gt;(...)</c>).</summary>
internal sealed class DelegateIntegrationEventMapper<TDomainEvent>(Func<TDomainEvent, object?> map) : IntegrationEventMapper<TDomainEvent>
    where TDomainEvent : IDomainEvent
{
    protected override IEnumerable<object> Map(TDomainEvent domainEvent) =>
        map(domainEvent) is { } integrationEvent ? [integrationEvent] : [];
}
