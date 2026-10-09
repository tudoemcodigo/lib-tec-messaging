namespace TEC.Messaging.Contracts;

/// <summary>
/// Marca um tipo como contrato de evento de integração, com nome estável e versão. O nome é o que trafega no envelope
/// (<see cref="MessageEnvelope.Type"/>): renomear a classe ou as propriedades do domínio não muda o contrato.
/// </summary>
/// <example>
/// <code>
/// [IntegrationEvent("vendas.pedido-aprovado", Version = 1)]
/// public sealed record PedidoAprovadoV1(Guid PedidoId, decimal Total);
/// </code>
/// </example>
/// <param name="name">
/// Nome do contrato: minúsculas, dígitos, <c>.</c> e <c>-</c> (ex.: <c>vendas.pedido-aprovado</c>), até
/// <see cref="MessageEnvelope.MaxTypeLength"/> caracteres. Validado no registro.
/// </param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class IntegrationEventAttribute(string name) : Attribute
{
    /// <summary>Nome estável do contrato.</summary>
    public string Name { get; } = name;

    /// <summary>Versão do contrato (padrão 1). Mudança incompatível = novo tipo com versão maior.</summary>
    public int Version { get; init; } = 1;
}
