using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;

namespace TEC.Messaging.Contracts;

/// <summary>Catálogo dos contratos de integração conhecidos: nome + versão ↔ tipo CLR.</summary>
public interface IMessageTypeRegistry
{
    /// <summary>Todos os contratos registrados.</summary>
    IReadOnlyCollection<MessageTypeInfo> Types { get; }

    /// <summary>Procura o contrato pelo nome e versão do envelope.</summary>
    /// <param name="name">Nome do contrato.</param>
    /// <param name="version">Versão.</param>
    /// <param name="info">Contrato encontrado.</param>
    /// <returns><c>true</c> se registrado.</returns>
    bool TryGet(string name, int version, [NotNullWhen(true)] out MessageTypeInfo? info);

    /// <summary>Procura o contrato pelo tipo CLR.</summary>
    /// <param name="clrType">Tipo do evento.</param>
    /// <param name="info">Contrato encontrado.</param>
    /// <returns><c>true</c> se registrado.</returns>
    bool TryGet(Type clrType, [NotNullWhen(true)] out MessageTypeInfo? info);
}

/// <summary>Catálogo imutável, montado uma vez a partir dos registros do <c>MessagingBuilder</c>.</summary>
internal sealed class MessageTypeRegistry : IMessageTypeRegistry
{
    private readonly FrozenDictionary<(string Name, int Version), MessageTypeInfo> _byName;
    private readonly FrozenDictionary<Type, MessageTypeInfo> _byType;

    public MessageTypeRegistry(IEnumerable<MessageTypeInfo> types)
    {
        var byName = new Dictionary<(string, int), MessageTypeInfo>();
        var byType = new Dictionary<Type, MessageTypeInfo>();
        foreach (var info in types)
        {
            if (byType.TryGetValue(info.ClrType, out var existing))
            {
                if (existing.Name == info.Name && existing.Version == info.Version)
                    continue;
                throw new InvalidOperationException($"O tipo {info.ClrType.FullName} foi registrado como '{existing.Name}' v{existing.Version} e '{info.Name}' v{info.Version}.");
            }

            if (!byName.TryAdd((info.Name, info.Version), info))
                throw new InvalidOperationException($"O contrato '{info.Name}' v{info.Version} foi registrado para dois tipos: {byName[(info.Name, info.Version)].ClrType.FullName} e {info.ClrType.FullName}.");
            byType.Add(info.ClrType, info);
        }

        _byName = byName.ToFrozenDictionary();
        _byType = byType.ToFrozenDictionary();
        Types = [.. _byType.Values];
    }

    public IReadOnlyCollection<MessageTypeInfo> Types { get; }

    public bool TryGet(string name, int version, [NotNullWhen(true)] out MessageTypeInfo? info) =>
        _byName.TryGetValue((name, version), out info);

    public bool TryGet(Type clrType, [NotNullWhen(true)] out MessageTypeInfo? info) => _byType.TryGetValue(clrType, out info);

    /// <summary>Valida o nome do contrato: minúsculas, dígitos, '.' e '-', sem separadores nas pontas nem repetidos.</summary>
    internal static bool IsValidName(string? name)
    {
        if (string.IsNullOrEmpty(name) || name.Length > MessageEnvelope.MaxTypeLength)
            return false;
        var previousSeparator = true;
        foreach (var c in name)
        {
            var separator = c is '.' or '-';
            if (separator)
            {
                if (previousSeparator)
                    return false;
            }
            else if (c is not ((>= 'a' and <= 'z') or (>= '0' and <= '9')))
            {
                return false;
            }

            previousSeparator = separator;
        }

        return !previousSeparator;
    }
}
