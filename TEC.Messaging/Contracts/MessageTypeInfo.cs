using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace TEC.Messaging.Contracts;

/// <summary>Contrato registrado: nome e versão no fio, tipo CLR e a serialização JSON dele.</summary>
public sealed class MessageTypeInfo
{
    private readonly JsonTypeInfo _jsonTypeInfo;

    internal MessageTypeInfo(string name, int version, JsonTypeInfo jsonTypeInfo)
    {
        Name = name;
        Version = version;
        _jsonTypeInfo = jsonTypeInfo;
    }

    /// <summary>Nome estável do contrato (vai em <see cref="MessageEnvelope.Type"/>).</summary>
    public string Name { get; }

    /// <summary>Versão do contrato.</summary>
    public int Version { get; }

    /// <summary>Tipo CLR do contrato.</summary>
    public Type ClrType => _jsonTypeInfo.Type;

    /// <summary>Serializa um evento deste contrato.</summary>
    /// <param name="integrationEvent">Evento (do tipo <see cref="ClrType"/>).</param>
    /// <returns>JSON do payload.</returns>
    /// <exception cref="ArgumentException">O evento não é do tipo do contrato.</exception>
    public string Serialize(object integrationEvent)
    {
        ArgumentNullException.ThrowIfNull(integrationEvent);
        if (!ClrType.IsInstanceOfType(integrationEvent))
            throw new ArgumentException($"O evento não é do tipo {ClrType.Name} do contrato '{Name}'.", nameof(integrationEvent));
        return JsonSerializer.Serialize(integrationEvent, _jsonTypeInfo);
    }

    /// <summary>Desserializa o payload deste contrato.</summary>
    /// <param name="payload">JSON do payload.</param>
    /// <returns>O evento, ou <c>null</c> quando o JSON é <c>null</c>.</returns>
    /// <exception cref="JsonException">JSON inválido para o contrato.</exception>
    public object? Deserialize(string payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        return JsonSerializer.Deserialize(payload, _jsonTypeInfo);
    }

    /// <inheritdoc />
    public override string ToString() => $"{Name} v{Version} ({ClrType.Name})";
}
