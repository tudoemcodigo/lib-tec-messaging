namespace TEC.Messaging.Internal;

/// <summary>
/// UUID versão 7 (RFC 9562): 48 bits de milissegundos Unix + 74 bits aleatórios. Ordenável pelo tempo, o que mantém os
/// índices do Outbox compactos. Usa o <c>Guid.CreateVersion7</c> da BCL; instantes anteriores à época Unix (que a BCL
/// recusa) viram a própria época.
/// </summary>
internal static class GuidV7
{
    public static Guid Create(DateTimeOffset timestamp) =>
        Guid.CreateVersion7(timestamp < DateTimeOffset.UnixEpoch ? DateTimeOffset.UnixEpoch : timestamp);
}
