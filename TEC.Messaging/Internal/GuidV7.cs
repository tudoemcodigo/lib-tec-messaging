using System.Buffers.Binary;
using System.Security.Cryptography;

namespace TEC.Messaging.Internal;

/// <summary>
/// UUID versão 7 (RFC 9562): 48 bits de milissegundos Unix + 74 bits aleatórios. Ordenável pelo tempo, o que mantém os
/// índices do Outbox compactos. Equivale ao <c>Guid.CreateVersion7</c> do .NET 9+, também no net8.0.
/// </summary>
internal static class GuidV7
{
    public static Guid Create(DateTimeOffset timestamp)
    {
        Span<byte> bytes = stackalloc byte[16];
        RandomNumberGenerator.Fill(bytes);

        var milliseconds = Math.Max(0, timestamp.ToUnixTimeMilliseconds());
        Span<byte> time = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(time, milliseconds);
        time[2..].CopyTo(bytes);

        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x70);   // versão 7
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);   // variante RFC 4122
        return new Guid(bytes, bigEndian: true);
    }
}
