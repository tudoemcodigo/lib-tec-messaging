using System.Security.Cryptography;
using System.Text;

namespace TEC.Messaging.Internal;

internal static class DeterministicGuid
{
    public static Guid Create(string? input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return Guid.Empty;

        var bytes = Encoding.UTF8.GetBytes(input.Trim());
        var hash = SHA256.HashData(bytes);
        return new Guid(hash.AsSpan(0, 16));
    }
}
