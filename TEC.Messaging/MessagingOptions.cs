using System.Text.Json;
using Microsoft.Extensions.Options;

namespace TEC.Messaging;

/// <summary>Opções gerais do TEC.Messaging (<c>services.AddTecMessaging(o =&gt; ...)</c>), validadas na subida.</summary>
public sealed class MessagingOptions
{
    /// <summary>Tamanho máximo do payload em caracteres (1 KiB a 16 MiB). Padrão 256 KiB: eventos devem ser "magros".</summary>
    public int MaxPayloadLength { get; set; } = 256 * 1024;

    /// <summary>
    /// Opções JSON dos contratos registrados por reflexão (<c>AddEventType&lt;T&gt;()</c>). <c>null</c> = as do TEC.Core
    /// (<c>JsonDefaults.Options</c>: camelCase, enums por nome, nulos omitidos). Contratos registrados com
    /// <c>JsonTypeInfo</c> (Native AOT) usam as próprias.
    /// </summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }
}

internal sealed class MessagingOptionsValidator : IValidateOptions<MessagingOptions>
{
    public ValidateOptionsResult Validate(string? name, MessagingOptions options) =>
        options.MaxPayloadLength is >= 1024 and <= 16 * 1024 * 1024
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail("MaxPayloadLength deve estar entre 1024 e 16777216 caracteres.");
}
