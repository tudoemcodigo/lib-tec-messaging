using System.Text.Json;
using Microsoft.Extensions.Options;

namespace TEC.Messaging;

/// <summary>Opções gerais do TEC.Messaging (<c>services.AddTecMessaging(o =&gt; ...)</c>), validadas na subida.</summary>
public sealed class MessagingOptions
{
    /// <summary>
    /// Tamanho máximo do payload em caracteres (1 KiB a 4 MiB). Padrão 256 KiB: eventos devem ser "magros". Vale na gravação
    /// (Outbox) e no consumo. O teto garante que o envelope (até 3 bytes por caractere em UTF-8, mais os metadados) caiba no
    /// <c>max_message_size</c> padrão do RabbitMQ (16 MiB); acima dele o broker recusaria a publicação.
    /// </summary>
    public int MaxPayloadLength { get; set; } = 256 * 1024;

    /// <summary>Maior <see cref="MaxPayloadLength"/> aceito.</summary>
    internal const int MaxPayloadLengthLimit = 4 * 1024 * 1024;

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
        options.MaxPayloadLength is >= 1024 and <= MessagingOptions.MaxPayloadLengthLimit
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail("MaxPayloadLength deve estar entre 1024 e 4194304 caracteres.");
}
