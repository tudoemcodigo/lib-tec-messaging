using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace TEC.Messaging.RabbitMQ.Internal;

/// <summary>
/// Formato no fio: corpo JSON com o envelope completo (payload embutido como objeto) e os metadados repetidos nas
/// propriedades AMQP e nos cabeçalhos, para roteamento, ferramentas e rastreio.
/// </summary>
internal static class AmqpEnvelope
{
    public const string ContentType = "application/json";
    public const string CausationHeader = "causation-id";
    public const string TraceParentHeader = "traceparent";
    public const string VersionHeader = "version";

    /// <summary>Falhas já registradas desta mensagem (retry com backoff e DLQ).</summary>
    public const string AttemptHeader = "x-tec-attempt";

    /// <summary>Último erro (na fila de espera ou na DLQ).</summary>
    public const string ErrorHeader = "x-tec-error";

    /// <summary>Mensagem devolvida da DLQ pelo administrador.</summary>
    public const string RequeuedHeader = "x-tec-requeued";

    /// <summary>Tamanho máximo do corpo lido (protege o consumidor de mensagens gigantes).</summary>
    public const int MaxBodyBytes = 16 * 1024 * 1024 + 4096;

    public static ReadOnlyMemory<byte> Serialize(MessageEnvelope envelope)
    {
        var buffer = new ArrayBufferWriter<byte>(envelope.Payload.Length + 512);
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WriteString("messageId", envelope.MessageId);
            json.WriteString("type", envelope.Type);
            json.WriteNumber("version", envelope.Version);
            json.WriteString("occurredAt", envelope.OccurredAt);
            WriteOptional(json, "correlationId", envelope.CorrelationId);
            WriteOptional(json, "causationId", envelope.CausationId);
            WriteOptional(json, "traceparent", envelope.TraceParent);
            json.WritePropertyName("payload");
            json.WriteRawValue(string.IsNullOrWhiteSpace(envelope.Payload) ? "{}" : envelope.Payload);
            json.WriteEndObject();
        }

        return buffer.WrittenMemory;
    }

    public static BasicProperties Properties(MessageEnvelope envelope)
    {
        var headers = new Dictionary<string, object?>(StringComparer.Ordinal) { [VersionHeader] = envelope.Version };
        if (envelope.CausationId is not null)
            headers[CausationHeader] = envelope.CausationId;
        if (envelope.TraceParent is not null)
            headers[TraceParentHeader] = envelope.TraceParent;

        return new BasicProperties
        {
            MessageId = envelope.MessageId.ToString(),
            Type = envelope.Type,
            CorrelationId = envelope.CorrelationId,
            ContentType = ContentType,
            ContentEncoding = "utf-8",
            DeliveryMode = DeliveryModes.Persistent,
            Timestamp = new AmqpTimestamp(envelope.OccurredAt.ToUnixTimeSeconds()),
            Headers = headers,
        };
    }

    /// <summary>Lê o envelope do corpo; <c>null</c> (com o motivo) se a mensagem é ilegível.</summary>
    public static MessageEnvelope? Read(ReadOnlyMemory<byte> body, out string? error)
    {
        if (body.Length > MaxBodyBytes)
        {
            error = $"Mensagem maior que {MaxBodyBytes} bytes.";
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(body, new JsonDocumentOptions { MaxDepth = 64 });
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                error = "Envelope não é um objeto JSON.";
                return null;
            }

            if (!root.TryGetProperty("messageId", out var id) || !id.TryGetGuid(out var messageId))
            {
                error = "Envelope sem messageId válido.";
                return null;
            }

            var type = Text(root, "type");
            if (string.IsNullOrWhiteSpace(type) || type.Length > MessageEnvelope.MaxTypeLength)
            {
                error = "Envelope sem type válido.";
                return null;
            }

            var version = root.TryGetProperty("version", out var v) && v.TryGetInt32(out var number) && number >= 1 ? number : 1;
            var occurred = root.TryGetProperty("occurredAt", out var o) && o.TryGetDateTimeOffset(out var when) ? when : DateTimeOffset.MinValue;
            var payload = root.TryGetProperty("payload", out var p) ? p.GetRawText() : "{}";
            error = null;
            return new MessageEnvelope(
                messageId, type, version, occurred, Limit(Text(root, "correlationId"), MessageEnvelope.MaxIdLength),
                Limit(Text(root, "causationId"), MessageEnvelope.MaxIdLength), Limit(Text(root, "traceparent"), MessageEnvelope.MaxTraceParentLength),
                payload);
        }
        catch (JsonException ex)
        {
            error = $"Envelope JSON inválido: {ex.Message}";
            return null;
        }
    }

    /// <summary>Cabeçalho texto (o cliente entrega strings AMQP como <c>byte[]</c>).</summary>
    public static string? HeaderText(IDictionary<string, object?>? headers, string name) =>
        headers is not null && headers.TryGetValue(name, out var value)
            ? value switch
            {
                byte[] bytes => Encoding.UTF8.GetString(bytes),
                string s => s,
                null => null,
                _ => Convert.ToString(value, CultureInfo.InvariantCulture),
            }
            : null;

    public static int HeaderInt(IDictionary<string, object?>? headers, string name) =>
        headers is not null && headers.TryGetValue(name, out var value)
            ? value switch
            {
                int i => Math.Max(0, i),
                long l => (int)Math.Clamp(l, 0, int.MaxValue),
                byte[] bytes when int.TryParse(Encoding.UTF8.GetString(bytes), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) => Math.Max(0, n),
                _ => 0,
            }
            : 0;

    /// <summary>Cópia das propriedades de uma mensagem recebida, com cabeçalhos editáveis (republicação).</summary>
    public static BasicProperties Copy(IReadOnlyBasicProperties source) =>
        new(source)
        {
            Headers = source.Headers is null
                ? new Dictionary<string, object?>(StringComparer.Ordinal)
                : new Dictionary<string, object?>(source.Headers, StringComparer.Ordinal),
            DeliveryMode = DeliveryModes.Persistent,
        };

    private static string? Limit(string? value, int max) => value is { Length: > 0 } && value.Length <= max ? value : null;

    private static string? Text(JsonElement root, string name) =>
        root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static void WriteOptional(Utf8JsonWriter json, string name, string? value)
    {
        if (value is null)
            json.WriteNull(name);
        else
            json.WriteString(name, value);
    }
}
