using TEC.Core.Common.Results;

namespace TEC.Messaging;

/// <summary>
/// Envelope padrão de uma mensagem de integração: identificação, tipo e versão do contrato, correlação ponta a ponta e o
/// payload JSON. É o que o Outbox grava, o que o broker transporta e o que o consumidor recebe.
/// </summary>
/// <param name="MessageId">Identificador único da mensagem (chave da idempotência no consumidor).</param>
/// <param name="Type">Nome estável do contrato (ex.: <c>vendas.pedido-aprovado</c>), ver <see cref="Contracts.IntegrationEventAttribute"/>.</param>
/// <param name="Version">Versão do contrato (1 ou mais).</param>
/// <param name="OccurredAt">Instante em que o fato ocorreu.</param>
/// <param name="CorrelationId">Correlação do fluxo de negócio (a mesma em todas as mensagens geradas por uma ação).</param>
/// <param name="CausationId">Mensagem que causou esta (o <see cref="MessageId"/> da mensagem consumida que a gerou).</param>
/// <param name="TraceParent">Contexto W3C (<c>traceparent</c>) para continuar o trace no consumidor.</param>
/// <param name="Payload">Corpo do evento em JSON.</param>
public sealed record MessageEnvelope(
    Guid MessageId,
    string Type,
    int Version,
    DateTimeOffset OccurredAt,
    string? CorrelationId,
    string? CausationId,
    string? TraceParent,
    string Payload)
{
    /// <summary>Tamanho máximo do <see cref="Type"/>.</summary>
    public const int MaxTypeLength = 200;

    /// <summary>Tamanho máximo de <see cref="CorrelationId"/> e <see cref="CausationId"/>.</summary>
    public const int MaxIdLength = 128;

    /// <summary>Tamanho máximo do <see cref="TraceParent"/> (o formato W3C tem 55 caracteres).</summary>
    public const int MaxTraceParentLength = 128;

    /// <summary>
    /// Valida o envelope contra os limites (nome e versão do tipo, identificadores e tamanho do payload em caracteres).
    /// </summary>
    /// <param name="maxPayloadLength">Tamanho máximo do payload, em caracteres.</param>
    /// <returns>Sucesso, ou falha de validação com o campo inválido.</returns>
    public Result Validate(int maxPayloadLength)
    {
        if (MessageId == Guid.Empty)
            return Error.Validation("MENSAGEM_ID_INVALIDO", "A mensagem precisa de um identificador.", nameof(MessageId));
        if (string.IsNullOrWhiteSpace(Type) || Type.Length > MaxTypeLength)
            return Error.Validation("MENSAGEM_TIPO_INVALIDO", $"O tipo da mensagem é obrigatório e tem até {MaxTypeLength} caracteres.", nameof(Type));
        if (Version < 1)
            return Error.Validation("MENSAGEM_VERSAO_INVALIDA", "A versão do contrato começa em 1.", nameof(Version));
        if (CorrelationId is { Length: > MaxIdLength } || CausationId is { Length: > MaxIdLength })
            return Error.Validation("MENSAGEM_CORRELACAO_INVALIDA", $"Correlação e causa têm até {MaxIdLength} caracteres.", nameof(CorrelationId));
        if (TraceParent is { Length: > MaxTraceParentLength })
            return Error.Validation("MENSAGEM_TRACE_INVALIDO", $"O traceparent tem até {MaxTraceParentLength} caracteres.", nameof(TraceParent));
        if (string.IsNullOrEmpty(Payload) || Payload.Length > maxPayloadLength)
            return Error.Validation("MENSAGEM_PAYLOAD_INVALIDO", $"O payload é obrigatório e tem até {maxPayloadLength} caracteres.", nameof(Payload));
        return Result.Success();
    }
}
