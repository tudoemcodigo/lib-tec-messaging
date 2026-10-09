namespace TEC.Messaging.Transport;

/// <summary>
/// Porta de saída para o broker (RabbitMQ, Service Bus...). Usada pelo relay do Outbox, nunca diretamente pelos casos de
/// uso: publicar fora da transação perderia eventos quando o commit falha.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publica a mensagem e só retorna depois da confirmação do broker. Qualquer falha (indisponível, recusa, tempo
    /// esgotado) deve lançar exceção, para o Outbox tentar de novo.
    /// </summary>
    /// <param name="envelope">Mensagem.</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Tarefa concluída com a confirmação do broker.</returns>
    Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken);
}
