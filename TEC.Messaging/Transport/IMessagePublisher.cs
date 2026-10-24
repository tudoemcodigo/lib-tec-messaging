namespace TEC.Messaging.Transport;

/// <summary>
/// Porta de saída para o broker (RabbitMQ, Service Bus...). Usada pelo relay do Outbox, nunca diretamente pelos casos de
/// uso: publicar fora da transação perderia eventos quando o commit falha.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publica a mensagem e só retorna depois da confirmação do broker. Qualquer falha (indisponível, recusa, tempo
    /// esgotado) deve lançar exceção, para o Outbox tentar de novo. Quando o problema é do transporte e não da mensagem
    /// (broker fora, conexão recusada, transporte desligado), a exceção deve implementar
    /// <see cref="ITransportUnavailableException"/>: o Outbox então devolve o lote sem contar tentativa.
    /// </summary>
    /// <param name="envelope">Mensagem.</param>
    /// <param name="cancellationToken">Cancelamento.</param>
    /// <returns>Tarefa concluída com a confirmação do broker.</returns>
    Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken);
}

/// <summary>
/// Marca as exceções de <see cref="IMessagePublisher.PublishAsync"/> que indicam transporte indisponível (broker fora,
/// conexão recusada, circuito aberto, transporte desligado), e não defeito da mensagem. O relay do Outbox não conta
/// tentativa para essas falhas: devolve a mensagem e o resto do lote e pausa, de modo que uma indisponibilidade longa não
/// leva mensagens a <see cref="Outbox.OutboxMessageStatus.Dead"/>.
/// </summary>
/// <remarks>
/// Interface marcadora para o núcleo não depender do transporte: cada satélite a implementa nas próprias exceções. A marca
/// também é reconhecida numa exceção interna (<see cref="Exception.InnerException"/>), para decoradores do publicador.
/// </remarks>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1040:Avoid empty interfaces", Justification = "Interface marcadora de exceções; atributo não serve porque a marca é testada na instância.")]
public interface ITransportUnavailableException;
