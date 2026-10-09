using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using TEC.Core.Common.Serialization;
using TEC.Core.Domain;
using TEC.Messaging.Contracts;
using TEC.Messaging.Inbox;
using TEC.Messaging.Mapping;
using TEC.Messaging.Outbox;

namespace TEC.Messaging.DependencyInjection;

/// <summary>
/// Configuração do TEC.Messaging devolvida por <see cref="MessagingServiceCollectionExtensions.AddTecMessaging"/>:
/// contratos, mapeadores, relay do Outbox e limpeza do Inbox. Os satélites acrescentam o armazenamento
/// (<c>UseSqlServer&lt;TContext&gt;()</c>) e o transporte (<c>UseRabbitMq(...)</c>).
/// </summary>
public sealed class MessagingBuilder
{
    internal const string ReflectionMessage =
        "Serialização JSON por reflexão. Em Native AOT/trimming registre o contrato com AddEventType<T>(JsonTypeInfo<T>), de um JsonSerializerContext gerado.";

    private readonly MessageTypeRegistrations _registrations;

    internal MessagingBuilder(IServiceCollection services, MessageTypeRegistrations registrations)
    {
        Services = services;
        _registrations = registrations;
    }

    /// <summary>Container de serviços.</summary>
    public IServiceCollection Services { get; }

    /// <summary>Registra um contrato pelo <see cref="IntegrationEventAttribute"/> do tipo, com JSON por reflexão.</summary>
    /// <typeparam name="T">Tipo do contrato.</typeparam>
    /// <returns>O builder.</returns>
    /// <exception cref="InvalidOperationException">O tipo não tem <see cref="IntegrationEventAttribute"/> ou o nome é inválido.</exception>
    [RequiresUnreferencedCode(ReflectionMessage)]
    [RequiresDynamicCode(ReflectionMessage)]
    public MessagingBuilder AddEventType<T>() where T : class
    {
        var attribute = AttributeOf(typeof(T));
        _registrations.Add(attribute.Name, attribute.Version, options => ResolveTypeInfo(options, typeof(T)));
        return this;
    }

    /// <summary>Registra um contrato com a metadata JSON gerada (compatível com Native AOT).</summary>
    /// <typeparam name="T">Tipo do contrato.</typeparam>
    /// <param name="typeInfo">Metadata do <c>JsonSerializerContext</c> gerado.</param>
    /// <param name="name">Nome do contrato; <c>null</c> = o do <see cref="IntegrationEventAttribute"/>.</param>
    /// <param name="version">Versão; <c>null</c> = a do atributo (ou 1).</param>
    /// <returns>O builder.</returns>
    public MessagingBuilder AddEventType<T>(JsonTypeInfo<T> typeInfo, string? name = null, int? version = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        var attribute = typeof(T).GetCustomAttribute<IntegrationEventAttribute>(inherit: false);
        var contractName = name ?? attribute?.Name
            ?? throw new InvalidOperationException($"Informe o nome do contrato de {typeof(T).Name} ou marque-o com [IntegrationEvent].");
        _registrations.Add(contractName, version ?? attribute?.Version ?? 1, _ => typeInfo);
        return this;
    }

    /// <summary>Registra todos os tipos do assembly marcados com <see cref="IntegrationEventAttribute"/> (JSON por reflexão).</summary>
    /// <param name="assembly">Assembly dos contratos.</param>
    /// <returns>O builder.</returns>
    [RequiresUnreferencedCode(ReflectionMessage)]
    [RequiresDynamicCode(ReflectionMessage)]
    public MessagingBuilder AddEventTypesFromAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        foreach (var type in assembly.GetTypes().Where(t => t is { IsAbstract: false, IsGenericTypeDefinition: false }))
        {
            if (type.GetCustomAttribute<IntegrationEventAttribute>(inherit: false) is { } attribute)
                _registrations.Add(attribute.Name, attribute.Version, options => ResolveTypeInfo(options, type));
        }

        return this;
    }

    /// <summary>Registra um mapeador de eventos de domínio (singleton, sem estado).</summary>
    /// <typeparam name="TMapper">Mapeador.</typeparam>
    /// <returns>O builder.</returns>
    public MessagingBuilder AddMapper<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMapper>()
        where TMapper : class, IIntegrationEventMapper
    {
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IIntegrationEventMapper, TMapper>());
        return this;
    }

    /// <summary>Mapeia um evento de domínio para um evento de integração com uma função (<c>null</c> = nenhum).</summary>
    /// <typeparam name="TDomainEvent">Evento de domínio.</typeparam>
    /// <param name="map">Função de mapeamento (sem estado, rápida).</param>
    /// <returns>O builder.</returns>
    /// <example><code>builder.Map&lt;PedidoAprovado&gt;(e => new PedidoAprovadoV1(e.PedidoId, e.Total));</code></example>
    public MessagingBuilder Map<TDomainEvent>(Func<TDomainEvent, object?> map) where TDomainEvent : IDomainEvent
    {
        ArgumentNullException.ThrowIfNull(map);
        Services.AddSingleton<IIntegrationEventMapper>(new DelegateIntegrationEventMapper<TDomainEvent>(map));
        return this;
    }

    /// <summary>
    /// Liga o relay do Outbox (serviço em segundo plano). Requer um <see cref="IOutboxStore"/> (ex.: <c>UseSqlServer</c>) e um
    /// <see cref="Transport.IMessagePublisher"/> (ex.: <c>UseRabbitMq</c>). Pode rodar em vários processos ao mesmo tempo.
    /// </summary>
    /// <param name="configure">Opções do relay.</param>
    /// <returns>O builder.</returns>
    public MessagingBuilder AddOutboxRelay(Action<OutboxOptions>? configure = null)
    {
        var options = Services.AddOptions<OutboxOptions>();
        if (configure is not null)
            options.Configure(configure);
        options.ValidateOnStart();
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<OutboxOptions>, OutboxOptionsValidator>());
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxRelay>());
        return this;
    }

    /// <summary>Liga a limpeza periódica do Inbox. Requer um <see cref="IInboxStore"/>.</summary>
    /// <param name="configure">Opções da limpeza.</param>
    /// <returns>O builder.</returns>
    public MessagingBuilder AddInboxCleanup(Action<InboxOptions>? configure = null)
    {
        var options = Services.AddOptions<InboxOptions>();
        if (configure is not null)
            options.Configure(configure);
        options.ValidateOnStart();
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<InboxOptions>, InboxOptionsValidator>());
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, InboxCleanupService>());
        return this;
    }

    private static IntegrationEventAttribute AttributeOf(Type type) =>
        type.GetCustomAttribute<IntegrationEventAttribute>(inherit: false)
        ?? throw new InvalidOperationException($"O tipo {type.FullName} não tem [IntegrationEvent]: informe nome e versão do contrato.");

    [RequiresUnreferencedCode(ReflectionMessage)]
    [RequiresDynamicCode(ReflectionMessage)]
    private static JsonTypeInfo ResolveTypeInfo(JsonSerializerOptions? configured, Type type)
    {
        var options = configured ?? JsonDefaults.Options;
        if (options.TypeInfoResolver is null)
        {
            options = new JsonSerializerOptions(options) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
            options.MakeReadOnly();
        }

        return options.GetTypeInfo(type);
    }
}

/// <summary>Registros de contratos acumulados pelo builder; o catálogo é montado uma vez, na primeira resolução.</summary>
internal sealed class MessageTypeRegistrations
{
    private readonly List<(string Name, int Version, Func<JsonSerializerOptions?, JsonTypeInfo> TypeInfo)> _items = [];

    public void Add(string name, int version, Func<JsonSerializerOptions?, JsonTypeInfo> typeInfo)
    {
        if (!MessageTypeRegistry.IsValidName(name))
        {
            throw new InvalidOperationException(
                $"Nome de contrato inválido: '{name}'. Use minúsculas, dígitos, '.' e '-' (ex.: vendas.pedido-aprovado), até {MessageEnvelope.MaxTypeLength} caracteres.");
        }

        if (version < 1)
            throw new InvalidOperationException($"A versão do contrato '{name}' deve ser 1 ou maior.");
        lock (_items)
            _items.Add((name, version, typeInfo));
    }

    public MessageTypeRegistry Build(JsonSerializerOptions? options)
    {
        lock (_items)
            return new MessageTypeRegistry(_items.Select(i => new MessageTypeInfo(i.Name, i.Version, i.TypeInfo(options))).ToList());
    }
}
