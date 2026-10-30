using Microsoft.Extensions.Hosting;

namespace TEC.Messaging.Internal;

/// <summary>
/// Regra comum aos transportes para a opção <c>RequireTls</c>: <c>null</c> (padrão) exige TLS fora de Development, como no
/// resto do ecossistema TEC (<see cref="HostEnvironmentEnvExtensions.IsDevelopment"/> do <see cref="IHostEnvironment"/>).
/// Sem ambiente registrado, o processo é tratado como produção (falha fechada).
/// </summary>
internal static class TransportSecurity
{
    public static bool RequiresTls(bool? requireTls, IHostEnvironment? environment) =>
        requireTls ?? environment?.IsDevelopment() != true;
}
