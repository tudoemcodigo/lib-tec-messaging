using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;

namespace TEC.Messaging.Tests.Shared;

/// <summary>Ambiente de host com nome escolhido pelo teste (Development, Production...).</summary>
internal sealed class TestHostEnvironment(string environmentName) : IHostEnvironment
{
    public string EnvironmentName { get; set; } = environmentName;

    public string ApplicationName { get; set; } = "TEC.Messaging.Tests";

    public string ContentRootPath { get; set; } = AppContext.BaseDirectory;

    public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
}
