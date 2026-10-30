[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › 🛠️ Desenvolvimento

# 🛠️ Desenvolvimento

> Como compilar, testar contra componentes vizinhos e publicar.

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [🚀 Uso](#-uso)
- [⚙️ Opções](#️-opções)
- [❌ Erros](#-erros)
- [🛡️ Segurança](#️-segurança)
- [❓ Perguntas frequentes](#-perguntas-frequentes)

---

## 🎯 Visão geral

| Item | Regra |
|---|---|
| Versão | Única, em `Directory.Build.props` (`<Version>`), igual para todos os pacotes |
| Alvo | `net10.0` (LTS), vindo do `build/Tec.Build.props` canônico (SDK `10.0.402` com `rollForward: latestPatch`, em `global.json`) |
| Dependências TEC | `<TecReference Include="TEC.Core" />`; versão publicada em `Directory.Packages.props` |
| Modo local | `-p:TecUseLocalProjects=true` usa os repositórios vizinhos (`..\TEC.Core`, `..\TEC.Vault`) |
| Lock | `packages.lock.json` versionado (modo pacote): `dotnet restore TEC.Messaging.slnx --force-evaluate` |
| Pacotes e análise | Pacotes compilam com avisos como erro, documentação XML, validação de pacote e `IsAotCompatible` (desligado nos satélites de EF Core e de nuvem) |

### Estrutura do repositório

| Pasta | Conteúdo |
|---|---|
| `TEC.Messaging/` | Núcleo (pacote): envelope, contratos, Outbox/Inbox (abstrações), relay, sanitizador, telemetria |
| `TEC.Messaging.SqlServer/` · `TEC.Messaging.PostgreSql/` | Persistência com EF Core (pacotes) |
| `TEC.Messaging.RabbitMQ/` · `TEC.Messaging.AzureServiceBus/` · `TEC.Messaging.Aws/` | Transportes (pacotes) |
| `TEC.Messaging.Tests/` · `TEC.Messaging.LoadTests/` · `TEC.Messaging.Benchmarks/` | Testes, carga e benchmarks (nunca viram pacote) |
| `samples/` | Exemplo executável (nunca vira pacote) |
| `build/` | `Tec.Build.props` e `Tec.Build.targets` (canônicos) |
| `docs/` · `.github/` | Documentação, workflows e scripts de integração |

Os seis pacotes saem juntos, com a mesma versão. Os satélites têm acesso aos internos do núcleo (`InternalsVisibleTo`): telemetria, `GuidV7`, `SensitiveDataSanitizer` e a regra de `RequireTls`.

Arquivos **canônicos**, mantidos no repositório `tec-workflows` e sincronizados (não os edite aqui; o job "Convenções" do CI falha se divergirem): `build/Tec.Build.props`, `build/Tec.Build.targets`, `Directory.Build.targets`, `.editorconfig`, `.gitattributes`, `nuget.config`, `.gitignore`, `.github/dependabot.yml`, `.github/zizmor.yml`. O que é do componente: `Directory.Build.props` (identidade e versão), `Directory.Packages.props`, os `.csproj` e os workflows `ci.yml`, `release.yml` e `performance.yml`.

## 🚀 Uso

```bash
dotnet build TEC.Messaging.slnx -c Release
dotnet build TEC.Messaging.slnx -c Release -p:TecUseLocalProjects=true    # testando mudanças no TEC.Core local
```

### Credencial do feed

O feed `tec-interno` (GitHub Packages) exige token mesmo para pacote público. Uma vez por máquina, fora do repositório:

```bash
dotnet nuget add source https://nuget.pkg.github.com/tudoemcodigo/index.json -n tec-interno -u <usuario-github> -p <PAT read:packages>
```

(no Linux/macOS acrescente `--store-password-in-clear-text`). Com os repositórios vizinhos clonados lado a lado e `-p:TecUseLocalProjects=true`, o restore dos `TEC.*` nem consulta o feed.

### Modo local

O modo local (`TecUseLocalProjects`) transforma cada `TecReference` em `ProjectReference` para o repositório vizinho (`D:\Projetos\Componentes\TEC.Core\...`), se ele existir; senão continua `PackageReference`. É ignorado no CI (que valida o que o consumidor recebe). O lock file do modo local é `packages.local.lock.json` (fora do git).

### Lock files

```bash
dotnet restore TEC.Messaging.slnx --force-evaluate     # regenera o packages.lock.json do modo pacote (precisa do token)
```

No CI o restore é travado (`RestoreLockedMode`): lock desatualizado quebra o build.

### Executar o exemplo

```bash
dotnet run --project samples/TEC.Messaging.SampleWorker      # precisa de SQL Server e RabbitMQ (ver samples/README.md)
```

### Testes

Veja [🧪 Testes](testes.md).

### Publicação

Merge na `main` gera `X.Y.Z-preview.N`; versão estável pelo workflow **Publicar versão**. Depois de publicar
`X.Y.Z`, suba a `<Version>`. O passo a passo está em [⚙️ CI/CD](../.github/workflows/README.md#-como-publicar).

### Contribuição

Pull request para a `main` com o `ci-ok` verde. Pacotes compilam com **qualquer aviso como erro** (inclusive IL de AOT e nullability); o `.editorconfig` é a fonte do estilo. Mudou código público? Atualize o `CHANGELOG.md` e a documentação do tema afetado em `docs/`. Decisões de arquitetura novas ganham um ADR em [🏛️ docs/adr](adr/README.md).

## ⚙️ Opções

Ver [CI/CD](../.github/workflows/README.md). Propriedades de build úteis:

| Propriedade | Efeito |
|---|---|
| `-p:TecUseLocalProjects=true` | Usa os repositórios vizinhos dos componentes `TEC.*` (ignorado no CI) |
| `-p:TecComponentsRoot=<pasta>` | Pasta que contém os repositórios vizinhos (padrão: a pasta pai do repositório) |
| `-p:NuGetAudit=false` | Desliga a auditoria de vulnerabilidades (só para diagnóstico local; o CI não aceita) |

## ❌ Erros

| Situação | Causa | O que fazer |
|---|---|---|
| `NU1004` no CI | Lock desatualizado | `dotnet restore TEC.Messaging.slnx --force-evaluate` e commit |
| `NU1301` / `401 Unauthorized` | Sem credencial do feed `tec-interno` | Configure o PAT (acima) ou use o modo local |
| `NU1101` (TEC.Core) | Versão não publicada no feed | Aguarde a prévia ou use o modo local |
| `NU1010` | `TecReference` sem `PackageVersion` em `Directory.Packages.props` | Declare a versão publicada do componente |
| `NU1902`/`NU1903` como erro | Dependência transitiva vulnerável (ex.: `System.Security.Cryptography.Pkcs` via `Microsoft.Data.SqlClient`) | Fixe a versão corrigida com `PackageVersion` no `Directory.Packages.props` (fixação transitiva ligada) e regenere os locks |
| Erro "Use `<TecReference ...>` em vez de `PackageReference`" | `PackageReference` solto para um `TEC.*` | Use `TecReference` (build/Tec.Build.targets) |

## 🛡️ Segurança

> [!NOTE]
> Credenciais do feed `tec-interno` ficam no NuGet.Config do usuário, nunca no repositório.

- O `nuget.config` limpa as origens e mapeia `TEC.*` só para o feed interno (contra *dependency confusion*).
- No CI, as imagens do SQL Server e do RabbitMQ são fixadas por digest e as senhas são aleatórias e mascaradas.
- Os workflows declaram permissões mínimas e passam por análise (`zizmor`, CodeQL).

## ❓ Perguntas frequentes

<details>
<summary><b>Por que o TEC.Messaging não depende do TEC.Cqrs?</b></summary>

Para servir a qualquer aplicação (com ou sem CQRS). A integração é pelo `SaveChanges` (interceptor) e por interfaces próprias.
</details>

<details>
<summary><b>Ainda dá para usar no .NET 8?</b></summary>

Não. Os pacotes têm um único alvo, `net10.0` (LTS), com EF Core e Npgsql 10.x. Quem ainda roda no .NET 8 precisa migrar o runtime antes de adotar o componente.
</details>

---
⬅️ [🧪 Testes](testes.md) · [📚 Índice](README.md) · [🔍 Revisão técnica](revisao-tecnica.md) ➡️
