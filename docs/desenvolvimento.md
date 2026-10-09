[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › Desenvolvimento

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
| Versão | Única, em `Directory.Build.props` |
| Dependências TEC | `<TecReference Include="TEC.Core" />`; versão publicada em `Directory.Packages.props` |
| Modo local | `-p:TecUseLocalProjects=true` usa os repositórios vizinhos (`..\TEC.Core`, `..\TEC.Vault`) |
| Lock | `packages.lock.json` versionado (modo pacote): `dotnet restore TEC.Messaging.slnx --force-evaluate` |

## 🚀 Uso

```bash
dotnet build TEC.Messaging.slnx -c Release
dotnet build TEC.Messaging.slnx -c Release -p:TecUseLocalProjects=true    # testando mudanças no TEC.Core local
```

Publicação: merge na `main` gera `X.Y.Z-preview.N`; versão estável pelo workflow **Publicar versão**. Depois de publicar
`X.Y.Z`, suba a `<Version>`.

## ⚙️ Opções

Ver [CI/CD](../.github/workflows/README.md).

## ❌ Erros

| Situação | Causa | O que fazer |
|---|---|---|
| `NU1004` no CI | Lock desatualizado | `dotnet restore TEC.Messaging.slnx --force-evaluate` e commit |
| `NU1101` (TEC.Core) | Versão não publicada no feed | Aguarde a prévia ou use o modo local |

## 🛡️ Segurança

> [!NOTE]
> Credenciais do feed `tec-interno` ficam no NuGet.Config do usuário, nunca no repositório.

## ❓ Perguntas frequentes

<details>
<summary><b>Por que o TEC.Messaging não depende do TEC.Cqrs?</b></summary>

Para servir a qualquer aplicação (com ou sem CQRS). A integração é pelo `SaveChanges` (interceptor) e por interfaces próprias.
</details>

---
⬅️ [Testes](testes.md) · [📚 Índice](README.md)
