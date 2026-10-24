<div align="center">
<img src="https://www.tudoemcodigo.com/marca/PNG/tudoemcodigo-icone-512.png" alt="Tudo em Código" width="90" />
</div>

[🏠 TEC.Messaging](../../README.md) › [📚 Documentação](../../docs/README.md) › ⚙️ CI/CD

# ⚙️ CI/CD e publicação

> Os três workflows são curtos: chamam os workflows reutilizáveis do [tec-workflows](https://github.com/tudoemcodigo/tec-workflows)
> e só declaram o que é deste repositório (solução, testes, SQL Server e RabbitMQ em container).

## 📑 Sumário

- [🎯 Visão geral](#-visão-geral)
- [📂 Arquivos](#-arquivos)
- [🔑 Variables e Secrets](#-variables-e-secrets)
- [🚀 Como publicar](#-como-publicar)

---

## 🎯 Visão geral

| Evento | Workflow | O que roda | Publica? |
|---|---|---|:---:|
| `pull_request` / `merge_group` | `ci.yml` | Convenções, build + pack (6 pacotes), unitários em matriz, integração (SQL Server + RabbitMQ em container), CodeQL → `ci-ok` | ❌ |
| `push` na `main` | `ci.yml` | O mesmo e, com `ci-ok` verde, `publicar-previa` | ✅ `<Version>-preview.N` |
| `schedule` segunda 06:00 UTC / manual | `ci.yml` | O mesmo na `main` | ❌ |
| Manual (**Performance**) | `performance.yml` | `Carga-CI`, `Carga-Pesada` e benchmarks opcionais | ❌ |
| Manual (**Publicar versão**) | `release.yml` | Convenções, pack, unitários + cobertura, CodeQL, tag, Release e push dos 6 pacotes | ✅ `X.Y.Z` ou `-rc.N` |

## 📂 Arquivos

| Arquivo | Função |
|---|---|
| [`ci.yml`](ci.yml) | PR, push na `main` (prévia) e semanal, com `dotnet-ci.yml@v2` |
| [`release.yml`](release.yml) | Versão estável ou `-rc.N`, com `dotnet-release.yml@v2` |
| [`performance.yml`](performance.yml) | Carga sob demanda (`dotnet-test.yml@v2`) e benchmarks (`dotnet-benchmark.yml@v2`) |
| [`../scripts/integration-setup.sh`](../scripts/integration-setup.sh) | Sobe SQL Server e RabbitMQ descartáveis (senhas aleatórias, mascaradas) e exporta as conexões |
| [`../scripts/integration-teardown.sh`](../scripts/integration-teardown.sh) | Remove os containers (sempre) |
| [`../dependabot.yml`](../dependabot.yml) · [`../zizmor.yml`](../zizmor.yml) | Canônicos do tec-workflows |

## 🔑 Variables e Secrets

Nenhuma variável própria: o feed `tec-interno` usa o `GITHUB_TOKEN` (organização). Não há testes contra Azure.

## 🚀 Como publicar

1. Merge na `main` com `ci-ok` verde → prévia `0.0.1-preview.N` (a `<Version>` do `Directory.Build.props`).
2. Actions → **Publicar versão** → `0.0.1` → tag `v0.0.1` e pacotes (`TEC.Messaging`, `.SqlServer`, `.PostgreSql`, `.RabbitMQ`, `.AzureServiceBus` e `.Aws`).
3. Suba a `<Version>` do `Directory.Build.props` (ex.: `0.0.2`) para voltar a gerar prévias.
