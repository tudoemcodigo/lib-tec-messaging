[🏠 TEC.Messaging](../README.md) › [📚 Documentação](README.md) › 🔍 Revisão técnica

# 🔍 Revisão técnica

> Análise de segurança, bugs, implementação, organização e aderência da documentação dos pacotes `TEC.Messaging`,
> `TEC.Messaging.SqlServer` e `TEC.Messaging.RabbitMQ`.

> [!NOTE]
> Este documento é um **instantâneo datado** (2026-10-19): números de linha, versões (`0.1.0`/`0.2.0`, antes de a primeira publicação oficial ser numerada `0.0.1`) e contagens de testes descrevem aquele momento e não são atualizados. Os pacotes `PostgreSql`, `AzureServiceBus` e `Aws` não estavam no escopo; a seção [Complemento da revisão documental](#-complemento-da-revisão-documental--2026-10-24) registra o que a conferência da documentação contra o código encontrou neles.

## 📑 Sumário

- [🎯 Escopo](#-escopo)
- [🧪 Build e testes](#-build-e-testes)
- [📋 Resumo dos achados](#-resumo-dos-achados)
- [🔴 Severidade alta](#-severidade-alta)
- [🟠 Severidade média](#-severidade-média)
- [🟡 Severidade baixa](#-severidade-baixa)
- [ℹ️ Informativos](#ℹ️-informativos)
- [✅ Pontos fortes](#-pontos-fortes)
- [🚀 Melhorias sugeridas](#-melhorias-sugeridas)
- [📝 Ajustes na documentação](#-ajustes-na-documentação)
- [🔧 Correções aplicadas — 2026-10-19](#-correções-aplicadas--2026-10-19)
- [🔎 Complemento da revisão documental — 2026-10-24](#-complemento-da-revisão-documental--2026-10-24)

---

## 🎯 Escopo

| Item | Valor |
|---|---|
| Data | 2026-10-19 |
| Commit analisado | `dba4b09 Circuit breaker da conexão RabbitMQ, índice do Outbox e dependências estáveis (0.1.0) (#3)` |
| Versão | `0.1.0` (`Directory.Build.props`) |
| Código lido | `TEC.Messaging/`, `TEC.Messaging.SqlServer/`, `TEC.Messaging.RabbitMQ/`, testes, sample, `.github/` (workflows e scripts `.sh`), `docs/` |
| Contexto consultado | `TEC.ORM` (`OrmUnitOfWork`: transação explícita em volta do `SaveChanges`), `TEC.Core` (`JsonDefaults`) |
| Fora do escopo | Execução dos testes de integração e de carga (precisam de SQL Server e RabbitMQ em container) |

Severidades: **Crítica** (perda/vazamento explorável agora), **Alta** (quebra de garantia central ou do build), **Média**
(bug real com cenário plausível ou contenção de risco operacional), **Baixa** (caso de borda, robustez), **Info**
(observação/melhoria).

---

## 🧪 Build e testes

| Comando | Resultado |
|---|---|
| `dotnet build TEC.Messaging.slnx -c Release` (modo pacote) | ❌ `NU1301` — restore do feed `tec-interno` com `401 Unauthorized` (sem PAT nesta máquina; ambiente, não código) |
| `dotnet build ... -p:TecUseLocalProjects=true` | ❌ `NU1902`/`NU1903` como erro: `System.Security.Cryptography.Pkcs 9.0.11` vulnerável em `TEC.Messaging.SqlServer`, `TEC.Messaging.Tests` e `TEC.Messaging.SampleWorker` (ver [A2](#a2--dependência-transitiva-vulnerável-quebra-o-build)) |
| `dotnet build ... -p:TecUseLocalProjects=true -p:NuGetAudit=false --no-incremental` | ✅ 0 avisos, 0 erros (net8.0 e net10.0) |
| Unitários net8.0 (`--treenode-filter "/*/*/*/*[Category!=Integracao]"`) | ✅ 39/39 aprovados (~11 s) |
| Unitários net10.0 (mesmo filtro) | ✅ 39/39 aprovados (~11 s) |
| Integração / carga | ⏭️ Não executados (containers) |

---

## 📋 Resumo dos achados

| Severidade | Quantidade |
|---|:-:|
| Crítica | 0 |
| Alta | 2 |
| Média | 5 |
| Baixa | 13 |
| Info | 5 |

| ID | Severidade | Categoria | Local | Resumo | Status |
|---|---|---|---|---|---|
| A1 | Alta | Bug | `TEC.Messaging/Outbox/OutboxRelay.cs:109-151` | Broker fora (ou `Enabled = false`) consome tentativas: em ~14 min as mensagens viram `Dead` e exigem ação manual | ✅ Corrigido |
| A2 | Alta | Segurança/Build | `TEC.Messaging.SqlServer/packages.lock.json:59,248` | `System.Security.Cryptography.Pkcs 9.0.11` (CVE-2026-84008, alta) quebra o build com `NuGetAudit` | ✅ Corrigido (locks pendentes) |
| M1 | Média | Bug | `OutboxRelay.cs:29,168` · `IInboxStore.cs:73,86` (Inbox) | Limpeza apaga no máximo 5000 linhas por `CleanupInterval`: acima de ~1,4 msg/s as tabelas crescem sem limite | ✅ Corrigido |
| M2 | Média | Desempenho | `SqlServerOutboxStore.cs:117-135` · `OutboxRelay.cs:55-59` | Estatísticas fazem `GROUP BY Status` na tabela inteira a cada 30 s, em todas as instâncias | ✅ Corrigido (sem eleição) |
| M3 | Média | API | `SqlServerOutboxStore.cs:159` · `RabbitMqDeadLetterAdministration.cs:76` | `RequeueAsync` com coleção **vazia** reprocessa **tudo** (mesmo efeito de `null`) | ✅ Corrigido |
| M4 | Média | Bug/Design | `RabbitMqConsumer.cs:332-335` | `ConsumeResult.Requeue` reentrega em laço imediato e manda para a DLQ em segundos, justamente no cenário "dependência fora" | ✅ Corrigido |
| M5 | Média | Design | `RabbitMqPublisher.cs:90-91` | O publicador declara a topologia de todos os consumidores: um `PRECONDITION_FAILED` numa fila para todo o Outbox (e agrava A1) | ✅ Corrigido |
| B1 | Baixa | Concorrência | `SqlServerOutboxStore.cs:26-43` · `OutboxRelay.cs:99,109,122` | Reserva usa o relógio da aplicação e a checagem do prazo é só entre mensagens: relógio adiantado ou publicação lenta duplicam | ✅ Corrigido |
| B2 | Baixa | Bug | `SqlServerOutboxStore.cs:157-170` | `RequeueAsync` desfaz a reserva de mensagens em publicação (duplicidade) | ✅ Corrigido |
| B3 | Baixa | Inbox | `SqlServerInboxStore.cs:23-28` · `RabbitMqConsumer.cs:308-311` | Corrida do Inbox vira `Retry` com log de erro e tentativa consumida | ✅ Corrigido (helper) |
| B4 | Baixa | Shutdown | `RabbitMqConsumer.cs:302-307,226` | Desligamento cancela o handler na hora e devolve com NACK (conta no `x-delivery-limit`), sem drenar | ✅ Corrigido |
| B5 | Baixa | Dispose | `RabbitMqPublisher.cs:62,66-72` | `DisposeAsync` descarta a trava e o canal sem esperar publicação em andamento | ✅ Corrigido |
| B6 | Baixa | Topologia | `QueueDefinition.cs:71-74` | Rótulo da fila de espera ignora milissegundos: atrasos distintos colidem no mesmo nome | ✅ Corrigido |
| B7 | Baixa | DoS/Limites | `AmqpEnvelope.cs:30,77` · `MessagingOptions.cs:23` | Limite de corpo checado depois do buffer; consumo não aplica `MaxPayloadLength`; 16 MiB de payload não cabe no `max_message_size` padrão | ✅ Corrigido |
| B8 | Baixa | Telemetria | `SqlServerOutboxStore.cs:195` | `IOutbox.Enqueue` conta `outbox.enqueued` antes do commit | ✅ Corrigido |
| B9 | Baixa | SQL | `ModelBuilderExtensions.cs:43,46` | Filtros dos índices com `[Status]` fixo: quebram com convenção de nomes de coluna | ✅ Corrigido |
| B10 | Baixa | CI/Script | `.github/scripts/integration-setup.sh:44,20-23` | Senha do SA na linha de comando do `docker exec`; imagens sem digest | ✅ Corrigido |
| B11 | Baixa | Telemetria | `MessagingDiagnostics.cs:32` · `AmqpEnvelope.cs:99` | Nome do span de consumo vem do `type` não confiável da mensagem (cardinalidade) | ✅ Corrigido |
| B12 | Baixa | Admin | `RabbitMqDeadLetterAdministration.cs:141-151` | Qualquer `OperationInterruptedException` (ex.: 403) vira "fila não encontrada" | ✅ Corrigido |
| B13 | Baixa | Docs | `TecMessagingSaveChangesInterceptor.cs:69-82` · `docs/outbox.md` | Eventos são descartados depois do `SaveChanges`, não do `COMMIT` da transação externa | ✅ Corrigido (docs) |
| I1 | Info | Desempenho | `RabbitMqPublisher.cs:46-52` | Uma publicação por vez, esperando cada confirmação | ⏭️ Não corrigido |
| I2 | Info | Consumo | `RabbitMqConsumer.cs:192-194` | `Prefetch` não dá paralelismo (despacho de 1 por canal) | 📝 Documentado |
| I3 | Info | Contrato | `RabbitMqConsumer.cs:284,323` | `x-tec-attempt` vem do produtor e é confiado | ⏭️ Não corrigido |
| I4 | Info | Telemetria | `MessagingDiagnostics.cs:87-90` | Gauges do Outbox são globais mas emitidos por instância | 📝 Documentado |
| I5 | Info | Testes | `TEC.Messaging.Tests/` | Sem testes unitários de `AmqpEnvelope`, `QueueDefinition`, `FinishAsync`, filtro da DLQ e limpeza | ✅ Parcial (testes) |

---

## 🔴 Severidade alta

### A1 — Broker fora consome tentativas e leva mensagens a `Dead`

**Problema.** Quando o broker cai, as `ConsecutiveFailuresToPause` primeiras mensagens do lote contam tentativa (`Failed`
com backoff curto) e só o resto volta sem contar. No ciclo seguinte (depois de `PauseDuration`) as mesmas mensagens mais
antigas estão vencidas de novo e voltam a falhar. Com o circuito aberto o efeito é o mesmo:
`RabbitMqCircuitOpenException` também é contada.

**Evidência.**

```csharp
// TEC.Messaging/Outbox/OutboxRelay.cs:134-149
catch (Exception ex)
{
    consecutiveFailures++;
    var attempts = message.Attempts + 1;
    ...
    if (attempts >= settings.MaxAttempts)
        outcomes.Add(OutboxOutcome.Dead(message, attempts, error));
```

O teste `Consecutive_failures_release_the_rest_without_counting_attempts` (`OutboxRelayTests.cs:116-130`) confirma que as
primeiras falham com `Attempts == 1`. Com `RabbitMqOptions.Enabled = false` a publicação lança
`InvalidOperationException` (`RabbitMqPublisher.cs:43-44`) e cai no mesmo caminho.

**Impacto.** Com os padrões (`MaxAttempts = 10`, backoff 2 s → 5 min, pausa 10 s), as mensagens mais antigas chegam a
`Dead` em ~14 min de indisponibilidade (≈ 10+10+10+16+32+64+128+256+300 s). Uma janela de manutenção do RabbitMQ ou uma
rotação de credencial malsucedida deixa eventos parados até alguém chamar `RequeueAsync`; a ordem se perde. A
documentação promete o contrário ("Broker fora: ... volta sem contar tentativa"; "`Enabled=false`: ... o Outbox retém").

**Sugestão.** Quando a pausa é disparada, converter as falhas da sequência atual em `Released` (sem contar tentativa) e
tratar falhas de transporte (circuito aberto, transporte desligado, conexão recusada) como indisponibilidade, não como
defeito da mensagem.

```csharp
// Guarda as falhas da sequência; se a sequência pausar o relay, elas não contam tentativa
var streak = new List<(LeasedOutboxMessage Message, int Index)>();
...
catch (Exception ex) when (ex is ITransportUnavailable or RabbitMqCircuitOpenException)
{
    paused = true; // broker fora: devolve tudo, inclusive esta, sem contar
    ...
}
catch (Exception ex)
{
    consecutiveFailures++;
    streak.Add((message, outcomes.Count));
    outcomes.Add(/* Failed ou Dead como hoje */);
}
...
if (paused)
    foreach (var (m, index) in streak)
        outcomes[index] = OutboxOutcome.Released(m, now + settings.PauseDuration);
```

Uma interface marcadora no núcleo (ex.: `ITransportUnavailableException`) implementada pelas exceções do satélite evita
o núcleo conhecer o RabbitMQ.

### A2 — Dependência transitiva vulnerável quebra o build

**Problema.** No net10.0, `Microsoft.EntityFrameworkCore.SqlServer 10.0.12` → `Microsoft.Data.SqlClient 6.1.6` →
`System.Security.Cryptography.Pkcs 9.0.11`, afetada por GHSA-846f-f764-76mx (CVE-2026-84008, alta, DoS por CPU em
`SignedCms.CheckSignature`, corrigida em 9.0.21/10.0.13) e GHSA-2fmv-rv2c-59x7 (moderada).

**Evidência.** `TEC.Messaging.SqlServer/packages.lock.json:59` e `:248` (`"resolved": "9.0.11"`); restore local
falhou com `error NU1903: Aviso como Erro` em `TEC.Messaging.SqlServer`, `TEC.Messaging.Tests` e
`TEC.Messaging.SampleWorker`. O build trata `NU1901`–`NU1904` como erro (`build/Tec.Build.props:55`).

**Impacto.** O CI (inclusive o agendamento semanal) e qualquer build local passam a falhar; os consumidores do pacote
recebem a versão vulnerável pela árvore de dependências.

**Sugestão.** A fixação transitiva já está ligada (`CentralPackageTransitivePinningEnabled`); basta declarar a versão
corrigida e regenerar os lock files (`dotnet restore TEC.Messaging.slnx --force-evaluate`):

```xml
<!-- Directory.Packages.props -->
<ItemGroup Condition="'$(TargetFramework)' != 'net8.0'">
  <PackageVersion Include="System.Security.Cryptography.Pkcs" Version="10.0.13" />
</ItemGroup>
```

Conferir o net8.0 (`SqlClient 5.1.9`) com `dotnet list package --vulnerable --include-transitive` e, se necessário, fixar
`8.0.2`. Quando o EF Core 10.0.13 sair com o SqlClient atualizado, a fixação pode sair.

---

## 🟠 Severidade média

### M1 — Limpeza limitada a 5000 linhas por intervalo

**Problema.** O relay e o `InboxCleanupService` chamam o purge **uma vez** por `CleanupInterval` com `PurgeBatch = 5000`.

**Evidência.**

```csharp
// OutboxRelay.cs:29 e :168
private const int PurgeBatch = 5000;
var removed = await store.PurgePublishedAsync(now - settings.PublishedRetention, PurgeBatch, ct)...
// IInboxStore.cs:73 e :86 (InboxCleanupService) — mesmo padrão
```

**Impacto.** Com o padrão de 1 h, o teto é 120 mil linhas/dia. Um serviço com 2 msg/s (~173 mil/dia) acumula ~53 mil
linhas por dia para sempre, no Outbox e no Inbox (este cresce por consumidor). Disco, backup e o GROUP BY de M2
degradam com o tempo.

**Sugestão.** Repetir o purge enquanto o lote vier cheio (com teto de tempo e respeitando o cancelamento):

```csharp
int removed, total = 0;
do
{
    removed = await store.PurgePublishedAsync(before, PurgeBatch, ct).ConfigureAwait(false);
    total += removed;
} while (removed == PurgeBatch && !ct.IsCancellationRequested && time.GetUtcNow() - started < MaxCleanupTime);
```

### M2 — Estatísticas varrem a tabela inteira em todas as instâncias

**Problema.** `GetStatisticsAsync` agrupa por `Status` sem filtro (inclui as publicadas retidas por 7 dias) e é chamado a
cada `StatisticsInterval` (30 s) por **cada** relay.

**Evidência.** `SqlServerOutboxStore.cs:120-124` (`messages.GroupBy(m => m.Status)...`) e `OutboxRelay.cs:55-59`.

**Impacto.** Com milhões de publicadas retidas, N instâncias fazem varredura do índice clusterizado a cada 30 s, com
leituras que competem com a reserva. Os gauges também saem repetidos por instância (ver I4).

**Sugestão.** Contar só o que interessa ao alerta com predicados que casam com os índices filtrados
(`Status = 0` e `Status = 2`), tornar a contagem de `Published` opcional (só na administração) e/ou eleger uma instância
para as estatísticas (lock de aplicação `sp_getapplock` ou reserva de uma linha de controle).

### M3 — Coleção vazia em `RequeueAsync` reprocessa tudo

**Problema.** O contrato diz "`null` = todas", mas o código também trata `[]` como todas.

**Evidência.**

```csharp
// SqlServerOutboxStore.cs:159
if (ids is { Count: > 0 }) { ... filtra ... }      // vazio cai no "todas"
// RabbitMqDeadLetterAdministration.cs:76
messageIds is { Count: > 0 } ? [.. messageIds] : null
```

**Impacto.** Uma tela de administração que envia a seleção vazia (nenhum item marcado) reprocessa todas as mortas do
Outbox ou republica toda a DLQ na fila principal.

**Sugestão.** Rejeitar coleção vazia (`ArgumentOutOfRangeException` / `Error.Validation`) e manter só `null` como "todas",
como já faz o `DiscardAsync`.

### M4 — `Requeue` entra em laço imediato até a DLQ

**Problema.** `ConsumeResult.Requeue` faz `BasicNack(requeue: true)`: o broker reentrega na hora, o handler falha de novo
(a dependência continua fora) e, depois de `DeliveryLimit` (50) entregas em poucos segundos, a fila quorum manda a
mensagem para a DLQ.

**Evidência.** `RabbitMqConsumer.cs:332-335`; documentação sugere o uso para "dependência fora do ar"
(`RabbitMqConsumer.cs:25`, `docs/rabbitmq.md:35`).

**Impacto.** Justamente no cenário recomendado (dependência fora), o resultado é CPU em laço e mensagens na DLQ em
segundos, exigindo reprocessamento manual.

**Sugestão.** Implementar `Requeue` como republicação na fila de espera do primeiro atraso **sem** incrementar
`x-tec-attempt` (ou com um cabeçalho próprio de devoluções), ou pausar o consumidor automaticamente após N `Requeue`
seguidos. No mínimo, documentar que `Requeue` deve ser combinado com `Pause()`.

### M5 — Publicador acoplado à topologia dos consumidores

**Problema.** Ao abrir o canal, o publicador declara todas as filas duráveis dos consumidores do processo. Se uma
declaração falhar (ex.: `PRECONDITION_FAILED` porque `RetryDelays` ou `DeliveryLimit` mudou numa fila existente), o canal
não é criado e **nenhuma** mensagem do Outbox é publicada.

**Evidência.** `RabbitMqPublisher.cs:90-91` (`foreach (var consumer in services.GetServices<RabbitMqConsumer>()...)
await Topology.DeclareAsync(...)`).

**Impacto.** Uma mudança de configuração de um consumidor para toda a publicação do serviço e, por A1, leva o Outbox a
`Dead`.

**Sugestão.** Declarar no publicador só a exchange; garantir as filas no `StartAsync` dos consumidores (antes do relay,
pela ordem de registro) ou capturar a falha por fila, registrar e seguir.

---

## 🟡 Severidade baixa

### B1 — Reserva pelo relógio da aplicação e prazo checado só entre mensagens

`LeaseAsync` compara `LeasedUntil < @now` com `@now = time.GetUtcNow()` da instância (`SqlServerOutboxStore.cs:26-31`).
Com relógios de servidores divergindo mais que a folga de 20% da reserva (24 s no padrão), outra instância assume um lote
ainda em publicação. O prazo também só é testado antes de cada mensagem (`OutboxRelay.cs:109`): uma publicação que
inclui criação de conexão (cofre + `CreateConnectionAsync`, até ~30 s) pode ultrapassar a reserva. Resultado: duplicidade
(tolerada por design, mas evitável). **Sugestão:** usar `SYSUTCDATETIMEOFFSET()` no SQL para `now`/`until` e passar ao
`PublishAsync` um token que expire no prazo da reserva.

### B2 — `RequeueAsync` desfaz reservas vigentes

O filtro inclui pendentes com `Attempts > 0` sem olhar `LeasedUntil` e zera `LeaseToken` (`SqlServerOutboxStore.cs:157-170`).
Uma mensagem em publicação pode ser reservada de novo por outro relay; a conclusão do primeiro é ignorada (token
diferente). **Sugestão:** acrescentar `&& (m.LeasedUntil == null || m.LeasedUntil < now)`.

### B3 — Corrida do Inbox tratada como falha

`TryBeginAsync` é "verifica e insere" (`SqlServerInboxStore.cs:23-28`). Na entrega concorrente, o perdedor recebe
`DbUpdateException` de chave duplicada, que o consumidor trata como `Retry` com log de erro 4204 e tentativa consumida
(`RabbitMqConsumer.cs:308-311`). Funciona (documentado), mas polui alertas e atrasa 30 s. **Sugestão:** expor um helper
(`IInboxStore.IsDuplicate(Exception)` ou captura de `SqlException` 2627/2601 no store) para o handler devolver
`Completed`.

### B4 — Desligamento sem drenagem

O handler recebe o `stoppingToken` (`RabbitMqConsumer.cs:302`) e é cancelado assim que o host para; o cancelamento vira
`Requeue` (NACK), e o canal é fechado em seguida (`:226`) sem esperar handlers em andamento. Cada deploy pode incrementar o
contador de entregas da quorum. **Sugestão:** no `StopAsync`, cancelar o consumo (`BasicCancel`), aguardar os handlers em
andamento por um tempo configurável e só então cancelar o token; no desligamento, deixar o canal fechar sem NACK.

### B5 — `DisposeAsync` do publicador concorre com publicação

`DisposeAsync` chama `DisposeChannelAsync` e `_lock.Dispose()` sem adquirir a trava (`RabbitMqPublisher.cs:66-72`); uma
publicação em andamento termina com `ObjectDisposedException` no `_lock.Release()` (`:62`). **Sugestão:** adquirir a
trava com tempo limite antes de descartar (como o `RabbitMqConnection` faz) e não descartar o `SemaphoreSlim`.

### B6 — Colisão de nomes das filas de espera

`Label` usa `TotalMinutes`/`Seconds` e ignora frações (`QueueDefinition.cs:71-74`): `60 s` e `60,5 s` geram `*.retry.1m`
com TTLs diferentes, e a segunda declaração falha com `PRECONDITION_FAILED`. **Sugestão:** validar que `RetryDelays` são
segundos inteiros ou usar o rótulo em milissegundos.

### B7 — Limites de tamanho incoerentes entre produção e consumo

`AmqpEnvelope.Read` compara `body.Length` depois de o cliente já ter recebido o corpo inteiro (`AmqpEnvelope.cs:77`): a
proteção real é o `max_message_size` do broker. O consumo não aplica `MaxPayloadLength`. E `MaxPayloadLength` aceita até
16 MiB (`MessagingOptions.cs:23`), mas o envelope (payload + metadados) passa do `max_message_size` padrão do RabbitMQ 4
(16 MiB): a publicação seria recusada sempre e, por A1, a mensagem morreria. **Sugestão:** limitar `MaxPayloadLength` a
um valor abaixo do limite do broker e documentar o `max_message_size`.

### B8 — Métrica `enqueued` antes do commit

`SqlServerOutbox.Enqueue` chama `metrics.Enqueued` ao adicionar ao rastreador (`SqlServerOutboxStore.cs:195`), mesmo que o
`SaveChanges` nunca aconteça; o interceptor, ao contrário, conta só em `SavedChanges`. **Sugestão:** contar no interceptor
também as `OutboxMessage` adicionadas por `IOutbox` (estado `Added` no `SavingChanges`).

### B9 — Filtros de índice com nome de coluna fixo

`HasFilter("[Status] = 0")` e `"[Status] = 1"` (`ModelBuilderExtensions.cs:43,46`) não acompanham convenções de nome
(ex.: `snake_case`), enquanto o SQL à mão usa `NamesOf`. A migration gera índice inválido. **Sugestão:** montar o filtro
com o nome de coluna resolvido (`b.Property(m => m.Status).Metadata.GetColumnName()`), ou documentar a restrição.

### B10 — Script de integração

`docker exec -e SQLCMDPASSWORD="$SQL_PASSWORD"` (`integration-setup.sh:44`) põe a senha nos argumentos do processo, ao
contrário do comentário "fora da linha de comando" (o `docker run` usa a forma segura `-e MSSQL_SA_PASSWORD`). Imagens
`mssql/server:2022-latest` e `rabbitmq:4.1` sem digest (`:20-23`). Risco baixo (runner efêmero). **Sugestão:**
`SQLCMDPASSWORD="$SQL_PASSWORD" docker exec -e SQLCMDPASSWORD ...` e fixar por digest.

### B11 — Nome do span com dado não confiável

`StartProcess` usa `envelope.Type` lido do corpo sem validar contra o catálogo (`MessagingDiagnostics.cs:32`,
`AmqpEnvelope.cs:99`): um produtor pode gerar nomes de span arbitrários (até 200 caracteres), inflando a cardinalidade
no backend de traces. **Sugestão:** validar com `MessageTypeRegistry.IsValidName` e usar o nome da fila quando inválido.

### B12 — Erros de permissão viram "fila não encontrada"

`CountMessagesAsync` captura qualquer `OperationInterruptedException` como inexistente (`RabbitMqDeadLetterAdministration.cs:148-151`);
um `ACCESS_REFUSED` (403) aparece como `FILA_NAO_ENCONTRADA`. **Sugestão:** checar `ShutdownReason.ReplyCode == 404`.

### B13 — Momento em que os eventos de domínio são descartados

O interceptor limpa os eventos em `SavedChanges` (`TecMessagingSaveChangesInterceptor.cs:69-82`). Com transação externa
(ex.: `OrmUnitOfWork` do TEC.ORM, que chama `SaveChanges` dentro da transação), isso acontece **antes** do `COMMIT`. A
atomicidade se mantém (o `OrmUnitOfWork` limpa o rastreador na falha), mas a documentação dizia "depois do commit".
Documentação corrigida (ver [Ajustes](#-ajustes-na-documentação)).

---

## ℹ️ Informativos

- **I1 — Vazão da publicação.** Uma mensagem por vez, aguardando cada confirmação sob trava (`RabbitMqPublisher.cs:46-52`):
  vazão ≈ 1/RTT por processo. Com `publisherConfirmationTrackingEnabled`, publicar o lote com várias tarefas e aguardar
  todas multiplica a vazão.
- **I2 — Prefetch sem paralelismo.** O `AsyncEventingBasicConsumer` despacha 1 mensagem por vez por canal
  (`ConsumerDispatchConcurrency` padrão): `Prefetch = 10` só antecipa a entrega. Vale documentar ou expor
  `QueueDefinition.Concurrency`.
- **I3 — `x-tec-attempt` confiado.** O cabeçalho vem da mensagem (`RabbitMqConsumer.cs:284,323`); um produtor pode mandar
  direto para a DLQ (valor alto). Aceitável num broker interno; registrar como premissa.
- **I4 — Gauges globais por instância.** `outbox.pending` etc. são contagens globais do banco emitidas por todas as
  instâncias: somar em dashboards multiplica por N. Documentar `max()` ou emitir de uma instância (M2).
- **I5 — Lacunas de teste.** Sem testes unitários (sem broker) de `AmqpEnvelope.Read/Serialize` (entrada malformada,
  limites), `QueueDefinition.Validate`/`RetryQueue`, `FinishAsync` (Retry → Reject na `MaxAttempts`, transitória),
  filtro e `lastKept` da administração da DLQ, `InboxCleanupService`, shutdown do relay (conclusão com token cancelado) e
  `RequeueAsync` com coleção vazia.

---

## ✅ Pontos fortes

- SQL à mão com nomes vindos do modelo e delimitados (`Quote` escapa `]`) e valores sempre parametrizados.
- Reserva com `UPDLOCK, READPAST, ROWLOCK` + token, conclusão condicionada ao token vigente, índice filtrado na ordem da
  reserva e teste de integração de reservas concorrentes.
- Publicação fora da transação com publisher confirms e mensagens persistentes; ack só depois do handler.
- URI do broker só pelo cofre, sem recuperação automática (relê o segredo na reconexão), circuit breaker com semântica
  cuidadosa de meia-abertura.
- Desserialização por `JsonTypeInfo` do tipo registrado (sem polimorfismo aberto), `MaxDepth` no envelope, correlação
  sanitizada.
- Build com `NuGetAudit` como erro, lock files, mapeamento de origens (contra dependency confusion) e workflows com
  permissões mínimas.

---

## 🚀 Melhorias sugeridas

| Prioridade | Melhoria | Por quê |
|:-:|---|---|
| 1 | Classificação de falhas de transporte no relay (A1) | Indisponibilidade do broker não pode matar mensagens |
| 2 | Purge em laço + estatísticas leves e eleitas (M1, M2) | Operação sustentável em volume |
| 3 | Atraso real no `Requeue` e pausa automática do consumidor (M4) | Torna seguro o cenário "dependência fora" |
| 4 | Drenagem no desligamento do consumidor (B4) | Deploy sem reentregas desnecessárias |
| 5 | Publicação em pipeline com confirms concorrentes (I1) | Vazão do relay limitada a 1/RTT hoje |
| 6 | `QueueDefinition.Concurrency` (canal com `ConsumerDispatchConcurrency` > 1) (I2) | Paralelismo controlado por fila |
| 7 | Helper de duplicidade do Inbox (`TryBeginAsync` + tratamento da chave duplicada) (B3) | Corrida vira `Completed` sem ruído |
| 8 | Ordenação opcional por chave (ex.: `PartitionKey` no envelope e reserva que não pula chaves com pendência anterior) | Consumidores que dependem de ordem por agregado |
| 9 | Exigir `amqps://` fora de `Development` (opção `RequireTls`, padrão `true`) | Hoje é só aviso na documentação |
| 10 | Testes unitários listados em I5 e teste de A1 (broker fora por N ciclos não gera `Dead`) | Proteger as garantias centrais contra regressão |

---

## 📝 Ajustes na documentação

Corrigidos nesta revisão:

| Arquivo | Antes | Agora |
|---|---|---|
| `docs/outbox.md` (tabela de desfechos) | "Broker fora: o resto do lote volta sem contar tentativa" | Explicita que as falhas da sequência contam tentativa e que indisponibilidade longa leva a `Dead` (A1) |
| `docs/outbox.md` (erros) | `Dead` só por recusa do broker | Inclui broker/transporte fora por mais de ~14 min com os padrões |
| `docs/outbox.md` (FAQ) | "só limpa os eventos depois do commit" | "depois do `SaveChanges` bem-sucedido" e o comportamento com transação externa (B13) |
| `docs/rabbitmq.md` (`Enabled`) | "publicação falha e o Outbox retém" | Publicação falha e as falhas contam tentativa: com o transporte desligado por muito tempo as mensagens viram `Dead` |
| `docs/README.md` | — | Linha da revisão técnica na tabela de temas e no mapa |

Pendentes desta lista aplicados em 2026-10-19 (ver [Correções aplicadas](#-correções-aplicadas--2026-10-19)):

- `docs/desenvolvimento.md` › Erros: acrescentar `NU1902`/`NU1903` (dependência transitiva vulnerável) com a correção por
  `PackageVersion` fixada (A2).
- `docs/seguranca.md`: "corpo AMQP limitado na leitura" — esclarecer que o limite efetivo é o `max_message_size` do broker (B7).
- `docs/rabbitmq.md`: avisar que `Requeue` deve vir com `Pause()` (M4), que mudar `RetryDelays`/`DeliveryLimit` de fila
  existente gera `PRECONDITION_FAILED` também no publicador (M5) e que `Prefetch` não paraleliza (I2).
- `docs/telemetria.md`: gauges do Outbox são globais (usar `max` entre instâncias) (I4).
- `docs/outbox.md` / `docs/rabbitmq.md`: deixar explícito que coleção vazia em `RequeueAsync` hoje significa "todas" até a
  correção de M3.


---

## 🔧 Correções aplicadas — 2026-10-19

Branch `fix/revisao-tecnica-2026-10-19`, versão `0.2.0` (minor: há mudanças incompatíveis em `0.x`, ver `CHANGELOG.md`).

| ID | Status | Arquivos | Teste de regressão |
|---|---|---|---|
| A1 | ✅ Corrigido | `Transport/IMessagePublisher.cs` (`ITransportUnavailableException`), `Outbox/OutboxRelay.cs`, `RabbitMqOptions.cs` (`RabbitMqUnavailableException`; marca em `RabbitMqCircuitOpenException`), `Internal/RabbitMqPublisher.cs` | `OutboxRelayTests`: `Transport_unavailable_never_counts_attempts_nor_kills_messages`, `Failures_that_precede_transport_unavailable_are_released_without_counting`, `Disabled_transport_keeps_messages_without_counting_attempts`; `RabbitMqUnitTests`: `Disabled_transport_fails_as_transport_unavailable`, `Broker_down_fails_as_transport_unavailable_also_with_the_circuit_open` |
| A2 | ✅ Corrigido; locks pendentes | `Directory.Packages.props` (`System.Security.Cryptography.Pkcs` 10.0.13 no net10.0; no net8.0 o pacote não entra na árvore) | Build local com `NuGetAudit` ligado: 0 avisos. Lock files do modo pacote **não** regenerados (feed `tec-interno` 401): rodar com token `dotnet restore TEC.Messaging.slnx --force-evaluate` |
| M1 | ✅ Corrigido | `OutboxRelay.cs`, `Inbox/IInboxStore.cs` (lotes enquanto vierem cheios, teto de 1 min) | `Cleanup_purges_in_batches_until_the_backlog_is_gone`, `Inbox_cleanup_purges_in_batches_until_the_backlog_is_gone` |
| M2 | ✅ Corrigido (sem eleição de instância) | `IOutboxStore.cs` (`GetPendingStatisticsAsync`), `SqlServerOutboxStore.cs`, `ModelBuilderExtensions.cs` (`IX_<tabela>_Dead`, `Attempts` no `IX_<tabela>_Pending`: exige migration) | `Statistics_indexes_cover_pending_and_dead_messages`; integração `Relay_statistics_skip_the_published_messages` |
| M3 | ✅ Corrigido (incompatível) | `SqlServerOutboxStore.cs`, `RabbitMqDeadLetterAdministration.cs`, XML docs das interfaces | `SqlServerUnitTests.Requeue_with_an_empty_selection_is_rejected`, `RabbitMqUnitTests.Requeue_with_an_empty_selection_is_rejected` |
| M4 | ✅ Corrigido (incompatível) | `RabbitMqConsumer.cs` (requeue pela fila de espera do 1º atraso, sem contar tentativa; temporária segue com NACK) | integração `Requeue_waits_in_the_wait_queue_without_consuming_attempts_or_hitting_the_dlq` |
| M5 | ✅ Corrigido | `RabbitMqPublisher.cs` (cada fila num canal próprio; falha registrada, evento 4212) | integração `Publisher_does_not_depend_on_the_topology_of_a_broken_consumer` |
| B1 | ✅ Corrigido | `SqlServerOutboxStore.cs` (`LeasedUntil` pelo relógio do banco), `OutboxRelay.cs` (token que expira em 80% da reserva) | `Publication_is_cancelled_at_the_lease_deadline_and_released_without_counting`; integração `Lease_validity_uses_the_database_clock` |
| B2 | ✅ Corrigido | `SqlServerOutboxStore.cs` | integração `Requeue_does_not_touch_messages_being_published` |
| B3 | ✅ Corrigido (helper) | `IInboxStore.IsDuplicate` (padrão `false`), `SqlServerInboxStore.cs` (2627 com o nome da PK do Inbox) | integração `Inbox_recognizes_only_its_own_duplicate_key` |
| B4 | ✅ Corrigido | `RabbitMqConsumer.cs` (`StopAsync` com drenagem, `RabbitMqOptions.DrainTimeout`; sem NACK no desligamento) | integração `Shutdown_drains_the_handler_in_progress_and_acks_it` |
| B5 | ✅ Corrigido | `RabbitMqPublisher.cs` | `Dispose_waits_for_the_publication_in_progress` |
| B6 | ✅ Corrigido | `QueueDefinition.cs` (fração de segundo → rótulo em ms) | `Wait_queue_names_never_collide` |
| B7 | ✅ Corrigido (incompatível) | `MessagingOptions.cs` (teto 4 MiB), `AmqpEnvelope.cs` (`MaxPayloadLength` no consumo), `RabbitMqConsumer.cs` | `Consumption_applies_the_payload_limit` |
| B8 | ✅ Corrigido | `SqlServerOutboxStore.cs`, `TecMessagingSaveChangesInterceptor.cs` | `Enqueued_metric_counts_only_after_a_successful_save` |
| B9 | ✅ Corrigido | `ModelBuilderExtensions.cs` | `Index_filters_use_the_real_column_name` |
| B10 | ✅ Corrigido | `.github/scripts/integration-setup.sh` (digest `mssql/server` 2022 16.0.4305.5 e `rabbitmq` 4.1.8; senha por `-e SQLCMDPASSWORD` sem valor) | `bash -n` |
| B11 | ✅ Corrigido | `MessagingDiagnostics.cs` | `Process_span_name_ignores_an_invalid_message_type` |
| B12 | ✅ Corrigido | `RabbitMqDeadLetterAdministration.cs` (`ReplyCode == 404`) | `Only_not_found_is_reported_as_missing_queue` |
| B13 | ✅ Corrigido (docs) | XML doc do interceptor, `docs/outbox.md` | — |
| I1 | ⏭️ Não corrigido | — | Melhoria de vazão (publicação em pipeline), fora do escopo de correção |
| I2 | 📝 Documentado | `docs/rabbitmq.md` | — (`Concurrency` por fila é melhoria) |
| I3 | ⏭️ Não corrigido | — | Premissa de broker interno; mudar exigiria contador assinado ou do lado do servidor |
| I4 | 📝 Documentado | `docs/telemetria.md` (agregar com `max`) | — |
| I5 | ✅ Parcial | `RabbitMqUnitTests.cs` (envelope, nomes de filas, DLQ), `SqlServerUnitTests.cs` | `FinishAsync` e filtro/`lastKept` da DLQ seguem só na integração |

Drift do tec-workflows aplicado aqui: B9 (imagens por digest) e I3 (`TEC.Vault`/`TEC.Vault.InMemory` 0.0.1 → 0.1.0). Não
aplicado: I4 (`PackageValidationBaselineVersion`), porque a baseline `TEC.Messaging 0.1.0` só existe no feed `tec-interno`
e quebraria o build sem token; decidir no template.

Documentação atualizada: `outbox.md`, `rabbitmq.md`, `seguranca.md`, `telemetria.md`, `desenvolvimento.md`,
`contratos-de-integracao.md`, `CHANGELOG.md`.

### Build e testes

| Comando | Resultado |
|---|---|
| `dotnet build TEC.Messaging.slnx -c Release -p:TecUseLocalProjects=true --no-incremental` (sem `NuGetAudit=false`) | ✅ 0 avisos, 0 erros (net8.0 e net10.0) |
| Unitários net8.0 (`--treenode-filter "/*/*/*/*[Category!=Integracao]"`) | ✅ 66/66 (39 antes + 27 novos) |
| Unitários net10.0 | ✅ 66/66 |
| Integração | ⏭️ Não executados até o fim localmente (rodam no CI). Uma execução parcial com containers revelou `SYSUTCDATETIMEOFFSET` inexistente no SQL Server; corrigido para `TODATETIMEOFFSET(SYSUTCDATETIME(), 0)`, sem nova execução |
| `dotnet restore` em modo pacote | ❌ `401` no `tec-interno`: lock files não regenerados (bloqueado) |

### Decisões para revisar

- A1: falhas seguidas **sem** a marca de transporte continuam contando tentativa (não contar travaria o Outbox atrás de
  mensagens defeituosas); só exceções marcadas (ou que têm a marca numa `InnerException`) são tratadas como indisponibilidade.
  Timeout de confirmação conta como indisponibilidade (broker bloqueado por alarme).
- M2: sem eleição de instância para as estatísticas; as consultas passaram a usar só índices filtrados pequenos.
- M4: requeue sem limite: com a dependência fora para sempre, a mensagem circula a cada `RetryDelays[0]` sem ir à DLQ.

---

## 🔎 Complemento da revisão documental — 2026-10-24

Conferência de toda a documentação contra o código atual (`0.0.1`), que cobriu também `TEC.Messaging.PostgreSql`, `TEC.Messaging.AzureServiceBus` e `TEC.Messaging.Aws`. A documentação foi corrigida para refletir o código; **o código não foi alterado**. Os itens abaixo são **suspeitas** lidas no código, ainda não reproduzidas por teste: confirme antes de corrigir.

| ID | Severidade sugerida | Local | Suspeita |
|---|---|---|---|
| C1 | Alta | `TEC.Messaging.PostgreSql/DependencyInjection/MessagingBuilderPostgreSqlExtensions.cs:33` · `Interception/TecMessagingPostgreSqlSaveChangesInterceptor.cs:33` | `UsePostgreSql<TContext>()` registra o interceptor com `TryAddSingleton<T>()`, mas o construtor é `internal` (recebe `IntegrationEnvelopeFactory`, observadores, `MessagingMetrics` e `TimeProvider`). O container só usa construtores públicos: `UseTecMessagingPostgreSql(sp)` deve falhar ao resolver o interceptor. O SQL Server usa uma fábrica (`sp => new ...`). Não há teste que resolva o registro do PostgreSQL. Também faltam a extensão de registro do observador (`IPostgreSqlDomainEventsSavingObserver`) e testes de integração |
| C2 | Média | `PostgreSqlOutboxStore.cs:34,40` · `RequeueAsync` | A reserva grava `LeasedUntil` com `NOW() AT TIME ZONE 'UTC'` (um `timestamp` sem fuso) numa coluna `timestamptz`, e o `RequeueAsync` compara `LeasedUntil` com `DateTimeOffset.UtcNow` (traduzido pelo provedor). Se a sessão do banco não estiver em UTC, o `timestamp` é interpretado no fuso da sessão: a validade gravada fica deslocada em relação a `timestamptz` reais (e entre sessões com fusos diferentes). Verificar o fuso das sessões do Npgsql ou usar `clock_timestamp()`/`now()` direto em `timestamptz` |
| C3 | Média | `AwsMessagingPublisher.cs:22` · `AzureServiceBusPublisher.cs:27` | Com `Enabled = false` (ou nuvem fora do ar) a publicação lança exceções que **não** implementam `ITransportUnavailableException`; o relay conta tentativa e leva as mensagens a `Dead` (a correção A1 só cobriu o RabbitMQ) |
| C4 | Média | `AwsSqsConsumer.cs:60` · `AzureServiceBusConsumer.cs:59` | `GetSqsAsync`/`GetClientAsync` ficam **fora** do `try` do laço: segredo ausente ou credencial inválida lança do `ExecuteAsync` e, com o comportamento padrão do host, derruba a aplicação. O consumidor do RabbitMQ reabre com espera exponencial |
| C5 | Baixa | `AwsSqsConsumer.cs` · `AzureServiceBusConsumer.cs` | Exceções do handler viram `Retry` sem nenhum log; os consumidores não registram `consumer.messages`/`consumer.duration`, não aplicam `MaxPayloadLength` nem validam o envelope recebido (`Validate`); os consumidores ignoram `Enabled` |
| C6 | Baixa | `AwsSqsConsumer.cs:132` · `AzureServiceBusConsumer.cs:110` | `OccurredAt` do envelope recebido é a hora do recebimento (AWS) ou o `EnqueuedTime` (Azure), não o `occurredAt` publicado no atributo/propriedade |
| C7 | Baixa | `AwsMessagingPublisher.cs:25-27` · `AwsMessagingOptions.cs` | `DefaultDestination` vazio faz o destino ser o `Type` do contrato, que não é ARN nem URL de fila: a publicação falha. Não há validação na subida |
| C8 | Baixa | `AwsSqsConsumer.cs:156-166` | `Reject` e `Requeue` usam o mesmo `ChangeMessageVisibility(0)`: sem `RedrivePolicy` na fila a mensagem rejeitada reaparece sem parar, e com `Requeue` o retorno é imediato (laço) |
| C9 | Info | `TEC.Messaging.Aws.csproj:19` · `TEC.Messaging.AzureServiceBus.csproj:18` | `Polly.Core` é referenciado mas não é usado no código dos dois satélites |

**Reconferência em 2026-10-29 (leitura do código, sem executar testes).** O código mudou depois da tabela acima; estado atual de cada suspeita:

| ID | Estado | Observação |
|---|---|---|
| C1 | Resolvido no código | O interceptor do PostgreSQL passou a ser registrado por fábrica e há teste de resolução (`MessagingIssueRegressionTests`). Segue sem extensão `AddDomainEventsObserver` e sem integração com PostgreSQL real |
| C2 | Resolvido, com outra divergência | A reserva usa `NOW()` (`timestamptz`). Em compensação, `PostgreSqlOutboxStore.RequeueAsync` compara `LeasedUntil` com o relógio da aplicação, ao contrário do SQL Server |
| C3 | Resolvido, com efeito colateral | As exceções do Azure e da AWS implementam `ITransportUnavailableException`, mas por serem usadas para toda falha do SDK, uma mensagem recusada nunca conta tentativa e bloqueia o Outbox (ver [ADR 0003](adr/0003-falhas-isoladas-backoff-e-transporte-indisponivel-sem-contar-tentativa.md)) |
| C4 | Aberto só na AWS | `AwsSqsConsumer` ainda chama `GetSqsAsync` fora do `try`; o consumidor do Azure agora tenta de novo a cada 5 s |
| C5 | Aberto | Sem log das exceções do handler, sem métricas de consumo, sem `MaxPayloadLength` nem `Validate`, e `Enabled` ignorado nos dois consumidores |
| C6 | Resolvido | Os dois consumidores leem `occurred-at`/`occurredAt` antes de usar o horário de recebimento |
| C7 | Aberto | Sem validação de `DefaultDestination` na subida |
| C8 | Aberto | `Reject` e `Requeue` seguem iguais na AWS |
| C9 | Aberto | `Polly.Core` segue referenciado e sem uso no Azure e na AWS |

Também observado e já corrigido na documentação (não é defeito do código): nomes e padrões de API divergentes em `docs/aws.md` e `docs/azureservicebus.md` (`UseAwsMessaging`, `AddConsumer`, `AddTecAwsMessaging`, `CredentialsSecretName`, `MaxConcurrency`, `AwsSqsQueueDefinition`, `TopicOrQueuePrefix`, `AzureServiceBusSubscription` não existem), `docs/postgresql.md` (a extensão que liga o interceptor é `UseTecMessagingPostgreSql`; o SQL de reserva e os nomes dos índices estavam diferentes do código) e o texto de mascaramento do sanitizador (`***REDACTED***`, não `[REDACTED]`).

---
⬅️ [🛠️ Desenvolvimento](desenvolvimento.md) · [📚 Índice](README.md) · [🏛️ ADRs](adr/README.md) ➡️
