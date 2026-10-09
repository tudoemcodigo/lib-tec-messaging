#!/usr/bin/env bash
# Prepara a integração do TEC.Messaging no CI (chamado pelo dotnet-test.yml do tec-workflows, antes do build).
#
# 1. SQL Server descartável em container, com senha aleatória por execução (mascarada no log e passada ao container por
#    variável de ambiente, fora da linha de comando) e banco tec_testes.
# 2. RabbitMQ descartável em container (4.x, filas quorum), com usuário e senha aleatórios e vhost tec-testes.
# Conexões entregues aos testes em TEC_TESTES_MESSAGING_SQL_CONEXAO e TEC_TESTES_MESSAGING_RABBITMQ_URI.
set -euo pipefail

SQL_PASSWORD="Tec1!$(openssl rand -hex 16)"
RABBIT_USER="tec$(openssl rand -hex 4)"
RABBIT_PASSWORD="$(openssl rand -hex 16)"
echo "::add-mask::$SQL_PASSWORD"
echo "::add-mask::$RABBIT_PASSWORD"
export MSSQL_SA_PASSWORD="$SQL_PASSWORD"
export RABBITMQ_DEFAULT_USER="$RABBIT_USER"
export RABBITMQ_DEFAULT_PASS="$RABBIT_PASSWORD"
export RABBITMQ_DEFAULT_VHOST="tec-testes"

docker run -d --name tec-mssql-ci -p 127.0.0.1:1433:1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD \
  mcr.microsoft.com/mssql/server:2022-latest >/dev/null
docker run -d --name tec-rabbitmq-ci -p 127.0.0.1:5672:5672 -e RABBITMQ_DEFAULT_USER -e RABBITMQ_DEFAULT_PASS \
  -e RABBITMQ_DEFAULT_VHOST rabbitmq:4.1 >/dev/null

# Log lido numa variável: com pipefail, "docker logs | grep -q" falha por SIGPIPE quando o grep encerra antes
wait_for() {
  local container="$1" pattern="$2" logs
  for _ in $(seq 1 90); do
    logs="$(docker logs "$container" 2>&1)"
    if grep -q "$pattern" <<<"$logs"; then
      return 0
    fi
    sleep 2
  done
  echo "::error::$container não ficou pronto em 3 minutos."
  docker logs "$container" 2>&1 | tail -n 30
  return 1
}

wait_for tec-mssql-ci 'SQL Server is now ready for client connections'
wait_for tec-rabbitmq-ci 'Server startup complete'

for attempt in $(seq 1 10); do
  if docker exec -e SQLCMDPASSWORD="$SQL_PASSWORD" tec-mssql-ci /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b \
      -Q "IF DB_ID(N'tec_testes') IS NULL CREATE DATABASE tec_testes;" >/dev/null; then
    break
  fi
  if [ "$attempt" -eq 10 ]; then
    echo "::error::Não foi possível criar o banco tec_testes."
    exit 1
  fi
  sleep 3
done

echo "TEC_TESTES_MESSAGING_SQL_CONEXAO=Server=localhost,1433;Database=tec_testes;User ID=sa;Password=$SQL_PASSWORD;Encrypt=True;TrustServerCertificate=True" >> "$GITHUB_ENV"
echo "TEC_TESTES_MESSAGING_RABBITMQ_URI=amqp://$RABBIT_USER:$RABBIT_PASSWORD@127.0.0.1:5672/tec-testes" >> "$GITHUB_ENV"
