#!/usr/bin/env bash
# Remove o SQL Server e o RabbitMQ descartáveis. Chamado sempre no fim do job, mesmo com falha.
set -uo pipefail

docker rm -f tec-mssql-ci tec-rabbitmq-ci >/dev/null 2>&1 || true
