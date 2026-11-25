#!/usr/bin/env bash
# Полный прогон стенда. Запуск из корня репозитория: bash scripts/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
TRINO="docker compose exec -T trino trino --output-format ALIGNED"

echo "== 0. venv"
[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install -q -r requirements.txt

echo "== 1. docker compose up"
docker compose up -d
until curl -sf http://localhost:8083/connectors >/dev/null; do sleep 3; done
until $TRINO --execute "SELECT 1" >/dev/null 2>&1; do sleep 3; done

echo "== 2. Iceberg DDL (bronze/silver/gold/features)"
$TRINO -f /dev/stdin < sql/lakehouse/01_ddl.sql

echo "== 3. Debezium: регистрация коннектора -> initial snapshot"
curl -s -X POST -H 'Content-Type: application/json' --data @scripts/debezium-connector.json http://localhost:8083/connectors || true
sleep 15
curl -s http://localhost:8083/connectors/vzhuh-oltp/status; echo

echo "== 4. CDC -> SCD: снапшот справочника"
(cd pipelines && ../$PY cdc_to_scd2.py)

echo "== 5. Изменения в источнике (переезды, тарифы, блокировки, удаление)"
docker compose exec -T pg-oltp psql -U app -d vzhuh < sql/oltp/02_changes.sql
sleep 5
(cd pipelines && ../$PY cdc_to_scd2.py)
$TRINO -f /dev/stdin < sql/lakehouse/02_history_queries.sql

echo "== 6. Поток событий: генерация с дублями/опозданиями -> Iceberg"
for tp in app.events telemetry.raw; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic $tp --partitions 3 --replication-factor 1
done
# потребитель работает параллельно с генератором (живой поток, ~400 сообщений/с)
(cd pipelines && ../$PY events_to_iceberg.py) &
sleep 4
(cd pipelines && ../$PY events_generator.py 400)
wait
$TRINO -f /dev/stdin < sql/lakehouse/03_gold_daily.sql
$TRINO -f /dev/stdin < sql/lakehouse/04_quality_checks.sql

echo "== 7. Feature Store: offline (Iceberg) + PIT training set + online (Redis)"
$TRINO -f /dev/stdin < sql/lakehouse/05_features.sql
$PY features/push_online.py

echo "== 8. RAG: ingest -> pgvector, оценка поиска, промпт"
(cd rag && ../$PY ingest.py && ../$PY search.py && ../$PY search.py "Какой штраф за самокат на проезжей части?")

docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
