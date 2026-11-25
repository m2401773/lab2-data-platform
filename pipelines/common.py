"""Общие хелперы стенда: подключения и безопасные SQL-литералы для Trino."""
from __future__ import annotations

import datetime as dt
import json
import os

import trino

TRINO = dict(host=os.getenv("TRINO_HOST", "localhost"), port=int(os.getenv("TRINO_PORT", "8080")),
             user="pipeline", catalog="lakehouse")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
PG_META = os.getenv("PG_META_DSN", "postgresql://meta:meta@localhost:5434/meta")
HIGH_DATE = "9999-12-31 00:00:00.000000 UTC"


def trino_conn():
    return trino.dbapi.connect(**TRINO)


def run(cur, sql: str):
    cur.execute(sql)
    try:
        return cur.fetchall()
    except Exception:  # DDL без результата
        return []


def run_file(cur, path: str):
    with open(path, encoding="utf-8") as f:
        text = "\n".join(l for l in f.read().splitlines() if not l.strip().startswith("--"))
    for stmt in [s.strip() for s in text.split(";") if s.strip()]:
        run(cur, stmt)


def ts_lit(value) -> str:
    """datetime/ISO-строка -> литерал TIMESTAMP WITH TIME ZONE в UTC."""
    if value is None:
        return "CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE)"
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value)
    if isinstance(value, (int, float)):  # epoch ms
        value = dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc)
    value = value.astimezone(dt.timezone.utc)
    return f"TIMESTAMP '{value:%Y-%m-%d %H:%M:%S.%f} UTC'"


def lit(value, sql_type: str) -> str:
    if value is None:
        return f"CAST(NULL AS {sql_type})"
    if sql_type.startswith("TIMESTAMP"):
        return ts_lit(value)
    if sql_type == "BOOLEAN":
        return "TRUE" if value else "FALSE"
    if sql_type in ("INTEGER", "BIGINT", "DOUBLE"):
        return f"CAST({value} AS {sql_type})"
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False)
    return "'" + value.replace("'", "''") + "'"


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)
