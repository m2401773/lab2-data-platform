"""Поток событий Kafka -> Iceberg (bronze/silver) + живые метрики в serving.live_metrics.

Учебная замена Flink-джобы. Повторяет её логику:
* event-time + watermark = min по партициям(max event_time) - 10 мин (как per-partition watermarks во Flink);
* дедупликация в потоке: keyed state event_id с TTL 48 ч -> живые метрики без задвоений;
* опоздавшие (event_time < watermark) не попадают в живое окно (side output), НО пишутся в Iceberg;
* silver.events: MERGE ... WHEN NOT MATCHED по event_id -> идемпотентно, повторная доставка не задваивает;
* offsets коммитятся после записи в Iceberg (аналог checkpoint + двухфазного коммита Iceberg sink).
"""
from __future__ import annotations

import datetime as dt
import json
from collections import Counter

import psycopg
from confluent_kafka import Consumer

from common import KAFKA, PG_META, lit, now_utc, run, trino_conn, ts_lit

TOPICS = ["app.events", "telemetry.raw"]
ALLOWED_LATENESS = dt.timedelta(minutes=10)
STATE_TTL = dt.timedelta(hours=48)
BATCH = 500


class Watermarks:
    """Монотонный watermark: min по ВСЕМ партициям (пока не видели каждую — watermark не определён)."""
    def __init__(self, expected_partitions: int):
        self.expected = expected_partitions
        self.max_by_partition: dict[tuple, dt.datetime] = {}
        self.current = None

    def observe(self, tp, t):
        if tp not in self.max_by_partition or t > self.max_by_partition[tp]:
            self.max_by_partition[tp] = t
        if len(self.max_by_partition) >= self.expected:
            candidate = min(self.max_by_partition.values()) - ALLOWED_LATENESS
            if self.current is None or candidate > self.current:
                self.current = candidate

    @property
    def value(self):
        return self.current


def main():
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "events-writer",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False,
                         # маленькие выборки -> партиции читаются вперемешку (ближе к живому потоку,
                         # а не «одна партиция целиком» при догоне бэклога)
                         "max.partition.fetch.bytes": 4096})
    consumer.subscribe(TOPICS)
    n_partitions = sum(len(consumer.list_topics(t).topics[t].partitions) for t in TOPICS)
    cur = trino_conn().cursor()
    run(cur, "USE lakehouse.silver")
    pg = psycopg.connect(PG_META, autocommit=True)
    wm, seen = Watermarks(n_partitions), {}
    totals = Counter()
    idle = 0
    while idle < 3:
        msgs = [m for m in consumer.consume(num_messages=BATCH, timeout=5) if not m.error()]
        if not msgs:
            idle += 1
            continue
        idle = 0
        ing = now_utc()
        bronze, silver, live = [], {}, Counter()
        for m in msgs:
            e = json.loads(m.value())
            t = dt.datetime.fromisoformat(e["event_time"])
            w = wm.value
            late = w is not None and t < w
            wm.observe((m.topic(), m.partition()), t)
            bronze.append("(" + ",".join([lit(e["event_id"], "VARCHAR"), lit(e["event_type"], "VARCHAR"), ts_lit(t),
                                          lit(m.value().decode(), "VARCHAR"), lit(m.partition(), "INTEGER"),
                                          lit(m.offset(), "BIGINT"), lit(late, "BOOLEAN"), ts_lit(ing)]) + ")")
            totals["received"] += 1
            if e["event_id"] in seen:             # дубль -> в живые метрики не идёт
                totals["dup_in_stream"] += 1
                continue
            seen[e["event_id"]] = t
            silver[e["event_id"]] = e
            if late:
                totals["late_side_output"] += 1   # в живое окно опоздало, но в Iceberg попадёт
            else:
                live[(t.replace(minute=0, second=0, microsecond=0), e["event_type"])] += 1

        # 1) bronze: всё как пришло
        run(cur, "INSERT INTO bronze.events_raw VALUES " + ",".join(bronze))
        # 2) silver: идемпотентный upsert по event_id
        rows = []
        for e in silver.values():
            rows.append("(" + ",".join([
                lit(e["event_id"], "VARCHAR"), lit(e["event_type"], "VARCHAR"), lit(e["ride_id"], "VARCHAR"),
                lit(e["client_id"], "INTEGER"), lit(e["scooter_id"], "INTEGER"), ts_lit(e["event_time"]),
                lit(e.get("battery"), "INTEGER"), lit(e.get("speed_kmh"), "DOUBLE"), lit(e.get("amount_rub"), "DOUBLE"),
                ts_lit(ing)]) + ")")
        if rows:
            res = run(cur, f"""
MERGE INTO silver.events t
USING (VALUES {",".join(rows)}) AS s(event_id, event_type, ride_id, client_id, scooter_id,
                                    event_time, battery, speed_kmh, amount_rub, ingested_at)
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT VALUES (s.event_id, s.event_type, s.ride_id, s.client_id, s.scooter_id,
                                    s.event_time, s.battery, s.speed_kmh, s.amount_rub, s.ingested_at)""")
            totals["silver_inserted"] += res[0][0] if res else 0
        # 3) живой мониторинг: инкремент окон (event-time, 1 час)
        with pg.cursor() as c:
            for (win, etype), n in live.items():
                c.execute("""INSERT INTO serving.live_metrics(window_start, event_type, events_cnt)
                             VALUES (%s, %s, %s)
                             ON CONFLICT (window_start, event_type)
                             DO UPDATE SET events_cnt = serving.live_metrics.events_cnt + EXCLUDED.events_cnt,
                                           updated_at = now()""", (win, etype, n))
        consumer.commit(asynchronous=False)
        # TTL состояния дедупликации
        if wm.value:
            for k in [k for k, t in seen.items() if t < wm.value - STATE_TTL]:
                del seen[k]
        print(f"batch: {len(msgs)} msgs, watermark={wm.value and f"{wm.value:%Y-%m-%d %H:%M}"}, totals={dict(totals)}")
    consumer.close()
    print("events -> iceberg: done", dict(totals))


if __name__ == "__main__":
    main()
