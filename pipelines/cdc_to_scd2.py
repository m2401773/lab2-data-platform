"""CDC (Debezium -> Kafka) -> Iceberg: bronze.cdc_raw + silver.dim_client (SCD2/SCD1) + silver.dim_scooter (SCD1).

Учебная замена Spark Structured Streaming с trigger(availableNow=True):
читаем Kafka микробатчами, на каждый микробатч — ОДИН атомарный MERGE в Iceberg,
offsets коммитим только после успешной записи (at-least-once + идемпотентность по LSN = effectively-once).
"""
from __future__ import annotations

import json
from collections import defaultdict

from confluent_kafka import Consumer

from common import HIGH_DATE, KAFKA, lit, now_utc, run, trino_conn, ts_lit

TOPICS = ["oltp.public.clients", "oltp.public.scooters"]
SCD2_ATTRS = ("city", "tariff_code", "status")      # меняют смысл для аналитики -> новая версия
SCD1_ATTRS = ("full_name", "phone", "email")        # технические/исправления -> перезапись
BATCH = 200


def event_ts(ev: dict):
    """Бизнес-время изменения: updated_at из строки, для delete — время коммита в источнике."""
    if ev.get("after") and ev["after"].get("updated_at"):
        return ev["after"]["updated_at"]
    return ev["source"]["ts_ms"]


def write_bronze(cur, msgs):
    rows = []
    ing = now_utc()
    for m, ev in msgs:
        rows.append("(" + ",".join([
            lit(m.topic(), "VARCHAR"), lit(m.partition(), "INTEGER"), lit(m.offset(), "BIGINT"),
            lit(ev["op"], "VARCHAR"), lit(ev["source"]["lsn"], "BIGINT"), ts_lit(ev["source"]["ts_ms"]),
            lit(m.key().decode() if m.key() else None, "VARCHAR"),
            lit(json.dumps(ev.get("before"), ensure_ascii=False), "VARCHAR"),
            lit(json.dumps(ev.get("after"), ensure_ascii=False), "VARCHAR"), ts_lit(ing)]) + ")")
    run(cur, "INSERT INTO bronze.cdc_raw VALUES " + ",".join(rows))


def apply_clients(cur, events: list[dict]) -> dict:
    """SCD2-хеш одним MERGE: строки с merge_key=client_id закрывают/правят текущую версию,
    строки с merge_key=NULL вставляют новые версии."""
    by_key = defaultdict(list)
    for ev in events:
        key = (ev["after"] or ev["before"])["client_id"]
        by_key[key].append(ev)
    keys = ",".join(str(k) for k in by_key)

    # защита от повторной обработки (replay после падения до коммита offsets)
    applied = dict(run(cur, f"SELECT client_id, max(updated_lsn) FROM silver.dim_client "
                            f"WHERE client_id IN ({keys}) GROUP BY client_id"))
    has_current = {r[0] for r in run(cur, f"SELECT client_id FROM silver.dim_client "
                                          f"WHERE is_current AND client_id IN ({keys})")}

    stats = defaultdict(int)
    table_actions = {}   # client_id -> {close_ts, t1:{...}, lsn}
    inserts = []         # новые версии
    for key, evs in by_key.items():
        evs.sort(key=lambda e: e["source"]["lsn"])
        batch_version = None  # последняя версия, созданная в этом батче
        for ev in evs:
            lsn = ev["source"]["lsn"]
            if lsn <= (applied.get(key) or -1):
                stats["skipped_replay"] += 1
                continue
            op, before, after, ts = ev["op"], ev["before"], ev["after"], event_ts(ev)
            is_scd2 = op in ("c", "r") or op == "d" or any(before.get(a) != after.get(a) for a in SCD2_ATTRS)

            if op in ("u", "d") and is_scd2:      # закрываем предыдущую версию
                if batch_version is not None:
                    batch_version.update(valid_to=ts, is_current=False, updated_lsn=lsn)
                elif key in has_current:
                    table_actions.setdefault(key, {}).update(close_ts=ts, lsn=lsn)
                stats["closed"] += 1
            if op in ("c", "r", "u") and is_scd2:  # новая версия
                batch_version = dict(after, valid_from=ts, valid_to=None, is_current=True,
                                     src_lsn=lsn, updated_lsn=lsn)
                inserts.append(batch_version)
                stats["versions_inserted"] += 1
            elif op == "u":                        # только SCD1-атрибуты -> перезапись
                t1 = {a: after[a] for a in SCD1_ATTRS}
                if batch_version is not None:
                    batch_version.update(t1, updated_lsn=lsn)
                else:
                    table_actions.setdefault(key, {}).setdefault("t1", {}).update(t1)
                    table_actions[key]["lsn"] = lsn
                stats["scd1_overwrites"] += 1

    if not table_actions and not inserts:
        return dict(stats)

    T = dict(client_id="INTEGER", full_name="VARCHAR", phone="VARCHAR", email="VARCHAR", city="VARCHAR",
             tariff_code="VARCHAR", status="VARCHAR", valid_from="TIMESTAMP", valid_to="TIMESTAMP",
             is_current="BOOLEAN", src_lsn="BIGINT", updated_lsn="BIGINT")
    rows = []
    for key, a in table_actions.items():   # строки для MATCHED
        t1 = a.get("t1", {})
        rows.append([lit(key, "INTEGER"), ts_lit(a.get("close_ts")), lit(t1.get("full_name"), "VARCHAR"),
                     lit(t1.get("phone"), "VARCHAR"), lit(t1.get("email"), "VARCHAR"), lit(a["lsn"], "BIGINT")]
                    + [lit(None, t if t != "TIMESTAMP" else "TIMESTAMP(6) WITH TIME ZONE") for t in T.values()])
    for v in inserts:                      # строки для NOT MATCHED
        v = dict(v, valid_to=v["valid_to"] or HIGH_DATE.replace(" UTC", "+00:00"))
        rows.append([lit(None, "INTEGER"), lit(None, "TIMESTAMP"), lit(None, "VARCHAR"), lit(None, "VARCHAR"),
                     lit(None, "VARCHAR"), lit(None, "BIGINT")] + [lit(v[c], t) for c, t in T.items()])
    values = ",\n".join("(" + ",".join(r) + ")" for r in rows)
    cols = "merge_key, close_ts, t1_full_name, t1_phone, t1_email, action_lsn, " + ", ".join(T)

    run(cur, f"""
MERGE INTO silver.dim_client t
USING (VALUES {values}) AS s({cols})
ON t.client_id = s.merge_key AND t.is_current
WHEN MATCHED THEN UPDATE SET
    full_name   = coalesce(s.t1_full_name, t.full_name),
    phone       = coalesce(s.t1_phone, t.phone),
    email       = coalesce(s.t1_email, t.email),
    valid_to    = coalesce(s.close_ts, t.valid_to),
    is_current  = (s.close_ts IS NULL),
    updated_lsn = s.action_lsn
WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT
    ({", ".join(T)}) VALUES ({", ".join("s." + c for c in T)})""")
    return dict(stats)


def apply_scooters(cur, events: list[dict]) -> int:
    """SCD1: храним только последнее состояние."""
    last = {}
    for ev in sorted(events, key=lambda e: e["source"]["lsn"]):
        key = (ev["after"] or ev["before"])["scooter_id"]
        last[key] = ev
    rows = []
    for key, ev in last.items():
        a = ev["after"] or {}
        rows.append("(" + ",".join([lit(key, "INTEGER"), lit(a.get("model"), "VARCHAR"), lit(a.get("zone"), "VARCHAR"),
                                    lit(a.get("status"), "VARCHAR"), ts_lit(a.get("updated_at")),
                                    lit(ev["source"]["lsn"], "BIGINT"), lit(ev["op"] == "d", "BOOLEAN")]) + ")")
    run(cur, f"""
MERGE INTO silver.dim_scooter t
USING (VALUES {",".join(rows)}) AS s(scooter_id, model, zone, status, updated_at, src_lsn, is_deleted)
ON t.scooter_id = s.scooter_id
WHEN MATCHED AND s.is_deleted THEN DELETE
WHEN MATCHED AND s.src_lsn > t.src_lsn THEN UPDATE SET
    model = s.model, zone = s.zone, status = s.status, updated_at = s.updated_at, src_lsn = s.src_lsn
WHEN NOT MATCHED AND NOT s.is_deleted THEN INSERT VALUES
    (s.scooter_id, s.model, s.zone, s.status, s.updated_at, s.src_lsn)""")
    return len(rows)


def main():
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "scd-writer",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe(TOPICS)
    cur = trino_conn().cursor()
    run(cur, "USE lakehouse.silver")
    batch_no, idle = 0, 0
    while idle < 3:  # availableNow: дочитываем то, что есть, и выходим
        msgs = [m for m in consumer.consume(num_messages=BATCH, timeout=5) if not m.error()]
        if not msgs:
            idle += 1
            continue
        idle = 0
        batch_no += 1
        parsed = [(m, json.loads(m.value())) for m in msgs if m.value()]  # tombstone (value=None) пропускаем
        if parsed:
            write_bronze(cur, parsed)
            clients = [ev for m, ev in parsed if m.topic().endswith("clients")]
            scooters = [ev for m, ev in parsed if m.topic().endswith("scooters")]
            st = apply_clients(cur, clients) if clients else {}
            sc = apply_scooters(cur, scooters) if scooters else 0
            print(f"batch {batch_no}: msgs={len(msgs)} clients={st} scooters_upserted={sc}")
        consumer.commit(asynchronous=False)   # offsets — только после успешного MERGE
    consumer.close()
    print("CDC -> SCD: done")


if __name__ == "__main__":
    main()
