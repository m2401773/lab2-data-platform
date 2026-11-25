"""ONLINE STORE: материализация признаков в Redis + проверка consistency offline == online.

Учебная замена `feast materialize-incremental` (batch-признаки) и Flink push (streaming-признаки).
Запрос модели при старте поездки = HGETALL по ключу клиента (миллисекунды, без обращения к Lakehouse).
"""
import sys

import redis

sys.path.insert(0, "pipelines")
from common import run, trino_conn  # noqa: E402

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
cur = trino_conn().cursor()

# «Сейчас» для стенда = начало последнего дня данных (события у нас исторические)
AS_OF = "TIMESTAMP '2026-03-23 00:00:00 Europe/Moscow'"

rows = run(cur, f"""
SELECT f.client_id, f.rides_30d, f.avg_amount_30d, dc.tariff_code,
       (SELECT count(DISTINCT e.ride_id) FROM lakehouse.silver.events e
         WHERE e.client_id = f.client_id AND e.event_type = 'ride_start'
           AND e.event_time < {AS_OF} AND e.event_time >= {AS_OF} - INTERVAL '24' HOUR) AS rides_24h
FROM lakehouse.features.client_30d f
JOIN lakehouse.silver.dim_client dc ON dc.client_id = f.client_id AND dc.is_current
WHERE f.feature_ts = {AS_OF}""")

pipe = r.pipeline()
for client_id, rides_30d, avg30, tariff, rides24 in rows:
    key = f"preauth:client:{client_id}"
    pipe.hset(key, mapping={"f_rides_30d": rides_30d, "f_avg_amount_30d": float(avg30),
                            "f_tariff": tariff, "f_rides_24h": rides24, "feature_ts": "2026-03-23T00:00+03:00"})
    pipe.expire(key, 2 * 24 * 3600)   # TTL: устаревший признак лучше не отдавать модели
pipe.execute()
print(f"materialized {len(rows)} clients to Redis")

# Consistency check: то, что модель получит онлайн, совпадает с offline-снимком на тот же момент
offline = {row[0]: (row[1], float(row[2])) for row in run(cur, f"""
    SELECT client_id, rides_30d, avg_amount_30d FROM lakehouse.features.client_30d WHERE feature_ts = {AS_OF}""")}
mismatch = 0
for cid, (rides, avg) in offline.items():
    online = r.hgetall(f"preauth:client:{cid}")
    if online and (int(online["f_rides_30d"]), float(online["f_avg_amount_30d"])) != (rides, avg):
        mismatch += 1
print(f"online/offline consistency: {len(offline)} clients checked, mismatches={mismatch}")
print("пример запроса модели при старте поездки клиента 1:", r.hgetall("preauth:client:1"))
