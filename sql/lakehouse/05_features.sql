-- OFFLINE STORE: batch feature view «клиент за 30 дней», снимок на начало каждого дня.
-- feature_ts = начало дня: признак посчитан только по данным ДО этого момента -> нет утечки будущего.
CREATE OR REPLACE TABLE lakehouse.features.client_30d AS
WITH rides AS (
    SELECT ride_id, client_id,
           min(event_time) FILTER (WHERE event_type = 'ride_start') AS started_at,
           max(amount_rub) AS amount_rub
    FROM lakehouse.silver.events GROUP BY ride_id, client_id
),
days AS (
    SELECT with_timezone(CAST(d AS TIMESTAMP(6)), 'Europe/Moscow') AS feature_ts
    FROM UNNEST(sequence(DATE '2026-02-01', DATE '2026-03-23', INTERVAL '1' DAY)) AS t(d)
),
clients AS (SELECT DISTINCT client_id FROM lakehouse.silver.dim_client)
SELECT c.client_id, d.feature_ts,
       count(r.ride_id)                         AS rides_30d,
       coalesce(round(avg(r.amount_rub), 2), 0) AS avg_amount_30d,
       coalesce(max(r.amount_rub), 0)           AS max_amount_30d
FROM clients c CROSS JOIN days d
LEFT JOIN rides r
  ON r.client_id = c.client_id
 AND r.started_at <  d.feature_ts
 AND r.started_at >= d.feature_ts - INTERVAL '30' DAY
GROUP BY c.client_id, d.feature_ts;

-- TRAINING SET: point-in-time join (аналог feast get_historical_features).
-- Сущность = поездка на момент старта; label = итоговая сумма (модель предавторизации).
CREATE OR REPLACE TABLE lakehouse.features.training_preauth AS
WITH rides AS (
    SELECT ride_id, client_id,
           min(event_time) FILTER (WHERE event_type = 'ride_start') AS event_ts,
           max(amount_rub) AS label_amount
    FROM lakehouse.silver.events GROUP BY ride_id, client_id
)
SELECT r.ride_id, r.client_id, r.event_ts, r.label_amount,
       -- 1) из SCD2: тариф, действовавший в момент поездки
       dc.tariff_code AS f_tariff,
       -- 2) batch-признаки: последний снимок feature_ts <= event_ts (TTL 1 день)
       f.rides_30d    AS f_rides_30d,
       f.avg_amount_30d AS f_avg_amount_30d,
       -- 3) «streaming»-признак: поездки клиента за 24 ч строго до старта
       (SELECT count(*) FROM rides r2
         WHERE r2.client_id = r.client_id AND r2.event_ts < r.event_ts
           AND r2.event_ts >= r.event_ts - INTERVAL '24' HOUR) AS f_rides_24h
FROM rides r
JOIN lakehouse.silver.dim_client dc
  ON dc.client_id = r.client_id AND r.event_ts >= dc.valid_from AND r.event_ts < dc.valid_to
LEFT JOIN lakehouse.features.client_30d f
  ON f.client_id = r.client_id
 AND f.feature_ts = (date_trunc('day', r.event_ts AT TIME ZONE 'Europe/Moscow'))
WHERE r.event_ts IS NOT NULL AND r.label_amount IS NOT NULL;

-- Демонстрация утечки: если взять «текущий» тариф вместо исторического — сколько строк обучения станут неверными
SELECT count(*) AS rows_total,
       count_if(t.f_tariff <> cur.tariff_code) AS rows_with_wrong_tariff_if_naive_join
FROM lakehouse.features.training_preauth t
JOIN lakehouse.silver.dim_client cur ON cur.client_id = t.client_id AND cur.is_current;
