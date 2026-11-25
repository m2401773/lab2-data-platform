-- Суточный batch-отчёт (D+1): считаем из silver.events — там уже есть опоздавшие и нет дублей.
-- Город и тариф берём на момент поездки из SCD2 (point-in-time join), а не «текущий».
CREATE OR REPLACE TABLE lakehouse.gold.daily_rides AS
WITH rides AS (
    SELECT ride_id, client_id,
           min(event_time) FILTER (WHERE event_type = 'ride_start') AS started_at,
           max(event_time) FILTER (WHERE event_type = 'ride_end')   AS finished_at,
           max(amount_rub)                                         AS amount_rub
    FROM lakehouse.silver.events
    GROUP BY ride_id, client_id
)
SELECT date(r.started_at AT TIME ZONE 'Europe/Moscow') AS ride_date,
       c.city, c.tariff_code,
       count(*)                       AS rides,
       count(r.finished_at)           AS finished_rides,
       round(sum(r.amount_rub), 2)    AS revenue_rub
FROM rides r
JOIN lakehouse.silver.dim_client c
  ON c.client_id = r.client_id
 AND r.started_at >= c.valid_from AND r.started_at < c.valid_to
WHERE r.started_at IS NOT NULL
GROUP BY 1, 2, 3;
