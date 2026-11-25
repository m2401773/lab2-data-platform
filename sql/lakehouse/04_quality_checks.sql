-- Проверки «без потерь и без задвоений»
-- bronze (как пришло) vs silver (дедуп): разница = дубли
SELECT (SELECT count(*) FROM lakehouse.bronze.events_raw)                    AS bronze_rows,
       (SELECT count(DISTINCT event_id) FROM lakehouse.bronze.events_raw)    AS bronze_unique_ids,
       (SELECT count(*) FROM lakehouse.silver.events)                        AS silver_rows,
       (SELECT count(*) - count(DISTINCT event_id) FROM lakehouse.silver.events) AS silver_duplicates,
       (SELECT count(*) FROM lakehouse.bronze.events_raw WHERE late_for_live) AS late_for_live;

-- Итог отчёта за февраль
SELECT sum(rides) AS rides, sum(revenue_rub) AS revenue
FROM lakehouse.gold.daily_rides WHERE ride_date BETWEEN DATE '2026-02-01' AND DATE '2026-02-28';
