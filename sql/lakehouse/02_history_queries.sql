-- Вопросы бухгалтерии и аналитиков к SCD2 (выполняются в Trino, источник не трогаем).

-- 1) Какие город/тариф/статус были у клиентов на 1 марта 2026 (МСК)?
SELECT client_id, city, tariff_code, status,
       valid_from AT TIME ZONE 'Europe/Moscow' AS valid_from_msk,
       valid_to   AT TIME ZONE 'Europe/Moscow' AS valid_to_msk
FROM lakehouse.silver.dim_client
WHERE TIMESTAMP '2026-03-01 00:00:00 Europe/Moscow' >= valid_from
  AND TIMESTAMP '2026-03-01 00:00:00 Europe/Moscow' <  valid_to
ORDER BY client_id;

-- 2) Полная история клиентов 1 и 3 (тариф Pro; блокировка/разблокировка)
SELECT client_id, phone, city, tariff_code, status,
       valid_from AT TIME ZONE 'Europe/Moscow' AS valid_from_msk,
       valid_to   AT TIME ZONE 'Europe/Moscow' AS valid_to_msk, is_current
FROM lakehouse.silver.dim_client
WHERE client_id IN (1, 3)
ORDER BY client_id, valid_from;

-- 3) Какой статус был у клиента 3 до разблокировки?
SELECT status FROM lakehouse.silver.dim_client
WHERE client_id = 3 AND valid_to = TIMESTAMP '2026-03-20 11:00:00 Europe/Moscow';

-- 4) Удалённые клиенты: нет текущей версии, история осталась
SELECT client_id, max(valid_to) AS deleted_at FROM lakehouse.silver.dim_client
GROUP BY client_id HAVING count_if(is_current) = 0;

-- 5) Iceberg time travel: какие снапшоты сделал CDC-потребитель
SELECT committed_at, snapshot_id, operation, element_at(summary, 'added-records') AS added, element_at(summary, 'deleted-records') AS deleted
FROM lakehouse.silver."dim_client$snapshots" ORDER BY committed_at;
