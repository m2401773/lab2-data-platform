-- Iceberg-таблицы Lakehouse (выполняются через Trino). Медальонная модель.
CREATE SCHEMA IF NOT EXISTS lakehouse.bronze   WITH (location = 's3://warehouse/bronze/');
CREATE SCHEMA IF NOT EXISTS lakehouse.silver   WITH (location = 's3://warehouse/silver/');
CREATE SCHEMA IF NOT EXISTS lakehouse.gold     WITH (location = 's3://warehouse/gold/');
CREATE SCHEMA IF NOT EXISTS lakehouse.features WITH (location = 's3://warehouse/features/');

-- BRONZE: сырые CDC-события Debezium как есть (аудит + возможность пересобрать silver)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.cdc_raw (
    topic           VARCHAR,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    op              VARCHAR,          -- r=snapshot, c=insert, u=update, d=delete
    lsn             BIGINT,
    src_ts          TIMESTAMP(6) WITH TIME ZONE,
    record_key      VARCHAR,
    before_json     VARCHAR,
    after_json      VARCHAR,
    ingested_at     TIMESTAMP(6) WITH TIME ZONE
) WITH (partitioning = ARRAY['day(ingested_at)']);

-- SILVER: клиент, гибрид SCD2 (city, tariff_code, status) + SCD1 (full_name, phone, email)
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_client (
    client_id    INTEGER,
    full_name    VARCHAR,
    phone        VARCHAR,
    email        VARCHAR,
    city         VARCHAR,
    tariff_code  VARCHAR,
    status       VARCHAR,
    valid_from   TIMESTAMP(6) WITH TIME ZONE,
    valid_to     TIMESTAMP(6) WITH TIME ZONE,   -- 9999-12-31 для текущей версии
    is_current   BOOLEAN,
    src_lsn      BIGINT,                         -- LSN, породивший версию
    updated_lsn  BIGINT                          -- последний применённый LSN (защита от повторов)
);

-- SILVER: самокат, SCD1 (статус меняется сотни раз в день — историю берём из событий)
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_scooter (
    scooter_id  INTEGER,
    model       VARCHAR,
    zone        VARCHAR,
    status      VARCHAR,
    updated_at  TIMESTAMP(6) WITH TIME ZONE,
    src_lsn     BIGINT
);

-- BRONZE: поток событий как пришёл (с дублями и опоздавшими)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.events_raw (
    event_id         VARCHAR,
    event_type       VARCHAR,
    event_time       TIMESTAMP(6) WITH TIME ZONE,
    payload          VARCHAR,
    kafka_partition  INTEGER,
    kafka_offset     BIGINT,
    late_for_live    BOOLEAN,      -- пришло позже watermark -> в живой мониторинг не попало
    ingested_at      TIMESTAMP(6) WITH TIME ZONE
) WITH (partitioning = ARRAY['day(ingested_at)']);

-- SILVER: очищенные события, ровно одна строка на event_id, партиции по event-time
CREATE TABLE IF NOT EXISTS lakehouse.silver.events (
    event_id    VARCHAR,
    event_type  VARCHAR,           -- ride_start / telemetry / ride_end
    ride_id     VARCHAR,
    client_id   INTEGER,
    scooter_id  INTEGER,
    event_time  TIMESTAMP(6) WITH TIME ZONE,
    battery     INTEGER,
    speed_kmh   DOUBLE,
    amount_rub  DOUBLE,
    ingested_at TIMESTAMP(6) WITH TIME ZONE
) WITH (partitioning = ARRAY['day(event_time)']);
