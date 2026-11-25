-- Операционная БД кикшеринга «Вжух»: справочники, которые постоянно меняются.
CREATE TABLE tariffs (
    tariff_code  TEXT PRIMARY KEY,
    title        TEXT NOT NULL,
    price_minute NUMERIC(6,2) NOT NULL
);

CREATE TABLE clients (
    client_id   INT PRIMARY KEY,
    full_name   TEXT NOT NULL,
    phone       TEXT,
    email       TEXT,
    city        TEXT NOT NULL,
    tariff_code TEXT NOT NULL REFERENCES tariffs,
    status      TEXT NOT NULL,          -- active / blocked / verification
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE scooters (
    scooter_id  INT PRIMARY KEY,
    model       TEXT NOT NULL,
    zone        TEXT NOT NULL,
    status      TEXT NOT NULL,          -- free / in_ride / service / lost
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL: Debezium получает полный образ "before",
-- по нему потребитель решает, менялся ли версионируемый атрибут (SCD2) или технический (SCD1).
ALTER TABLE clients  REPLICA IDENTITY FULL;
ALTER TABLE scooters REPLICA IDENTITY FULL;

INSERT INTO tariffs VALUES
  ('base',  'Базовый',      7.50),
  ('pro',   'Подписка Pro', 5.00),
  ('night', 'Ночной',       4.00);

INSERT INTO clients VALUES
  (1, 'Клиент Один',    '+79000000001', 'c1@mail.test', 'Москва',          'base',  'active',       '2026-01-10 10:00+03'),
  (2, 'Клиент Два',     '+79000000002', 'c2@mail.test', 'Москва',          'base',  'active',       '2026-01-10 10:00+03'),
  (3, 'Клиент Три',     '+79000000003', 'c3@mail.test', 'Санкт-Петербург', 'pro',   'active',       '2026-01-10 10:00+03'),
  (4, 'Клиент Четыре',  '+79000000004', 'c4@mail.test', 'Казань',          'night', 'active',       '2026-01-10 10:00+03'),
  (5, 'Клиент Пять',    '+79000000005', 'c5@mail.test', 'Москва',          'base',  'verification', '2026-01-10 10:00+03'),
  (6, 'Клиент Шесть',   '+79000000006', 'c6@mail.test', 'Санкт-Петербург', 'base',  'active',       '2026-01-10 10:00+03');

INSERT INTO scooters
SELECT g, CASE WHEN g % 2 = 0 THEN 'Ninebot Max G30' ELSE 'Xiaomi 4 Pro' END,
       (ARRAY['msk-center','msk-north','spb-center','kzn-center'])[1 + g % 4],
       'free', '2026-01-10 10:00+03'
FROM generate_series(1, 20) g;
