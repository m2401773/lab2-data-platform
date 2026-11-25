-- Жизнь справочника: то, ради чего нужна история (SCD2) и CDC.
-- updated_at выставляет приложение = бизнес-время изменения.

-- 1) Клиент 1 купил подписку: base -> pro (версионируем, SCD2)
UPDATE clients SET tariff_code = 'pro',  updated_at = '2026-02-15 12:00+03' WHERE client_id = 1;
-- 2) Клиент 1 сменил телефон (опечатка/технический атрибут — перезапись, SCD1)
UPDATE clients SET phone = '+79990000001', updated_at = '2026-02-20 09:30+03' WHERE client_id = 1;
-- 3) Клиент 2 переехал Москва -> Казань (SCD2)
UPDATE clients SET city = 'Казань', updated_at = '2026-03-03 18:00+03' WHERE client_id = 2;
-- 4) Клиента 3 заблокировали за нарушение, потом разблокировали (две версии)
UPDATE clients SET status = 'blocked', updated_at = '2026-03-10 14:00+03' WHERE client_id = 3;
UPDATE clients SET status = 'active',  updated_at = '2026-03-20 11:00+03' WHERE client_id = 3;
-- 5) Клиент 5 прошёл верификацию и сразу перешёл на ночной тариф одной транзакцией
UPDATE clients SET status = 'active', tariff_code = 'night', updated_at = '2026-02-01 08:00+03' WHERE client_id = 5;
-- 6) Клиент 6 удалил аккаунт
DELETE FROM clients WHERE client_id = 6;
-- 7) Новый клиент
INSERT INTO clients VALUES (7, 'Клиент Семь', '+79000000007', 'c7@mail.test', 'Москва', 'base', 'active', '2026-03-05 20:00+03');

-- Самокаты: статус меняется сотни раз в день -> SCD1 (история живёт в потоке событий)
UPDATE scooters SET status = 'service', zone = 'msk-depot', updated_at = '2026-03-01 07:00+03' WHERE scooter_id = 4;
UPDATE scooters SET status = 'lost',    updated_at = '2026-03-02 23:10+03' WHERE scooter_id = 9;
