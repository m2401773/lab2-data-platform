"""Генератор событий самокатов/приложения в Kafka — с теми проблемами, что бывают в жизни:
* дубли (QoS1 / ретраи приложения отправляют тот же event_id повторно) — ~5%;
* опоздания (самокат без связи отдаёт буфер позже) — ~8% событий отправляются на 1–6 ч позже;
* вразнобой по партициям (ключ = scooter_id).
Тариф для расчёта стоимости берётся «как было на момент поездки» — та же история, что в SCD2.
"""
from __future__ import annotations

import datetime as dt
import json
import random
import sys
import time
import uuid

from confluent_kafka import Producer

from common import KAFKA

random.seed(42)
MSK = dt.timezone(dt.timedelta(hours=3))
PRICE = {"base": 7.5, "pro": 5.0, "night": 4.0}
# (client_id, с какого момента, тариф) — зеркало 02_changes.sql
TARIFF_HISTORY = {
    1: [("2026-01-10", "base"), ("2026-02-15 12:00", "pro")],
    2: [("2026-01-10", "base")], 3: [("2026-01-10", "pro")], 4: [("2026-01-10", "night")],
    5: [("2026-01-10", "base"), ("2026-02-01 08:00", "night")], 6: [("2026-01-10", "base")],
    7: [("2026-03-05 20:00", "base")],
}
CLIENT_ACTIVE_TILL = {6: "2026-03-01"}          # удалил аккаунт
START, DAYS, RIDES = dt.datetime(2026, 2, 1, tzinfo=MSK), 50, 900


def tariff_at(client_id: int, t: dt.datetime) -> str | None:
    cur = None
    for since, code in TARIFF_HISTORY[client_id]:
        if dt.datetime.fromisoformat(since).replace(tzinfo=MSK) <= t:
            cur = code
    return cur


def make_rides():
    events = []
    for _ in range(RIDES):
        client = random.randint(1, 7)
        start = START + dt.timedelta(days=random.randrange(DAYS), minutes=random.randrange(7 * 60, 23 * 60))
        tariff = tariff_at(client, start)
        till = CLIENT_ACTIVE_TILL.get(client)
        if tariff is None or (till and start >= dt.datetime.fromisoformat(till).replace(tzinfo=MSK)):
            continue
        ride_id, scooter = str(uuid.uuid4()), random.randint(1, 20)
        minutes = random.randint(4, 40)
        base = dict(ride_id=ride_id, client_id=client, scooter_id=scooter)
        events.append(dict(base, event_type="ride_start", event_time=start))
        for k in range(1, 4):  # телеметрия во время поездки
            events.append(dict(base, event_type="telemetry", event_time=start + dt.timedelta(minutes=minutes * k / 4),
                               battery=random.randint(15, 100), speed_kmh=round(random.uniform(5, 25), 1)))
        events.append(dict(base, event_type="ride_end", event_time=start + dt.timedelta(minutes=minutes),
                           amount_rub=round(minutes * PRICE[tariff], 2)))
    for e in events:
        e["event_id"] = str(uuid.uuid4())   # генерируется на устройстве -> ключ дедупликации
    return events


def main():
    events = make_rides()
    # порядок отправки = время доставки: event_time + сетевая задержка; часть приходит сильно позже
    for e in events:
        delay = dt.timedelta(seconds=random.randint(1, 30))
        if random.random() < 0.08:
            delay = dt.timedelta(hours=random.uniform(1, 6))
            e["_late"] = True
        e["_arrival"] = e["event_time"] + delay
    events.sort(key=lambda e: e["_arrival"])
    to_send = []
    for e in events:
        to_send.append(e)
        if random.random() < 0.05:
            to_send.append(e)                 # дубль: тот же event_id ещё раз
    p = Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True, "acks": "all"})
    topic_of = {"telemetry": "telemetry.raw"}
    rate = float(sys.argv[1]) if len(sys.argv) > 1 else 0   # сообщений/с; 0 = сразу всё (бэклог)
    for e in to_send:
        if rate:
            time.sleep(1 / rate)
        msg = {k: (v.isoformat() if isinstance(v, dt.datetime) else v) for k, v in e.items() if not k.startswith("_")}
        p.produce(topic_of.get(e["event_type"], "app.events"), key=str(e["scooter_id"]),
                  value=json.dumps(msg, ensure_ascii=False))
        p.poll(0)
    p.flush()
    late = sum(1 for e in events if e.get("_late"))
    print(f"unique events={len(events)} sent={len(to_send)} duplicates={len(to_send) - len(events)} late(1-6h)={late}")


if __name__ == "__main__":
    main()
