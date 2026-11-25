"""Определения признаков — ОДНО место описания для обучения и инференса (защита от рассинхрона)."""
from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, PushSource
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import TrinoSource
from feast.types import Float64, Int64, String

client = Entity(name="client", join_keys=["client_id"])

# batch: Spark/Trino раз в сутки -> features.client_30d (offline) -> materialize -> Redis (online)
client_30d_src = TrinoSource(table="lakehouse.features.client_30d", timestamp_field="feature_ts")
client_30d = FeatureView(
    name="client_30d", entities=[client], ttl=timedelta(days=1),
    schema=[Field(name="rides_30d", dtype=Int64), Field(name="avg_amount_30d", dtype=Float64)],
    source=client_30d_src,
)

# SCD2 -> признак «тариф на момент события»: offline-источник = dim_client, ts = valid_from
client_profile_src = TrinoSource(table="lakehouse.silver.dim_client", timestamp_field="valid_from")
client_profile = FeatureView(
    name="client_profile", entities=[client], ttl=timedelta(days=3650),
    schema=[Field(name="tariff_code", dtype=String), Field(name="city", dtype=String)],
    source=client_profile_src,
)

# streaming: Flink считает скользящее окно и пушит в online + пишет тот же лог в Iceberg (offline)
rides_24h_push = PushSource(
    name="rides_24h_push",
    batch_source=TrinoSource(table="lakehouse.features.client_rides_24h_log", timestamp_field="event_ts"),
)
client_realtime = FeatureView(
    name="client_realtime", entities=[client], ttl=timedelta(hours=2),
    schema=[Field(name="rides_24h", dtype=Int64)], source=rides_24h_push,
)

preauth_v1 = FeatureService(name="preauth_v1", features=[client_30d, client_profile, client_realtime])
