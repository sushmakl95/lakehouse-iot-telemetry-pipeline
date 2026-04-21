"""Delta Live Tables: Silver transform with expectations."""

from __future__ import annotations

import dlt  # type: ignore[import-not-found]
from pyspark.sql import functions as F


@dlt.table(
    name="silver_device_events",
    comment="Cleansed, enriched device telemetry events.",
    partition_cols=["event_date", "tenant_id"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "device_id,event_timestamp",
    },
)
# CRITICAL: fail pipeline on violation
@dlt.expect_or_fail("device_id_not_null", "device_id IS NOT NULL AND device_id != ''")
@dlt.expect_or_fail("tenant_id_not_null", "tenant_id IS NOT NULL")
@dlt.expect_or_fail("event_timestamp_not_null", "event_timestamp IS NOT NULL")
# ERROR: drop the offending row, continue
@dlt.expect_or_drop(
    "event_timestamp_not_future",
    "event_timestamp <= current_timestamp() + INTERVAL 5 MINUTES",
)
@dlt.expect_or_drop(
    "signal_strength_in_range",
    "signal_strength_dbm IS NULL OR (signal_strength_dbm BETWEEN -120 AND 0)",
)
@dlt.expect_or_drop(
    "throughput_non_negative",
    "(throughput_mbps_down IS NULL OR throughput_mbps_down >= 0) AND "
    "(throughput_mbps_up IS NULL OR throughput_mbps_up >= 0)",
)
# WARN: keep row, emit metric
@dlt.expect("cpu_pct_in_range", "cpu_utilization_pct IS NULL OR cpu_utilization_pct BETWEEN 0 AND 100")
@dlt.expect(
    "memory_pct_in_range",
    "memory_utilization_pct IS NULL OR memory_utilization_pct BETWEEN 0 AND 100",
)
def silver_device_events():
    """Silver device events with DQ expectations and enrichments."""
    bronze = dlt.read_stream("bronze_raw_telemetry")

    return (
        bronze.withColumn("event_date", F.date_format(F.col("event_timestamp"), "yyyy-MM-dd"))
        .withColumn("event_hour", F.hour(F.col("event_timestamp")))
        .withColumn(
            "signal_quality_bucket",
            F.when(F.col("signal_strength_dbm") >= -50, "EXCELLENT")
            .when(F.col("signal_strength_dbm") >= -60, "GOOD")
            .when(F.col("signal_strength_dbm") >= -70, "FAIR")
            .when(F.col("signal_strength_dbm") >= -80, "POOR")
            .otherwise("UNUSABLE"),
        )
        .withColumn(
            "throughput_ratio",
            F.when(
                (F.col("throughput_mbps_down").isNotNull())
                & (F.col("throughput_mbps_down") > 0),
                F.round(F.col("throughput_mbps_up") / F.col("throughput_mbps_down"), 4),
            ).otherwise(None),
        )
        .withColumn(
            "error_count",
            F.when(F.col("error_codes").isNotNull(), F.size(F.col("error_codes"))).otherwise(0),
        )
        .withColumnRenamed("_ingest_timestamp", "_source_ingest_ts")
        .withColumn("_processed_timestamp", F.current_timestamp())
    )


@dlt.view(comment="Latest known attributes per device (used to drive SCD2 dim).")
def silver_device_latest_attrs():
    events = dlt.read("silver_device_events")
    window = F.row_number().over(
        F.window(F.col("event_timestamp"), "1 day").alias("_w")
    )
    del window

    return (
        events.groupBy("tenant_id", "device_id")
        .agg(
            F.max("event_timestamp").alias("latest_event_ts"),
            F.last("device_model", ignorenulls=True).alias("device_model"),
            F.last("firmware_version", ignorenulls=True).alias("firmware_version"),
            F.last("geo_region", ignorenulls=True).alias("geo_region"),
        )
    )
