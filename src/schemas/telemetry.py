"""Typed schemas for device telemetry events.

Why centralized schemas?
- Schema-on-write enforcement at Bronze layer prevents bad data from propagating
- Single source of truth avoids drift between producers and consumers
- Pydantic mirrors let us validate the generator output matches the Spark schema
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Raw event schema (matches JSON emitted by devices / synthetic generator)
# ---------------------------------------------------------------------------
RAW_TELEMETRY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("tenant_id", StringType(), nullable=False),
        StructField("device_id", StringType(), nullable=False),
        StructField("device_model", StringType(), nullable=True),
        StructField("firmware_version", StringType(), nullable=True),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("signal_strength_dbm", IntegerType(), nullable=True),
        StructField("throughput_mbps_down", DoubleType(), nullable=True),
        StructField("throughput_mbps_up", DoubleType(), nullable=True),
        StructField("connected_clients", IntegerType(), nullable=True),
        StructField("cpu_utilization_pct", DoubleType(), nullable=True),
        StructField("memory_utilization_pct", DoubleType(), nullable=True),
        StructField("channel_utilization_pct_2g", DoubleType(), nullable=True),
        StructField("channel_utilization_pct_5g", DoubleType(), nullable=True),
        StructField("uptime_seconds", LongType(), nullable=True),
        StructField("reboot_flag", BooleanType(), nullable=True),
        StructField("error_codes", ArrayType(StringType()), nullable=True),
        StructField("geo_region", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Bronze schema = raw + ingestion metadata
# ---------------------------------------------------------------------------
BRONZE_METADATA_FIELDS = [
    StructField("_ingest_timestamp", TimestampType(), nullable=False),
    StructField("_source_file", StringType(), nullable=True),
    StructField("_ingest_date", StringType(), nullable=False),
]

BRONZE_SCHEMA = StructType(list(RAW_TELEMETRY_SCHEMA.fields) + BRONZE_METADATA_FIELDS)

# ---------------------------------------------------------------------------
# Silver: cleaned, typed, with derived columns
# ---------------------------------------------------------------------------
SILVER_DEVICE_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("tenant_id", StringType(), nullable=False),
        StructField("device_id", StringType(), nullable=False),
        StructField("device_model", StringType(), nullable=True),
        StructField("firmware_version", StringType(), nullable=True),
        StructField("event_timestamp", TimestampType(), nullable=False),
        StructField("event_date", StringType(), nullable=False),
        StructField("event_hour", IntegerType(), nullable=False),
        StructField("signal_strength_dbm", IntegerType(), nullable=True),
        StructField("signal_quality_bucket", StringType(), nullable=True),
        StructField("throughput_mbps_down", DoubleType(), nullable=True),
        StructField("throughput_mbps_up", DoubleType(), nullable=True),
        StructField("throughput_ratio", DoubleType(), nullable=True),
        StructField("connected_clients", IntegerType(), nullable=True),
        StructField("cpu_utilization_pct", DoubleType(), nullable=True),
        StructField("memory_utilization_pct", DoubleType(), nullable=True),
        StructField("health_score", DoubleType(), nullable=True),
        StructField("reboot_flag", BooleanType(), nullable=True),
        StructField("error_count", IntegerType(), nullable=True),
        StructField("geo_region", StringType(), nullable=True),
        StructField("_processed_timestamp", TimestampType(), nullable=False),
    ]
)

# ---------------------------------------------------------------------------
# Silver: SCD2 device dimension
# ---------------------------------------------------------------------------
SILVER_DEVICE_DIM_SCHEMA = StructType(
    [
        StructField("device_sk", StringType(), nullable=False),  # surrogate key (hash)
        StructField("device_id", StringType(), nullable=False),
        StructField("tenant_id", StringType(), nullable=False),
        StructField("device_model", StringType(), nullable=True),
        StructField("firmware_version", StringType(), nullable=True),
        StructField("geo_region", StringType(), nullable=True),
        StructField("valid_from", TimestampType(), nullable=False),
        StructField("valid_to", TimestampType(), nullable=True),  # null = current
        StructField("is_current", BooleanType(), nullable=False),
        StructField("row_hash", StringType(), nullable=False),
    ]
)
