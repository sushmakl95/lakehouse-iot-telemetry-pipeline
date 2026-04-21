"""Unit tests for Silver layer transformation logic."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.silver.transform_telemetry import transform_to_silver

pytestmark = pytest.mark.unit


def _bronze_row(**overrides):
    base = {
        "event_id": "e1",
        "tenant_id": "tenant_a",
        "device_id": "dev_1",
        "device_model": "X1",
        "firmware_version": "3.14.2",
        "event_timestamp": datetime(2026, 4, 15, 10, 30, 0, tzinfo=UTC),
        "signal_strength_dbm": -55,
        "throughput_mbps_down": 200.0,
        "throughput_mbps_up": 40.0,
        "connected_clients": 3,
        "cpu_utilization_pct": 20.0,
        "memory_utilization_pct": 40.0,
        "channel_utilization_pct_2g": 10.0,
        "channel_utilization_pct_5g": 8.0,
        "uptime_seconds": 1000,
        "reboot_flag": False,
        "error_codes": ["ERR_DNS_FAIL"],
        "geo_region": "NA-East",
        "_ingest_timestamp": datetime(2026, 4, 15, 10, 31, 0, tzinfo=UTC),
        "_source_file": "s3://test/file.json",
        "_ingest_date": "2026-04-15",
    }
    base.update(overrides)
    return base


def test_event_date_derived_correctly(spark):
    df = spark.createDataFrame([_bronze_row()])
    out = transform_to_silver(df).collect()[0]
    assert out["event_date"] == "2026-04-15"
    assert out["event_hour"] == 10


def test_signal_quality_bucket_excellent(spark):
    df = spark.createDataFrame([_bronze_row(signal_strength_dbm=-45)])
    out = transform_to_silver(df).collect()[0]
    assert out["signal_quality_bucket"] == "EXCELLENT"


def test_signal_quality_bucket_unusable(spark):
    df = spark.createDataFrame([_bronze_row(signal_strength_dbm=-95)])
    out = transform_to_silver(df).collect()[0]
    assert out["signal_quality_bucket"] == "UNUSABLE"


def test_throughput_ratio_computed(spark):
    df = spark.createDataFrame(
        [_bronze_row(throughput_mbps_down=200.0, throughput_mbps_up=50.0)]
    )
    out = transform_to_silver(df).collect()[0]
    assert out["throughput_ratio"] == pytest.approx(0.25)


def test_throughput_ratio_null_when_divide_by_zero(spark):
    df = spark.createDataFrame([_bronze_row(throughput_mbps_down=0.0)])
    out = transform_to_silver(df).collect()[0]
    assert out["throughput_ratio"] is None


def test_error_count_from_array(spark):
    df = spark.createDataFrame(
        [_bronze_row(error_codes=["ERR_A", "ERR_B", "ERR_C"])]
    )
    out = transform_to_silver(df).collect()[0]
    assert out["error_count"] == 3


def test_error_count_zero_when_null(spark):
    df = spark.createDataFrame([_bronze_row(error_codes=None)])
    out = transform_to_silver(df).collect()[0]
    assert out["error_count"] == 0


def test_health_score_bounds(spark):
    """Health score should always be within [0, 100]."""
    df = spark.createDataFrame(
        [
            _bronze_row(
                signal_strength_dbm=-30,  # great
                cpu_utilization_pct=5.0,
                memory_utilization_pct=10.0,
                throughput_mbps_down=100.0,
                throughput_mbps_up=80.0,
            ),
            _bronze_row(
                event_id="e2",
                signal_strength_dbm=-95,  # terrible
                cpu_utilization_pct=99.0,
                memory_utilization_pct=99.0,
                throughput_mbps_down=100.0,
                throughput_mbps_up=1.0,
            ),
        ]
    )
    scores = [r["health_score"] for r in transform_to_silver(df).collect()]
    for score in scores:
        assert 0 <= score <= 100
