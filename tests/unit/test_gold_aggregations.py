"""Unit tests for Gold layer aggregations."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from src.gold.aggregate_kpis import (
    build_device_health_daily,
    build_reboot_events,
    build_tenant_sla_hourly,
)

pytestmark = pytest.mark.unit


def _silver_row(**overrides):
    base = {
        "event_id": "e1",
        "tenant_id": "tenant_a",
        "device_id": "dev_1",
        "device_model": "X1",
        "firmware_version": "3.14.2",
        "event_timestamp": datetime(2026, 4, 15, 10, 30, 0, tzinfo=UTC),
        "event_date": "2026-04-15",
        "event_hour": 10,
        "signal_strength_dbm": -60,
        "signal_quality_bucket": "GOOD",
        "throughput_mbps_down": 200.0,
        "throughput_mbps_up": 40.0,
        "throughput_ratio": 0.2,
        "connected_clients": 5,
        "cpu_utilization_pct": 25.0,
        "memory_utilization_pct": 50.0,
        "health_score": 80.0,
        "reboot_flag": False,
        "error_count": 0,
        "geo_region": "NA-East",
        "_source_ingest_ts": datetime(2026, 4, 15, 10, 31, 0, tzinfo=UTC),
        "_processed_timestamp": datetime(2026, 4, 15, 10, 32, 0, tzinfo=UTC),
    }
    base.update(overrides)
    return base


def test_device_health_daily_aggregates_correctly(spark):
    rows = [
        _silver_row(event_id="e1", health_score=80.0, reboot_flag=False),
        _silver_row(event_id="e2", health_score=60.0, reboot_flag=True),
        _silver_row(event_id="e3", health_score=70.0, reboot_flag=False),
    ]
    df = spark.createDataFrame(rows)
    result = build_device_health_daily(df).collect()[0]

    assert result["event_count"] == 3
    assert result["avg_health_score"] == pytest.approx(70.0)
    assert result["min_health_score"] == 60.0
    assert result["max_health_score"] == 80.0
    assert result["reboot_count"] == 1


def test_tenant_sla_hourly_healthy_pct(spark):
    """Devices with avg health >= 70 in the hour are 'healthy'."""
    rows = [
        _silver_row(event_id="e1", device_id="dev_1", health_score=90.0),
        _silver_row(event_id="e2", device_id="dev_2", health_score=50.0),  # unhealthy
        _silver_row(event_id="e3", device_id="dev_3", health_score=75.0),
    ]
    df = spark.createDataFrame(rows)
    result = build_tenant_sla_hourly(df).collect()[0]

    assert result["active_devices"] == 3
    assert result["healthy_devices"] == 2
    assert result["healthy_device_pct"] == pytest.approx(66.67, rel=0.01)


def test_reboot_events_filters_correctly(spark):
    rows = [
        _silver_row(event_id="e1", reboot_flag=False),
        _silver_row(event_id="e2", reboot_flag=True, device_id="dev_x"),
        _silver_row(event_id="e3", reboot_flag=True, device_id="dev_y"),
    ]
    df = spark.createDataFrame(rows)
    result = build_reboot_events(df).collect()
    assert len(result) == 2
    device_ids = {r["device_id"] for r in result}
    assert device_ids == {"dev_x", "dev_y"}
