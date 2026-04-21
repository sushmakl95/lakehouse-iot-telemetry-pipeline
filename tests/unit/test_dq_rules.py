"""Unit tests for the Data Quality rule engine."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from chispa.dataframe_comparer import assert_df_equality  # noqa: F401

from src.data_quality.rules import TELEMETRY_RULES, Severity, apply_rules

pytestmark = pytest.mark.unit


def _make_row(**overrides):
    """Helper: build a default-valid telemetry row, with optional field overrides."""
    base = {
        "event_id": "e1",
        "tenant_id": "tenant_a",
        "device_id": "dev_1",
        "device_model": "Airwave-X1",
        "firmware_version": "3.14.2",
        "event_timestamp": datetime.now(UTC).replace(microsecond=0),
        "signal_strength_dbm": -60,
        "throughput_mbps_down": 150.0,
        "throughput_mbps_up": 25.0,
        "connected_clients": 5,
        "cpu_utilization_pct": 30.0,
        "memory_utilization_pct": 45.0,
        "channel_utilization_pct_2g": 20.0,
        "channel_utilization_pct_5g": 15.0,
        "uptime_seconds": 3600,
        "reboot_flag": False,
        "error_codes": [],
        "geo_region": "NA-East",
        "_ingest_timestamp": datetime.now(UTC).replace(microsecond=0),
        "_source_file": "test",
        "_ingest_date": "2026-04-21",
    }
    base.update(overrides)
    return base


def test_all_valid_rows_pass(spark):
    """Clean rows should all pass and quarantine should be empty."""
    rows = [_make_row(event_id=f"e{i}", device_id=f"dev_{i}") for i in range(10)]
    df = spark.createDataFrame(rows)

    clean, quarantine, result = apply_rules(df, TELEMETRY_RULES)

    assert result.total == 10
    assert result.passed == 10
    assert result.quarantined == 0
    assert clean.count() == 10
    assert quarantine.count() == 0


def test_null_device_id_triggers_critical(spark):
    """Null device_id is CRITICAL — row is quarantined, counter increments."""
    rows = [
        _make_row(event_id="e1"),  # valid
        _make_row(event_id="e2", device_id=None),  # CRITICAL
    ]
    df = spark.createDataFrame(rows)
    clean, quarantine, result = apply_rules(df, TELEMETRY_RULES)

    assert result.passed == 1
    assert result.quarantined == 1
    assert result.critical_failures == 1
    assert result.rule_violations["device_id_not_null"] == 1


def test_out_of_range_signal_quarantines(spark):
    """signal_strength outside [-120, 0] is quarantined (ERROR severity)."""
    rows = [
        _make_row(event_id="e1", signal_strength_dbm=-60),  # valid
        _make_row(event_id="e2", signal_strength_dbm=50),  # invalid (positive)
        _make_row(event_id="e3", signal_strength_dbm=-200),  # invalid (too low)
    ]
    df = spark.createDataFrame(rows)
    _, _, result = apply_rules(df, TELEMETRY_RULES)

    assert result.passed == 1
    assert result.quarantined == 2
    assert result.rule_violations["signal_strength_in_range"] == 2
    # Critical count should be 0 — these are ERROR severity, not CRITICAL
    assert result.critical_failures == 0


def test_future_timestamp_quarantines(spark):
    """Events with timestamps >5min in the future should be quarantined."""
    rows = [
        _make_row(event_id="e1"),
        _make_row(
            event_id="e2",
            event_timestamp=datetime.now(UTC).replace(microsecond=0)
            + timedelta(hours=1),
        ),
    ]
    df = spark.createDataFrame(rows)
    _, _, result = apply_rules(df, TELEMETRY_RULES)

    assert result.rule_violations["event_timestamp_not_future"] == 1


def test_negative_throughput_quarantines(spark):
    """Negative throughput should be quarantined."""
    rows = [
        _make_row(event_id="e1", throughput_mbps_down=100.0),
        _make_row(event_id="e2", throughput_mbps_down=-5.0),
    ]
    df = spark.createDataFrame(rows)
    _, _, result = apply_rules(df, TELEMETRY_RULES)
    assert result.rule_violations["throughput_non_negative"] == 1


def test_warn_severity_does_not_quarantine(spark):
    """WARN-level violations should NOT quarantine the row."""
    # CPU > 100 is WARN, not ERROR/CRITICAL
    rows = [_make_row(event_id="e1", cpu_utilization_pct=150.0)]
    df = spark.createDataFrame(rows)
    clean, quarantine, result = apply_rules(df, TELEMETRY_RULES)

    assert clean.count() == 1
    assert quarantine.count() == 0
    assert result.rule_violations["cpu_pct_in_range"] == 1


def test_rule_metadata_present():
    """All rules must declare a non-empty description."""
    for rule in TELEMETRY_RULES:
        assert rule.name
        assert rule.description, f"Rule {rule.name} missing description"
        assert isinstance(rule.severity, Severity)
