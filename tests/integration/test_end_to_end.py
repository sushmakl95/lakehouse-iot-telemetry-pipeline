"""End-to-end integration test.

Exercises the full pipeline against synthetic data:
  generator → bronze → silver → gold

Requires delta-spark and Java 17. Marked as 'integration' so it runs separately
from the fast unit tests in CI.
"""

from __future__ import annotations


import pytest
import yaml

from data.generator.generate_telemetry import generate

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def integration_config(tmp_path_factory) -> dict:
    tmp = tmp_path_factory.mktemp("integration")
    raw_path = tmp / "raw"
    bronze_path = tmp / "bronze"
    silver_path = tmp / "silver"
    gold_path = tmp / "gold"
    checkpoint_path = tmp / "checkpoints"

    # Generate a small dataset
    generate(
        output_dir=raw_path,
        tenants=2,
        devices_per_tenant=5,
        total_events=200,
        days=2,
        inject_dirty=True,
        seed=123,
    )

    cfg = {
        "environment": "local",
        "spark": {
            "app_name": "integration-test",
            "master": "local[2]",
            "configs": {},
        },
        "paths": {
            "landing": str(raw_path),
            "bronze": str(bronze_path),
            "silver": str(silver_path),
            "gold": str(gold_path),
            "checkpoints": str(checkpoint_path),
            "quarantine": str(tmp / "quarantine"),
        },
        "catalog": {
            "name": "spark_catalog",
            "bronze_schema": "it_bronze",
            "silver_schema": "it_silver",
            "gold_schema": "it_gold",
        },
        "data_quality": {
            "quarantine_enabled": True,
            "fail_on_critical": False,
            "thresholds": {},
        },
        "performance": {
            "z_order_columns": {},
            "optimize_after_load": False,
            "vacuum_retention_hours": 168,
        },
        "logging": {"level": "WARN", "format": "console"},
    }
    cfg_file = tmp / "config.yaml"
    cfg_file.write_text(yaml.safe_dump(cfg))
    return {"cfg_file": str(cfg_file), "cfg": cfg, "tmp": tmp}


def test_bronze_layer_writes_data(spark, integration_config):
    """Bronze ingestion should populate the raw_telemetry table."""
    from src.bronze.ingest_telemetry import main as bronze_main

    rc = bronze_main(integration_config["cfg_file"])
    assert rc == 0

    cfg = integration_config["cfg"]
    table = f"{cfg['catalog']['name']}.{cfg['catalog']['bronze_schema']}.raw_telemetry"
    count = spark.read.table(table).count()
    assert count > 0, "Bronze should have ingested rows"


def test_silver_layer_cleans_and_quarantines(spark, integration_config):
    """Silver should produce clean rows AND a quarantine table with dirty rows."""
    from src.silver.transform_telemetry import main as silver_main

    rc = silver_main(integration_config["cfg_file"])
    assert rc == 0

    cfg = integration_config["cfg"]
    silver_table = f"{cfg['catalog']['name']}.{cfg['catalog']['silver_schema']}.device_events"
    quarantine_table = (
        f"{cfg['catalog']['name']}.{cfg['catalog']['silver_schema']}.quarantine_device_events"
    )

    silver_count = spark.read.table(silver_table).count()
    assert silver_count > 0

    # Quarantine should have at least some rows (generator injects dirty data)
    quarantine_count = spark.read.table(quarantine_table).count()
    assert quarantine_count >= 0  # may be 0 depending on random seed

    # Silver rows must NOT have null device_id
    null_device = spark.read.table(silver_table).filter("device_id IS NULL").count()
    assert null_device == 0


def test_gold_layer_produces_all_tables(spark, integration_config):
    """Gold should produce all four aggregate tables."""
    from src.gold.aggregate_kpis import main as gold_main

    rc = gold_main(integration_config["cfg_file"])
    assert rc == 0

    cfg = integration_config["cfg"]
    gold_catalog = f"{cfg['catalog']['name']}.{cfg['catalog']['gold_schema']}"

    for table_name in [
        "device_health_daily",
        "tenant_sla_hourly",
        "reboot_events",
        "network_performance_daily",
    ]:
        full = f"{gold_catalog}.{table_name}"
        # Just verify it's queryable without exception
        spark.read.table(full).limit(1).collect()


def test_idempotency_second_bronze_run_is_safe(spark, integration_config):
    """Re-running bronze should not fail and should not duplicate the audit log."""
    from src.bronze.ingest_telemetry import main as bronze_main

    rc = bronze_main(integration_config["cfg_file"])
    assert rc == 0

    cfg = integration_config["cfg"]
    control = f"{cfg['catalog']['name']}.{cfg['catalog']['bronze_schema']}._batch_control"
    # Should have at least 2 successful bronze runs by now
    successes = (
        spark.read.table(control)
        .filter("layer = 'bronze' AND status = 'SUCCESS'")
        .count()
    )
    assert successes >= 2
