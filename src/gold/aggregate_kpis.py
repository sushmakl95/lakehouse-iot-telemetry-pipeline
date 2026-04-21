"""Gold layer: business-facing aggregates and KPIs.

Produces:
- gold.device_health_daily — per-device daily rollup
- gold.tenant_sla_hourly   — per-tenant hourly SLA view
- gold.reboot_events       — materialized anomaly view
- gold.network_performance_daily — throughput / signal quality trending

Design choices:
- Overwrite-by-partition (replaceWhere) for daily aggregates — cheap, deterministic re-runs
- Partitioned by event_date for time-window queries
- Z-Ordered on device_id / tenant_id for BI point queries
"""

from __future__ import annotations

import argparse
import sys
import traceback
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils import (
    IdempotencyTracker,
    configure_logging,
    get_logger,
    get_spark_session,
    load_config,
)

LAYER = "gold"


def build_device_health_daily(silver_df: DataFrame) -> DataFrame:
    """Per-device daily health rollup."""
    return (
        silver_df.groupBy("tenant_id", "device_id", "event_date")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("health_score").alias("avg_health_score"),
            F.min("health_score").alias("min_health_score"),
            F.max("health_score").alias("max_health_score"),
            F.avg("signal_strength_dbm").alias("avg_signal_dbm"),
            F.avg("throughput_mbps_down").alias("avg_throughput_down_mbps"),
            F.avg("throughput_mbps_up").alias("avg_throughput_up_mbps"),
            F.avg("connected_clients").alias("avg_connected_clients"),
            F.max("connected_clients").alias("peak_connected_clients"),
            F.sum(F.when(F.col("reboot_flag"), 1).otherwise(0)).alias("reboot_count"),
            F.sum("error_count").alias("total_errors"),
            F.sum(
                F.when(F.col("signal_quality_bucket") == "POOR", 1).otherwise(0)
            ).alias("poor_signal_events"),
            F.sum(
                F.when(F.col("signal_quality_bucket") == "UNUSABLE", 1).otherwise(0)
            ).alias("unusable_signal_events"),
        )
        .withColumn("_processed_timestamp", F.current_timestamp())
    )


def build_tenant_sla_hourly(silver_df: DataFrame) -> DataFrame:
    """Per-tenant hourly SLA metrics.

    Defines device 'healthy' as avg health_score >= 70 within the hour.
    Healthy % is the SLA metric reported to tenants.
    """
    hourly_device = (
        silver_df.groupBy(
            "tenant_id",
            "event_date",
            "event_hour",
            "device_id",
        )
        .agg(F.avg("health_score").alias("hourly_health_score"))
    )

    return (
        hourly_device.groupBy("tenant_id", "event_date", "event_hour")
        .agg(
            F.countDistinct("device_id").alias("active_devices"),
            F.sum(F.when(F.col("hourly_health_score") >= 70, 1).otherwise(0)).alias(
                "healthy_devices"
            ),
            F.avg("hourly_health_score").alias("avg_tenant_health_score"),
        )
        .withColumn(
            "healthy_device_pct",
            F.round(F.col("healthy_devices") / F.col("active_devices") * 100, 2),
        )
        .withColumn("_processed_timestamp", F.current_timestamp())
    )


def build_reboot_events(silver_df: DataFrame) -> DataFrame:
    """Extract reboot events for anomaly monitoring."""
    return (
        silver_df.filter(F.col("reboot_flag"))
        .select(
            "tenant_id",
            "device_id",
            "event_date",
            "event_timestamp",
            "firmware_version",
            "device_model",
            "geo_region",
        )
        .withColumn("_processed_timestamp", F.current_timestamp())
    )


def build_network_performance_daily(silver_df: DataFrame) -> DataFrame:
    """Tenant-level daily throughput / signal distribution."""
    return (
        silver_df.groupBy("tenant_id", "event_date")
        .agg(
            F.countDistinct("device_id").alias("active_devices"),
            F.expr("percentile_approx(throughput_mbps_down, 0.5)").alias(
                "throughput_down_p50_mbps"
            ),
            F.expr("percentile_approx(throughput_mbps_down, 0.95)").alias(
                "throughput_down_p95_mbps"
            ),
            F.expr("percentile_approx(throughput_mbps_down, 0.99)").alias(
                "throughput_down_p99_mbps"
            ),
            F.avg("signal_strength_dbm").alias("avg_signal_dbm"),
            F.expr("percentile_approx(signal_strength_dbm, 0.05)").alias(
                "signal_dbm_p05"
            ),
            F.sum(F.when(F.col("signal_quality_bucket") == "EXCELLENT", 1).otherwise(0)).alias(
                "excellent_signal_events"
            ),
            F.sum(F.when(F.col("signal_quality_bucket") == "POOR", 1).otherwise(0)).alias(
                "poor_signal_events"
            ),
        )
        .withColumn("_processed_timestamp", F.current_timestamp())
    )


def write_partitioned(
    df: DataFrame,
    table: str,
    partition_cols: list[str],
    replace_where: str | None = None,
    spark: SparkSession | None = None,
) -> int:
    """Write to a gold table using replaceWhere for idempotent partition overwrite."""
    count = df.count()
    if count == 0:
        return 0

    assert spark is not None
    if not spark.catalog.tableExists(table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .saveAsTable(table)
        )
        return count

    writer = df.write.format("delta").mode("overwrite")
    if replace_where:
        writer = writer.option("replaceWhere", replace_where)
    writer.saveAsTable(table)
    return count


def main(config_path: str) -> int:
    cfg = load_config(config_path)
    configure_logging(level=cfg["logging"]["level"], fmt=cfg["logging"]["format"])
    log = get_logger(__name__, layer=LAYER, env=cfg["environment"])

    spark = get_spark_session(
        app_name=cfg["spark"]["app_name"],
        master=cfg["spark"].get("master"),
        configs=cfg["spark"].get("configs", {}),
    )

    catalog = cfg["catalog"]["name"]
    silver_table = f"{catalog}.{cfg['catalog']['silver_schema']}.device_events"
    gold_schema = cfg["catalog"]["gold_schema"]
    control_table = f"{catalog}.{gold_schema}._batch_control"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{gold_schema}")

    batch_id = f"gold_{uuid.uuid4().hex[:12]}"
    tracker = IdempotencyTracker(spark, control_table)
    started_at = tracker.mark_started(batch_id, LAYER)
    log.info("gold_aggregate_started", batch_id=batch_id, source=silver_table)

    try:
        silver_df = spark.read.table(silver_table)

        # Device health daily
        dhd = build_device_health_daily(silver_df)
        dhd_count = write_partitioned(
            dhd,
            f"{catalog}.{gold_schema}.device_health_daily",
            partition_cols=["event_date", "tenant_id"],
            spark=spark,
        )
        log.info("gold_device_health_daily_written", count=dhd_count)

        # Tenant SLA hourly
        sla = build_tenant_sla_hourly(silver_df)
        sla_count = write_partitioned(
            sla,
            f"{catalog}.{gold_schema}.tenant_sla_hourly",
            partition_cols=["event_date", "tenant_id"],
            spark=spark,
        )
        log.info("gold_tenant_sla_hourly_written", count=sla_count)

        # Reboot events
        reboots = build_reboot_events(silver_df)
        reboot_count = write_partitioned(
            reboots,
            f"{catalog}.{gold_schema}.reboot_events",
            partition_cols=["event_date", "tenant_id"],
            spark=spark,
        )
        log.info("gold_reboot_events_written", count=reboot_count)

        # Network perf daily
        netperf = build_network_performance_daily(silver_df)
        netperf_count = write_partitioned(
            netperf,
            f"{catalog}.{gold_schema}.network_performance_daily",
            partition_cols=["event_date", "tenant_id"],
            spark=spark,
        )
        log.info("gold_network_performance_daily_written", count=netperf_count)

        # Optimize
        if cfg["performance"]["optimize_after_load"]:
            for gtable, zcols in [
                (
                    f"{catalog}.{gold_schema}.device_health_daily",
                    cfg["performance"]["z_order_columns"].get(
                        "gold_device_health_daily", ["device_id"]
                    ),
                ),
            ]:
                spark.sql(f"OPTIMIZE {gtable} ZORDER BY ({', '.join(zcols)})")

        total = dhd_count + sla_count + reboot_count + netperf_count
        tracker.mark_processed(batch_id, LAYER, started_at, row_count=total)
        log.info("gold_aggregate_completed", batch_id=batch_id, total=total)
        return 0

    except Exception as exc:
        log.error(
            "gold_aggregate_failed",
            batch_id=batch_id,
            error=str(exc),
            traceback=traceback.format_exc(),
        )
        tracker.mark_failed(batch_id, LAYER, started_at, str(exc))
        return 1


def cli() -> None:
    parser = argparse.ArgumentParser(description="Gold KPI aggregation")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    sys.exit(main(args.config))


if __name__ == "__main__":
    cli()
