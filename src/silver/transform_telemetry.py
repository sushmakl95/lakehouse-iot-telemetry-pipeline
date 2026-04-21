"""Silver layer: cleanse, enrich, conform.

Responsibilities:
1. Apply DQ rules (quarantine failures, fail on CRITICAL)
2. Derive business columns: event_date, event_hour, health_score, signal_quality_bucket
3. Conform types (nulls → sensible defaults where appropriate)
4. Write partitioned by (event_date, tenant_id) for pruning
5. Apply Z-Order on device_id + event_timestamp (hot query columns)
6. Idempotent MERGE using event_id as dedup key (Delta MERGE is ACID)

Partitioning rationale:
- event_date: most analytical queries filter by time window
- tenant_id: enforces tenant isolation at the partition level (file separation)
- Z-Order on device_id: high-cardinality column used in WHERE/JOIN predicates

Why MERGE instead of append?
- Upstream retries can re-deliver events (at-least-once semantics)
- MERGE on event_id gives us exactly-once at the table level
"""

from __future__ import annotations

import argparse
import sys
import traceback
import uuid

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.data_quality import TELEMETRY_RULES, Severity, apply_rules
from src.utils import (
    IdempotencyTracker,
    configure_logging,
    get_logger,
    get_spark_session,
    load_config,
)

LAYER = "silver"


def read_bronze_incremental(
    spark: SparkSession,
    bronze_table: str,
    high_watermark: str | None,
) -> DataFrame:
    """Read Bronze incrementally using a watermark on _ingest_timestamp.

    Falls back to full read when no watermark (first run).
    """
    if high_watermark:
        return spark.read.table(bronze_table).filter(
            F.col("_ingest_timestamp") > F.lit(high_watermark)
        )
    return spark.read.table(bronze_table)


def get_current_watermark(
    spark: SparkSession, silver_table: str, tracker_table: str
) -> str | None:
    """Return the max _ingest_timestamp already processed into silver, or None."""
    try:
        row = spark.sql(
            f"SELECT MAX(_source_ingest_ts) AS wm FROM {silver_table}"
        ).collect()
        return row[0]["wm"].isoformat() if row and row[0]["wm"] else None
    except Exception:  # noqa: BLE001  — first run, table doesn't exist
        return None


# ---------------------------------------------------------------------------
# Enrichment functions
# ---------------------------------------------------------------------------
def _signal_quality_bucket(col: F.Column) -> F.Column:
    """WiFi dBm → qualitative bucket (industry-standard thresholds)."""
    return (
        F.when(col >= -50, F.lit("EXCELLENT"))
        .when(col >= -60, F.lit("GOOD"))
        .when(col >= -70, F.lit("FAIR"))
        .when(col >= -80, F.lit("POOR"))
        .otherwise(F.lit("UNUSABLE"))
    )


def _health_score(df: DataFrame) -> F.Column:
    """Composite health score [0, 100].

    Weighted: signal 40%, CPU 20%, memory 20%, throughput ratio 20%.
    Each sub-score is clamped to [0, 100]. Nulls contribute a neutral 50.
    """
    signal_sub = F.when(
        F.col("signal_strength_dbm").isNull(), F.lit(50.0)
    ).otherwise(
        F.greatest(
            F.lit(0.0),
            F.least(F.lit(100.0), (F.col("signal_strength_dbm") + 100) * 2),
        )
    )
    cpu_sub = F.when(F.col("cpu_utilization_pct").isNull(), F.lit(50.0)).otherwise(
        F.greatest(F.lit(0.0), F.lit(100.0) - F.col("cpu_utilization_pct"))
    )
    mem_sub = F.when(
        F.col("memory_utilization_pct").isNull(), F.lit(50.0)
    ).otherwise(
        F.greatest(F.lit(0.0), F.lit(100.0) - F.col("memory_utilization_pct"))
    )
    tp_ratio_sub = F.when(
        F.col("throughput_mbps_down").isNull() | (F.col("throughput_mbps_down") == 0),
        F.lit(50.0),
    ).otherwise(
        F.greatest(
            F.lit(0.0),
            F.least(
                F.lit(100.0),
                F.col("throughput_mbps_up") / F.col("throughput_mbps_down") * 100,
            ),
        )
    )
    return F.round(
        signal_sub * 0.40 + cpu_sub * 0.20 + mem_sub * 0.20 + tp_ratio_sub * 0.20, 2
    )


def transform_to_silver(df: DataFrame) -> DataFrame:
    """Apply enrichments and project to the Silver schema shape."""
    return (
        df.withColumn("event_date", F.date_format(F.col("event_timestamp"), "yyyy-MM-dd"))
        .withColumn("event_hour", F.hour(F.col("event_timestamp")))
        .withColumn("signal_quality_bucket", _signal_quality_bucket(F.col("signal_strength_dbm")))
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
        .withColumn("health_score", _health_score(df))
        .withColumn("_processed_timestamp", F.current_timestamp())
        .withColumnRenamed("_ingest_timestamp", "_source_ingest_ts")
        .select(
            "event_id",
            "tenant_id",
            "device_id",
            "device_model",
            "firmware_version",
            "event_timestamp",
            "event_date",
            "event_hour",
            "signal_strength_dbm",
            "signal_quality_bucket",
            "throughput_mbps_down",
            "throughput_mbps_up",
            "throughput_ratio",
            "connected_clients",
            "cpu_utilization_pct",
            "memory_utilization_pct",
            "health_score",
            "reboot_flag",
            "error_count",
            "geo_region",
            "_source_ingest_ts",
            "_processed_timestamp",
        )
    )


def upsert_silver(spark: SparkSession, df: DataFrame, target_table: str) -> int:
    """MERGE into silver on event_id — exactly-once semantics.

    Creates the table if it doesn't exist; otherwise upserts.
    """
    count = df.count()
    if count == 0:
        return 0

    if not spark.catalog.tableExists(target_table):
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy("event_date", "tenant_id")
            .saveAsTable(target_table)
        )
        return count

    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(df.alias("s"), "t.event_id = s.event_id AND t.tenant_id = s.tenant_id")
        .whenNotMatchedInsertAll()
        .whenMatchedUpdateAll()  # late-arriving corrections overwrite
        .execute()
    )
    return count


def optimize_silver(spark: SparkSession, table: str, z_order_cols: list[str]) -> None:
    """OPTIMIZE + Z-ORDER to compact small files and co-locate on hot columns.

    Why Z-Order on device_id + event_timestamp?
    - device_id is the highest-cardinality filter predicate in downstream queries
    - event_timestamp drives time-window queries
    - Co-locating these in the same files reduces data scanned for point queries
    """
    cols = ", ".join(z_order_cols)
    spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")


def write_quarantine(
    df: DataFrame, quarantine_table: str, spark: SparkSession, catalog: str, schema: str
) -> int:
    """Write failed rows to a quarantine table for investigation."""
    if df.rdd.isEmpty():
        return 0
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{schema}")
    count = df.count()
    (
        df.withColumn("_quarantined_at", F.current_timestamp())
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_table)
    )
    return count


def main(config_path: str) -> int:  # noqa: PLR0915
    cfg = load_config(config_path)
    configure_logging(level=cfg["logging"]["level"], fmt=cfg["logging"]["format"])
    log = get_logger(__name__, layer=LAYER, env=cfg["environment"])

    spark = get_spark_session(
        app_name=cfg["spark"]["app_name"],
        master=cfg["spark"].get("master"),
        configs=cfg["spark"].get("configs", {}),
    )

    catalog = cfg["catalog"]["name"]
    bronze_table = f"{catalog}.{cfg['catalog']['bronze_schema']}.raw_telemetry"
    silver_schema = cfg["catalog"]["silver_schema"]
    silver_table = f"{catalog}.{silver_schema}.device_events"
    quarantine_table = f"{catalog}.{silver_schema}.quarantine_device_events"
    control_table = f"{catalog}.{silver_schema}._batch_control"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{silver_schema}")

    batch_id = f"silver_{uuid.uuid4().hex[:12]}"
    tracker = IdempotencyTracker(spark, control_table)
    started_at = tracker.mark_started(batch_id, LAYER)
    log.info("silver_transform_started", batch_id=batch_id, source=bronze_table)

    try:
        watermark = get_current_watermark(spark, silver_table, control_table)
        log.info("watermark_determined", watermark=watermark)

        bronze_df = read_bronze_incremental(spark, bronze_table, watermark)
        bronze_count = bronze_df.count()
        log.info("bronze_rows_to_process", count=bronze_count)

        if bronze_count == 0:
            log.info("no_new_rows_nothing_to_do", batch_id=batch_id)
            tracker.mark_processed(batch_id, LAYER, started_at, row_count=0)
            return 0

        # Data Quality
        clean_df, quarantine_df, dq_result = apply_rules(bronze_df, TELEMETRY_RULES)
        log.info(
            "dq_complete",
            total=dq_result.total,
            passed=dq_result.passed,
            quarantined=dq_result.quarantined,
            critical_failures=dq_result.critical_failures,
            violations=dq_result.rule_violations,
        )

        if (
            cfg["data_quality"]["fail_on_critical"]
            and dq_result.critical_failures > 0
        ):
            critical_rules = [
                r.name for r in TELEMETRY_RULES if r.severity == Severity.CRITICAL
            ]
            raise ValueError(
                f"Critical DQ failures: {dq_result.critical_failures} rows failed "
                f"one of {critical_rules}"
            )

        quarantined_count = 0
        if cfg["data_quality"]["quarantine_enabled"]:
            quarantined_count = write_quarantine(
                quarantine_df, quarantine_table, spark, catalog, silver_schema
            )

        # Enrich + write
        silver_df = transform_to_silver(clean_df)
        written = upsert_silver(spark, silver_df, silver_table)
        log.info("silver_upserted", count=written, quarantined=quarantined_count)

        # Optimize (optional, based on config)
        if cfg["performance"]["optimize_after_load"]:
            z_cols = cfg["performance"]["z_order_columns"].get(
                "silver_device_events", ["device_id"]
            )
            optimize_silver(spark, silver_table, z_cols)
            log.info("silver_optimized", z_order=z_cols)

        tracker.mark_processed(batch_id, LAYER, started_at, row_count=written)
        return 0

    except Exception as exc:
        log.error(
            "silver_transform_failed",
            batch_id=batch_id,
            error=str(exc),
            traceback=traceback.format_exc(),
        )
        tracker.mark_failed(batch_id, LAYER, started_at, str(exc))
        return 1


def cli() -> None:
    parser = argparse.ArgumentParser(description="Silver telemetry transform")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    sys.exit(main(args.config))


if __name__ == "__main__":
    cli()
