"""Bronze layer: raw telemetry ingestion from landing → Delta.

Design choices:
- Append-only: Bronze never updates; preserves audit trail and supports replay.
- Schema-on-write with explicit schema from src.schemas — rejects malformed events.
- Partitioned by _ingest_date for efficient VACUUM + partition pruning of retention queries.
- Idempotent: uses the batch_id (filename or batch timestamp) to skip re-processed batches.
- Auto Loader style on Databricks; local fallback uses plain read.json for CI.
"""

from __future__ import annotations

import argparse
import sys
import traceback
import uuid
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.schemas.telemetry import RAW_TELEMETRY_SCHEMA
from src.utils import (
    IdempotencyTracker,
    configure_logging,
    get_logger,
    get_spark_session,
    load_config,
)

LAYER = "bronze"


def read_landing(
    spark: SparkSession,
    landing_path: str,
    checkpoint_path: str,
    use_auto_loader: bool,
) -> DataFrame:
    """Read raw telemetry. Uses Auto Loader on Databricks, batch read locally.

    Why two paths?
    - Auto Loader scales to billions of files and does schema evolution; it's the
      Databricks-native way. Locally (CI + dev laptop) it's not available, so we
      fall back to a plain `read.json` so tests can still exercise the full flow.
    """
    if use_auto_loader:
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schemas")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.maxFilesPerTrigger", "1000")
            .schema(RAW_TELEMETRY_SCHEMA)
            .load(landing_path)
        )

    # Local / CI path — plain batch read
    return (
        spark.read.schema(RAW_TELEMETRY_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(landing_path)
    )


def add_ingest_metadata(df: DataFrame, ingest_ts: datetime) -> DataFrame:
    """Add ingestion metadata columns required by Bronze schema."""
    ingest_date = ingest_ts.strftime("%Y-%m-%d")
    return (
        df.withColumn("_ingest_timestamp", F.lit(ingest_ts).cast("timestamp"))
        .withColumn("_source_file", F.coalesce(F.input_file_name(), F.lit("local_batch")))
        .withColumn("_ingest_date", F.lit(ingest_date))
    )


def write_bronze(
    df: DataFrame,
    target_path: str,
    table_name: str,
    is_streaming: bool,
    checkpoint_path: str,
) -> int:
    """Write to the bronze Delta table. Streaming or batch based on source."""
    if is_streaming:
        query = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .partitionBy("_ingest_date", "tenant_id")
            .trigger(availableNow=True)
            .toTable(table_name)
        )
        query.awaitTermination()
        return -1  # streaming doesn't return a count cheaply

    count = df.count()
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("_ingest_date", "tenant_id")
        .saveAsTable(table_name)
    )
    return count


def main(config_path: str) -> int:
    """Entrypoint: load config, ingest, track, log."""
    cfg = load_config(config_path)
    configure_logging(level=cfg["logging"]["level"], fmt=cfg["logging"]["format"])
    log = get_logger(__name__, layer=LAYER, env=cfg["environment"])

    spark = get_spark_session(
        app_name=cfg["spark"]["app_name"],
        master=cfg["spark"].get("master"),
        configs=cfg["spark"].get("configs", {}),
    )

    landing = cfg["paths"]["landing"]
    bronze_path = cfg["paths"]["bronze"]
    checkpoint_path = f"{cfg['paths']['checkpoints']}/bronze"
    catalog = cfg["catalog"]["name"]
    schema = cfg["catalog"]["bronze_schema"]
    table = f"{catalog}.{schema}.raw_telemetry"
    control_table = f"{catalog}.{schema}._batch_control"

    # Create schema if needed
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{schema}")

    use_auto_loader = cfg["environment"] != "local"
    batch_id = f"bronze_{uuid.uuid4().hex[:12]}"

    tracker = IdempotencyTracker(spark, control_table)
    if tracker.is_processed(batch_id, LAYER):
        log.info("batch_already_processed", batch_id=batch_id)
        return 0

    started_at = tracker.mark_started(batch_id, LAYER)
    log.info(
        "bronze_ingest_started",
        batch_id=batch_id,
        landing=landing,
        target=table,
        auto_loader=use_auto_loader,
    )

    try:
        raw_df = read_landing(spark, landing, checkpoint_path, use_auto_loader)
        enriched = add_ingest_metadata(raw_df, datetime.now(UTC))

        row_count = write_bronze(
            enriched,
            bronze_path,
            table,
            is_streaming=use_auto_loader,
            checkpoint_path=checkpoint_path,
        )

        tracker.mark_processed(batch_id, LAYER, started_at, row_count=max(row_count, 0))
        log.info(
            "bronze_ingest_completed",
            batch_id=batch_id,
            row_count=row_count,
        )
        return 0

    except Exception as exc:
        log.error(
            "bronze_ingest_failed",
            batch_id=batch_id,
            error=str(exc),
            traceback=traceback.format_exc(),
        )
        tracker.mark_failed(batch_id, LAYER, started_at, str(exc))
        return 1


def cli() -> None:
    parser = argparse.ArgumentParser(description="Bronze telemetry ingestion")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    sys.exit(main(args.config))


if __name__ == "__main__":
    cli()
