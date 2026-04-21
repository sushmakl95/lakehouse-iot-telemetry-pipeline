"""Historical backfill utility.

Re-runs the Bronze → Silver → Gold pipeline for a specified date range.
Useful for:
- Recovering from a pipeline outage
- Rebuilding Gold after a DQ rule change
- Onboarding a new tenant's historical data

Usage:
    python scripts/backfill.py --config config/prod.yaml \\
        --start 2026-01-01 --end 2026-03-31 \\
        --layers silver gold \\
        --tenant tenant_a

Why a separate script vs. just re-running the pipeline?
- Backfills need to process specific date partitions, not whatever's new
- Silver MERGE can hit concurrent-write conflicts with the live pipeline — we pause scheduled runs
- Large backfills need different cluster sizing (more workers)
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from typing import Sequence

from pyspark.sql import functions as F

from src.utils import configure_logging, get_logger, get_spark_session, load_config


def daterange(start: date, end: date) -> Sequence[date]:
    days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(days)]


def backfill_silver_partition(spark, cfg, target_date: date, tenant: str | None) -> int:
    """Re-derive silver for a single date partition, optionally filtered by tenant."""
    from src.silver.transform_telemetry import transform_to_silver
    from src.data_quality import TELEMETRY_RULES, apply_rules

    catalog = cfg["catalog"]["name"]
    bronze_table = f"{catalog}.{cfg['catalog']['bronze_schema']}.raw_telemetry"
    silver_table = f"{catalog}.{cfg['catalog']['silver_schema']}.device_events"

    df = spark.read.table(bronze_table).filter(
        F.date_format(F.col("event_timestamp"), "yyyy-MM-dd") == target_date.isoformat()
    )
    if tenant:
        df = df.filter(F.col("tenant_id") == tenant)

    if df.rdd.isEmpty():
        return 0

    clean, _, _ = apply_rules(df, TELEMETRY_RULES)
    silver_df = transform_to_silver(clean)

    # replaceWhere = idempotent partition overwrite
    replace_where_parts = [f"event_date = '{target_date.isoformat()}'"]
    if tenant:
        replace_where_parts.append(f"tenant_id = '{tenant}'")
    replace_where = " AND ".join(replace_where_parts)

    count = silver_df.count()
    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .saveAsTable(silver_table)
    )
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Historical pipeline backfill")
    parser.add_argument("--config", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--layers",
        nargs="+",
        choices=["silver", "gold"],
        default=["silver", "gold"],
        help="Layers to backfill (bronze is append-only, not backfillable)",
    )
    parser.add_argument("--tenant", help="Optional tenant_id filter")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    configure_logging(level=cfg["logging"]["level"], fmt=cfg["logging"]["format"])
    log = get_logger(__name__, operation="backfill")

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    if end < start:
        log.error("invalid_range", start=str(start), end=str(end))
        return 1

    dates = daterange(start, end)
    log.info(
        "backfill_plan",
        start=str(start),
        end=str(end),
        day_count=len(dates),
        layers=args.layers,
        tenant=args.tenant,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        return 0

    spark = get_spark_session(
        app_name=cfg["spark"]["app_name"] + "-backfill",
        master=cfg["spark"].get("master"),
        configs=cfg["spark"].get("configs", {}),
    )

    total = 0
    for d in dates:
        if "silver" in args.layers:
            count = backfill_silver_partition(spark, cfg, d, args.tenant)
            log.info("silver_partition_done", date=str(d), rows=count)
            total += count

    # Gold is re-derived fully from Silver — just re-run the main aggregator
    if "gold" in args.layers:
        from src.gold.aggregate_kpis import main as gold_main
        rc = gold_main(args.config)
        if rc != 0:
            log.error("gold_backfill_failed", rc=rc)
            return rc

    log.info("backfill_complete", total_rows=total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
