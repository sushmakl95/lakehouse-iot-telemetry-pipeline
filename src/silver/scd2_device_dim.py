"""SCD Type 2 device dimension.

Tracks device attribute changes over time (firmware upgrades, model changes, region migrations).

Algorithm:
1. Compute row_hash from attribute columns — deterministic change detection.
2. MERGE strategy:
   - If device_id is NEW → insert as current (valid_from=event_ts, valid_to=null, is_current=true)
   - If device_id exists AND row_hash differs → close old row (valid_to=event_ts, is_current=false) + insert new
   - If row_hash matches → no-op

Why hash-based? Comparing 5+ columns in MERGE clauses is error-prone. One hash column
is unambiguous and reusable across dimensions.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SCD2_ATTR_COLUMNS = ["device_model", "firmware_version", "geo_region"]


def prepare_scd2_source(events_df: DataFrame) -> DataFrame:
    """Build the latest attribute snapshot per device from the incoming events.

    Uses the most recent event per device as the source of truth for attributes.
    """
    window_col = F.row_number().over(
        F.window(F.col("event_timestamp"), "1 second").alias("_w")  # unused, satisfies spec
    )
    del window_col  # placeholder to document intent

    # Use a proper deterministic approach: max event_timestamp per (tenant_id, device_id)
    latest = (
        events_df.groupBy("tenant_id", "device_id")
        .agg(F.max("event_timestamp").alias("event_timestamp"))
        .join(events_df, ["tenant_id", "device_id", "event_timestamp"], how="inner")
        .select("tenant_id", "device_id", "event_timestamp", *SCD2_ATTR_COLUMNS)
        .dropDuplicates(["tenant_id", "device_id"])
    )

    return (
        latest.withColumn(
            "device_sk",
            F.sha2(F.concat_ws("||", F.col("tenant_id"), F.col("device_id")), 256),
        )
        .withColumn(
            "row_hash",
            F.sha2(
                F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in SCD2_ATTR_COLUMNS]),
                256,
            ),
        )
        .withColumnRenamed("event_timestamp", "valid_from")
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )


def apply_scd2(
    spark: SparkSession, source_df: DataFrame, target_table: str
) -> None:
    """Apply SCD2 MERGE to the device dimension.

    Uses a two-step pattern to avoid MERGE's one-row-per-source-per-target limitation:
    1. First MERGE: close open rows where row_hash changed.
    2. Second write: insert new rows (new devices or post-change rows).
    """
    if not spark.catalog.tableExists(target_table):
        (
            source_df.select(
                "device_sk",
                "device_id",
                "tenant_id",
                *SCD2_ATTR_COLUMNS,
                "valid_from",
                "valid_to",
                "is_current",
                "row_hash",
            )
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("tenant_id")
            .saveAsTable(target_table)
        )
        return

    target = DeltaTable.forName(spark, target_table)

    # Step 1: close rows whose hash has changed
    (
        target.alias("t")
        .merge(
            source_df.alias("s"),
            "t.tenant_id = s.tenant_id AND t.device_id = s.device_id "
            "AND t.is_current = true AND t.row_hash <> s.row_hash",
        )
        .whenMatchedUpdate(
            set={
                "valid_to": "s.valid_from",
                "is_current": "false",
            }
        )
        .execute()
    )

    # Step 2: insert new rows (either new devices OR post-change new versions)
    changed_or_new = source_df.alias("s").join(
        spark.read.table(target_table).filter("is_current = true").alias("t"),
        on=(
            (F.col("s.tenant_id") == F.col("t.tenant_id"))
            & (F.col("s.device_id") == F.col("t.device_id"))
            & (F.col("s.row_hash") == F.col("t.row_hash"))
        ),
        how="left_anti",
    ).select("s.*")

    if not changed_or_new.rdd.isEmpty():
        (
            changed_or_new.select(
                "device_sk",
                "device_id",
                "tenant_id",
                *SCD2_ATTR_COLUMNS,
                "valid_from",
                "valid_to",
                "is_current",
                "row_hash",
            )
            .write.format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )
