"""Delta Live Tables: Bronze ingest.

Why DLT alongside the standalone scripts?
- DLT gives declarative lineage, managed expectations, and auto-scaling on Databricks.
- The standalone scripts (src/bronze/*) cover local dev + CI where DLT isn't available.
- Both read the same schemas from src.schemas, so rules stay in sync.

Deploy via databricks.yml (resources/pipelines.yml).
"""

from __future__ import annotations

import dlt  # type: ignore[import-not-found]  # only resolvable in DLT runtime
from pyspark.sql import functions as F

from src.schemas.telemetry import RAW_TELEMETRY_SCHEMA


@dlt.table(
    name="bronze_raw_telemetry",
    comment="Raw IoT telemetry events, append-only, partitioned by ingest date.",
    partition_cols=["_ingest_date", "tenant_id"],
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "device_id,event_timestamp",
        "delta.appendOnly": "true",
    },
)
@dlt.expect("valid_event_id", "event_id IS NOT NULL")
def bronze_raw_telemetry():  # noqa: ANN201
    """Ingest raw telemetry from the landing path via Auto Loader."""
    landing = spark.conf.get("landing_path")  # noqa: F821  — spark is global in DLT

    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", "1000")
        .schema(RAW_TELEMETRY_SCHEMA)
        .load(landing)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingest_date", F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
    )
