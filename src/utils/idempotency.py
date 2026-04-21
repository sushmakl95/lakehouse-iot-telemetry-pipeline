"""Idempotency tracker backed by a Delta control table.

Why? Pipelines retry on failure. Without an idempotency guard, the same batch
can be written twice, producing duplicates. Delta MERGE handles row-level
idempotency; this module handles batch-level (did this batch_id already run?).

Pattern:
    tracker = IdempotencyTracker(spark, "local_catalog.bronze._batch_control")
    if tracker.is_processed(batch_id):
        log.info("batch_already_processed", batch_id=batch_id)
        return
    # ... do work ...
    tracker.mark_processed(batch_id, row_count=n)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_CONTROL_TABLE_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), nullable=False),
        StructField("layer", StringType(), nullable=False),
        StructField("tenant_id", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),  # STARTED / SUCCESS / FAILED
        StructField("row_count", LongType(), nullable=True),
        StructField("started_at", TimestampType(), nullable=False),
        StructField("completed_at", TimestampType(), nullable=True),
        StructField("duration_ms", IntegerType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
    ]
)


@dataclass(frozen=True)
class BatchRecord:
    batch_id: str
    layer: str
    tenant_id: str | None
    status: str
    row_count: int | None
    started_at: datetime
    completed_at: datetime | None
    duration_ms: int | None
    error_message: str | None


class IdempotencyTracker:
    """Delta-backed idempotency + audit log."""

    def __init__(self, spark: SparkSession, control_table: str):
        self.spark = spark
        self.control_table = control_table
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the control table if it doesn't exist."""
        empty_df = self.spark.createDataFrame([], _CONTROL_TABLE_SCHEMA)
        (
            empty_df.write.format("delta")
            .mode("ignore")
            .option("mergeSchema", "true")
            .saveAsTable(self.control_table)
        )

    def is_processed(self, batch_id: str, layer: str) -> bool:
        """Return True if this batch has already completed successfully."""
        query = (
            f"SELECT COUNT(*) AS c FROM {self.control_table} "
            f"WHERE batch_id = '{batch_id}' AND layer = '{layer}' AND status = 'SUCCESS'"
        )
        return self.spark.sql(query).collect()[0]["c"] > 0

    def mark_started(
        self, batch_id: str, layer: str, tenant_id: str | None = None
    ) -> datetime:
        """Insert a STARTED record and return the start timestamp."""
        started_at = datetime.now(UTC)
        row = [(batch_id, layer, tenant_id, "STARTED", None, started_at, None, None, None)]
        df = self.spark.createDataFrame(row, _CONTROL_TABLE_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.control_table)
        return started_at

    def mark_processed(
        self,
        batch_id: str,
        layer: str,
        started_at: datetime,
        row_count: int,
        tenant_id: str | None = None,
    ) -> None:
        """Update the control row with SUCCESS + metrics."""
        completed_at = datetime.now(UTC)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)

        self.spark.sql(
            f"""
            MERGE INTO {self.control_table} t
            USING (
                SELECT '{batch_id}' AS batch_id, '{layer}' AS layer
            ) s
            ON t.batch_id = s.batch_id AND t.layer = s.layer AND t.status = 'STARTED'
            WHEN MATCHED THEN UPDATE SET
                status = 'SUCCESS',
                row_count = {row_count},
                completed_at = timestamp('{completed_at.isoformat()}'),
                duration_ms = {duration_ms}
            """
        )

    def mark_failed(
        self, batch_id: str, layer: str, started_at: datetime, error_message: str
    ) -> None:
        """Update the control row with FAILED + error."""
        completed_at = datetime.now(UTC)
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        safe_msg = error_message.replace("'", "''")[:1000]

        self.spark.sql(
            f"""
            MERGE INTO {self.control_table} t
            USING (
                SELECT '{batch_id}' AS batch_id, '{layer}' AS layer
            ) s
            ON t.batch_id = s.batch_id AND t.layer = s.layer AND t.status = 'STARTED'
            WHEN MATCHED THEN UPDATE SET
                status = 'FAILED',
                completed_at = timestamp('{completed_at.isoformat()}'),
                duration_ms = {duration_ms},
                error_message = '{safe_msg}'
            """
        )
