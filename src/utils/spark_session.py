"""SparkSession factory that wires up Delta Lake and sensible defaults.

On Databricks the session is already configured; this is used for local dev
and CI integration tests where we need to bootstrap Delta manually.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "iot-telemetry-pipeline",
    master: str | None = None,
    configs: dict[str, Any] | None = None,
) -> SparkSession:
    """Return a SparkSession with Delta Lake extensions configured.

    When running on Databricks, an active session exists — we reuse it.
    When running locally, we build one with delta-spark packages loaded.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    )

    if master:
        builder = builder.master(master)

    if configs:
        for key, value in configs.items():
            builder = builder.config(key, str(value))

    return builder.getOrCreate()
