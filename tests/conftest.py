"""Shared pytest fixtures for unit + integration tests.

The `spark` fixture is session-scoped — a single Spark session is expensive to
create (JVM + Delta JARs), so we reuse across tests.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session-scoped SparkSession with Delta Lake configured."""
    session = (
        SparkSession.builder.appName("iot-telemetry-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="spark-warehouse-"))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


@pytest.fixture
def tmp_warehouse(tmp_path: Path) -> Path:
    """Temp directory for Delta tables in a single test."""
    path = tmp_path / "warehouse"
    path.mkdir()
    yield path
    shutil.rmtree(path, ignore_errors=True)
