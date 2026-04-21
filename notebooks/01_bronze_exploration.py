# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer Exploration
# MAGIC
# MAGIC **Purpose:** Inspect freshly ingested raw telemetry. Validates that Auto Loader is
# MAGIC running, schema evolution is behaving, and ingestion lag is within SLA.
# MAGIC
# MAGIC **Author:** Sushma K L
# MAGIC **Layer:** Bronze
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What this notebook does
# MAGIC 1. Reads the bronze table
# MAGIC 2. Shows freshness metrics (lag, event counts per tenant)
# MAGIC 3. Identifies schema drift (new columns appearing via Auto Loader)
# MAGIC 4. Surfaces corrupt / malformed events

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "iot_lakehouse_dev")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("table", "raw_telemetry")
dbutils.widgets.text("window_hours", "24")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
window_hours = int(dbutils.widgets.get("window_hours"))

fq_table = f"{catalog}.{schema}.{table}"
print(f"Inspecting {fq_table} over the last {window_hours}h")

# COMMAND ----------

# MAGIC %md ## 1. Row counts per tenant (last N hours)

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.read.table(fq_table).filter(
    F.col("_ingest_timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {window_hours} HOURS")
)

display(
    df.groupBy("tenant_id")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("device_id").alias("unique_devices"),
        F.min("event_timestamp").alias("earliest_event"),
        F.max("event_timestamp").alias("latest_event"),
        F.min("_ingest_timestamp").alias("earliest_ingest"),
        F.max("_ingest_timestamp").alias("latest_ingest"),
    )
    .orderBy("tenant_id")
)

# COMMAND ----------

# MAGIC %md ## 2. Ingestion lag distribution
# MAGIC
# MAGIC Lag = `_ingest_timestamp - event_timestamp`. High lag means device buffering or
# MAGIC Auto Loader backlog.

# COMMAND ----------

lag_df = df.withColumn(
    "ingest_lag_sec",
    (F.col("_ingest_timestamp").cast("long") - F.col("event_timestamp").cast("long"))
)

display(
    lag_df.groupBy("tenant_id").agg(
        F.avg("ingest_lag_sec").alias("avg_lag_sec"),
        F.expr("percentile_approx(ingest_lag_sec, 0.5)").alias("p50_lag_sec"),
        F.expr("percentile_approx(ingest_lag_sec, 0.95)").alias("p95_lag_sec"),
        F.expr("percentile_approx(ingest_lag_sec, 0.99)").alias("p99_lag_sec"),
        F.max("ingest_lag_sec").alias("max_lag_sec"),
    )
)

# COMMAND ----------

# MAGIC %md ## 3. Schema snapshot

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md ## 4. Delta table history (verify write cadence)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {fq_table} LIMIT 20"))

# COMMAND ----------

# MAGIC %md ## 5. Detect rows likely to fail DQ (preview of what Silver will quarantine)

# COMMAND ----------

dq_preview = df.select(
    F.sum(F.when(F.col("device_id").isNull(), 1).otherwise(0)).alias("null_device_id"),
    F.sum(F.when(F.col("event_timestamp") > F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"), 1).otherwise(0)).alias("future_timestamp"),
    F.sum(F.when(~F.col("signal_strength_dbm").between(-120, 0) & F.col("signal_strength_dbm").isNotNull(), 1).otherwise(0)).alias("signal_out_of_range"),
    F.sum(F.when(F.col("throughput_mbps_down") < 0, 1).otherwise(0)).alias("negative_throughput"),
)
display(dq_preview)
