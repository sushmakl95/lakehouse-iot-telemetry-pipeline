# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Table Maintenance: OPTIMIZE + VACUUM
# MAGIC
# MAGIC **Purpose:** Scheduled maintenance task. Compacts small files, runs Z-ORDER, and
# MAGIC vacuums old file versions.
# MAGIC
# MAGIC **Cadence:** Runs daily from the Databricks workflow (after Gold completes).
# MAGIC
# MAGIC **Why this matters for cost:**
# MAGIC - Small-file compaction cuts read costs by 2–5x on partitioned tables
# MAGIC - Z-ORDER reduces data scanned for common predicates by 5–20x
# MAGIC - VACUUM reclaims S3 storage from old file versions
# MAGIC
# MAGIC **Author:** Sushma K L

# COMMAND ----------

dbutils.widgets.text("catalog", "iot_lakehouse_dev")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("vacuum_retention_hours", "168")

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
vacuum_retention = int(dbutils.widgets.get("vacuum_retention_hours"))

# COMMAND ----------

# MAGIC %md ## 1. Bronze: OPTIMIZE + VACUUM
# MAGIC Bronze is append-only and high-volume. Small-file compaction is the biggest win.

# COMMAND ----------

bronze_table = f"{catalog}.{bronze_schema}.raw_telemetry"
print(f"Optimizing {bronze_table}...")
spark.sql(f"OPTIMIZE {bronze_table} ZORDER BY (device_id, event_timestamp)")
spark.sql(f"VACUUM {bronze_table} RETAIN {vacuum_retention} HOURS")

# COMMAND ----------

# MAGIC %md ## 2. Silver: OPTIMIZE + VACUUM

# COMMAND ----------

silver_table = f"{catalog}.{silver_schema}.device_events"
print(f"Optimizing {silver_table}...")
spark.sql(f"OPTIMIZE {silver_table} ZORDER BY (device_id, event_timestamp)")
spark.sql(f"VACUUM {silver_table} RETAIN {vacuum_retention} HOURS")

# COMMAND ----------

# MAGIC %md ## 3. Gold: OPTIMIZE only
# MAGIC Gold tables are smaller aggregates; aggressive VACUUM isn't needed.

# COMMAND ----------

for gt in ["device_health_daily", "tenant_sla_hourly", "reboot_events", "network_performance_daily"]:
    full = f"{catalog}.{gold_schema}.{gt}"
    try:
        print(f"Optimizing {full}...")
        if gt == "device_health_daily":
            spark.sql(f"OPTIMIZE {full} ZORDER BY (device_id)")
        elif gt == "tenant_sla_hourly":
            spark.sql(f"OPTIMIZE {full} ZORDER BY (tenant_id, event_hour)")
        else:
            spark.sql(f"OPTIMIZE {full}")
    except Exception as e:
        print(f"[warn] Could not optimize {full}: {e}")

# COMMAND ----------

# MAGIC %md ## 4. Size + file-count report
# MAGIC Emits metrics to a monitoring table so we can trend file counts over time.

# COMMAND ----------

from pyspark.sql import functions as F

tables = [
    f"{catalog}.{bronze_schema}.raw_telemetry",
    f"{catalog}.{silver_schema}.device_events",
    f"{catalog}.{gold_schema}.device_health_daily",
    f"{catalog}.{gold_schema}.tenant_sla_hourly",
]

metrics = []
for t in tables:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {t}").collect()[0]
        metrics.append(
            (t, detail["numFiles"], detail["sizeInBytes"], detail["lastModified"])
        )
    except Exception as e:
        print(f"[warn] {t}: {e}")

metrics_df = spark.createDataFrame(
    metrics, ["table_name", "num_files", "size_bytes", "last_modified"]
).withColumn("observed_at", F.current_timestamp()).withColumn(
    "size_gb", F.round(F.col("size_bytes") / (1024 * 1024 * 1024), 3)
)

display(metrics_df)

metrics_df.write.format("delta").mode("append").saveAsTable(
    f"{catalog}.{gold_schema}._table_maintenance_metrics"
)
