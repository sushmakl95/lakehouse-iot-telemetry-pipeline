# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Data Quality Dashboard
# MAGIC
# MAGIC **Purpose:** Ongoing DQ monitoring for the Silver layer. Surfaces quarantine trends,
# MAGIC rule violation hotspots, and SLA on data completeness per tenant.
# MAGIC
# MAGIC **Author:** Sushma K L
# MAGIC **Layer:** Silver

# COMMAND ----------

dbutils.widgets.text("catalog", "iot_lakehouse_dev")
dbutils.widgets.text("schema", "silver")
dbutils.widgets.text("window_days", "7")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
window_days = int(dbutils.widgets.get("window_days"))

silver_table = f"{catalog}.{schema}.device_events"
quarantine_table = f"{catalog}.{schema}.quarantine_device_events"

# COMMAND ----------

# MAGIC %md ## 1. Quarantine volume trend

# COMMAND ----------

from pyspark.sql import functions as F

q = spark.read.table(quarantine_table).filter(
    F.col("_quarantined_at") >= F.current_timestamp() - F.expr(f"INTERVAL {window_days} DAYS")
)

display(
    q.groupBy(F.to_date("_quarantined_at").alias("date"), "tenant_id")
    .count()
    .orderBy("date", "tenant_id")
)

# COMMAND ----------

# MAGIC %md ## 2. Top violated rules (last N days)

# COMMAND ----------

display(
    q.withColumn("rule", F.explode("_dq_failed_rules"))
    .groupBy("rule")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md ## 3. Quarantine % per tenant vs. total ingested

# COMMAND ----------

silver_count = (
    spark.read.table(silver_table)
    .filter(F.col("_processed_timestamp") >= F.current_timestamp() - F.expr(f"INTERVAL {window_days} DAYS"))
    .groupBy("tenant_id")
    .agg(F.count("*").alias("silver_rows"))
)
quarantine_count = q.groupBy("tenant_id").agg(F.count("*").alias("quarantined_rows"))

display(
    silver_count.join(quarantine_count, "tenant_id", "left")
    .fillna(0)
    .withColumn(
        "quarantine_pct",
        F.round(
            F.col("quarantined_rows")
            / (F.col("silver_rows") + F.col("quarantined_rows"))
            * 100,
            3,
        ),
    )
    .orderBy(F.desc("quarantine_pct"))
)

# COMMAND ----------

# MAGIC %md ## 4. Device coverage: devices present today vs. yesterday
# MAGIC
# MAGIC Drops in unique device counts indicate upstream issues (device offline, tenant outage).

# COMMAND ----------

display(
    spark.read.table(silver_table)
    .filter(F.col("event_date") >= F.date_sub(F.current_date(), window_days))
    .groupBy("event_date", "tenant_id")
    .agg(F.countDistinct("device_id").alias("unique_devices"))
    .orderBy("event_date", "tenant_id")
)

# COMMAND ----------

# MAGIC %md ## 5. Signal quality distribution

# COMMAND ----------

display(
    spark.read.table(silver_table)
    .filter(F.col("event_date") >= F.date_sub(F.current_date(), window_days))
    .groupBy("tenant_id", "signal_quality_bucket")
    .count()
    .orderBy("tenant_id", "signal_quality_bucket")
)
