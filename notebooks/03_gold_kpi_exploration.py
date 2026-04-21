# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold KPI Exploration
# MAGIC
# MAGIC **Purpose:** Interactive exploration of Gold KPIs. Ideal for tenant SLA reports,
# MAGIC device health investigations, and feeding Tableau / Power BI.
# MAGIC
# MAGIC **Author:** Sushma K L
# MAGIC **Layer:** Gold

# COMMAND ----------

dbutils.widgets.text("catalog", "iot_lakehouse_dev")
dbutils.widgets.text("schema", "gold")
dbutils.widgets.text("tenant_id", "tenant_a")
dbutils.widgets.text("window_days", "30")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
tenant_id = dbutils.widgets.get("tenant_id")
window_days = int(dbutils.widgets.get("window_days"))

# COMMAND ----------

# MAGIC %md ## 1. Tenant SLA — hourly healthy device %

# COMMAND ----------

sla = spark.read.table(f"{catalog}.{schema}.tenant_sla_hourly").filter(
    (f"tenant_id = '{tenant_id}'")
    + f" AND event_date >= date_sub(current_date(), {window_days})"
)
display(sla.orderBy("event_date", "event_hour"))

# COMMAND ----------

# MAGIC %md ## 2. Worst-performing devices (last 7 days)

# COMMAND ----------

from pyspark.sql import functions as F

dhd = spark.read.table(f"{catalog}.{schema}.device_health_daily").filter(
    (F.col("tenant_id") == tenant_id)
    & (F.col("event_date") >= F.date_sub(F.current_date(), 7))
)

display(
    dhd.groupBy("device_id")
    .agg(
        F.avg("avg_health_score").alias("avg_health_score"),
        F.sum("reboot_count").alias("total_reboots"),
        F.sum("total_errors").alias("total_errors"),
        F.sum("poor_signal_events").alias("poor_signal_events"),
    )
    .orderBy("avg_health_score")
    .limit(50)
)

# COMMAND ----------

# MAGIC %md ## 3. Reboot storm detection
# MAGIC A device with >3 reboots in a day is an anomaly worth investigating.

# COMMAND ----------

display(
    spark.read.table(f"{catalog}.{schema}.device_health_daily")
    .filter(F.col("reboot_count") > 3)
    .filter(F.col("event_date") >= F.date_sub(F.current_date(), window_days))
    .select("tenant_id", "device_id", "event_date", "reboot_count", "avg_health_score")
    .orderBy(F.desc("reboot_count"))
)

# COMMAND ----------

# MAGIC %md ## 4. Throughput trending — tenant-wide P50 / P95 / P99

# COMMAND ----------

display(
    spark.read.table(f"{catalog}.{schema}.network_performance_daily")
    .filter(
        (F.col("tenant_id") == tenant_id)
        & (F.col("event_date") >= F.date_sub(F.current_date(), window_days))
    )
    .select(
        "event_date",
        "active_devices",
        "throughput_down_p50_mbps",
        "throughput_down_p95_mbps",
        "throughput_down_p99_mbps",
        "avg_signal_dbm",
    )
    .orderBy("event_date")
)

# COMMAND ----------

# MAGIC %md ## 5. Firmware-level reboot rate
# MAGIC Useful for identifying bad firmware releases.

# COMMAND ----------

display(
    spark.read.table(f"{catalog}.{schema}.reboot_events")
    .filter(F.col("event_date") >= F.date_sub(F.current_date(), window_days))
    .groupBy("tenant_id", "firmware_version")
    .agg(F.count("*").alias("reboot_count"))
    .orderBy(F.desc("reboot_count"))
)
