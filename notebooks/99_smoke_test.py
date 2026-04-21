# Databricks notebook source
# MAGIC %md
# MAGIC # 99 — Smoke Test
# MAGIC
# MAGIC **Purpose:** Post-deployment sanity check. Verifies catalog/schemas exist, tables
# MAGIC are queryable, and sample data flows through all layers.
# MAGIC
# MAGIC **Invoked by:** GitHub Actions `deploy.yml` workflow after bundle deploy.

# COMMAND ----------

dbutils.widgets.text("catalog", "iot_lakehouse_dev")

catalog = dbutils.widgets.get("catalog")
failures = []

def check(name: str, fn):
    try:
        fn()
        print(f"[ok] {name}")
    except Exception as e:
        print(f"[FAIL] {name}: {e}")
        failures.append(name)

# COMMAND ----------

check("catalog exists", lambda: spark.sql(f"DESCRIBE CATALOG {catalog}"))

for schema in ["bronze", "silver", "gold"]:
    check(f"schema {schema} exists", lambda s=schema: spark.sql(f"DESCRIBE SCHEMA {catalog}.{s}"))

check("bronze raw_telemetry queryable", lambda: spark.read.table(f"{catalog}.bronze.raw_telemetry").limit(1).collect())
check("silver device_events queryable", lambda: spark.read.table(f"{catalog}.silver.device_events").limit(1).collect())
check("gold device_health_daily queryable", lambda: spark.read.table(f"{catalog}.gold.device_health_daily").limit(1).collect())

# COMMAND ----------

if failures:
    raise AssertionError(f"Smoke test failed: {failures}")
print("Smoke test PASSED")
