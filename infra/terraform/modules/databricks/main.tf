# Unity Catalog objects: catalog, schemas, grants.
# Assumes workspace already has a metastore attached.

resource "databricks_catalog" "main" {
  name    = var.catalog_name
  comment = "IoT Lakehouse catalog managed by terraform"

  properties = {
    purpose = "iot-telemetry"
  }
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.main.name
  name         = var.bronze_schema
  comment      = "Raw ingested telemetry — append-only"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.main.name
  name         = var.silver_schema
  comment      = "Cleansed and enriched telemetry + SCD2 dimensions"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.main.name
  name         = var.gold_schema
  comment      = "Business-facing KPIs and aggregates"
}

# Grants — admin group owns everything, consumer group reads gold
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.main.name

  grant {
    principal  = var.admin_group
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = var.consumer_group
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "bronze" {
  schema = "${databricks_catalog.main.name}.${databricks_schema.bronze.name}"

  grant {
    principal  = var.admin_group
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_grants" "silver" {
  schema = "${databricks_catalog.main.name}.${databricks_schema.silver.name}"

  grant {
    principal  = var.admin_group
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_grants" "gold" {
  schema = "${databricks_catalog.main.name}.${databricks_schema.gold.name}"

  grant {
    principal  = var.admin_group
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = var.consumer_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}
