output "catalog_name" {
  value = databricks_catalog.main.name
}

output "bronze_schema_full_name" {
  value = "${databricks_catalog.main.name}.${databricks_schema.bronze.name}"
}

output "silver_schema_full_name" {
  value = "${databricks_catalog.main.name}.${databricks_schema.silver.name}"
}

output "gold_schema_full_name" {
  value = "${databricks_catalog.main.name}.${databricks_schema.gold.name}"
}
