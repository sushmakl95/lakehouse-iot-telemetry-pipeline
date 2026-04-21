output "landing_bucket" {
  value       = module.s3_landing.bucket_name
  description = "S3 bucket for raw event landing"
}

output "landing_bucket_arn" {
  value       = module.s3_landing.bucket_arn
  description = "ARN of the landing S3 bucket"
}

output "lakehouse_bucket" {
  value       = module.s3_lakehouse.bucket_name
  description = "S3 bucket for Bronze/Silver/Gold data"
}

output "lakehouse_bucket_arn" {
  value       = module.s3_lakehouse.bucket_arn
  description = "ARN of the lakehouse S3 bucket"
}

output "databricks_instance_profile" {
  value       = module.iam.databricks_instance_profile_arn
  description = "ARN of the IAM instance profile for Databricks clusters"
}

output "catalog_name" {
  value       = var.catalog_name
  description = "Unity Catalog name"
}
