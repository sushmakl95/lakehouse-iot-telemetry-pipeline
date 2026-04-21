variable "aws_region" {
  type        = string
  description = "AWS region for all resources"
  default     = "ap-south-1"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, staging, prod)"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "project_prefix" {
  type        = string
  description = "Prefix used for all resource names"
  default     = "iot-telemetry"
}

variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL (e.g., https://adb-xxx.cloud.databricks.com)"
}

variable "catalog_name" {
  type        = string
  description = "Unity Catalog name"
}

variable "bronze_schema" {
  type        = string
  default     = "bronze"
}

variable "silver_schema" {
  type        = string
  default     = "silver"
}

variable "gold_schema" {
  type        = string
  default     = "gold"
}

variable "admin_group" {
  type        = string
  description = "Databricks group with CAN_MANAGE on catalog"
  default     = "data-platform-admins"
}

variable "consumer_group" {
  type        = string
  description = "Databricks group with SELECT on gold schema"
  default     = "data-consumers"
}
