terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.30"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.38"
    }
  }

  backend "s3" {
    # Configure via backend-config file:
    # terraform init -backend-config=backend-dev.hcl
    key = "lakehouse-iot-telemetry-pipeline/terraform.tfstate"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "lakehouse-iot-telemetry-pipeline"
      Environment = var.environment
      Owner       = "data-platform"
      ManagedBy   = "terraform"
    }
  }
}

provider "databricks" {
  host = var.databricks_host
  # Auth via DATABRICKS_TOKEN env var
}

# -----------------------------------------------------------------------------
# Core infrastructure
# -----------------------------------------------------------------------------
module "s3_landing" {
  source = "./modules/s3"

  bucket_name = "${var.project_prefix}-landing-${var.environment}"
  environment = var.environment

  lifecycle_rules = [
    {
      id      = "expire-old-raw-events"
      enabled = true
      days    = 90
      prefix  = "events/"
    }
  ]

  enable_encryption = true
  enable_versioning = true
}

module "s3_lakehouse" {
  source = "./modules/s3"

  bucket_name = "${var.project_prefix}-lakehouse-${var.environment}"
  environment = var.environment

  lifecycle_rules = [
    {
      id      = "expire-checkpoints"
      enabled = true
      days    = 30
      prefix  = "checkpoints/"
    },
    {
      id      = "expire-quarantine"
      enabled = true
      days    = 180
      prefix  = "quarantine/"
    }
  ]

  enable_encryption = true
  enable_versioning = true
}

module "iam" {
  source = "./modules/iam"

  project_prefix  = var.project_prefix
  environment     = var.environment
  landing_bucket  = module.s3_landing.bucket_arn
  lakehouse_bucket = module.s3_lakehouse.bucket_arn
}

module "databricks_workspace_objects" {
  source = "./modules/databricks"

  catalog_name   = var.catalog_name
  bronze_schema  = var.bronze_schema
  silver_schema  = var.silver_schema
  gold_schema    = var.gold_schema
  admin_group    = var.admin_group
  consumer_group = var.consumer_group
}
