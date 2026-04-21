variable "project_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "landing_bucket" {
  type        = string
  description = "ARN of the landing S3 bucket"
}

variable "lakehouse_bucket" {
  type        = string
  description = "ARN of the lakehouse S3 bucket"
}
