variable "bucket_name" {
  type        = string
  description = "S3 bucket name"
}

variable "environment" {
  type        = string
  description = "Environment tag"
}

variable "enable_versioning" {
  type        = bool
  default     = true
  description = "Enable S3 versioning"
}

variable "enable_encryption" {
  type        = bool
  default     = true
  description = "Enable SSE-S3 encryption"
}

variable "lifecycle_rules" {
  type = list(object({
    id      = string
    enabled = bool
    days    = number
    prefix  = string
  }))
  default     = []
  description = "Lifecycle rules for object expiration"
}
