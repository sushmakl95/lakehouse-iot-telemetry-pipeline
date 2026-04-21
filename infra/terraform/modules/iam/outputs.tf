output "databricks_instance_profile_arn" {
  value = aws_iam_instance_profile.databricks.arn
}

output "databricks_role_arn" {
  value = aws_iam_role.databricks_cluster_role.arn
}

output "autoloader_queue_url" {
  value = aws_sqs_queue.autoloader_events.url
}

output "autoloader_queue_arn" {
  value = aws_sqs_queue.autoloader_events.arn
}
