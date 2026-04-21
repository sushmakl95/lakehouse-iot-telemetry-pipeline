# IAM role assumed by Databricks cluster EC2 instances.
# Principle of least privilege: only the buckets this pipeline needs.

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "databricks_cluster_role" {
  name = "${var.project_prefix}-databricks-cluster-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Environment = var.environment
  }
}

resource "aws_iam_role_policy" "s3_access" {
  name = "${var.project_prefix}-s3-access-${var.environment}"
  role = aws_iam_role.databricks_cluster_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "BucketList"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketNotification",
        ]
        Resource = [
          var.landing_bucket,
          var.lakehouse_bucket,
        ]
      },
      {
        Sid    = "ObjectRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
        ]
        Resource = [
          "${var.landing_bucket}/*",
          "${var.lakehouse_bucket}/*",
        ]
      },
      {
        Sid    = "ObjectWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = [
          "${var.lakehouse_bucket}/*",
        ]
      }
    ]
  })
}

# SQS queue for S3 event notifications (powers Auto Loader file notification mode)
resource "aws_sqs_queue" "autoloader_events_dlq" {
  name                      = "${var.project_prefix}-autoloader-dlq-${var.environment}"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "autoloader_events" {
  name                       = "${var.project_prefix}-autoloader-events-${var.environment}"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 345600 # 4 days
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.autoloader_events_dlq.arn
    maxReceiveCount     = 5
  })
}

resource "aws_iam_role_policy" "sqs_access" {
  name = "${var.project_prefix}-sqs-access-${var.environment}"
  role = aws_iam_role.databricks_cluster_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ChangeMessageVisibility",
        ]
        Resource = [
          aws_sqs_queue.autoloader_events.arn,
          aws_sqs_queue.autoloader_events_dlq.arn,
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "databricks" {
  name = "${var.project_prefix}-databricks-ip-${var.environment}"
  role = aws_iam_role.databricks_cluster_role.name
}

# CloudWatch logs for job cluster log delivery
resource "aws_iam_role_policy" "cloudwatch_logs" {
  name = "${var.project_prefix}-cw-logs-${var.environment}"
  role = aws_iam_role.databricks_cluster_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
        ]
        Resource = "arn:aws:logs:*:${data.aws_caller_identity.current.account_id}:*"
      }
    ]
  })
}
