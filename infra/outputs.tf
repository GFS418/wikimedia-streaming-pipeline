output "landing_bucket" {
  value = aws_s3_bucket.landing.bucket
}

output "landing_url" {
  description = "Use as the Databricks external location URL."
  value       = "s3://${aws_s3_bucket.landing.bucket}/landing/"
}

output "databricks_role_arn" {
  description = "Paste into the Databricks storage credential."
  value       = aws_iam_role.databricks_uc.arn
}
