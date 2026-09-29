variable "aws_region" {
  description = "AWS region for the landing bucket. Same region as the Databricks workspace (metastore_aws_us_east_2) to avoid cross-region transfer."
  type        = string
  default     = "us-east-2"
}

variable "landing_retention_days" {
  description = "Days before landed files expire. Bronze Delta is the durable copy; this only bounds replay-from-S3."
  type        = number
  default     = 30
}

variable "noncurrent_retention_days" {
  description = "Days to keep overwritten versions (evidence for rewrite-after-crash drills)."
  type        = number
  default     = 7
}

variable "databricks_uc_master_role_arn" {
  description = "Databricks Unity Catalog master role (AWS commercial), from the Databricks docs."
  type        = string
  default     = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
}

variable "databricks_external_id" {
  description = "External ID shown by Databricks when the storage credential is created. Placeholder until then."
  type        = string
  default     = "0000"
}

variable "self_assume" {
  description = "Add the role itself to its trust policy (required by Databricks). Only possible once the role exists."
  type        = bool
  default     = false
}

variable "monthly_budget_usd" {
  description = "Monthly budget for resources tagged project=wikistream."
  type        = string
  default     = "5"
}

variable "budget_alert_email" {
  description = "Email for budget alerts. Empty disables the budget."
  type        = string
  default     = ""
}
