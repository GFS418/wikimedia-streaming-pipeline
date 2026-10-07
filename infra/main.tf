# Wikimedia streaming pipeline - AWS infrastructure.
#
# A private, versioned S3 landing bucket that the local Kafka consumer writes
# offset-range files into, and an IAM role that Databricks Unity Catalog assumes
# to READ them (read-only external location on landing/). Databricks never gets
# write access to the raw landing zone; its checkpoints and Delta tables live in
# Databricks-managed storage. Plus a project-scoped budget alarm.
#
# Everything here is disposable: `terraform destroy` removes it all
# (force_destroy empties the bucket, including old versions, first).

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.66"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = { project = "wikistream" }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id  = data.aws_caller_identity.current.account_id
  bucket_name = "wikistream-landing-${local.account_id}"
  role_name   = "wikistream-databricks-uc"
  role_arn    = "arn:aws:iam::${local.account_id}:role/${local.role_name}"
}

# ---------------------------------------------------------------- landing bucket

resource "aws_s3_bucket" "landing" {
  bucket        = local.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Versioning makes consumer rewrites visible: an offset-range file rewritten after
# a crash becomes a second version of the same key, and identical ETags prove the
# rewrite was byte-identical. Old versions expire quickly (see lifecycle).
resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket     = aws_s3_bucket.landing.id
  depends_on = [aws_s3_bucket_versioning.landing]

  rule {
    id     = "expire-landing"
    status = "Enabled"
    filter {
      prefix = "landing/"
    }
    expiration {
      days = var.landing_retention_days
    }
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_retention_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Delete markers left behind once all versions of a key have expired.
  rule {
    id     = "cleanup-delete-markers"
    status = "Enabled"
    filter {}
    expiration {
      expired_object_delete_marker = true
    }
  }
}

# ------------------------------------------------- Databricks Unity Catalog role
#
# Two-step setup (Databricks docs, "Create a storage credential ... manually"):
#   1. apply with defaults: trust = Databricks UC master role, placeholder external ID
#   2. create the storage credential in Databricks with this role's ARN; it shows
#      an external ID
#   3. apply again with databricks_external_id = <that ID> and self_assume = true
# IAM rejects a trust policy naming a role that does not exist yet, which is why
# self-trust can only be added on the second apply.

data "aws_iam_policy_document" "databricks_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type = "AWS"
      identifiers = concat(
        [var.databricks_uc_master_role_arn],
        var.self_assume ? [local.role_arn] : [],
      )
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_external_id]
    }
  }
}

resource "aws_iam_role" "databricks_uc" {
  name               = local.role_name
  assume_role_policy = data.aws_iam_policy_document.databricks_trust.json
}

# Read-only on landing/. ListBucket is scoped to the prefix by condition.
data "aws_iam_policy_document" "databricks_read_landing" {
  statement {
    sid       = "ReadLandingObjects"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.landing.arn}/landing/*"]
  }
  statement {
    sid       = "ListLandingPrefix"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.landing.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["landing", "landing/*"]
    }
  }
  statement {
    sid       = "BucketLocation"
    actions   = ["s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }
  statement {
    sid       = "SelfAssume"
    actions   = ["sts:AssumeRole"]
    resources = [local.role_arn]
  }
}

resource "aws_iam_role_policy" "databricks_read_landing" {
  name   = "read-landing"
  role   = aws_iam_role.databricks_uc.id
  policy = data.aws_iam_policy_document.databricks_read_landing.json
}

# ------------------------------------------------------------ local consumer role
#
# The laptop consumer writes offset-range files under landing/recentchange/ and finds
# its position by LISTING that prefix (decision D11: S3 is the record). It can create
# objects and list its own prefix, nothing else: no read, no delete, no other prefix.
# Assumed from the identity that runs Terraform (or var.consumer_principal_arn) via an
# AWS CLI profile with role_arn + source_profile.

locals {
  consumer_prefix = "landing/recentchange/"
}

data "aws_iam_policy_document" "consumer_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [coalesce(var.consumer_principal_arn, data.aws_caller_identity.current.arn)]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name                 = "wikistream-consumer"
  assume_role_policy   = data.aws_iam_policy_document.consumer_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "consumer_write_landing" {
  statement {
    sid       = "PutLandingFiles"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.landing.arn}/${local.consumer_prefix}*"]
  }
  statement {
    sid       = "ListOwnPrefix"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.landing.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.consumer_prefix}*"]
    }
  }
}

resource "aws_iam_role_policy" "consumer_write_landing" {
  name   = "write-landing-recentchange"
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.consumer_write_landing.json
}

# ---------------------------------------------------------------- cost guardrail
#
# Scoped to resources tagged project=wikistream. The tag must be activated as a
# cost allocation tag in the Billing console (one-time, takes up to 24 h) before
# this filter sees any spend. Created only when an alert email is provided.

resource "aws_budgets_budget" "project" {
  count = var.budget_alert_email == "" ? 0 : 1

  name         = "wikistream-monthly"
  budget_type  = "COST"
  limit_amount = var.monthly_budget_usd
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:project$wikistream"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
