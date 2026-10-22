terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project = "data-engineer-a-z"
      owner   = var.owner
    }
  }
}

# ── Data lake bucket ────────────────────────────────────────────────
resource "aws_s3_bucket" "lake" {
  bucket        = var.bucket_name
  force_destroy = true # lab bucket: cho phép destroy khi còn object
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-bronze"
    status = "Enabled"
    filter {
      prefix = "bronze/"
    }
    expiration {
      days = var.bronze_retention_days
    }
  }
}

resource "aws_s3_object" "layers" {
  for_each = toset(["bronze/", "silver/", "gold/"])
  bucket   = aws_s3_bucket.lake.id
  key      = each.value
  content  = ""
}

# ── Glue catalog ────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "lake" {
  name         = var.glue_database
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/"
}

# ── IAM: least-privilege pipeline user ──────────────────────────────
resource "aws_iam_user" "pipeline" {
  name = "${var.name_prefix}-pipeline"
}

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid       = "ObjectRW"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_user_policy" "pipeline" {
  name   = "${var.name_prefix}-pipeline-policy"
  user   = aws_iam_user.pipeline.name
  policy = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_access_key" "pipeline" {
  user = aws_iam_user.pipeline.name
}
