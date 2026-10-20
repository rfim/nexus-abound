# NEXUS for Abound on AWS: Aurora → DMS → S3 → Glue (Spark + Iceberg) → Athena.
# Written to be read and reviewed; not applied (there is no AWS account behind a portfolio).

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
}

provider "aws" { region = var.region }

variable "region" { default = "eu-west-2" }
variable "aurora_endpoint" {}
variable "aurora_secret_arn" {}           # username and password live in Secrets Manager, never here
variable "subnet_ids" { type = list(string) }
variable "vpc_security_group_ids" { type = list(string) }

resource "aws_kms_key" "lake" {
  description         = "NEXUS lake: landing and warehouse"
  enable_key_rotation = true
}

resource "aws_s3_bucket" "lake" { bucket = "nexus-lake-${data.aws_caller_identity.me.account_id}" }
data "aws_caller_identity" "me" {}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# landing files are replayable from bronze after 30 days; keep the bill down
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "landing-expiry"
    status = "Enabled"
    filter { prefix = "dms/" }
    expiration { days = 30 }
  }
}

# ---------- DMS ----------
resource "aws_dms_replication_subnet_group" "this" {
  replication_subnet_group_id          = "nexus-dms"
  replication_subnet_group_description = "NEXUS DMS"
  subnet_ids                           = var.subnet_ids
}

resource "aws_dms_replication_instance" "this" {
  replication_instance_id     = "nexus-cdc"
  replication_instance_class  = "dms.t3.medium"
  allocated_storage           = 50
  multi_az                    = true
  publicly_accessible         = false
  replication_subnet_group_id = aws_dms_replication_subnet_group.this.id
  vpc_security_group_ids      = var.vpc_security_group_ids
}

resource "aws_dms_endpoint" "aurora" {
  endpoint_id                     = "aurora-lending"
  endpoint_type                   = "source"
  engine_name                     = "aurora-postgresql"
  database_name                   = "lending"
  secrets_manager_arn             = var.aurora_secret_arn
  secrets_manager_access_role_arn = aws_iam_role.dms.arn
  ssl_mode                        = "require"
  # logical replication slot + a heartbeat so the slot advances, and freshness is measurable, on quiet nights
  extra_connection_attributes = "PluginName=pglogical;HeartbeatEnable=true;HeartbeatFrequency=5"
}

resource "aws_dms_s3_endpoint" "landing" {
  endpoint_id             = "s3-landing"
  endpoint_type           = "target"
  bucket_name             = aws_s3_bucket.lake.bucket
  bucket_folder           = "dms"
  service_access_role_arn = aws_iam_role.dms.arn
  data_format             = "csv"
  add_column_name         = true
  include_op_for_full_load = true
  timestamp_column_name   = "cdc_ts"
  cdc_path                = "lending"
  date_partition_enabled  = true
  date_partition_sequence = "YYYYMMDD"
  date_partition_delimiter = "SLASH"
  cdc_max_batch_interval  = 3600
  encryption_mode         = "SSE_KMS"
  server_side_encryption_kms_key_id = aws_kms_key.lake.arn
}

resource "aws_dms_replication_task" "cdc" {
  replication_task_id      = "lending-full-load-and-cdc"
  migration_type           = "full-load-and-cdc"
  replication_instance_arn = aws_dms_replication_instance.this.replication_instance_arn
  source_endpoint_arn      = aws_dms_endpoint.aurora.endpoint_arn
  target_endpoint_arn      = aws_dms_s3_endpoint.landing.endpoint_arn
  table_mappings           = file("${path.module}/../dms/table-mappings.json")
  replication_task_settings = file("${path.module}/../dms/task-settings.json")
}

# alert on change volume per table: a gap pages before a month-end report finds it
resource "aws_cloudwatch_metric_alarm" "cdc_latency" {
  alarm_name          = "nexus-dms-cdc-latency-source"
  namespace           = "AWS/DMS"
  metric_name         = "CDCLatencySource"
  dimensions          = { ReplicationInstanceIdentifier = "nexus-cdc", ReplicationTaskIdentifier = "lending-full-load-and-cdc" }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 900
  comparison_operator = "GreaterThanThreshold"
}

# ---------- Glue ----------
resource "aws_glue_catalog_database" "layers" {
  for_each = toset(["bronze", "silver", "gold", "ops"])
  name     = each.key
}

resource "aws_glue_job" "cdc" {
  name              = "nexus-cdc-apply"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 60
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/code/glue/cdc_job.py"
  }
  default_arguments = {
    "--datalake-formats"                 = "iceberg"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.lake.bucket}/code/nexus.zip"
    "--warehouse"                        = "s3://${aws_s3_bucket.lake.bucket}/warehouse/"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"   # the file log in ops is the bookmark
  }
}

resource "aws_glue_trigger" "hourly" {
  name     = "nexus-cdc-hourly"
  type     = "SCHEDULED"
  schedule = "cron(10 * * * ? *)"
  actions { job_name = aws_glue_job.cdc.name }
}

# ---------- IAM: least privilege ----------
data "aws_iam_policy_document" "dms_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["dms.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dms" {
  name               = "nexus-dms"
  assume_role_policy = data.aws_iam_policy_document.dms_assume.json
}

data "aws_iam_policy_document" "dms" {
  statement {
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/dms/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.aurora_secret_arn]
  }
  statement {
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [aws_kms_key.lake.arn]
  }
}

resource "aws_iam_role_policy" "dms" {
  role   = aws_iam_role.dms.id
  policy = data.aws_iam_policy_document.dms.json
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "nexus-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/dms/*", "${aws_s3_bucket.lake.arn}/code/*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/warehouse/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
               "glue:GetPartitions"]
    resources = ["*"]
  }
  statement {
    actions   = ["kms:GenerateDataKey", "kms:Decrypt"]
    resources = [aws_kms_key.lake.arn]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}
