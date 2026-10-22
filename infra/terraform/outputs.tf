output "bucket_name" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.lake.name
}

output "pipeline_access_key_id" {
  value = aws_iam_access_key.pipeline.id
}

output "pipeline_secret_access_key" {
  value     = aws_iam_access_key.pipeline.secret
  sensitive = true
}
