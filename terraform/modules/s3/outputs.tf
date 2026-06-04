###############################################################################
# modules/s3/outputs.tf
###############################################################################

# Names — used by other modules that need to construct S3 URIs or set
# bucket-name-based event filters.
output "raw_bucket_name" {
  description = "Name of the raw landing bucket."
  value       = aws_s3_bucket.raw.id
}

output "processed_bucket_name" {
  description = "Name of the processed (Parquet) bucket."
  value       = aws_s3_bucket.processed.id
}

output "archive_bucket_name" {
  description = "Name of the archive bucket."
  value       = aws_s3_bucket.archive.id
}

output "scripts_bucket_name" {
  description = "Name of the Glue scripts bucket."
  value       = aws_s3_bucket.scripts.id
}

# ARNs — used by IAM policies that grant access to these buckets.
output "raw_bucket_arn" {
  description = "ARN of the raw bucket."
  value       = aws_s3_bucket.raw.arn
}

output "processed_bucket_arn" {
  description = "ARN of the processed bucket."
  value       = aws_s3_bucket.processed.arn
}

output "archive_bucket_arn" {
  description = "ARN of the archive bucket."
  value       = aws_s3_bucket.archive.arn
}

output "scripts_bucket_arn" {
  description = "ARN of the scripts bucket."
  value       = aws_s3_bucket.scripts.arn
}

# Reference data S3 keys — passed to Glue jobs as parameters so the
# scripts know where to read from.
output "songs_reference_key" {
  description = "S3 key of the uploaded songs.csv reference file."
  value       = aws_s3_object.songs_reference.key
}

output "users_reference_key" {
  description = "S3 key of the uploaded users.csv reference file."
  value       = aws_s3_object.users_reference.key
}
