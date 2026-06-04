###############################################################################
# modules/glue/outputs.tf
###############################################################################

output "validate_job_name" {
  description = "Name of the validate Glue job — referenced by Step Functions."
  value       = aws_glue_job.validate.name
}

output "transform_job_name" {
  description = "Name of the transform Glue job."
  value       = aws_glue_job.transform.name
}

output "load_job_name" {
  description = "Name of the load Glue job."
  value       = aws_glue_job.load.name
}

# Aggregate list — used by the monitoring module to create one
# CloudWatch alarm per job in a loop.
output "all_job_names" {
  description = "All 3 Glue job names — used for CloudWatch alarm fan-out."
  value = [
    aws_glue_job.validate.name,
    aws_glue_job.transform.name,
    aws_glue_job.load.name,
  ]
}

output "catalog_database_name" {
  description = "Glue Data Catalog database name."
  value       = aws_glue_catalog_database.this.name
}
