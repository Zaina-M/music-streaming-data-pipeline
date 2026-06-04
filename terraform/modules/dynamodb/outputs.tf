###############################################################################
# modules/dynamodb/outputs.tf
###############################################################################

output "daily_genre_kpis_name" {
  description = "Name of the daily-genre-kpis table."
  value       = aws_dynamodb_table.daily_genre_kpis.name
}

output "top_songs_per_genre_name" {
  description = "Name of the top-songs-per-genre table."
  value       = aws_dynamodb_table.top_songs_per_genre.name
}

output "top_genres_daily_name" {
  description = "Name of the top-genres-daily table."
  value       = aws_dynamodb_table.top_genres_daily.name
}

# Aggregate list — convenient for IAM policies that grant write access
# to all 3 tables in a single statement.
output "table_arns" {
  description = "List of all 3 table ARNs — used by the IAM module to scope DynamoDB permissions."
  value = [
    aws_dynamodb_table.daily_genre_kpis.arn,
    aws_dynamodb_table.top_songs_per_genre.arn,
    aws_dynamodb_table.top_genres_daily.arn,
  ]
}

output "table_names" {
  description = "List of all 3 table names — surfaced as a root output for visibility."
  value = [
    aws_dynamodb_table.daily_genre_kpis.name,
    aws_dynamodb_table.top_songs_per_genre.name,
    aws_dynamodb_table.top_genres_daily.name,
  ]
}
