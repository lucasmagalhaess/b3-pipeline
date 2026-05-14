output "bucket_name" {
  value = google_storage_bucket.data_lake.name
}

output "analytics_dataset" {
  value = google_bigquery_dataset.analytics.dataset_id
}
